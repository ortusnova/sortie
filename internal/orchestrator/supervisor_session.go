package orchestrator

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/prompt"
	"github.com/sortie-ai/sortie/internal/redact"
)

// What starts a supervisor session.
const (
	SupervisorTriggerPark                = "park"                 // a story parked
	SupervisorTriggerUnclassifiedFailure = "unclassified_failure" // a stopped failure whose signature is in no class
	SupervisorTriggerStalledReaction     = "stalled_reaction"     // a pull request reaction made no progress for an hour
	SupervisorTriggerIdleSlot            = "idle_slot"            // a free slot with nothing to dispatch while stories are held
	SupervisorTriggerSweep               = "sweep"                // the periodic sweep
)

// How a supervisor session ended.
const (
	SupervisorOutcomeCompleted  = "completed"        // the turn completed
	SupervisorOutcomeFailed     = "failed"           // the prompt, the session start, or the turn failed
	SupervisorOutcomePaused     = "paused"           // a pause stopped it
	SupervisorOutcomeBudget     = "budget_exhausted" // it reached the daily token budget
	SupervisorOutcomeSessionCap = "session_cap"      // it reached supervisor.session_tokens
	SupervisorOutcomeCancelled  = "cancelled"        // Sortie stopped
)

// What the supervisor is doing, as the state API serves it.
const (
	SupervisorStatusIdle            = "idle"
	SupervisorStatusRunning         = "running"
	SupervisorStatusPaused          = "paused"
	SupervisorStatusBudgetExhausted = "budget_exhausted"
	SupervisorStatusUnavailable     = "unavailable"
)

// supervisorStallAfter is how long a pull request reaction goes without
// progress before it starts a supervisor session.
const supervisorStallAfter = time.Hour

// maxPendingSupervisorTriggers bounds the triggers kept for the next
// session; the oldest is dropped first.
const maxPendingSupervisorTriggers = 32

// Session variables of a supervisor session.
const (
	envSupervisorToken      = "SORTIE_SUPERVISOR_TOKEN" //nolint:gosec // a variable name, not a credential
	sessionPurposeSupervise = "supervisor"
)

var (
	errSupervisorPaused     = errors.New("the operator paused the supervisor")
	errSupervisorBudget     = errors.New("the supervisor reached its daily token budget")
	errSupervisorSessionCap = errors.New("the supervisor session reached supervisor.session_tokens")
)

// SupervisorTrigger is one event that asks for a supervisor session.
// IssueID and Identifier are empty for a trigger about no one story.
type SupervisorTrigger struct {
	Kind       string
	IssueID    string
	Identifier string
	Detail     string
	At         time.Time
}

// SupervisorPause is a pause of the supervisor an operator asked for. It
// lives in memory only, so a restart clears it.
type SupervisorPause struct {
	Since  time.Time
	By     string
	Reason string
}

// SupervisorSession is one supervisor session: running while EndedAt is
// zero. Triggers holds what started it; a session read back from the
// store holds its first trigger, and TriggerCount counts them all. ID is
// its stored row, 0 when it was not stored. Digest is the state digest it
// started on. Actions counts the supervisor actions Sortie took in it, -1
// while it runs or when they were not counted.
type SupervisorSession struct {
	ID           int64
	StartedAt    time.Time
	EndedAt      time.Time
	Triggers     []SupervisorTrigger
	TriggerCount int
	Outcome      string
	Error        string
	Tokens       int64
	Digest       string
	Actions      int
}

// SupervisorSuspension is a trigger kind that starts no session until the
// state digest differs from Digest: its last Sessions sessions, the first
// started at Since, took no action on that digest. At is when it began.
type SupervisorSuspension struct {
	Kind     string
	Digest   string
	Sessions int
	Since    time.Time
	At       time.Time
}

// SupervisorBudget is the supervisor's spend in the current UTC day
// against its daily budget.
type SupervisorBudget struct {
	Day                time.Time // UTC midnight that began the day
	ResetsAt           time.Time
	SessionsUsed       int
	SessionsLimit      int
	TokensUsed         int64
	TokensLimit        int64
	SessionTokensLimit int64 // supervisor.session_tokens
	HourTokensUsed     int64 // the tokens of the last hour, the running session's included
	HourTokensLimit    int64 // the hourly spend over which only sweeps start a session
	HourPace           int64 // the even hourly pace of the daily token budget
	Reserve            bool  // less than a fifth of the day's tokens is left
	Velocity           bool  // the hourly spend went over HourTokensLimit and is not yet back under HourPace
}

// SupervisorSnapshot is the supervisor's runtime state for the state API.
type SupervisorSnapshot struct {
	Status      string
	Agent       string
	Unavailable string // why no session can start; empty when one can
	Pause       *SupervisorPause
	Running     *SupervisorSession
	LastSession *SupervisorSession
	LastTrigger *SupervisorTrigger
	Pending     []SupervisorTrigger
	Budget      SupervisorBudget
	NextSweepAt time.Time
	Suspended   []SupervisorSuspension // sorted by kind
	Available   bool                   // the supervisor can start the sessions it would start
	Reason      string                 // why it cannot: budget_exhausted, credential, paused or velocity; empty while available
	ResetsAt    time.Time              // when Reason resets, zero when not known
	Since       time.Time              // when the supervisor became unavailable, zero while available
	Deferred    []SupervisorDeferral
}

// supervisorRun is the running supervisor session.
type supervisorRun struct {
	rowID     int64
	startedAt time.Time
	triggers  []SupervisorTrigger
	digest    string
	quiet     bool   // a quiet sweep: the digest is the last session's
	mark      int64  // the newest supervisor action before it started; -1 when unknown
	kind      string // the agent kind it runs
	sampled   int64  // its tokens already counted in the hourly spend; the event loop's only
	tokens    atomic.Int64
	cancel    context.CancelCauseFunc
}

// noopStreak counts the consecutive sessions of one trigger kind that
// took no action on one state digest.
type noopStreak struct {
	digest string
	count  int
	since  time.Time
}

// supervisorState is the event loop's record of the supervisor. Only the
// event loop touches it, except supervisorRun.tokens.
type supervisorState struct {
	enabled      bool
	cfg          config.SupervisorConfig
	loaded       bool
	pending      []SupervisorTrigger
	run          *supervisorRun
	pause        *SupervisorPause
	lastTrigger  *SupervisorTrigger
	last         *SupervisorSession
	day          time.Time
	sessionsUsed int
	tokensUsed   int64
	nextSweepAt  time.Time
	unavailable  string
	budgetLogged time.Time
	stalledSeen  map[string]time.Time
	readySince   map[string]time.Time // owner/repo#number@head of each ready pull request to when it was first seen ready
	noops        map[string]*noopStreak
	suspended    map[string]*SupervisorSuspension
	spend        []spendSample // the tokens spent in about the last hour
	velocity     bool
	deferred     []SupervisorDeferral
	avail        supervisorAvailability
}

func newSupervisorState() *supervisorState {
	return &supervisorState{
		stalledSeen: map[string]time.Time{}, readySince: map[string]time.Time{},
		noops: map[string]*noopStreak{}, suspended: map[string]*SupervisorSuspension{},
	}
}

// supervisorSessionStore records the supervisor's sessions.
// [persistence.Store] implements it.
type supervisorSessionStore interface {
	StartSupervisorSession(ctx context.Context, session persistence.SupervisorSession) (int64, error)
	EndSupervisorSession(ctx context.Context, id int64, endedAt time.Time, outcome, errText string, tokens int64, actions int) error
	SupervisorUsageSince(ctx context.Context, since time.Time) (persistence.SupervisorUsage, error)
	RecentSupervisorSessions(ctx context.Context, limit int) ([]persistence.SupervisorSession, error)
	LastSupervisorActionID(ctx context.Context) (int64, error)
	SupervisorActionsDoneAfter(ctx context.Context, afterID int64) (int, error)
}

var _ supervisorSessionStore = (*persistence.Store)(nil)

func utcDay(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// noteSupervisorTrigger keeps t for the next supervisor session. A
// trigger of the kind and story of one already kept replaces it. It does
// nothing while no supervisor is configured.
func noteSupervisorTrigger(state *State, t SupervisorTrigger) {
	s := state.supervisor
	if s == nil || !s.enabled {
		return
	}
	if t.At.IsZero() {
		t.At = time.Now()
	}
	t.At = t.At.UTC()
	last := t
	s.lastTrigger = &last
	s.pending = keepTrigger(s.pending, t)
}

// keepTrigger returns triggers with t kept: in place of one of its kind
// and story, else appended, the oldest dropped past
// maxPendingSupervisorTriggers.
func keepTrigger(triggers []SupervisorTrigger, t SupervisorTrigger) []SupervisorTrigger {
	for i, p := range triggers {
		if p.Kind == t.Kind && p.IssueID == t.IssueID {
			triggers[i] = t
			return triggers
		}
	}
	if len(triggers) >= maxPendingSupervisorTriggers {
		triggers = slices.Delete(triggers, 0, 1)
	}
	return append(triggers, t)
}

// reactionProgressAt is the newest time a pending reaction recorded
// progress: its creation, a new pull request head, or a restarted watch.
func reactionProgressAt(p *PendingReaction) time.Time {
	at := p.CreatedAt
	for _, t := range []time.Time{p.HeadRecordedAt, p.WatchRestartedAt} {
		if t.After(at) {
			at = t
		}
	}
	return at
}

// noteStalledReactions starts a supervisor session for each pending
// reaction whose last progress is an hour or more before now, once per
// progress mark.
func noteStalledReactions(state *State, now time.Time) {
	s := state.supervisor
	if s == nil || !s.enabled {
		return
	}
	for key := range s.stalledSeen {
		if _, ok := state.PendingReactions[key]; !ok {
			delete(s.stalledSeen, key)
		}
	}
	keys := slices.Sorted(func(yield func(string) bool) {
		for k := range state.PendingReactions {
			if !yield(k) {
				return
			}
		}
	})
	for _, key := range keys {
		p := state.PendingReactions[key]
		mark := reactionProgressAt(p)
		if mark.IsZero() || now.Sub(mark) < supervisorStallAfter || s.stalledSeen[key].Equal(mark) {
			continue
		}
		s.stalledSeen[key] = mark
		detail := fmt.Sprintf("the %s reaction has made no progress since %s", p.Kind, mark.UTC().Format(time.RFC3339))
		if pr := pullRequestOf(p); pr.Number > 0 {
			detail += " on " + formatPullRequest(pr)
		}
		noteSupervisorTrigger(state, SupervisorTrigger{
			Kind: SupervisorTriggerStalledReaction, IssueID: p.IssueID, Identifier: p.Identifier, Detail: detail, At: now,
		})
	}
}

// noteIdleSlot asks for a supervisor session when a dispatch pass left a
// story slot free and dispatched nothing while stories are held. Whether
// one starts is the state digest's to decide, so a pass that finds the
// same stories held for the same reasons starts none. It also keeps the
// time each ready pull request was first seen ready.
func noteIdleSlot(state *State, dispatched int, now time.Time) {
	s := state.supervisor
	if s == nil || !s.enabled {
		return
	}
	observeSupervisorWake(state, now)
	held := heldStoryIDs(state)
	idle := dispatched == 0 && state.dispatchPause == nil && len(held) > 0 &&
		GlobalAvailableSlots(state.MaxConcurrentAgents, AgentsRunning(state.Running)) > 0
	if !idle {
		return
	}
	noteSupervisorTrigger(state, SupervisorTrigger{
		Kind:   SupervisorTriggerIdleSlot,
		Detail: fmt.Sprintf("a story slot is free, nothing was dispatched, and %d stories are held", len(held)),
		At:     now,
	})
}

// heldStoryIDs returns the sorted IDs of the stories held out of dispatch
// for a reason other than a full slot or a live lease, whose holder is
// already at work on the story.
func heldStoryIDs(state *State) []string {
	set := map[string]struct{}{}
	for id, h := range state.Held {
		if h.Reason != SkipAgentSlotsFull && h.Reason != SkipSSHHostsFull && h.Reason != SkipStandingHold {
			set[id] = struct{}{}
		}
	}
	for id := range state.FaultHolds {
		set[id] = struct{}{}
	}
	for id := range state.BudgetExhausted {
		set[id] = struct{}{}
	}
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// supervisorDir returns the directory the supervisor session runs in.
func (o *Orchestrator) supervisorDir() string {
	if o.supervisorWorkDir != "" {
		return o.supervisorWorkDir
	}
	if o.dbPath != "" {
		return filepath.Join(filepath.Dir(o.dbPath), "supervisor")
	}
	return filepath.Join(os.TempDir(), "sortie-supervisor")
}

// supervisorPromptPath resolves the prompt path against the directory of
// the workflow file.
func (o *Orchestrator) supervisorPromptPath(p string) string {
	if filepath.IsAbs(p) {
		return p
	}
	if abs := o.workflowManager.WorkflowAbsPath(); abs != "" {
		return filepath.Join(filepath.Dir(abs), p)
	}
	return p
}

// loadSupervisorUsage reads, once, the sessions and tokens the current UTC
// day already spent and the newest session, so a restart keeps the budget.
func (o *Orchestrator) loadSupervisorUsage(ctx context.Context, now time.Time) {
	s := o.state.supervisor
	if s.loaded {
		return
	}
	s.loaded = true
	s.day = utcDay(now)
	store, ok := o.store.(supervisorSessionStore)
	if !ok {
		return
	}
	usage, err := store.SupervisorUsageSince(ctx, s.day)
	if err != nil {
		o.logger.Warn("supervisor usage read failed, counting from zero", slog.Any("error", err))
	} else {
		s.sessionsUsed, s.tokensUsed = usage.Sessions, usage.Tokens
	}
	recent, err := store.RecentSupervisorSessions(ctx, supervisorStreakWindow)
	if err != nil {
		o.logger.Warn("supervisor sessions read failed, starting with no last digest", slog.Any("error", err))
		return
	}
	var prev *persistence.SupervisorSession
	for i := range recent {
		row := &recent[i]
		quiet := row.TriggerKind == SupervisorTriggerSweep && prev != nil && row.Digest != "" && row.Digest == prev.Digest
		s.noteSessionNoops(row.TriggerKind, row.Digest, quiet, row.Outcome, row.Actions, row.StartedAt, cmp.Or(row.EndedAt, row.StartedAt))
		if !row.EndedAt.IsZero() && now.Sub(row.EndedAt) < time.Hour {
			s.noteSpend(row.EndedAt, row.Tokens)
		}
		prev = row
	}
	if prev == nil {
		return
	}
	s.last = &SupervisorSession{
		ID: prev.ID, StartedAt: prev.StartedAt, EndedAt: prev.EndedAt, TriggerCount: prev.Triggers,
		Triggers: []SupervisorTrigger{{Kind: prev.TriggerKind, Identifier: prev.TriggerIssue, Detail: prev.TriggerDetail, At: prev.StartedAt}},
		Outcome:  prev.Outcome, Error: prev.Error, Tokens: prev.Tokens, Digest: prev.Digest, Actions: prev.Actions,
	}
	if s.last.Outcome == "" {
		s.last.Outcome = SupervisorOutcomeCancelled
	}
}

// supervisorStreakWindow is how many of the newest stored sessions a
// restart reads to rebuild the no-op streaks.
const supervisorStreakWindow = 64

// noteSessionNoops applies one ended session of the trigger kind kind,
// started at startedAt on digest, to that kind's no-op streak, and returns
// the suspension it began, nil when it began none. A quiet sweep, a
// session a pause or a stop cut short, and one whose actions were not
// counted leave the streak alone; a session that took an action ends it.
func (s *supervisorState) noteSessionNoops(kind, digest string, quiet bool, outcome string, actions int, startedAt, now time.Time) *SupervisorSuspension {
	switch {
	case quiet, digest == "", actions < 0, outcome == "", outcome == SupervisorOutcomePaused, outcome == SupervisorOutcomeCancelled:
		return nil
	case actions > 0:
		delete(s.noops, kind)
		return nil
	}
	streak := s.noops[kind]
	if streak == nil || streak.digest != digest {
		streak = &noopStreak{digest: digest, since: startedAt}
		s.noops[kind] = streak
	}
	streak.count++
	limit := max(s.cfg.NoopLimit, 1)
	if streak.count < limit {
		return nil
	}
	if held, ok := s.suspended[kind]; ok && held.Digest == digest {
		held.Sessions = streak.count
		return nil
	}
	suspension := &SupervisorSuspension{Kind: kind, Digest: digest, Sessions: streak.count, Since: streak.since, At: now.UTC()}
	s.suspended[kind] = suspension
	return suspension
}

// admitSupervisorTriggers keeps of the waiting triggers those that start a
// session on digest, the state digest now: none when digest is the last
// session's, except a sweep once supervisor.quiet_sweep_ms has passed
// since that session started; and none of a suspended kind while digest
// is the one it was suspended on. A trigger of a suspended kind on another
// digest lifts the suspension. It reports whether the session it admits is
// a quiet sweep. Must run on the event loop.
func (o *Orchestrator) admitSupervisorTriggers(digest string, now time.Time) (quiet bool) {
	s := o.state.supervisor
	changed := s.last == nil || s.last.Digest != digest
	quietDue := s.last == nil || !now.Before(s.last.StartedAt.Add(time.Duration(s.cfg.QuietSweepMS)*time.Millisecond))
	kept := make([]SupervisorTrigger, 0, len(s.pending))
	var dropped []string
	for _, t := range s.pending {
		quietSweep := !changed && quietDue && t.Kind == SupervisorTriggerSweep
		if held, ok := s.suspended[t.Kind]; ok {
			if held.Digest != digest {
				delete(s.suspended, t.Kind)
				delete(s.noops, t.Kind)
				o.logger.Info("supervisor trigger kind resumed: the state digest changed",
					slog.String("supervisor_trigger", t.Kind), slog.String("suspended_digest", held.Digest), slog.String("digest", digest))
			} else if !quietSweep {
				dropped = append(dropped, t.Kind)
				continue
			}
		}
		if !changed && !quietSweep {
			dropped = append(dropped, t.Kind)
			continue
		}
		if quietSweep {
			t.Detail = "the quiet sweep: the state digest is unchanged since the last session"
		}
		kept = append(kept, t)
	}
	if len(dropped) > 0 {
		o.logger.Debug("supervisor triggers started no session: the state digest is unchanged or their kind is suspended",
			slog.String("digest", digest), slog.Any("supervisor_triggers", dropped))
	}
	s.pending = kept
	return !changed && len(kept) > 0
}

// budgetStop returns why the daily budget starts no session, or "".
func (s *supervisorState) budgetStop(sc config.SupervisorConfig) string {
	switch {
	case s.sessionsUsed >= sc.MaxSessionsPerDay:
		return fmt.Sprintf("the supervisor started %d of supervisor.max_sessions_per_day %d sessions today", s.sessionsUsed, sc.MaxSessionsPerDay)
	case s.tokensUsed >= int64(sc.MaxTokensPerDay):
		return fmt.Sprintf("the supervisor spent %d of supervisor.max_tokens_per_day %d tokens today", s.tokensUsed, sc.MaxTokensPerDay)
	}
	return ""
}

// unavailableReason returns why no supervisor session can start, or "".
func (o *Orchestrator) unavailableReason(sc config.SupervisorConfig) string {
	switch {
	case o.supervisorToken == "":
		return "server.supervisor_token is not set, so the supervisor could not act"
	case o.sessionAPIURL == "":
		return "the HTTP server is off, so the supervisor could not reach Sortie"
	}
	if _, err := o.agentAdapterByKind(sc.Agent); err != nil {
		return "supervisor.agent " + sc.Agent + " is not available: " + err.Error()
	}
	return ""
}

// superviseOnLoop keeps the supervisor's state in step with the
// configuration and now: it rolls the budget day, adds the sweep when it
// is due, and starts a session for the kept triggers when none runs, the
// supervisor is not paused, and the budget allows one. Must run on the
// event loop.
func (o *Orchestrator) superviseOnLoop(ctx context.Context, now time.Time) {
	s := o.state.supervisor
	if s == nil || ctx.Err() != nil {
		return
	}
	sc := o.workflowManager.Config().Supervisor
	if !sc.Enabled() || o.once != nil {
		if s.enabled {
			s.enabled, s.pending, s.nextSweepAt, s.unavailable = false, nil, time.Time{}, ""
			s.deferred, s.avail = nil, supervisorAvailability{}
			o.notifyObservers()
		}
		return
	}
	o.superviseStep(ctx, sc, now)
	o.noteSupervisorAvailability(ctx, sc, now)
}

// superviseStep is superviseOnLoop's work while a supervisor is
// configured. Must run on the event loop.
func (o *Orchestrator) superviseStep(ctx context.Context, sc config.SupervisorConfig, now time.Time) {
	s := o.state.supervisor
	s.cfg = sc
	interval := time.Duration(sc.SweepIntervalMS) * time.Millisecond
	if !s.enabled {
		s.enabled = true
		s.nextSweepAt = now.Add(interval)
	}
	o.loadSupervisorUsage(ctx, now)
	if day := utcDay(now); day.After(s.day) {
		s.day, s.sessionsUsed, s.tokensUsed = day, 0, 0
	}
	if s.nextSweepAt.IsZero() || s.nextSweepAt.After(now.Add(interval)) {
		s.nextSweepAt = now.Add(interval)
	}
	if !now.Before(s.nextSweepAt) {
		noteSupervisorTrigger(o.state, SupervisorTrigger{Kind: SupervisorTriggerSweep, Detail: "the periodic sweep", At: now})
		s.nextSweepAt = now.Add(interval)
	}
	o.sampleSupervisorSpend(now)
	o.guardSupervisorSpend(ctx, sc, now)
	if s.run != nil || s.pause != nil || len(s.pending) == 0 {
		return
	}
	if supervisorCredentialFault(o.state, sc.Agent) != nil {
		return
	}
	digest := supervisorStateDigest(o.state, now)
	quiet := o.admitSupervisorTriggers(digest, now)
	if len(s.pending) == 0 {
		o.notifyObservers()
		return
	}
	if reason := o.unavailableReason(sc); reason != "" {
		if reason != s.unavailable {
			o.logger.Error("supervisor session not started", slog.String("reason", reason), slog.Int("triggers", len(s.pending)))
		}
		s.unavailable, s.pending = reason, nil
		o.notifyObservers()
		return
	}
	s.unavailable = ""
	if reason := s.budgetStop(sc); reason != "" {
		if !s.budgetLogged.Equal(s.day) {
			s.budgetLogged = s.day
			o.logger.Warn("supervisor budget exhausted, no session starts until the next UTC day",
				slog.String("reason", reason), slog.Int("triggers_dropped", len(s.pending)))
		}
		s.pending, s.deferred = nil, nil
		o.notifyObservers()
		return
	}
	o.startSupervisorSession(ctx, sc, now, digest, quiet)
}

// startSupervisorSession starts a session on the state digest for every
// kept trigger. Must run on the event loop.
func (o *Orchestrator) startSupervisorSession(ctx context.Context, sc config.SupervisorConfig, now time.Time, digest string, quiet bool) {
	s := o.state.supervisor
	adapter, _ := o.agentAdapterByKind(sc.Agent)
	triggers := s.pending
	s.pending = nil
	runCtx, cancel := context.WithCancelCause(ctx)
	run := &supervisorRun{startedAt: now.UTC(), triggers: triggers, digest: digest, quiet: quiet, mark: -1, kind: sc.Agent, cancel: cancel}
	s.run = run
	s.sessionsUsed++

	first := triggers[0]
	if store, ok := o.store.(supervisorSessionStore); ok {
		if mark, err := store.LastSupervisorActionID(ctx); err != nil {
			o.logger.Warn("supervisor action mark read failed, the session's actions go uncounted", slog.Any("error", err))
		} else {
			run.mark = mark
		}
		id, err := store.StartSupervisorSession(ctx, persistence.SupervisorSession{
			TriggerKind: first.Kind, TriggerIssue: first.Identifier, TriggerDetail: redact.Mask(first.Detail),
			Triggers: len(triggers), Digest: digest, StartedAt: run.startedAt,
		})
		if err != nil {
			o.logger.Error("supervisor session record failed", slog.Any("error", err))
		}
		run.rowID = id
	}

	cfg := o.workflowManager.Config()
	launch := supervisorLaunch{
		adapter:     adapter,
		cfg:         cfg,
		kind:        sc.Agent,
		promptPath:  o.supervisorPromptPath(sc.Prompt),
		dir:         o.supervisorDir(),
		token:       o.supervisorToken,
		apiURL:      o.sessionAPIURL,
		triggers:    triggers,
		digest:      buildSupervisorDigest(o.state, now),
		tokensLeft:  int64(sc.MaxTokensPerDay) - s.tokensUsed,
		sessionCap:  int64(sc.SessionTokens),
		workflowDir: filepath.Dir(o.workflowManager.WorkflowAbsPath()),
	}
	log := o.logger.With(slog.String("supervisor_trigger", first.Kind), slog.Int("triggers", len(triggers)))
	log.Info("supervisor session started",
		slog.String("agent_kind", sc.Agent),
		slog.String("digest", digest),
		slog.Bool("quiet_sweep", quiet),
		slog.Int("sessions_today", s.sessionsUsed),
		slog.Int64("tokens_today", s.tokensUsed),
	)
	o.notifyObservers()

	o.supervisorWg.Go(func() {
		result := o.runSupervisorSession(runCtx, run, launch, log)
		cancel(nil)
		result.actions = persistence.SupervisorActionsUnknown
		if store, ok := o.store.(supervisorSessionStore); ok {
			endCtx, endCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			if run.mark >= 0 {
				if n, err := store.SupervisorActionsDoneAfter(endCtx, run.mark); err != nil {
					log.Warn("supervisor session actions read failed", slog.Any("error", err))
				} else {
					result.actions = n
				}
			}
			if run.rowID != 0 {
				if err := store.EndSupervisorSession(endCtx, run.rowID, result.endedAt, result.outcome, redact.Mask(result.err), result.tokens, result.actions); err != nil {
					log.Error("supervisor session end record failed", slog.Any("error", err))
				}
			}
			endCancel()
		}
		o.runOnLoop(context.WithoutCancel(ctx), func(loopCtx context.Context) {
			o.endSupervisorSession(loopCtx, run, result, log)
		})
	})
}

// endSupervisorSession records the end of run and starts the next session
// when triggers came in while it ran. Must run on the event loop.
func (o *Orchestrator) endSupervisorSession(ctx context.Context, run *supervisorRun, result supervisorResult, log *slog.Logger) {
	s := o.state.supervisor
	if s.run == run {
		s.run = nil
	}
	if d := result.tokens - run.sampled; d > 0 {
		s.noteSpend(result.endedAt, d)
		run.sampled = result.tokens
	}
	if !utcDay(run.startedAt).Before(s.day) {
		s.tokensUsed += result.tokens
	}
	s.last = &SupervisorSession{
		ID: run.rowID, StartedAt: run.startedAt, EndedAt: result.endedAt, Triggers: run.triggers, TriggerCount: len(run.triggers),
		Outcome: result.outcome, Error: redact.Mask(result.err), Tokens: result.tokens, Digest: run.digest, Actions: result.actions,
	}
	attrs := []any{
		slog.String("outcome", result.outcome),
		slog.Int64("tokens", result.tokens),
		slog.Int("actions", result.actions),
		slog.Duration("duration", result.endedAt.Sub(run.startedAt)),
	}
	if result.err != "" {
		attrs = append(attrs, slog.String("error", redact.Mask(result.err)))
		log.Warn("supervisor session ended", attrs...)
	} else {
		log.Info("supervisor session ended", attrs...)
	}
	if result.credential != "" {
		o.recordAgentCredentialRefused(ctx, run.kind, "the supervisor's session", result.credential)
	}
	kind := run.triggers[0].Kind
	if suspension := s.noteSessionNoops(kind, run.digest, run.quiet, result.outcome, result.actions, run.startedAt, result.endedAt); suspension != nil {
		o.publishLoopSuspected(ctx, suspension, log)
	}
	o.notifyObservers()
	o.superviseOnLoop(ctx, time.Now())
}

// supervisorLaunch is what a supervisor session runs with, captured on
// the event loop.
type supervisorLaunch struct {
	adapter     domain.AgentAdapter
	cfg         config.ServiceConfig
	kind        string
	promptPath  string
	dir         string
	token       string
	apiURL      string
	triggers    []SupervisorTrigger
	digest      supervisorDigest
	tokensLeft  int64
	sessionCap  int64
	workflowDir string
}

// supervisorResult is how a supervisor session ended.
type supervisorResult struct {
	endedAt    time.Time
	outcome    string
	err        string
	tokens     int64
	actions    int    // the supervisor actions Sortie took in the session; -1 when not counted
	credential string // the refusal, when the agent runtime refused or could not confirm its credential
}

// publishLoopSuspected logs that a trigger kind is suspended and publishes
// supervisor.loop_suspected. The event names no story, so it never goes to
// tracker_comment.
func (o *Orchestrator) publishLoopSuspected(ctx context.Context, suspension *SupervisorSuspension, log *slog.Logger) {
	log.Warn("supervisor loop suspected: the trigger kind starts no session until the state digest changes",
		slog.String("suspended_trigger", suspension.Kind),
		slog.String("digest", suspension.Digest),
		slog.Int("sessions", suspension.Sessions),
	)
	o.publishSupervisorEvent(ctx, domain.EventSupervisorLoopSuspected, domain.SupervisorLoopSuspectedFacts{
		TriggerKind: suspension.Kind, Digest: suspension.Digest, Sessions: suspension.Sessions, Since: suspension.Since,
	}, log)
}

// publishSupervisorEvent routes a supervisor event to every destination
// whose events list it. The event names no story, so it never goes to
// tracker_comment.
func (o *Orchestrator) publishSupervisorEvent(ctx context.Context, t domain.EventType, facts domain.MessageFacts, log *slog.Logger) {
	if o.router == nil {
		return
	}
	_, delivery := o.router.Route(orchestratorNotification(t, "", domain.NotificationEnvelope{}, facts)).SplitTrackerComment()
	if delivery.Empty() {
		log.Info("supervisor event had no destination", slog.String("event_type", string(t)))
		return
	}
	deliverDetached(ctx, &o.state.TrackerOpsWg, delivery, log, nil)
}

// supervisorPromptOf renders the supervisor's prompt file for launch.
func supervisorPromptOf(launch supervisorLaunch) (string, error) {
	body, err := os.ReadFile(launch.promptPath)
	if err != nil {
		return "", fmt.Errorf("supervisor.prompt: %w", err)
	}
	tmpl, err := prompt.ParseSupervisor(string(body), launch.promptPath)
	if err != nil {
		return "", err
	}
	triggers := make([]prompt.SupervisorTrigger, 0, len(launch.triggers))
	for _, t := range launch.triggers {
		triggers = append(triggers, prompt.SupervisorTrigger{
			Kind: t.Kind, IssueIdentifier: t.Identifier, Detail: t.Detail, At: t.At.UTC().Format(time.RFC3339),
		})
	}
	return tmpl.Render(prompt.SupervisorContext{Token: launch.token, APIURL: launch.apiURL, Triggers: triggers, State: launch.digest})
}

// runSupervisorSession runs one supervisor session of one turn and
// returns how it ended. A session whose tokens reach what the day's
// budget has left, or supervisor.session_tokens, is stopped.
func (o *Orchestrator) runSupervisorSession(ctx context.Context, run *supervisorRun, launch supervisorLaunch, log *slog.Logger) supervisorResult {
	ended := func(err error) supervisorResult {
		r := supervisorResult{endedAt: time.Now().UTC(), outcome: SupervisorOutcomeCompleted, tokens: run.tokens.Load()}
		switch cause := context.Cause(ctx); {
		case errors.Is(cause, errSupervisorPaused):
			r.outcome, r.err = SupervisorOutcomePaused, cause.Error()
		case errors.Is(cause, errSupervisorBudget):
			r.outcome, r.err = SupervisorOutcomeBudget, cause.Error()
		case errors.Is(cause, errSupervisorSessionCap):
			r.outcome, r.err = SupervisorOutcomeSessionCap, fmt.Sprintf("%s at %d tokens", cause.Error(), r.tokens)
		case ctx.Err() != nil:
			r.outcome, r.err = SupervisorOutcomeCancelled, "Sortie stopped the session"
		case err != nil:
			r.outcome, r.err = SupervisorOutcomeFailed, err.Error()
			if agentErr, ok := errors.AsType[*domain.AgentError](err); ok && agentErr.Kind == domain.ErrCredentialUnverified {
				r.credential = agentErr.Message
			}
		}
		return r
	}

	text, err := supervisorPromptOf(launch)
	if err != nil {
		return ended(err)
	}
	if err := os.MkdirAll(launch.dir, 0o700); err != nil {
		return ended(fmt.Errorf("supervisor directory: %w", err))
	}
	settings := config.ResolveAgentSettings(launch.cfg, config.SettingsSelection{Kind: launch.kind}, launch.workflowDir)
	session, err := launch.adapter.StartSession(ctx, domain.StartSessionParams{
		WorkspacePath: launch.dir,
		AgentConfig:   toDomainAgentConfig(launch.cfg, launch.kind, false),
		Settings:      settings.Passthrough,
		SessionEnv: []domain.EnvVar{
			{Name: envSessionAPIURL, Value: launch.apiURL},
			{Name: envSessionPurpose, Value: sessionPurposeSupervise},
			{Name: envSessionWorkspace, Value: launch.dir},
			{Name: envSupervisorToken, Value: launch.token},
		},
	})
	if err != nil {
		return ended(fmt.Errorf("supervisor session start: %w", err))
	}
	defer stopSessionBestEffort(ctx, launch.adapter, session, launch.cfg, log)

	note := func(u domain.TokenUsage) {
		tokens := u.Uncached()
		if tokens <= run.tokens.Load() {
			return
		}
		run.tokens.Store(tokens)
		switch {
		case tokens >= launch.tokensLeft:
			run.cancel(errSupervisorBudget)
		case launch.sessionCap > 0 && tokens >= launch.sessionCap:
			run.cancel(errSupervisorSessionCap)
		}
	}
	turnCtx, cancelTurn := context.WithTimeout(ctx, time.Duration(effectiveTurnTimeoutMS(launch.cfg.Agent.TurnTimeoutMS))*time.Millisecond)
	defer cancelTurn()
	result, err := launch.adapter.RunTurn(turnCtx, session, domain.RunTurnParams{
		Prompt: text,
		Issue:  domain.Issue{ID: "supervisor", Identifier: "supervisor", Title: "Sortie supervisor"},
		OnEvent: func(ev domain.AgentEvent) {
			if ev.Usage != (domain.TokenUsage{}) {
				note(ev.Usage)
			}
		},
	})
	note(result.Usage)
	if err == nil && result.ExitReason != "" && result.ExitReason != domain.EventTurnCompleted {
		err = fmt.Errorf("supervisor turn ended: %s", result.ExitReason)
	}
	return ended(err)
}

// stopSupervisor stops the running supervisor session and waits for it.
// Must run on the event loop goroutine during shutdown.
func (o *Orchestrator) stopSupervisor() {
	if s := o.state.supervisor; s != nil && s.run != nil {
		s.run.cancel(context.Canceled)
	}
	o.supervisorWg.Wait()
}

// snapshotSupervisor returns the supervisor's state for the state API, or
// nil while no supervisor is configured.
func snapshotSupervisor(state *State, now time.Time) *SupervisorSnapshot {
	s := state.supervisor
	if s == nil || !s.enabled {
		return nil
	}
	sc := s.cfg
	snap := &SupervisorSnapshot{
		Agent:       sc.Agent,
		Unavailable: s.unavailable,
		Pending:     slices.Clone(s.pending),
		NextSweepAt: s.nextSweepAt,
		Budget: SupervisorBudget{
			Day: s.day, ResetsAt: s.day.Add(24 * time.Hour),
			SessionsUsed: s.sessionsUsed, SessionsLimit: sc.MaxSessionsPerDay,
			TokensUsed: s.tokensUsed, TokensLimit: int64(sc.MaxTokensPerDay),
			SessionTokensLimit: int64(sc.SessionTokens),
			HourTokensUsed:     s.hourSpend(now),
			Reserve:            inReserve(sc, s.tokensUsed),
			Velocity:           s.velocity,
		},
		Available: s.avail.reason == "",
		Reason:    s.avail.reason,
		ResetsAt:  s.avail.resetsAt,
		Since:     s.avail.since,
		Deferred:  slices.Clone(s.deferred),
	}
	snap.Budget.HourPace, snap.Budget.HourTokensLimit = hourlyPace(sc)
	if s.pause != nil {
		snap.Pause = new(*s.pause)
	}
	if s.lastTrigger != nil {
		snap.LastTrigger = new(*s.lastTrigger)
	}
	if s.last != nil {
		last := *s.last
		last.Triggers = slices.Clone(last.Triggers)
		snap.LastSession = &last
	}
	if s.run != nil {
		tokens := s.run.tokens.Load()
		snap.Running = &SupervisorSession{
			ID: s.run.rowID, StartedAt: s.run.startedAt, Triggers: slices.Clone(s.run.triggers), TriggerCount: len(s.run.triggers), Tokens: tokens,
			Digest: s.run.digest, Actions: persistence.SupervisorActionsUnknown,
		}
		snap.Budget.TokensUsed += tokens
		snap.Budget.HourTokensUsed += tokens - s.run.sampled
	}
	for _, kind := range slices.Sorted(maps.Keys(s.suspended)) {
		snap.Suspended = append(snap.Suspended, *s.suspended[kind])
	}
	switch {
	case s.pause != nil:
		snap.Status = SupervisorStatusPaused
	case s.run != nil:
		snap.Status = SupervisorStatusRunning
	case s.unavailable != "":
		snap.Status = SupervisorStatusUnavailable
	case s.budgetStop(sc) != "":
		snap.Status = SupervisorStatusBudgetExhausted
	default:
		snap.Status = SupervisorStatusIdle
	}
	return snap
}

// SupervisorPauseStatus is what a request to pause or resume the
// supervisor did.
type SupervisorPauseStatus int

const (
	SupervisorPauseDone          SupervisorPauseStatus = iota + 1 // the loop took the request
	SupervisorPauseNotConfigured                                  // the workflow configures no supervisor; nothing changed
	SupervisorPauseDraining                                       // shutdown began before the loop took the request; nothing changed
	SupervisorPauseWithdrawn                                      // the caller's context ended before the loop took the request; nothing changed
)

// SupervisorPauseResult is the answer to a request to pause or resume
// the supervisor. Pause is the pause in force after the request, nil
// while the supervisor runs.
type SupervisorPauseResult struct {
	Status SupervisorPauseStatus
	Pause  *SupervisorPause
}

// SupervisorPauseFunc returns a function that pauses the supervisor when
// pause is true and resumes it otherwise. A pause stops the running
// session and starts no new one; a second pause keeps the first. Resume
// starts a session at once for the triggers that came in while paused.
// Safe for concurrent use.
func (o *Orchestrator) SupervisorPauseFunc() func(ctx context.Context, pause bool, by, reason string) SupervisorPauseResult {
	return func(ctx context.Context, pause bool, by, reason string) SupervisorPauseResult {
		var result SupervisorPauseResult
		switch o.runOnLoop(ctx, func(loopCtx context.Context) {
			s := o.state.supervisor
			if s == nil || !o.workflowManager.Config().Supervisor.Enabled() {
				result.Status = SupervisorPauseNotConfigured
				return
			}
			if pause {
				o.pauseSupervisorOnLoop(by, reason)
			} else {
				o.resumeSupervisorOnLoop(loopCtx)
			}
			result.Status = SupervisorPauseDone
			if s.pause != nil {
				result.Pause = new(*s.pause)
			}
		}) {
		case loopRan:
			return result
		case loopDraining:
			return SupervisorPauseResult{Status: SupervisorPauseDraining}
		default:
			return SupervisorPauseResult{Status: SupervisorPauseWithdrawn}
		}
	}
}

func (o *Orchestrator) pauseSupervisorOnLoop(by, reason string) {
	s := o.state.supervisor
	if s.pause != nil {
		return
	}
	s.pause = &SupervisorPause{Since: time.Now().UTC(), By: by, Reason: reason}
	if s.run != nil {
		s.run.cancel(errSupervisorPaused)
	}
	o.logger.Info("supervisor paused", slog.String("by", by), slog.String("reason", reason), slog.Bool("session_stopped", s.run != nil))
	o.notifyObservers()
}

func (o *Orchestrator) resumeSupervisorOnLoop(ctx context.Context) {
	s := o.state.supervisor
	if s.pause == nil {
		return
	}
	paused := s.pause
	s.pause = nil
	o.logger.Info("supervisor resumed", slog.String("paused_by", paused.By), slog.Duration("paused_for", time.Since(paused.Since)),
		slog.Int("triggers", len(s.pending)))
	o.notifyObservers()
	o.superviseOnLoop(ctx, time.Now())
}
