package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/gitsync"
	"github.com/sortie-ai/sortie/internal/jobtoken"
	"github.com/sortie-ai/sortie/internal/logging"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/prompt"
	"github.com/sortie-ai/sortie/internal/redact"
	"github.com/sortie-ai/sortie/internal/worker"
)

// remoteRunStore keeps the record of each running remote attempt, so a
// restarted process re-adopts the run instead of losing it.
type remoteRunStore interface {
	SaveRemoteRun(ctx context.Context, r persistence.RemoteRun) error
	UpdateRemoteRunEntry(ctx context.Context, issueID, entry string) error
	UpdateRemoteRunSession(ctx context.Context, issueID, sessionID string, turnsCompleted int, ended bool) error
	DeleteRemoteRun(ctx context.Context, issueID string) error
	LoadRemoteRuns(ctx context.Context) ([]persistence.RemoteRun, error)
}

// remoteRunRecord is what a restarted process needs of a remote attempt's
// job: the job and its run descriptor, the digests of the tokens the
// worker holds, and the seed the returned branch is checked against.
type remoteRunRecord struct {
	Job      worker.Job               `json:"job"`
	Flows    map[string]string        `json:"flows"`
	Run      worker.Run               `json:"run"`
	Tokens   map[jobtoken.Flow]string `json:"tokens"`
	Seed     *gitsync.Seed            `json:"seed,omitempty"`
	SeedPath string                   `json:"seed_path,omitempty"`
	Dir      string                   `json:"dir"`
}

// remoteRunEntry is a remote run as the orchestrator tracks it: its issue,
// dispatch, attempt, stage and budgets so far, and its broker.
type remoteRunEntry struct {
	Issue               domain.Issue                 `json:"issue"`
	DispatchLabels      []string                     `json:"dispatch_labels,omitempty"`
	DispatchID          string                       `json:"dispatch_id"`
	RetryAttempt        *int                         `json:"retry_attempt,omitempty"`
	StartedAt           time.Time                    `json:"started_at"`
	WorkflowFile        string                       `json:"workflow_file,omitempty"`
	AgentKind           string                       `json:"agent_kind"`
	RuleName            string                       `json:"rule_name,omitempty"`
	TemplateID          string                       `json:"template_id,omitempty"`
	StageLineage        StageLineage                 `json:"stage_lineage"`
	FirstAfterHop       bool                         `json:"first_after_hop,omitempty"`
	RuleSettingsApplied bool                         `json:"rule_settings_applied,omitempty"`
	ConfiguredModel     string                       `json:"configured_model,omitempty"`
	ConfiguredEffort    string                       `json:"configured_effort,omitempty"`
	ContinuationContext map[string]any               `json:"continuation_context,omitempty"`
	ReactionKind        string                       `json:"reaction_kind,omitempty"`
	Trigger             string                       `json:"trigger,omitempty"`
	UsageLimitNotified  bool                         `json:"usage_limit_notified,omitempty"`
	Continuation        ContinuationTarget           `json:"continuation"`
	SessionID           string                       `json:"session_id,omitempty"`
	BrokerURL           string                       `json:"broker_url,omitempty"`
	BrokerViewerToken   string                       `json:"broker_viewer_token,omitempty"`
	HeldTotal           time.Duration                `json:"held_total,omitempty"`
	TurnCount           int                          `json:"turn_count"`
	InputTokens         int64                        `json:"input_tokens"`
	OutputTokens        int64                        `json:"output_tokens"`
	TotalTokens         int64                        `json:"total_tokens"`
	CacheReadTokens     int64                        `json:"cache_read_tokens"`
	CacheWriteTokens    int64                        `json:"cache_write_tokens"`
	UsageByModel        map[string]domain.TokenUsage `json:"usage_by_model,omitempty"`
	ModelName           string                       `json:"model_name,omitempty"`
	APIRequestCount     int                          `json:"api_request_count"`
	RequestsByModel     map[string]int               `json:"requests_by_model,omitempty"`
	UnaccountedTurns    int                          `json:"unaccounted_turns"`
	ToolTimeMs          int64                        `json:"tool_time_ms"`
	APITimeMs           int64                        `json:"api_time_ms"`
}

// remoteAdoption is a run a previous process left with a worker, restored
// at start and adopted on the first pass of the event loop.
type remoteAdoption struct {
	run    persistence.RemoteRun
	record remoteRunRecord
	entry  remoteRunEntry
	ended  <-chan worker.Outcome
}

// job returns the adopted attempt's matcher job.
func (a *remoteAdoption) job() worker.Job {
	j := a.record.Job
	j.Flows = maps.Clone(a.record.Flows)
	j.Run = a.record.Run
	return j
}

// adoptedRun tells the worker how far the adopted run got.
func (a *remoteAdoption) adoptedRun() *AdoptedRun {
	return &AdoptedRun{TurnsCompleted: a.run.TurnsCompleted, SessionEnded: a.run.SessionEnded}
}

type remoteAdoptionCtxKey struct{}

func withRemoteAdoption(ctx context.Context, a *remoteAdoption) context.Context {
	return context.WithValue(ctx, remoteAdoptionCtxKey{}, a)
}

func remoteAdoptionFrom(ctx context.Context) *remoteAdoption {
	a, _ := ctx.Value(remoteAdoptionCtxKey{}).(*remoteAdoption)
	return a
}

// snapshotRemoteEntry returns the parts of entry a restart restores.
func snapshotRemoteEntry(entry *RunningEntry) remoteRunEntry {
	return remoteRunEntry{
		Issue: entry.Issue, DispatchLabels: slices.Clone(entry.DispatchLabels), DispatchID: entry.DispatchID,
		RetryAttempt: entry.RetryAttempt, StartedAt: entry.StartedAt, WorkflowFile: entry.WorkflowFile,
		AgentKind: entry.AgentKind, RuleName: entry.RuleName, TemplateID: entry.TemplateID,
		StageLineage: entry.StageLineage, FirstAfterHop: entry.FirstAfterHop, RuleSettingsApplied: entry.RuleSettingsApplied,
		ConfiguredModel: entry.ConfiguredModel, ConfiguredEffort: entry.ConfiguredEffort,
		ContinuationContext: storableContinuation(entry.ContinuationContext), ReactionKind: entry.ReactionKind, Trigger: entry.Trigger,
		UsageLimitNotified: entry.UsageLimitNotified, Continuation: entry.Continuation, SessionID: entry.SessionID,
		BrokerURL: entry.BrokerURL, BrokerViewerToken: entry.BrokerViewerToken, HeldTotal: entry.HeldTotal,
		TurnCount: entry.TurnCount, InputTokens: entry.AgentInputTokens, OutputTokens: entry.AgentOutputTokens,
		TotalTokens: entry.AgentTotalTokens, CacheReadTokens: entry.CacheReadTokens, CacheWriteTokens: entry.CacheWriteTokens,
		UsageByModel: maps.Clone(entry.UsageByModel), ModelName: entry.ModelName, APIRequestCount: entry.APIRequestCount,
		RequestsByModel: maps.Clone(entry.RequestsByModel), UnaccountedTurns: entry.UnaccountedTurns,
		ToolTimeMs: entry.ToolTimeMs, APITimeMs: entry.APITimeMs,
	}
}

// restoreInto gives entry what the run had spent and recorded. The
// watermarks of the agent's cumulative usage start again at zero: the
// adopted session is a new agent process, which counts from zero, so its
// spend adds to what the run had spent.
func (s remoteRunEntry) restoreInto(entry *RunningEntry) {
	entry.DispatchLabels = slices.Clone(s.DispatchLabels)
	entry.WorkflowFile = s.WorkflowFile
	entry.AgentKind, entry.RuleName, entry.TemplateID = s.AgentKind, s.RuleName, s.TemplateID
	entry.StageLineage, entry.FirstAfterHop = s.StageLineage, s.FirstAfterHop
	entry.RuleSettingsApplied, entry.ConfiguredModel, entry.ConfiguredEffort = s.RuleSettingsApplied, s.ConfiguredModel, s.ConfiguredEffort
	entry.ContinuationContext = s.ContinuationContext
	entry.ReactionKind, entry.Trigger, entry.UsageLimitNotified = s.ReactionKind, s.Trigger, s.UsageLimitNotified
	entry.Continuation = s.Continuation
	entry.SessionID = s.SessionID
	if s.BrokerURL != "" && s.BrokerViewerToken != "" {
		redact.Add("broker.viewer_token", s.BrokerViewerToken)
		entry.BrokerURL, entry.BrokerViewerToken = s.BrokerURL, s.BrokerViewerToken
	}
	entry.HeldTotal = s.HeldTotal
	entry.TurnCount = s.TurnCount
	entry.AgentInputTokens, entry.AgentOutputTokens, entry.AgentTotalTokens = s.InputTokens, s.OutputTokens, s.TotalTokens
	entry.CacheReadTokens, entry.CacheWriteTokens = s.CacheReadTokens, s.CacheWriteTokens
	entry.UsageByModel = maps.Clone(s.UsageByModel)
	entry.ModelName, entry.APIRequestCount = s.ModelName, s.APIRequestCount
	entry.RequestsByModel = maps.Clone(s.RequestsByModel)
	entry.UnaccountedTurns = s.UnaccountedTurns
	entry.ToolTimeMs, entry.APITimeMs = s.ToolTimeMs, s.APITimeMs
}

// storableContinuation returns a copy of a continuation context fit for
// the database: a CI log excerpt, which may hold a secret a build
// printed, is never stored, so a rebuilt CI fix turn has an empty one.
func storableContinuation(v map[string]any) map[string]any {
	if v == nil {
		return nil
	}
	out := make(map[string]any, len(v))
	for k, x := range v {
		switch t := x.(type) {
		case map[string]any:
			out[k] = storableContinuation(t)
		case string:
			if k == "log_excerpt" {
				t = ""
			}
			out[k] = t
		default:
			out[k] = x
		}
	}
	return out
}

// decodeRemoteRunEntry reads a stored entry. Its continuation context
// comes back with the shapes reaction templates and the reconcile passes
// read: lists of objects as []map[string]any and whole numbers as int.
func decodeRemoteRunEntry(raw string) (remoteRunEntry, error) {
	var e remoteRunEntry
	if raw == "" {
		return e, errors.New("the run's entry was never recorded")
	}
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		return e, err
	}
	if e.DispatchID == "" || e.Issue.ID == "" {
		return e, errors.New("the run's entry names no dispatch or issue")
	}
	if e.ContinuationContext != nil {
		e.ContinuationContext, _ = templateValue(e.ContinuationContext).(map[string]any)
	}
	return e, nil
}

func templateValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, x := range t {
			t[k] = templateValue(x)
		}
		return t
	case []any:
		maps := make([]map[string]any, 0, len(t))
		strs := make([]string, 0, len(t))
		for i, x := range t {
			t[i] = templateValue(x)
			if m, ok := t[i].(map[string]any); ok {
				maps = append(maps, m)
			}
			if s, ok := t[i].(string); ok {
				strs = append(strs, s)
			}
		}
		switch {
		case len(t) > 0 && len(maps) == len(t):
			return maps
		case len(t) > 0 && len(strs) == len(t):
			return strs
		}
		return t
	case float64:
		if t == math.Trunc(t) && math.Abs(t) < 1<<53 {
			return int(t)
		}
	}
	return v
}

// recordRemoteEntry stores the running entry of issueID in its remote
// run's record. It runs on the event loop.
func (o *Orchestrator) recordRemoteEntry(ctx context.Context, issueID string) {
	if o.remote == nil || o.remote.runs == nil {
		return
	}
	entry := o.state.Running[issueID]
	if entry == nil {
		return
	}
	raw, err := json.Marshal(snapshotRemoteEntry(entry))
	if err == nil {
		err = o.remote.runs.UpdateRemoteRunEntry(ctx, issueID, string(raw))
	}
	if err != nil {
		logging.WithIssue(o.logger, issueID, entry.Identifier).Warn("remote run entry not recorded; a restart now would not re-adopt the run", slog.Any("error", err))
	}
}

// recordRemoteEntrySoon asks the event loop to store the running entry
// of issueID; called from a worker goroutine, it never blocks it.
func (o *Orchestrator) recordRemoteEntrySoon(issueID string) {
	go o.runOnLoop(context.Background(), func(ctx context.Context) { o.recordRemoteEntry(ctx, issueID) })
}

// RestoreRemoteRuns restores, under the remote kind, the lease and job
// tokens of every run a previous process recorded, so the worker that
// holds it keeps it. It must run before the worker API is served: a
// heartbeat answered 409 makes the worker stop the run's machine. A run
// whose entry was never recorded is dropped, and its worker's lease
// lapses as any lost lease does. The runs are adopted when [Orchestrator.Run]
// starts.
func (o *Orchestrator) RestoreRemoteRuns(ctx context.Context) {
	cfg := o.workflowManager.Config()
	if o.remote == nil || o.remote.runs == nil || !isRemoteKind(cfg.Environment) {
		return
	}
	o.remote.applyConfig(cfg.Environment)
	runs, err := o.remote.runs.LoadRemoteRuns(ctx)
	if err != nil {
		o.logger.Error("remote runs not loaded; their workers' leases lapse", slog.Any("error", err))
		return
	}
	for _, r := range runs {
		log := o.logger.With(slog.String("issue_id", r.IssueID), slog.String("job", r.JobID), slog.Int("attempt", r.Attempt))
		_, err := o.remote.restore(r)
		if err != nil {
			log.Warn("remote run not re-adopted; its lease lapses and the issue is dispatched again", slog.Any("error", err))
			if derr := o.remote.runs.DeleteRemoteRun(ctx, r.IssueID); derr != nil {
				log.Warn("remote run record not deleted", slog.Any("error", derr))
			}
			continue
		}
		o.state.Claimed[r.IssueID] = struct{}{}
		log.Info("remote run restored", slog.String("worker", r.WorkerID), slog.Int("turns_completed", r.TurnsCompleted),
			slog.Bool("session_ended", r.SessionEnded))
	}
}

// restore decodes r and restores its job tokens and lease.
func (r *remoteWorkers) restore(run persistence.RemoteRun) (*remoteAdoption, error) {
	a := &remoteAdoption{run: run}
	if err := json.Unmarshal([]byte(run.Record), &a.record); err != nil {
		return nil, fmt.Errorf("the run's record: %w", err)
	}
	entry, err := decodeRemoteRunEntry(run.Entry)
	if err != nil {
		return nil, err
	}
	a.entry = entry
	if err := r.tokens.Restore(run.JobID, uint64(max(run.Attempt, 0)), a.record.Tokens); err != nil {
		return nil, err
	}
	ended, err := r.matcher.Adopt(a.job(), run.WorkerID)
	if err != nil {
		r.tokens.OnLeaseEnd(run.JobID)
		return nil, err
	}
	a.ended = ended
	r.mu.Lock()
	r.adoptions = append(r.adoptions, a)
	r.mu.Unlock()
	return a, nil
}

// takeAdoptions returns the restored runs not adopted yet.
func (r *remoteWorkers) takeAdoptions() []*remoteAdoption {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.adoptions
	r.adoptions = nil
	return out
}

// adoptRemoteRuns starts a worker for every restored run, as the run it
// was: the same dispatch, attempt, stage and budgets. Nothing is
// dispatched again and no session is counted: the adopted worker resumes
// the agent's session on the job's machine and continues the run from
// the turn it was in.
func (o *Orchestrator) adoptRemoteRuns(ctx context.Context) {
	if o.remote == nil {
		return
	}
	o.remote.started(ctx)
	cfg := o.workflowManager.Config()
	for _, a := range o.remote.takeAdoptions() {
		e := a.entry
		log := logging.WithIssue(o.logger, e.Issue.ID, e.Issue.Identifier)
		adapter, err := o.agentAdapterByKind(e.AgentKind)
		if err != nil {
			log.Error("remote run not adopted: its agent kind is unavailable; the issue is dispatched again", slog.String("agent_kind", e.AgentKind), slog.Any("error", err))
			o.remote.abandon(ctx, a)
			delete(o.state.Claimed, e.Issue.ID)
			continue
		}
		settings := o.resolveAttemptSettings(cfg, DispatchResolution{AgentKind: e.AgentKind, TemplateID: e.TemplateID, RuleName: e.RuleName}, "")
		runCtx := ctx
		if e.ContinuationContext != nil {
			runCtx = WithContinuationContext(runCtx, e.ContinuationContext)
		}
		if e.UsageLimitNotified {
			runCtx = withUsageLimitNotified(runCtx)
		}
		runCtx = withStageRender(runCtx, prompt.StageContext{
			Current:         stageCurrent(cfg.Dispatch, e.RuleName),
			Previous:        e.StageLineage.PreviousRule,
			PreviousOutcome: e.StageLineage.PreviousOutcome,
		})
		runCtx = withSessionIdentity(runCtx, sessionIdentity{RuleName: e.RuleName, ChainID: e.StageLineage.ChainID})
		runCtx = withRemoteAdoption(runCtx, a)
		adoptIssue(runCtx, o.state, a, o.makeWorkerFn(a.run.SessionID, "", e.AgentKind, e.TemplateID, e.ReactionKind, adapter, settings))
		if entry := o.state.Running[e.Issue.ID]; entry != nil {
			entry.UsageArrival, entry.UsageAttribution = settings.UsageArrival, settings.UsageAttribution
			freezeIssueTokenBaseline(ctx, o.state, e.Issue.ID, o.store, o.logger)
		}
		log.Info("remote run adopted", slog.String("dispatch_id", e.DispatchID), slog.String("worker", a.run.WorkerID),
			slog.Int("turns_completed", a.run.TurnsCompleted), slog.Bool("session_ended", a.run.SessionEnded))
	}
	o.remote.matcher.Recovered()
}

// abandon withdraws a restored run that cannot be adopted: its worker's
// next heartbeat is refused and it stops the machine.
func (r *remoteWorkers) abandon(ctx context.Context, a *remoteAdoption) {
	r.matcher.Cancel(a.run.JobID)
	if r.runs != nil {
		_ = r.runs.DeleteRemoteRun(ctx, a.run.IssueID) //nolint:errcheck // a leftover row is dropped at the next start
	}
}

// adoptIssue claims the issue, puts the adopted run back in the running
// map as it was, and starts its worker. It differs from [DispatchIssue]
// only in what it restores: the run keeps its dispatch ID, start and
// recorded fields.
func adoptIssue(ctx context.Context, state *State, a *remoteAdoption, workerFn WorkerFunc) {
	e := a.entry
	runCtx, cancelRunCause := context.WithCancelCause(ctx) //nolint:gosec // G118: cancelRunCause is stored in RunningEntry.CancelCauseFunc
	workerCtx, cancelWorkerCause := context.WithCancelCause(runCtx)
	workerCtx = withRunContext(workerCtx, runCtx)
	workerCtx = withDispatchID(workerCtx, e.DispatchID)

	sessionToken := mintSessionToken()
	state.SessionTokens.register(sessionToken, e.Issue.ID, e.Issue.Identifier)
	clock := newTurnClock()
	identity := sessionIdentityFrom(workerCtx)
	identity.Token = sessionToken
	identity.TurnClock = clock
	workerCtx = withSessionIdentity(workerCtx, identity)

	state.Claimed[e.Issue.ID] = struct{}{}
	entry := &RunningEntry{
		Identifier:        e.Issue.Identifier,
		Issue:             e.Issue,
		DispatchID:        e.DispatchID,
		SessionToken:      sessionToken,
		TurnClock:         clock,
		RetryAttempt:      e.RetryAttempt,
		StartedAt:         e.StartedAt,
		AdoptedAt:         time.Now(),
		CancelFunc:        func() { cancelRunCause(nil) },
		CancelCauseFunc:   cancelRunCause,
		CeilingCancelFunc: func() { cancelWorkerCause(errCeilingStop) },
		WorkerDone:        workerCtx.Done(),
		events:            state.RetainedEvents.register(e.DispatchID),
	}
	e.restoreInto(entry)
	if state.DescribeEnvironment != nil {
		entry.Environment = state.DescribeEnvironment(e.Issue.Identifier)
	}
	state.Running[e.Issue.ID] = entry
	CancelRetry(state, e.Issue.ID)

	var attempt *int
	if e.RetryAttempt != nil {
		attempt = new(*e.RetryAttempt)
	}
	issue := e.Issue
	state.WorkerWg.Go(func() {
		workerFn(workerCtx, issue, attempt)
	})
}

// detachRemoteRuns hands every running remote attempt whose job a
// worker holds to the next start, at shutdown: its entry is recorded and
// its data plane stops without telling the worker that the agent's input
// closed. The worker keeps the job, and the attempt's exit records
// nothing (see [Orchestrator.keepRemoteExit]).
func (o *Orchestrator) detachRemoteRuns(ctx context.Context) {
	if o.remote == nil || o.remote.runs == nil {
		return
	}
	for issueID, entry := range o.state.Running {
		if !o.remote.leased(issueID) {
			continue
		}
		o.recordRemoteEntry(ctx, issueID)
		if o.remote.detach(issueID) {
			logging.WithIssue(o.logger, issueID, entry.Identifier).Info("remote run left with its worker for the next start")
		}
	}
}

// keepRemoteExit takes the exit of a run left with its worker for the
// next start: its entry is recorded and it leaves the running map, with
// no run history, retry or handoff. It reports false for any other exit.
func (o *Orchestrator) keepRemoteExit(ctx context.Context, result WorkerResult) bool {
	if o.remote == nil || !o.remote.wasKept(result.IssueID) {
		return false
	}
	o.recordRemoteEntry(context.WithoutCancel(ctx), result.IssueID)
	delete(o.state.Running, result.IssueID)
	delete(o.state.Claimed, result.IssueID)
	return true
}

// AdoptedRun is how far a run a previous process left with a worker got.
type AdoptedRun struct {
	// TurnsCompleted counts the session's completed turns; the turn
	// after them, the one the restart interrupted if any, runs again.
	TurnsCompleted int
	// SessionEnded reports that the session was stopped: no turn runs,
	// and the run goes on with the worker's returned result.
	SessionEnded bool
}

// resume sets the worker's turn counters as the run left them and
// returns the turn to run next, and whether no turn is left to run.
func (a *AdoptedRun) resume(turnsCompleted, turnsStarted *int, maxTurns int) (int, bool) {
	*turnsCompleted, *turnsStarted = a.TurnsCompleted, a.TurnsCompleted
	next := a.TurnsCompleted + 1
	return next, a.SessionEnded || next > maxTurns
}

// adoptedPresentedComments returns the comment IDs the run's first turn
// presented to the agent, as that turn computed them.
func adoptedPresentedComments(reactionKind string, tmpl *prompt.Template, issueMap map[string]any, attempt, maxTurns int, continuation map[string]any, opts []prompt.RenderOption) map[string][]string {
	if reactionKind != "" {
		return carriedCommentIDs(continuation)
	}
	_, presented, err := renderFirstTurnPrompt(tmpl, issueMap, attempt, maxTurns, continuation, opts...)
	if err != nil {
		return nil
	}
	return presented
}
