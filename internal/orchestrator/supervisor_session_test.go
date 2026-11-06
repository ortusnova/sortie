package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/parkanswer"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/waittest"
)

const (
	testSupervisorKind  = "supervisor-agent"
	testSupervisorToken = "sup-token-1"
	testSupervisorAPI   = "http://127.0.0.1:7678"
)

// fakeSupervisorAgent is the supervisor's agent: it records each session
// and the prompt of its turn, and runs the turn the test gives it, which
// completes at once by default.
type fakeSupervisorAgent struct {
	mu      sync.Mutex
	starts  []domain.StartSessionParams
	prompts []string
	stops   int
	turn    func(ctx context.Context, prompt string, onEvent func(domain.AgentEvent)) (domain.TurnResult, error)
	turns   chan string
}

func newFakeSupervisorAgent() *fakeSupervisorAgent {
	return &fakeSupervisorAgent{turns: make(chan string, 16)}
}

func (a *fakeSupervisorAgent) StartSession(_ context.Context, params domain.StartSessionParams) (domain.Session, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.starts = append(a.starts, params)
	return domain.Session{ID: fmt.Sprintf("sup-%d", len(a.starts))}, nil
}

func (a *fakeSupervisorAgent) RunTurn(ctx context.Context, session domain.Session, params domain.RunTurnParams) (domain.TurnResult, error) {
	a.mu.Lock()
	a.prompts = append(a.prompts, params.Prompt)
	turn := a.turn
	a.mu.Unlock()
	a.turns <- params.Prompt
	if turn != nil {
		return turn(ctx, params.Prompt, params.OnEvent)
	}
	return domain.TurnResult{SessionID: session.ID, ExitReason: domain.EventTurnCompleted}, nil
}

func (a *fakeSupervisorAgent) StopSession(context.Context, domain.Session) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stops++
	return nil
}

func (a *fakeSupervisorAgent) setTurn(turn func(ctx context.Context, prompt string, onEvent func(domain.AgentEvent)) (domain.TurnResult, error)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.turn = turn
}

func (a *fakeSupervisorAgent) sessions() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.starts)
}

// blockingTurn returns a turn that runs until release closes or the
// session is stopped.
func blockingTurn(release <-chan struct{}) func(context.Context, string, func(domain.AgentEvent)) (domain.TurnResult, error) {
	return func(ctx context.Context, _ string, _ func(domain.AgentEvent)) (domain.TurnResult, error) {
		select {
		case <-release:
			return domain.TurnResult{ExitReason: domain.EventTurnCompleted}, nil
		case <-ctx.Done():
			return domain.TurnResult{ExitReason: domain.EventTurnCancelled}, ctx.Err()
		}
	}
}

// supTracker serves the candidates the test sets and records comments.
type supTracker struct {
	*mockTrackerAdapter
	mu         sync.Mutex
	candidates []domain.Issue
	done       map[string]bool
	blockers   map[string][]string // issue ID to the IDs of the issues that block it
	polls      chan struct{}
}

func (s *supTracker) FetchCandidateIssues(context.Context) ([]domain.Issue, error) {
	defer func() {
		select {
		case s.polls <- struct{}{}:
		default:
		}
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.Issue, 0, len(s.candidates))
	for _, c := range s.candidates {
		if s.done[c.ID] {
			continue
		}
		c.BlockedBy = nil
		for _, id := range s.blockers[c.ID] {
			state := "To Do"
			if s.done[id] {
				state = "Done"
			}
			c.BlockedBy = append(c.BlockedBy, domain.BlockerRef{ID: id, Identifier: strings.ToUpper(id), State: state})
		}
		out = append(out, c)
	}
	return out, nil
}

func (s *supTracker) setCandidates(issues ...domain.Issue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.candidates = issues
}

func (s *supTracker) finish(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.done[id] = true
}

func (s *supTracker) stateOf(id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done[id] {
		return "Done"
	}
	return "To Do"
}

// supHarness is an orchestrator with a supervisor section, a story agent,
// a fake supervisor agent, a migrated store, and a fake forge.
type supHarness struct {
	t       *testing.T
	o       *Orchestrator
	cfg     config.ServiceConfig
	store   *persistence.Store
	sup     *fakeSupervisorAgent
	story   domain.AgentAdapter
	tracker *supTracker
	forge   *supervisorForge
	logs    *lockedBuf
	params  func(p *OrchestratorParams)
	cancel  context.CancelFunc
	done    chan struct{}
}

func newSupHarness(t *testing.T, mutate func(cfg *config.ServiceConfig)) *supHarness {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "supervisor.md"), []byte("You supervise. Started by {{ .trigger.kind }}."), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := defaultWorkerConfig(filepath.Join(dir, "workspaces"))
	cfg.Tracker.Kind = "mock"
	cfg.Polling = config.PollingConfig{IntervalMS: int(time.Hour / time.Millisecond)}
	cfg.Agent.MaxConcurrentAgents = 2
	cfg.Supervisor = config.SupervisorConfig{
		Agent: testSupervisorKind, Prompt: "supervisor.md",
		SweepIntervalMS: int(time.Hour / time.Millisecond), MaxSessionsPerDay: 10, MaxTokensPerDay: 1_000_000,
		QuietSweepMS: int(2 * time.Hour / time.Millisecond), NoopLimit: 3,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	h := &supHarness{
		t: t, cfg: cfg, store: openRepeatedFailureStore(t), sup: newFakeSupervisorAgent(), forge: newSupervisorForge(),
		story: &mockAgentAdapter{}, logs: &lockedBuf{},
	}
	h.tracker = &supTracker{done: map[string]bool{}, blockers: map[string][]string{}, polls: make(chan struct{}, 1)}
	h.tracker.mockTrackerAdapter = &mockTrackerAdapter{
		fetchIssueByIDFn: func(_ context.Context, id string) (domain.Issue, error) {
			return domain.Issue{ID: id, Identifier: strings.ToUpper(id), State: h.tracker.stateOf(id)}, nil
		},
		fetchStatesFn: func(_ context.Context, ids []string) (map[string]string, error) {
			out := make(map[string]string, len(ids))
			for _, id := range ids {
				out[id] = h.tracker.stateOf(id)
			}
			return out, nil
		},
	}
	return h
}

func (h *supHarness) run() {
	h.t.Helper()
	pf := passingPreflightRegistries()
	pf.ReloadWorkflow = func() error { return nil }
	pf.ConfigFunc = func() config.ServiceConfig { return h.cfg }
	wm := &stubWorkflowManager{config: h.cfg, template: mustParseTemplate(h.t, "do {{ .issue.identifier }}"), absPath: filepath.Join(filepath.Dir(h.cfg.Workspace.Root), "WORKFLOW.md")}
	state := NewState(h.cfg.Polling.IntervalMS, h.cfg.Agent.MaxConcurrentAgents, 0, nil, AgentTotals{})
	params := OrchestratorParams{
		State:          state,
		Logger:         slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		TrackerAdapter: h.tracker,
		AgentAdapterByKind: func(kind string) (domain.AgentAdapter, error) {
			switch kind {
			case h.cfg.Agent.Kind:
				return h.story, nil
			case testSupervisorKind:
				return h.sup, nil
			}
			return nil, fmt.Errorf("agent kind %q is not available", kind)
		},
		WorkflowManager:  wm,
		Store:            h.store,
		PreflightParams:  pf,
		SCMAdapter:       h.forge,
		ReviewConfigured: true,
		SessionAPIURL:    testSupervisorAPI,
		SupervisorToken:  testSupervisorToken,
		SupervisorDir:    filepath.Join(h.t.TempDir(), "supervisor"),
	}
	if h.params != nil {
		h.params(&params)
	}
	h.o = NewOrchestrator(params)
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel, h.done = cancel, make(chan struct{})
	go func() {
		h.o.Run(ctx)
		close(h.done)
	}()
	h.t.Cleanup(h.stop)
	waittest.Receive(h.t, h.tracker.polls, "the first poll")
}

func (h *supHarness) stop() {
	if h.cancel != nil {
		h.cancel()
		<-h.done
		h.cancel = nil
	}
}

// onLoop runs apply on the event loop; the supervisor reacts to what it
// did once the action returns.
func (h *supHarness) onLoop(apply func(ctx context.Context)) {
	h.t.Helper()
	if h.o.runOnLoop(h.t.Context(), apply) != loopRan {
		h.t.Fatal("the loop did not run the action")
	}
}

func (h *supHarness) park(issueID, identifier string) {
	h.t.Helper()
	h.onLoop(func(ctx context.Context) {
		parkIssue(h.o.state, parkIssueParams{
			IssueID: issueID, Identifier: identifier, Reason: parkReasonAgentBlocked, Store: h.o.store, Metrics: h.o.metrics,
			Logger: discardLogger(), Ctx: ctx,
			Run: parkedRun{HandOver: parkHandOver{Statement: "Please push the fix commit to the branch."}},
		})
	})
}

func (h *supHarness) snapshot() *SupervisorSnapshot {
	h.t.Helper()
	snap, err := h.o.SnapshotFunc()()
	if err != nil {
		h.t.Fatalf("snapshot: %v", err)
	}
	return snap.Supervisor
}

func (h *supHarness) refresh() {
	h.t.Helper()
	h.o.RefreshFunc()()
	waittest.Receive(h.t, h.tracker.polls, "a poll")
}

// waitEnded waits until the one supervisor session has ended.
func (h *supHarness) waitEnded() *SupervisorSnapshot {
	h.t.Helper()
	var snap *SupervisorSnapshot
	waittest.Until(h.t, func() error {
		snap = h.snapshot()
		if snap == nil || snap.Running != nil || snap.LastSession == nil || h.sup.sessions() < 1 {
			return fmt.Errorf("supervisor = %+v with %d sessions, want one ended", snap, h.sup.sessions())
		}
		return nil
	})
	return snap
}

func triggerKinds(triggers []SupervisorTrigger) []string {
	kinds := make([]string, 0, len(triggers))
	for _, t := range triggers {
		kinds = append(kinds, t.Kind+" "+t.Identifier)
	}
	return kinds
}

// A park starts a supervisor session whose prompt carries the supervisor
// token, the trigger, and the state digest, in the supervisor's own
// directory with the token in its environment.
func TestSupervisor_StartsOnAPark(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, nil)
	h.run()

	h.park("id-7", "ONS-7")
	prompt := waittest.Receive(t, h.sup.turns, "the supervisor's turn")
	for _, want := range []string{
		"You supervise. Started by park.",
		"Supervisor token (send as `Authorization: Bearer <token>`): " + testSupervisorToken,
		"Sortie API: " + testSupervisorAPI,
		"- park ONS-7: parked: agent_blocked",
		`"parked": [`,
		`"issue_identifier": "ONS-7"`,
		`"statement": "Please push the fix commit to the branch."`,
	} {
		if !strings.Contains(prompt, want) {
			t.Errorf("prompt lacks %q:\n%s", want, prompt)
		}
	}
	snap := h.waitEnded()
	if snap.LastTrigger == nil || snap.LastTrigger.Kind != SupervisorTriggerPark || snap.LastTrigger.Identifier != "ONS-7" {
		t.Errorf("last trigger = %+v, want the park of ONS-7", snap.LastTrigger)
	}
	if snap.LastSession.Outcome != SupervisorOutcomeCompleted || !slices.Equal(triggerKinds(snap.LastSession.Triggers), []string{"park ONS-7"}) {
		t.Errorf("last session = %+v, want the completed park session", snap.LastSession)
	}
	if snap.Status != SupervisorStatusIdle || snap.Budget.SessionsUsed != 1 || snap.Budget.SessionsLimit != 10 {
		t.Errorf("status %s budget %+v, want idle with one session used", snap.Status, snap.Budget)
	}

	h.sup.mu.Lock()
	params := h.sup.starts[0]
	h.sup.mu.Unlock()
	if params.WorkspacePath != h.o.supervisorDir() || params.AgentConfig.Kind != testSupervisorKind {
		t.Errorf("session = %q kind %q, want the supervisor directory and kind", params.WorkspacePath, params.AgentConfig.Kind)
	}
	env := map[string]string{}
	for _, v := range params.SessionEnv {
		env[v.Name] = v.Value
	}
	if env[envSupervisorToken] != testSupervisorToken || env[envSessionAPIURL] != testSupervisorAPI || env[envSessionPurpose] != sessionPurposeSupervise {
		t.Errorf("session env = %v, want the supervisor token, the API URL and the supervisor purpose", env)
	}
	latest, ok, err := h.store.LatestSupervisorSession(t.Context())
	if err != nil || !ok || latest.TriggerKind != SupervisorTriggerPark || latest.TriggerIssue != "ONS-7" || latest.Outcome != SupervisorOutcomeCompleted {
		t.Errorf("stored session = %+v, %v, %v, want the completed park session", latest, ok, err)
	}
}

// A stopped failure whose signature is in no class starts a session as an
// unclassified failure; one in a class is only the failure of a run.
func TestSupervisor_StartsOnAnUnclassifiedStoppedFailure(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, nil)
	h.run()

	var pending []SupervisorTrigger
	h.onLoop(func(context.Context) {
		failRun(t, h.o.state, h.store, "id-49", "ONS-49", nil, refusedPushOf("ONS-49"))
		failRun(t, h.o.state, h.store, "id-49", "ONS-49", new(1), refusedPushOf("ONS-49"))
		odd := errors.New("run: the disk sang")
		failRun(t, h.o.state, h.store, "id-8", "ONS-8", nil, odd)
		failRun(t, h.o.state, h.store, "id-8", "ONS-8", new(1), odd)
		pending = slices.Clone(h.o.state.supervisor.pending)
	})
	want := []string{"failure ONS-49", "failure ONS-8", "unclassified_failure ONS-8"}
	if got := triggerKinds(pending); !slices.Equal(got, want) {
		t.Fatalf("triggers = %v, want %v: a classified platform fault is no unclassified failure", got, want)
	}
	prompt := waittest.Receive(t, h.sup.turns, "the supervisor's turn")
	if !strings.Contains(prompt, "- unclassified_failure ONS-8: platform fault pf-") {
		t.Errorf("prompt = %s, want the unclassified failure of ONS-8", prompt)
	}
	if !strings.Contains(prompt, `"class": "unclassified"`) {
		t.Errorf("prompt's digest lacks the unclassified fault:\n%s", prompt)
	}
}

// A pull request reaction without progress for an hour starts a session
// once per progress mark.
func TestSupervisor_StartsOnAStalledReaction(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, nil)
	h.run()

	var pending []SupervisorTrigger
	h.onLoop(func(context.Context) {
		fresh := newReviewPendingEntry("id-31", 31)
		fresh.CreatedAt = time.Now().Add(-59 * time.Minute)
		h.o.state.PendingReactions[ReactionKey("id-31", ReactionKindReview)] = fresh
		h.o.state.supervisor.pause = &SupervisorPause{Since: time.Now(), By: "test"}
		noteStalledReactions(h.o.state, time.Now())
		pending = slices.Clone(h.o.state.supervisor.pending)
	})
	if len(pending) != 0 {
		t.Fatalf("a reaction 59 minutes old kept triggers %v, want none", triggerKinds(pending))
	}
	h.onLoop(func(context.Context) {
		stalled := newReviewPendingEntry("id-41", 41)
		stalled.CreatedAt = time.Now().Add(-2 * time.Hour)
		h.o.state.PendingReactions[ReactionKey("id-41", ReactionKindReview)] = stalled
		noteStalledReactions(h.o.state, time.Now())
		noteStalledReactions(h.o.state, time.Now())
		pending = slices.Clone(h.o.state.supervisor.pending)
		h.o.state.supervisor.pause = nil
	})
	if got := triggerKinds(pending); !slices.Equal(got, []string{"stalled_reaction id-41-ident"}) {
		t.Fatalf("triggers = %v, want one stalled reaction of id-41", got)
	}
	if !strings.Contains(pending[0].Detail, "owner/repo#41") {
		t.Errorf("detail = %q, want the pull request", pending[0].Detail)
	}
	h.onLoop(func(context.Context) {})
	prompt := waittest.Receive(t, h.sup.turns, "the supervisor's turn")
	if !strings.Contains(prompt, "Started by stalled_reaction.") || !strings.Contains(prompt, `"pull_request": "owner/repo#41"`) {
		t.Errorf("prompt = %s, want the stalled reaction and its pull request", prompt)
	}
}

// A poll that leaves a slot free and dispatches nothing while stories are
// held starts a session; the next poll on the same state digest starts none.
func TestSupervisor_StartsOnAFreeSlotWithStoriesHeld(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, nil)
	h.run()

	h.tracker.setCandidates(domain.Issue{ID: "id-5", Identifier: "ONS-5", Title: "five", State: "To Do"})
	h.onLoop(func(context.Context) {
		h.o.state.FaultHolds["id-5"] = "pf-held"
	})
	h.refresh()
	prompt := waittest.Receive(t, h.sup.turns, "the supervisor's turn")
	if !strings.Contains(prompt, "Started by idle_slot.") || !strings.Contains(prompt, "1 stories are held") {
		t.Errorf("prompt = %s, want the free slot with one held story", prompt)
	}
	h.waitEnded()

	// The same held stories on the next poll start nothing new.
	h.refresh()
	var pending []SupervisorTrigger
	h.onLoop(func(context.Context) { pending = slices.Clone(h.o.state.supervisor.pending) })
	if len(pending) != 0 || h.sup.sessions() != 1 {
		t.Errorf("second poll kept %v and started %d sessions, want nothing new", triggerKinds(pending), h.sup.sessions())
	}
}

// A live lease is the holder's work, so a story it holds starts no session
// on a free slot; once the lease expires, the story counts as held, and the
// digest carries the lease with its evidence and expiry.
func TestSupervisor_FreeSlotAndALease(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, nil)
	h.run()

	now := time.Now().UTC()
	until := now.Add(time.Hour)
	h.tracker.setCandidates(domain.Issue{ID: "id-5", Identifier: "ONS-5", Title: "five", State: "To Do"})
	h.onLoop(func(context.Context) {
		h.o.state.StandingHolds["id-5"] = &StandingHold{
			Identifier: "ONS-5", Holder: "supervisor", Reason: "proving the chain",
			Evidence: "sandbox run chain-4", Since: now, RenewedAt: now, Until: until,
		}
	})
	h.refresh()
	h.refresh()
	var last *SupervisorTrigger
	var held bool
	h.onLoop(func(context.Context) {
		last = h.o.state.supervisor.lastTrigger
		_, held = h.o.state.Held["id-5"]
	})
	if !held || last != nil || h.sup.sessions() != 0 {
		t.Fatalf("a live lease (held %v) noted %+v and started %d sessions, want none", held, last, h.sup.sessions())
	}

	h.onLoop(func(context.Context) { h.o.state.StandingHolds["id-5"].ExpiredAt = now })
	h.refresh()
	prompt := waittest.Receive(t, h.sup.turns, "the supervisor's turn")
	for _, want := range []string{
		"Started by idle_slot.",
		"1 stories are held",
		`"leases": [`,
		`"holder": "supervisor"`,
		`"evidence": "sandbox run chain-4"`,
		`"until": "` + until.Format(time.RFC3339Nano) + `"`,
		`"expired_at": "` + now.Format(time.RFC3339Nano) + `"`,
	} {
		if !strings.Contains(prompt, want) {
			t.Errorf("prompt lacks %q:\n%s", want, prompt)
		}
	}
	h.waitEnded()
}

// The sweep starts a session every sweep interval.
func TestSupervisor_StartsOnTheSweep(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, func(cfg *config.ServiceConfig) { cfg.Supervisor.SweepIntervalMS = 1 })
	h.run()

	prompt := waittest.Receive(t, h.sup.turns, "the sweep's turn")
	if !strings.Contains(prompt, "Started by sweep.") || !strings.Contains(prompt, "- sweep: the periodic sweep") {
		t.Errorf("prompt = %s, want the sweep", prompt)
	}
	if snap := h.snapshot(); snap.NextSweepAt.IsZero() {
		t.Errorf("next sweep = %v, want one scheduled", snap.NextSweepAt)
	}
}

// Scenario "The supervisor never takes a story's slot": with every story
// slot busy, a park starts the supervisor in its own slot, and a story
// dispatches while the supervisor still runs.
func TestScenario_TheSupervisorNeverTakesAStorysSlot(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, func(cfg *config.ServiceConfig) { cfg.Agent.MaxConcurrentAgents = 1 })
	// Only ONS-A's turn is released: ONS-C keeps running for the test to
	// see, where a released ONS-C could end before a snapshot showed it.
	releaseStory := make(chan struct{})
	h.story = &mockAgentAdapter{
		runTurnFn: func(ctx context.Context, session domain.Session, p domain.RunTurnParams) (domain.TurnResult, error) {
			var release <-chan struct{}
			if p.Issue.Identifier == "ONS-A" {
				release = releaseStory
			}
			select {
			case <-release:
				return domain.TurnResult{SessionID: session.ID, ExitReason: domain.EventTurnCompleted}, nil
			case <-ctx.Done():
				return domain.TurnResult{SessionID: session.ID, ExitReason: domain.EventTurnCancelled}, ctx.Err()
			}
		},
	}
	releaseSupervisor := make(chan struct{})
	h.sup.setTurn(blockingTurn(releaseSupervisor))
	defer close(releaseSupervisor)
	h.tracker.setCandidates(domain.Issue{ID: "id-a", Identifier: "ONS-A", Title: "a", State: "To Do"})
	h.run()

	running := func() []string {
		snap, err := h.o.SnapshotFunc()()
		if err != nil {
			t.Fatal(err)
		}
		ids := []string{}
		for _, r := range snap.Running {
			ids = append(ids, r.Identifier)
		}
		slices.Sort(ids)
		return ids
	}
	waittest.Until(t, func() error {
		if got := running(); !slices.Equal(got, []string{"ONS-A"}) {
			return fmt.Errorf("running = %v, want ONS-A; log: %s", got, h.logs.String())
		}
		return nil
	})

	h.park("id-b", "ONS-B")
	waittest.Receive(t, h.sup.turns, "the supervisor's turn")
	if snap := h.snapshot(); snap.Running == nil || snap.Status != SupervisorStatusRunning {
		t.Fatalf("supervisor = %+v, want running", snap)
	}
	if got := running(); !slices.Equal(got, []string{"ONS-A"}) {
		t.Fatalf("running with the supervisor = %v, want only the story ONS-A", got)
	}

	h.tracker.finish("id-a")
	h.tracker.setCandidates(domain.Issue{ID: "id-c", Identifier: "ONS-C", Title: "c", State: "To Do"})
	close(releaseStory)
	waittest.Until(t, func() error {
		if got := running(); slices.Contains(got, "ONS-A") {
			return fmt.Errorf("running = %v, want ONS-A ended", got)
		}
		return nil
	})
	h.refresh()
	waittest.Until(t, func() error {
		if got := running(); !slices.Equal(got, []string{"ONS-C"}) {
			return fmt.Errorf("running = %v, want ONS-C dispatched in the one story slot", got)
		}
		return nil
	})
	if snap := h.snapshot(); snap.Running == nil {
		t.Errorf("supervisor = %+v, want it still running beside ONS-C", snap)
	}
}

// The daily session budget starts no session past it, and the state says
// why.
func TestSupervisor_SessionBudgetStops(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, func(cfg *config.ServiceConfig) { cfg.Supervisor.MaxSessionsPerDay = 1 })
	h.run()

	h.park("id-7", "ONS-7")
	h.waitEnded()
	h.park("id-8", "ONS-8")
	h.onLoop(func(context.Context) {})
	snap := h.snapshot()
	if snap.Running != nil || snap.Status != SupervisorStatusBudgetExhausted || h.sup.sessions() != 1 {
		t.Errorf("supervisor = %+v with %d sessions, want budget_exhausted after one", snap, h.sup.sessions())
	}
	if snap.LastTrigger == nil || snap.LastTrigger.Identifier != "ONS-8" || len(snap.Pending) != 0 {
		t.Errorf("last trigger %+v pending %v, want the park of ONS-8 dropped", snap.LastTrigger, snap.Pending)
	}
}

// A session that reaches the daily token budget is stopped, and no
// further session starts that day.
func TestSupervisor_TokenBudgetStopsTheRunningSession(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, func(cfg *config.ServiceConfig) { cfg.Supervisor.MaxTokensPerDay = 1000 })
	h.sup.setTurn(func(ctx context.Context, _ string, onEvent func(domain.AgentEvent)) (domain.TurnResult, error) {
		onEvent(domain.AgentEvent{Type: domain.EventTokenUsage, Usage: domain.TokenUsage{InputTokens: 1400, OutputTokens: 100, TotalTokens: 1500}})
		<-ctx.Done()
		return domain.TurnResult{ExitReason: domain.EventTurnCancelled}, ctx.Err()
	})
	h.run()

	h.park("id-7", "ONS-7")
	snap := h.waitEnded()
	if snap.LastSession.Outcome != SupervisorOutcomeBudget || snap.LastSession.Tokens != 1500 {
		t.Errorf("last session = %+v, want stopped on the budget at 1500 tokens", snap.LastSession)
	}
	if snap.Status != SupervisorStatusBudgetExhausted || snap.Budget.TokensUsed != 1500 {
		t.Errorf("status %s budget %+v, want budget_exhausted with 1500 tokens used", snap.Status, snap.Budget)
	}
	h.park("id-8", "ONS-8")
	h.onLoop(func(context.Context) {})
	if h.sup.sessions() != 1 {
		t.Errorf("sessions = %d after the budget, want 1", h.sup.sessions())
	}
}

// The day's budget outlives a restart.
func TestSupervisor_BudgetOutlivesARestart(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, func(cfg *config.ServiceConfig) { cfg.Supervisor.MaxSessionsPerDay = 2 })
	for range 2 {
		if _, err := h.store.StartSupervisorSession(t.Context(), persistence.SupervisorSession{TriggerKind: "sweep", StartedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	h.run()

	h.park("id-7", "ONS-7")
	h.onLoop(func(context.Context) {})
	if snap := h.snapshot(); snap.Status != SupervisorStatusBudgetExhausted || snap.Budget.SessionsUsed != 2 || h.sup.sessions() != 0 {
		t.Errorf("supervisor = %+v with %d sessions, want the stored two sessions to spend the budget", snap, h.sup.sessions())
	}
}

// A pause stops the running session and starts none; resume starts one
// for what came in while paused.
func TestSupervisor_PauseAndResume(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, nil)
	h.sup.setTurn(blockingTurn(make(chan struct{})))
	h.run()
	pause := h.o.SupervisorPauseFunc()

	h.park("id-7", "ONS-7")
	waittest.Receive(t, h.sup.turns, "the supervisor's turn")

	got := pause(t.Context(), true, "taichi", "rewriting the prompt")
	if got.Status != SupervisorPauseDone || got.Pause == nil || got.Pause.By != "taichi" {
		t.Fatalf("pause = %+v, want done by taichi", got)
	}
	if again := pause(t.Context(), true, "someone", ""); again.Pause == nil || *again.Pause != *got.Pause {
		t.Errorf("second pause = %+v, want the first kept", again)
	}
	var snap *SupervisorSnapshot
	waittest.Until(t, func() error {
		snap = h.snapshot()
		if snap.LastSession == nil || snap.Running != nil {
			return fmt.Errorf("supervisor = %+v, want the session stopped", snap)
		}
		return nil
	})
	if snap.LastSession.Outcome != SupervisorOutcomePaused || snap.Status != SupervisorStatusPaused {
		t.Errorf("supervisor = %+v, want paused with the session stopped by the pause", snap)
	}

	h.sup.setTurn(nil)
	h.park("id-8", "ONS-8")
	h.onLoop(func(context.Context) {})
	if snap := h.snapshot(); snap.Running != nil || h.sup.sessions() != 1 || len(snap.Pending) != 1 {
		t.Fatalf("paused supervisor = %+v with %d sessions, want the park of ONS-8 kept and no session", snap, h.sup.sessions())
	}

	if got := pause(t.Context(), false, "", ""); got.Status != SupervisorPauseDone || got.Pause != nil {
		t.Fatalf("resume = %+v, want done with no pause", got)
	}
	prompt := waittest.Receive(t, h.sup.turns, "the turn after resume")
	if !strings.Contains(prompt, "- park ONS-8") {
		t.Errorf("prompt after resume = %s, want the park of ONS-8", prompt)
	}
}

func TestSupervisor_PauseWithoutASupervisor(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, func(cfg *config.ServiceConfig) { cfg.Supervisor = config.SupervisorConfig{} })
	h.run()
	if got := h.o.SupervisorPauseFunc()(t.Context(), true, "taichi", ""); got.Status != SupervisorPauseNotConfigured {
		t.Errorf("pause = %+v, want not configured", got)
	}
	h.park("id-7", "ONS-7")
	h.onLoop(func(context.Context) {})
	if snap := h.snapshot(); snap != nil || h.sup.sessions() != 0 {
		t.Errorf("supervisor = %+v with %d sessions, want none", snap, h.sup.sessions())
	}
}

// Without the supervisor token no session starts, and the state says why.
func TestSupervisor_UnavailableWithoutAToken(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, nil)
	h.run()
	h.onLoop(func(context.Context) { h.o.supervisorToken = "" })
	h.park("id-7", "ONS-7")
	h.onLoop(func(context.Context) {})
	if snap := h.snapshot(); snap.Status != SupervisorStatusUnavailable || !strings.Contains(snap.Unavailable, "server.supervisor_token") || h.sup.sessions() != 0 {
		t.Errorf("supervisor = %+v, want unavailable for the missing token", snap)
	}
}

// Scenario "A merged story stays parked": ONS-7 is parked and the forge
// shows its pull request merged. Within one sweep the supervisor records
// the merge and releases the park, the story ends in the done state, and
// ONS-9, which ONS-7 blocked, is dispatched.
func TestScenario_AMergedStoryStaysParked(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, func(cfg *config.ServiceConfig) {
		cfg.Tracker.TerminalStates = []string{"Done"}
		cfg.Supervisor.SweepIntervalMS, cfg.Supervisor.QuietSweepMS = 1, 1
	})
	h.params = func(p *OrchestratorParams) {
		p.MergeCompletionConfig = MergeCompletionReactionConfig{TargetState: "Done"}
		p.MergeCompletionReactionConfigured = true
	}
	h.forge.status = domain.PRMergeStatus{Merged: true, Closed: true, MergeCommitSHA: "abc123"}
	h.tracker.transitionIssueFn = func(_ context.Context, id, target string) error {
		if target == "Done" {
			h.tracker.finish(id)
		}
		return nil
	}
	h.tracker.blockers["id-9"] = []string{"id-7"}
	dispatched := make(chan string, 16)
	h.story = &mockAgentAdapter{startSessionFn: func(_ context.Context, params domain.StartSessionParams) (domain.Session, error) {
		dispatched <- filepath.Base(params.WorkspacePath)
		return domain.Session{ID: "story"}, nil
	}}
	var once sync.Once
	results := make(chan []SupervisorResult, 1)
	h.sup.setTurn(func(ctx context.Context, prompt string, _ func(domain.AgentEvent)) (domain.TurnResult, error) {
		if strings.Contains(prompt, "- sweep") && strings.Contains(prompt, `"reason": "agent_blocked"`) {
			once.Do(func() {
				sup := h.o.Supervisor()
				results <- []SupervisorResult{sup.RecordMerge(ctx, "ONS-7", "o/r#12", "abc123"), sup.ReleasePark(ctx, "ONS-7")}
			})
		}
		return domain.TurnResult{ExitReason: domain.EventTurnCompleted}, nil
	})
	h.run()
	h.park("id-7", "ONS-7")
	h.tracker.setCandidates(
		domain.Issue{ID: "id-7", Identifier: "ONS-7", Title: "seven", State: "To Do"},
		domain.Issue{ID: "id-9", Identifier: "ONS-9", Title: "nine", State: "To Do"},
	)

	got := waittest.Receive(t, results, "the sweep's actions")
	if got[0].Status != SupervisorDone || got[0].MergeCommit != "abc123" || got[1].Status != SupervisorDone {
		t.Fatalf("actions = %+v, want the merge recorded and the park released", got)
	}
	rows, err := h.store.LoadPlanSatisfaction(t.Context(), []string{"id-7"})
	if err != nil || len(rows["id-7"]) != 1 || rows["id-7"][0].Kind != persistence.PlanSatisfactionMerged {
		t.Errorf("plan satisfaction = %+v, %v, want the merge", rows, err)
	}

	// The release queued a poll: it moves ONS-7 to Done.
	waittest.Until(t, func() error {
		if h.tracker.stateOf("id-7") != "Done" {
			return fmt.Errorf("ONS-7 is %s, want Done", h.tracker.stateOf("id-7"))
		}
		return nil
	})
	// The next poll finds ONS-9's blocker done and dispatches it.
	h.refresh()
	if got := waittest.Receive(t, dispatched, "the dispatch of ONS-9"); got != "ONS-9" {
		t.Errorf("dispatched %s, want ONS-9", got)
	}
}

// Scenario "A question with a false premise": a story parks asking a
// person to push a commit the forge already holds; the supervisor answers
// with what the forge shows and what is left, and the story continues.
func TestScenario_AQuestionWithAFalsePremise(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, nil)
	answered := make(chan ControlReply, 1)
	h.sup.setTurn(func(_ context.Context, prompt string, _ func(domain.AgentEvent)) (domain.TurnResult, error) {
		if !strings.Contains(prompt, "Please push the fix commit") {
			return domain.TurnResult{}, errors.New("the park's statement is not in the digest")
		}
		answered <- h.o.AnswerFunc()("ONS-7", parkanswer.Answer{
			Text:   "The forge already holds the fix commit head-1 on the branch; what is left is to rerun make test.",
			Author: "supervisor",
		})
		return domain.TurnResult{ExitReason: domain.EventTurnCompleted}, nil
	})
	h.run()

	h.park("id-7", "ONS-7")
	if reply := waittest.Receive(t, answered, "the supervisor's answer"); reply.Outcome != ControlDone {
		t.Fatalf("answer = %+v, want done", reply)
	}
	h.tracker.commentMu.Lock()
	comments := slices.Clone(h.tracker.commentCalls)
	h.tracker.commentMu.Unlock()
	if len(comments) != 1 || comments[0].IssueID != "id-7" || !strings.Contains(comments[0].Text, "already holds the fix commit") {
		t.Errorf("comments = %+v, want the supervisor's answer on ONS-7", comments)
	}
	h.onLoop(func(context.Context) {
		release := h.o.state.ParkReleases["id-7"]
		if _, parked := h.o.state.Parked["id-7"]; parked || release == nil || release.ReleaseGesture != unparkTriggerAnswered {
			t.Errorf("park = %v release %+v, want released by the answer", parked, release)
		}
	})
}

// Scenario "A pull request nobody is moving": a pull request whose review
// and CI passed has waited 30 minutes on a person to merge it, too short a
// wait for the stalled-reaction trigger; the sweep starts the supervisor,
// which merges it, and the merge is recorded.
func TestScenario_APullRequestNobodyIsMoving(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, func(cfg *config.ServiceConfig) { cfg.Supervisor.SweepIntervalMS, cfg.Supervisor.QuietSweepMS = 1, 1 })
	var once sync.Once
	merged := make(chan SupervisorResult, 1)
	h.sup.setTurn(func(ctx context.Context, prompt string, _ func(domain.AgentEvent)) (domain.TurnResult, error) {
		if strings.Contains(prompt, "- sweep") && strings.Contains(prompt, `"pull_request": "owner/repo#41"`) {
			once.Do(func() { merged <- h.o.Supervisor().Merge(ctx, "id-41-ident") })
		}
		return domain.TurnResult{ExitReason: domain.EventTurnCompleted}, nil
	})
	h.run()

	var pending []SupervisorTrigger
	h.onLoop(func(context.Context) {
		waiting := newReviewPendingEntry("id-41", 41)
		waiting.CreatedAt = time.Now().Add(-31 * time.Minute)
		h.o.state.PendingReactions[ReactionKey("id-41", ReactionKindReview)] = waiting
		noteStalledReactions(h.o.state, time.Now())
		pending = slices.Clone(h.o.state.supervisor.pending)
	})
	if slices.ContainsFunc(pending, func(t SupervisorTrigger) bool { return t.Kind == SupervisorTriggerStalledReaction }) {
		t.Fatalf("a 31-minute wait started a stalled-reaction trigger: %v", triggerKinds(pending))
	}
	got := waittest.Receive(t, merged, "the supervisor's merge")
	if got.Status != SupervisorDone || got.MergeCommit != "merge-1" || len(h.forge.merges) != 1 {
		t.Fatalf("merge = %+v with merges %v, want the pull request merged once", got, h.forge.merges)
	}
	rows, err := h.store.LoadPlanSatisfaction(t.Context(), []string{"id-41"})
	if err != nil || len(rows["id-41"]) != 1 || rows["id-41"][0].Ref != "merge-1" {
		t.Errorf("plan satisfaction = %+v, %v, want the merge recorded", rows, err)
	}
}

// Scenario "A pull request nobody is moving", the conflict: the pull
// request conflicts and no fix run started for an hour; the stalled
// reaction starts the supervisor, whose merge the forge refuses as not
// mergeable, so it starts the fix run.
func TestScenario_AConflictingPullRequestNobodyIsMoving(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, nil)
	h.forge.status.Mergeability = domain.MergeabilityDirty
	acted := make(chan []SupervisorResult, 1)
	h.sup.setTurn(func(ctx context.Context, prompt string, _ func(domain.AgentEvent)) (domain.TurnResult, error) {
		if !strings.Contains(prompt, "- stalled_reaction id-41-ident") {
			return domain.TurnResult{}, errors.New("not the stalled reaction")
		}
		sup := h.o.Supervisor()
		merge := sup.Merge(ctx, "id-41-ident")
		acted <- []SupervisorResult{merge, sup.FixRun(ctx, "id-41-ident", "owner/repo#41 conflicts with its base; rebase it and resolve the conflict.", "")}
		return domain.TurnResult{ExitReason: domain.EventTurnCompleted}, nil
	})
	h.run()

	h.onLoop(func(context.Context) {
		stalled := newReviewPendingEntry("id-41", 41)
		stalled.CreatedAt = time.Now().Add(-90 * time.Minute)
		h.o.state.PendingReactions[ReactionKey("id-41", ReactionKindReview)] = stalled
		noteStalledReactions(h.o.state, time.Now())
	})
	got := waittest.Receive(t, acted, "the supervisor's actions")
	if got[0].Status != SupervisorPrecondition || got[0].Code != "not_mergeable" || len(h.forge.merges) != 0 {
		t.Errorf("merge = %+v with merges %v, want refused as not mergeable", got[0], h.forge.merges)
	}
	if got[1].Status != SupervisorDone || got[1].PullRequest != "owner/repo#41" {
		t.Fatalf("fix run = %+v, want started on owner/repo#41", got[1])
	}
	h.onLoop(func(context.Context) {
		retry := h.o.state.RetryAttempts["id-41"]
		if retry == nil || retry.ReactionKind != ReactionKindReview {
			t.Fatalf("retry = %+v, want a review fix run", retry)
		}
		comments, _ := retry.ContinuationContext[reviewCommentsKey].([]map[string]any)
		if len(comments) != 1 || !strings.Contains(comments[0]["body"].(string), "conflicts with its base") {
			t.Errorf("review_comments = %#v, want the supervisor's message", retry.ContinuationContext[reviewCommentsKey])
		}
	})
}

// Scenario "A token scope": pushes fail on a missing scope; the supervisor
// raises one question naming the token, the scope, the stories it holds,
// and what it checked. The stories do not retry until the check passes.
func TestScenario_ASupervisorQuestionForATokenScope(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, nil)
	question := SupervisorQuestion{
		Kind:    "credential",
		Action:  `Give taichi's GitHub token "Workflows: Read and write"`,
		Where:   "GitHub, Settings, Developer settings, Personal access tokens",
		Stories: []string{"ONS-49", "ONS-50"},
		Checked: "Both pushes were refused for the workflow scope; the token lists repo but not workflow.",
		Cause:   "Pushes that change .github/workflows need the workflow scope.",
	}
	asked := make(chan []SupervisorQuestionResult, 1)
	h.sup.setTurn(func(ctx context.Context, _ string, _ func(domain.AgentEvent)) (domain.TurnResult, error) {
		sup := h.o.Supervisor()
		first := sup.AskPerson(ctx, question)
		again := question
		again.Action = strings.ToLower(question.Action)
		again.Stories = []string{"ONS-50"}
		asked <- []SupervisorQuestionResult{first, sup.AskPerson(ctx, again)}
		return domain.TurnResult{ExitReason: domain.EventTurnCompleted}, nil
	})
	h.run()

	h.onLoop(func(ctx context.Context) {
		h.o.state.Claimed["id-50"] = struct{}{}
		ScheduleRetry(h.o.state, ScheduleRetryParams{
			IssueID: "id-50", Identifier: "ONS-50", Attempt: 1, DelayMS: time.Hour.Milliseconds(), Error: "push refused",
		}, h.o.onRetryFire)
		parkIssue(h.o.state, parkIssueParams{
			IssueID: "id-49", Identifier: "ONS-49", Reason: parkReasonRepeatedFailure, Store: h.o.store, Metrics: h.o.metrics,
			Logger: discardLogger(), Ctx: ctx,
		})
	})
	got := waittest.Receive(t, asked, "the supervisor's questions")
	first, second := got[0], got[1]
	if first.Status != SupervisorDone || first.Existing || second.Status != SupervisorDone || !second.Existing || first.Fault.ID != second.Fault.ID {
		t.Fatalf("questions = %+v, want one question the second joined", got)
	}
	f := first.Fault
	if f.Class != PlatformFaultClassSupervisorQuestion || f.Action == nil || f.Action.Action != question.Action || f.Action.Where != question.Where ||
		f.Detail != question.Checked || f.Cause != question.Cause {
		t.Errorf("fault = %+v, want the action, where, what was checked and why", f)
	}
	h.onLoop(func(context.Context) {
		if n := len(h.o.state.PlatformFaults); n != 1 {
			t.Errorf("platform faults = %d, want one question", n)
		}
		for _, id := range []string{"id-49", "id-50"} {
			if h.o.state.FaultHolds[id] != f.ID {
				t.Errorf("%s is not held by the question", id)
			}
		}
		if _, retrying := h.o.state.RetryAttempts["id-50"]; retrying {
			t.Error("ONS-50 still has its retry while the question holds it")
		}
	})

	done := h.o.PlatformFaultDoneFunc()(t.Context(), f.ID, "taichi", "")
	if done.Status != PlatformFaultDoneChecked || !done.Resolved {
		t.Fatalf("action done = %+v, want the check to pass and release the stories", done)
	}
	h.onLoop(func(context.Context) {
		if len(h.o.state.FaultHolds) != 0 {
			t.Errorf("fault holds after the check = %v, want none", h.o.state.FaultHolds)
		}
	})
}

func TestSupervisor_AskPersonRefusals(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, nil)
	h.run()
	h.park("id-7", "ONS-7")
	sup := h.o.Supervisor()
	q := SupervisorQuestion{Kind: "approval", Action: "Approve the spec", Where: "the workbench", Stories: []string{"ONS-7"}, Checked: "the spec has no approver"}

	for name, change := range map[string]func(q *SupervisorQuestion){
		"no action":       func(q *SupervisorQuestion) { q.Action = " " },
		"no place":        func(q *SupervisorQuestion) { q.Where = "" },
		"nothing checked": func(q *SupervisorQuestion) { q.Checked = "" },
		"no story":        func(q *SupervisorQuestion) { q.Stories = nil },
		"no kind":         func(q *SupervisorQuestion) { q.Kind = "" },
		"an unknown kind": func(q *SupervisorQuestion) { q.Kind = "chore" },
	} {
		bad := q
		change(&bad)
		if got := sup.AskPerson(t.Context(), bad); got.Status != SupervisorPrecondition || got.Code != "invalid_request" {
			t.Errorf("%s: %+v, want invalid_request", name, got)
		}
	}
	unknown := q
	unknown.Stories = []string{"ONS-404"}
	if got := sup.AskPerson(t.Context(), unknown); got.Status != SupervisorUnknownIssue {
		t.Errorf("unknown story: %+v, want unknown issue", got)
	}
	if got := sup.AskPerson(t.Context(), q); got.Status != SupervisorDone || got.Identifier != "ONS-7" {
		t.Errorf("question = %+v, want done", got)
	}
}
