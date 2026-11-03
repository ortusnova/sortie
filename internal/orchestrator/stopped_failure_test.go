package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/failcause"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/waittest"
)

// testsOutput is what make test prints when a test fails.
const testsOutput = "--- FAIL: TestParse (0.00s)\n" +
	"    parse_test.go:12: Parse(\"1+1\") = 3, want 2\n" +
	"FAIL\n" +
	"FAIL\tgithub.com/ortusnova/sortie/internal/parse\t0.004s\n" +
	"make: *** [Makefile:40: test] Error 1"

// makeTestError is the failure of make test, carrying its output.
type makeTestError struct{}

func (makeTestError) Error() string         { return "make test: exit status 2" }
func (makeTestError) FailureOutput() string { return testsOutput }

// failingTests is the error a run ends on when its make test fails.
func failingTests() error {
	return fmt.Errorf("agent turn 1: %w", &domain.AgentError{
		Kind: domain.ErrTurnFailed, Message: "make test: exit status 2", Err: makeTestError{},
	})
}

// refusedPushOf is the error the export of identifier's branch fails with
// when GitHub refuses the push for the token's missing workflow scope.
func refusedPushOf(identifier string) error {
	return &ExportError{
		Branch: "sortie/" + identifier,
		Err: errors.New("push sortie/" + identifier + " to https://github.com/ortusnova/sortie.git: " +
			"[remote rejected] (refusing to allow a Personal Access Token to create or update workflow `.github/workflows/ci.yml` without `workflow` scope): " +
			"git push: exit status 1"),
	}
}

// faultHarness returns a run harness over a migrated store, polling a
// tracker that holds issues as active candidates, with a budget of 16
// sessions per issue and Sortie on taichi.
func faultHarness(t *testing.T, issues ...domain.Issue) (*runHarness, *persistence.Store) {
	t.Helper()
	cfg := runConfig(t, map[string]any{"agent": map[string]any{"kind": "kind-a", "command": "a-cmd"}})
	cfg.Agent.MaxSessions = 16
	cfg.Agent.MaxConcurrentAgents = 4
	cfg.Agent.MaxRetryBackoffMS = 300_000
	h := newRunHarness(t, cfg, "kind-a")
	h.state.MaxConcurrentAgents = 4
	for _, issue := range issues {
		h.tracker.candidates = append(h.tracker.candidates, issue)
		h.tracker.issues[issue.ID] = issue
	}
	store := openRepeatedFailureStore(t)
	h.orchestratorStore = store
	h.build()
	h.o.hostName = "taichi"
	return h, store
}

// stopRun is the fake worker: it runs issue as its pending retry, or as a
// first dispatch, and reports the run stopped on err to the orchestrator.
// The event loop is not running, so the caller is the loop.
func stopRun(h *runHarness, issue domain.Issue, err error) {
	h.t.Helper()
	s := h.o.state
	var attempt *int
	var trigger string
	if pending, ok := s.RetryAttempts[issue.ID]; ok {
		attempt, trigger = new(pending.Attempt), pending.OriginTrigger
	}
	CancelRetry(s, issue.ID)
	s.Running[issue.ID] = &RunningEntry{
		Identifier: issue.Identifier, Issue: issue, StartedAt: time.Now().UTC(),
		RetryAttempt: attempt, Trigger: trigger, AgentKind: "kind-a",
	}
	s.Claimed[issue.ID] = struct{}{}
	h.o.handleWorkerExit(context.Background(), WorkerResult{
		IssueID: issue.ID, Identifier: issue.Identifier, ExitKind: WorkerExitError,
		Error: err, AgentAdapter: "kind-a", Attempt: attempt,
	})
}

// sessionsUsed returns the sessions each issue has used of its budget.
func sessionsUsed(t *testing.T, store *persistence.Store, ids ...string) []int {
	t.Helper()
	used := make([]int, len(ids))
	for i, id := range ids {
		n, err := store.CountRunHistoryByIssue(context.Background(), id)
		if err != nil {
			t.Fatalf("CountRunHistoryByIssue(%s): %v", id, err)
		}
		used[i] = n
	}
	return used
}

// Scenario: A missing token scope. ONS-49, ONS-50 and ONS-51 each stop on a
// push refused for the workflow scope: there is one platform fault that
// holds the three stories, and one question that says to give taichi's
// GitHub token "Workflows: Read and write". Once the token has it, Sortie's
// next check passes and the three stories run again with their budgets.
func TestScenario_AMissingTokenScope(t *testing.T) {
	t.Parallel()

	stories := []domain.Issue{candidate("id-49", "ONS-49"), candidate("id-50", "ONS-50"), candidate("id-51", "ONS-51")}
	ids := []string{"id-49", "id-50", "id-51"}
	h, store := faultHarness(t, stories...)
	var tokenHasScope atomic.Bool
	var checks atomic.Int64
	h.o.faultChecker = func(_ context.Context, check PlatformFaultCheck) error {
		checks.Add(1)
		if len(check.Stories) != 3 {
			return fmt.Errorf("check names %d stories, want 3", len(check.Stories))
		}
		if !tokenHasScope.Load() {
			return errors.New("push sortie/ONS-49: [remote rejected] (refusing to allow a Personal Access Token to create or update workflow without `workflow` scope)")
		}
		return nil
	}

	// ONS-49 stops on the refusal twice; ONS-50 and ONS-51 stop on it once
	// each, after the fault is recorded.
	stopRun(h, stories[0], refusedPushOf("ONS-49"))
	if _, retried := h.state.RetryAttempts["id-49"]; !retried {
		t.Fatal("the first refusal scheduled no retry")
	}
	stopRun(h, stories[0], refusedPushOf("ONS-49"))
	stopRun(h, stories[1], refusedPushOf("ONS-50"))
	stopRun(h, stories[2], refusedPushOf("ONS-51"))

	if len(h.state.PlatformFaults) != 1 {
		t.Fatalf("platform faults = %d, want one", len(h.state.PlatformFaults))
	}
	snap := RuntimeSnapshot(h.state, time.Now())
	if len(snap.PlatformFaults) != 1 {
		t.Fatalf("snapshot platform faults = %d, want one", len(snap.PlatformFaults))
	}
	fault := snap.PlatformFaults[0]
	if fault.Class != "token_scope" || fault.Step != "export" {
		t.Errorf("fault = class %q, step %q, want token_scope at export", fault.Class, fault.Step)
	}
	if fault.Action == nil || fault.Action.Action != `Give taichi's GitHub token "Workflows: Read and write"` {
		t.Fatalf("fault person action = %+v, want taichi's GitHub token given Workflows: Read and write", fault.Action)
	}
	if !strings.Contains(fault.Action.Where, "Personal access tokens") || !strings.Contains(fault.Cause, "without `workflow` scope") {
		t.Errorf("fault where %q, cause %q, want the token settings and the refusal", fault.Action.Where, fault.Cause)
	}
	var held []string
	for _, hold := range fault.Holds {
		held = append(held, hold.Identifier)
	}
	if !slices.Equal(held, []string{"ONS-49", "ONS-50", "ONS-51"}) {
		t.Errorf("fault holds %v, want ONS-49, ONS-50 and ONS-51", held)
	}
	for _, id := range ids {
		if _, ok := h.state.RetryAttempts[id]; ok {
			t.Errorf("%s has a retry, want it held", id)
		}
		if _, ok := h.state.Claimed[id]; ok {
			t.Errorf("%s is claimed, want it held", id)
		}
		if _, ok := h.state.Parked[id]; ok {
			t.Errorf("%s is parked, want it held under the fault", id)
		}
	}
	open, err := store.LoadOpenPlatformFaults(context.Background())
	if err != nil || len(open) != 1 || len(open[0].Holds) != 3 || open[0].Action != fault.Action.Action {
		t.Fatalf("stored faults = %+v, %v, want the one fault holding three stories", open, err)
	}
	usedAtHold := sessionsUsed(t, store, ids...)
	if !slices.Equal(usedAtHold, []int{2, 1, 1}) {
		t.Fatalf("sessions used = %v, want 2, 1 and 1", usedAtHold)
	}

	// Held stories use no budget: polls dispatch none of them, and the
	// scheduled check is not due before its interval.
	for range 2 {
		if dispatched, _ := tickOnce(h); len(dispatched) != 0 {
			t.Fatalf("a poll dispatched %v while the fault holds them", dispatched)
		}
	}
	if due := dueFaultChecks(h.state, time.Now()); len(due) != 0 {
		t.Fatalf("checks due before the interval: %+v", due)
	}

	// The scheduled check while the token still lacks the scope keeps
	// every hold.
	due := dueFaultChecks(h.state, time.Now().Add(PlatformFaultCheckInterval))
	if len(due) != 1 {
		t.Fatalf("checks due after the interval = %d, want 1", len(due))
	}
	if _, resolved := h.o.applyFaultCheckOnLoop(context.Background(), due[0].FaultID, h.o.runFaultCheck(context.Background(), due[0])); resolved {
		t.Fatal("a failing check resolved the fault")
	}
	if dispatched, _ := tickOnce(h); len(dispatched) != 0 {
		t.Fatalf("a poll after the failing check dispatched %v", dispatched)
	}
	if got := RuntimeSnapshot(h.state, time.Now()).PlatformFaults; len(got) != 1 || !strings.Contains(got[0].LastCheckError, "workflow") {
		t.Fatalf("fault after the failing check = %+v, want it open with the check's error", got)
	}
	if got := sessionsUsed(t, store, ids...); !slices.Equal(got, usedAtHold) {
		t.Fatalf("sessions used while held = %v, want %v unchanged", got, usedAtHold)
	}

	// The token gets the scope: the next check passes.
	tokenHasScope.Store(true)
	due = dueFaultChecks(h.state, time.Now().Add(2*PlatformFaultCheckInterval))
	if len(due) != 1 {
		t.Fatalf("checks due at the next interval = %d, want 1", len(due))
	}
	if _, resolved := h.o.applyFaultCheckOnLoop(context.Background(), due[0].FaultID, h.o.runFaultCheck(context.Background(), due[0])); !resolved {
		t.Fatal("a passing check left the fault open")
	}
	if checks.Load() != 2 {
		t.Errorf("checks run = %d, want 2", checks.Load())
	}
	if len(h.state.PlatformFaults) != 0 || len(h.state.FaultHolds) != 0 {
		t.Fatalf("after the passing check: faults %v, holds %v, want none", h.state.PlatformFaults, h.state.FaultHolds)
	}
	if open, err := store.LoadOpenPlatformFaults(context.Background()); err != nil || len(open) != 0 {
		t.Errorf("stored open faults after the passing check = %+v, %v, want none", open, err)
	}

	dispatched, _ := tickOnce(h)
	if !slices.Equal(dispatched, ids) {
		t.Fatalf("the poll after the passing check dispatched %v, want %v", dispatched, ids)
	}
	for i, id := range ids {
		if _, exhausted := h.state.BudgetExhausted[id]; exhausted {
			t.Errorf("%s is budget-exhausted, want it run with its budget", id)
		}
		if left := 16 - usedAtHold[i]; left < 14 {
			t.Errorf("%s has %d sessions left, want at least 14 of 16", id, left)
		}
	}
}

// Scenario: A missing token scope, cleared by a person. Marking the person
// action done makes Sortie check again at once, through the running event
// loop, and the default check passes a person-only fault once its action
// is marked done.
func TestScenario_AMissingTokenScopeMarkedDoneChecksAtOnce(t *testing.T) {
	t.Parallel()

	stories := []domain.Issue{candidate("id-49", "ONS-49"), candidate("id-50", "ONS-50"), candidate("id-51", "ONS-51")}
	h, store := faultHarness(t, stories...)
	stopRun(h, stories[0], refusedPushOf("ONS-49"))
	stopRun(h, stories[0], refusedPushOf("ONS-49"))
	stopRun(h, stories[1], refusedPushOf("ONS-50"))
	stopRun(h, stories[2], refusedPushOf("ONS-51"))
	var faultID string
	for id := range h.state.PlatformFaults {
		faultID = id
	}
	if faultID == "" {
		t.Fatal("no platform fault recorded")
	}

	h.run()
	done := h.o.PlatformFaultDoneFunc()
	if got := done(waittest.Context(t), "pf-unknown", "taichi"); got.Status != PlatformFaultDoneNotFound {
		t.Errorf("marking an unknown fault done = %+v, want not found", got)
	}
	result := done(waittest.Context(t), faultID, "taichi")
	if result.Status != PlatformFaultDoneChecked || !result.Resolved {
		t.Fatalf("marking the action done = %+v, want the fault checked and resolved", result)
	}
	if result.Fault.ActionDoneBy != "taichi" || result.Fault.ActionDoneAt.IsZero() || len(result.Fault.Holds) != 3 {
		t.Errorf("answered fault = %+v, want it marked done by taichi with its three stories", result.Fault)
	}

	sessions := h.waitForSessions(3)
	for _, story := range stories {
		sessionOfWorkspace(t, sessions, story.Identifier)
	}
	h.stop()
	if open, err := store.LoadOpenPlatformFaults(context.Background()); err != nil || len(open) != 0 {
		t.Errorf("stored open faults = %+v, %v, want none", open, err)
	}
}

// Marking the person action done while a scheduled check that did not know
// it runs makes Sortie check again at once when that check fails, not one
// interval later.
func TestScenario_AMissingTokenScopeMarkedDoneDuringACheckChecksAgainAtOnce(t *testing.T) {
	t.Parallel()

	story := candidate("id-49", "ONS-49")
	h, _ := faultHarness(t, story)
	stopRun(h, story, refusedPushOf("ONS-49"))
	stopRun(h, story, refusedPushOf("ONS-49"))
	var fault *PlatformFault
	for _, f := range h.state.PlatformFaults {
		fault = f
	}
	if fault == nil {
		t.Fatal("no platform fault recorded")
	}
	fault.NextCheckAt = time.Time{}

	started := make(chan PlatformFaultCheck, 4)
	release := make(chan struct{})
	h.o.faultChecker = func(_ context.Context, check PlatformFaultCheck) error {
		started <- check
		if !check.ActionDone {
			<-release
			return errAwaitingPersonAction
		}
		return nil
	}

	h.run()
	if first := waittest.Receive(t, started, "the scheduled check"); first.ActionDone {
		t.Fatalf("scheduled check = %+v, want it run before the action was marked done", first)
	}
	if got := h.o.PlatformFaultDoneFunc()(waittest.Context(t), fault.ID, "taichi"); got.Status != PlatformFaultDoneBusy {
		t.Fatalf("marking the action done during a check = %+v, want busy", got)
	}
	close(release)
	if again := waittest.Receive(t, started, "the check after the action was marked done"); !again.ActionDone {
		t.Fatalf("second check = %+v, want it to know the action is done", again)
	}
	h.waitForSessions(1)
	h.stop()
}

// A platform fault restored at startup is checked on the first tick, and
// a passing check releases the story it holds to run.
func TestRun_RestoredPlatformFaultIsCheckedAndReleased(t *testing.T) {
	t.Parallel()

	story := candidate("id-52", "ONS-52")
	h, store := faultHarness(t, story)
	row := persistence.PlatformFault{
		ID: "pf-restored", Signature: "run | worker | the worker's lease expired", Class: "worker_lost",
		Step: "run", RecordedAt: "2026-11-02T10:00:00Z",
		Holds: []persistence.PlatformFaultHold{{IssueID: "id-52", Identifier: "ONS-52", HeldAt: "2026-11-02T10:00:00Z"}},
	}
	if err := store.SavePlatformFault(context.Background(), row); err != nil {
		t.Fatal(err)
	}
	rows, err := store.LoadOpenPlatformFaults(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	PopulatePlatformFaults(h.state, rows, discardLogger())
	if h.state.FaultHolds["id-52"] != "pf-restored" {
		t.Fatalf("restored holds = %v, want ONS-52 under pf-restored", h.state.FaultHolds)
	}

	h.run()
	h.waitForSessions(1)
	h.stop()
	if open, err := store.LoadOpenPlatformFaults(context.Background()); err != nil || len(open) != 0 {
		t.Errorf("stored open faults = %+v, %v, want the restored fault resolved", open, err)
	}
}

// Scenario: Tests fail twice. A story's second run fails because make test
// fails the same way: a new session of its agent starts with the failing
// output in its prompt.
func TestScenario_TestsFailTwice(t *testing.T) {
	t.Parallel()

	story := candidate("id-60", "ONS-60")
	h, store := faultHarness(t, story)
	prompts := make(chan string, 8)
	h.recorders["kind-a"].adapter.runTurnFn = func(_ context.Context, session domain.Session, params domain.RunTurnParams) (domain.TurnResult, error) {
		prompts <- params.Prompt
		return domain.TurnResult{SessionID: session.ID, ExitReason: domain.EventTurnCompleted}, nil
	}

	stopRun(h, story, failingTests())
	first, ok := h.state.RetryAttempts["id-60"]
	if !ok || first.OriginTrigger != triggerError {
		t.Fatalf("after the first failure: retry = %+v, %v, want an error retry", first, ok)
	}
	stopRun(h, story, failingTests())

	fix, ok := h.state.RetryAttempts["id-60"]
	if !ok || fix.OriginTrigger != triggerStoryFault || fix.Attempt != 2 {
		t.Fatalf("after the second failure: retry = %+v, %v, want the story-fault session as attempt 2", fix, ok)
	}
	if fix.Failure == nil || !strings.Contains(fix.Failure.Output, "--- FAIL: TestParse") || !strings.Contains(fix.Failure.Error, "make test") {
		t.Fatalf("story-fault retry failure = %+v, want the make test failure and its output", fix.Failure)
	}
	if len(h.state.PlatformFaults) != 0 {
		t.Errorf("platform faults = %v, want none for a story fault", h.state.PlatformFaults)
	}
	if _, parked := h.state.Parked["id-60"]; parked {
		t.Error("the story is parked, want a new session")
	}
	if used := sessionsUsed(t, store, "id-60"); used[0] != 2 {
		t.Errorf("sessions used = %d, want 2", used[0])
	}

	h.run()
	got := waittest.Receive(t, prompts, "the prompt of the story-fault session")
	h.stop()
	for _, want := range []string{
		"work on ONS-60",
		"## The previous runs failed",
		`<failure_error>"agent turn 1: agent: turn_failed: make test: exit status 2`,
		`--- FAIL: TestParse (0.00s)\n    parse_test.go:12: Parse(\"1+1\") = 3, want 2`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("session prompt = %q, want it to hold %q", got, want)
		}
	}
	sessions := h.started()["kind-a"]
	if len(sessions) == 0 || !strings.HasSuffix(sessions[0].WorkspacePath, "ONS-60") {
		t.Errorf("sessions started = %+v, want one session of the story's agent", sessions)
	}
}

// A story-fault retry restored from the store hands over its error, the
// one fact its row keeps.
func TestStoryFailureOfRetry(t *testing.T) {
	t.Parallel()

	if _, ok := storyFailureOfRetry(&RetryEntry{OriginTrigger: triggerError, Error: "x"}); ok {
		t.Error("an error retry hands over a failure")
	}
	got, ok := storyFailureOfRetry(&RetryEntry{OriginTrigger: triggerStoryFault, Error: WorkerExitedErrorPrefix + "agent turn 1: make test: exit status 2"})
	if !ok || got.Error != "agent turn 1: make test: exit status 2" || got.Output != "" {
		t.Errorf("restored hand-over = %+v, %v, want the error without the prefix", got, ok)
	}
	got, ok = storyFailureOfRetry(&RetryEntry{OriginTrigger: triggerStoryFault, Failure: &StoryFailure{Step: "agent turn 1", Error: "e", Output: "o"}})
	if !ok || got.Step != "agent turn 1" || got.Error != "e" || got.Output != "o" {
		t.Errorf("hand-over = %+v, %v, want the failure the exit kept", got, ok)
	}
}

func TestCheckPlatformFault_Default(t *testing.T) {
	t.Parallel()

	h, _ := faultHarness(t)
	tests := []struct {
		name  string
		check PlatformFaultCheck
		want  error
	}{
		{"person action not done", PlatformFaultCheck{PersonAction: true}, errAwaitingPersonAction},
		{"person action done", PlatformFaultCheck{PersonAction: true, ActionDone: true}, nil},
		{"no person action", PlatformFaultCheck{}, nil},
		{"export fault without a repository to probe", PlatformFaultCheck{Step: "export", PersonAction: true, Stories: []PlatformFaultHold{{Identifier: "ONS-49"}}}, errAwaitingPersonAction},
	}
	for _, tt := range tests {
		if got := h.o.checkPlatformFault(context.Background(), tt.check); !errors.Is(got, tt.want) {
			t.Errorf("%s: checkPlatformFault() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestHandleWorkerExit_ATransientFailureIsNeverHeld(t *testing.T) {
	t.Parallel()

	state := NewState(5000, 4, 0, nil, AgentTotals{})
	store := openRepeatedFailureStore(t)
	state.PlatformFaults["pf-x"] = &PlatformFault{ID: "pf-x", Signature: runFailureSignature(github502()).String()}
	failRun(t, state, store, "issue-7", "ONS-7", nil, github502())
	if _, held := state.FaultHolds["issue-7"]; held {
		t.Error("a transient failure was held under a platform fault")
	}
	if _, ok := state.RetryAttempts["issue-7"]; !ok {
		t.Error("no retry scheduled, want the backoff")
	}
}

func TestHandleWorkerExit_ASignatureInNoClassIsAPersonAction(t *testing.T) {
	t.Parallel()

	state := NewState(5000, 4, 0, nil, AgentTotals{})
	store := openRepeatedFailureStore(t)
	odd := errors.New("run: the disk sang")
	failRun(t, state, store, "issue-8", "ONS-8", nil, odd)
	failRun(t, state, store, "issue-8", "ONS-8", new(1), odd)

	faultID, held := state.FaultHolds["issue-8"]
	if !held {
		t.Fatal("an unclassified repeated failure is not held under a platform fault")
	}
	f := state.PlatformFaults[faultID]
	if f.Class != failcause.ClassUnclassified || f.Action == nil || !strings.Contains(f.Action.Action, "taichi") {
		t.Errorf("fault = %+v, want an unclassified fault with a person action on taichi", f)
	}
}

func TestHandleRetryTimer_AFaultHeldIssueReleasesItsRetry(t *testing.T) {
	t.Parallel()

	state := NewState(5000, 4, 0, nil, AgentTotals{})
	state.FaultHolds["issue-9"] = "pf-9"
	state.Claimed["issue-9"] = struct{}{}
	state.RetryAttempts["issue-9"] = &RetryEntry{IssueID: "issue-9", Identifier: "ONS-9", Attempt: 1}
	store := &stubStore{}
	HandleRetryTimer(state, "issue-9", HandleRetryTimerParams{Store: store, Logger: discardLogger()})
	if _, claimed := state.Claimed["issue-9"]; claimed {
		t.Error("the claim of a fault-held issue is kept")
	}
	if !slices.Contains(store.deletedRetryIDs, "issue-9") {
		t.Errorf("deleted retry rows = %v, want issue-9", store.deletedRetryIDs)
	}
}
