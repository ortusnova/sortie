package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// openRepeatedFailureStore returns a migrated store in a temporary file.
func openRepeatedFailureStore(t *testing.T) *persistence.Store {
	t.Helper()
	ctx := context.Background()
	store, err := persistence.Open(ctx, t.TempDir()+"/sortie.db")
	if err != nil {
		t.Fatalf("persistence.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("store.Close: %v", err)
		}
	})
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("store.Migrate: %v", err)
	}
	return store
}

// failRun puts a run of issueID in state as dispatched by its retry, or as
// the first dispatch when retryAttempt is nil, and ends it on err.
func failRun(t *testing.T, state *State, store WorkerExitStore, issueID, identifier string, retryAttempt *int, err error) {
	t.Helper()
	CancelRetry(state, issueID)
	state.Running[issueID] = &RunningEntry{
		Identifier:   identifier,
		Issue:        domain.Issue{ID: issueID, Identifier: identifier, State: "In Progress"},
		StartedAt:    baseTime,
		RetryAttempt: retryAttempt,
	}
	state.Claimed[issueID] = struct{}{}
	params := HandleWorkerExitParams{
		Store:               store,
		MaxRetryBackoffMS:   300_000,
		OnRetryFire:         noopRetryFire,
		NowFunc:             func() time.Time { return baseTime.Add(60 * time.Second) },
		Logger:              discardLogger(),
		Ctx:                 context.Background(),
		HandoffParkingLabel: "needs-human",
	}
	HandleWorkerExit(state, WorkerResult{
		IssueID:      issueID,
		Identifier:   identifier,
		ExitKind:     WorkerExitError,
		Error:        err,
		AgentAdapter: "mock",
		Attempt:      retryAttempt,
	}, params)
}

// refusedPush is the error the export of ONS-49's branch fails with when
// the git host refuses the push.
func refusedPush() error {
	return &ExportError{
		Branch: "sortie/ONS-49",
		Err: errors.New("push sortie/ONS-49 to https://github.com/ortusnova/sortie.git: " +
			"[remote rejected] (refusing to allow a Personal Access Token to create or update workflow `.github/workflows/ci.yml` without `workflow` scope): " +
			"git push: exit status 1"),
	}
}

// github502 is the error the export fails with when GitHub answers the
// pull request call with a 502.
func github502() error {
	cause := &domain.TrackerError{
		Kind:    domain.ErrTrackerTransport,
		Message: "POST /repos/ortusnova/sortie/pulls: server error 502: <html>Bad Gateway</html>",
		Status:  502,
	}
	return &ExportError{
		Branch: "sortie/ONS-49",
		Err:    fmt.Errorf("ensure pull request: %w", &domain.SCMError{Kind: domain.ErrSCMAPI, Message: cause.Message, Err: cause}),
	}
}

func TestHandleWorkerExit_TheSecondRefusedPush(t *testing.T) {
	t.Parallel()

	const issueID, identifier, maxSessions = "issue-49", "ONS-49", 16
	ctx := context.Background()
	store := openRepeatedFailureStore(t)
	state := NewState(5000, 4, 0, nil, AgentTotals{})

	failRun(t, state, store, issueID, identifier, nil, refusedPush())
	first, retried := state.RetryAttempts[issueID]
	if !retried || first.Attempt != 1 {
		t.Fatalf("after the first refused push: retry = %+v, %v, want attempt 1", first, retried)
	}
	if _, parked := state.Parked[issueID]; parked {
		t.Fatal("the first refused push parked the issue, want a retry")
	}

	failRun(t, state, store, issueID, identifier, new(first.Attempt), refusedPush())

	if retry, ok := state.RetryAttempts[issueID]; ok {
		t.Errorf("after the second refused push: retry %+v scheduled, want no third run", retry)
	}
	if _, claimed := state.Claimed[issueID]; claimed {
		t.Error("after the second refused push the claim is held, want it released")
	}
	parked, ok := state.Parked[issueID]
	if !ok {
		t.Fatal("after the second refused push the issue is not parked, want the failure routed to a person")
	}
	if parked.Reason != parkReasonRepeatedFailure || parked.Label != "needs-human" {
		t.Errorf("park = reason %q, label %q, want %q under needs-human", parked.Reason, parked.Label, parkReasonRepeatedFailure)
	}
	if !strings.Contains(parked.Statement, "[remote rejected]") {
		t.Errorf("park statement = %q, want the refusal", parked.Statement)
	}

	rows, err := store.ListParkedIssues(ctx)
	if err != nil || len(rows) != 1 || rows[0].Reason != parkReasonRepeatedFailure {
		t.Errorf("ListParkedIssues() = %+v, %v, want one repeated_failure park", rows, err)
	}
	retries, err := store.LoadRetryEntries(ctx)
	if err != nil || len(retries) != 0 {
		t.Errorf("LoadRetryEntries() = %+v, %v, want none", retries, err)
	}

	used, err := store.CountRunHistoryByIssue(ctx, issueID)
	if err != nil || used != 2 {
		t.Fatalf("CountRunHistoryByIssue() = %d, %v, want 2 sessions used", used, err)
	}
	if left := maxSessions - used; left != 14 {
		t.Errorf("sessions left = %d, want 14 of %d", left, maxSessions)
	}

	runs, err := store.QueryRunHistoryByIssue(ctx, issueID)
	if err != nil || len(runs) != 2 {
		t.Fatalf("QueryRunHistoryByIssue() = %d runs, %v, want 2", len(runs), err)
	}
	signature, err := store.LatestRunFailureSignature(ctx, issueID)
	if err != nil || !strings.HasPrefix(signature, "export | error | ") || strings.Contains(signature, identifier) {
		t.Errorf("LatestRunFailureSignature() = %q, %v, want an export signature without the issue key", signature, err)
	}
}

func TestHandleWorkerExit_ATransientFailure(t *testing.T) {
	t.Parallel()

	const issueID, identifier = "issue-49", "ONS-49"
	store := openRepeatedFailureStore(t)
	state := NewState(5000, 4, 0, nil, AgentTotals{})

	failRun(t, state, store, issueID, identifier, nil, github502())
	first, ok := state.RetryAttempts[issueID]
	if !ok {
		t.Fatal("after the first 502 no retry is scheduled")
	}
	before := time.Now().UnixMilli()
	failRun(t, state, store, issueID, identifier, new(first.Attempt), github502())
	after := time.Now().UnixMilli()

	if _, parked := state.Parked[issueID]; parked {
		t.Error("two 502 answers parked the issue, want the backoff kept")
	}
	second, ok := state.RetryAttempts[issueID]
	if !ok {
		t.Fatal("after the second 502 no retry is scheduled, want a retry with backoff")
	}
	if second.Attempt != 2 || second.Trigger != triggerError {
		t.Errorf("second retry = attempt %d, trigger %q, want attempt 2 on %q", second.Attempt, second.Trigger, triggerError)
	}
	if delay := computeBackoffDelay(2, 300_000); second.DueAtMS < before+delay || second.DueAtMS > after+delay {
		t.Errorf("second retry due at %d, want the backoff of attempt 2 (%d ms) after the exit in [%d, %d]", second.DueAtMS, delay, before, after)
	}
	if signature, err := store.LatestRunFailureSignature(context.Background(), issueID); err != nil ||
		!strings.Contains(signature, "| server_error |") {
		t.Errorf("LatestRunFailureSignature() = %q, %v, want the server_error class", signature, err)
	}
}

func TestHandleWorkerExit_DifferentFailuresKeepTheBackoff(t *testing.T) {
	t.Parallel()

	const issueID = "issue-7"
	store := &mockExitStore{}
	state := NewState(5000, 4, 0, nil, AgentTotals{})

	failRun(t, state, store, issueID, "ONS-7", nil, refusedPush())
	failRun(t, state, store, issueID, "ONS-7", new(1), errors.New("agent turn 1: agent: turn_failed: the model gave up"))

	if _, parked := state.Parked[issueID]; parked {
		t.Error("two different failures parked the issue")
	}
	if retry, ok := state.RetryAttempts[issueID]; !ok || retry.Attempt != 2 {
		t.Errorf("retry = %+v, %v, want attempt 2", retry, ok)
	}
}

func TestHandleWorkerExit_AFailureAfterASuccessKeepsTheBackoff(t *testing.T) {
	t.Parallel()

	const issueID = "issue-8"
	store := &mockExitStore{}
	state := NewState(5000, 4, 0, nil, AgentTotals{})

	failRun(t, state, store, issueID, "ONS-8", nil, refusedPush())
	store.runHistories = append(store.runHistories, persistence.RunHistory{IssueID: issueID, Status: "succeeded"})
	failRun(t, state, store, issueID, "ONS-8", new(1), refusedPush())

	if _, parked := state.Parked[issueID]; parked {
		t.Error("a failure whose previous run succeeded parked the issue")
	}
	if _, ok := state.RetryAttempts[issueID]; !ok {
		t.Error("no retry scheduled, want the backoff")
	}
}

func TestHandleWorkerExit_SignatureReadFailureKeepsTheBackoff(t *testing.T) {
	t.Parallel()

	const issueID = "issue-9"
	store := &mockExitStore{}
	state := NewState(5000, 4, 0, nil, AgentTotals{})

	failRun(t, state, store, issueID, "ONS-9", nil, refusedPush())
	store.failureSignatureErr = errors.New("database is locked")
	failRun(t, state, store, issueID, "ONS-9", new(1), refusedPush())

	if _, parked := state.Parked[issueID]; parked {
		t.Error("a failed signature read parked the issue")
	}
	if _, ok := state.RetryAttempts[issueID]; !ok {
		t.Error("no retry scheduled, want the backoff")
	}
}

func TestHandleWorkerExit_ARepeatedFailureOfAReadOnlyPassKeepsTheBackoff(t *testing.T) {
	t.Parallel()

	const issueID = "issue-10"
	store := &mockExitStore{}
	state := NewState(5000, 4, 0, nil, AgentTotals{})

	for attempt, retry := range []*int{nil, new(1)} {
		CancelRetry(state, issueID)
		state.Running[issueID] = &RunningEntry{
			Identifier:   "ONS-10",
			StartedAt:    baseTime,
			RetryAttempt: retry,
			ReactionKind: ReactionKindLabelReview,
		}
		state.Claimed[issueID] = struct{}{}
		HandleWorkerExit(state, WorkerResult{
			IssueID: issueID, Identifier: "ONS-10", ExitKind: WorkerExitError,
			Error: errors.New("agent session start: agent: agent_not_found: no such command"), AgentAdapter: "mock",
		}, defaultExitParams(t, store))
		if _, parked := state.Parked[issueID]; parked {
			t.Fatalf("run %d of a read-only pass parked the issue", attempt+1)
		}
	}
	if got := store.runHistories[1].FailureSignature; got == "" || got != store.runHistories[0].FailureSignature {
		t.Errorf("signatures = %q, %q, want two equal signatures", store.runHistories[0].FailureSignature, got)
	}
}

func TestFailureStep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err  error
		want string
	}{
		{refusedPush(), "export"},
		{&ExportError{Err: errors.New("run stopped before the export")}, "export"},
		{fmt.Errorf("agent session start: %w", errors.New("boom")), "agent session start"},
		{errors.New("agent turn 3: agent: turn_failed: x"), "agent turn 3"},
		{errors.New("no separator"), "run"},
	}
	for _, tt := range tests {
		if got := failureStep(tt.err); got != tt.want {
			t.Errorf("failureStep(%v) = %q, want %q", tt.err, got, tt.want)
		}
	}
	if got := runFailureSignature(errors.New("agent turn 3: agent: turn_failed: x")); got.Step != "agent_turn" {
		t.Errorf("runFailureSignature().Step = %q, want agent_turn", got.Step)
	}
}
