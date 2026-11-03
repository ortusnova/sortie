package orchestrator

import (
	"context"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/persistence"
)

type mergedRowStore struct {
	stubStore
	satisfactionStore
}

func newMergedToDoneOrchestrator(t *testing.T, issues []domain.Issue, store OrchestratorStore, configured bool) (*Orchestrator, *mockTrackerAdapter) {
	t.Helper()

	cfg := config.ServiceConfig{
		Tracker: config.TrackerConfig{
			Kind:           "mock",
			ActiveStates:   []string{"To Do", "In Progress"},
			TerminalStates: []string{"Done", "Canceled"},
		},
		Polling:   config.PollingConfig{IntervalMS: 1000},
		Workspace: config.WorkspaceConfig{Root: t.TempDir()},
		Hooks:     config.HooksConfig{TimeoutMS: 5000},
		Agent: config.AgentConfig{
			Kind:                "mock",
			Command:             "/usr/bin/agent",
			MaxConcurrentAgents: 10,
			MaxTurns:            1,
			ReadTimeoutMS:       1000,
		},
	}
	pf := passingPreflightRegistries()
	pf.ReloadWorkflow = func() error { return nil }
	pf.ConfigFunc = func() config.ServiceConfig { return cfg }

	tracker := &mockTrackerAdapter{}
	o := NewOrchestrator(OrchestratorParams{
		State:  NewState(1000, 10, 0, nil, AgentTotals{}),
		Logger: discardLogger(),
		TrackerAdapter: &candidateTrackerAdapter{
			mockTrackerAdapter: tracker,
			fetchCandidatesFn: func(context.Context) ([]domain.Issue, error) {
				return issues, nil
			},
		},
		AgentAdapter:                      &mockAgentAdapter{},
		WorkflowManager:                   &stubWorkflowManager{config: cfg, template: mustParseTemplate(t, "do {{.issue.identifier}}")},
		Store:                             store,
		PreflightParams:                   pf,
		MergeCompletionConfig:             MergeCompletionReactionConfig{TargetState: "Done"},
		MergeCompletionReactionConfigured: configured,
	})
	return o, tracker
}

// TestHandleTick_MergedIssueMovesToDone asserts that the first tick (the
// one a start runs) moves an issue whose pull request already merged to
// the merge-completion target state instead of dispatching it, and still
// dispatches an issue without a merge record.
func TestHandleTick_MergedIssueMovesToDone(t *testing.T) {
	t.Parallel()

	merged := domain.Issue{ID: "M-1", Identifier: "M-1", Title: "merged", State: "In Progress"}
	open := domain.Issue{ID: "O-1", Identifier: "O-1", Title: "open", State: "In Progress"}
	store := &mergedRowStore{}
	_ = store.UpsertPlanSatisfaction(context.Background(), persistence.PlanSatisfaction{
		IssueID: merged.ID, Kind: persistence.PlanSatisfactionMerged, Ref: "abc", PR: "o/r#1",
	})

	o, tracker := newMergedToDoneOrchestrator(t, []domain.Issue{merged, open}, store, true)
	o.handleTick(context.Background())
	o.state.WorkerWg.Wait()

	if len(tracker.transitionCalls) != 1 || tracker.transitionCalls[0] != (transitionIssueCall{IssueID: merged.ID, TargetState: "Done"}) {
		t.Fatalf("transition calls = %+v, want one move of %s to Done", tracker.transitionCalls, merged.ID)
	}
	if _, ok := o.state.Running[merged.ID]; ok {
		t.Errorf("merged issue %s was dispatched", merged.ID)
	}
	if _, ok := o.state.Running[open.ID]; !ok {
		t.Errorf("issue %s without a merge record was not dispatched", open.ID)
	}
}

// TestHandleTick_MergedIssueLeftWithoutMergeCompletion asserts nothing
// moves when merge completion is not configured: there is no done state.
func TestHandleTick_MergedIssueLeftWithoutMergeCompletion(t *testing.T) {
	t.Parallel()

	merged := domain.Issue{ID: "M-1", Identifier: "M-1", Title: "merged", State: "In Progress"}
	store := &mergedRowStore{}
	_ = store.UpsertPlanSatisfaction(context.Background(), persistence.PlanSatisfaction{
		IssueID: merged.ID, Kind: persistence.PlanSatisfactionMerged, Ref: "abc",
	})

	o, tracker := newMergedToDoneOrchestrator(t, []domain.Issue{merged}, store, false)
	o.handleTick(context.Background())
	o.state.WorkerWg.Wait()

	if len(tracker.transitionCalls) != 0 {
		t.Fatalf("transition calls = %+v, want none", tracker.transitionCalls)
	}
}

// TestHandleTick_MergedIssueTransitionFailureHolds asserts a merged issue
// whose move fails is not dispatched either.
func TestHandleTick_MergedIssueTransitionFailureHolds(t *testing.T) {
	t.Parallel()

	merged := domain.Issue{ID: "M-1", Identifier: "M-1", Title: "merged", State: "In Progress"}
	store := &mergedRowStore{}
	_ = store.UpsertPlanSatisfaction(context.Background(), persistence.PlanSatisfaction{
		IssueID: merged.ID, Kind: persistence.PlanSatisfactionMerged, Ref: "abc",
	})

	o, tracker := newMergedToDoneOrchestrator(t, []domain.Issue{merged}, store, true)
	tracker.transitionIssueFn = func(context.Context, string, string) error {
		return &domain.TrackerError{Kind: domain.ErrTrackerTransport, Message: "down"}
	}
	o.handleTick(context.Background())
	o.state.WorkerWg.Wait()

	if _, ok := o.state.Running[merged.ID]; ok {
		t.Errorf("merged issue %s was dispatched after its move failed", merged.ID)
	}
}

// TestHandleRetryTimer_MergedIssueMovesToDone asserts a retry that fires
// for an issue whose pull request already merged moves it to the done
// state and releases its claim instead of dispatching it, and that a
// failed move reschedules the retry without dispatching.
func TestHandleRetryTimer_MergedIssueMovesToDone(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		moveErr error
	}{
		{name: "moved"},
		{name: "move failed", moveErr: &domain.TrackerError{Kind: domain.ErrTrackerTransport, Message: "down"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			const id = "MERGED-RETRY"
			state := NewState(5000, 4, 0, nil, AgentTotals{})
			state.Claimed[id] = struct{}{}
			state.RetryAttempts[id] = &RetryEntry{IssueID: id, Identifier: id, Attempt: 1}

			sat := &satisfactionStore{}
			_ = sat.UpsertPlanSatisfaction(context.Background(), persistence.PlanSatisfaction{
				IssueID: id, Kind: persistence.PlanSatisfactionMerged, Ref: "abc",
			})
			mover := &mockTrackerAdapter{transitionIssueFn: func(context.Context, string, string) error { return tc.moveErr }}

			store := &mockRetryStore{}
			params := defaultRetryParams(t, store, &mockRetryTracker{fetchedIssue: candidateIssue(id, id, "In Progress")})
			params.CompleteMerged = func(issue domain.Issue) (domain.Issue, bool) {
				out := completeMergedCandidates(context.Background(), state, mover, sat, "Done", params.TerminalStates, []domain.Issue{issue}, discardLogger())
				if len(out) == 0 {
					return issue, false
				}
				return out[0], true
			}

			HandleRetryTimer(state, id, params)

			if _, running := state.Running[id]; running {
				t.Fatalf("merged issue %s was dispatched", id)
			}
			if len(mover.transitionCalls) != 1 || mover.transitionCalls[0].TargetState != "Done" {
				t.Fatalf("transition calls = %+v, want one move to Done", mover.transitionCalls)
			}
			_, claimed := state.Claimed[id]
			_, retrying := state.RetryAttempts[id]
			if tc.moveErr == nil && (claimed || retrying) {
				t.Errorf("claimed=%v retrying=%v after the move, want both released", claimed, retrying)
			}
			if tc.moveErr != nil && !retrying {
				t.Errorf("retry dropped after a failed move, want it rescheduled")
			}
		})
	}
}
