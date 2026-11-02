package orchestrator

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/waittest"
)

func TestDispatchPause_RunningRunFinishesAndNothingNewStarts(t *testing.T) {
	t.Parallel()

	const (
		runningID = "run-1"
		waitingID = "wait-1"
		retryID   = "retry-held"
	)

	cfg := defaultWorkerConfig(t.TempDir())
	cfg.Tracker.Kind = "mock"
	cfg.Polling = config.PollingConfig{IntervalMS: int(time.Hour / time.Millisecond)}
	cfg.Agent.MaxConcurrentAgents = 3

	var (
		startedMu sync.Mutex
		started   = map[string]int{} // issue identifier -> sessions started
		runDone   atomic.Bool
		finishRun = make(chan struct{})
	)
	startCount := func(identifier string) int {
		startedMu.Lock()
		defer startedMu.Unlock()
		return started[identifier]
	}
	agent := &mockAgentAdapter{
		startSessionFn: func(_ context.Context, params domain.StartSessionParams) (domain.Session, error) {
			identifier := filepath.Base(params.WorkspacePath)
			startedMu.Lock()
			started[identifier]++
			startedMu.Unlock()
			return domain.Session{ID: "sess-" + identifier}, nil
		},
		runTurnFn: func(ctx context.Context, session domain.Session, _ domain.RunTurnParams) (domain.TurnResult, error) {
			if session.ID == "sess-RUN-1" {
				select {
				case <-finishRun:
					return domain.TurnResult{SessionID: session.ID, ExitReason: domain.EventTurnCompleted}, nil
				case <-ctx.Done():
				}
			} else {
				<-ctx.Done()
			}
			return domain.TurnResult{SessionID: session.ID, ExitReason: domain.EventTurnCancelled}, ctx.Err()
		},
	}

	issues := map[string]domain.Issue{
		runningID: {ID: runningID, Identifier: "RUN-1", Title: "running", State: "To Do"},
		waitingID: {ID: waitingID, Identifier: "WAIT-1", Title: "waiting", State: "To Do"},
		retryID:   {ID: retryID, Identifier: "RETRY-1", Title: "retry", State: "To Do"},
	}
	stateOf := func(id string) string {
		if id == runningID && runDone.Load() {
			return "Done"
		}
		return "To Do"
	}
	var (
		candidatesMu sync.Mutex
		candidates   = []string{runningID}
		polls        atomic.Int64
	)
	setCandidates := func(ids ...string) {
		candidatesMu.Lock()
		defer candidatesMu.Unlock()
		candidates = ids
	}
	tracker := &candidateTrackerAdapter{
		mockTrackerAdapter: &mockTrackerAdapter{
			fetchIssueByIDFn: func(_ context.Context, id string) (domain.Issue, error) {
				issue := issues[id]
				issue.State = stateOf(id)
				return issue, nil
			},
			fetchStatesFn: func(_ context.Context, ids []string) (map[string]string, error) {
				result := make(map[string]string, len(ids))
				for _, id := range ids {
					result[id] = stateOf(id)
				}
				return result, nil
			},
		},
		fetchCandidatesFn: func(context.Context) ([]domain.Issue, error) {
			defer polls.Add(1)
			candidatesMu.Lock()
			defer candidatesMu.Unlock()
			out := make([]domain.Issue, 0, len(candidates))
			for _, id := range candidates {
				out = append(out, issues[id])
			}
			return out, nil
		},
	}

	pf := passingPreflightRegistries()
	pf.ReloadWorkflow = func() error { return nil }
	pf.ConfigFunc = func() config.ServiceConfig { return cfg }

	state := NewState(cfg.Polling.IntervalMS, cfg.Agent.MaxConcurrentAgents, 0, nil, AgentTotals{})
	o := NewOrchestrator(OrchestratorParams{
		State:           state,
		Logger:          discardLogger(),
		TrackerAdapter:  tracker,
		AgentAdapter:    agent,
		WorkflowManager: &stubWorkflowManager{config: cfg, template: mustParseTemplate(t, "do {{ .issue.identifier }}")},
		Store:           &stubStore{},
		PreflightParams: pf,
	})
	state.Claimed[retryID] = struct{}{}
	ScheduleRetry(state, ScheduleRetryParams{
		IssueID:    retryID,
		Identifier: "RETRY-1",
		Attempt:    1,
		DelayMS:    time.Hour.Milliseconds(),
		Error:      "worker exited: boom",
	}, o.onRetryFire)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		o.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	snapshot := o.SnapshotFunc()
	running := func() []string {
		snap, err := snapshot()
		if err != nil {
			t.Fatalf("snapshot(): %v", err)
		}
		ids := make([]string, 0, len(snap.Running))
		for _, r := range snap.Running {
			ids = append(ids, r.IssueID)
		}
		slices.Sort(ids)
		return ids
	}
	sessionStarted := func(identifier string, want int) func() error {
		return func() error {
			if got := startCount(identifier); got < want {
				return fmt.Errorf("%s sessions = %d, want %d", identifier, got, want)
			}
			return nil
		}
	}
	// tick asks for a poll and returns once the loop has run it.
	tick := func() {
		t.Helper()
		before := polls.Load()
		o.RefreshFunc()()
		waittest.Until(t, func() error {
			if polls.Load() <= before {
				return fmt.Errorf("no poll since the refresh")
			}
			return nil
		})
	}
	pauseFn := o.DispatchPauseFunc()

	waittest.Until(t, sessionStarted("RUN-1", 1))

	first := pauseFn(ctx, true, "stage0-deploy", "kit change")
	if first.Status != DispatchPauseDone || first.Pause == nil || first.Pause.By != "stage0-deploy" || first.Pause.Reason != "kit change" {
		t.Fatalf("pause = %+v, want done with by stage0-deploy and reason kit change", first)
	}
	again := pauseFn(ctx, true, "someone else", "")
	if again.Status != DispatchPauseDone || again.Pause == nil || *again.Pause != *first.Pause {
		t.Fatalf("second pause = %+v, want the first pause %+v kept", again, *first.Pause)
	}
	if snap, _ := snapshot(); snap.DispatchPause == nil || *snap.DispatchPause != *first.Pause {
		t.Fatalf("snapshot DispatchPause = %v, want %+v", snap.DispatchPause, *first.Pause)
	}

	// A new candidate is not dispatched.
	setCandidates(runningID, waitingID)
	tick()
	if got := running(); !slices.Equal(got, []string{runningID}) {
		t.Fatalf("running while paused = %v, want only %s", got, runningID)
	}

	// A retry that comes due is held, not dispatched.
	beforeDue := time.Now().UnixMilli()
	if result := o.RetryNowFunc()(ctx, "RETRY-1"); result.Status != RetryNowDue {
		t.Fatalf("RetryNowFunc() = %+v, want due", result)
	}
	waittest.Until(t, func() error {
		row := snapshotRetryRow(t, snapshot, retryID)
		if row.DueAtMS < beforeDue+time.Minute.Milliseconds() {
			return fmt.Errorf("retry due at %d, want it held for a poll interval", row.DueAtMS)
		}
		return nil
	})

	// The running run finishes; nothing takes its place.
	setCandidates(waitingID)
	runDone.Store(true)
	close(finishRun)
	waittest.Until(t, func() error {
		if got := running(); len(got) != 0 {
			return fmt.Errorf("running = %v, want none", got)
		}
		return nil
	})
	tick()
	if got := running(); len(got) != 0 {
		t.Fatalf("running after the run finished while paused = %v, want none", got)
	}
	for _, identifier := range []string{"WAIT-1", "RETRY-1"} {
		if n := startCount(identifier); n != 0 {
			t.Errorf("%s sessions while paused = %d, want 0", identifier, n)
		}
	}

	// Resume dispatches the waiting candidate and the held retry.
	resumed := pauseFn(ctx, false, "", "")
	if resumed.Status != DispatchPauseDone || resumed.Pause != nil {
		t.Fatalf("resume = %+v, want done with no pause", resumed)
	}
	waittest.Until(t, sessionStarted("WAIT-1", 1))
	waittest.Until(t, sessionStarted("RETRY-1", 1))
	if snap, _ := snapshot(); snap.DispatchPause != nil {
		t.Errorf("snapshot DispatchPause after resume = %+v, want nil", *snap.DispatchPause)
	}

	if again := pauseFn(ctx, false, "", ""); again.Status != DispatchPauseDone || again.Pause != nil {
		t.Errorf("resume when not paused = %+v, want done with no pause", again)
	}
}
