package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
)

func heldTestState() *State {
	state := NewState(5000, 4, 0, nil, AgentTotals{})
	state.Running["id-run"] = &RunningEntry{Identifier: "LIVE-1", StartedAt: reconcileBaseTime, TurnClock: newTurnClock(), RuleName: "implement"}
	state.RetryAttempts["id-retry"] = &RetryEntry{IssueID: "id-retry", Identifier: "LIVE-2", Attempt: 1}
	return state
}

func TestApplyHold(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		identifier string
		want       ControlOutcome
		wantHeld   bool
	}{
		{"running issue", "LIVE-1", ControlDone, true},
		{"known issue that is not running, with no evidence", "LIVE-2", ControlInvalid, false},
		{"unknown issue", "NOPE-9", ControlUnknownIssue, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			state := heldTestState()
			reply := applyHold(context.Background(), state, nil, tt.identifier, HoldRequest{Reason: "human takeover", Holder: "acp-broker"}, reconcileBaseTime, discardLogger())
			if reply.Outcome != tt.want {
				t.Fatalf("applyHold(%s) outcome = %v, want %v", tt.identifier, reply.Outcome, tt.want)
			}
			if got := !state.Running["id-run"].HeldSince.IsZero(); got != tt.wantHeld {
				t.Errorf("LIVE-1 held = %v, want %v", got, tt.wantHeld)
			}
			if tt.wantHeld {
				h := reply.Hold
				if !h.Held || h.Reason != "human takeover" || h.Holder != "acp-broker" || !h.HeldSince.Equal(reconcileBaseTime) || h.Identifier != "LIVE-1" {
					t.Errorf("hold state = %+v, want held by acp-broker for human takeover since %v", h, reconcileBaseTime)
				}
			}
		})
	}
}

func TestApplyHold_TwiceKeepsStart(t *testing.T) {
	t.Parallel()

	state := heldTestState()
	applyHold(context.Background(), state, nil, "LIVE-1", HoldRequest{Reason: "first", Holder: "a"}, reconcileBaseTime, discardLogger())
	reply := applyHold(context.Background(), state, nil, "LIVE-1", HoldRequest{Reason: "second", Holder: "b"}, reconcileBaseTime.Add(5*time.Minute), discardLogger())

	if reply.Outcome != ControlDone || !reply.Hold.HeldSince.Equal(reconcileBaseTime) || reply.Hold.Reason != "first" {
		t.Errorf("second hold = %+v, want ControlDone keeping start %v and reason %q", reply, reconcileBaseTime, "first")
	}
}

func TestApplyRelease(t *testing.T) {
	t.Parallel()

	t.Run("release after a hold adds the held time", func(t *testing.T) {
		t.Parallel()
		state := heldTestState()
		spy := &spyMetrics{}
		applyHold(context.Background(), state, nil, "LIVE-1", HoldRequest{Reason: "human takeover", Holder: "acp-broker"}, reconcileBaseTime, discardLogger())
		releaseAt := reconcileBaseTime.Add(90 * time.Second)

		reply := applyRelease(context.Background(), state, nil, "LIVE-1", releaseAt, spy, discardLogger())

		entry := state.Running["id-run"]
		if reply.Outcome != ControlDone || reply.Hold.Held || !entry.HeldSince.IsZero() {
			t.Fatalf("release = %+v, entry HeldSince %v, want the run no longer held", reply, entry.HeldSince)
		}
		if entry.HeldTotal < 90*time.Second || reply.Hold.HeldTotal < 90*time.Second {
			t.Errorf("held total = %v / %v, want at least 90s", entry.HeldTotal, reply.Hold.HeldTotal)
		}
		if !entry.HoldReleasedAt.Equal(releaseAt) {
			t.Errorf("HoldReleasedAt = %v, want %v", entry.HoldReleasedAt, releaseAt)
		}
		if len(spy.holdSeconds) != 1 || spy.holdSeconds[0].rule != "implement" || spy.holdSeconds[0].seconds != 90 {
			t.Errorf("hold seconds recorded = %+v, want one 90s record for rule implement", spy.holdSeconds)
		}
	})

	t.Run("release of a run that is not held changes nothing", func(t *testing.T) {
		t.Parallel()
		state := heldTestState()
		spy := &spyMetrics{}
		reply := applyRelease(context.Background(), state, nil, "LIVE-1", reconcileBaseTime, spy, discardLogger())
		entry := state.Running["id-run"]
		if reply.Outcome != ControlDone || entry.HeldTotal != 0 || !entry.HoldReleasedAt.IsZero() || len(spy.holdSeconds) != 0 {
			t.Errorf("release of an unheld run = %+v, entry %+v, metrics %+v, want no change", reply, entry, spy.holdSeconds)
		}
	})

	t.Run("not running and unknown", func(t *testing.T) {
		t.Parallel()
		state := heldTestState()
		if got := applyRelease(context.Background(), state, nil, "LIVE-2", reconcileBaseTime, nil, discardLogger()).Outcome; got != ControlNotRunning {
			t.Errorf("release(LIVE-2) = %v, want ControlNotRunning", got)
		}
		if got := applyRelease(context.Background(), state, nil, "NOPE-9", reconcileBaseTime, nil, discardLogger()).Outcome; got != ControlUnknownIssue {
			t.Errorf("release(NOPE-9) = %v, want ControlUnknownIssue", got)
		}
	})
}

func TestReconcileStalled_HeldRunIsNotStopped(t *testing.T) {
	t.Parallel()

	params := defaultReconcileParams(t, &mockReconcileStore{}, &mockReconcileTracker{states: map[string]string{"ISSUE-1": "In Progress"}})
	params.StallTimeoutMS = 300_000

	state := NewState(5000, 4, 0, nil, AgentTotals{})
	cc := &cancelCounter{}
	state.Running["ISSUE-1"] = &RunningEntry{
		Identifier:         "ISSUE-1-ident",
		StartedAt:          reconcileBaseTime.Add(-30 * time.Minute),
		LastAgentTimestamp: reconcileBaseTime.Add(-20 * time.Minute),
		HeldSince:          reconcileBaseTime.Add(-20 * time.Minute),
		CancelFunc:         cc.cancel,
		Issue:              domain.Issue{State: "In Progress"},
	}
	state.Claimed["ISSUE-1"] = struct{}{}

	ReconcileRunningIssues(state, params)

	if cc.count != 0 {
		t.Errorf("held run cancelled %d times after 20 minutes of silence, want 0", cc.count)
	}
	if _, ok := state.RetryAttempts["ISSUE-1"]; ok {
		t.Error("stall retry scheduled for a held run")
	}
}

func TestReconcileStalled_ReleaseRestartsStallClock(t *testing.T) {
	t.Parallel()

	const timeout = 60 * time.Second
	releasedAt := reconcileBaseTime
	tests := []struct {
		name     string
		now      time.Time
		wantStop bool
	}{
		{"just after release", releasedAt.Add(time.Second), false},
		{"exactly release plus the timeout", releasedAt.Add(timeout), false},
		{"past release plus the timeout", releasedAt.Add(timeout + time.Second), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			params := defaultReconcileParams(t, &mockReconcileStore{}, &mockReconcileTracker{states: map[string]string{"ISSUE-1": "In Progress"}})
			params.StallTimeoutMS = int(timeout.Milliseconds())
			params.NowFunc = func() time.Time { return tt.now }

			state := NewState(5000, 4, 0, nil, AgentTotals{})
			cc := &cancelCounter{}
			state.Running["ISSUE-1"] = &RunningEntry{
				Identifier:         "ISSUE-1-ident",
				StartedAt:          releasedAt.Add(-30 * time.Minute),
				LastAgentTimestamp: releasedAt.Add(-25 * time.Minute),
				HoldReleasedAt:     releasedAt,
				CancelFunc:         cc.cancel,
				Issue:              domain.Issue{State: "In Progress"},
			}
			state.Claimed["ISSUE-1"] = struct{}{}

			ReconcileRunningIssues(state, params)

			if got := cc.count == 1; got != tt.wantStop {
				t.Errorf("stopped = %v at %v after a release at %v, want %v", got, tt.now, releasedAt, tt.wantStop)
			}
		})
	}
}

func TestReconcileTrackerState_HeldTerminalIssueIsStoppedAndHoldEnds(t *testing.T) {
	t.Parallel()

	spy := &spyMetrics{}
	params := defaultReconcileParams(t, &mockReconcileStore{}, &mockReconcileTracker{states: map[string]string{"ISSUE-1": "Done"}})
	params.StallTimeoutMS = 0
	params.Metrics = spy

	state := NewState(5000, 4, 0, nil, AgentTotals{})
	cc := &cancelCounter{}
	state.Running["ISSUE-1"] = &RunningEntry{
		Identifier: "ISSUE-1-ident",
		StartedAt:  reconcileBaseTime.Add(-10 * time.Minute),
		HeldSince:  reconcileBaseTime.Add(-5 * time.Minute),
		RuleName:   "implement",
		TurnClock:  newTurnClock(),
		CancelFunc: cc.cancel,
		Issue:      domain.Issue{State: "In Progress"},
	}
	state.Claimed["ISSUE-1"] = struct{}{}

	ReconcileRunningIssues(state, params)

	entry := state.Running["ISSUE-1"]
	if cc.count != 1 || !entry.PendingCleanup {
		t.Errorf("held terminal issue: cancels = %d, PendingCleanup = %v, want stopped for cleanup", cc.count, entry.PendingCleanup)
	}
	if !entry.HeldSince.IsZero() || entry.HeldTotal < 5*time.Minute {
		t.Errorf("hold after the stop: HeldSince %v, HeldTotal %v, want ended with at least 5m recorded", entry.HeldSince, entry.HeldTotal)
	}
	if len(spy.holdSeconds) != 1 || spy.holdSeconds[0].rule != "implement" {
		t.Errorf("hold seconds recorded = %+v, want one record for rule implement", spy.holdSeconds)
	}
}

func TestEnforceInFlightTokenCeiling_StopsHeldRun(t *testing.T) {
	t.Parallel()

	state := NewState(5000, 4, 100, nil, AgentTotals{})
	var cancelled int
	state.Running["ISS-HELD"] = &RunningEntry{
		Identifier:        "ISS-HELD-ident",
		HeldSince:         time.Now(),
		CeilingCancelFunc: func() { cancelled++ },
	}
	store := &fakeTokenStore{responses: []tokenStoreResponse{{}}}

	driveEvent(state, "ISS-HELD", domain.TokenUsage{InputTokens: 150, TotalTokens: 150}, store, &spyMetrics{}, discardLogger())

	if cancelled != 1 || state.Running["ISS-HELD"].CeilingStopRequest == nil {
		t.Errorf("held run over the token ceiling: cancels = %d, stop request %v, want stopped", cancelled, state.Running["ISS-HELD"].CeilingStopRequest)
	}
}

func TestHandleControl_ThroughTheEventLoop(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	h := newRunHarness(t, runConfig(t, stagedRulesRaw()), "kind-a", "kind-b", "kind-c")
	h.recorders["kind-a"].adapter.runTurnFn = func(ctx context.Context, session domain.Session, params domain.RunTurnParams) (domain.TurnResult, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return domain.TurnResult{SessionID: session.ID, ExitReason: domain.EventTurnCompleted}, nil
	}
	h.tracker.candidates = []domain.Issue{candidate("id-1", "S-1")}
	h.run()
	h.waitForSessions(1)

	hold, rel := h.o.HoldFunc(), h.o.ReleaseFunc()
	first := hold("S-1", HoldRequest{Reason: "human takeover", Holder: "acp-broker"})
	if first.Outcome != ControlDone || !first.Hold.Held {
		t.Fatalf("hold(S-1) = %+v, want held", first)
	}
	snap, err := h.o.SnapshotFunc()()
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Running) != 1 || !snap.Running[0].Held || snap.Running[0].HoldReason != "human takeover" {
		t.Errorf("snapshot running = %+v, want S-1 held for human takeover", snap.Running)
	}
	time.Sleep(20 * time.Millisecond)
	if got := hold("NOPE-1", HoldRequest{}).Outcome; got != ControlUnknownIssue {
		t.Errorf("hold(NOPE-1) = %v, want ControlUnknownIssue", got)
	}
	if got := rel("S-1"); got.Outcome != ControlDone || got.Hold.Held {
		t.Errorf("release(S-1) = %+v, want released", got)
	}
	close(release)
	row := h.waitForRunHistory("id-1")
	h.stop()
	if row.HeldMS <= 0 {
		t.Errorf("run history held_ms = %d, want the hold recorded", row.HeldMS)
	}
}

func TestHandleWorkerExit_EndsOpenHoldAndRecordsHeldTime(t *testing.T) {
	t.Parallel()

	h := newRunHarness(t, runConfig(t, stagedRulesRaw()), "kind-a", "kind-b", "kind-c")
	held := make(chan struct{})
	h.recorders["kind-a"].adapter.runTurnFn = func(ctx context.Context, session domain.Session, params domain.RunTurnParams) (domain.TurnResult, error) {
		<-held
		time.Sleep(50 * time.Millisecond)
		return domain.TurnResult{SessionID: session.ID, ExitReason: domain.EventTurnCompleted}, nil
	}
	h.tracker.candidates = []domain.Issue{candidate("id-1", "S-1")}
	h.run()
	h.waitForSessions(1)
	if got := h.o.HoldFunc()("S-1", HoldRequest{Reason: "takeover", Holder: "test"}); got.Outcome != ControlDone {
		t.Fatalf("hold = %+v", got)
	}
	close(held)
	row := h.waitForRunHistory("id-1")
	snap, err := h.o.SnapshotFunc()()
	h.stop()
	if err != nil {
		t.Fatal(err)
	}
	if row.HeldMS < 50 {
		t.Errorf("run history held_ms = %d, want at least 50 for a run that exited while held", row.HeldMS)
	}
	for _, r := range snap.Running {
		if r.Identifier == "S-1" && r.Held {
			t.Error("S-1 still shown as held after its worker exited")
		}
	}
}
