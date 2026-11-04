package orchestrator

import (
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/worker"
)

const (
	toolCallTestStallMS    = 15 * 60 * 1000
	toolCallTestToolCallMS = 90 * 60 * 1000
)

// toolCallRun is one running run driven by agent events at instants of a
// fake clock, then reconciled at a later instant.
type toolCallRun struct {
	t      *testing.T
	state  *State
	cancel *cancelCounter
	start  time.Time
}

func newToolCallRun(t *testing.T) *toolCallRun {
	t.Helper()
	state := NewState(5000, 4, 0, nil, AgentTotals{})
	state.StallTimeoutMS = toolCallTestStallMS
	state.ToolCallTimeoutMS = toolCallTestToolCallMS
	cc := &cancelCounter{}
	start := reconcileBaseTime
	state.Running["ISSUE-1"] = &RunningEntry{
		Identifier: "ISSUE-1-ident",
		StartedAt:  start,
		CancelFunc: cc.cancel,
		Issue:      domain.Issue{ID: "ISSUE-1", State: "In Progress"},
	}
	state.Claimed["ISSUE-1"] = struct{}{}
	return &toolCallRun{t: t, state: state, cancel: cc, start: start}
}

// event delivers an agent event of type typ at start plus at.
func (r *toolCallRun) event(at time.Duration, typ domain.AgentEventType, callID string) {
	HandleAgentEvent(r.state, "ISSUE-1", domain.AgentEvent{
		Type: typ, Timestamp: r.start.Add(at), ToolCallID: callID,
		ToolName: "execute", ToolKind: domain.ToolKindShell, ToolArgument: "make seed",
	}, discardLogger(), nil)
}

// reconcile runs one poll tick's reconciliation at start plus at and
// reports whether it stopped the run as stalled.
func (r *toolCallRun) reconcile(at time.Duration) bool {
	r.t.Helper()
	store := &mockReconcileStore{}
	tracker := &mockReconcileTracker{states: map[string]string{"ISSUE-1": "In Progress"}}
	params := defaultReconcileParams(r.t, store, tracker)
	params.StallTimeoutMS = toolCallTestStallMS
	params.ToolCallTimeoutMS = toolCallTestToolCallMS
	now := r.start.Add(at)
	params.NowFunc = func() time.Time { return now }
	before := r.cancel.count
	ReconcileRunningIssues(r.state, params)
	_, retried := r.state.RetryAttempts["ISSUE-1"]
	stalled := r.cancel.count > before
	if stalled != retried {
		r.t.Fatalf("at %v: cancelled = %v but stall retry scheduled = %v", at, stalled, retried)
	}
	return stalled
}

func TestReconcileStalled_ToolCall(t *testing.T) {
	t.Parallel()

	t.Run("a tool call running 40 minutes is not stalled", func(t *testing.T) {
		t.Parallel()
		r := newToolCallRun(t)
		r.event(time.Minute, domain.EventSessionStarted, "")
		r.event(2*time.Minute, domain.EventToolStarted, "call-1")

		if r.reconcile(42 * time.Minute) {
			t.Error("run stalled 40 minutes into a tool call, want it running under agent.tool_call_timeout_ms")
		}
	})

	t.Run("no events and no tool call for 16 minutes is stalled", func(t *testing.T) {
		t.Parallel()
		r := newToolCallRun(t)
		r.event(time.Minute, domain.EventSessionStarted, "")

		if !r.reconcile(17 * time.Minute) {
			t.Error("run silent for 16 minutes outside any tool call not stalled, want it stopped under agent.stall_timeout_ms")
		}
	})

	t.Run("a tool call that ended leaves the stall rule unchanged", func(t *testing.T) {
		t.Parallel()
		r := newToolCallRun(t)
		r.event(time.Minute, domain.EventToolStarted, "call-1")
		r.event(3*time.Minute, domain.EventToolResult, "call-1")

		if !r.reconcile(19 * time.Minute) {
			t.Error("run silent for 16 minutes after its tool call ended not stalled, want the 15-minute rule")
		}
	})

	t.Run("a tool call longer than the bound is stalled", func(t *testing.T) {
		t.Parallel()
		r := newToolCallRun(t)
		r.event(time.Minute, domain.EventToolStarted, "call-1")

		if r.reconcile(90 * time.Minute) {
			t.Fatal("run stalled 89 minutes into a tool call, want it running until the 90-minute bound")
		}
		if !r.reconcile(92 * time.Minute) {
			t.Error("run silent for 91 minutes in one tool call not stalled, want it stopped at agent.tool_call_timeout_ms")
		}
	})

	t.Run("a worker that stopped heartbeating takes the stall rule", func(t *testing.T) {
		t.Parallel()
		r := newToolCallRun(t)
		live := true
		r.state.WorkerLive = func(string, time.Time) bool { return live }
		r.event(time.Minute, domain.EventToolStarted, "call-1")

		if r.reconcile(20 * time.Minute) {
			t.Fatal("run stalled while its worker's lease is current, want the tool-call bound")
		}
		live = false
		if !r.reconcile(20 * time.Minute) {
			t.Error("run inside a tool call whose worker lease lapsed not stalled, want the 15-minute rule")
		}
	})

	t.Run("a turn's end ends its open tool calls", func(t *testing.T) {
		t.Parallel()
		r := newToolCallRun(t)
		r.event(time.Minute, domain.EventToolStarted, "call-1")
		r.event(2*time.Minute, domain.EventTurnCompleted, "")

		if len(r.state.Running["ISSUE-1"].OpenToolCalls) != 0 {
			t.Fatalf("OpenToolCalls = %v after turn_completed, want none", r.state.Running["ISSUE-1"].OpenToolCalls)
		}
		if !r.reconcile(18 * time.Minute) {
			t.Error("run silent for 16 minutes after its turn ended not stalled, want the 15-minute rule")
		}
	})

	t.Run("a tool-call bound no longer than the stall timeout changes nothing", func(t *testing.T) {
		t.Parallel()
		r := newToolCallRun(t)
		r.event(time.Minute, domain.EventToolStarted, "call-1")
		store := &mockReconcileStore{}
		params := defaultReconcileParams(t, store, &mockReconcileTracker{states: map[string]string{"ISSUE-1": "In Progress"}})
		params.StallTimeoutMS = toolCallTestStallMS
		params.ToolCallTimeoutMS = 0
		params.NowFunc = func() time.Time { return r.start.Add(17 * time.Minute) }
		ReconcileRunningIssues(r.state, params)
		if r.cancel.count != 1 {
			t.Errorf("CancelFunc called %d times with agent.tool_call_timeout_ms 0, want 1", r.cancel.count)
		}
	})
}

func TestRuntimeSnapshot_ToolCallMovesStallAt(t *testing.T) {
	t.Parallel()

	r := newToolCallRun(t)
	r.event(time.Minute, domain.EventToolStarted, "call-2")
	r.event(2*time.Minute, domain.EventToolStarted, "call-1")
	now := r.start.Add(30 * time.Minute)

	row := RuntimeSnapshot(r.state, now).Running[0]
	if want := r.start.Add(time.Minute); !row.InToolCallSince.Equal(want) {
		t.Errorf("InToolCallSince = %v, want %v, the earliest open call's start", row.InToolCallSince, want)
	}
	if want := r.start.Add(2*time.Minute + 90*time.Minute); !row.StallAt.Equal(want) || !row.StallToolCallBound {
		t.Errorf("StallAt = %v, StallToolCallBound = %v, want %v under the tool-call bound", row.StallAt, row.StallToolCallBound, want)
	}
	if want := r.start.Add(2*time.Minute + 45*time.Minute); !row.QuietAt.Equal(want) {
		t.Errorf("QuietAt = %v, want %v", row.QuietAt, want)
	}

	r.state.WorkerLive = func(string, time.Time) bool { return false }
	row = RuntimeSnapshot(r.state, now).Running[0]
	if want := r.start.Add(2*time.Minute + 15*time.Minute); !row.StallAt.Equal(want) || row.StallToolCallBound {
		t.Errorf("with the worker lease lapsed StallAt = %v, StallToolCallBound = %v, want %v under the stall timeout", row.StallAt, row.StallToolCallBound, want)
	}

	r.state.WorkerLive = nil
	r.event(3*time.Minute, domain.EventToolResult, "call-2")
	r.event(4*time.Minute, domain.EventToolResult, "call-1")
	row = RuntimeSnapshot(r.state, now).Running[0]
	if !row.InToolCallSince.IsZero() || row.StallToolCallBound {
		t.Errorf("after both calls ended InToolCallSince = %v, StallToolCallBound = %v, want zero and false", row.InToolCallSince, row.StallToolCallBound)
	}
	if want := r.start.Add(4*time.Minute + 15*time.Minute); !row.StallAt.Equal(want) {
		t.Errorf("after both calls ended StallAt = %v, want %v", row.StallAt, want)
	}
}

func TestRemoteLeaseCurrent(t *testing.T) {
	t.Parallel()

	clock := reconcileBaseTime
	rw := newRemoteWorkers(func() time.Time { return clock })
	rw.leaser.SetTTL(time.Minute)
	rw.track(&remoteAttempt{issueID: "ISSUE-1", job: worker.Job{ID: "ONS-19", Attempt: 1}})

	if rw.leaseCurrent("ISSUE-1", clock) {
		t.Fatal("leaseCurrent before any worker took the job = true, want false")
	}
	if _, err := rw.leaser.Acquire("ONS-19", "taichi", clock); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if !rw.leaseCurrent("ISSUE-1", clock.Add(30*time.Second)) {
		t.Error("leaseCurrent within the lease = false, want true")
	}
	if _, err := rw.leaser.Renew("ONS-19", "taichi", clock.Add(50*time.Second)); err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	if !rw.leaseCurrent("ISSUE-1", clock.Add(100*time.Second)) {
		t.Error("leaseCurrent after a heartbeat renewed the lease = false, want true")
	}
	if rw.leaseCurrent("ISSUE-1", clock.Add(111*time.Second)) {
		t.Error("leaseCurrent after the heartbeats stopped and the lease ran out = true, want false")
	}
	if rw.leaseCurrent("ISSUE-2", clock) {
		t.Error("leaseCurrent of an issue with no attempt = true, want false")
	}
}
