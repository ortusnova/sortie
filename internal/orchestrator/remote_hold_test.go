package orchestrator

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/worker"
)

// remoteHoldConfig is the remote kind with homelab, whose jobs need a
// nested worker with 8 CPUs, 20 GiB memory and 200 GiB disk, and sortie,
// the default, whose jobs need little; maxAgents agent slots.
func remoteHoldConfig(t *testing.T, maxAgents int) config.ServiceConfig {
	t.Helper()
	cfg := runConfig(t, map[string]any{"agent": map[string]any{"kind": "kind-a", "command": "a-cmd"}})
	cfg.Agent.MaxConcurrentAgents = maxAgents
	repos := []config.Repository{
		{Name: "homelab", URL: "https://forge.invalid/o/homelab.git", Base: "main", BranchPrefix: "sortie/", Provider: "github", Project: "o/homelab",
			Requires: &config.RemoteRequirements{Capabilities: []string{"nested"}, CPU: 8, MemGiB: 20, DiskGiB: 200}},
		{Name: "sortie", URL: "https://forge.invalid/o/sortie.git", Base: "main", BranchPrefix: "sortie/", Provider: "github", Project: "o/sortie"},
	}
	cfg.Environment = config.EnvironmentConfig{Kind: config.EnvironmentKindRemote, Remote: config.RemoteConfig{
		IdentityHeader: "X-W", Backend: "smolvm", Image: "ghcr.io/ortusnova/agent:1",
		Requires: config.RemoteRequirements{CPU: 2, MemGiB: 4, DiskGiB: 10},
		Flows:    remoteFlowURLs(), LeaseTTLMS: 60000,
		Repositories: repos, Repository: &repos[1], DefaultRepository: "sortie",
	}}
	return cfg
}

func remoteHoldHarness(t *testing.T, maxAgents int, candidates ...domain.Issue) *runHarness {
	t.Helper()
	cfg := remoteHoldConfig(t, maxAgents)
	h := newRunHarness(t, cfg, "kind-a")
	h.state.MaxConcurrentAgents = maxAgents
	h.tracker.candidates = candidates
	h.build()
	return h
}

// The ONS-54 story: homelab work that needs nested virtualization, first
// in dispatch order.
func ons54() domain.Issue {
	issue := candidate("issue-54", "ONS-54", "sortie-repository:homelab")
	one := 1
	issue.Priority = &one
	return issue
}

func ons60() domain.Issue {
	issue := candidate("issue-60", "ONS-60")
	two := 2
	issue.Priority = &two
	return issue
}

func p1Worker() worker.Declaration {
	d := remoteHost()
	d.Nested = false
	return d
}

func taichiWorker(diskGiB float64) worker.Declaration {
	d := remoteHost()
	d.Free.DiskGiB = diskGiB
	return d
}

// A remote job that no worker can take now takes no agent slot: it is
// held with the workers' reasons, the next story takes the slot, and the
// held job is dispatched once a worker that can run it checks in.
func TestHandleTick_RemoteJobWaitsForAWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	h := remoteHoldHarness(t, 1, ons54(), ons60())
	h.o.remote.matcher.CheckIn("p1", p1Worker())
	h.o.remote.matcher.CheckIn("taichi", taichiWorker(92.04))

	h.o.handleTick(ctx)

	got := requireHeld(t, h, "issue-54", SkipNoWorker)
	want := WorkerHold{Capabilities: []string{"nested", "smolvm"},
		Waiting: []string{"worker p1: no nested virtualization", "worker taichi: 92.04 of 200 GiB disk free"}}
	if got.Worker == nil || !reflect.DeepEqual(*got.Worker, want) {
		t.Errorf("State.Held[issue-54].Worker = %+v, want %+v", got.Worker, want)
	}
	if _, running := h.state.Running["issue-54"]; running {
		t.Error("ONS-54 is running, want it held without an agent slot")
	}
	if _, running := h.state.Running["issue-60"]; !running {
		t.Errorf("ONS-60 not running, want it on the one agent slot; held: %+v", h.state.Held)
	}

	delete(h.state.Running, "issue-60")
	delete(h.state.Claimed, "issue-60")
	h.tracker.candidates = []domain.Issue{ons54()}
	h.o.handleTick(ctx)
	requireHeld(t, h, "issue-54", SkipNoWorker)

	h.o.remote.matcher.CheckIn("taichi", taichiWorker(210))
	h.o.handleTick(ctx)

	if _, running := h.state.Running["issue-54"]; !running {
		t.Errorf("ONS-54 not running after taichi checked in with room, held: %+v", h.state.Held["issue-54"])
	}
	if held := h.state.Held["issue-54"]; held != nil {
		t.Errorf("State.Held[issue-54] = %+v after its dispatch, want none", held)
	}
}

// A restarted Sortie, whose matcher knows no worker yet, holds the job
// again until a worker that can run it checks in.
func TestHandleTick_RemoteJobWaitsAcrossARestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	before := remoteHoldHarness(t, 3, ons54())
	before.o.remote.matcher.CheckIn("p1", p1Worker())
	before.o.handleTick(ctx)
	requireHeld(t, before, "issue-54", SkipNoWorker)

	after := remoteHoldHarness(t, 3, ons54())
	after.o.handleTick(ctx)
	got := requireHeld(t, after, "issue-54", SkipNoWorker)
	if got.Worker == nil || !reflect.DeepEqual(got.Worker.Waiting, []string{"no worker has checked in"}) {
		t.Errorf("State.Held[issue-54].Worker after the restart = %+v, want no worker has checked in", got.Worker)
	}
	if len(after.state.Running) != 0 {
		t.Errorf("running after the restart = %d, want 0", len(after.state.Running))
	}

	after.o.remote.matcher.CheckIn("p1", p1Worker())
	after.o.handleTick(ctx)
	requireHeld(t, after, "issue-54", SkipNoWorker)

	after.o.remote.matcher.CheckIn("taichi", taichiWorker(210))
	after.o.handleTick(ctx)
	if _, running := after.state.Running["issue-54"]; !running {
		t.Error("ONS-54 not running once taichi checked in with room")
	}
}

// A retry of a remote job no worker can take now keeps its attempt and
// takes no slot: it is re-armed with the wait, and dispatched once a
// worker can take it.
func TestHandleRetryTimer_WaitsForAWorker(t *testing.T) {
	store := &mockRetryStore{}
	tracker := &mockRetryTracker{fetchedIssue: candidateIssue("ISS-54", "ONS-54", "To Do")}
	state := retryState(t, "ISS-54", "ONS-54", 2)
	params := defaultRetryParams(t, store, tracker)
	params.PollIntervalMS = 60_000
	wait := &WorkerHold{Capabilities: []string{"nested"}, Waiting: []string{"worker p1: no nested virtualization"}}
	params.WorkerWait = func(domain.Issue) *WorkerHold { return wait }
	dispatched := 0
	params.MakeWorkerFn = func(_, _, _, _, _ string, _ domain.AgentAdapter, _ AttemptSettings) WorkerFunc {
		dispatched++
		return func(context.Context, domain.Issue, *int) {}
	}

	HandleRetryTimer(state, "ISS-54", params)

	if _, running := state.Running["ISS-54"]; running || dispatched != 0 {
		t.Fatalf("running = %v, dispatches = %d; want the retry held without a slot", running, dispatched)
	}
	entry := state.RetryAttempts["ISS-54"]
	if entry == nil {
		t.Fatal("RetryAttempts[ISS-54] missing, want it re-armed")
	}
	if entry.TimerHandle != nil {
		entry.TimerHandle.Stop()
	}
	if entry.Attempt != 2 {
		t.Errorf("re-armed Attempt = %d, want 2: a wait for a worker costs no attempt", entry.Attempt)
	}
	if entry.WorkerWait == nil || !reflect.DeepEqual(*entry.WorkerWait, *wait) {
		t.Errorf("re-armed WorkerWait = %+v, want %+v", entry.WorkerWait, wait)
	}

	wait = nil
	entry.scheduledAt = time.Time{} // the re-armed timer fires
	HandleRetryTimer(state, "ISS-54", params)
	if _, running := state.Running["ISS-54"]; !running || dispatched != 1 {
		t.Errorf("running = %v, dispatches = %d once a worker can take it; want 1 dispatch", running, dispatched)
	}
}

// A dispatch pass sends no more jobs than the workers have free slots:
// a job dispatched in the pass but not yet queued counts as the queued
// ones do. Live, 2026-11-03: under a cap of 16, one free slot on p1
// and one on taichi took ONS-101, ONS-105 and ONS-109 in one pass; the
// third sat in the queue as running until the stall timeout ended it.
func TestHandleTick_DispatchesOneJobPerFreeSlot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	h := remoteHoldHarness(t, 16, candidate("issue-101", "ONS-101"), candidate("issue-105", "ONS-105"), candidate("issue-109", "ONS-109"))
	one := remoteHost()
	one.RunSlots, one.Running = 3, 2
	h.o.remote.matcher.CheckIn("p1", one)
	h.o.remote.matcher.CheckIn("taichi", one)

	h.o.handleTick(ctx)

	for _, id := range []string{"issue-101", "issue-105"} {
		if _, running := h.state.Running[id]; !running {
			t.Errorf("%s not running, want it on a free slot; held: %+v", id, h.state.Held[id])
		}
	}
	got := requireHeld(t, h, "issue-109", SkipNoWorker)
	want := []string{"worker p1: all 3 run slots in use", "worker taichi: all 3 run slots in use"}
	if got.Worker == nil || !reflect.DeepEqual(got.Worker.Waiting, want) {
		t.Errorf("State.Held[issue-109].Worker = %+v, want waiting %v", got.Worker, want)
	}
}

// A running entry whose job waits in the matcher's queue holds no agent
// slot and does not stall; once a worker takes the job, stall detection
// measures from then.
func TestRefreshWorkerQueue_AQueuedJobIsNotRunning(t *testing.T) {
	h := remoteHoldHarness(t, 1)
	now := time.Now()
	h.state.Running["issue-101"] = &RunningEntry{Identifier: "ONS-101", Issue: candidate("issue-101", "ONS-101"), StartedAt: now.Add(-time.Hour)}
	h.o.remote.matcher.Submit(worker.Job{ID: "ONS-101", Attempt: 1, Issue: worker.Issue{Identifier: "ONS-101"}})

	h.o.refreshWorkerQueue(now)

	e := h.state.Running["issue-101"]
	if !e.WorkerQueued || AgentsRunning(h.state.Running) != 0 || !HasAvailableSlots(h.state, "To Do") {
		t.Fatalf("queued = %v, agents running = %d; want the queued job to hold no agent slot", e.WorkerQueued, AgentsRunning(h.state.Running))
	}
	reconcileStalled(h.state, ReconcileParams{StallTimeoutMS: 60_000}, discardLogger(), context.Background(), now, nil)
	if _, running := h.state.Running["issue-101"]; !running {
		t.Fatal("the queued job was ended as stalled")
	}

	h.o.remote.matcher.CheckIn("taichi", remoteHost())
	if _, ok, err := h.o.remote.matcher.Poll("taichi"); !ok || err != nil {
		t.Fatalf("Poll() = %v, %v; want ONS-101 offered", ok, err)
	}
	later := now.Add(time.Minute)
	h.o.refreshWorkerQueue(later)
	if e.WorkerQueued || !e.WorkerGrantedAt.Equal(later) || !stallReference(e).Equal(later) || AgentsRunning(h.state.Running) != 1 {
		t.Errorf("after the grant: queued %v, granted at %v, stall reference %v; want measured from %v", e.WorkerQueued, e.WorkerGrantedAt, stallReference(e), later)
	}
}
