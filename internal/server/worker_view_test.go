package server

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/hostview"
	"github.com/sortie-ai/sortie/internal/orchestrator"
	"github.com/sortie-ai/sortie/internal/worker"
)

var viewNow = time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)

func viewServer(t *testing.T, p Params) *httptest.Server {
	t.Helper()
	p.RefreshFn = acceptingRefresh()
	p.Logger = slog.New(slog.DiscardHandler)
	if p.WorkersFn != nil {
		p.WorkerIdentityHeaderFn = func() string { return "X-Worker" }
	}
	ts := httptest.NewServer(New(p).httpServer.Handler)
	t.Cleanup(ts.Close)
	return ts
}

func rows(t *testing.T, body map[string]any, member string) []map[string]any {
	t.Helper()
	list, ok := body[member].([]any)
	if !ok {
		t.Fatalf("%s = %v, want a list", member, body[member])
	}
	out := make([]map[string]any, len(list))
	for i, v := range list {
		out[i] = v.(map[string]any)
	}
	return out
}

func running(identifier, rule string) orchestrator.SnapshotRunningEntry {
	return orchestrator.SnapshotRunningEntry{IssueID: "id-" + identifier, Identifier: identifier, RuleName: rule,
		StartedAt: viewNow.Add(-10 * time.Minute)}
}

// Under remote, a running job names the worker that holds its lease and
// the step its worker's heartbeat carries; the worker's check-in carries
// what its jobs use and the machines no Sortie started; and a job no
// worker can take waits for a capability.
func TestStateServesTheRemoteJobsWorkerAndStep(t *testing.T) {
	t.Parallel()

	m := worker.NewMatcher(worker.NewTTLLeaser(time.Minute), func() time.Time { return viewNow }, nil)
	snap := orchestrator.RuntimeSnapshotResult{GeneratedAt: viewNow, Running: []orchestrator.SnapshotRunningEntry{running("ONS-34", "proof")}}
	ts := viewServer(t, Params{
		SnapshotFn: fixedSnapshot(snap),
		WorkersFn:  func() *worker.Matcher { return m },
		HostFn: func(context.Context) *hostview.Host {
			t.Error("the host was read under remote")
			return &hostview.Host{Name: "p1"}
		},
	})
	m.Submit(worker.Job{ID: "ONS-34", Attempt: 1, Issue: worker.Issue{Identifier: "ONS-34"}})
	m.Submit(worker.Job{ID: "ONS-35", Attempt: 1, Issue: worker.Issue{Identifier: "ONS-35"},
		Requires: worker.Requires{Capabilities: []string{"gpu"}}, QueuedAt: viewNow.Add(-time.Minute)})
	checkIn := `{"os":"linux","arch":"x86_64","kvm":true,"nested":true,"backends":["smolvm"],"run_slots":2,"running":0,` +
		`"free":{"cpu":16,"mem_gib":52,"disk_gib":210},"used":{"cpu":4,"mem_gib":8,"disk_gib":20},` +
		`"unmanaged":[{"name":"sandbox-brian","since":"2026-11-02T07:00:00Z","state":"running"}]}`
	for _, step := range []struct{ path, body string }{
		{"/api/v1/workers/checkin", checkIn},
		{"/api/v1/workers/poll", ""},
		{"/api/v1/workers/jobs/ONS-34/attempts/1/heartbeat", `{"step":"seed-vm-recreate","phase":"live_proof","step_at":"2026-11-02T08:58:00Z"}`},
	} {
		if got := workerPost(t, ts, step.path, step.body); got != http.StatusOK {
			t.Fatalf("%s = %d", step.path, got)
		}
	}

	body := getJSONMap(t, ts.URL+"/api/v1/state")
	if _, ok := body["host"]; ok {
		t.Errorf("host served under remote: %v", body["host"])
	}
	r := rows(t, body, "running")[0]
	if r["worker"] != "taichi" || r["phase"] != "live_proof" || r["step"] != "seed-vm-recreate" || r["step_at"] != "2026-11-02T08:58:00Z" {
		t.Errorf("running row = worker %v phase %v step %v step_at %v", r["worker"], r["phase"], r["step"], r["step_at"])
	}
	decl := body["workers"].(map[string]any)["workers"].([]any)[0].(map[string]any)["declaration"].(map[string]any)
	if !reflect.DeepEqual(decl["used"], map[string]any{"cpu": 4.0, "mem_gib": 8.0, "disk_gib": 20.0}) {
		t.Errorf("declaration.used = %v", decl["used"])
	}
	if !reflect.DeepEqual(decl["unmanaged"], []any{map[string]any{"name": "sandbox-brian", "since": "2026-11-02T07:00:00Z", "state": "running"}}) {
		t.Errorf("declaration.unmanaged = %v", decl["unmanaged"])
	}
	waits := rows(t, body, "waits")
	if len(waits) != 1 || waits[0]["issue_identifier"] != "ONS-35" || waits[0]["kind"] != "capability" ||
		waits[0]["worker"] != nil || waits[0]["since"] != "2026-11-02T08:59:00Z" || waits[0]["release"] != "a worker checks in that has gpu" {
		t.Errorf("waits = %v", waits)
	}
}

// Under remote, a job queued for a worker's slot is not running: it is
// a wait, with why it waits, and counts.running leaves it out; a run
// still preparing its job is running on no worker, never on Sortie's
// host (live, 2026-11-03: ONS-101, ONS-105 and ONS-109 read as running
// on p1 while every worker's slots were in use).
func TestStateServesAQueuedJobAsAWaitNotARun(t *testing.T) {
	t.Parallel()

	m := worker.NewMatcher(worker.NewTTLLeaser(time.Minute), func() time.Time { return viewNow }, nil)
	snap := orchestrator.RuntimeSnapshotResult{GeneratedAt: viewNow, Running: []orchestrator.SnapshotRunningEntry{
		running("ONS-34", "build"), running("ONS-101", "build"), running("ONS-110", "build")}}
	ts := viewServer(t, Params{
		SnapshotFn: fixedSnapshot(snap),
		WorkersFn:  func() *worker.Matcher { return m },
		HostFn:     func(context.Context) *hostview.Host { return &hostview.Host{Name: "p1"} },
	})
	m.Submit(worker.Job{ID: "ONS-34", Attempt: 1, Issue: worker.Issue{Identifier: "ONS-34"}})
	checkIn := `{"os":"linux","arch":"x86_64","kvm":true,"nested":true,"backends":["smolvm"],"run_slots":1,"running":0,` +
		`"free":{"cpu":16,"mem_gib":52,"disk_gib":210}}`
	for _, path := range []string{"/api/v1/workers/checkin", "/api/v1/workers/poll"} {
		if got := workerPost(t, ts, path, checkIn); got != http.StatusOK {
			t.Fatalf("%s = %d", path, got)
		}
	}
	m.Submit(worker.Job{ID: "ONS-101", Attempt: 1, Issue: worker.Issue{Identifier: "ONS-101"}, QueuedAt: viewNow})

	body := getJSONMap(t, ts.URL+"/api/v1/state")
	var got []string
	for _, r := range rows(t, body, "running") {
		got = append(got, r["issue_identifier"].(string)+"@"+strconv.Quote(fmtAny(r["worker"])))
	}
	if want := []string{`ONS-34@"taichi"`, `ONS-110@""`}; !reflect.DeepEqual(got, want) {
		t.Errorf("running = %v, want %v: the queued ONS-101 is not running, and the preparing ONS-110 is on no worker", got, want)
	}
	if n := body["counts"].(map[string]any)["running"]; n != 2.0 {
		t.Errorf("counts.running = %v, want 2", n)
	}
	waits := rows(t, body, "waits")
	if len(waits) != 1 || waits[0]["issue_identifier"] != "ONS-101" || waits[0]["kind"] != "slot" ||
		waits[0]["reason"] != "worker taichi: all 1 run slots in use" {
		t.Errorf("waits = %v, want ONS-101 waiting for taichi's slot", waits)
	}
}

func fmtAny(v any) string {
	if v == nil {
		return ""
	}
	return v.(string)
}

// A queued job that a checked-in worker could run waits for that
// worker's slot.
func TestQueueWaitForASlot(t *testing.T) {
	t.Parallel()

	workers := []worker.Worker{{ID: "taichi", Declaration: worker.Declaration{OS: "linux", KVM: true, Nested: true, Backends: []string{"smolvm"}}}}
	q := worker.Queued{Job: worker.Job{ID: "ONS-7", Requires: worker.Requires{Capabilities: []string{"nested", "smolvm"}}},
		QueuedAt: viewNow, Waiting: []string{"worker taichi: all 2 run slots in use"}}
	got := queueWait(q, workers)
	want := waitResponse{IssueIdentifier: "ONS-7", Worker: "taichi", Kind: "slot", Reason: "worker taichi: all 2 run slots in use",
		Release: "worker taichi frees a slot", Since: viewNow}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("wait %+v, want %+v", got, want)
	}
}

// On Sortie's own host, the state serves the host, places each run on
// it or on its SSH host, and lists what waits: a slot hold, and the
// drain of a paused dispatch while runs remain.
func TestStateServesTheHostAndItsWaits(t *testing.T) {
	t.Parallel()

	ssh := running("ONS-2", "implement")
	ssh.SSHHost = "build-1"
	snap := orchestrator.RuntimeSnapshotResult{
		GeneratedAt: viewNow,
		Running:     []orchestrator.SnapshotRunningEntry{running("ONS-1", "implement"), ssh},
		Held: []orchestrator.SnapshotHeldEntry{
			{IssueID: "id-3", Identifier: "ONS-3", Hold: orchestrator.SnapshotHold{Reason: orchestrator.SkipAgentSlotsFull, Since: viewNow.Add(-time.Minute),
				Slot: &orchestrator.SnapshotSlotHold{Scope: orchestrator.SlotScopeGlobal, Used: 3, Limit: 3}}},
			{IssueID: "id-4", Identifier: "ONS-4", Hold: orchestrator.SnapshotHold{Reason: orchestrator.SkipSSHHostsFull, Since: viewNow}},
			{IssueID: "id-5", Identifier: "ONS-5", Hold: orchestrator.SnapshotHold{Reason: orchestrator.SkipBlockedBy, Since: viewNow}},
		},
		DispatchPause: &orchestrator.DispatchPause{Since: viewNow.Add(-2 * time.Minute), By: "stage0-deploy", Reason: "kit change"},
	}
	host := &hostview.Host{Name: "p1", Capacity: &hostview.Size{CPU: 16, MemGiB: 62, DiskGiB: 900},
		Used: &hostview.Size{CPU: 4, MemGiB: 8, DiskGiB: 40}, Unmanaged: []hostview.Machine{{Name: "sandbox", Since: viewNow.Add(-time.Hour), State: "stopped"}}}
	ts := viewServer(t, Params{SnapshotFn: fixedSnapshot(snap), HostFn: func(context.Context) *hostview.Host { return host }})

	body := getJSONMap(t, ts.URL+"/api/v1/state")
	want := map[string]any{
		"name":      "p1",
		"capacity":  map[string]any{"cpu": 16.0, "mem_gib": 62.0, "disk_gib": 900.0},
		"used":      map[string]any{"cpu": 4.0, "mem_gib": 8.0, "disk_gib": 40.0},
		"unmanaged": []any{map[string]any{"name": "sandbox", "since": "2026-11-02T08:00:00Z", "state": "stopped"}},
	}
	if !reflect.DeepEqual(body["host"], want) {
		t.Errorf("host = %v", body["host"])
	}
	run := rows(t, body, "running")
	if run[0]["worker"] != "p1" || run[1]["worker"] != "build-1" {
		t.Errorf("workers = %v, %v", run[0]["worker"], run[1]["worker"])
	}
	for _, member := range []string{"phase", "step", "step_at", "estimated_end"} {
		if _, ok := run[0][member]; ok {
			t.Errorf("running row has %s with no source: %v", member, run[0][member])
		}
	}
	waits := rows(t, body, "waits")
	if len(waits) != 4 {
		t.Fatalf("waits = %v", waits)
	}
	if w := waits[0]; w["issue_identifier"] != "ONS-4" || w["kind"] != "slot" || w["worker"] != nil {
		t.Errorf("SSH slot wait = %v", w)
	}
	if w := waits[1]; w["worker"] != "build-1" || w["kind"] != "drain" || w["release"] != "1 run here end" {
		t.Errorf("drain of build-1 = %v", w)
	}
	if w := waits[2]; w["worker"] != "p1" || w["kind"] != "drain" || w["issue_identifier"] != nil ||
		w["reason"] != "dispatch is paused by stage0-deploy: kit change" || w["since"] != "2026-11-02T08:58:00Z" {
		t.Errorf("drain of p1 = %v", w)
	}
	if w := waits[3]; w["issue_identifier"] != "ONS-3" || w["worker"] != "p1" || w["kind"] != "slot" ||
		w["reason"] == "" || w["release"] == "" || w["since"] != "2026-11-02T08:59:00Z" {
		t.Errorf("slot wait = %v", w)
	}
	if pulls := rows(t, body, "pulls"); len(pulls) != 0 {
		t.Errorf("pulls = %v", pulls)
	}
}

// With no host function and no workers, the state serves no host and
// empty lists.
func TestStateWithoutAHost(t *testing.T) {
	t.Parallel()

	ts := viewServer(t, Params{SnapshotFn: fixedSnapshot(orchestrator.RuntimeSnapshotResult{GeneratedAt: viewNow,
		Running: []orchestrator.SnapshotRunningEntry{running("ONS-1", "")}})})
	body := getJSONMap(t, ts.URL+"/api/v1/state")
	if _, ok := body["host"]; ok {
		t.Errorf("host = %v", body["host"])
	}
	if _, ok := rows(t, body, "running")[0]["worker"]; ok {
		t.Error("a run is placed on no known host")
	}
	if len(rows(t, body, "waits")) != 0 || len(rows(t, body, "pulls")) != 0 {
		t.Errorf("waits %v, pulls %v", body["waits"], body["pulls"])
	}
}

type countingHistory struct {
	fakeRunHistory
	mu      sync.Mutex
	queries []RunHistoryQuery
}

func (c *countingHistory) QueryRuns(ctx context.Context, q RunHistoryQuery) ([]RunHistoryEntry, error) {
	c.mu.Lock()
	c.queries = append(c.queries, q)
	c.mu.Unlock()
	return c.fakeRunHistory.QueryRuns(ctx, q)
}

func succeeded(minutes int) RunHistoryEntry {
	start := viewNow.Add(-24 * time.Hour)
	return RunHistoryEntry{Status: "succeeded", RuleName: "implement", StartedAt: start.Format(time.RFC3339),
		CompletedAt: start.Add(time.Duration(minutes) * time.Minute).Format(time.RFC3339)}
}

// A run's estimated end is its start plus the median length of its
// rule's newest succeeded runs, read once per rule and minute; a rule
// with none has no estimate.
func TestStateEstimatesARunsEnd(t *testing.T) {
	t.Parallel()

	history := &countingHistory{fakeRunHistory: fakeRunHistory{rows: []RunHistoryEntry{succeeded(40), succeeded(10), succeeded(20), succeeded(30)}}}
	ts := viewServer(t, Params{
		SnapshotFn: fixedSnapshot(orchestrator.RuntimeSnapshotResult{GeneratedAt: viewNow,
			Running: []orchestrator.SnapshotRunningEntry{running("ONS-1", "implement"), running("ONS-2", "implement")}}),
		RunHistory: history,
	})
	for range 2 {
		run := rows(t, getJSONMap(t, ts.URL+"/api/v1/state"), "running")
		for _, r := range run {
			if r["estimated_end"] != "2026-11-02T09:15:00Z" {
				t.Fatalf("estimated_end = %v, want the start plus 25 minutes", r["estimated_end"])
			}
		}
	}
	history.mu.Lock()
	defer history.mu.Unlock()
	if len(history.queries) != 1 {
		t.Fatalf("history read %d times", len(history.queries))
	}
	q := history.queries[0]
	if q.Status != "succeeded" || q.RuleName == nil || *q.RuleName != "implement" || q.Limit != estimateRuns {
		t.Fatalf("query %+v", q)
	}

	none := viewServer(t, Params{
		SnapshotFn: fixedSnapshot(orchestrator.RuntimeSnapshotResult{GeneratedAt: viewNow,
			Running: []orchestrator.SnapshotRunningEntry{running("ONS-1", "review")}}),
		RunHistory: &fakeRunHistory{},
	})
	if r := rows(t, getJSONMap(t, none.URL+"/api/v1/state"), "running")[0]; r["estimated_end"] != nil {
		t.Fatalf("estimated_end with no history = %v", r["estimated_end"])
	}
}

func pending(identifier, kind string, number int, check *orchestrator.ReactionCheck) orchestrator.SnapshotPendingReaction {
	return orchestrator.SnapshotPendingReaction{IssueID: "id-" + identifier, Identifier: identifier, Kind: kind,
		PullRequest: orchestrator.SnapshotPullRequest{Number: number, Repository: "ortusnova/homelab",
			URL: "https://github.com/ortusnova/homelab/pull/" + strconv.Itoa(number)}, LastCheck: check}
}

// Each pull request a pending reaction watches is served once, with what
// it waits on, since when this process first saw it so.
func TestStatePullsSayWhatTheyWaitOn(t *testing.T) {
	t.Parallel()

	no, yes := false, true
	escalated := viewNow.Add(-time.Hour)
	person := pending("ONS-5", orchestrator.ReactionKindReview, 5, nil)
	person.EscalatedAt, person.EscalationTrigger = &escalated, "max_continuation_turns"
	reactions := []orchestrator.SnapshotPendingReaction{
		pending("ONS-1", orchestrator.ReactionKindCI, 1, &orchestrator.ReactionCheck{CI: &orchestrator.CICheck{Status: "pending", Pending: 2}}),
		pending("ONS-1", orchestrator.ReactionKindReview, 1, &orchestrator.ReactionCheck{Comments: &orchestrator.CommentCheck{}}),
		pending("ONS-2", orchestrator.ReactionKindMergeConflict, 2, &orchestrator.ReactionCheck{Mergeability: &orchestrator.MergeabilityCheck{Mergeable: &no}}),
		pending("ONS-2", orchestrator.ReactionKindCI, 2, nil),
		pending("ONS-3", orchestrator.ReactionKindCI, 3, &orchestrator.ReactionCheck{CI: &orchestrator.CICheck{Status: "passing", Passing: 4}}),
		pending("ONS-3", orchestrator.ReactionKindReview, 3, &orchestrator.ReactionCheck{Comments: &orchestrator.CommentCheck{}}),
		pending("ONS-3", orchestrator.ReactionKindMergeConflict, 3, &orchestrator.ReactionCheck{Mergeability: &orchestrator.MergeabilityCheck{Mergeable: &yes}}),
		pending("ONS-4", orchestrator.ReactionKindReview, 4, &orchestrator.ReactionCheck{Comments: &orchestrator.CommentCheck{AwaitingApproval: true}}),
		person,
		pending("ONS-6", orchestrator.ReactionKindMergeCompletion, 6, nil),
	}
	var mu sync.Mutex
	now := viewNow
	snapshot := func() (orchestrator.RuntimeSnapshotResult, error) {
		mu.Lock()
		defer mu.Unlock()
		return orchestrator.RuntimeSnapshotResult{GeneratedAt: now, PendingReactions: reactions}, nil
	}
	ts := viewServer(t, Params{SnapshotFn: snapshot, HostFn: func(context.Context) *hostview.Host { return &hostview.Host{Name: "p1"} }})

	type pull struct{ ident, waitsOn, reason, since string }
	want := []pull{
		{"ONS-1", "ci", "2 checks pending", "2026-11-02T09:00:00Z"},
		{"ONS-2", "conflict", "it conflicts with its base branch", "2026-11-02T09:00:00Z"},
		{"ONS-3", "none", "ready to merge: CI and its review and the conflict check passed", "2026-11-02T09:00:00Z"},
		{"ONS-4", "review", "it waits for an approving review", "2026-11-02T09:00:00Z"},
		{"ONS-5", "person", "escalated to a person: max continuation turns", "2026-11-02T08:00:00Z"},
	}
	check := func(label string, want []pull) {
		t.Helper()
		got := rows(t, getJSONMap(t, ts.URL+"/api/v1/state"), "pulls")
		if len(got) != len(want) {
			t.Fatalf("%s: pulls = %v", label, got)
		}
		for i, p := range got {
			w := want[i]
			if p["issue_identifier"] != w.ident || p["waits_on"] != w.waitsOn || p["reason"] != w.reason || p["since"] != w.since ||
				p["repository"] != "ortusnova/homelab" || p["worker"] != "p1" {
				t.Errorf("%s: pull %d = %v, want %+v", label, i, p, w)
			}
		}
	}
	check("first read", want)

	mu.Lock()
	now = viewNow.Add(10 * time.Minute)
	reactions = append([]orchestrator.SnapshotPendingReaction(nil), reactions...)
	reactions[0].LastCheck = &orchestrator.ReactionCheck{CI: &orchestrator.CICheck{Status: "failing", Failing: 1}}
	reactions[1].LastCheck = &orchestrator.ReactionCheck{Comments: &orchestrator.CommentCheck{Actionable: 3}}
	reactions[2].LastCheck = nil
	mu.Unlock()
	want[0] = pull{"ONS-1", "ci", "1 check failing", "2026-11-02T09:00:00Z"}
	want[1] = pull{"ONS-2", "ci", "CI has not reported on it yet", "2026-11-02T09:10:00Z"}
	check("ten minutes later", want)
}

// Under remote, a story held because no worker can take its job, and a
// retry that waits so, wait for a worker as a queued job does, and
// neither is running, so a drain waits on neither.
func TestStateServesTheJobsNoWorkerCanTake(t *testing.T) {
	t.Parallel()

	m := worker.NewMatcher(worker.NewTTLLeaser(time.Minute), func() time.Time { return viewNow }, nil)
	m.CheckIn("p1", worker.Declaration{OS: "linux", Arch: "x86_64", KVM: true, Backends: []string{"smolvm"}, RunSlots: 2,
		Free: worker.Free{CPU: 16, MemGiB: 52, DiskGiB: 210}})
	m.CheckIn("taichi", worker.Declaration{OS: "linux", Arch: "x86_64", KVM: true, Nested: true, Backends: []string{"smolvm"}, RunSlots: 2,
		Free: worker.Free{CPU: 16, MemGiB: 52, DiskGiB: 92.04}})
	wait := &orchestrator.WorkerHold{Capabilities: []string{"nested", "smolvm"},
		Waiting: []string{"worker p1: no nested virtualization", "worker taichi: 92.04 of 200 GiB disk free"}}
	snap := orchestrator.RuntimeSnapshotResult{
		GeneratedAt: viewNow,
		Held: []orchestrator.SnapshotHeldEntry{{IssueID: "id-54", Identifier: "ONS-54",
			Hold: orchestrator.SnapshotHold{Reason: orchestrator.SkipNoWorker, Since: viewNow.Add(-time.Minute), Worker: wait}}},
		Retrying: []orchestrator.SnapshotRetryEntry{{IssueID: "id-87", Identifier: "ONS-87", Attempt: 2, DueAtMS: viewNow.UnixMilli(),
			WorkerWait: wait, WorkerWaitSince: viewNow.Add(-2 * time.Minute)}},
		DispatchPause: &orchestrator.DispatchPause{Since: viewNow, By: "stage0-deploy"},
	}
	ts := viewServer(t, Params{SnapshotFn: fixedSnapshot(snap), WorkersFn: func() *worker.Matcher { return m }})

	body := getJSONMap(t, ts.URL+"/api/v1/state")
	if n := len(rows(t, body, "running")); n != 0 {
		t.Errorf("running = %d, want 0", n)
	}
	held := rows(t, body, "held")[0]["hold"].(map[string]any)
	if held["reason"] != "no_worker" ||
		held["reason_text"] != "No worker can take its job: worker p1: no nested virtualization; worker taichi: 92.04 of 200 GiB disk free" ||
		!reflect.DeepEqual(held["worker"], map[string]any{"capabilities": []any{"nested", "smolvm"},
			"waiting": []any{"worker p1: no nested virtualization", "worker taichi: 92.04 of 200 GiB disk free"}}) {
		t.Errorf("hold = %v", held)
	}
	waits := rows(t, body, "waits")
	if len(waits) != 2 {
		t.Fatalf("waits = %v, want ONS-54 and ONS-87 and no drain", waits)
	}
	for i, want := range []struct{ id, since string }{{"ONS-54", "2026-11-02T08:59:00Z"}, {"ONS-87", "2026-11-02T08:58:00Z"}} {
		w := waits[i]
		if w["issue_identifier"] != want.id || w["kind"] != "slot" || w["worker"] != "taichi" || w["since"] != want.since ||
			w["reason"] != "worker p1: no nested virtualization; worker taichi: 92.04 of 200 GiB disk free" || w["release"] != "worker taichi frees a slot" {
			t.Errorf("waits[%d] = %v, want %s waiting for taichi since %s", i, w, want.id, want.since)
		}
	}
}

// job-egress-policy: A typo merged. An issue held because its
// repository's egress policy is invalid shows egress policy invalid in
// GET /api/v1/state, with the repository, the commit, the file and the
// entry, and it waits on no worker.
func TestStateServesAnEgressHold(t *testing.T) {
	t.Parallel()

	hold := &orchestrator.EgressHold{Repository: "homelab", Branch: "main", Commit: "4b825dc642cb6eb9a060e54bf8d69288fbee4904",
		File: ".sortie/egress.yaml", Entry: "hosts[0].host", Error: `"ghcr.io:443" has a port: write the DNS name alone`}
	snap := orchestrator.RuntimeSnapshotResult{
		GeneratedAt: viewNow,
		Held: []orchestrator.SnapshotHeldEntry{{IssueID: "id-30", Identifier: "ONS-30",
			Hold: orchestrator.SnapshotHold{Reason: orchestrator.SkipEgressInvalid, Since: viewNow.Add(-time.Minute), Egress: hold}}},
		Retrying: []orchestrator.SnapshotRetryEntry{{IssueID: "id-31", Identifier: "ONS-31", Attempt: 2, DueAtMS: viewNow.UnixMilli(),
			EgressHold: hold}},
	}
	ts := viewServer(t, Params{SnapshotFn: fixedSnapshot(snap)})

	body := getJSONMap(t, ts.URL+"/api/v1/state")
	want := map[string]any{"repository": "homelab", "branch": "main", "commit": hold.Commit, "file": ".sortie/egress.yaml",
		"entry": "hosts[0].host", "error": hold.Error, "text": hold.Text()}
	got := rows(t, body, "held")[0]["hold"].(map[string]any)
	if got["reason"] != "egress_invalid" || !reflect.DeepEqual(got["egress"], want) ||
		!strings.HasPrefix(got["reason_text"].(string), "Egress policy invalid: homelab at "+hold.Commit+", .sortie/egress.yaml hosts[0].host: ") {
		t.Errorf("hold = %v", got)
	}
	if !strings.HasPrefix(hold.Text(), "egress policy invalid: repository homelab at main commit "+hold.Commit) {
		t.Errorf("Text() = %q", hold.Text())
	}
	if retry := rows(t, body, "retrying")[0]; !reflect.DeepEqual(retry["egress_hold"], want) {
		t.Errorf("retrying[0].egress_hold = %v, want %v", retry["egress_hold"], want)
	}
	if waits := rows(t, body, "waits"); len(waits) != 0 {
		t.Errorf("waits = %v, want none: the hold waits on no worker", waits)
	}
}
