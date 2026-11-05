package orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// gatedFinder is a forge whose revert listing records each call and, while
// gate is open, blocks until the test closes it, as a slow forge does.
type gatedFinder struct {
	mu      sync.Mutex
	since   []time.Time
	reverts []domain.MergedRevert
	entered chan struct{}
	gate    chan struct{}
}

func (g *gatedFinder) FindMergedReverts(ctx context.Context, _, _ string, since time.Time) ([]domain.MergedRevert, error) {
	g.mu.Lock()
	g.since = append(g.since, since)
	entered, gate := g.entered, g.gate
	g.mu.Unlock()
	if entered != nil {
		entered <- struct{}{}
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]domain.MergedRevert(nil), g.reverts...), nil
}

func (g *gatedFinder) calls() []time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]time.Time(nil), g.since...)
}

// newRevertFixture is a prove fixture with ONS-121 Done, its merge of
// o/kit#142 recorded, and the forge's revert listing replaced by finder.
func newRevertFixture(t *testing.T, finder *gatedFinder) *proveFixture {
	t.Helper()
	f := newProveFixture(t)
	f.o.revertFinder = finder
	f.stories.add("id-121", "ONS-121", "Done")
	if err := f.store.UpsertPlanSatisfaction(t.Context(), persistence.PlanSatisfaction{
		IssueID: "id-121", Kind: persistence.PlanSatisfactionMerged, Ref: mergeSHA(142), PR: "o/kit#142", RecordedAt: f.t0,
	}); err != nil {
		t.Fatal(err)
	}
	return f
}

// The revert scan reads the forge off the event loop: while the forge
// does not answer, the loop answers a snapshot at once, and no second scan
// starts. Seen on p1 on 2026-11-05, when the scan on the loop held GET
// /api/v1/state for 10 to 20 seconds of every 40.
func TestStoryRevert_ScanRunsOffTheLoop(t *testing.T) {
	t.Parallel()
	finder := &gatedFinder{entered: make(chan struct{}, 4), gate: make(chan struct{}),
		reverts: []domain.MergedRevert{{Number: 144, PullRequests: []int{142}}}}
	f := newRevertFixture(t, finder)
	cfg := f.o.workflowManager.Config()

	start := time.Now()
	onLoop(t, f.o, func() { f.o.detectReverts(t.Context(), cfg, f.t0) })
	select {
	case <-finder.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the scan did not read the forge")
	}
	if took := time.Since(start); took > 100*time.Millisecond {
		t.Errorf("detectReverts held the loop %v with the forge blocked, want under 100ms", took)
	}

	start = time.Now()
	onLoop(t, f.o, func() { _ = snapshotMergeAndProve(f.o.state) })
	if took := time.Since(start); took > 100*time.Millisecond {
		t.Errorf("a snapshot during the scan took %v, want under 100ms", took)
	}

	// A tick past the interval while the scan is in flight starts none.
	onLoop(t, f.o, func() { f.o.detectReverts(t.Context(), cfg, f.t0.Add(time.Hour)) })
	if n := len(finder.calls()); n != 1 {
		t.Errorf("forge reads with a scan in flight = %d, want 1", n)
	}
	if f.stories.stateOf("id-121") != "Done" {
		t.Error("the story moved before the forge answered")
	}

	close(finder.gate)
	f.o.state.TrackerOpsWg.Wait()
	if state := f.stories.stateOf("id-121"); state != "Todo" {
		t.Errorf("ONS-121 after the scan = %q, want Todo", state)
	}
	if _, found := f.planMerge(t, "id-121"); found {
		t.Error("ONS-121 keeps its merge record after the revert")
	}
	if c := f.stories.lastComment("id-121"); c == "" {
		t.Error("no comment on the reverted story")
	}
	if sent := f.spy.notifications(); len(sent) != 1 {
		t.Errorf("notifications = %d, want one story.reverted", len(sent))
	}
	var running bool
	onLoop(t, f.o, func() { running = f.o.revertScan.running })
	if running {
		t.Error("the scan is still marked in flight after it was applied")
	}
}

// The revert scan runs once per merge_and_prove.revert_scan_interval, not
// on every poll tick, and reads only what merged since the last scan.
func TestStoryRevert_ScanThrottledAndIncremental(t *testing.T) {
	t.Parallel()
	finder := &gatedFinder{}
	f := newRevertFixture(t, finder)
	every := config.DefaultRevertScanInterval
	for _, at := range []time.Duration{0, 15 * time.Second, 30 * time.Second, every - time.Second} {
		f.tick(t, f.t0.Add(at))
	}
	calls := finder.calls()
	if len(calls) != 1 {
		t.Fatalf("forge reads over ticks within one interval = %d, want 1", len(calls))
	}
	if want := f.t0.Add(-revertScanLookback); !calls[0].Equal(want) {
		t.Errorf("first scan reads from %v, want the oldest record less the lookback, %v", calls[0], want)
	}
	f.tick(t, f.t0.Add(every))
	calls = finder.calls()
	if len(calls) != 2 {
		t.Fatalf("forge reads after one interval = %d, want 2", len(calls))
	}
	if want := f.t0.Add(-revertScanOverlap); !calls[1].Equal(want) {
		t.Errorf("second scan reads from %v, want the first scan less the overlap, %v", calls[1], want)
	}

	cfg := f.o.workflowManager.Config()
	cfg.MergeAndProve.RevertScanInterval = 10 * time.Minute
	f.o.workflowManager = &stubWorkflowManager{config: cfg}
	f.tick(t, f.t0.Add(2*every))
	if n := len(finder.calls()); n != 3 {
		t.Fatalf("forge reads = %d, want 3", n)
	}
	f.tick(t, f.t0.Add(2*every+5*time.Minute))
	if n := len(finder.calls()); n != 3 {
		t.Errorf("forge reads within a configured 10m interval = %d, want 3", n)
	}
}
