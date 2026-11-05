package server

import (
	"net/http"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/orchestrator"
)

func TestState_ServesSnapshotFreshness(t *testing.T) {
	t.Parallel()

	t.Run("stale snapshot answers 200 with its age and the busy loop", func(t *testing.T) {
		t.Parallel()

		built := time.Now().Add(-3 * time.Second)
		snap := orchestrator.RuntimeSnapshotResult{
			GeneratedAt: built,
			Stale:       true,
			Loop: orchestrator.LoopStatus{
				BusyNow:        2500 * time.Millisecond,
				BusyHandler:    "tick/reconcile.review_comments",
				MaxHold:        4200 * time.Millisecond,
				SlowestHandler: "tick/fetch_candidates",
				SlowHolds:      1,
			},
		}
		ts := testServer(t, fixedSnapshot(snap), acceptingRefresh())

		status, body := getJSON[map[string]any](t, ts, "/api/v1/state")
		if status != http.StatusOK {
			t.Fatalf("GET /api/v1/state = %d, want 200", status)
		}
		if body["snapshot_stale"] != true {
			t.Errorf("snapshot_stale = %v, want true", body["snapshot_stale"])
		}
		if age, _ := body["snapshot_age_ms"].(float64); age < 3000 || age > 60000 {
			t.Errorf("snapshot_age_ms = %v, want the 3 s since the snapshot was built", body["snapshot_age_ms"])
		}
		loop, _ := body["loop"].(map[string]any)
		want := map[string]any{
			"busy_now_ms":        2500.0,
			"busy_handler":       "tick/reconcile.review_comments",
			"max_hold_ms_5m":     4200.0,
			"slowest_handler_5m": "tick/fetch_candidates",
			"slow_holds_10m":     1.0,
		}
		for key, w := range want {
			if loop[key] != w {
				t.Errorf("loop.%s = %v, want %v", key, loop[key], w)
			}
		}
	})

	t.Run("fresh snapshot is not stale and an idle loop serves nulls", func(t *testing.T) {
		t.Parallel()

		ts := testServer(t, fixedSnapshot(orchestrator.RuntimeSnapshotResult{GeneratedAt: time.Now()}), acceptingRefresh())

		status, body := getJSON[map[string]any](t, ts, "/api/v1/state")
		if status != http.StatusOK {
			t.Fatalf("GET /api/v1/state = %d, want 200", status)
		}
		if body["snapshot_stale"] != false {
			t.Errorf("snapshot_stale = %v, want false", body["snapshot_stale"])
		}
		if _, ok := body["snapshot_age_ms"].(float64); !ok {
			t.Errorf("snapshot_age_ms = %v, want a number", body["snapshot_age_ms"])
		}
		loop, _ := body["loop"].(map[string]any)
		for _, key := range []string{"busy_handler", "slowest_handler_5m"} {
			if v, ok := loop[key]; !ok || v != nil {
				t.Errorf("loop.%s = %v (present %v), want null", key, v, ok)
			}
		}
		if loop["busy_now_ms"] != 0.0 {
			t.Errorf("loop.busy_now_ms = %v, want 0", loop["busy_now_ms"])
		}
	})
}

func TestIssueDetail_ServesSnapshotFreshness(t *testing.T) {
	t.Parallel()

	snap := orchestrator.RuntimeSnapshotResult{
		GeneratedAt: time.Now().Add(-2 * time.Second),
		Stale:       true,
		Running:     []orchestrator.SnapshotRunningEntry{{IssueID: "id-1", Identifier: "TEST-1", StartedAt: time.Now()}},
	}
	ts := testServer(t, fixedSnapshot(snap), acceptingRefresh())

	status, body := getJSON[map[string]any](t, ts, "/api/v1/TEST-1")
	if status != http.StatusOK {
		t.Fatalf("GET /api/v1/TEST-1 = %d, want 200", status)
	}
	if body["snapshot_stale"] != true {
		t.Errorf("snapshot_stale = %v, want true", body["snapshot_stale"])
	}
	if age, _ := body["snapshot_age_ms"].(float64); age < 2000 {
		t.Errorf("snapshot_age_ms = %v, want at least 2000", body["snapshot_age_ms"])
	}
}

func TestFreshnessOf_ClampsAFutureSnapshot(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	got := freshnessOf(orchestrator.RuntimeSnapshotResult{GeneratedAt: now.Add(time.Second)}, now)
	if got.SnapshotAgeMS != 0 || got.SnapshotStale {
		t.Fatalf("freshnessOf() = %+v, want age 0 and fresh", got)
	}
}
