package server

import (
	"time"

	"github.com/sortie-ai/sortie/internal/orchestrator"
)

// snapshotFreshness says how old the runtime snapshot behind a response is,
// and whether it is the last one the event loop built because the loop did
// not answer in time.
type snapshotFreshness struct {
	SnapshotAgeMS int64 `json:"snapshot_age_ms"`
	SnapshotStale bool  `json:"snapshot_stale"`
}

func freshnessOf(snap orchestrator.RuntimeSnapshotResult, now time.Time) snapshotFreshness {
	return snapshotFreshness{
		SnapshotAgeMS: max(now.Sub(snap.GeneratedAt), 0).Milliseconds(),
		SnapshotStale: snap.Stale,
	}
}

// loopResponse is how long the event loop holds its work, on GET
// /api/v1/state.
type loopResponse struct {
	// BusyNowMS is how long the hold in progress has lasted, and
	// BusyHandler what holds the loop; 0 and null while the loop waits.
	BusyNowMS   int64   `json:"busy_now_ms"`
	BusyHandler *string `json:"busy_handler"`

	// MaxHoldMS5m is the longest hold of 100 ms or more in the last 5
	// minutes, and SlowestHandler5m what held it; 0 and null without one.
	MaxHoldMS5m      int64   `json:"max_hold_ms_5m"`
	SlowestHandler5m *string `json:"slowest_handler_5m"`

	// SlowHolds10m counts the holds over 5 seconds in the last 10 minutes.
	SlowHolds10m int `json:"slow_holds_10m"`
}

func toLoopResponse(st orchestrator.LoopStatus) loopResponse {
	resp := loopResponse{
		BusyNowMS:    st.BusyNow.Milliseconds(),
		MaxHoldMS5m:  st.MaxHold.Milliseconds(),
		SlowHolds10m: st.SlowHolds,
	}
	if st.BusyHandler != "" {
		resp.BusyHandler = new(st.BusyHandler)
	}
	if st.SlowestHandler != "" {
		resp.SlowestHandler5m = new(st.SlowestHandler)
	}
	return resp
}
