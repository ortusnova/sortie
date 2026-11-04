package server

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/orchestrator"
)

func handoffTestServer(t *testing.T, parts []orchestrator.SnapshotHandoffPart) *httptest.Server {
	t.Helper()
	srv := New(Params{
		SnapshotFn: func() (orchestrator.RuntimeSnapshotResult, error) {
			return orchestrator.RuntimeSnapshotResult{HandoffParts: parts}, nil
		},
		RefreshFn:    acceptingRefresh(),
		Logger:       slog.New(slog.DiscardHandler),
		ControlToken: testControlToken,
	})
	ts := httptest.NewServer(srv.Mux())
	t.Cleanup(ts.Close)
	return ts
}

func TestState_ListsEachOpenHandoffPart(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	ts := handoffTestServer(t, []orchestrator.SnapshotHandoffPart{{
		IssueID: "id-77", Identifier: "ONS-77", ID: "live-chain", Kind: orchestrator.HandoffKindJob,
		Need: "reach gitea.homelab.lan", Check: "curl -fsS https://gitea.homelab.lan", Capabilities: []string{"reach:homelab"},
		Status: orchestrator.HandoffFault, JobAttempt: 1, FaultID: "pf-1", Result: "no worker", RecordedAt: at, UpdatedAt: at,
	}})
	status, state := callDispatchPause(t, ts, http.MethodGet, "/api/v1/state", "", "")
	if status != http.StatusOK {
		t.Fatalf("GET /api/v1/state status = %d", status)
	}
	parts, _ := state["handoff_parts"].([]any)
	if len(parts) != 1 {
		t.Fatalf("handoff_parts = %v, want one part", state["handoff_parts"])
	}
	part := parts[0].(map[string]any)
	want := map[string]any{
		"issue_id": "id-77", "issue_identifier": "ONS-77", "id": "live-chain", "kind": "job",
		"need": "reach gitea.homelab.lan", "check": "curl -fsS https://gitea.homelab.lan", "capabilities": []any{"reach:homelab"},
		"repository": "", "status": "fault", "job_attempt": float64(1), "work_identifier": "", "platform_fault_id": "pf-1",
		"result": "no worker", "recorded_at": "2026-11-03T10:00:00Z", "updated_at": "2026-11-03T10:00:00Z",
	}
	if !reflect.DeepEqual(part, want) {
		t.Errorf("handoff_parts[0] = %v, want %v", part, want)
	}

	empty := handoffTestServer(t, nil)
	if _, state := callDispatchPause(t, empty, http.MethodGet, "/api/v1/state", "", ""); state["handoff_parts"] == nil {
		t.Errorf("handoff_parts with no part = %v, want an empty list", state["handoff_parts"])
	}
}
