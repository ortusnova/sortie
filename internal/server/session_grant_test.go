package server

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/sortie-ai/sortie/internal/orchestrator"
)

// fakeSessionGrants keeps grants the way the orchestrator does, for one
// held issue, ONS-68, at 2 of max_sessions 2.
type fakeSessionGrants struct {
	mu          sync.Mutex
	maxSessions int
	granted     int
	requests    []orchestrator.SessionGrantRequest
}

func (f *fakeSessionGrants) fn(_ context.Context, req orchestrator.SessionGrantRequest) orchestrator.SessionGrantResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, req)
	if req.Identifier != "ONS-68" {
		return orchestrator.SessionGrantResult{Status: orchestrator.SessionGrantUnknownIssue, Message: "issue " + req.Identifier + " is not known"}
	}
	f.granted += req.Add
	budget := 0
	if f.maxSessions > 0 {
		budget = f.maxSessions + f.granted
	}
	return orchestrator.SessionGrantResult{
		Status: orchestrator.SessionGrantDone, IssueID: "id-68", Identifier: "ONS-68",
		UsedSessions: 2, BudgetSessions: budget, GrantedSessions: f.granted,
	}
}

func (f *fakeSessionGrants) snapshot() (orchestrator.RuntimeSnapshotResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	snap := orchestrator.RuntimeSnapshotResult{
		MaxSessions: f.maxSessions,
		Running: []orchestrator.SnapshotRunningEntry{{
			IssueID: "id-68", Identifier: "ONS-68", IssueUsage: &orchestrator.SnapshotIssueUsage{Sessions: 2},
		}},
		BudgetExhausted: []orchestrator.SnapshotBudgetEntry{{
			IssueID: "id-50", Identifier: "ONS-50", Reason: "session_budget", UsedSessions: 2, BudgetSessions: f.maxSessions,
		}},
	}
	if f.granted > 0 {
		snap.SessionGrants = map[string]int{"id-68": f.granted}
	}
	return snap, nil
}

func (f *fakeSessionGrants) calls() []orchestrator.SessionGrantRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]orchestrator.SessionGrantRequest(nil), f.requests...)
}

func sessionGrantTestServer(t *testing.T, fake *fakeSessionGrants) *httptest.Server {
	t.Helper()
	srv := New(Params{
		SnapshotFn:   fake.snapshot,
		RefreshFn:    acceptingRefresh(),
		Logger:       slog.New(slog.DiscardHandler),
		ControlToken: testControlToken,
		SessionTokenFn: func(token string) (string, bool) {
			return "ONS-68", token == tokenLive1
		},
		SessionGrantFn: fake.fn,
	})
	ts := httptest.NewServer(srv.Mux())
	t.Cleanup(ts.Close)
	return ts
}

func TestSessionGrant_AnswersWithTheNewBudget(t *testing.T) {
	t.Parallel()

	fake := &fakeSessionGrants{maxSessions: 2}
	ts := sessionGrantTestServer(t, fake)
	auth := "Bearer " + testControlToken

	status, body := callDispatchPause(t, ts, http.MethodPost, "/api/v1/ONS-68/sessions", auth, `{"add":4,"by":"brian","reason":"one more try"}`)
	if status != http.StatusOK {
		t.Fatalf("POST /api/v1/ONS-68/sessions status = %d, want 200 (body %v)", status, body)
	}
	want := map[string]any{"issue_identifier": "ONS-68", "issue_id": "id-68", "used_sessions": 2.0, "budget_sessions": 6.0, "granted_sessions": 4.0}
	for key, value := range want {
		if body[key] != value {
			t.Errorf("response %s = %v, want %v", key, body[key], value)
		}
	}
	if got := fake.calls(); len(got) != 1 || got[0] != (orchestrator.SessionGrantRequest{Identifier: "ONS-68", Add: 4, By: "brian", Reason: "one more try"}) {
		t.Errorf("SessionGrantFn requests = %+v, want one for ONS-68 adding 4 by brian", got)
	}

	if status, _ := callDispatchPause(t, ts, http.MethodPost, "/api/v1/ONS-68/sessions", auth, `{"add":1}`); status != http.StatusOK {
		t.Fatalf("second grant status = %d, want 200", status)
	}
	if got := fake.calls(); len(got) != 2 || got[1].By != defaultGrantedBy {
		t.Errorf("a grant without by = %+v, want by %q", got, defaultGrantedBy)
	}

	_, state := callDispatchPause(t, ts, http.MethodGet, "/api/v1/state", "", "")
	_, detail := callDispatchPause(t, ts, http.MethodGet, "/api/v1/ONS-68", "", "")
	runningRows := []any{}
	if rows, ok := state["running"].([]any); ok {
		runningRows = rows
	}
	if row, ok := detail["running"]; ok {
		runningRows = append(runningRows, row)
	}
	if len(runningRows) != 2 {
		t.Fatalf("running rows on /api/v1/state and /api/v1/ONS-68 = %v, want one on each", runningRows)
	}
	for _, raw := range runningRows {
		row, _ := raw.(map[string]any)
		if row["granted_sessions"] != 5.0 || row["budget_sessions"] != 7.0 {
			t.Errorf("running row granted_sessions, budget_sessions = %v, %v, want 5, 7", row["granted_sessions"], row["budget_sessions"])
		}
	}
	held, _ := state["budget_exhausted"].([]any)
	if len(held) != 1 {
		t.Fatalf("budget_exhausted = %v, want one row", state["budget_exhausted"])
	}
	if row, _ := held[0].(map[string]any); row["granted_sessions"] != 0.0 || row["budget_sessions"] != 2.0 {
		t.Errorf("held row without a grant: granted_sessions, budget_sessions = %v, %v, want 0, 2", row["granted_sessions"], row["budget_sessions"])
	}
}

func TestSessionGrant_NoSessionBudgetServesNull(t *testing.T) {
	t.Parallel()

	ts := sessionGrantTestServer(t, &fakeSessionGrants{})
	status, body := callDispatchPause(t, ts, http.MethodPost, "/api/v1/ONS-68/sessions", "Bearer "+testControlToken, `{"add":2}`)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %v)", status, body)
	}
	if value, present := body["budget_sessions"]; !present || value != nil || body["granted_sessions"] != 2.0 {
		t.Errorf("budget_sessions, granted_sessions = %v (present %v), %v, want null, 2", value, present, body["granted_sessions"])
	}
}

func TestSessionGrant_RefusedRequests(t *testing.T) {
	t.Parallel()

	auth := "Bearer " + testControlToken
	tests := []struct {
		name          string
		method        string
		path          string
		authorization string
		body          string
		wantStatus    int
		wantCode      string
	}{
		{"no token", http.MethodPost, "/api/v1/ONS-68/sessions", "", `{"add":1}`, http.StatusUnauthorized, ""},
		{"wrong token", http.MethodPost, "/api/v1/ONS-68/sessions", "Bearer wrong-token", `{"add":1}`, http.StatusUnauthorized, ""},
		{"a session token", http.MethodPost, "/api/v1/ONS-68/sessions", "Bearer " + tokenLive1, `{"add":1}`, http.StatusForbidden, ""},
		{"GET", http.MethodGet, "/api/v1/ONS-68/sessions", auth, "", http.StatusMethodNotAllowed, ""},
		{"malformed body", http.MethodPost, "/api/v1/ONS-68/sessions", auth, "{not json", http.StatusBadRequest, "invalid_request"},
		{"no body", http.MethodPost, "/api/v1/ONS-68/sessions", auth, "", http.StatusBadRequest, "invalid_request"},
		{"no add", http.MethodPost, "/api/v1/ONS-68/sessions", auth, `{"by":"brian"}`, http.StatusBadRequest, "invalid_request"},
		{"add 0", http.MethodPost, "/api/v1/ONS-68/sessions", auth, `{"add":0}`, http.StatusBadRequest, "invalid_request"},
		{"add 65", http.MethodPost, "/api/v1/ONS-68/sessions", auth, `{"add":65}`, http.StatusBadRequest, "invalid_request"},
		{"add a fraction", http.MethodPost, "/api/v1/ONS-68/sessions", auth, `{"add":1.5}`, http.StatusBadRequest, "invalid_request"},
		{"an unknown issue", http.MethodPost, "/api/v1/ONS-404/sessions", auth, `{"add":1}`, http.StatusNotFound, "issue_not_found"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeSessionGrants{maxSessions: 2}
			ts := sessionGrantTestServer(t, fake)
			status, body := callDispatchPause(t, ts, tt.method, tt.path, tt.authorization, tt.body)
			if status != tt.wantStatus {
				t.Fatalf("%s %s status = %d, want %d (body %v)", tt.method, tt.path, status, tt.wantStatus, body)
			}
			if tt.wantCode != "" {
				if errBody, _ := body["error"].(map[string]any); errBody["code"] != tt.wantCode {
					t.Errorf("error = %v, want code %s", body["error"], tt.wantCode)
				}
			}
			wantCalls := 0
			if tt.wantStatus == http.StatusNotFound {
				wantCalls = 1
			}
			if got := len(fake.calls()); got != wantCalls {
				t.Errorf("SessionGrantFn calls = %d, want %d", got, wantCalls)
			}
		})
	}
}

func TestSessionGrant_Unavailable(t *testing.T) {
	t.Parallel()

	ts := testServer(t, fixedSnapshot(orchestrator.RuntimeSnapshotResult{}), acceptingRefresh())
	if status, body := callDispatchPause(t, ts, http.MethodPost, "/api/v1/ONS-68/sessions", "", `{"add":1}`); status != http.StatusServiceUnavailable {
		t.Errorf("POST without SessionGrantFn status = %d, want 503 (body %v)", status, body)
	}

	for _, result := range []orchestrator.SessionGrantStatus{orchestrator.SessionGrantDraining, orchestrator.SessionGrantUnavailable} {
		srv := New(Params{
			SnapshotFn: fixedSnapshot(orchestrator.RuntimeSnapshotResult{}),
			RefreshFn:  acceptingRefresh(),
			Logger:     slog.New(slog.DiscardHandler),
			SessionGrantFn: func(context.Context, orchestrator.SessionGrantRequest) orchestrator.SessionGrantResult {
				return orchestrator.SessionGrantResult{Status: result}
			},
		})
		unavailable := httptest.NewServer(srv.Mux())
		if status, body := callDispatchPause(t, unavailable, http.MethodPost, "/api/v1/ONS-68/sessions", "", `{"add":1}`); status != http.StatusServiceUnavailable {
			t.Errorf("POST with status %d status = %d, want 503 (body %v)", result, status, body)
		}
		unavailable.Close()
	}
}
