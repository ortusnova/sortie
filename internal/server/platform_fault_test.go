package server

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/failcause"
	"github.com/sortie-ai/sortie/internal/orchestrator"
)

// fakePlatformFaults holds one open fault the way the orchestrator does:
// marking its action done checks it, and the check passes once the
// token has the scope.
type fakePlatformFaults struct {
	mu            sync.Mutex
	fault         *orchestrator.SnapshotPlatformFault
	tokenHasScope bool
	calls         []string
}

func scopeFault() *orchestrator.SnapshotPlatformFault {
	at := time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)
	return &orchestrator.SnapshotPlatformFault{
		ID:        "pf-1a2b3c",
		Class:     "token_scope",
		Step:      "export",
		Signature: "export | git_push | push sortie/<id> to <url>: [remote rejected]",
		Cause:     "push sortie/ONS-49: [remote rejected] (refusing to allow a Personal Access Token to create or update workflow without `workflow` scope)",
		Detail:    "remote: refusing to allow a Personal Access Token",
		Action: &failcause.PersonAction{
			Action: `Give taichi's GitHub token "Workflows: Read and write"`,
			Where:  "GitHub, Settings > Developer settings > Personal access tokens: the token Sortie on taichi pushes with",
		},
		RecordedAt:  at,
		NextCheckAt: at.Add(orchestrator.PlatformFaultCheckInterval),
		Holds: []orchestrator.PlatformFaultHold{
			{IssueID: "id-49", Identifier: "ONS-49", HeldAt: at},
			{IssueID: "id-50", Identifier: "ONS-50", HeldAt: at.Add(time.Minute)},
			{IssueID: "id-51", Identifier: "ONS-51", DisplayID: "ortusnova/ONS-51", HeldAt: at.Add(2 * time.Minute)},
		},
	}
}

func (f *fakePlatformFaults) done(_ context.Context, id, by, answer string) orchestrator.PlatformFaultDoneResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	call := id + " by " + by
	if answer != "" {
		call += " answer " + answer
	}
	f.calls = append(f.calls, call)
	if f.fault == nil || f.fault.ID != id {
		return orchestrator.PlatformFaultDoneResult{Status: orchestrator.PlatformFaultDoneNotFound}
	}
	if answer == "maybe" {
		return orchestrator.PlatformFaultDoneResult{Status: orchestrator.PlatformFaultDoneInvalid, Message: "answer requeue or cancel"}
	}
	answered := *f.fault
	answered.ActionDoneAt, answered.ActionDoneBy, answered.Answer = time.Date(2026, 11, 2, 11, 0, 0, 0, time.UTC), by, answer
	answered.LastCheckAt = answered.ActionDoneAt
	if !f.tokenHasScope {
		answered.LastCheckError = "push refused"
		f.fault = &answered
		return orchestrator.PlatformFaultDoneResult{Status: orchestrator.PlatformFaultDoneChecked, Fault: answered}
	}
	f.fault = nil
	return orchestrator.PlatformFaultDoneResult{Status: orchestrator.PlatformFaultDoneChecked, Resolved: true, Fault: answered}
}

func (f *fakePlatformFaults) snapshot() (orchestrator.RuntimeSnapshotResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var snap orchestrator.RuntimeSnapshotResult
	if f.fault != nil {
		snap.PlatformFaults = []orchestrator.SnapshotPlatformFault{*f.fault}
	}
	return snap, nil
}

func platformFaultTestServer(t *testing.T, fake *fakePlatformFaults) *httptest.Server {
	t.Helper()
	srv := New(Params{
		SnapshotFn:      fake.snapshot,
		RefreshFn:       acceptingRefresh(),
		Logger:          slog.New(slog.DiscardHandler),
		ControlToken:    testControlToken,
		SupervisorToken: testSupervisorToken,
		SessionTokenFn: func(token string) (string, bool) {
			return "LIVE-1", token == tokenLive1
		},
		PlatformFaultDoneFn: fake.done,
	})
	ts := httptest.NewServer(srv.Mux())
	t.Cleanup(ts.Close)
	return ts
}

func TestState_ListsEachPlatformFault(t *testing.T) {
	t.Parallel()

	ts := platformFaultTestServer(t, &fakePlatformFaults{fault: scopeFault()})
	status, state := callDispatchPause(t, ts, http.MethodGet, "/api/v1/state", "", "")
	if status != http.StatusOK {
		t.Fatalf("GET /api/v1/state status = %d", status)
	}
	faults, _ := state["platform_faults"].([]any)
	if len(faults) != 1 {
		t.Fatalf("platform_faults = %v, want one fault", state["platform_faults"])
	}
	fault := faults[0].(map[string]any)
	for key, want := range map[string]any{
		"id":               "pf-1a2b3c",
		"class":            "token_scope",
		"step":             "export",
		"cause":            "push sortie/ONS-49: [remote rejected] (refusing to allow a Personal Access Token to create or update workflow without `workflow` scope)",
		"recorded_at":      "2026-11-02T10:00:00Z",
		"next_check_at":    "2026-11-02T10:05:00Z",
		"last_check_at":    nil,
		"last_check_error": "",
		"checking":         false,
	} {
		if fault[key] != want {
			t.Errorf("platform_faults[0].%s = %v, want %v", key, fault[key], want)
		}
	}
	action, _ := fault["person_action"].(map[string]any)
	if action["action"] != `Give taichi's GitHub token "Workflows: Read and write"` || action["done_at"] != nil || action["done_by"] != "" {
		t.Errorf("person_action = %v, want the token action, not done", action)
	}
	if action["where"] != "GitHub, Settings > Developer settings > Personal access tokens: the token Sortie on taichi pushes with" {
		t.Errorf("person_action.where = %v", action["where"])
	}
	stories, _ := fault["stories"].([]any)
	if len(stories) != 3 {
		t.Fatalf("stories = %v, want three", fault["stories"])
	}
	for i, want := range []string{"ONS-49", "ONS-50", "ONS-51"} {
		if got := stories[i].(map[string]any)["issue_identifier"]; got != want {
			t.Errorf("stories[%d].issue_identifier = %v, want %s", i, got, want)
		}
	}
	if got := stories[2].(map[string]any)["display_identifier"]; got != "ortusnova/ONS-51" {
		t.Errorf("stories[2].display_identifier = %v", got)
	}

	empty := platformFaultTestServer(t, &fakePlatformFaults{})
	if _, state := callDispatchPause(t, empty, http.MethodGet, "/api/v1/state", "", ""); state["platform_faults"] == nil {
		t.Errorf("platform_faults with no fault = %v, want an empty list", state["platform_faults"])
	}
}

func TestPlatformFaultActionDone_MarksDoneAndChecksAtOnce(t *testing.T) {
	t.Parallel()

	fake := &fakePlatformFaults{fault: scopeFault()}
	ts := platformFaultTestServer(t, fake)
	auth := "Bearer " + testControlToken
	route := "/api/v1/platform-faults/pf-1a2b3c/action-done"

	status, body := callDispatchPause(t, ts, http.MethodPost, route, auth, `{"by":"taichi"}`)
	if status != http.StatusOK || body["resolved"] != false {
		t.Fatalf("POST %s while the token lacks the scope = %d %v, want 200 not resolved", route, status, body)
	}
	fault, _ := body["platform_fault"].(map[string]any)
	action, _ := fault["person_action"].(map[string]any)
	if action["done_by"] != "taichi" || action["done_at"] != "2026-11-02T11:00:00Z" || fault["last_check_error"] != "push refused" {
		t.Errorf("answered fault = %v, want it marked done by taichi and checked", fault)
	}

	fake.mu.Lock()
	fake.tokenHasScope = true
	fake.mu.Unlock()
	status, body = callDispatchPause(t, ts, http.MethodPost, route, auth, "")
	if status != http.StatusOK || body["resolved"] != true {
		t.Fatalf("POST %s once the token has the scope = %d %v, want 200 resolved", route, status, body)
	}
	if _, state := callDispatchPause(t, ts, http.MethodGet, "/api/v1/state", "", ""); len(state["platform_faults"].([]any)) != 0 {
		t.Errorf("platform_faults after the release = %v, want none", state["platform_faults"])
	}

	status, body = callDispatchPause(t, ts, http.MethodPost, route, auth, "")
	if status != http.StatusNotFound {
		t.Errorf("POST %s for a resolved fault = %d %v, want 404", route, status, body)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	want := []string{"pf-1a2b3c by taichi", "pf-1a2b3c by operator", "pf-1a2b3c by operator"}
	if len(fake.calls) != len(want) {
		t.Fatalf("calls = %v, want %v", fake.calls, want)
	}
	for i := range want {
		if fake.calls[i] != want[i] {
			t.Errorf("calls = %v, want %v", fake.calls, want)
		}
	}
}

// A question that asks for a choice takes it in answer: the route passes
// it on with who gave it, serves it back, and answers 400 to a choice the
// question does not take.
func TestPlatformFaultActionDone_CarriesTheAnswer(t *testing.T) {
	t.Parallel()

	fake := &fakePlatformFaults{fault: scopeFault()}
	ts := platformFaultTestServer(t, fake)
	auth := "Bearer " + testControlToken
	route := "/api/v1/platform-faults/pf-1a2b3c/action-done"

	status, body := callDispatchPause(t, ts, http.MethodPost, route, auth, `{"by":"taichi","answer":"maybe"}`)
	if status != http.StatusBadRequest || body["error"].(map[string]any)["code"] != "invalid_answer" {
		t.Errorf("POST %s with an answer the question does not take = %d %v, want 400 invalid_answer", route, status, body)
	}
	status, body = callDispatchPause(t, ts, http.MethodPost, route, auth, `{"by":"taichi","answer":"cancel"}`)
	if status != http.StatusOK {
		t.Fatalf("POST %s with an answer = %d %v, want 200", route, status, body)
	}
	action, _ := body["platform_fault"].(map[string]any)["person_action"].(map[string]any)
	if action["answer"] != "cancel" || action["done_by"] != "taichi" {
		t.Errorf("person_action = %v, want the answer cancel by taichi", action)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if want := []string{"pf-1a2b3c by taichi answer maybe", "pf-1a2b3c by taichi answer cancel"}; len(fake.calls) != 2 || fake.calls[0] != want[0] || fake.calls[1] != want[1] {
		t.Errorf("calls = %v, want %v", fake.calls, want)
	}
}

func TestPlatformFaultActionDone_ControlTokenOnly(t *testing.T) {
	t.Parallel()

	route := "/api/v1/platform-faults/pf-1a2b3c/action-done"
	tests := []struct {
		name, method, authorization, body string
		want                              int
	}{
		{"no token", http.MethodPost, "", "", http.StatusUnauthorized},
		{"a session token", http.MethodPost, "Bearer " + tokenLive1, "", http.StatusForbidden},
		{"the supervisor token answering", http.MethodPost, "Bearer " + testSupervisorToken, `{"by":"supervisor","answer":"requeue","evidence":"rejected"}`, http.StatusForbidden},
		{"GET", http.MethodGet, "Bearer " + testControlToken, "", http.StatusMethodNotAllowed},
		{"malformed body", http.MethodPost, "Bearer " + testControlToken, "{not json", http.StatusBadRequest},
	}
	for _, tt := range tests {
		fake := &fakePlatformFaults{fault: scopeFault()}
		ts := platformFaultTestServer(t, fake)
		if status, body := callDispatchPause(t, ts, tt.method, route, tt.authorization, tt.body); status != tt.want {
			t.Errorf("%s: status = %d, want %d (body %v)", tt.name, status, tt.want, body)
		}
		if len(fake.calls) != 0 {
			t.Errorf("%s: PlatformFaultDoneFn called %v, want never", tt.name, fake.calls)
		}
	}

	ts := testServer(t, fixedSnapshot(orchestrator.RuntimeSnapshotResult{}), acceptingRefresh())
	if status, _ := callDispatchPause(t, ts, http.MethodPost, route, "", ""); status != http.StatusServiceUnavailable {
		t.Errorf("POST without PlatformFaultDoneFn status = %d, want 503", status)
	}
}
