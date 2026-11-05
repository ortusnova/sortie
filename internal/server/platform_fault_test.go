package server

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/failcause"
	"github.com/sortie-ai/sortie/internal/orchestrator"
	"github.com/sortie-ai/sortie/internal/persistence"
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

func (f *fakePlatformFaults) done(_ context.Context, id, by, raisedBy string) orchestrator.PlatformFaultDoneResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	call := id + " by " + by
	if raisedBy != "" {
		call += " raised by " + raisedBy
	}
	f.calls = append(f.calls, call)
	if f.fault == nil || f.fault.ID != id {
		return orchestrator.PlatformFaultDoneResult{Status: orchestrator.PlatformFaultDoneNotFound}
	}
	if raisedBy != "" && f.fault.RaisedBy != raisedBy {
		return orchestrator.PlatformFaultDoneResult{Status: orchestrator.PlatformFaultDoneNotRaisedBy, Fault: *f.fault}
	}
	answered := *f.fault
	answered.ActionDoneAt, answered.ActionDoneBy = time.Date(2026, 11, 2, 11, 0, 0, 0, time.UTC), by
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
	return platformFaultServer(t, fake, nil)
}

// platformFaultServer serves fake with the control, supervisor and LIVE-1
// session tokens, recording supervisor actions in log.
func platformFaultServer(t *testing.T, fake *fakePlatformFaults, log SupervisorLog) *httptest.Server {
	t.Helper()
	srv := New(Params{
		SnapshotFn:      fake.snapshot,
		RefreshFn:       acceptingRefresh(),
		Logger:          slog.New(slog.DiscardHandler),
		ControlToken:    testControlToken,
		SupervisorToken: testSupervisorToken,
		SupervisorLog:   log,
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

func TestPlatformFaultActionDone_RefusedRequests(t *testing.T) {
	t.Parallel()

	route := "/api/v1/platform-faults/pf-1a2b3c/action-done"
	tests := []struct {
		name, method, authorization, body string
		want                              int
	}{
		{"no token", http.MethodPost, "", "", http.StatusUnauthorized},
		{"a session token", http.MethodPost, "Bearer " + tokenLive1, "", http.StatusForbidden},
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

// supervisorQuestionFault is a question the supervisor raised that holds
// ONS-7.
func supervisorQuestionFault() *orchestrator.SnapshotPlatformFault {
	at := time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)
	return &orchestrator.SnapshotPlatformFault{
		ID:           "pf-q1",
		Class:        orchestrator.PlatformFaultClassSupervisorQuestion,
		Step:         "supervisor",
		Action:       &failcause.PersonAction{Action: "Run the live proof of o/r#12 and record it", Where: "the homelab"},
		QuestionKind: "live_proof",
		RaisedBy:     orchestrator.QuestionRaisedBySupervisor,
		RecordedAt:   at,
		NextCheckAt:  at.Add(orchestrator.PlatformFaultCheckInterval),
		Holds:        []orchestrator.PlatformFaultHold{{IssueID: "id-7", Identifier: "ONS-7", HeldAt: at}},
	}
}

func openSupervisorLog(t *testing.T) *persistence.Store {
	t.Helper()
	store, err := persistence.Open(t.Context(), t.TempDir()+"/sortie.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Migrate(t.Context()); err != nil {
		t.Fatal(err)
	}
	return store
}

// Scenario "A question the supervisor settled itself": the supervisor
// token marks done, with its evidence, the action of a question it
// raised; the action is done by the supervisor, the fault is checked at
// once, and the request is recorded as fault_action_done.
func TestPlatformFaultActionDone_SupervisorClosesItsOwnQuestion(t *testing.T) {
	t.Parallel()
	fake := &fakePlatformFaults{fault: supervisorQuestionFault(), tokenHasScope: true}
	store := openSupervisorLog(t)
	ts := platformFaultServer(t, fake, store)
	route := "/api/v1/platform-faults/pf-q1/action-done"

	status, body := callDispatchPause(t, ts, http.MethodPost, route, "Bearer "+testSupervisorToken,
		`{"by": "taichi", "evidence": "the live-proof job of o/r#12 recorded sortie:live-proof proven abc123"}`)
	if status != http.StatusOK || body["resolved"] != true {
		t.Fatalf("POST %s with the supervisor token = %d %v, want 200 resolved", route, status, body)
	}
	fault, _ := body["platform_fault"].(map[string]any)
	if action, _ := fault["person_action"].(map[string]any); action["done_by"] != "supervisor" {
		t.Errorf("person_action = %v, want it done by the supervisor whatever the body names", action)
	}
	fake.mu.Lock()
	calls := append([]string(nil), fake.calls...)
	fake.mu.Unlock()
	if len(calls) != 1 || calls[0] != "pf-q1 by supervisor raised by supervisor" {
		t.Errorf("calls = %v, want one mark by the supervisor of a fault it raised", calls)
	}

	actions, err := store.RecentSupervisorActions(t.Context(), 10)
	if err != nil || len(actions) != 1 {
		t.Fatalf("supervisor actions = %+v, %v, want one", actions, err)
	}
	a := actions[0]
	if a.Action != "fault_action_done" || a.Result != persistence.SupervisorDone || a.IssueIdentifier != "ONS-7" ||
		a.Evidence != "the live-proof job of o/r#12 recorded sortie:live-proof proven abc123" || a.Route != "POST "+route {
		t.Errorf("record = %+v, want fault_action_done for ONS-7 with its evidence", a)
	}
}

// The control token marks a supervisor's question done as it does any
// fault, with no evidence, and records no supervisor action.
func TestPlatformFaultActionDone_ControlTokenOnASupervisorQuestion(t *testing.T) {
	t.Parallel()
	fake := &fakePlatformFaults{fault: supervisorQuestionFault(), tokenHasScope: true}
	store := openSupervisorLog(t)
	ts := platformFaultServer(t, fake, store)

	status, body := callDispatchPause(t, ts, http.MethodPost, "/api/v1/platform-faults/pf-q1/action-done", "Bearer "+testControlToken, `{"by": "taichi"}`)
	if status != http.StatusOK || body["resolved"] != true {
		t.Fatalf("control token = %d %v, want 200 resolved", status, body)
	}
	if action, _ := body["platform_fault"].(map[string]any)["person_action"].(map[string]any); action["done_by"] != "taichi" {
		t.Errorf("person_action = %v, want it done by taichi", action)
	}
	if actions, err := store.RecentSupervisorActions(t.Context(), 10); err != nil || len(actions) != 0 {
		t.Errorf("supervisor actions = %+v, %v, want none for the control token", actions, err)
	}
}

// Scenario "A fault Sortie raised": the supervisor token marking done the
// action of a fault a run's failure, a check or a person raised is
// refused 403 not_raised_by_supervisor, recorded as refused, and the fault
// waits for a person, whose control token still marks it.
func TestPlatformFaultActionDone_SupervisorRefusedOnAFaultItDidNotRaise(t *testing.T) {
	t.Parallel()
	byOperator := supervisorQuestionFault()
	byOperator.RaisedBy = orchestrator.QuestionRaisedByOperator
	healthCheck := scopeFault()
	healthCheck.Step, healthCheck.Class = "health", "credentials"

	for name, fault := range map[string]*orchestrator.SnapshotPlatformFault{
		"a run's failure": scopeFault(),
		"a check":         healthCheck,
		"the operator":    byOperator,
	} {
		fake := &fakePlatformFaults{fault: fault, tokenHasScope: true}
		store := openSupervisorLog(t)
		ts := platformFaultServer(t, fake, store)
		route := "/api/v1/platform-faults/" + fault.ID + "/action-done"

		status, body := callDispatchPause(t, ts, http.MethodPost, route, "Bearer "+testSupervisorToken, `{"evidence": "I did the action"}`)
		if errObj, _ := body["error"].(map[string]any); status != http.StatusForbidden || errObj["code"] != "not_raised_by_supervisor" {
			t.Errorf("%s: supervisor token = %d %v, want 403 not_raised_by_supervisor", name, status, body)
		}
		if _, state := callDispatchPause(t, ts, http.MethodGet, "/api/v1/state", "", ""); len(state["platform_faults"].([]any)) != 1 {
			t.Errorf("%s: platform_faults = %v, want the fault still open", name, state["platform_faults"])
		} else if action := state["platform_faults"].([]any)[0].(map[string]any)["person_action"].(map[string]any); action["done_at"] != nil {
			t.Errorf("%s: person_action = %v, want it not done", name, action)
		}
		actions, err := store.RecentSupervisorActions(t.Context(), 10)
		if err != nil || len(actions) != 1 || actions[0].Action != "fault_action_done" || actions[0].Result != persistence.SupervisorRefused ||
			actions[0].Evidence != "I did the action" || !strings.HasPrefix(actions[0].Detail, "not_raised_by_supervisor") {
			t.Errorf("%s: records = %+v, %v, want the refusal with its evidence", name, actions, err)
		}

		if status, body := callDispatchPause(t, ts, http.MethodPost, route, "Bearer "+testControlToken, ""); status != http.StatusOK || body["resolved"] != true {
			t.Errorf("%s: control token after the refusal = %d %v, want 200 resolved", name, status, body)
		}
	}
}

// A supervisor request without evidence is refused 400 evidence_required,
// recorded as rejected, and marks nothing.
func TestPlatformFaultActionDone_SupervisorNeedsEvidence(t *testing.T) {
	t.Parallel()
	fake := &fakePlatformFaults{fault: supervisorQuestionFault(), tokenHasScope: true}
	store := openSupervisorLog(t)
	ts := platformFaultServer(t, fake, store)

	for _, body := range []string{"", `{}`, `{"evidence": "  "}`} {
		status, resp := callDispatchPause(t, ts, http.MethodPost, "/api/v1/platform-faults/pf-q1/action-done", "Bearer "+testSupervisorToken, body)
		if errObj, _ := resp["error"].(map[string]any); status != http.StatusBadRequest || errObj["code"] != "evidence_required" {
			t.Errorf("body %q: = %d %v, want 400 evidence_required", body, status, resp)
		}
	}
	fake.mu.Lock()
	calls := len(fake.calls)
	fake.mu.Unlock()
	if calls != 0 {
		t.Errorf("PlatformFaultDoneFn called %d times without evidence, want never", calls)
	}
	actions, err := store.RecentSupervisorActions(t.Context(), 10)
	if err != nil || len(actions) != 3 {
		t.Fatalf("records = %+v, %v, want three rejections", actions, err)
	}
	for _, a := range actions {
		if a.Action != "fault_action_done" || a.Result != persistence.SupervisorRejected {
			t.Errorf("record = %+v, want a rejected fault_action_done", a)
		}
	}
}
