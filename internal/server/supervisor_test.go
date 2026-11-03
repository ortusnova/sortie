package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/orchestrator"
	"github.com/sortie-ai/sortie/internal/parkanswer"
	"github.com/sortie-ai/sortie/internal/persistence"
)

const testSupervisorToken = "supervisor-token-5678"

// fakeSupervisor answers every supervisor action as done for ONS-7, whose
// pull request is o/r#12 merged at abc123, and records each call.
type fakeSupervisor struct {
	mu    sync.Mutex
	calls []string
}

func (f *fakeSupervisor) called(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, name)
}

func (f *fakeSupervisor) callsMade() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *fakeSupervisor) result(identifier string) orchestrator.SupervisorResult {
	if identifier != "ONS-7" {
		return orchestrator.SupervisorResult{Status: orchestrator.SupervisorUnknownIssue, Message: "issue " + identifier + " is not known"}
	}
	return orchestrator.SupervisorResult{Status: orchestrator.SupervisorDone, IssueID: "id-7", Identifier: "ONS-7"}
}

func (f *fakeSupervisor) ReleasePark(_ context.Context, id string) orchestrator.SupervisorResult {
	f.called("release")
	return f.result(id)
}

func (f *fakeSupervisor) RecordMerge(_ context.Context, id, pr, commit string) orchestrator.SupervisorResult {
	f.called("record_merge " + pr + " " + commit)
	r := f.result(id)
	if r.Status == orchestrator.SupervisorDone {
		if pr != "o/r#12" || commit != "abc123" {
			return orchestrator.SupervisorResult{Status: orchestrator.SupervisorPrecondition, Code: "commit_mismatch", Message: "the forge shows o/r#12 merged at abc123", IssueID: "id-7", Identifier: id}
		}
		r.PullRequest, r.MergeCommit = pr, commit
	}
	return r
}

func (f *fakeSupervisor) FixRun(_ context.Context, id, message string) orchestrator.SupervisorResult {
	f.called("fix_run " + message)
	return f.result(id)
}

func (f *fakeSupervisor) MarkDraft(_ context.Context, id string) orchestrator.SupervisorResult {
	f.called("mark_draft")
	return f.result(id)
}

func (f *fakeSupervisor) Merge(_ context.Context, id string) orchestrator.SupervisorResult {
	f.called("merge")
	return f.result(id)
}

func (f *fakeSupervisor) FileIssue(_ context.Context, title, _ string) orchestrator.SupervisorResult {
	f.called("file_issue " + title)
	return orchestrator.SupervisorResult{Status: orchestrator.SupervisorDone, IssueID: "id-200", Identifier: "ONS-200"}
}

func (f *fakeSupervisor) RequestLiveProof(_ context.Context, id string) orchestrator.SupervisorResult {
	f.called("live_proof")
	return f.result(id)
}

type supervisorHarness struct {
	ts       *httptest.Server
	store    *persistence.Store
	sup      *fakeSupervisor
	grants   *fakeSessionGrants
	answers  sync.Map
	operator sync.Map // operator handlers reached, by name
}

func newSupervisorHarness(t *testing.T, sessionCap int) *supervisorHarness {
	t.Helper()
	ctx := context.Background()
	store, err := persistence.Open(ctx, t.TempDir()+"/sortie.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	h := &supervisorHarness{store: store, sup: &fakeSupervisor{}, grants: &fakeSessionGrants{maxSessions: 2}}
	reached := func(name string) { h.operator.Store(name, true) }
	planControl := func(context.Context, string) orchestrator.ControlReply {
		reached("plan")
		return orchestrator.ControlReply{Outcome: orchestrator.ControlDone}
	}
	srv := New(Params{
		SnapshotFn:      h.grants.snapshot,
		RefreshFn:       func() bool { reached("refresh"); return true },
		Logger:          slog.New(slog.DiscardHandler),
		ControlToken:    testControlToken,
		SupervisorToken: testSupervisorToken,
		SessionTokenFn: func(token string) (string, bool) {
			return "ONS-7", token == tokenLive1
		},
		SupervisorSessionCap: sessionCap,
		Supervisor:           h.sup,
		SupervisorLog:        store,
		SessionGrantFn:       h.grants.fn,
		AnswerFn: func(identifier string, answer parkanswer.Answer) orchestrator.ControlReply {
			h.answers.Store(identifier, answer.Text)
			return orchestrator.ControlReply{Outcome: orchestrator.ControlDone, Answer: orchestrator.ParkAnswer{Identifier: identifier}}
		},
		HoldFn: func(string, string, string) orchestrator.ControlReply {
			reached("hold")
			return orchestrator.ControlReply{Outcome: orchestrator.ControlDone}
		},
		ReleaseFn: func(string) orchestrator.ControlReply {
			reached("release")
			return orchestrator.ControlReply{Outcome: orchestrator.ControlDone}
		},
		EscalateFn: func(string, orchestrator.EscalationRequest) orchestrator.ControlReply {
			reached("escalate")
			return orchestrator.ControlReply{Outcome: orchestrator.ControlDone}
		},
		PullBackFn:   planControl,
		PlanCancelFn: planControl,
	})
	h.ts = httptest.NewServer(srv.Mux())
	t.Cleanup(h.ts.Close)
	return h
}

func (h *supervisorHarness) do(t *testing.T, method, path, token, body string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, h.ts.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := h.ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // test
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

// supervisorActions returns supervisor_actions as GET /api/v1/state serves
// them.
func (h *supervisorHarness) supervisorActions(t *testing.T) []map[string]any {
	t.Helper()
	status, state := h.do(t, http.MethodGet, "/api/v1/state", "", "")
	if status != http.StatusOK {
		t.Fatalf("GET /api/v1/state = %d", status)
	}
	raw, ok := state["supervisor_actions"].([]any)
	if !ok {
		t.Fatalf("supervisor_actions = %#v, want an array", state["supervisor_actions"])
	}
	out := make([]map[string]any, len(raw))
	for i, a := range raw {
		out[i] = a.(map[string]any)
	}
	return out
}

// Scenario "A refused action": the supervisor tries to approve a story,
// Sortie refuses it, and the refusal is recorded.
func TestSupervisor_RefusedActionIsRecorded(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, 4)

	approve := `{"apply": true, "preview_id": "p1", "evidence": "the review session passed every criterion"}`
	status, body := h.do(t, http.MethodPost, "/api/v1/plan/sync", testSupervisorToken, approve)
	if status != http.StatusForbidden {
		t.Fatalf("approve with the supervisor token = %d, want 403", status)
	}
	if errObj, _ := body["error"].(map[string]any); errObj["code"] != "forbidden" {
		t.Errorf("error = %v, want forbidden", body["error"])
	}

	actions := h.supervisorActions(t)
	if len(actions) != 1 {
		t.Fatalf("supervisor_actions = %v, want the refusal", actions)
	}
	a := actions[0]
	if a["action"] != "POST /api/v1/plan/sync" || a["result"] != persistence.SupervisorRefused || a["status"] != float64(403) ||
		a["evidence"] != "the review session passed every criterion" || a["at"] == nil {
		t.Errorf("record = %v, want the refused approval with its evidence and time", a)
	}
}

// Every other operator route answers the supervisor token 403, reaches no
// handler, and records the refusal.
func TestSupervisor_EveryOtherOperatorRouteIsForbidden(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, 4)

	routes := []struct{ method, path string }{
		{http.MethodPost, "/api/v1/refresh"},
		{http.MethodPost, "/api/v1/dispatch/pause"},
		{http.MethodPost, "/api/v1/dispatch/resume"},
		{http.MethodPost, "/api/v1/retries/ONS-7/run"},
		{http.MethodPost, "/api/v1/attention/0123456789abcdef0123456789abcdef/acknowledge"},
		{http.MethodPost, "/api/v1/reactions/ONS-7/ci/check"},
		{http.MethodPost, "/api/v1/tokens/introspect"},
		{http.MethodPost, "/api/v1/plan/sync"},
		{http.MethodPost, "/api/v1/plan/pull-back"},
		{http.MethodPost, "/api/v1/plan/cancel"},
		{http.MethodGet, "/api/v1/plan/graph"},
		{http.MethodGet, "/api/v1/plan/capabilities"},
		{http.MethodGet, "/api/v1/plan/projects/ONS"},
		{http.MethodPut, "/api/v1/plan/projects/ONS"},
		{http.MethodPost, "/api/v1/ONS-7/hold"},
		{http.MethodPost, "/api/v1/ONS-7/release"},
		{http.MethodPost, "/api/v1/ONS-7/escalate"},
		{http.MethodPost, "/api/v1/ONS-7/broker"},
		{http.MethodGet, "/api/v1/ONS-7/broker"},
	}
	for _, rt := range routes {
		if status, _ := h.do(t, rt.method, rt.path, testSupervisorToken, `{"evidence": "e"}`); status != http.StatusForbidden {
			t.Errorf("%s %s with the supervisor token = %d, want 403", rt.method, rt.path, status)
		}
	}
	h.operator.Range(func(name, _ any) bool {
		t.Errorf("operator handler %v reached with the supervisor token", name)
		return true
	})
	if calls := h.sup.callsMade(); len(calls) != 0 {
		t.Errorf("supervisor actions called = %v, want none", calls)
	}
	actions, err := h.store.RecentSupervisorActions(t.Context(), 100)
	if err != nil || len(actions) != len(routes) {
		t.Fatalf("records = %d, %v, want one per refused request (%d)", len(actions), err, len(routes))
	}
	for _, a := range actions {
		if a.Result != persistence.SupervisorRefused || a.Status != http.StatusForbidden {
			t.Errorf("record %+v, want refused with 403", a)
		}
	}

	// The control token still reaches those routes.
	if status, _ := h.do(t, http.MethodPost, "/api/v1/plan/pull-back", testControlToken, `{"plan_key": "k"}`); status != http.StatusOK {
		t.Errorf("pull-back with the control token = %d, want 200", status)
	}
}

// Scenario "An action and its evidence": the supervisor records the merge
// of a parked story, and the record names the pull request, the merge
// commit, and the time, beside the story's identifier.
func TestSupervisor_ActionIsRecordedWithItsEvidence(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, 4)

	before := time.Now().UTC().Add(-time.Second)
	status, body := h.do(t, http.MethodPost, "/api/v1/ONS-7/merge-record", testSupervisorToken,
		`{"pull_request": "o/r#12", "commit": "abc123", "evidence": "gh shows o/r#12 merged by the operator"}`)
	if status != http.StatusOK || body["merge_commit"] != "abc123" || body["pull_request"] != "o/r#12" || body["action"] != "record_merge" {
		t.Fatalf("record merge = %d %v, want 200 with the pull request and commit", status, body)
	}

	actions := h.supervisorActions(t)
	if len(actions) != 1 {
		t.Fatalf("supervisor_actions = %v, want one", actions)
	}
	a := actions[0]
	if a["action"] != "record_merge" || a["issue_identifier"] != "ONS-7" || a["pull_request"] != "o/r#12" ||
		a["merge_commit"] != "abc123" || a["result"] != persistence.SupervisorDone || a["detail"] != nil ||
		a["evidence"] != "gh shows o/r#12 merged by the operator" {
		t.Errorf("record = %v, want the merge of ONS-7 with its evidence", a)
	}
	at, err := time.Parse(time.RFC3339Nano, a["at"].(string))
	if err != nil || at.Before(before) {
		t.Errorf("at = %v, %v, want the time of the action", a["at"], err)
	}

	// A merge the forge does not show is rejected, and recorded so.
	status, _ = h.do(t, http.MethodPost, "/api/v1/ONS-7/merge-record", testSupervisorToken,
		`{"pull_request": "o/r#12", "commit": "fff000", "evidence": "I think it merged"}`)
	if status != http.StatusConflict {
		t.Errorf("record of another commit = %d, want 409", status)
	}
	if latest := h.supervisorActions(t)[0]; latest["result"] != persistence.SupervisorRejected || !strings.HasPrefix(latest["detail"].(string), "commit_mismatch") {
		t.Errorf("latest record = %v, want the rejection with its cause", latest)
	}
}

// The supervisor token reaches each of its actions, each needs evidence,
// and each is recorded.
func TestSupervisor_AllowedActions(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, 4)

	for _, tt := range []struct {
		path, body, action, call string
	}{
		{"/api/v1/ONS-7/answer", `{"text": "use the v2 api", "author": "supervisor"}`, "answer", ""},
		{"/api/v1/ONS-7/unpark", `{}`, "release", "release"},
		{"/api/v1/ONS-68/sessions", `{"add": 2}`, "grant_sessions", ""},
		{"/api/v1/ONS-7/merge-record", `{"pull_request": "o/r#12", "commit": "abc123"}`, "record_merge", "record_merge o/r#12 abc123"},
		{"/api/v1/ONS-7/fix", `{"message": "rename the column"}`, "fix_run", "fix_run rename the column"},
		{"/api/v1/ONS-7/draft", `{}`, "mark_draft", "mark_draft"},
		{"/api/v1/ONS-7/merge", `{}`, "merge", "merge"},
		{"/api/v1/issues", `{"title": "Flaky seed", "body": "seen twice"}`, "file_issue", "file_issue Flaky seed"},
		{"/api/v1/ONS-7/live-proof", `{}`, "live_proof", "live_proof"},
	} {
		without := len(h.sup.callsMade())
		if status, body := h.do(t, http.MethodPost, tt.path, testSupervisorToken, tt.body); status != http.StatusBadRequest ||
			body["error"].(map[string]any)["code"] != "evidence_required" {
			t.Errorf("%s without evidence = %d %v, want 400 evidence_required", tt.path, status, body)
		}
		if len(h.sup.callsMade()) != without {
			t.Errorf("%s ran without evidence", tt.path)
		}
		withEvidence := strings.Replace(tt.body, "{", `{"evidence": "seen in run 4", `, 1)
		withEvidence = strings.Replace(withEvidence, ", }", "}", 1)
		if status, body := h.do(t, http.MethodPost, tt.path, testSupervisorToken, withEvidence); status != http.StatusOK {
			t.Errorf("%s with evidence = %d %v, want 200", tt.path, status, body)
		}
		latest := h.supervisorActions(t)[0]
		if latest["action"] != tt.action || latest["result"] != persistence.SupervisorDone || latest["evidence"] != "seen in run 4" {
			t.Errorf("%s record = %v, want %s done with its evidence", tt.path, latest, tt.action)
		}
		if tt.call != "" {
			if calls := h.sup.callsMade(); calls[len(calls)-1] != tt.call {
				t.Errorf("%s called %v, want %q last", tt.path, calls, tt.call)
			}
		}
	}
	if text, _ := h.answers.Load("ONS-7"); text != "use the v2 api" {
		t.Errorf("answer = %v, want the supervisor's text", text)
	}
	if grants := h.grants.calls(); len(grants) != 1 || grants[0].By != "supervisor" || grants[0].Cap != 4 {
		t.Errorf("grants = %+v, want one by the supervisor capped at 4", grants)
	}
	if n := len(h.supervisorActions(t)); n != 18 {
		t.Errorf("records = %d, want 18: a rejection and an action per route", n)
	}

	// The control token reaches the same routes, needs no evidence and
	// records nothing.
	if status, _ := h.do(t, http.MethodPost, "/api/v1/ONS-7/merge", testControlToken, `{}`); status != http.StatusOK {
		t.Errorf("merge with the control token = %d, want 200", status)
	}
	if status, _ := h.do(t, http.MethodPost, "/api/v1/ONS-7/merge", tokenLive1, `{}`); status != http.StatusForbidden {
		t.Errorf("merge with a session token = %d, want 403", status)
	}
	if n := len(h.supervisorActions(t)); n != 18 {
		t.Errorf("records after operator calls = %d, want still 18", n)
	}
}

func TestSupervisor_SessionCap(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, 0)
	status, body := h.do(t, http.MethodPost, "/api/v1/ONS-68/sessions", testSupervisorToken, `{"add": 1, "evidence": "e"}`)
	if status != http.StatusConflict || body["error"].(map[string]any)["code"] != "over_cap" {
		t.Errorf("supervisor grant with no cap = %d %v, want 409 over_cap", status, body)
	}
	if len(h.grants.calls()) != 0 {
		t.Error("grant made past a zero cap")
	}
	if status, _ := h.do(t, http.MethodPost, "/api/v1/ONS-68/sessions", testControlToken, `{"add": 1}`); status != http.StatusOK {
		t.Errorf("operator grant = %d, want 200", status)
	}
	if grants := h.grants.calls(); len(grants) != 1 || grants[0].Cap != 0 || grants[0].By != "operator" {
		t.Errorf("operator grant = %+v, want uncapped", grants)
	}
}
