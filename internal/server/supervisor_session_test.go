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

	"github.com/sortie-ai/sortie/internal/orchestrator"
)

// fakeSupervisorPause keeps a supervisor pause the way the orchestrator
// does and serves it in the snapshot.
type fakeSupervisorPause struct {
	mu         sync.Mutex
	configured bool
	pause      *orchestrator.SupervisorPause
}

func (f *fakeSupervisorPause) fn(_ context.Context, pause bool, by, reason string) orchestrator.SupervisorPauseResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.configured {
		return orchestrator.SupervisorPauseResult{Status: orchestrator.SupervisorPauseNotConfigured}
	}
	switch {
	case pause && f.pause == nil:
		f.pause = &orchestrator.SupervisorPause{Since: time.Date(2026, 11, 3, 9, 0, 0, 0, time.UTC), By: by, Reason: reason}
	case !pause:
		f.pause = nil
	}
	var copied *orchestrator.SupervisorPause
	if f.pause != nil {
		copied = new(*f.pause)
	}
	return orchestrator.SupervisorPauseResult{Status: orchestrator.SupervisorPauseDone, Pause: copied}
}

func (f *fakeSupervisorPause) snapshot() (orchestrator.RuntimeSnapshotResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var snap orchestrator.RuntimeSnapshotResult
	if !f.configured {
		return snap, nil
	}
	day := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)
	park := orchestrator.SupervisorTrigger{Kind: orchestrator.SupervisorTriggerPark, IssueID: "id-7", Identifier: "ONS-7", Detail: "parked: agent_blocked", At: day.Add(8 * time.Hour)}
	status := orchestrator.SupervisorStatusIdle
	if f.pause != nil {
		status = orchestrator.SupervisorStatusPaused
	}
	snap.Supervisor = &orchestrator.SupervisorSnapshot{
		Status:      status,
		Agent:       "claude-code",
		Pause:       f.pause,
		LastTrigger: &park,
		LastSession: &orchestrator.SupervisorSession{
			ID: 74, StartedAt: day.Add(8 * time.Hour), EndedAt: day.Add(8*time.Hour + time.Minute),
			Triggers: []orchestrator.SupervisorTrigger{park}, TriggerCount: 1,
			Outcome: orchestrator.SupervisorOutcomeCompleted, Tokens: 1200, Digest: "4f2a9c01d3e5b778", Actions: 0,
		},
		Suspended: []orchestrator.SupervisorSuspension{{
			Kind: orchestrator.SupervisorTriggerIdleSlot, Digest: "9e0b7f1c2a3d4e5f", Sessions: 3,
			Since: day.Add(6 * time.Hour), At: day.Add(7 * time.Hour),
		}},
		Budget: orchestrator.SupervisorBudget{
			Day: day, ResetsAt: day.Add(24 * time.Hour),
			SessionsUsed: 3, SessionsLimit: 48, TokensUsed: 4200, TokensLimit: 2_000_000,
		},
		NextSweepAt: day.Add(9 * time.Hour),
	}
	return snap, nil
}

func supervisorPauseTestServer(t *testing.T, fake *fakeSupervisorPause) *httptest.Server {
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
		SupervisorPauseFn: fake.fn,
	})
	ts := httptest.NewServer(srv.Mux())
	t.Cleanup(ts.Close)
	return ts
}

func TestSupervisorPause_OperatorPausesAndResumes(t *testing.T) {
	t.Parallel()

	fake := &fakeSupervisorPause{configured: true}
	ts := supervisorPauseTestServer(t, fake)
	operator := "Bearer " + testControlToken

	for _, token := range []string{"Bearer " + testSupervisorToken, "Bearer " + tokenLive1} {
		if status, _ := callDispatchPause(t, ts, http.MethodPost, "/api/v1/supervisor/pause", token, ""); status != http.StatusForbidden {
			t.Errorf("pause with a non-operator token = %d, want 403", status)
		}
	}
	if status, _ := callDispatchPause(t, ts, http.MethodPost, "/api/v1/supervisor/pause", "", ""); status != http.StatusUnauthorized {
		t.Errorf("pause without a token = %d, want 401", status)
	}
	if status, _ := callDispatchPause(t, ts, http.MethodGet, "/api/v1/supervisor/pause", operator, ""); status != http.StatusMethodNotAllowed {
		t.Errorf("GET pause = %d, want 405", status)
	}

	status, body := callDispatchPause(t, ts, http.MethodPost, "/api/v1/supervisor/pause", operator, `{"by": "taichi", "reason": "rewriting the prompt"}`)
	paused, _ := body["supervisor_paused"].(map[string]any)
	if status != http.StatusOK || paused["by"] != "taichi" || paused["reason"] != "rewriting the prompt" || paused["since"] != "2026-11-03T09:00:00Z" {
		t.Fatalf("pause = %d %v, want 200 with the pause", status, body)
	}
	_, state := callDispatchPause(t, ts, http.MethodGet, "/api/v1/state", "", "")
	sup, _ := state["supervisor"].(map[string]any)
	if sup["status"] != "paused" || sup["paused"].(map[string]any)["by"] != "taichi" {
		t.Errorf("state supervisor = %v, want paused by taichi", sup)
	}

	status, body = callDispatchPause(t, ts, http.MethodPost, "/api/v1/supervisor/resume", operator, "")
	if status != http.StatusOK || body["supervisor_paused"] != nil {
		t.Fatalf("resume = %d %v, want 200 with supervisor_paused null", status, body)
	}
	_, state = callDispatchPause(t, ts, http.MethodGet, "/api/v1/state", "", "")
	sup, _ = state["supervisor"].(map[string]any)
	if sup["status"] != "idle" || sup["paused"] != nil {
		t.Errorf("state supervisor after resume = %v, want idle", sup)
	}
}

func TestSupervisorState_ServesStatusLastTriggerAndBudget(t *testing.T) {
	t.Parallel()

	ts := supervisorPauseTestServer(t, &fakeSupervisorPause{configured: true})
	_, state := callDispatchPause(t, ts, http.MethodGet, "/api/v1/state", "", "")
	sup, ok := state["supervisor"].(map[string]any)
	if !ok {
		t.Fatalf("supervisor = %#v, want an object", state["supervisor"])
	}
	if sup["status"] != "idle" || sup["agent"] != "claude-code" || sup["running"] != nil || sup["unavailable"] != nil ||
		sup["next_sweep_at"] != "2026-11-03T09:00:00Z" {
		t.Errorf("supervisor = %v, want idle claude-code with the next sweep", sup)
	}
	trigger, _ := sup["last_trigger"].(map[string]any)
	if trigger["kind"] != "park" || trigger["issue_identifier"] != "ONS-7" || trigger["detail"] != "parked: agent_blocked" {
		t.Errorf("last_trigger = %v, want the park of ONS-7", trigger)
	}
	budget, _ := sup["budget"].(map[string]any)
	if budget["sessions_used"] != float64(3) || budget["sessions_limit"] != float64(48) || budget["tokens_used"] != float64(4200) ||
		budget["tokens_limit"] != float64(2_000_000) || budget["resets_at"] != "2026-11-04T00:00:00Z" {
		t.Errorf("budget = %v, want 3 of 48 sessions and 4200 of 2000000 tokens", budget)
	}
	last, _ := sup["last_session"].(map[string]any)
	if last["outcome"] != "completed" || last["tokens"] != float64(1200) || last["trigger_count"] != float64(1) ||
		last["id"] != float64(74) || last["digest"] != "4f2a9c01d3e5b778" || last["actions"] != float64(0) {
		t.Errorf("last_session = %v, want the completed park session", last)
	}
	if pending, _ := sup["pending_triggers"].([]any); pending == nil || len(pending) != 0 {
		t.Errorf("pending_triggers = %#v, want []", sup["pending_triggers"])
	}
	digest, _ := sup["last_digest"].(map[string]any)
	if digest["digest"] != "4f2a9c01d3e5b778" || digest["session_id"] != float64(74) ||
		digest["session_started_at"] != "2026-11-03T08:00:00Z" || digest["trigger_kind"] != "park" {
		t.Errorf("last_digest = %v, want the last session's digest", digest)
	}
	suspended, _ := sup["suspended_triggers"].([]any)
	if len(suspended) != 1 {
		t.Fatalf("suspended_triggers = %#v, want idle_slot", sup["suspended_triggers"])
	}
	if s, _ := suspended[0].(map[string]any); s["kind"] != "idle_slot" || s["digest"] != "9e0b7f1c2a3d4e5f" || s["sessions"] != float64(3) ||
		s["since"] != "2026-11-03T06:00:00Z" || s["suspended_at"] != "2026-11-03T07:00:00Z" {
		t.Errorf("suspended_triggers[0] = %v, want idle_slot after 3 no-ops", s)
	}
}

func TestSupervisorPause_NotConfigured(t *testing.T) {
	t.Parallel()

	ts := supervisorPauseTestServer(t, &fakeSupervisorPause{})
	status, body := callDispatchPause(t, ts, http.MethodPost, "/api/v1/supervisor/pause", "Bearer "+testControlToken, "")
	if errObj, _ := body["error"].(map[string]any); status != http.StatusConflict || errObj["code"] != "supervisor_not_configured" {
		t.Errorf("pause without a supervisor = %d %v, want 409 supervisor_not_configured", status, body)
	}
	_, state := callDispatchPause(t, ts, http.MethodGet, "/api/v1/state", "", "")
	if v, present := state["supervisor"]; !present || v != nil {
		t.Errorf("supervisor = %#v (present %v), want null", v, present)
	}
}

// The question the supervisor raises is served as the platform fault that
// carries it: the action, where, the stories it holds, and what was
// checked.
func TestSupervisorQuestion_ServesThePersonAction(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, 4)

	status, body := h.do(t, http.MethodPost, "/api/v1/supervisor/questions", testSupervisorToken,
		`{"evidence": "two pushes refused", "kind": "credential", "action": "Give taichi's GitHub token \"Workflows: Read and write\"", "where": "GitHub, Settings, Developer settings", "stories": ["ONS-7"], "checked": "the refusal names the workflow scope", "cause": "the push needs the workflow scope"}`)
	if status != http.StatusOK || body["action"] != "ask_person" || body["existing"] != false {
		t.Fatalf("question = %d %v, want 200", status, body)
	}
	fault, _ := body["platform_fault"].(map[string]any)
	action, _ := fault["person_action"].(map[string]any)
	stories, _ := fault["stories"].([]any)
	if fault["class"] != "supervisor_question" || fault["detail"] != "the refusal names the workflow scope" ||
		action["action"] != `Give taichi's GitHub token "Workflows: Read and write"` || action["where"] != "GitHub, Settings, Developer settings" ||
		len(stories) != 1 || stories[0].(map[string]any)["issue_identifier"] != "ONS-7" {
		t.Errorf("platform_fault = %v, want the question with its action, place, story and check", fault)
	}
	if latest := h.supervisorActions(t)[0]; latest["action"] != "ask_person" || latest["issue_identifier"] != "ONS-7" || latest["result"] != "done" {
		t.Errorf("record = %v, want the question recorded against ONS-7", latest)
	}
	h.sup.mu.Lock()
	asked := h.sup.lastQuestion
	h.sup.mu.Unlock()
	if asked.Kind != "credential" || asked.RaisedBy != "supervisor" || asked.Evidence != "two pushes refused" {
		t.Errorf("question asked = %+v, want the kind, the evidence, raised by the supervisor", asked)
	}
	if status, _ := h.do(t, http.MethodPost, "/api/v1/supervisor/questions", testControlToken,
		`{"kind": "decision", "action": "a", "where": "w", "stories": ["ONS-7"], "checked": "c"}`); status != http.StatusOK {
		t.Errorf("question with the control token = %d, want 200", status)
	}
	h.sup.mu.Lock()
	asked = h.sup.lastQuestion
	h.sup.mu.Unlock()
	if asked.RaisedBy != "operator" {
		t.Errorf("question with the control token raised by %q, want operator", asked.RaisedBy)
	}

	status, body = h.do(t, http.MethodPost, "/api/v1/supervisor/questions", testSupervisorToken,
		`{"evidence": "e", "action": "a", "where": "w", "stories": ["ONS-7"]}`)
	if errObj, _ := body["error"].(map[string]any); status != http.StatusBadRequest || errObj["code"] != "invalid_request" {
		t.Errorf("question without what was checked = %d %v, want 400 invalid_request", status, body)
	}
	status, _ = h.do(t, http.MethodPost, "/api/v1/supervisor/questions", testSupervisorToken,
		`{"evidence": "e", "action": "a", "where": "w", "stories": ["ONS-9"], "checked": "c"}`)
	if status != http.StatusNotFound {
		t.Errorf("question for an unknown story = %d, want 404", status)
	}
}

// The supervisor withdraws its question with evidence, optionally with a
// replacement; the withdrawal is recorded, and a question it did not raise
// is refused with a precondition.
func TestSupervisorQuestion_Withdraw(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, 4)

	if status, body := h.do(t, http.MethodPost, "/api/v1/supervisor/questions/pf-q1/withdraw", testSupervisorToken, `{}`); status != http.StatusBadRequest ||
		body["error"].(map[string]any)["code"] != "evidence_required" {
		t.Errorf("withdrawal without evidence = %d %v, want 400 evidence_required", status, body)
	}

	status, body := h.do(t, http.MethodPost, "/api/v1/supervisor/questions/pf-q1/withdraw", testSupervisorToken,
		`{"evidence": "the forge shows o/r#12 merged and reverted"}`)
	if status != http.StatusOK || body["action"] != "withdraw_question" || body["replacement"] != nil {
		t.Fatalf("withdrawal = %d %v, want 200 with no replacement", status, body)
	}
	if fault, _ := body["platform_fault"].(map[string]any); fault["id"] != "pf-q1" || fault["question_kind"] != "live_proof" || fault["raised_by"] != "supervisor" {
		t.Errorf("platform_fault = %v, want the withdrawn question", fault)
	}
	if latest := h.supervisorActions(t)[0]; latest["action"] != "withdraw_question" || latest["issue_identifier"] != "ONS-7" ||
		latest["result"] != "done" || latest["evidence"] != "the forge shows o/r#12 merged and reverted" {
		t.Errorf("record = %v, want the withdrawal with its evidence against ONS-7", latest)
	}

	status, body = h.do(t, http.MethodPost, "/api/v1/supervisor/questions/pf-q1/withdraw", testSupervisorToken,
		`{"evidence": "the revert reopened the story", "replacement": {"kind": "decision", "action": "Decide whether ONS-7 lands again", "where": "the workbench", "stories": ["ONS-7"], "checked": "o/r#12 was reverted"}}`)
	replacement, _ := body["replacement"].(map[string]any)
	if status != http.StatusOK || replacement == nil || replacement["existing"] != false {
		t.Fatalf("withdrawal with a replacement = %d %v, want 200 with the replacement", status, body)
	}
	h.sup.mu.Lock()
	asked := h.sup.lastQuestion
	h.sup.mu.Unlock()
	if asked.Kind != "decision" || asked.RaisedBy != "supervisor" || asked.Evidence != "the revert reopened the story" {
		t.Errorf("replacement = %+v, want the decision raised by the supervisor on the withdrawal's evidence", asked)
	}

	status, body = h.do(t, http.MethodPost, "/api/v1/supervisor/questions/pf-agent/withdraw", testSupervisorToken, `{"evidence": "stale"}`)
	if status != http.StatusConflict || body["error"].(map[string]any)["code"] != "not_supervisor_question" {
		t.Errorf("withdrawal of an agent's fault = %d %v, want 409 not_supervisor_question", status, body)
	}
	if latest := h.supervisorActions(t)[0]; latest["result"] != "rejected" || !strings.HasPrefix(latest["detail"].(string), "not_supervisor_question") {
		t.Errorf("record = %v, want the refused withdrawal", latest)
	}
	status, body = h.do(t, http.MethodPost, "/api/v1/supervisor/questions/pf-gone/withdraw", testSupervisorToken, `{"evidence": "stale"}`)
	if status != http.StatusNotFound || body["error"].(map[string]any)["code"] != "question_not_found" {
		t.Errorf("withdrawal of an unknown question = %d %v, want 404 question_not_found", status, body)
	}
}

// POST /api/v1/issues passes the kind, stories and signature on, and
// serves whether the filing joined an open issue.
func TestSupervisorIssue_ServesTheJoin(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, 4)

	status, body := h.do(t, http.MethodPost, "/api/v1/issues", testSupervisorToken,
		`{"evidence": "deploy.json is an hour old", "title": "Deploy state is stale", "kind": "platform_defect", "signature": "deploy | state | stale"}`)
	if status != http.StatusOK || body["action"] != "file_issue" || body["issue_identifier"] != "ONS-200" || body["existing"] != true {
		t.Errorf("filing = %d %v, want 200 joined to ONS-200", status, body)
	}
	if status, body := h.do(t, http.MethodPost, "/api/v1/issues", testSupervisorToken,
		`{"evidence": "e", "title": "t", "stories": ["ONS-7"]}`); status != http.StatusBadRequest || body["error"].(map[string]any)["code"] != "invalid_request" {
		t.Errorf("filing without a kind = %d %v, want 400 invalid_request", status, body)
	}
}
