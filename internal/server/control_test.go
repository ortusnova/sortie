package server

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/orchestrator"
)

type recordedHold struct {
	identifier, reason, holder string
	req                        orchestrator.HoldRequest
}

func controlTestServer(t *testing.T, reply orchestrator.ControlReply, holds *[]recordedHold) *httptest.Server {
	t.Helper()
	srv := New(Params{
		SnapshotFn: fixedSnapshot(orchestrator.RuntimeSnapshotResult{}),
		RefreshFn:  acceptingRefresh(),
		Logger:     slog.New(slog.DiscardHandler),
		HoldFn: func(identifier string, req orchestrator.HoldRequest) orchestrator.ControlReply {
			*holds = append(*holds, recordedHold{identifier, req.Reason, req.Holder, req})
			return reply
		},
		ReleaseFn: func(identifier string) orchestrator.ControlReply {
			*holds = append(*holds, recordedHold{identifier: identifier})
			return reply
		},
	})
	ts := httptest.NewServer(srv.Mux())
	t.Cleanup(ts.Close)
	return ts
}

func postControl(t *testing.T, ts *httptest.Server, method, path, body string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // test cleanup
	raw, _ := io.ReadAll(resp.Body)
	var decoded map[string]any
	_ = json.Unmarshal(raw, &decoded)
	return resp.StatusCode, decoded
}

func TestHoldAndRelease_StatusCodes(t *testing.T) {
	t.Parallel()

	since := time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)
	held := orchestrator.ControlReply{Outcome: orchestrator.ControlDone, Hold: orchestrator.HoldState{
		Identifier: "LIVE-1", IssueID: "id-1", Held: true, Reason: "human takeover", Holder: "acp-broker", HeldSince: since, HeldTotal: 90 * time.Second,
	}}
	tests := []struct {
		name       string
		route      string
		reply      orchestrator.ControlReply
		wantStatus int
		wantCode   string
	}{
		{"hold a running issue", "hold", held, http.StatusOK, ""},
		{"release", "release", orchestrator.ControlReply{Outcome: orchestrator.ControlDone, Hold: orchestrator.HoldState{Identifier: "LIVE-1", HeldTotal: 90 * time.Second}}, http.StatusOK, ""},
		{"hold an issue that is not running", "hold", orchestrator.ControlReply{Outcome: orchestrator.ControlNotRunning}, http.StatusConflict, "issue_not_running"},
		{"release an issue that is not running", "release", orchestrator.ControlReply{Outcome: orchestrator.ControlNotRunning}, http.StatusConflict, "issue_not_running"},
		{"hold an unknown issue", "hold", orchestrator.ControlReply{Outcome: orchestrator.ControlUnknownIssue}, http.StatusNotFound, "issue_not_found"},
		{"release an unknown issue", "release", orchestrator.ControlReply{Outcome: orchestrator.ControlUnknownIssue}, http.StatusNotFound, "issue_not_found"},
		{"orchestrator unavailable", "hold", orchestrator.ControlReply{Outcome: orchestrator.ControlUnavailable}, http.StatusServiceUnavailable, "orchestrator_unavailable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var calls []recordedHold
			ts := controlTestServer(t, tt.reply, &calls)
			status, body := postControl(t, ts, http.MethodPost, "/api/v1/LIVE-1/"+tt.route, `{"reason":"human takeover","holder":"acp-broker"}`)
			if status != tt.wantStatus {
				t.Fatalf("POST %s status = %d, want %d (body %v)", tt.route, status, tt.wantStatus, body)
			}
			if tt.wantCode != "" {
				if errObj, _ := body["error"].(map[string]any); errObj["code"] != tt.wantCode {
					t.Errorf("error body = %v, want code %q", body, tt.wantCode)
				}
				return
			}
			if body["issue_identifier"] != "LIVE-1" || body["held_total_ms"] != float64(90000) {
				t.Errorf("body = %v, want issue_identifier LIVE-1 and held_total_ms 90000", body)
			}
			if tt.route == "hold" {
				if body["held"] != true || body["hold_reason"] != "human takeover" || body["holder"] != "acp-broker" || body["held_since"] != "2026-10-11T10:00:00Z" {
					t.Errorf("hold body = %v, want the held state", body)
				}
				if len(calls) != 1 || calls[0].identifier != "LIVE-1" || calls[0].reason != "human takeover" || calls[0].holder != "acp-broker" {
					t.Errorf("HoldFn calls = %+v, want one for LIVE-1 with the body's reason and holder", calls)
				}
			} else if body["held"] != false || body["held_since"] != nil {
				t.Errorf("release body = %v, want not held and a null held_since", body)
			}
		})
	}
}

func TestHold_BodyHandling(t *testing.T) {
	t.Parallel()

	ok := orchestrator.ControlReply{Outcome: orchestrator.ControlDone}
	var calls []recordedHold
	ts := controlTestServer(t, ok, &calls)

	if status, _ := postControl(t, ts, http.MethodPost, "/api/v1/LIVE-1/hold", ""); status != http.StatusOK {
		t.Errorf("hold with an empty body status = %d, want 200", status)
	}
	if status, _ := postControl(t, ts, http.MethodPost, "/api/v1/LIVE-1/hold", "{not json"); status != http.StatusBadRequest {
		t.Errorf("hold with a malformed body status = %d, want 400", status)
	}
	if status, _ := postControl(t, ts, http.MethodGet, "/api/v1/LIVE-1/hold", ""); status != http.StatusMethodNotAllowed {
		t.Errorf("GET hold status = %d, want 405", status)
	}
	if len(calls) != 1 {
		t.Errorf("HoldFn calls = %d, want 1 (only the well-formed request)", len(calls))
	}
}

func TestHold_LeaseRequestAndReply(t *testing.T) {
	t.Parallel()

	since := time.Date(2026, 11, 3, 13, 0, 0, 0, time.UTC)
	leased := orchestrator.ControlReply{Outcome: orchestrator.ControlDone, Hold: orchestrator.HoldState{
		Identifier: "ONS-54", IssueID: "id-54", Held: true, Reason: "full vm chain proof running", Holder: "supervisor", HeldSince: since,
		Lease: &orchestrator.StandingHold{
			Identifier: "ONS-54", Holder: "supervisor", Reason: "full vm chain proof running", Evidence: "sandbox hl-ons-54-proof-1",
			Since: since, RenewedAt: since, Until: since.Add(time.Hour),
		},
	}}
	var calls []recordedHold
	ts := controlTestServer(t, leased, &calls)

	status, body := postControl(t, ts, http.MethodPost, "/api/v1/ONS-54/hold",
		`{"by":"supervisor","reason":"full vm chain proof running","evidence":"sandbox hl-ons-54-proof-1","until":"2026-11-03T14:00:00Z"}`)
	if status != http.StatusOK {
		t.Fatalf("hold status = %d, want 200 (body %v)", status, body)
	}
	want := orchestrator.HoldRequest{Reason: "full vm chain proof running", Holder: "supervisor", Evidence: "sandbox hl-ons-54-proof-1", Until: since.Add(time.Hour)}
	if len(calls) != 1 || calls[0].req != want {
		t.Errorf("HoldFn calls = %+v, want one with %+v", calls, want)
	}
	lease, _ := body["lease"].(map[string]any)
	if lease["by"] != "supervisor" || lease["evidence"] != "sandbox hl-ons-54-proof-1" || lease["until"] != "2026-11-03T14:00:00Z" ||
		lease["renewed_at"] != "2026-11-03T13:00:00Z" || lease["expired"] != false || lease["expired_at"] != nil {
		t.Errorf("lease = %v, want the lease with by, evidence, renewed_at, and until", lease)
	}

	if status, _ := postControl(t, ts, http.MethodPost, "/api/v1/ONS-54/hold", `{"evidence":"e","minutes":30}`); status != http.StatusOK {
		t.Errorf("hold with minutes status = %d, want 200", status)
	}
	if len(calls) != 2 || calls[1].req.For != 30*time.Minute {
		t.Errorf("HoldFn calls = %+v, want the second for 30 minutes", calls)
	}
	for _, bad := range []string{`{"evidence":"e","until":"tomorrow"}`, `{"evidence":"e","minutes":-1}`} {
		if status, _ := postControl(t, ts, http.MethodPost, "/api/v1/ONS-54/hold", bad); status != http.StatusBadRequest {
			t.Errorf("hold %s status = %d, want 400", bad, status)
		}
	}
	if len(calls) != 2 {
		t.Errorf("HoldFn calls = %d, want 2 (no call for a bad body)", len(calls))
	}
}

func TestStateAndDetail_ShowHeldRuns(t *testing.T) {
	t.Parallel()

	since := time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)
	snap := orchestrator.RuntimeSnapshotResult{
		GeneratedAt: since.Add(time.Minute),
		Running: []orchestrator.SnapshotRunningEntry{
			{IssueID: "id-1", Identifier: "LIVE-1", Held: true, HoldReason: "human takeover", Holder: "acp-broker", HeldSince: since, HeldTotal: time.Minute},
			{IssueID: "id-2", Identifier: "LIVE-2"},
		},
	}
	ts := testServer(t, fixedSnapshot(snap), acceptingRefresh())

	_, state := postControl(t, ts, http.MethodGet, "/api/v1/state", "")
	running, _ := state["running"].([]any)
	if len(running) != 2 {
		t.Fatalf("state running = %v, want two rows", state["running"])
	}
	first, second := running[0].(map[string]any), running[1].(map[string]any)
	if first["held"] != true || first["hold_reason"] != "human takeover" || first["holder"] != "acp-broker" || first["held_since"] != "2026-10-11T10:00:00Z" || first["held_total_ms"] != float64(60000) {
		t.Errorf("held row = %v, want the hold fields", first)
	}
	if second["held"] != false || second["held_since"] != nil {
		t.Errorf("unheld row = %v, want held false and a null held_since", second)
	}

	_, detail := postControl(t, ts, http.MethodGet, "/api/v1/LIVE-1", "")
	if r, _ := detail["running"].(map[string]any); r["held"] != true || r["hold_reason"] != "human takeover" {
		t.Errorf("detail running = %v, want the hold fields", detail["running"])
	}
}

func TestEscalate_StatusCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		reply      orchestrator.ControlReply
		wantStatus int
		wantCode   string
	}{
		{"accepted", `{"message":"Which database should I target?","kind":"question","url":"https://v.example/s"}`, orchestrator.ControlReply{Outcome: orchestrator.ControlAccepted}, http.StatusAccepted, ""},
		{"empty message", `{"message":""}`, orchestrator.ControlReply{Outcome: orchestrator.ControlInvalid, Message: "message must not be empty"}, http.StatusBadRequest, "invalid_request"},
		{"not running", `{"message":"x"}`, orchestrator.ControlReply{Outcome: orchestrator.ControlNotRunning}, http.StatusConflict, "issue_not_running"},
		{"unknown issue", `{"message":"x"}`, orchestrator.ControlReply{Outcome: orchestrator.ControlUnknownIssue}, http.StatusNotFound, "issue_not_found"},
		{"rate limited", `{"message":"x"}`, orchestrator.ControlReply{Outcome: orchestrator.ControlRateLimited}, http.StatusTooManyRequests, "rate_limited"},
		{"malformed body", `{"message":`, orchestrator.ControlReply{Outcome: orchestrator.ControlAccepted}, http.StatusBadRequest, "invalid_request"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var got []orchestrator.EscalationRequest
			srv := New(Params{
				SnapshotFn: fixedSnapshot(orchestrator.RuntimeSnapshotResult{}),
				RefreshFn:  acceptingRefresh(),
				Logger:     slog.New(slog.DiscardHandler),
				EscalateFn: func(identifier string, req orchestrator.EscalationRequest) orchestrator.ControlReply {
					if identifier == "LIVE-1" {
						got = append(got, req)
					}
					return tt.reply
				},
			})
			ts := httptest.NewServer(srv.Mux())
			t.Cleanup(ts.Close)

			status, body := postControl(t, ts, http.MethodPost, "/api/v1/LIVE-1/escalate", tt.body)
			if status != tt.wantStatus {
				t.Fatalf("POST escalate status = %d, want %d (body %v)", status, tt.wantStatus, body)
			}
			if tt.wantCode != "" {
				if errObj, _ := body["error"].(map[string]any); errObj["code"] != tt.wantCode {
					t.Errorf("error body = %v, want code %q", body, tt.wantCode)
				}
				return
			}
			if body["event_type"] != "session.escalated" || body["issue_identifier"] != "LIVE-1" {
				t.Errorf("body = %v, want the accepted session.escalated for LIVE-1", body)
			}
			if len(got) != 1 || got[0] != (orchestrator.EscalationRequest{Message: "Which database should I target?", Kind: "question", URL: "https://v.example/s"}) {
				t.Errorf("EscalateFn calls = %+v, want the decoded request", got)
			}
		})
	}
}

func TestState_ServesStandingHolds(t *testing.T) {
	t.Parallel()

	until := time.Date(2026, 11, 3, 14, 0, 0, 0, time.UTC)
	srv := New(Params{
		SnapshotFn: fixedSnapshot(orchestrator.RuntimeSnapshotResult{
			GeneratedAt: until,
			Holds: []orchestrator.SnapshotStandingHold{
				{IssueID: "id-54", Identifier: "ONS-54", Holder: "supervisor", Reason: "full vm chain proof running", Evidence: "sandbox hl-ons-54-proof-1",
					Since: until.Add(-time.Hour), RenewedAt: until.Add(-time.Hour), Until: until, ExpiredAt: until},
			},
		}),
		RefreshFn: acceptingRefresh(),
		Logger:    slog.New(slog.DiscardHandler),
	})
	ts := httptest.NewServer(srv.Mux())
	t.Cleanup(ts.Close)

	status, body := getJSON[map[string]any](t, ts, "/api/v1/state")
	if status != http.StatusOK {
		t.Fatalf("GET /api/v1/state status = %d", status)
	}
	holds, _ := body["holds"].([]any)
	if len(holds) != 1 {
		t.Fatalf("holds = %v, want one", body["holds"])
	}
	h, _ := holds[0].(map[string]any)
	if h["issue_identifier"] != "ONS-54" || h["by"] != "supervisor" || h["reason"] != "full vm chain proof running" ||
		h["evidence"] != "sandbox hl-ons-54-proof-1" || h["renewed_at"] != "2026-11-03T13:00:00Z" || h["until"] != "2026-11-03T14:00:00Z" ||
		h["expired"] != true || h["expired_at"] != "2026-11-03T14:00:00Z" {
		t.Errorf("hold = %v, want the expired lease of ONS-54", h)
	}
	attention, _ := body["attention"].([]any)
	if len(attention) != 1 || attention[0].(map[string]any)["kind"] != "hold_expired" {
		t.Errorf("attention = %v, want one hold_expired item", attention)
	}
}
