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
)

// fakeDispatchPause keeps a pause the way the orchestrator does: the first
// pause holds until a resume.
type fakeDispatchPause struct {
	mu    sync.Mutex
	pause *orchestrator.DispatchPause
	calls int
}

func (f *fakeDispatchPause) fn(_ context.Context, pause bool, by, reason string) orchestrator.DispatchPauseResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	switch {
	case pause && f.pause == nil:
		f.pause = &orchestrator.DispatchPause{Since: time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC), By: by, Reason: reason}
	case !pause:
		f.pause = nil
	}
	var copied *orchestrator.DispatchPause
	if f.pause != nil {
		copied = new(*f.pause)
	}
	return orchestrator.DispatchPauseResult{Status: orchestrator.DispatchPauseDone, Pause: copied}
}

func (f *fakeDispatchPause) snapshot() (orchestrator.RuntimeSnapshotResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var snap orchestrator.RuntimeSnapshotResult
	if f.pause != nil {
		snap.DispatchPause = new(*f.pause)
	}
	return snap, nil
}

func (f *fakeDispatchPause) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func dispatchPauseTestServer(t *testing.T, controlToken string, fake *fakeDispatchPause) *httptest.Server {
	t.Helper()
	srv := New(Params{
		SnapshotFn:   fake.snapshot,
		RefreshFn:    acceptingRefresh(),
		Logger:       slog.New(slog.DiscardHandler),
		ControlToken: controlToken,
		SessionTokenFn: func(token string) (string, bool) {
			return "LIVE-1", token == tokenLive1
		},
		DispatchPauseFn: fake.fn,
	})
	ts := httptest.NewServer(srv.Mux())
	t.Cleanup(ts.Close)
	return ts
}

func callDispatchPause(t *testing.T, ts *httptest.Server, method, path, authorization, body string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
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

func TestDispatchPause_PauseAndResume(t *testing.T) {
	t.Parallel()

	fake := &fakeDispatchPause{}
	ts := dispatchPauseTestServer(t, testControlToken, fake)
	auth := "Bearer " + testControlToken
	wantPaused := map[string]any{"since": "2026-11-02T09:00:00Z", "by": "stage0-deploy", "reason": "kit change"}

	steps := []struct {
		name  string
		path  string
		body  string
		want  map[string]any // nil: dispatch_paused is null
		calls int
	}{
		{"resume when not paused", "/api/v1/dispatch/resume", "", nil, 1},
		{"pause", "/api/v1/dispatch/pause", `{"by":"stage0-deploy","reason":"kit change"}`, wantPaused, 2},
		{"pause again keeps the first pause", "/api/v1/dispatch/pause", `{"by":"someone else"}`, wantPaused, 3},
		{"resume", "/api/v1/dispatch/resume", "", nil, 4},
		{"resume again", "/api/v1/dispatch/resume", "", nil, 5},
		{"pause with no body", "/api/v1/dispatch/pause", "", map[string]any{"since": "2026-11-02T09:00:00Z", "by": "operator", "reason": ""}, 6},
	}
	for _, step := range steps {
		status, body := callDispatchPause(t, ts, http.MethodPost, step.path, auth, step.body)
		if status != http.StatusOK {
			t.Fatalf("%s: POST %s status = %d, want 200 (body %v)", step.name, step.path, status, body)
		}
		if got := fake.callCount(); got != step.calls {
			t.Fatalf("%s: DispatchPauseFn calls = %d, want %d", step.name, got, step.calls)
		}
		_, state := callDispatchPause(t, ts, http.MethodGet, "/api/v1/state", "", "")
		for _, served := range []map[string]any{body, state} {
			value, present := served["dispatch_paused"]
			if !present {
				t.Fatalf("%s: response %v has no dispatch_paused", step.name, served)
			}
			if step.want == nil {
				if value != nil {
					t.Errorf("%s: dispatch_paused = %v, want null", step.name, value)
				}
				continue
			}
			got, _ := value.(map[string]any)
			for key, want := range step.want {
				if got[key] != want {
					t.Errorf("%s: dispatch_paused.%s = %v, want %v", step.name, key, got[key], want)
				}
			}
		}
	}
}

func TestDispatchPause_ControlTokenOnly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		method        string
		authorization string
		body          string
		wantStatus    int
	}{
		{"control token", http.MethodPost, "Bearer " + testControlToken, "", http.StatusOK},
		{"no token", http.MethodPost, "", "", http.StatusUnauthorized},
		{"wrong token", http.MethodPost, "Bearer wrong-token", "", http.StatusUnauthorized},
		{"a session token", http.MethodPost, "Bearer " + tokenLive1, "", http.StatusForbidden},
		{"GET", http.MethodGet, "Bearer " + testControlToken, "", http.StatusMethodNotAllowed},
		{"malformed body", http.MethodPost, "Bearer " + testControlToken, "{not json", http.StatusBadRequest},
	}
	for _, route := range []string{"/api/v1/dispatch/pause", "/api/v1/dispatch/resume"} {
		for _, tt := range tests {
			if tt.name == "malformed body" && strings.HasSuffix(route, "/resume") {
				continue
			}
			t.Run(route+" "+tt.name, func(t *testing.T) {
				t.Parallel()
				fake := &fakeDispatchPause{}
				ts := dispatchPauseTestServer(t, testControlToken, fake)
				status, body := callDispatchPause(t, ts, tt.method, route, tt.authorization, tt.body)
				if status != tt.wantStatus {
					t.Fatalf("%s %s status = %d, want %d (body %v)", tt.method, route, status, tt.wantStatus, body)
				}
				wantCalls := 0
				if tt.wantStatus == http.StatusOK {
					wantCalls = 1
				}
				if got := fake.callCount(); got != wantCalls {
					t.Errorf("DispatchPauseFn calls = %d, want %d", got, wantCalls)
				}
			})
		}
	}
}

func TestDispatchPause_Unavailable(t *testing.T) {
	t.Parallel()

	ts := testServer(t, fixedSnapshot(orchestrator.RuntimeSnapshotResult{}), acceptingRefresh())
	for _, route := range []string{"/api/v1/dispatch/pause", "/api/v1/dispatch/resume"} {
		if status, body := callDispatchPause(t, ts, http.MethodPost, route, "", ""); status != http.StatusServiceUnavailable {
			t.Errorf("POST %s without DispatchPauseFn status = %d, want 503 (body %v)", route, status, body)
		}
	}

	srv := New(Params{
		SnapshotFn: fixedSnapshot(orchestrator.RuntimeSnapshotResult{}),
		RefreshFn:  acceptingRefresh(),
		Logger:     slog.New(slog.DiscardHandler),
		DispatchPauseFn: func(context.Context, bool, string, string) orchestrator.DispatchPauseResult {
			return orchestrator.DispatchPauseResult{Status: orchestrator.DispatchPauseDraining}
		},
	})
	draining := httptest.NewServer(srv.Mux())
	t.Cleanup(draining.Close)
	if status, body := callDispatchPause(t, draining, http.MethodPost, "/api/v1/dispatch/pause", "", ""); status != http.StatusServiceUnavailable {
		t.Errorf("POST /api/v1/dispatch/pause while draining status = %d, want 503 (body %v)", status, body)
	}
}
