package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/forge"
	"github.com/sortie-ai/sortie/internal/forgeapp"
	"github.com/sortie-ai/sortie/internal/forgetoken"
	"github.com/sortie-ai/sortie/internal/orchestrator"
	"github.com/sortie-ai/sortie/internal/worker"
)

// sandboxToken is long, as GitHub's stateless tokens are.
const sandboxToken = "ghs_424242_" + "aGVsbG8gd29ybGQgdGhpcyBpcyBhIGxvbmcgdG9rZW4gdGhhdCBpcyBub3QgNDAgY2hhcmFjdGVycw"

type fakeForgeSandbox struct {
	mu    sync.Mutex
	mints []string
	ends  []string
	err   error
}

func (f *fakeForgeSandbox) mint(_ context.Context, repo string) (orchestrator.ForgeSandbox, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mints = append(f.mints, repo)
	if f.err != nil {
		return orchestrator.ForgeSandbox{}, f.err
	}
	expires := time.Date(2026, 11, 20, 13, 0, 0, 0, time.UTC)
	return orchestrator.ForgeSandbox{ID: "fts-0a1b",
		Source: worker.ForgeSource{Repository: repo, Branch: "master", Commit: "4b825dc", File: forge.File, Declared: true},
		Set: forgetoken.Set{ExpiresAt: expires, Tokens: []forgetoken.Token{{Owner: "ortusnova", Value: sandboxToken, ExpiresAt: expires,
			Repositories: []string{"ortusnova/fleet-vm"}, Permissions: map[string]string{"contents": "write"}}}}}, nil
}

func (f *fakeForgeSandbox) end(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ends = append(f.ends, id)
	if id != "fts-0a1b" {
		return forgetoken.ErrNotFound
	}
	return nil
}

func (f *fakeForgeSandbox) calls() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.mints), len(f.ends)
}

func forgeSandboxServer(t *testing.T, controlToken string, fake *fakeForgeSandbox) *httptest.Server {
	t.Helper()
	srv := New(Params{
		SnapshotFn:      fixedSnapshot(orchestrator.RuntimeSnapshotResult{}),
		RefreshFn:       acceptingRefresh(),
		Logger:          slog.New(slog.DiscardHandler),
		ControlToken:    controlToken,
		SupervisorToken: testSupervisorToken,
		SessionTokenFn: func(token string) (string, bool) {
			return "LIVE-1", token == tokenLive1
		},
		ForgeSandboxMintFn: fake.mint,
		ForgeSandboxEndFn:  fake.end,
	})
	ts := httptest.NewServer(srv.Mux())
	t.Cleanup(ts.Close)
	return ts
}

func callForgeTokens(t *testing.T, ts *httptest.Server, method, path, authorization, body string) (int, http.Header, map[string]any) {
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
	var decoded map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&decoded)
	return resp.StatusCode, resp.Header, decoded
}

// forge-tokens: homelab sandboxes take tokens from the same broker. The
// operator's control token gets the sandbox's tokens, never cached, and
// revokes them by the set's ID.
func TestForgeTokens_MintAndRevoke(t *testing.T) {
	t.Parallel()
	fake := &fakeForgeSandbox{}
	ts := forgeSandboxServer(t, testControlToken, fake)
	auth := "Bearer " + testControlToken

	status, header, body := callForgeTokens(t, ts, http.MethodPost, "/api/v1/forge-tokens", auth, `{"repository":"homelab"}`)
	if status != http.StatusCreated || header.Get("Cache-Control") != "no-store" {
		t.Fatalf("POST status %d, Cache-Control %q, body %v", status, header.Get("Cache-Control"), body)
	}
	tokens, _ := body["tokens"].([]any)
	if body["id"] != "fts-0a1b" || len(tokens) != 1 {
		t.Fatalf("body %v", body)
	}
	tok := tokens[0].(map[string]any)
	if tok["owner"] != "ortusnova" || tok["token"] != sandboxToken || tok["expires_at"] != "2026-11-20T13:00:00Z" {
		t.Fatalf("token %v", tok)
	}
	if src, _ := body["source"].(map[string]any); src["repository"] != "homelab" || src["commit"] != "4b825dc" || src["declared"] != true {
		t.Fatalf("source %v", body["source"])
	}

	if status, _, body := callForgeTokens(t, ts, http.MethodDelete, "/api/v1/forge-tokens?id=fts-0a1b", auth, ""); status != http.StatusNoContent {
		t.Fatalf("DELETE status %d, body %v", status, body)
	}
	if status, _, _ := callForgeTokens(t, ts, http.MethodDelete, "/api/v1/forge-tokens?id=fts-gone", auth, ""); status != http.StatusNotFound {
		t.Fatalf("DELETE of an unknown set = %d, want 404", status)
	}
}

// forge-tokens: The endpoint refuses any other token: a wrong one, none,
// a session's, the supervisor's, and every caller of a Sortie with no
// control token.
func TestForgeTokens_ControlTokenOnly(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, control, authorization string
		want                         int
	}{
		{"no token", testControlToken, "", http.StatusUnauthorized},
		{"wrong token", testControlToken, "Bearer wrong-token", http.StatusUnauthorized},
		{"a session token", testControlToken, "Bearer " + tokenLive1, http.StatusForbidden},
		{"the supervisor token", testControlToken, "Bearer " + testSupervisorToken, http.StatusForbidden},
		{"no control token configured", "", "", http.StatusForbidden},
		{"no control token configured, any bearer", "", "Bearer anything", http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeForgeSandbox{}
			ts := forgeSandboxServer(t, tc.control, fake)
			for _, call := range []struct{ method, path, body string }{
				{http.MethodPost, "/api/v1/forge-tokens", `{"repository":"homelab"}`},
				{http.MethodDelete, "/api/v1/forge-tokens?id=fts-0a1b", ""},
			} {
				status, header, body := callForgeTokens(t, ts, call.method, call.path, tc.authorization, call.body)
				if status != tc.want {
					t.Errorf("%s %s = %d, want %d (body %v)", call.method, call.path, status, tc.want, body)
				}
				if header.Get("Cache-Control") != "no-store" {
					t.Errorf("%s %s Cache-Control = %q", call.method, call.path, header.Get("Cache-Control"))
				}
			}
			if mints, ends := fake.calls(); mints != 0 || ends != 0 {
				t.Errorf("the broker was called: %d mints, %d ends", mints, ends)
			}
		})
	}
}

// forge-tokens: The endpoint refuses a grant above the ceiling, names a
// repository the workflow does not list, and an owner without the App.
func TestForgeTokens_Refusals(t *testing.T) {
	t.Parallel()
	auth := "Bearer " + testControlToken
	for _, tc := range []struct {
		name string
		err  error
		body string
		want int
		code string
	}{
		{"above the ceiling", &orchestrator.ForgeRefusal{Hold: &orchestrator.ForgeHold{Repository: "homelab", Branch: "master", Commit: "4b825dc",
			File: forge.File, Entry: "access[1].repository", Error: "ortusnova/fleet is outside the ceiling"}}, `{"repository":"homelab"}`, http.StatusConflict, "forge_refused"},
		{"an unknown repository", orchestrator.ErrUnknownRepository, `{"repository":"nope"}`, http.StatusNotFound, "unknown_repository"},
		{"no App", &forgeapp.Error{Owner: "zz", Err: forgeapp.ErrNotInstalled}, `{"repository":"homelab"}`, http.StatusUnprocessableEntity, "forge_app_not_installed"},
		{"tokens off", orchestrator.ErrForgeTokensOff, `{"repository":"homelab"}`, http.StatusServiceUnavailable, "forge_tokens_off"},
		{"GitHub down", errors.New("POST /app/installations/11/access_tokens: HTTP 502"), `{"repository":"homelab"}`, http.StatusBadGateway, "forge_unavailable"},
		{"no repository named", nil, `{}`, http.StatusBadRequest, "invalid_request"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := forgeSandboxServer(t, testControlToken, &fakeForgeSandbox{err: tc.err})
			status, _, body := callForgeTokens(t, ts, http.MethodPost, "/api/v1/forge-tokens", auth, tc.body)
			errBody, _ := body["error"].(map[string]any)
			if status != tc.want || errBody["code"] != tc.code {
				t.Fatalf("status %d, body %v; want %d %s", status, body, tc.want, tc.code)
			}
			if tc.name == "above the ceiling" && !strings.Contains(errBody["message"].(string), "ortusnova/fleet is outside the ceiling") {
				t.Errorf("message %q does not name the refused entry", errBody["message"])
			}
		})
	}
}
