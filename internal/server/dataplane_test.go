package server

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/sortie-ai/sortie/internal/dataplane"
	"github.com/sortie-ai/sortie/internal/jobtoken"
	"github.com/sortie-ai/sortie/internal/orchestrator"
)

func dataPlaneTestServer(t *testing.T, plane func() http.Handler) *httptest.Server {
	t.Helper()
	srv := New(Params{
		SnapshotFn:  fixedSnapshot(orchestrator.RuntimeSnapshotResult{}),
		RefreshFn:   acceptingRefresh(),
		Logger:      slog.New(slog.DiscardHandler),
		DataPlaneFn: plane,
	})
	ts := httptest.NewServer(srv.httpServer.Handler)
	t.Cleanup(ts.Close)
	return ts
}

func getWithToken(t *testing.T, ts *httptest.Server, path, token string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // test
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// remote-runs: Seed. The server serves the data plane under the remote
// kind, to the job's own bundles token and without the control token.
func TestDataPlaneServedUnderTheRemoteKind(t *testing.T) {
	t.Parallel()

	issuer := jobtoken.NewIssuer()
	plane := dataplane.New(issuer)
	seed := filepath.Join(t.TempDir(), "seed.bundle")
	if err := os.WriteFile(seed, []byte("seed"), 0o600); err != nil {
		t.Fatal(err)
	}
	a, err := plane.Open("ONS-30", 1, dataplane.Options{Seed: seed, Dir: t.TempDir(), MaxBundleBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	toks, err := issuer.Mint("ONS-30", 1, []jobtoken.Flow{"bundles"})
	if err != nil {
		t.Fatal(err)
	}
	other, err := issuer.Mint("ONS-31", 1, []jobtoken.Flow{"bundles"})
	if err != nil {
		t.Fatal(err)
	}
	handler := plane.Handler()
	ts := dataPlaneTestServer(t, func() http.Handler { return handler })
	path := dataplane.Prefix + "bundles/jobs/ONS-30/attempts/1/seed"
	if code, body := getWithToken(t, ts, path, toks["bundles"].Reveal()); code != http.StatusOK || body != "seed" {
		t.Fatalf("seed = %d %q, want 200 with the seed", code, body)
	}
	if code, body := getWithToken(t, ts, path, other["bundles"].Reveal()); code != http.StatusUnauthorized || body == "seed" {
		t.Fatalf("seed with another job's token = %d %q, want 401 and nothing served", code, body)
	}
}

// remote-runs: A tool call from taichi. The server routes the mcp flow
// to the data plane, which takes the job's own mcp token and answers
// another job's with 401.
func TestDataPlaneServesTheMCPFlow(t *testing.T) {
	t.Parallel()

	issuer := jobtoken.NewIssuer()
	plane := dataplane.New(issuer)
	a, err := plane.Open("ONS-30", 1, dataplane.Options{Dir: t.TempDir(), MaxBundleBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	own, err := issuer.Mint("ONS-30", 1, []jobtoken.Flow{dataplane.FlowMCP})
	if err != nil {
		t.Fatal(err)
	}
	other, err := issuer.Mint("ONS-31", 1, []jobtoken.Flow{dataplane.FlowMCP})
	if err != nil {
		t.Fatal(err)
	}
	handler := plane.Handler()
	ts := dataPlaneTestServer(t, func() http.Handler { return handler })
	path := dataplane.Prefix + "mcp/jobs/ONS-30/attempts/1"
	if code, _ := getWithToken(t, ts, path, other[dataplane.FlowMCP].Reveal()); code != http.StatusUnauthorized {
		t.Fatalf("mcp flow with another job's token = %d, want 401", code)
	}
	// The session has no tool server yet, so the admitted request is
	// answered by the data plane itself.
	if code, _ := getWithToken(t, ts, path, own[dataplane.FlowMCP].Reveal()); code != http.StatusServiceUnavailable {
		t.Fatalf("mcp flow with the job's token before its session = %d, want 503", code)
	}
}

func TestDataPlaneNotServedUnderOtherKinds(t *testing.T) {
	t.Parallel()

	ts := dataPlaneTestServer(t, func() http.Handler { return nil })
	if code, _ := getWithToken(t, ts, dataplane.Prefix+"bundles/jobs/ONS-30/attempts/1/seed", "x"); code != http.StatusNotFound {
		t.Fatalf("data plane under another kind = %d, want 404", code)
	}
}
