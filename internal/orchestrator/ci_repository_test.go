package orchestrator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/registry"

	_ "github.com/sortie-ai/sortie/internal/scm/github"
)

// twoRepoForge is a fake GitHub holding two repositories: the workflow's
// default one, which has no pull request and none of the PR's commits, and
// the one the pull request is in. It records every path it is asked for.
type twoRepoForge struct {
	mu    sync.Mutex
	paths []string
}

func (f *twoRepoForge) requested() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.paths)
}

func (f *twoRepoForge) serve(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.paths = append(f.paths, r.URL.Path)
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/ortusnova/sortie/pulls/31":
			_, _ = w.Write([]byte(`{"state":"open","mergeable_state":"unstable",` +
				`"html_url":"https://github.com/ortusnova/sortie/pull/31",` +
				`"head":{"sha":"pr-head","ref":"sortie/ONS-78"},"base":{"ref":"plan-bench"}}`))
		case "/repos/ortusnova/sortie/commits/pr-head/check-runs":
			_, _ = w.Write([]byte(`{"total_count":1,"check_runs":[` +
				`{"id":7,"name":"Lint (linux)","status":"completed","conclusion":"failure",` +
				`"html_url":"https://github.com/ortusnova/sortie/runs/7","app":{"slug":"other"}}]}`))
		default:
			// What GitHub answers for a commit the repository does not have.
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte(`{"message":"No commit found for SHA: pr-head","status":"422"}`))
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// TestReconcileCIStatus_ReadsThePullRequestsRepository verifies that the CI
// reaction reads the check runs of the repository the pull request is in,
// not the repository the CI provider was configured with: on a workflow
// that dispatches several repositories, every pull request outside the
// default one otherwise has its CI fetched from a repository without its
// commits (GitHub answers 422), so its CI failure is never acted on.
func TestReconcileCIStatus_ReadsThePullRequestsRepository(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		wrap func(domain.CIStatusProvider) domain.CIStatusProvider
	}{
		{name: "provider", wrap: func(p domain.CIStatusProvider) domain.CIStatusProvider { return p }},
		{name: "observed provider", wrap: func(p domain.CIStatusProvider) domain.CIStatusProvider {
			return observeCI(p, &healthRecord{})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testCIReadsThePullRequestsRepository(t, tc.wrap)
		})
	}
}

func testCIReadsThePullRequestsRepository(t *testing.T, wrap func(domain.CIStatusProvider) domain.CIStatusProvider) {
	t.Helper()

	forge := &twoRepoForge{}
	srv := forge.serve(t)

	scmFactory, err := registry.SCMAdapters.Get("github")
	if err != nil {
		t.Fatalf("registry.SCMAdapters.Get(github): %v", err)
	}
	scm, err := scmFactory(map[string]any{"api_key": "token", "endpoint": srv.URL})
	if err != nil {
		t.Fatalf("github scm adapter: %v", err)
	}
	ciFactory, err := registry.CIProviders.Get("github")
	if err != nil {
		t.Fatalf("registry.CIProviders.Get(github): %v", err)
	}
	ci, err := ciFactory(0, map[string]any{
		"api_key":  "token",
		"endpoint": srv.URL,
		"project":  "ortusnova/agentic-orchestration",
	})
	if err != nil {
		t.Fatalf("github ci provider: %v", err)
	}

	const issueID = "ISS-CI-REPO"
	state := NewState(5000, 4, 0, nil, AgentTotals{})
	entry := newPendingEntry(issueID, "ONS-78", "sortie/ONS-78", 1)
	entry.KindData = &CIReactionData{PRNumber: 31, Owner: "ortusnova", Repo: "sortie", Branch: "sortie/ONS-78", SHA: "pr-head"}
	state.PendingReactions[ReactionKey(issueID, ReactionKindCI)] = entry
	state.Claimed[issueID] = struct{}{}

	store := &ciReconcileStore{}
	metrics := newCIMetricsSpy()
	params := ciParams(t, store, wrap(ci), nil, scm)

	reconcileCIStatus(state, params, discardLogger(), context.Background(), metrics)

	paths := forge.requested()
	for _, p := range paths {
		if p == "/repos/ortusnova/agentic-orchestration/commits/pr-head/check-runs" {
			t.Errorf("CI read from the default repository: requested %v", paths)
		}
	}
	if !slices.Contains(paths, "/repos/ortusnova/sortie/commits/pr-head/check-runs") {
		t.Errorf("CI not read from the pull request's repository: requested %v", paths)
	}
	if got := metrics.ciStatusChecks[string(domain.CIStatusFailing)]; got != 1 {
		t.Errorf("failing CI checks = %d (all: %v), want 1", got, metrics.ciStatusChecks)
	}
}
