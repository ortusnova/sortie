//go:build unix

package orchestrator

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/dataplane"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/waittest"
)

// homelabUpstream returns a new bare copy of the fixture's origin whose
// trunk branch holds a commit of its own, so its seed tells apart from
// sortie's.
func (f *gitFixture) homelabUpstream() string {
	f.t.Helper()
	upstream := f.upstream()
	work := filepath.Join(f.t.TempDir(), "homelab")
	f.git(filepath.Dir(work), "clone", "--quiet", upstream, work)
	if err := os.WriteFile(filepath.Join(work, "HOMELAB"), []byte("homelab\n"), 0o600); err != nil {
		f.t.Fatal(err)
	}
	f.git(work, "add", "HOMELAB")
	f.git(work, "commit", "--quiet", "-m", "homelab")
	f.git(work, "push", "--quiet", "origin", "HEAD:refs/heads/trunk")
	return upstream
}

// remoteRepositoriesConfig is the remote kind with homelab, which runs in
// its own image on a worker that declares nested, and sortie, the
// default, in its own image.
func remoteRepositoriesConfig(t *testing.T, srvURL, header, homelab, sortie string) config.ServiceConfig {
	t.Helper()
	cfg := baseRunConfig(t)
	flows := map[string]string{}
	for _, fl := range config.RemoteFlows {
		flows[fl] = srvURL + dataplane.Prefix + fl
	}
	repos := []config.SmolvmRepository{
		{Name: "homelab", URL: homelab, Base: "trunk", BranchPrefix: "sortie/", Provider: "github", Project: "o/homelab", MaxBundleMiB: 16,
			Image: "ghcr.io/ortusnova/homelab:1", Requires: &config.RemoteRequirements{Capabilities: []string{"nested"}}},
		{Name: "sortie", URL: sortie, Base: "main", BranchPrefix: "sortie/", Provider: "github", Project: "o/sortie", MaxBundleMiB: 16,
			Image: "ghcr.io/ortusnova/sortie:1"},
	}
	cfg.Environment = config.EnvironmentConfig{Kind: config.EnvironmentKindRemote, HostPrefix: []string{envPath(t)},
		Remote: config.RemoteConfig{IdentityHeader: header, Backend: "smolvm", Image: "ghcr.io/ortusnova/agent:1",
			Flows: flows, LeaseTTLMS: 60000, Repositories: repos, Repository: &repos[1], DefaultRepository: "sortie"}}
	return cfg
}

// remote-runs: A homelab story and a Sortie story at once. ONS-30 names
// homelab and ONS-31 names no repository, with sortie the default.
// ONS-30's seed is a bundle of homelab at its base, in the homelab image,
// offered only to a worker that declares nested; ONS-31's seed is a
// bundle of sortie, in its own image. Each branch is pushed to its own
// repository.
func TestRemoteRunEachRepositoryDecidesItsJob(t *testing.T) {
	f := newGitFixture(t)
	homelab, sortie := f.homelabUpstream(), f.upstream()
	homelabBase, sortieBase := f.branch(homelab, "trunk"), f.branch(sortie, "main")
	if homelabBase == "" || homelabBase == sortieBase {
		t.Fatalf("homelab trunk %q, sortie main %q: want two different bases", homelabBase, sortieBase)
	}

	rw := newRemoteWorkers(time.Now)
	const header = "X-Sortie-Worker-Identity"
	srv := remoteServer(t, rw, header)
	cfg := remoteRepositoriesConfig(t, srv.URL, header, homelab, sortie)

	ons30 := domain.Issue{ID: "issue-30", Identifier: "ONS-30", Title: "Add one and two", State: "To Do", Labels: []string{"sortie-repository:homelab"}}
	ons31 := domain.Issue{ID: "issue-31", Identifier: "ONS-31", Title: "Add one and two", State: "To Do"}
	if got := dispatchableIssueIDs(cfg.Environment, cfg.Workspace.Root, ons30, ons31); !slices.Equal(got, []string{"issue-30", "issue-31"}) {
		t.Fatalf("dispatched %v, want both issues", got)
	}

	type run struct {
		capture runCapture
		done    chan WorkerResult
	}
	runs := map[string]*run{}
	ctx := t.Context()
	for _, issue := range []domain.Issue{ons30, ons31} {
		r := &run{done: make(chan WorkerResult, 1)}
		runs[issue.Identifier] = r
		deps := runDeps(t, cfg, &lineAdapter{}, &r.capture, r.done)
		go RunRemoteAttempt(ctx, rw, issue, nil, deps, RemoteAttemptParams{DefaultCommand: "opencode acp"})
	}
	ended := func() bool { return len(runs["ONS-30"].done) > 0 || len(runs["ONS-31"].done) > 0 }

	// A worker without nested virtualization is offered only ONS-31.
	plain := &fakeWorker{t: t, git: f, base: srv.URL, id: "p1", header: header, issuer: rw.tokens, ended: ended, noNested: true}
	plain.take()
	if plain.offer.Job.ID != "ONS-31" || plain.offer.Run.Image != "ghcr.io/ortusnova/sortie:1" ||
		!slices.Equal(plain.offer.Job.Requires.Capabilities, []string{"smolvm"}) {
		t.Fatalf("p1 offered %s in %q requiring %v, want ONS-31 in the sortie image", plain.offer.Job.ID, plain.offer.Run.Image, plain.offer.Job.Requires.Capabilities)
	}
	waittest.Until(t, func() error {
		for _, q := range rw.matcher.Snapshot().Queue {
			if q.Job.ID == "ONS-30" {
				return nil
			}
		}
		return errors.New("ONS-30 not queued")
	})
	resp := plain.post("/api/v1/workers/poll", nil)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("p1 poll with ONS-30 queued: %d, want 204: ONS-30 needs nested", resp.StatusCode)
	}

	// A worker that declares nested takes ONS-30, in the homelab image.
	taichi := &fakeWorker{t: t, git: f, base: srv.URL, id: "taichi", header: header, issuer: rw.tokens, ended: ended}
	taichi.take()
	if taichi.offer.Job.ID != "ONS-30" || taichi.offer.Run.Image != "ghcr.io/ortusnova/homelab:1" ||
		!slices.Equal(taichi.offer.Job.Requires.Capabilities, []string{"nested", "smolvm"}) {
		t.Fatalf("taichi offered %s in %q requiring %v, want ONS-30 in the homelab image requiring nested",
			taichi.offer.Job.ID, taichi.offer.Run.Image, taichi.offer.Job.Requires.Capabilities)
	}

	// Each seed is a bundle of its issue's repository at that
	// repository's base.
	for _, tc := range []struct {
		w        *fakeWorker
		base     string
		homelab  bool
		upstream string
	}{{plain, sortieBase, false, sortie}, {taichi, homelabBase, true, homelab}} {
		tc.w.seed()
		if head := f.git(tc.w.clone, "rev-parse", "HEAD"); head != tc.base {
			t.Errorf("%s seed is at %s, want its repository's base %s", tc.w.offer.Job.ID, head, tc.base)
		}
		if _, err := os.Stat(filepath.Join(tc.w.clone, "HOMELAB")); (err == nil) != tc.homelab {
			t.Errorf("%s seed holds HOMELAB: %v, want %v", tc.w.offer.Job.ID, err == nil, tc.homelab)
		}
		tc.w.session()
		tc.w.giveBack()
	}

	for id, r := range runs {
		r.capture.result = waittest.Receive(t, r.done, id+" run result")
		if r.capture.result.ExitKind != WorkerExitNormal || r.capture.result.Error != nil {
			t.Fatalf("%s run: %v %v", id, r.capture.result.ExitKind, r.capture.result.Error)
		}
	}

	// Each issue branch went to its own repository, and its pull request
	// to that repository's project and base.
	if f.branch(homelab, "sortie/ONS-30") == "" || f.branch(sortie, "sortie/ONS-30") != "" {
		t.Errorf("sortie/ONS-30 pushed to homelab %q and sortie %q, want homelab only", f.branch(homelab, "sortie/ONS-30"), f.branch(sortie, "sortie/ONS-30"))
	}
	if f.branch(sortie, "sortie/ONS-31") == "" || f.branch(homelab, "sortie/ONS-31") != "" {
		t.Errorf("sortie/ONS-31 pushed to sortie %q and homelab %q, want sortie only", f.branch(sortie, "sortie/ONS-31"), f.branch(homelab, "sortie/ONS-31"))
	}
	for id, want := range map[string]ensureCall{
		"ONS-30": {Owner: "o", Repo: "homelab", Head: "sortie/ONS-30", Base: "trunk", Title: "Add one and two", Body: "Two changes."},
		"ONS-31": {Owner: "o", Repo: "sortie", Head: "sortie/ONS-31", Base: "main", Title: "Add one and two", Body: "Two changes."},
	} {
		if got := runs[id].capture.opener.ensureCalls(); !slices.Equal(got, []ensureCall{want}) {
			t.Errorf("%s pull requests %+v, want %+v", id, got, want)
		}
	}
}

// remote-runs: A repository closed to dispatch dispatches no issue under
// the remote kind, and an issue that names an unlisted repository is
// left alone too.
func TestRemoteClosedRepositoryDispatchesNothing(t *testing.T) {
	cfg := remoteRepositoriesConfig(t, "https://edge.example", "X-W", "https://example.com/homelab.git", "https://example.com/sortie.git")
	cfg.Environment.Remote.Repositories[0].ClosedToDispatch = true
	issues := []domain.Issue{
		{ID: "issue-30", Identifier: "ONS-30", Labels: []string{"sortie-repository:homelab"}},
		{ID: "issue-31", Identifier: "ONS-31"},
		{ID: "issue-32", Identifier: "ONS-32", Labels: []string{"sortie-repository:attic"}},
	}
	if got := dispatchableIssueIDs(cfg.Environment, cfg.Workspace.Root, issues...); !slices.Equal(got, []string{"issue-31"}) {
		t.Errorf("dispatched %v, want only ONS-31", got)
	}
	blocked := BlockerRunsElsewhere(cfg.Environment, cfg.Workspace.Root)
	if !blocked(domain.BlockerRef{ID: "issue-30", Identifier: "ONS-30", Labels: []string{"sortie-repository:homelab"}}) {
		t.Error("a blocker in the closed repository is not reported as running elsewhere")
	}
}

func dispatchableIssueIDs(cfg config.EnvironmentConfig, root string, issues ...domain.Issue) []string {
	kept, _ := dispatchableIssues(cfg, root, issues, nil, discardLogger())
	ids := make([]string, 0, len(kept))
	for _, i := range kept {
		ids = append(ids, i.ID)
	}
	return ids
}
