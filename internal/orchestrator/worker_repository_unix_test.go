//go:build unix

package orchestrator

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/environment"
	"github.com/sortie-ai/sortie/internal/gitsync"
	"github.com/sortie-ai/sortie/internal/prompt"
	"github.com/sortie-ai/sortie/internal/waittest"
	"github.com/sortie-ai/sortie/internal/workspace"
)

// twoRepositoryConfig returns exportWorkerConfig with two named
// repositories, sortie the default, each opening pull requests.
func twoRepositoryConfig(t *testing.T) config.ServiceConfig {
	t.Helper()
	cfg := exportWorkerConfig(t)
	repos := []config.Repository{
		{Name: "sortie", URL: "https://example.com/org/sortie.git", Base: "main", BranchPrefix: "sortie/", Provider: "github", Project: "org/sortie", MaxBundleMiB: 512},
		{Name: "ao", URL: "https://example.com/org/ao.git", Base: "trunk", BranchPrefix: "sortie/", Provider: "github", Project: "org/ao", MaxBundleMiB: 512},
	}
	cfg.Environment.Remote.Repositories, cfg.Environment.Remote.DefaultRepository, cfg.Environment.Remote.Repository = repos, "sortie", &repos[0]
	return cfg
}

// repoRun records what one attempt used: the repository URL seeded and
// pushed, the pull request project, and the reported repository name.
type repoRun struct {
	result   WorkerResult
	url      string
	project  string
	reported string
}

func runRepositoryWorker(t *testing.T, cfg config.ServiceConfig, issue domain.Issue, agent domain.AgentAdapter) repoRun {
	t.Helper()
	if agent == nil {
		agent = &mockAgentAdapter{}
	}
	var mu sync.Mutex
	var run repoRun
	env := recordingEnvironment(t)
	opener := &fakeOpener{pr: domain.PullRequest{Number: 7, Created: true}}
	ec := newExitCapture()
	RunWorkerAttempt(context.Background(), issue, nil, WorkerDeps{
		TrackerAdapter:         &mockTrackerAdapter{},
		AgentAdapter:           agent,
		ConfigFunc:             func() config.ServiceConfig { return cfg },
		PromptTemplateByIDFunc: func(_ string) *prompt.Template { return mustParseTemplate(t, "{{ .issue.title }}") },
		OnEvent:                func(_ string, _ domain.AgentEvent) {},
		OnExit:                 ec.onExit,
		Logger:                 discardLogger(),
		EnvironmentFunc: func(config.EnvironmentConfig) (environment.Environment, error) {
			return env, nil
		},
		RepositoryFunc: func(c config.EnvironmentConfig, _ string, _ *slog.Logger) (Repository, error) {
			mu.Lock()
			run.url = c.Remote.Repository.URL
			mu.Unlock()
			return &fakeRepository{result: gitsync.ExportResult{Branch: "sortie/" + issue.Identifier, Commit: "aaa", Pushed: true}}, nil
		},
		PullRequestOpenerFunc: func(c config.ServiceConfig) (domain.PullRequestOpener, error) {
			mu.Lock()
			run.project = c.Environment.Remote.Repository.Project
			mu.Unlock()
			return opener, nil
		},
		OnWorkspaceReady: func(_, _, repository string) {
			mu.Lock()
			run.reported = repository
			mu.Unlock()
		},
	})
	run.result = ec.waitResult(t)
	mu.Lock()
	defer mu.Unlock()
	return run
}

func repoIssue(id, identifier string, labels ...string) domain.Issue {
	return domain.Issue{ID: id, Identifier: identifier, Title: "Test issue", State: "To Do", Labels: labels}
}

// Each run uses its issue's repository: two issues in two repositories
// run at once, each seeding, pushing, opening, and recording the pull
// request it follows in its own repository.
func TestRunWorkerAttempt_TwoIssuesInTwoRepositoriesAtOnce(t *testing.T) {
	cfg := twoRepositoryConfig(t)

	arrived := make(chan string, 2)
	release := make(chan struct{})
	agent := &mockAgentAdapter{runTurnFn: func(ctx context.Context, s domain.Session, p domain.RunTurnParams) (domain.TurnResult, error) {
		arrived <- s.ID
		<-release
		return domain.TurnResult{SessionID: s.ID, ExitReason: domain.EventTurnCompleted}, nil
	}}

	issues := []domain.Issue{
		repoIssue("i-1", "ONE-1"),
		repoIssue("i-2", "TWO-2", config.RepositoryLabelPrefix+"ao"),
	}
	runs := make([]repoRun, len(issues))
	var wg sync.WaitGroup
	for i, issue := range issues {
		wg.Go(func() { runs[i] = runRepositoryWorker(t, cfg, issue, agent) })
	}
	waittest.Receive(t, arrived, "first agent turn")
	waittest.Receive(t, arrived, "second agent turn while the first still runs")
	close(release)
	wg.Wait()

	want := []struct{ url, project, name, owner, repo string }{
		{"https://example.com/org/sortie.git", "org/sortie", "sortie", "org", "sortie"},
		{"https://example.com/org/ao.git", "org/ao", "ao", "org", "ao"},
	}
	for i, w := range want {
		r := runs[i]
		if r.result.ExitKind != WorkerExitNormal {
			t.Fatalf("%s: result = %v, %v", issues[i].Identifier, r.result.ExitKind, r.result.Error)
		}
		if r.url != w.url || r.project != w.project || r.reported != w.name {
			t.Errorf("%s: url %q, project %q, reported %q; want %+v", issues[i].Identifier, r.url, r.project, r.reported, w)
		}
		rec := workspace.ReadHostSCMRecord(r.result.WorkspacePath, nil)
		if rec.Owner != w.owner || rec.Repo != w.repo || rec.PRNumber != 7 {
			t.Errorf("%s: SCM record = %+v, want pull request 7 of %s/%s", issues[i].Identifier, rec, w.owner, w.repo)
		}
	}
}

// A workspace keeps the repository it was seeded from, whatever the
// issue names later.
func TestRunWorkerAttempt_WorkspaceKeepsItsRepository(t *testing.T) {
	cfg := twoRepositoryConfig(t)

	first := runRepositoryWorker(t, cfg, repoIssue("i-1", "ONE-1", config.RepositoryLabelPrefix+"ao"), nil)
	if first.url != "https://example.com/org/ao.git" {
		t.Fatalf("first run used %q, want ao", first.url)
	}
	again := runRepositoryWorker(t, cfg, repoIssue("i-1", "ONE-1"), nil)
	if again.result.ExitKind != WorkerExitNormal || again.url != "https://example.com/org/ao.git" || again.reported != "ao" {
		t.Errorf("second run = %v, url %q, reported %q; want the seeded ao", again.result.Error, again.url, again.reported)
	}

	// A workflow that no longer lists it fails the preparation.
	cfg.Environment.Remote.Repositories = cfg.Environment.Remote.Repositories[:1]
	gone := runRepositoryWorker(t, cfg, repoIssue("i-1", "ONE-1"), nil)
	if gone.result.Error == nil || !strings.Contains(gone.result.Error.Error(), `seeded from repository "ao"`) {
		t.Errorf("error = %v, want the seeded repository named", gone.result.Error)
	}
}

// The poll dispatches no issue that names an unlisted repository; an
// attempt that meets one, after a reload dropped the repository, fails
// its preparation and seeds nothing.
func TestRunWorkerAttempt_UnlistedIssueRepositoryFails(t *testing.T) {
	cfg := twoRepositoryConfig(t)
	r := runRepositoryWorker(t, cfg, repoIssue("i-1", "ONE-1", config.RepositoryLabelPrefix+"elsewhere"), nil)
	if r.result.Error == nil || !strings.Contains(r.result.Error.Error(), `names repository "elsewhere"`) || r.url != "" {
		t.Errorf("result = %v, url %q; want a preparation error and nothing seeded", r.result.Error, r.url)
	}
}

// One mirror per URL: two repositories at the same URL share a mirror,
// and different URLs do not.
func TestRepositorySync_OneMirrorPerURL(t *testing.T) {
	t.Parallel()
	stateDir := t.TempDir()
	cfg := twoRepositoryConfig(t).Environment
	dirs := map[string]bool{}
	for _, url := range []string{"https://example.com/org/sortie.git", "https://example.com/org/ao.git", "https://example.com/org/sortie.git"} {
		c := cfg
		repo := *c.Remote.Repository
		repo.URL = url
		c.Remote.Repository = &repo
		if _, err := repositorySync(c, stateDir, discardLogger()); err != nil {
			t.Fatalf("repositorySync(%s) error = %v", url, err)
		}
		dirs[gitsync.MirrorPath(stateDir, url)] = true
	}
	if len(dirs) != 2 {
		t.Errorf("mirrors = %v, want one per URL", dirs)
	}
}

// State shows each issue's repository: the worker's report lands on the
// running entry and in the runtime snapshot.
func TestApplyAgentEvent_StateShowsTheRepository(t *testing.T) {
	t.Parallel()

	o, _ := tokenWarningApplyEventOrchestrator(t, 0, 0)
	o.applyAgentEvent(context.Background(), agentEventMsg{IssueID: "id-apply", Workspace: "/ws/ISS-1", Repository: "ao"}, true)

	snap := RuntimeSnapshot(o.state, time.Now())
	if len(snap.Running) != 1 || snap.Running[0].Repository != "ao" || snap.Running[0].WorkspacePath != "/ws/ISS-1" {
		t.Errorf("running = %+v, want repository ao", snap.Running)
	}
}
