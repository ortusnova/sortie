//go:build unix

package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/environment"
	"github.com/sortie-ai/sortie/internal/environment/environmenttest"
	"github.com/sortie-ai/sortie/internal/gitsync"
	"github.com/sortie-ai/sortie/internal/prompt"
	"github.com/sortie-ai/sortie/internal/workspace"
	"github.com/sortie-ai/sortie/internal/workspacekit"
)

// fakeRepository stands in for gitsync: seeding and refreshing do
// nothing, and Export returns result and err after recording the
// environment's operations so far, when it has an environment, and
// appending "export" to sequence.
type fakeRepository struct {
	env      *environmenttest.Recorder
	sequence *[]string
	result   gitsync.ExportResult
	err      error

	mu        sync.Mutex
	exports   []environment.Workspace
	opsAtCall []environmenttest.Call
}

var _ Repository = (*fakeRepository)(nil)

func (f *fakeRepository) Seed(context.Context, environment.Environment, environment.Workspace) error {
	return nil
}

func (f *fakeRepository) Refresh(context.Context, environment.Environment, environment.Workspace) error {
	return nil
}

func (f *fakeRepository) Export(_ context.Context, _ environment.Environment, ws environment.Workspace) (gitsync.ExportResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exports = append(f.exports, ws)
	if f.env != nil {
		f.opsAtCall = f.env.Calls()
	}
	if f.sequence != nil {
		*f.sequence = append(*f.sequence, "export")
	}
	return f.result, f.err
}

func (f *fakeRepository) exportCalls() []environment.Workspace {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.exports)
}

// exportWorkerConfig returns a one-turn worker configuration under the
// smolvm kind with a repository, an after_run hook, and the handoff
// evidence policy off.
func exportWorkerConfig(t *testing.T) config.ServiceConfig {
	t.Helper()
	cfg := defaultWorkerConfig(t.TempDir())
	cfg.Agent.MaxTurns = 1
	cfg.Hooks.AfterRun = "echo finished > after-run.txt"
	cfg.Tracker.HandoffEvidence = config.HandoffEvidenceOff
	cfg.Environment = config.EnvironmentConfig{
		Kind: config.EnvironmentKindRemote,
		Remote: config.RemoteConfig{Repository: &config.Repository{
			URL: "https://example.com/org/app.git", Base: "main", BranchPrefix: "sortie/", MaxBundleMiB: 512,
		}},
	}
	return cfg
}

// runExportWorker runs one worker attempt under cfg with env and repo,
// and returns its result and how many times the repository was built.
func runExportWorker(t *testing.T, ctx context.Context, cfg config.ServiceConfig, env environment.Environment, repo *fakeRepository, agent domain.AgentAdapter, posture DispatchPosture) (WorkerResult, int) {
	t.Helper()
	if agent == nil {
		agent = &mockAgentAdapter{}
	}
	var built int
	ec := newExitCapture()
	RunWorkerAttempt(ctx, workerTestIssue(), nil, WorkerDeps{
		TrackerAdapter:         &mockTrackerAdapter{},
		AgentAdapter:           agent,
		ConfigFunc:             func() config.ServiceConfig { return cfg },
		PromptTemplateByIDFunc: func(_ string) *prompt.Template { return mustParseTemplate(t, "{{ .issue.title }}") },
		OnEvent:                func(_ string, _ domain.AgentEvent) {},
		OnExit:                 ec.onExit,
		Logger:                 discardLogger(),
		Posture:                posture,
		EnvironmentFunc: func(config.EnvironmentConfig) (environment.Environment, error) {
			return env, nil
		},
		RepositoryFunc: func(config.EnvironmentConfig, string, *slog.Logger) (Repository, error) {
			built++
			return repo, nil
		},
	})
	return ec.waitResult(t), built
}

// handOff feeds result to the exit handling of an issue in an active
// state with a handoff state configured, and returns the state, the
// store, and the tracker.
func handOff(t *testing.T, result WorkerResult, sequence *[]string) (*State, *mockExitStore, *mockTrackerAdapter) {
	t.Helper()
	store := &mockExitStore{}
	tracker := &mockTrackerAdapter{transitionIssueFn: func(context.Context, string, string) error {
		*sequence = append(*sequence, "transition")
		return nil
	}}
	state := exitStateWithIssue(t, result.IssueID, "To Do")
	params := defaultExitParams(t, store)
	params.TrackerAdapter = tracker
	params.HandoffState = "Human Review"
	params.ActiveStates = []string{"To Do", "In Progress"}
	HandleWorkerExit(state, result, params)
	t.Cleanup(func() { CancelRetry(state, result.IssueID) })
	return state, store, tracker
}

func TestRunWorkerAttempt_ExportPrecedesTheHandoff(t *testing.T) {
	cfg := exportWorkerConfig(t)
	env := recordingEnvironment(t)
	var sequence []string
	const commit = "0b4c5d3e2f1a0b4c5d3e2f1a0b4c5d3e2f1a0b4c"
	repo := &fakeRepository{env: env, sequence: &sequence, result: gitsync.ExportResult{Branch: "sortie/TEST-1", Commit: commit, Pushed: true}}

	result, built := runExportWorker(t, context.Background(), cfg, env, repo, nil, PostureNormal)

	if built != 1 {
		t.Errorf("repository built %d times, want 1", built)
	}
	if result.ExitKind != WorkerExitNormal || result.Error != nil {
		t.Fatalf("result = %v, %v; want a normal exit", result.ExitKind, result.Error)
	}
	if result.ExportedCommit != commit {
		t.Errorf("ExportedCommit = %q, want %q", result.ExportedCommit, commit)
	}
	calls := repo.exportCalls()
	if len(calls) != 1 || calls[0].Key != "TEST-1" || calls[0].HostPath != result.WorkspacePath {
		t.Fatalf("Export calls = %+v, want one for the workspace %s", calls, result.WorkspacePath)
	}
	// The export follows after_run and precedes the release.
	before := repo.opsAtCall
	if !slices.Contains(shellScripts(before), cfg.Hooks.AfterRun) {
		t.Errorf("commands before the export = %q, want after_run among them", shellScripts(before))
	}
	if slices.ContainsFunc(before, func(c environmenttest.Call) bool { return c.Op == environmenttest.OpRelease }) {
		t.Error("the environment was released before the export")
	}
	if ops := env.Ops(); ops[len(ops)-1] != environmenttest.OpRelease {
		t.Errorf("environment ops = %q, want the release last", ops)
	}

	state, store, tracker := handOff(t, result, &sequence)
	if !slices.Equal(sequence, []string{"export", "transition"}) {
		t.Errorf("sequence = %q, want the export before the handoff transition", sequence)
	}
	if len(tracker.transitionCalls) != 1 || tracker.transitionCalls[0].TargetState != "Human Review" {
		t.Errorf("transitions = %+v, want the handoff", tracker.transitionCalls)
	}
	if _, retry := state.RetryAttempts[result.IssueID]; retry {
		t.Error("retry scheduled after a successful export and handoff")
	}
	if len(store.runHistories) != 1 || store.runHistories[0].ExportedCommit != commit || store.runHistories[0].Status != "succeeded" {
		t.Errorf("run history = %+v, want one succeeded row with the exported commit", store.runHistories)
	}
}

func TestRunWorkerAttempt_FailedExportFailsTheRun(t *testing.T) {
	cfg := exportWorkerConfig(t)
	env := recordingEnvironment(t)
	var sequence []string
	cause := fmt.Errorf("push sortie/TEST-1: %w", gitsync.ErrBranchMoved)
	repo := &fakeRepository{env: env, sequence: &sequence, result: gitsync.ExportResult{Branch: "sortie/TEST-1"}, err: cause}

	result, _ := runExportWorker(t, context.Background(), cfg, env, repo, nil, PostureNormal)

	if result.ExitKind != WorkerExitError {
		t.Fatalf("ExitKind = %v, want %v", result.ExitKind, WorkerExitError)
	}
	exportErr, ok := errors.AsType[*ExportError](result.Error)
	if !ok || exportErr.Branch != "sortie/TEST-1" || !errors.Is(result.Error, gitsync.ErrBranchMoved) {
		t.Fatalf("result error = %v, want an export error for sortie/TEST-1 wrapping %v", result.Error, gitsync.ErrBranchMoved)
	}
	if result.ExportedCommit != "" {
		t.Errorf("ExportedCommit = %q, want empty", result.ExportedCommit)
	}
	// The environment is stopped as after every run, never removed, and
	// the workspace stays.
	ops := env.Ops()
	if ops[len(ops)-1] != environmenttest.OpRelease || slices.Contains(ops, environmenttest.OpRemove) {
		t.Errorf("environment ops = %q, want a release and no removal", ops)
	}
	if _, err := os.Stat(result.WorkspacePath); err != nil {
		t.Errorf("workspace stat error = %v, want it kept", err)
	}

	state, store, tracker := handOff(t, result, &sequence)
	if len(tracker.transitionCalls) != 0 {
		t.Errorf("transitions = %+v, want none: the issue stays active", tracker.transitionCalls)
	}
	if retry, ok := state.RetryAttempts[result.IssueID]; !ok || retry.Attempt != 1 {
		t.Errorf("retry = %+v, %v; want attempt 1 scheduled", retry, ok)
	}
	if _, claimed := state.Claimed[result.IssueID]; !claimed {
		t.Error("claim released, want it kept for the retry")
	}
	if len(store.runHistories) != 1 {
		t.Fatalf("run history rows = %d, want 1", len(store.runHistories))
	}
	row := store.runHistories[0]
	if row.Status != "failed" || row.Error == nil || !strings.Contains(*row.Error, "branch moved upstream") || row.ExportedCommit != "" {
		t.Errorf("run history = status %q, error %v, exported %q; want failed with the export error and no commit", row.Status, row.Error, row.ExportedCommit)
	}
}

func TestRunWorkerAttempt_NoExportWithoutARepository(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(cfg *config.ServiceConfig)
		posture DispatchPosture
	}{
		{name: "local kind", mutate: func(cfg *config.ServiceConfig) { cfg.Environment.Kind = config.EnvironmentKindLocal }},
		{name: "default kind", mutate: func(cfg *config.ServiceConfig) { cfg.Environment.Kind = "" }},
		{name: "smolvm without repository", mutate: func(cfg *config.ServiceConfig) { cfg.Environment.Remote.Repository = nil }},
		{name: "scratch dispatch", mutate: func(*config.ServiceConfig) {}, posture: PostureReview},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := exportWorkerConfig(t)
			tt.mutate(&cfg)
			env := recordingEnvironment(t)
			repo := &fakeRepository{env: env, err: errors.New("must not be called")}

			result, built := runExportWorker(t, context.Background(), cfg, env, repo, nil, tt.posture)

			if built != 0 || len(repo.exportCalls()) != 0 {
				t.Errorf("repository built %d times, exported %d times; want neither", built, len(repo.exportCalls()))
			}
			if result.ExitKind != WorkerExitNormal || result.ExportedCommit != "" {
				t.Errorf("result = %v, %v, exported %q; want a normal exit without an export", result.ExitKind, result.Error, result.ExportedCommit)
			}
		})
	}
}

func TestRunWorkerAttempt_NoExportForACancelledRun(t *testing.T) {
	cfg := exportWorkerConfig(t)
	env := recordingEnvironment(t)
	repo := &fakeRepository{env: env, result: gitsync.ExportResult{Pushed: true, Commit: "c"}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	agent := &mockAgentAdapter{runTurnFn: func(context.Context, domain.Session, domain.RunTurnParams) (domain.TurnResult, error) {
		cancel()
		return domain.TurnResult{ExitReason: domain.EventTurnCompleted}, nil
	}}

	result, _ := runExportWorker(t, ctx, cfg, env, repo, agent, PostureNormal)

	if len(repo.exportCalls()) != 0 {
		t.Errorf("Export called %d times, want none for a cancelled run", len(repo.exportCalls()))
	}
	if result.ExitKind != WorkerExitCancelled || result.ExportedCommit != "" {
		t.Errorf("result = %v, %v, exported %q; want a cancelled exit without an export", result.ExitKind, result.Error, result.ExportedCommit)
	}
}

// cancellingEnvironment cancels the worker's context when the after_run
// hook starts, so the run ends normally with its context done.
type cancellingEnvironment struct {
	*environmenttest.Recorder
	script string
	cancel context.CancelFunc
}

func (e cancellingEnvironment) Command(ctx context.Context, ws environment.Workspace, spec environment.CommandSpec) (*exec.Cmd, error) {
	if len(spec.Argv) == 3 && spec.Argv[2] == e.script {
		e.cancel()
	}
	return e.Recorder.Command(ctx, ws, spec)
}

func TestRunWorkerAttempt_NoExportAfterTheRunWasStopped(t *testing.T) {
	cfg := exportWorkerConfig(t)
	rec := recordingEnvironment(t)
	repo := &fakeRepository{env: rec, result: gitsync.ExportResult{Pushed: true, Commit: "c"}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env := cancellingEnvironment{Recorder: rec, script: cfg.Hooks.AfterRun, cancel: cancel}

	result, _ := runExportWorker(t, ctx, cfg, env, repo, nil, PostureNormal)

	if len(repo.exportCalls()) != 0 {
		t.Errorf("Export called %d times, want none once the run was stopped", len(repo.exportCalls()))
	}
	if _, ok := errors.AsType[*ExportError](result.Error); !ok || result.ExitKind != WorkerExitCancelled {
		t.Errorf("result = %v, %v; want a cancelled exit reporting the skipped export", result.ExitKind, result.Error)
	}
}

func TestRunWorkerAttempt_ExportKeepsTheSCMRecord(t *testing.T) {
	cfg := exportWorkerConfig(t)
	env := recordingEnvironment(t)
	pushedAt := time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)
	repo := &fakeRepository{env: env, result: gitsync.ExportResult{Branch: "sortie/TEST-1", Commit: "aaa", Pushed: true, PushedAt: pushedAt}}

	result, _ := runExportWorker(t, context.Background(), cfg, env, repo, nil, PostureNormal)
	if result.ExitKind != WorkerExitNormal {
		t.Fatalf("result = %v, %v; want a normal exit", result.ExitKind, result.Error)
	}
	want := domain.SCMMetadata{Branch: "sortie/TEST-1", SHA: "aaa", PushedAt: "2026-10-12T10:00:00Z"}
	if got := workspace.ReadHostSCMRecord(result.WorkspacePath, nil); got != want {
		t.Fatalf("record after a push = %+v, want %+v", got, want)
	}
	info, err := os.Stat(filepath.Join(result.WorkspacePath, workspacekit.HostDir))
	if err != nil || info.Mode().Perm()&0o077 != 0 {
		t.Errorf("%s = %v, %v; want a directory private to its owner", workspacekit.HostDir, info, err)
	}

	// A pull request recorded since stays through a later export that
	// pushes nothing.
	want.PRNumber, want.Owner, want.Repo = 7, "org", "app"
	writeTrustedSCM(t, result.WorkspacePath, want)
	repo.result = gitsync.ExportResult{Branch: "sortie/TEST-1", Commit: "aaa"}
	result, _ = runExportWorker(t, context.Background(), cfg, env, repo, nil, PostureNormal)
	if result.ExitKind != WorkerExitNormal || result.ExportedCommit != "" {
		t.Fatalf("result = %v, %v, exported %q; want a normal exit without a push", result.ExitKind, result.Error, result.ExportedCommit)
	}
	if got := workspace.ReadHostSCMRecord(result.WorkspacePath, nil); got != want {
		t.Errorf("record after an export without a push = %+v, want %+v", got, want)
	}
}

// hostDirLinkingRepository plants a link where the workspace's host
// directory belongs before exporting.
type hostDirLinkingRepository struct {
	*fakeRepository
	target string
}

func (r hostDirLinkingRepository) Export(ctx context.Context, env environment.Environment, ws environment.Workspace) (gitsync.ExportResult, error) {
	if err := os.Symlink(r.target, filepath.Join(ws.HostPath, workspacekit.HostDir)); err != nil {
		return gitsync.ExportResult{}, err
	}
	return r.fakeRepository.Export(ctx, env, ws)
}

func TestRunWorkerAttempt_FailedSCMRecordFailsTheExport(t *testing.T) {
	cfg := exportWorkerConfig(t)
	env := recordingEnvironment(t)
	target := t.TempDir()
	// A link where the host directory belongs is refused, so the push
	// cannot be recorded.
	repo := hostDirLinkingRepository{
		fakeRepository: &fakeRepository{env: env, result: gitsync.ExportResult{Branch: "sortie/TEST-1", Commit: "aaa", Pushed: true}},
		target:         target,
	}
	ec := newExitCapture()
	RunWorkerAttempt(context.Background(), workerTestIssue(), nil, WorkerDeps{
		TrackerAdapter:         &mockTrackerAdapter{},
		AgentAdapter:           &mockAgentAdapter{},
		ConfigFunc:             func() config.ServiceConfig { return cfg },
		PromptTemplateByIDFunc: func(_ string) *prompt.Template { return mustParseTemplate(t, "{{ .issue.title }}") },
		OnEvent:                func(_ string, _ domain.AgentEvent) {},
		OnExit:                 ec.onExit,
		Logger:                 discardLogger(),
		EnvironmentFunc: func(config.EnvironmentConfig) (environment.Environment, error) {
			return env, nil
		},
		RepositoryFunc: func(config.EnvironmentConfig, string, *slog.Logger) (Repository, error) {
			return repo, nil
		},
	})
	result := ec.waitResult(t)

	exportErr, ok := errors.AsType[*ExportError](result.Error)
	if !ok || !errors.Is(result.Error, workspacekit.ErrLink) || exportErr.Branch != "sortie/TEST-1" {
		t.Fatalf("result error = %v, want an export error wrapping %v", result.Error, workspacekit.ErrLink)
	}
	if result.ExitKind != WorkerExitError || result.ExportedCommit != "" {
		t.Errorf("result = %v, exported %q; want a failed run", result.ExitKind, result.ExportedCommit)
	}
	if entries, _ := os.ReadDir(target); len(entries) != 0 {
		t.Errorf("link target holds %d entries, want it untouched", len(entries))
	}
}

func TestRunWorkerAttempt_UpstreamBranchRepairsAMissingSCMRecord(t *testing.T) {
	cfg := exportWorkerConfig(t)
	env := recordingEnvironment(t)
	target := t.TempDir()
	linking := hostDirLinkingRepository{
		fakeRepository: &fakeRepository{env: env, result: gitsync.ExportResult{Branch: "sortie/TEST-1", Commit: "aaa", Pushed: true}},
		target:         target,
	}
	ec := newExitCapture()
	RunWorkerAttempt(context.Background(), workerTestIssue(), nil, WorkerDeps{
		TrackerAdapter:         &mockTrackerAdapter{},
		AgentAdapter:           &mockAgentAdapter{},
		ConfigFunc:             func() config.ServiceConfig { return cfg },
		PromptTemplateByIDFunc: func(_ string) *prompt.Template { return mustParseTemplate(t, "{{ .issue.title }}") },
		OnEvent:                func(_ string, _ domain.AgentEvent) {},
		OnExit:                 ec.onExit,
		Logger:                 discardLogger(),
		EnvironmentFunc: func(config.EnvironmentConfig) (environment.Environment, error) {
			return env, nil
		},
		RepositoryFunc: func(config.EnvironmentConfig, string, *slog.Logger) (Repository, error) {
			return linking, nil
		},
	})
	first := ec.waitResult(t)
	if _, ok := errors.AsType[*ExportError](first.Error); !ok {
		t.Fatalf("first run error = %v, want the failed record write", first.Error)
	}
	if err := os.Remove(filepath.Join(first.WorkspacePath, workspacekit.HostDir)); err != nil {
		t.Fatal(err)
	}

	// The retry finds the pushed branch upstream with nothing to push,
	// and records it.
	repo := &fakeRepository{env: env, result: gitsync.ExportResult{Branch: "sortie/TEST-1", Commit: "aaa", UpToDate: true}}
	start := time.Now().Add(-time.Second).Truncate(time.Second)
	result, _ := runExportWorker(t, context.Background(), cfg, env, repo, nil, PostureNormal)
	if result.ExitKind != WorkerExitNormal || result.ExportedCommit != "" {
		t.Fatalf("result = %v, %v, exported %q; want a normal exit without a push", result.ExitKind, result.Error, result.ExportedCommit)
	}
	got := workspace.ReadHostSCMRecord(result.WorkspacePath, nil)
	pushedAt, err := time.Parse(time.RFC3339, got.PushedAt)
	if got.Branch != "sortie/TEST-1" || got.SHA != "aaa" || err != nil || pushedAt.Before(start) {
		t.Errorf("record = %+v, want sortie/TEST-1 at aaa with a push time", got)
	}
}

// fakeOpener records EnsurePullRequest calls and answers with pr and err.
type fakeOpener struct {
	mu    sync.Mutex
	pr    domain.PullRequest
	err   error
	calls []ensureCall
}

type ensureCall struct {
	Owner, Repo, Head, Base, Title, Body string
}

var _ domain.PullRequestOpener = (*fakeOpener)(nil)

func (f *fakeOpener) EnsurePullRequest(_ context.Context, owner, repo, head, base, title, body string) (domain.PullRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, ensureCall{owner, repo, head, base, title, body})
	return f.pr, f.err
}

func (f *fakeOpener) ensureCalls() []ensureCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

// pullRequestExport returns the parameters of an export from a
// workspace at ws through repo, whose pull request opener is opener.
func pullRequestExport(t *testing.T, ws string, repo *fakeRepository, opener *fakeOpener) exportParams {
	t.Helper()
	return exportParams{
		Repo:      repo,
		Workspace: environment.Workspace{Key: "ENG-42", HostPath: ws, GuestPath: ws, Identifier: "ENG-42"},
		Issue:     calculatorIssue(),
		PullRequest: &pullRequestTarget{
			Project: "group/sub/app", Base: "main",
			Opener: func() (domain.PullRequestOpener, error) { return opener, nil },
		},
		Logger: discardLogger(),
	}
}

func TestExportIssueBranch_EnsuresThePullRequest(t *testing.T) {
	t.Parallel()

	pushed := gitsync.ExportResult{Branch: "sortie/ENG-42", Commit: "aaa", Pushed: true, PushedAt: time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)}
	upToDate := gitsync.ExportResult{Branch: "sortie/ENG-42", Commit: "aaa", UpToDate: true}

	t.Run("a push opens the pull request with the suggested text and records it", func(t *testing.T) {
		t.Parallel()
		ws := t.TempDir()
		writeSuggestion(t, ws, []byte(`{"title": "Fix add()", "body": "Adds a test."}`))
		opener := &fakeOpener{pr: domain.PullRequest{Number: 9, URL: "https://forge.example/group/sub/app/pull/9", Created: true}}

		commit, err := exportIssueBranch(context.Background(), pullRequestExport(t, ws, &fakeRepository{result: pushed}, opener))
		if err != nil || commit != "aaa" {
			t.Fatalf("exportIssueBranch() = %q, %v; want aaa", commit, err)
		}
		want := []ensureCall{{Owner: "group/sub", Repo: "app", Head: "sortie/ENG-42", Base: "main", Title: "Fix add()", Body: "Adds a test."}}
		if got := opener.ensureCalls(); !slices.Equal(got, want) {
			t.Errorf("EnsurePullRequest calls = %+v, want %+v", got, want)
		}
		rec := domain.SCMMetadata{Branch: "sortie/ENG-42", SHA: "aaa", PushedAt: "2026-10-12T10:00:00Z", PRNumber: 9, Owner: "group/sub", Repo: "app"}
		if got := workspace.ReadHostSCMRecord(ws, nil); got != rec {
			t.Errorf("record = %+v, want %+v", got, rec)
		}
	})

	t.Run("without a suggestion the default title is used", func(t *testing.T) {
		t.Parallel()
		opener := &fakeOpener{pr: domain.PullRequest{Number: 9}}
		if _, err := exportIssueBranch(context.Background(), pullRequestExport(t, t.TempDir(), &fakeRepository{result: pushed}, opener)); err != nil {
			t.Fatal(err)
		}
		calls := opener.ensureCalls()
		if len(calls) != 1 || calls[0].Title != "ENG-42: Calculator adds wrong" || !strings.Contains(calls[0].Body, "https://tracker.example/ENG-42") {
			t.Errorf("EnsurePullRequest calls = %+v, want the default title and a body linking the issue", calls)
		}
	})

	t.Run("a failed call fails the export and the next export retries it", func(t *testing.T) {
		t.Parallel()
		ws := t.TempDir()
		cause := &domain.SCMError{Kind: domain.ErrSCMAuth, Message: "the token needs the api scope"}
		opener := &fakeOpener{err: cause}

		commit, err := exportIssueBranch(context.Background(), pullRequestExport(t, ws, &fakeRepository{result: pushed}, opener))
		exportErr, ok := errors.AsType[*ExportError](err)
		if !ok || exportErr.Branch != "sortie/ENG-42" || !errors.Is(err, cause) || commit != "" {
			t.Fatalf("exportIssueBranch() = %q, %v; want an export error wrapping the SCM error", commit, err)
		}
		if got := workspace.ReadHostSCMRecord(ws, nil); got.SHA != "aaa" || got.PRNumber != 0 {
			t.Errorf("record = %+v, want the push without a pull request", got)
		}

		// The retry finds the branch upstream and the record without a
		// pull request.
		opener.err, opener.pr = nil, domain.PullRequest{Number: 9}
		if _, err := exportIssueBranch(context.Background(), pullRequestExport(t, ws, &fakeRepository{result: upToDate}, opener)); err != nil {
			t.Fatalf("retried exportIssueBranch() error = %v", err)
		}
		if n := len(opener.ensureCalls()); n != 2 {
			t.Errorf("EnsurePullRequest calls = %d, want 2", n)
		}
		if got := workspace.ReadHostSCMRecord(ws, nil); got.PRNumber != 9 || got.Owner != "group/sub" || got.Repo != "app" {
			t.Errorf("record = %+v, want pull request 9 of group/sub/app", got)
		}

		// With the pull request recorded, an export that pushes nothing
		// does not ask again.
		if _, err := exportIssueBranch(context.Background(), pullRequestExport(t, ws, &fakeRepository{result: upToDate}, opener)); err != nil {
			t.Fatal(err)
		}
		if n := len(opener.ensureCalls()); n != 2 {
			t.Errorf("EnsurePullRequest calls = %d, want still 2", n)
		}
	})

	t.Run("an opener that cannot be built fails the export", func(t *testing.T) {
		t.Parallel()
		p := pullRequestExport(t, t.TempDir(), &fakeRepository{result: pushed}, nil)
		p.PullRequest.Opener = func() (domain.PullRequestOpener, error) { return nil, errors.New("no adapter") }
		if _, err := exportIssueBranch(context.Background(), p); err == nil || !strings.Contains(err.Error(), "no adapter") {
			t.Errorf("exportIssueBranch() error = %v, want the opener failure", err)
		}
	})

	for _, tt := range []struct {
		name   string
		result gitsync.ExportResult
	}{
		{"no issue branch", gitsync.ExportResult{Branch: "sortie/ENG-42", NothingToExport: true}},
		{"a branch at the base with no upstream branch", gitsync.ExportResult{Branch: "sortie/ENG-42", Commit: "base"}},
	} {
		t.Run(tt.name+" opens no pull request", func(t *testing.T) {
			t.Parallel()
			opener := &fakeOpener{pr: domain.PullRequest{Number: 9}}
			if _, err := exportIssueBranch(context.Background(), pullRequestExport(t, t.TempDir(), &fakeRepository{result: tt.result}, opener)); err != nil {
				t.Fatal(err)
			}
			if calls := opener.ensureCalls(); len(calls) != 0 {
				t.Errorf("EnsurePullRequest calls = %+v, want none", calls)
			}
		})
	}
}

func TestRunWorkerAttempt_OpensThePullRequestAfterThePush(t *testing.T) {
	cfg := exportWorkerConfig(t)
	cfg.Environment.Remote.Repository.Provider = "github"
	cfg.Environment.Remote.Repository.Project = "org/app"
	env := recordingEnvironment(t)
	repo := &fakeRepository{env: env, result: gitsync.ExportResult{Branch: "sortie/TEST-1", Commit: "aaa", Pushed: true}}

	run := func(opener *fakeOpener) (WorkerResult, []config.ServiceConfig) {
		var seen []config.ServiceConfig
		ec := newExitCapture()
		RunWorkerAttempt(context.Background(), workerTestIssue(), nil, WorkerDeps{
			TrackerAdapter:         &mockTrackerAdapter{},
			AgentAdapter:           &mockAgentAdapter{},
			ConfigFunc:             func() config.ServiceConfig { return cfg },
			PromptTemplateByIDFunc: func(_ string) *prompt.Template { return mustParseTemplate(t, "{{ .issue.title }}") },
			OnEvent:                func(_ string, _ domain.AgentEvent) {},
			OnExit:                 ec.onExit,
			Logger:                 discardLogger(),
			EnvironmentFunc: func(config.EnvironmentConfig) (environment.Environment, error) {
				return env, nil
			},
			RepositoryFunc: func(config.EnvironmentConfig, string, *slog.Logger) (Repository, error) {
				return repo, nil
			},
			PullRequestOpenerFunc: func(c config.ServiceConfig) (domain.PullRequestOpener, error) {
				seen = append(seen, c)
				return opener, nil
			},
		})
		return ec.waitResult(t), seen
	}

	failing := &fakeOpener{err: &domain.SCMError{Kind: domain.ErrSCMAPI, Message: "unavailable"}}
	result, _ := run(failing)
	if _, ok := errors.AsType[*ExportError](result.Error); !ok || result.ExitKind != WorkerExitError {
		t.Fatalf("result = %v, %v; want the run failed by the pull request call", result.ExitKind, result.Error)
	}

	opener := &fakeOpener{pr: domain.PullRequest{Number: 3, URL: "https://github.com/org/app/pull/3", Created: true}}
	repo.result = gitsync.ExportResult{Branch: "sortie/TEST-1", Commit: "aaa", UpToDate: true}
	result, seen := run(opener)
	if result.ExitKind != WorkerExitNormal || result.Error != nil {
		t.Fatalf("result = %v, %v; want a normal exit", result.ExitKind, result.Error)
	}
	calls := opener.ensureCalls()
	if len(calls) != 1 || calls[0].Head != "sortie/TEST-1" || calls[0].Base != "main" || calls[0].Title != "TEST-1: Test issue" {
		t.Errorf("EnsurePullRequest calls = %+v, want one for sortie/TEST-1 into main", calls)
	}
	if len(seen) != 1 || seen[0].Environment.Remote.Repository.Provider != "github" {
		t.Errorf("opener built for %d configurations, want the attempt's one", len(seen))
	}
	if got := workspace.ReadHostSCMRecord(result.WorkspacePath, nil); got.PRNumber != 3 || got.Owner != "org" || got.Repo != "app" || got.Branch != "sortie/TEST-1" {
		t.Errorf("record = %+v, want pull request 3 of org/app for sortie/TEST-1", got)
	}
}
