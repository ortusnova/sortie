package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/worker"
	"github.com/sortie-ai/sortie/internal/workspace"
)

// routedRepository returns a repositories entry called name.
func routedRepository(name string, closed bool) config.Repository {
	return config.Repository{
		Name: name, URL: "https://example.com/org/" + name + ".git", Base: "main",
		BranchPrefix: "sortie/", MaxBundleMiB: 512, ClosedToDispatch: closed,
	}
}

// routedEnvironment returns a remote environment that lists repos and
// names def the default, or no default when def is empty.
func routedEnvironment(def string, repos ...config.Repository) config.EnvironmentConfig {
	env := config.EnvironmentConfig{Kind: config.EnvironmentKindRemote, Remote: config.RemoteConfig{
		Backend: config.DefaultRemoteBackend, Repositories: repos, DefaultRepository: def,
	}}
	if def != "" {
		env.Remote.Repository, _ = env.RepositorySet().RepositoryNamed(def)
	}
	return env
}

// newRoutedHarness returns a run harness whose workflow runs under env,
// over tracker when it is not nil, built by [buildRouted].
func newRoutedHarness(t *testing.T, env config.EnvironmentConfig, tracker *runTracker) *runHarness {
	t.Helper()
	h := unbuiltRoutedHarness(t, env, tracker)
	buildRouted(h)
	return h
}

// unbuiltRoutedHarness returns the harness of [newRoutedHarness] before
// it is built.
func unbuiltRoutedHarness(t *testing.T, env config.EnvironmentConfig, tracker *runTracker) *runHarness {
	t.Helper()
	cfg := runConfig(t, map[string]any{"agent": map[string]any{"kind": "kind-a", "command": "a-cmd"}})
	cfg.Tracker.InProgressState = "In Progress"
	cfg.Agent.MaxSessions = 1
	cfg.Environment = env
	h := newRunHarness(t, cfg, "kind-a")
	if tracker != nil {
		h.tracker = tracker
	}
	return h
}

// buildRouted builds h without a worker matcher, so a dispatched issue
// is not held for a worker and its attempt fails its preparation, the
// remote environment running nothing on this host; the dispatch
// decision is what these tests read.
func buildRouted(h *runHarness) {
	h.t.Helper()
	h.build()
	h.o.remote = nil
}

func repoLabel(name string) string { return config.RepositoryLabelPrefix + name }

// trackerWrites returns the issue IDs of the state changes and of the
// comments tracker took, each in order.
func trackerWrites(tracker *runTracker) (moved, commented []string) {
	for _, c := range tracker.transitionCalls {
		moved = append(moved, c.IssueID)
	}
	tracker.commentMu.Lock()
	defer tracker.commentMu.Unlock()
	for _, c := range tracker.commentCalls {
		commented = append(commented, c.IssueID)
	}
	return moved, commented
}

// tickOnce runs one poll of h and waits for the attempts it started. It
// returns the issues the poll dispatched and the issues of every tracker
// write the poll and its attempts made.
func tickOnce(h *runHarness) (dispatched, written []string) {
	h.t.Helper()
	h.build()
	movedBefore, commentedBefore := trackerWrites(h.tracker)
	h.o.handleTick(context.Background())
	for id := range h.o.state.Running {
		dispatched = append(dispatched, id)
	}
	slices.Sort(dispatched)
	h.o.state.WorkerWg.Wait()
	moved, commented := trackerWrites(h.tracker)
	written = append(moved[len(movedBefore):], commented[len(commentedBefore):]...)
	return dispatched, written
}

// assertLeftAlone fails unless h holds nothing of the issue id, started
// no session for it and wrote nothing of it to the tracker.
func assertLeftAlone(t *testing.T, h *runHarness, written []string, id, identifier string) {
	t.Helper()
	s := h.o.state
	if _, ok := s.Running[id]; ok {
		t.Errorf("%s is running", id)
	}
	if _, ok := s.Claimed[id]; ok {
		t.Errorf("%s is claimed", id)
	}
	if _, ok := s.RetryAttempts[id]; ok {
		t.Errorf("%s has a retry", id)
	}
	if _, ok := s.Held[id]; ok {
		t.Errorf("%s is held", id)
	}
	if _, ok := s.BudgetExhausted[id]; ok {
		t.Errorf("%s is counted against max_sessions", id)
	}
	if slices.Contains(written, id) {
		t.Errorf("%s was moved or commented on", id)
	}
	for _, sess := range h.started()["kind-a"] {
		if filepath.Base(sess.WorkspacePath) == identifier {
			t.Errorf("%s had a session", id)
		}
	}
}

// A listed repository may be closed to dispatch: its issues are never
// dispatched, while an issue in an open repository is.
func TestHandleTick_ClosedRepositoryIsNotDispatched(t *testing.T) {
	t.Parallel()

	env := routedEnvironment("sortie", routedRepository("sortie", false), routedRepository("homelab", true))
	h := newRoutedHarness(t, env, nil)
	h.tracker.candidates = []domain.Issue{
		candidate("id-open", "ONS-1", repoLabel("sortie")),
		candidate("id-closed", "ONS-2", repoLabel("homelab")),
	}

	dispatched, written := tickOnce(h)
	if !slices.Equal(dispatched, []string{"id-open"}) {
		t.Fatalf("dispatched %v, want only id-open", dispatched)
	}
	assertLeftAlone(t, h, written, "id-closed", "ONS-2")
}

// The default repository is optional: without one, an issue that names
// no repository is not dispatched, and one that names a listed
// repository is.
func TestHandleTick_NoDefaultRepositoryLeavesUnlabeledIssues(t *testing.T) {
	t.Parallel()

	env := routedEnvironment("", routedRepository("homelab", false))
	h := newRoutedHarness(t, env, nil)
	h.tracker.candidates = []domain.Issue{
		candidate("id-unlabeled", "ONS-1"),
		candidate("id-homelab", "ONS-2", repoLabel("homelab")),
	}

	dispatched, written := tickOnce(h)
	if !slices.Equal(dispatched, []string{"id-homelab"}) {
		t.Fatalf("dispatched %v, want only id-homelab", dispatched)
	}
	assertLeftAlone(t, h, written, "id-unlabeled", "ONS-1")
}

// Another Sortie's issue is left alone: two Sorties poll one tracker
// project, each listing the repository the other dispatches as closed
// or not at all, and each runs only its own issue.
func TestHandleTick_TwoSortiesShareOneTrackerProject(t *testing.T) {
	t.Parallel()

	tracker := &runTracker{mockTrackerAdapter: &mockTrackerAdapter{}, issues: map[string]domain.Issue{}}
	tracker.candidates = []domain.Issue{
		candidate("id-sortie", "ONS-10"),
		candidate("id-homelab", "ONS-12", repoLabel("homelab")),
		candidate("id-elsewhere", "ONS-13", repoLabel("elsewhere")),
	}

	p1 := newRoutedHarness(t, routedEnvironment("sortie", routedRepository("sortie", false), routedRepository("homelab", true)), tracker)
	taichi := newRoutedHarness(t, routedEnvironment("", routedRepository("homelab", false)), tracker)

	for _, sortie := range []struct {
		name string
		h    *runHarness
		own  string
	}{{"p1", p1, "id-sortie"}, {"taichi", taichi, "id-homelab"}} {
		dispatched, written := tickOnce(sortie.h)
		if !slices.Equal(dispatched, []string{sortie.own}) {
			t.Errorf("%s dispatched %v, want only %s", sortie.name, dispatched, sortie.own)
		}
		for _, other := range tracker.candidates {
			if other.ID != sortie.own {
				assertLeftAlone(t, sortie.h, written, other.ID, other.Identifier)
			}
		}
	}
}

// A retry of an issue the workflow no longer dispatches, its label moved
// to another Sortie's repository, releases the claim without a dispatch.
func TestHandleRetryTimer_IssueLeftToAnotherWorkflowReleasesTheClaim(t *testing.T) {
	t.Parallel()

	issue := candidateIssue("ISS-1", "ISS-1", "To Do")
	issue.Labels = []string{repoLabel("homelab")}
	store := &mockRetryStore{}
	state := retryState(t, "ISS-1", "ISS-1", 1)
	params := defaultRetryParams(t, store, &mockRetryTracker{fetchedIssue: issue})
	env := routedEnvironment("sortie", routedRepository("sortie", false), routedRepository("homelab", true))
	params.Dispatches = func(i domain.Issue) bool { return dispatchesIssue(env, t.TempDir(), i) }
	params.MakeWorkerFn = func(_, _, _, _, _ string, _ domain.AgentAdapter, _ AttemptSettings) WorkerFunc {
		t.Error("the retry dispatched an issue its workflow leaves to another")
		return func(context.Context, domain.Issue, *int) {}
	}

	HandleRetryTimer(state, "ISS-1", params)

	if _, ok := state.Claimed["ISS-1"]; ok {
		t.Error("claim kept, want it released")
	}
	if _, ok := state.Running["ISS-1"]; ok {
		t.Error("issue running, want it left alone")
	}
	if !slices.Contains(store.deletedIssueID, "ISS-1") {
		t.Errorf("deleted retry entries = %v, want ISS-1", store.deletedIssueID)
	}
}

func TestDispatchesIssue(t *testing.T) {
	t.Parallel()

	listed := routedEnvironment("sortie", routedRepository("sortie", false), routedRepository("homelab", true))
	noDefault := routedEnvironment("", routedRepository("homelab", false))
	single := config.EnvironmentConfig{Kind: config.EnvironmentKindRemote}
	only := routedRepository("", false)
	single.Remote.Repository, single.Remote.Repositories = &only, []config.Repository{only}

	tests := []struct {
		name   string
		env    config.EnvironmentConfig
		labels []string
		want   bool
	}{
		{"no repository configured", config.EnvironmentConfig{}, []string{repoLabel("anything")}, true},
		{"default for an unlabeled issue", listed, nil, true},
		{"open repository", listed, []string{repoLabel("sortie")}, true},
		{"closed repository", listed, []string{repoLabel("homelab")}, false},
		{"unlisted repository", listed, []string{repoLabel("elsewhere")}, false},
		{"no default for an unlabeled issue", noDefault, []string{"bug"}, false},
		{"no default for a labeled issue", noDefault, []string{repoLabel("HomeLab")}, true},
		{"single block for an unlabeled issue", single, nil, true},
		{"single block for another repository", single, []string{repoLabel("homelab")}, false},
	}
	root := t.TempDir()
	for _, tt := range tests {
		if got := dispatchesIssue(tt.env, root, candidate("id", "ONS-1", tt.labels...)); got != tt.want {
			t.Errorf("%s: dispatchesIssue(%v) = %v, want %v", tt.name, tt.labels, got, tt.want)
		}
	}
}

// State shows the repositories each Sortie runs: config.repositories
// lists every repository with whether the Sortie dispatches it and
// which is its default.
func TestWorkflowShape_ShowsTheRepositoriesEachSortieRuns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		env  config.EnvironmentConfig
		want []ShapeRepository
	}{
		{
			name: "p1",
			env:  routedEnvironment("sortie", routedRepository("sortie", false), routedRepository("homelab", true)),
			want: []ShapeRepository{{Name: "sortie", Dispatch: true, Default: true, Requires: backendOnly()}, {Name: "homelab", Dispatch: false, Requires: backendOnly()}},
		},
		{
			name: "taichi",
			env:  routedEnvironment("", routedRepository("homelab", false)),
			want: []ShapeRepository{{Name: "homelab", Dispatch: true, Requires: backendOnly()}},
		},
		{name: "no repository", env: config.EnvironmentConfig{}, want: []ShapeRepository{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newRoutedHarness(t, tt.env, nil)
			h.build()
			got := h.o.WorkflowShape().Repositories
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Repositories = %+v, want %+v", got, tt.want)
			}
			raw, err := json.Marshal(h.o.WorkflowShape())
			if err != nil {
				t.Fatal(err)
			}
			var wire struct {
				Repositories []map[string]any `json:"repositories"`
			}
			if err := json.Unmarshal(raw, &wire); err != nil || wire.Repositories == nil || len(wire.Repositories) != len(tt.want) {
				t.Errorf("wire repositories = %s, want %d entries", raw, len(tt.want))
			}
		})
	}
}

// Under the remote kind, config.repositories serves what a worker must
// declare to take a job of each repository, the backend among the
// capabilities, so a client can match a queued job to the workers before
// Sortie offers it; any other kind configures no repository.
func TestWorkflowShape_ServesWhatEachRepositoryRequires(t *testing.T) {
	t.Parallel()

	homelab := routedRepository("homelab", false)
	homelab.Requires = &config.RemoteRequirements{Capabilities: []string{"kvm", "nested"}, CPU: 4, MemGiB: 16, DiskGiB: 80}
	sortie := routedRepository("sortie", false)
	env := config.EnvironmentConfig{Kind: config.EnvironmentKindRemote, Remote: config.RemoteConfig{
		Backend: "smolvm", Requires: config.RemoteRequirements{CPU: 2, MemGiB: 4},
		Repositories: []config.Repository{sortie, homelab}, DefaultRepository: "sortie",
	}}
	env.Remote.Repository = &env.Remote.Repositories[0]

	got := shapeRepositories(env)
	want := []ShapeRepository{
		{Name: "sortie", Dispatch: true, Default: true, Requires: &worker.Requires{Capabilities: []string{"smolvm"}, CPU: 2, MemGiB: 4}},
		{Name: "homelab", Dispatch: true, Requires: &worker.Requires{Capabilities: []string{"kvm", "nested", "smolvm"}, CPU: 4, MemGiB: 16, DiskGiB: 80}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("remote repositories = %+v, want %+v", got, want)
	}
	if homelab.Requires.Capabilities[len(homelab.Requires.Capabilities)-1] != "nested" {
		t.Errorf("the configured capabilities were changed: %v", homelab.Requires.Capabilities)
	}

	raw, err := json.Marshal(shapeRepositories(config.EnvironmentConfig{Kind: config.EnvironmentKindLocal}))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "[]" {
		t.Errorf("local repositories = %s, want none", raw)
	}
}

// leftAloneLines returns the debug lines buf holds for issue id, each
// left out of a poll.
func leftAloneLines(buf *bytes.Buffer, id string) []string {
	var lines []string
	for line := range strings.Lines(buf.String()) {
		if strings.Contains(line, "issue left to another workflow") && strings.Contains(line, "issue_id="+id+" ") {
			lines = append(lines, line)
		}
	}
	return lines
}

// A left-out issue is logged at debug level once, naming the issue and
// the reason, and again only when the reason changes or after a poll
// that did not leave it out.
func TestHandleTick_LogsALeftOutIssueOnce(t *testing.T) {
	t.Parallel()

	env := routedEnvironment("sortie", routedRepository("sortie", false), routedRepository("homelab", true))
	h := newRoutedHarness(t, env, nil)
	h.tracker.candidates = []domain.Issue{
		candidate("id-open", "ONS-1", repoLabel("sortie")),
		candidate("id-closed", "ONS-2", repoLabel("homelab")),
	}
	h.build()
	var buf bytes.Buffer
	h.o.logger = debugLogger(t, &buf)

	tickOnce(h)
	lines := leftAloneLines(&buf, "id-closed")
	if len(lines) != 1 || !strings.Contains(lines[0], "level=DEBUG") || !strings.Contains(lines[0], "issue_identifier=ONS-2") || !strings.Contains(lines[0], "closed to dispatch") {
		t.Fatalf("first poll logged %q, want one debug line naming ONS-2 and the reason", lines)
	}
	if lines := leftAloneLines(&buf, "id-open"); len(lines) != 0 {
		t.Errorf("first poll logged the dispatched issue: %q", lines)
	}
	buf.Reset()
	tickOnce(h)
	if lines := leftAloneLines(&buf, "id-closed"); len(lines) != 0 {
		t.Errorf("second poll logged %q, want nothing", lines)
	}
	buf.Reset()

	h.tracker.candidates = []domain.Issue{candidate("id-closed", "ONS-2", repoLabel("elsewhere"))}
	tickOnce(h)
	if lines := leftAloneLines(&buf, "id-closed"); len(lines) != 1 || !strings.Contains(lines[0], "elsewhere") {
		t.Errorf("poll with a new reason logged %q, want one line naming it", lines)
	}
	buf.Reset()

	h.tracker.candidates = nil
	tickOnce(h)
	h.tracker.candidates = []domain.Issue{candidate("id-closed", "ONS-2", repoLabel("elsewhere"))}
	tickOnce(h)
	if lines := leftAloneLines(&buf, "id-closed"); len(lines) != 1 {
		t.Errorf("poll after the issue was absent logged %q, want one line", lines)
	}
}

// A workspace keeps the repository it was seeded from: an issue whose
// workspace was seeded from an open repository is dispatched even when
// its label now names a repository the workflow leaves alone, and one
// seeded from a closed repository is left alone whatever its label.
func TestDispatchesIssue_SeededWorkspaceKeepsItsRepository(t *testing.T) {
	t.Parallel()

	env := routedEnvironment("", routedRepository("sortie", false), routedRepository("homelab", true))
	root := t.TempDir()
	seed := func(identifier, repo string) {
		t.Helper()
		path, err := workspace.ComputePath(root, identifier)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(path.Path, 0o750); err != nil {
			t.Fatal(err)
		}
		if err := workspace.WriteRepositoryRecord(path.Path, workspace.RepositoryRecord{Name: repo}); err != nil {
			t.Fatal(err)
		}
	}
	seed("ONS-1", "sortie")
	seed("ONS-2", "homelab")

	tests := []struct {
		name  string
		issue domain.Issue
		want  bool
	}{
		{"seeded open, label closed", candidate("id-1", "ONS-1", repoLabel("homelab")), true},
		{"seeded open, no label and no default", candidate("id-1", "ONS-1"), true},
		{"seeded closed, label open", candidate("id-2", "ONS-2", repoLabel("sortie")), false},
		{"not seeded, label closed", candidate("id-3", "ONS-3", repoLabel("homelab")), false},
	}
	for _, tt := range tests {
		if got := dispatchesIssue(env, root, tt.issue); got != tt.want {
			t.Errorf("%s: dispatchesIssue = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// backendOnly is what a worker must declare for a job of a repository
// of [routedEnvironment]: the default backend alone.
func backendOnly() *worker.Requires {
	return &worker.Requires{Capabilities: []string{config.DefaultRemoteBackend}}
}
