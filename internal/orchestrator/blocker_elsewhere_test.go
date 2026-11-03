package orchestrator

import (
	"context"
	"os"
	"slices"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/workspace"
)

// mergeRecordStore is a Sortie's own store holding the merge records of
// the stories that Sortie ran.
type mergeRecordStore struct {
	*stubStore
	satisfaction *fakeSatisfaction
}

func (s mergeRecordStore) LoadPlanSatisfaction(ctx context.Context, ids []string) (map[string][]persistence.PlanSatisfaction, error) {
	return s.satisfaction.LoadPlanSatisfaction(ctx, ids)
}

// newPlanRoutedHarness returns a routed harness with plan control on, over
// tracker, whose store holds the merge records of the issue IDs in merges.
func newPlanRoutedHarness(t *testing.T, env config.EnvironmentConfig, tracker *runTracker, merges ...string) *runHarness {
	t.Helper()
	h := unbuiltRoutedHarness(t, env, tracker)
	cfg := h.wm.Config()
	cfg.Tracker.ReleaseState, cfg.Tracker.BacklogState = "To Do", "Backlog"
	cfg.Tracker.DoneState, cfg.Tracker.CancelState = "Done", "Canceled"
	cfg.Tracker.TerminalStates = []string{"Done", "Canceled"}
	h.wm.setConfig(cfg)
	rows := map[string][]persistence.PlanSatisfaction{}
	for _, id := range merges {
		rows[id] = []persistence.PlanSatisfaction{merged(id)}
	}
	h.orchestratorStore = mergeRecordStore{stubStore: h.store, satisfaction: &fakeSatisfaction{rows: rows}}
	buildRouted(h)
	return h
}

// p1Env is p1's workflow: it runs sortie, the default, and lists homelab
// closed to dispatch. taichiEnv runs only homelab.
func p1Env() config.EnvironmentConfig {
	return routedEnvironment("sortie", routedRepository("sortie", false), routedRepository("homelab", true))
}

func taichiEnv() config.EnvironmentConfig {
	return routedEnvironment("", routedRepository("homelab", false))
}

// pilotTracker is one tracker both Sorties poll: homelab-pilot, in
// homelab, blocked by smolvm-machine-shape, a sortie story in state.
func pilotTracker(state string) *runTracker {
	tracker := &runTracker{mockTrackerAdapter: &mockTrackerAdapter{}, issues: map[string]domain.Issue{}}
	pilot := candidate("id-pilot", "ONS-19", repoLabel("homelab"))
	pilot.BlockedBy = []domain.BlockerRef{{
		ID: "id-shape", Identifier: "ONS-16", State: state,
		PlanKey: "smolvm-machine-shape", Labels: []string{repoLabel("sortie")},
	}}
	tracker.candidates = []domain.Issue{pilot}
	return tracker
}

// Scenario: a homelab story after a Sortie story. taichi's Sortie has no
// record of the merge p1's Sortie latched, and judges the blocker by its
// tracker state.
func TestHandleTick_BlockerAnotherSortieRunsIsJudgedByItsState(t *testing.T) {
	t.Parallel()

	tracker := pilotTracker("Done")
	p1 := newPlanRoutedHarness(t, p1Env(), tracker, "id-shape")
	taichi := newPlanRoutedHarness(t, taichiEnv(), tracker)

	dispatched, written := tickOnce(p1)
	if len(dispatched) != 0 {
		t.Errorf("p1 dispatched %v, want nothing", dispatched)
	}
	assertLeftAlone(t, p1, written, "id-pilot", "ONS-19")

	dispatched, _ = tickOnce(taichi)
	if !slices.Equal(dispatched, []string{"id-pilot"}) {
		t.Errorf("taichi dispatched %v, want id-pilot", dispatched)
	}
	if held, ok := taichi.o.state.Held["id-pilot"]; ok {
		t.Errorf("taichi holds id-pilot: %+v", held)
	}
}

// Scenario: not done yet. taichi holds the story and its hold names the
// blocker and says it is not done.
func TestHandleTick_BlockerAnotherSortieRunsNotDoneHolds(t *testing.T) {
	t.Parallel()

	tracker := pilotTracker("In Review")
	taichi := newPlanRoutedHarness(t, taichiEnv(), tracker, "id-shape")

	dispatched, _ := tickOnce(taichi)
	if len(dispatched) != 0 {
		t.Fatalf("taichi dispatched %v, want id-pilot held", dispatched)
	}
	held := taichi.o.state.Held["id-pilot"]
	if held == nil || held.Reason != SkipBlockedBy {
		t.Fatalf("hold = %+v, want held by its blocker", held)
	}
	if len(held.Blockers) != 1 || held.Blockers[0].Identifier != "ONS-16" || held.Blockers[0].Reason != BlockerNotDone {
		t.Errorf("hold blockers = %+v, want ONS-16 not done", held.Blockers)
	}
	snap := RuntimeSnapshot(taichi.o.state, held.Since)
	if len(snap.Held) != 1 || len(snap.Held[0].Hold.Blockers) != 1 {
		t.Fatalf("snapshot held = %+v, want one row naming one blocker", snap.Held)
	}
	if b := snap.Held[0].Hold.Blockers[0]; b.Identifier != "ONS-16" || b.State != "In Review" || b.Reason != BlockerNotDone {
		t.Errorf("snapshot blocker = %+v, want ONS-16 In Review not_done", b)
	}
}

// A blocker in a repository this Sortie dispatches keeps the merge rule:
// p1 holds a sortie story behind a Done sortie story it has no merge
// record of, names that blocker and why, and dispatches it once the
// record exists.
func TestHandleTick_OwnBlockerKeepsTheMergeRule(t *testing.T) {
	t.Parallel()

	newTracker := func() *runTracker {
		tracker := &runTracker{mockTrackerAdapter: &mockTrackerAdapter{}, issues: map[string]domain.Issue{}}
		next := candidate("id-next", "ONS-20", repoLabel("sortie"))
		next.BlockedBy = []domain.BlockerRef{
			{ID: "id-done", Identifier: "ONS-15", State: "Done", PlanKey: "done-unkeyed-merge", Labels: []string{repoLabel("sortie")}},
			{ID: "id-home", Identifier: "ONS-17", State: "Done", PlanKey: "homelab-done", Labels: []string{repoLabel("homelab")}},
			{ID: "id-shape", Identifier: "ONS-16", State: "Done", PlanKey: "smolvm-machine-shape"},
		}
		tracker.candidates = []domain.Issue{next}
		return tracker
	}

	held := newPlanRoutedHarness(t, p1Env(), newTracker(), "id-shape")
	if dispatched, _ := tickOnce(held); len(dispatched) != 0 {
		t.Fatalf("p1 dispatched %v past a Done blocker it holds no merge record of", dispatched)
	}
	entry := held.o.state.Held["id-next"]
	if entry == nil || len(entry.Blockers) != 1 || entry.Blockers[0].Identifier != "ONS-15" || entry.Blockers[0].Reason != BlockerAwaitingMerge {
		t.Errorf("hold = %+v, want only ONS-15 awaiting its merge record", entry)
	}

	free := newPlanRoutedHarness(t, p1Env(), newTracker(), "id-shape", "id-done")
	if dispatched, _ := tickOnce(free); !slices.Equal(dispatched, []string{"id-next"}) {
		t.Errorf("p1 dispatched %v, want id-next once both its own blockers merged", dispatched)
	}
}

func TestBlockerRunsElsewhere(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	path, err := workspace.ComputePath(root, "ONS-5")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(path.Path, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := workspace.WriteRepositoryRecord(path.Path, workspace.RepositoryRecord{Name: "homelab"}); err != nil {
		t.Fatal(err)
	}

	if f := BlockerRunsElsewhere(config.EnvironmentConfig{}, root); f != nil {
		t.Error("BlockerRunsElsewhere(no repository) is not nil, want every blocker this Sortie's own")
	}
	tests := []struct {
		name string
		env  config.EnvironmentConfig
		b    domain.BlockerRef
		want bool
	}{
		{"open repository", p1Env(), domain.BlockerRef{Identifier: "ONS-1", Labels: []string{repoLabel("sortie")}}, false},
		{"closed repository", p1Env(), domain.BlockerRef{Identifier: "ONS-1", Labels: []string{repoLabel("homelab")}}, true},
		{"unlisted repository", taichiEnv(), domain.BlockerRef{Identifier: "ONS-1", Labels: []string{repoLabel("sortie")}}, true},
		{"no label, default", p1Env(), domain.BlockerRef{Identifier: "ONS-1"}, false},
		{"no label, no default", taichiEnv(), domain.BlockerRef{Identifier: "ONS-1"}, true},
		{"seeded from a repository now closed", p1Env(), domain.BlockerRef{Identifier: "ONS-5", Labels: []string{repoLabel("sortie")}}, true},
		{"seeded from an open repository", taichiEnv(), domain.BlockerRef{Identifier: "ONS-5", Labels: []string{repoLabel("sortie")}}, false},
	}
	for _, tt := range tests {
		if got := BlockerRunsElsewhere(tt.env, root)(tt.b); got != tt.want {
			t.Errorf("%s: BlockerRunsElsewhere(%+v) = %v, want %v", tt.name, tt.b, got, tt.want)
		}
	}
}

// The judge applies the done-state rule to a blocker another Sortie runs,
// whatever its plan key and merge records.
func TestPlanJudge_BlockerRunsElsewhere(t *testing.T) {
	t.Parallel()

	elsewhere := func(b domain.BlockerRef) bool { return b.ID == "far" }
	tests := []struct {
		name string
		b    domain.BlockerRef
		want string
	}{
		{"done", domain.BlockerRef{ID: "far", PlanKey: "k", State: "Done"}, ""},
		{"canceled", domain.BlockerRef{ID: "far", PlanKey: "k", State: "Canceled"}, BlockerCanceled},
		{"in review", domain.BlockerRef{ID: "far", PlanKey: "k", State: "In Review"}, BlockerNotDone},
		{"another terminal state", domain.BlockerRef{ID: "far", PlanKey: "k", State: "Merged"}, BlockerNotDone},
		{"state unknown", domain.BlockerRef{ID: "far", PlanKey: "k"}, BlockerUnknown},
		{"own blocker done keeps the merge rule", domain.BlockerRef{ID: "near", PlanKey: "k", State: "Done"}, BlockerAwaitingMerge},
	}
	for _, tt := range tests {
		j := NewPlanJudge(context.Background(), &fakeSatisfaction{}, "Canceled", "Done", discardLogger())
		j.RunsElsewhere = elsewhere
		if got := j.Unsatisfied(tt.b, planTerminal); got != tt.want {
			t.Errorf("%s: Unsatisfied(%+v) = %q, want %q", tt.name, tt.b, got, tt.want)
		}
	}
}
