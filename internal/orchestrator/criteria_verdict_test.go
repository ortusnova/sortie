package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/workspace"
	"github.com/sortie-ai/sortie/internal/workspacekit"
)

// allMet is a verdict store whose every story's review found every
// criterion met.
type allMet struct{}

func (allMet) LoadCriteriaVerdict(_ context.Context, id string) (persistence.CriteriaVerdict, bool, error) {
	return persistence.CriteriaVerdict{IssueID: id, Outcome: CriteriaOutcomeMet}, true, nil
}

// verdictStore keeps verdicts in memory, as the store does.
type verdictStore struct {
	mu   sync.Mutex
	rows map[string]persistence.CriteriaVerdict
	err  error
}

func (s *verdictStore) UpsertCriteriaVerdict(_ context.Context, v persistence.CriteriaVerdict) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rows == nil {
		s.rows = map[string]persistence.CriteriaVerdict{}
	}
	s.rows[v.IssueID] = v
	return nil
}

func (s *verdictStore) LoadCriteriaVerdict(_ context.Context, id string) (persistence.CriteriaVerdict, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return persistence.CriteriaVerdict{}, false, s.err
	}
	v, ok := s.rows[id]
	return v, ok, nil
}

// verdictExitStore is the exit store with the verdict capability.
type verdictExitStore struct {
	*mockExitStore
	verdictStore
}

const (
	criterionChain = "Run by the one Sortie on taichi's worker, the full vm chain in its machine ends DONE with every step at exit code 0, and make test passes"
	criterionHosts = "scripts/smolvm/egress-hosts.yaml lists each host the chain needs"
)

func TestEvaluateCriteria(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		file    string
		outcome string
	}{
		{"no file", "", CriteriaOutcomeRefused},
		{"not json", "all good", CriteriaOutcomeRefused},
		{"no criterion", `{"criteria": []}`, CriteriaOutcomeRefused},
		{"every criterion met", `{"criteria": [{"criterion": "a", "verdict": "met", "evidence": "tests/test_a.py passes"}, {"criterion": "b", "verdict": "Met", "evidence": "make test: 12 passed"}]}`, CriteriaOutcomeMet},
		{"met with no evidence", `{"criteria": [{"criterion": "a", "verdict": "met"}]}`, CriteriaOutcomeRefused},
		{"one not met", `{"criteria": [{"criterion": "a", "verdict": "met", "evidence": "x"}, {"criterion": "b", "verdict": "not-met", "evidence": "the chain timed out"}]}`, CriteriaOutcomeRefused},
		{"one unsettled", `{"criteria": [{"criterion": "a", "verdict": "unsettled", "evidence": "conflicts with D8"}]}`, CriteriaOutcomeRefused},
		{"one waits for a live proof", `{"criteria": [{"criterion": "a", "verdict": "met", "evidence": "x"}, {"criterion": "b", "verdict": "live-proof", "evidence": "egress refused"}]}`, CriteriaOutcomeLiveProof},
		{"live proof and not met", `{"criteria": [{"criterion": "a", "verdict": "live-proof", "evidence": "x"}, {"criterion": "b", "verdict": "not-met", "evidence": "y"}]}`, CriteriaOutcomeRefused},
		{"an unknown verdict", `{"criteria": [{"criterion": "a", "verdict": "partly", "evidence": "x"}]}`, CriteriaOutcomeRefused},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := evaluateCriteria([]byte(tt.file)); got.Outcome != tt.outcome {
				t.Errorf("outcome = %q (%s), want %q", got.Outcome, got.Problem, tt.outcome)
			}
		})
	}
}

// reviewExitFixture is a criteria review under plan control that ended
// normally, writing no status, on a story with a pull request and
// auto-merge configured; criteria is the .sortie/criteria.json it left.
func reviewExitFixture(t *testing.T, criteria string) (*hopExitFixture, *verdictExitStore) {
	t.Helper()
	f := criteriaExitFixture(t, config.OnFailEscalate)
	f.result.SoftStop, f.result.SoftStopReason = false, ""
	f.state.Running[hopExitIssueID].LastAgentMessage = "No acceptance criterion is fully proven yet. I added no commits and wrote no `.sortie/status`."
	if criteria != "" {
		f.result.CriteriaFile = []byte(criteria)
	}
	store := &verdictExitStore{mockExitStore: f.store}
	f.params.Store = store
	return f, store
}

// ONS-19: a review that found the full-chain criterion not met, and
// stopped with no status, is a refusal: the pull request is not merged,
// and the story parks with the criterion as the supervisor's question.
func TestCriteriaReview_UnmetCriterionParksInsteadOfMerging(t *testing.T) {
	t.Parallel()
	for name, criteria := range map[string]string{
		"a criterion not met": `{"criteria": [
			{"criterion": "` + criterionChain + `", "verdict": "not-met", "evidence": "seed-vm-recreate timed out: the nested VMs have no network"},
			{"criterion": "` + criterionHosts + `", "verdict": "met", "evidence": "tests/test_egress_hosts.py passes"}]}`,
		"no verdict at all": "",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f, store := reviewExitFixture(t, criteria)
			f.exit()

			if _, ok := f.state.PendingReactions[ReactionKey(hopExitIssueID, ReactionKindAutoMerge)]; ok {
				t.Error("auto-merge seeded after a review that found a criterion not met")
			}
			park, parked := f.state.Parked[hopExitIssueID]
			if !parked {
				t.Fatal("story not parked")
			}
			want := "no verdict per criterion"
			if criteria != "" {
				want = "seed-vm-recreate timed out"
			}
			if !strings.Contains(park.Statement, want) || !strings.Contains(park.Statement, "Supervisor:") {
				t.Errorf("park statement = %q, want the question naming %q", park.Statement, want)
			}
			commented := false
			for _, c := range f.tracker.commentCalls {
				if strings.Contains(c.Text, "not merged") && strings.Contains(c.Text, want) {
					commented = true
				}
			}
			if !commented {
				t.Errorf("comments = %+v, want one naming %q", f.tracker.commentCalls, want)
			}
			if v, ok, _ := store.LoadCriteriaVerdict(context.Background(), hopExitIssueID); !ok || v.Outcome != CriteriaOutcomeRefused {
				t.Errorf("recorded verdict = %+v, %v; want refused", v, ok)
			}
		})
	}
}

// A review that found every criterion met lets auto-merge merge, as
// before, and records the verdict the gate reads.
func TestCriteriaReview_EveryCriterionMetSeedsTheMerge(t *testing.T) {
	t.Parallel()
	f, store := reviewExitFixture(t, `{"criteria": [{"criterion": "`+criterionHosts+`", "verdict": "met", "evidence": "tests/test_egress_hosts.py passes"}]}`)
	f.exit()

	if _, ok := f.state.PendingReactions[ReactionKey(hopExitIssueID, ReactionKindAutoMerge)]; !ok {
		t.Error("auto-merge not seeded after a review that found every criterion met")
	}
	if _, parked := f.state.Parked[hopExitIssueID]; parked {
		t.Error("story parked after a passing review")
	}
	if v, ok, _ := store.LoadCriteriaVerdict(context.Background(), hopExitIssueID); !ok || v.Outcome != CriteriaOutcomeMet || !strings.Contains(v.Criteria, "test_egress_hosts") {
		t.Errorf("recorded verdict = %+v, %v; want met with its criteria", v, ok)
	}
}

// A criterion only a live proof settles keeps the pull request open for
// the live-proof job, with no park, and says so on the story; the gate
// does not let auto-merge merge it.
func TestCriteriaReview_LiveProofHoldsTheMerge(t *testing.T) {
	t.Parallel()
	f, store := reviewExitFixture(t, `{"criteria": [
		{"criterion": "`+criterionChain+`", "verdict": "live-proof", "evidence": "the machine has no route out"},
		{"criterion": "`+criterionHosts+`", "verdict": "met", "evidence": "tests/test_egress_hosts.py passes"}]}`)
	f.exit()

	if _, parked := f.state.Parked[hopExitIssueID]; parked {
		t.Error("story parked, want its pull request left open for the live proof")
	}
	commented := false
	for _, c := range f.tracker.commentCalls {
		if strings.Contains(c.Text, "does not merge") && strings.Contains(c.Text, "live-proof") && strings.Contains(c.Text, "full vm chain") {
			commented = true
		}
	}
	if !commented {
		t.Errorf("comments = %+v, want the live-proof hold naming the criterion", f.tracker.commentCalls)
	}
	gate := planReviewGate(gateConfig(), &labelTracker{labels: []string{config.ReviewCriteriaLabel}}, store)
	if ok, why, err := gate(context.Background(), &PendingReaction{IssueID: hopExitIssueID, RuleName: "check"}); ok || err != nil {
		t.Errorf("gate = %v (%s), %v; want the merge refused", ok, why, err)
	}
}

// A reaction run on the review rule (a fix, a live proof, click-proofs)
// is not the review: it keeps the review's verdict and is not parked for
// leaving none.
func TestCriteriaReview_ReactionRunKeepsTheVerdict(t *testing.T) {
	t.Parallel()
	f, store := reviewExitFixture(t, "")
	f.state.Running[hopExitIssueID].ReactionKind = ReactionKindCI
	_ = store.UpsertCriteriaVerdict(context.Background(), persistence.CriteriaVerdict{IssueID: hopExitIssueID, Outcome: CriteriaOutcomeMet})
	f.exit()

	if _, parked := f.state.Parked[hopExitIssueID]; parked {
		t.Error("a fix run parked the story for writing no verdict")
	}
	if v, _, _ := store.LoadCriteriaVerdict(context.Background(), hopExitIssueID); v.Outcome != CriteriaOutcomeMet {
		t.Errorf("verdict after the fix run = %+v, want the review's met", v)
	}
}

// A review that stops blocked replaces an earlier met verdict, so a
// pull request a later review refused is not merged on the earlier one.
func TestCriteriaReview_BlockedReviewReplacesAnEarlierPass(t *testing.T) {
	t.Parallel()
	f, store := reviewExitFixture(t, "")
	_ = store.UpsertCriteriaVerdict(context.Background(), persistence.CriteriaVerdict{IssueID: hopExitIssueID, Outcome: CriteriaOutcomeMet})
	f.result.SoftStop, f.result.SoftStopReason = true, string(workspace.StatusBlocked)
	f.exit()

	if v, _, _ := store.LoadCriteriaVerdict(context.Background(), hopExitIssueID); v.Outcome != CriteriaOutcomeRefused {
		t.Errorf("verdict after a blocked review = %+v, want refused", v)
	}
}

// The gate merges only on a recorded verdict of every criterion met; a
// pull request auto-merge watches again after a restart is held to it
// too.
func TestPlanReviewGate_RequiresEveryCriterionMet(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		verdict *persistence.CriteriaVerdict
		want    bool
	}{
		{"no verdict recorded", nil, false},
		{"refused", &persistence.CriteriaVerdict{Outcome: CriteriaOutcomeRefused}, false},
		{"waits for a live proof", &persistence.CriteriaVerdict{Outcome: CriteriaOutcomeLiveProof}, false},
		{"every criterion met", &persistence.CriteriaVerdict{Outcome: CriteriaOutcomeMet}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := &verdictStore{}
			if tt.verdict != nil {
				v := *tt.verdict
				v.IssueID = "AM-V"
				_ = store.UpsertCriteriaVerdict(context.Background(), v)
			}
			state := stateWithAutoMergePending(t, "AM-V", 46)
			state.PendingReactions[ReactionKey("AM-V", ReactionKindAutoMerge)].RuleName = "check"
			merged := false
			scm := &controlledSCMAdapter{
				getMergeabilityFn: func(context.Context, int, string, string) (domain.PRMergeStatus, error) {
					return domain.PRMergeStatus{Mergeability: domain.MergeabilityClean, HeadSHA: "sha"}, nil
				},
				getReviewDecisionFn: func(context.Context, int, string, string) (domain.ReviewDecision, error) {
					return domain.ReviewDecisionNotRequired, nil
				},
				getCIStatusFn: func(context.Context, int, string, string) (string, error) { return "success", nil },
				mergePRFn: func(context.Context, int, string, string, domain.MergeStrategy, string, string, string) (domain.MergeResult, error) {
					merged = true
					return domain.MergeResult{SHA: "m"}, nil
				},
			}
			params := autoMergeParams(&reviewReconcileStore{}, scm, &reviewTrackerStub{})
			params.ReviewGate = planReviewGate(gateConfig(), &labelTracker{labels: []string{config.ReviewCriteriaLabel}}, store)
			reconcileAutoMerge(state, params, discardLogger(), context.Background(), newAutoMergeMetricsSpy())
			state.TrackerOpsWg.Wait()
			if merged != tt.want {
				t.Errorf("merged = %v, want %v", merged, tt.want)
			}
		})
	}

	// A failed verdict read keeps the entry for a retry.
	state := stateWithAutoMergePending(t, "AM-VERR", 47)
	state.PendingReactions[ReactionKey("AM-VERR", ReactionKindAutoMerge)].RuleName = "check"
	params := autoMergeParams(&reviewReconcileStore{}, &controlledSCMAdapter{}, &reviewTrackerStub{})
	params.ReviewGate = planReviewGate(gateConfig(), &labelTracker{labels: []string{config.ReviewCriteriaLabel}}, &verdictStore{err: errors.New("database locked")})
	reconcileAutoMerge(state, params, discardLogger(), context.Background(), newAutoMergeMetricsSpy())
	if _, ok := state.PendingReactions[ReactionKey("AM-VERR", ReactionKindAutoMerge)]; !ok {
		t.Error("entry dropped after a failed verdict read, want it kept")
	}
}

// A run clears a criteria file an earlier run left, so a review's verdict
// is never an earlier run's.
func TestClearCriteriaFile(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	if err := os.Mkdir(filepath.Join(ws, workspacekit.SortieDir), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := workspacekit.WriteSortieFile(ws, CriteriaFile, []byte(`{"criteria": []}`)); err != nil {
		t.Fatal(err)
	}
	if readCriteriaFile(ws, discardLogger()) == nil {
		t.Fatal("criteria file not read")
	}
	clearCriteriaFile(ws, discardLogger())
	if got := readCriteriaFile(ws, discardLogger()); got != nil {
		t.Errorf("criteria file after clearing = %q, want none", got)
	}
}
