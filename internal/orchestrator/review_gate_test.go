package orchestrator

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
)

// labelTracker answers FetchIssueByID with fixed labels.
type labelTracker struct {
	domain.TrackerAdapter
	labels []string
	err    error
}

func (l *labelTracker) FetchIssueByID(_ context.Context, id string) (domain.Issue, error) {
	if l.err != nil {
		return domain.Issue{}, l.err
	}
	return domain.Issue{ID: id, Labels: l.labels}, nil
}

func gateConfig() config.ServiceConfig {
	cfg := config.ServiceConfig{Tracker: config.TrackerConfig{ReleaseState: "Todo", BacklogState: "Backlog", CancelState: "Canceled", DoneState: "Done"}}
	cfg.Dispatch = config.DispatchConfig{Rules: []config.DispatchRule{
		{Name: "implement"},
		{Name: "check", Stage: "stage-check", Gate: config.GateCriteria, OnFail: config.OnFailEscalate},
	}}
	return cfg
}

func TestPlanReviewGate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		rule   string
		labels []string
		want   bool
	}{
		{"criteria story after the criteria check", "check", []string{"sortie-review:criteria"}, true},
		{"criteria story after the implementing run", "implement", []string{"sortie-review:criteria"}, false},
		{"human story after the criteria check", "check", []string{"sortie-review:human"}, true},
		{"human story after the implementing run", "implement", []string{"sortie-review:human"}, false},
		{"self-review is not the gate", "implement", []string{"sortie-review:criteria", "self-reviewed"}, false},
		{"ui story after the criteria check", "check", []string{"sortie-review:ui"}, true},
		{"ui story after the implementing run", "implement", []string{"sortie-review:ui"}, false},
		{"no gate label", "check", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gate := planReviewGate(gateConfig(), &labelTracker{labels: tt.labels}, allMet{})
			got, why, err := gate(context.Background(), &PendingReaction{IssueID: "i", RuleName: tt.rule})
			if err != nil || got != tt.want {
				t.Errorf("gate = %v (%s), %v, want %v", got, why, err, tt.want)
			}
		})
	}
	if planReviewGate(config.ServiceConfig{}, &labelTracker{}, nil) != nil {
		t.Error("gate set with plan control off, want nil")
	}
}

// Auto-merge leaves a human story alone when its chain ends in no criteria
// check, even when its pull request is clean, approved, and green.
func TestReconcileAutoMerge_ReviewGateKeepsHumanStories(t *testing.T) {
	t.Parallel()
	state := stateWithAutoMergePending(t, "AM-HUMAN", 42)
	state.PendingReactions[ReactionKey("AM-HUMAN", ReactionKindAutoMerge)].RuleName = "implement"
	merged := false
	scm := &controlledSCMAdapter{
		getMergeabilityFn: func(context.Context, int, string, string) (domain.PRMergeStatus, error) {
			return domain.PRMergeStatus{Mergeability: domain.MergeabilityClean, HeadSHA: "sha"}, nil
		},
		getReviewDecisionFn: func(context.Context, int, string, string) (domain.ReviewDecision, error) {
			return domain.ReviewDecisionApproved, nil
		},
		getCIStatusFn: func(context.Context, int, string, string) (string, error) { return "success", nil },
		mergePRFn: func(context.Context, int, string, string, domain.MergeStrategy, string, string, string) (domain.MergeResult, error) {
			merged = true
			return domain.MergeResult{SHA: "m"}, nil
		},
	}
	params := autoMergeParams(&reviewReconcileStore{}, scm, &reviewTrackerStub{})
	params.ReviewGate = planReviewGate(gateConfig(), &labelTracker{labels: []string{"sortie-review:human"}}, allMet{})

	reconcileAutoMerge(state, params, discardLogger(), context.Background(), newAutoMergeMetricsSpy())
	state.TrackerOpsWg.Wait()

	if merged {
		t.Error("auto-merge merged a human-review story")
	}
	if _, ok := state.PendingReactions[ReactionKey("AM-HUMAN", ReactionKindAutoMerge)]; ok {
		t.Error("entry kept, want it dropped: the human merges in the forge")
	}

	// A criteria story that passed its check is merged.
	state = stateWithAutoMergePending(t, "AM-CRIT", 43)
	state.PendingReactions[ReactionKey("AM-CRIT", ReactionKindAutoMerge)].RuleName = "check"
	params.ReviewGate = planReviewGate(gateConfig(), &labelTracker{labels: []string{"sortie-review:criteria"}}, allMet{})
	reconcileAutoMerge(state, params, discardLogger(), context.Background(), newAutoMergeMetricsSpy())
	state.TrackerOpsWg.Wait()
	if !merged {
		t.Error("auto-merge did not merge a criteria story after its check")
	}
}

// A failed label read keeps the entry for a retry.
func TestReconcileAutoMerge_ReviewGateReadFailureRetries(t *testing.T) {
	t.Parallel()
	state := stateWithAutoMergePending(t, "AM-ERR", 44)
	state.PendingReactions[ReactionKey("AM-ERR", ReactionKindAutoMerge)].RuleName = "check"
	params := autoMergeParams(&reviewReconcileStore{}, &controlledSCMAdapter{}, &reviewTrackerStub{})
	params.ReviewGate = planReviewGate(gateConfig(), &labelTracker{err: errors.New("huly down")}, nil)
	reconcileAutoMerge(state, params, discardLogger(), context.Background(), newAutoMergeMetricsSpy())
	if _, ok := state.PendingReactions[ReactionKey("AM-ERR", ReactionKindAutoMerge)]; !ok {
		t.Error("entry dropped after a failed label read, want it kept")
	}
}

// criteriaExitFixture is a hop-exit fixture whose run is a criteria check
// under plan control that ended asking for human review, with a pull
// request and auto-merge configured.
func criteriaExitFixture(t *testing.T, onFail string) *hopExitFixture {
	t.Helper()
	f := newHopExitFixture(t)
	f.params.PlanReleaseState = "In Progress"
	f.params.Dispatch = config.DispatchConfig{Rules: []config.DispatchRule{
		{Name: "specify", Stage: hopExitSourceLabel, Gate: config.GateCriteria, OnFail: onFail},
		{Name: "rework", Stage: "stage-rework"},
	}, MaxConsecutiveHops: 3}
	f.state.Running[hopExitIssueID].LastAgentMessage = "criterion 2 (tax lines) is not met"
	ws := t.TempDir()
	writePRSCMMetadata(t, ws, 42, "acme", "myrepo", "feature/HOP-1", "deadbeef")
	f.result.WorkspacePath = ws
	f.result.SoftStop = true
	f.result.SoftStopReason = "needs-human-review"
	f.params.SCMAdapter = &scmAdapterStubExit{}
	f.params.AutoMergeReactionConfigured = true
	return f
}

func TestCriteriaFailure_NoMergeAndAComment(t *testing.T) {
	t.Parallel()
	f := criteriaExitFixture(t, config.OnFailEscalate)
	f.exit()

	if _, ok := f.state.PendingReactions[ReactionKey(hopExitIssueID, ReactionKindAutoMerge)]; ok {
		t.Error("auto-merge seeded after a failed criteria check")
	}
	found := false
	for _, c := range f.tracker.commentCalls {
		if strings.Contains(c.Text, "criterion 2") && strings.Contains(c.Text, "not merged") {
			found = true
		}
	}
	if !found {
		t.Errorf("comments = %+v, want one naming criterion 2", f.tracker.commentCalls)
	}
}

func TestCriteriaFailure_OnFailSendsTheStoryBack(t *testing.T) {
	t.Parallel()
	f := criteriaExitFixture(t, "rework")
	f.exit()

	if !slices.Contains(f.tracker.added, "stage-rework") {
		t.Errorf("labels added = %v, want stage-rework", f.tracker.added)
	}
	last := f.tracker.transitionCalls[len(f.tracker.transitionCalls)-1]
	if last.TargetState != "In Progress" {
		t.Errorf("last transition = %+v, want back to the release state", last)
	}
	if _, ok := f.state.PendingReactions[ReactionKey(hopExitIssueID, ReactionKindAutoMerge)]; ok {
		t.Error("auto-merge seeded after a failed criteria check")
	}
}

// Without plan control, or for a rule without the gate, nothing changes.
func TestCriteriaCheckFailed_OnlyForGateRulesUnderPlanControl(t *testing.T) {
	t.Parallel()
	f := criteriaExitFixture(t, config.OnFailEscalate)
	entry := f.state.Running[hopExitIssueID]
	if !criteriaCheckFailed(f.params, entry, f.result) {
		t.Fatal("criteria check failure not detected")
	}
	off := f.params
	off.PlanReleaseState = ""
	if criteriaCheckFailed(off, entry, f.result) {
		t.Error("detected with plan control off")
	}
	passed := f.result
	passed.SoftStop = false
	if criteriaCheckFailed(f.params, entry, passed) {
		t.Error("detected for a passing check")
	}
}

// A criteria story stuck on a review the forge requires is warned about
// once, and is not merged.
func TestReconcileAutoMerge_WarnsOnceWhenTheForgeRequiresReview(t *testing.T) {
	t.Parallel()
	state := stateWithAutoMergePending(t, "AM-REQ", 45)
	state.PendingReactions[ReactionKey("AM-REQ", ReactionKindAutoMerge)].RuleName = "check"
	scm := &controlledSCMAdapter{
		getMergeabilityFn: func(context.Context, int, string, string) (domain.PRMergeStatus, error) {
			return domain.PRMergeStatus{Mergeability: domain.MergeabilityClean, HeadSHA: "sha"}, nil
		},
		getReviewDecisionFn: func(context.Context, int, string, string) (domain.ReviewDecision, error) {
			return domain.ReviewDecisionReviewRequired, nil
		},
	}
	params := autoMergeParams(&reviewReconcileStore{}, scm, &reviewTrackerStub{})
	params.ReviewGate = planReviewGate(gateConfig(), &labelTracker{labels: []string{"sortie-review:criteria"}}, allMet{})
	reconcileAutoMerge(state, params, discardLogger(), context.Background(), newAutoMergeMetricsSpy())
	if _, ok := state.CriteriaReviewWarned["AM-REQ"]; !ok {
		t.Error("no warning recorded for a criteria story that needs a forge review")
	}
	if _, ok := state.PendingReactions[ReactionKey("AM-REQ", ReactionKindAutoMerge)]; !ok {
		t.Error("entry dropped, want it kept waiting")
	}
}
