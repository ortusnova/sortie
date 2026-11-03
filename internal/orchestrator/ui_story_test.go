package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
)

// commentForge is a fake forge that also reads the conversation comments
// of a pull request.
type commentForge struct {
	mockSCMAdapter
	conversation []domain.ReviewComment
	commentErr   error
	commentCalls int
}

var _ domain.PullRequestCommentReader = (*commentForge)(nil)

func (f *commentForge) FetchPullRequestComments(context.Context, int, string, string) ([]domain.ReviewComment, error) {
	f.commentCalls++
	if f.commentErr != nil {
		return nil, f.commentErr
	}
	return f.conversation, nil
}

// countingLabelTracker is a labelTracker that counts its reads.
type countingLabelTracker struct {
	labelTracker
	reads int
}

func (c *countingLabelTracker) FetchIssueByID(ctx context.Context, id string) (domain.Issue, error) {
	c.reads++
	return c.labelTracker.FetchIssueByID(ctx, id)
}

func uiReviewParams(forge *commentForge, tracker domain.TrackerAdapter) ReconcileParams {
	params := reviewParams(&reviewReconcileStore{}, forge, tracker)
	params.OperatorComments = planOperatorComments(gateConfig(), tracker, forge)
	return params
}

// Scenario "A UI story from spec to merge": the criteria check passed and
// the pull request is clean, approved, and green, yet Sortie does not
// merge a ui story.
func TestUIStory_ReviewGateNeverMerges(t *testing.T) {
	t.Parallel()
	gate := planReviewGate(gateConfig(), &labelTracker{labels: []string{config.ReviewUILabel}})
	for _, rule := range []string{"check", "implement"} {
		if ok, why, err := gate(context.Background(), &PendingReaction{IssueID: "i", RuleName: rule}); ok || err != nil {
			t.Errorf("gate after %s = %v (%s), %v, want refused", rule, ok, why, err)
		}
	}

	state := stateWithAutoMergePending(t, "AM-UI", 45)
	state.PendingReactions[ReactionKey("AM-UI", ReactionKindAutoMerge)].RuleName = "check"
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
	params.ReviewGate = gate
	reconcileAutoMerge(state, params, discardLogger(), context.Background(), newAutoMergeMetricsSpy())
	state.TrackerOpsWg.Wait()
	if merged {
		t.Error("auto-merge merged a ui story")
	}
	if _, ok := state.PendingReactions[ReactionKey("AM-UI", ReactionKindAutoMerge)]; ok {
		t.Error("auto-merge entry kept, want it dropped: the operator merges")
	}
}

// Scenario "The operator asks for a change": a comment on the pull
// request of a ui story starts a fix run on the branch that carries it.
func TestUIStory_OperatorCommentStartsFixRun(t *testing.T) {
	t.Parallel()
	state := stateWithReviewReaction(t, "UI-1", 21)
	rkey := ReactionKey("UI-1", ReactionKindReview)
	forge := &commentForge{conversation: []domain.ReviewComment{{
		ID: "issuecomment-9", Reviewer: "ana", Body: "The worker column is missing.",
		SubmittedAt: reviewBaseTime.Add(-5 * time.Minute),
	}}}
	tracker := &countingLabelTracker{labelTracker: labelTracker{labels: []string{config.ReviewUILabel}}}
	metrics := newReviewMetricsSpy()

	reconcileReviewComments(state, uiReviewParams(forge, tracker), discardLogger(), context.Background(), metrics)

	retry, ok := state.RetryAttempts["UI-1"]
	if !ok {
		t.Fatal("no fix run scheduled for the operator's comment")
	}
	if retry.ReactionKind != ReactionKindReview {
		t.Errorf("run kind = %q, want a review fix run", retry.ReactionKind)
	}
	got, _ := retry.ContinuationContext[reviewCommentsKey].([]map[string]any)
	if len(got) != 1 || got[0]["body"] != "The worker column is missing." {
		t.Errorf("review_comments = %#v, want the operator's comment", retry.ContinuationContext[reviewCommentsKey])
	}
	if _, kept := state.PendingReactions[rkey]; kept {
		t.Error("review entry kept after dispatch, want it consumed")
	}
	if metrics.reviewChecks["dispatched"] != 1 {
		t.Errorf("dispatched = %d, want 1", metrics.reviewChecks["dispatched"])
	}
	if tracker.reads != 1 {
		t.Errorf("tracker reads = %d, want 1", tracker.reads)
	}
}

// A story that is not a ui story ignores conversation comments: only a
// changes-requested review starts its fix run.
func TestUIStory_OtherStoriesIgnoreConversationComments(t *testing.T) {
	t.Parallel()
	state := stateWithReviewReaction(t, "HU-1", 22)
	forge := &commentForge{conversation: []domain.ReviewComment{{ID: "issuecomment-3", Body: "nice", SubmittedAt: reviewBaseTime.Add(-time.Hour)}}}
	tracker := &countingLabelTracker{labelTracker: labelTracker{labels: []string{config.ReviewHumanLabel}}}
	params := uiReviewParams(forge, tracker)

	reconcileReviewComments(state, params, discardLogger(), context.Background(), newReviewMetricsSpy())
	reconcileReviewComments(state, params, discardLogger(), context.Background(), newReviewMetricsSpy())

	if _, ok := state.RetryAttempts["HU-1"]; ok {
		t.Error("fix run scheduled for a conversation comment on a human story")
	}
	if forge.commentCalls != 0 {
		t.Errorf("conversation reads = %d, want 0", forge.commentCalls)
	}
	if tracker.reads != 1 {
		t.Errorf("tracker reads = %d, want 1: the story is read once per entry", tracker.reads)
	}
	if planOperatorComments(config.ServiceConfig{}, tracker, forge) != nil {
		t.Error("operator comments read with plan control off")
	}
}

// A ui story with no comment waits for the operator past the watch
// window, and its watch ends when the operator merges.
func TestUIStory_WaitsForTheOperatorUntilMerge(t *testing.T) {
	t.Parallel()
	state := stateWithReviewReaction(t, "UI-2", 23)
	rkey := ReactionKey("UI-2", ReactionKindReview)
	forge := &commentForge{conversation: []domain.ReviewComment{}}
	forge.decision = domain.ReviewDecisionNotRequired
	params := uiReviewParams(forge, &labelTracker{labels: []string{config.ReviewUILabel}})
	params.ReviewPendingTTL = time.Minute

	reconcileReviewComments(state, params, discardLogger(), context.Background(), newReviewMetricsSpy())
	pending, ok := state.PendingReactions[rkey]
	if !ok {
		t.Fatal("watch dropped, want it kept for the operator")
	}
	if !awaits(pending.LastCheck) {
		t.Error("check does not read awaiting, want the story waiting for the operator")
	}

	// Long past the window, the watch still stands.
	later := reviewBaseTime.Add(24 * time.Hour)
	params.NowFunc = func() time.Time { return later }
	reconcileReviewComments(state, params, discardLogger(), context.Background(), newReviewMetricsSpy())
	if _, ok := state.PendingReactions[rkey]; !ok {
		t.Fatal("watch dropped by the window, want it kept for the operator")
	}

	forge.merged = true
	params.NowFunc = func() time.Time { return later.Add(2 * time.Hour) }
	reconcileReviewComments(state, params, discardLogger(), context.Background(), newReviewMetricsSpy())
	if _, ok := state.PendingReactions[rkey]; ok {
		t.Error("watch kept after the merge, want it ended")
	}
	if _, ok := state.RetryAttempts["UI-2"]; ok {
		t.Error("fix run scheduled with no comment")
	}
}

// A failed conversation read backs off like a failed review read.
func TestUIStory_CommentReadFailureBacksOff(t *testing.T) {
	t.Parallel()
	state := stateWithReviewReaction(t, "UI-3", 24)
	forge := &commentForge{commentErr: &domain.SCMError{Kind: domain.ErrSCMTransport, Message: "down"}}
	metrics := newReviewMetricsSpy()
	reconcileReviewComments(state, uiReviewParams(forge, &labelTracker{labels: []string{config.ReviewUILabel}}), discardLogger(), context.Background(), metrics)
	pending, ok := state.PendingReactions[ReactionKey("UI-3", ReactionKindReview)]
	if !ok || pending.PendingAttempts != 1 || metrics.reviewChecks["error"] != 1 {
		t.Errorf("entry %+v, errors %d, want it kept with one failed attempt", pending, metrics.reviewChecks["error"])
	}

	state = stateWithReviewReaction(t, "UI-4", 25)
	reconcileReviewComments(state, uiReviewParams(&commentForge{}, &labelTracker{err: errors.New("huly down")}), discardLogger(), context.Background(), metrics)
	if _, ok := state.PendingReactions[ReactionKey("UI-4", ReactionKindReview)]; !ok {
		t.Error("entry dropped after a failed label read, want it kept")
	}
}

// Scenario "The operator is satisfied": the operator's merge moves the ui
// story to Done.
func TestUIStory_OperatorMergeIsDone(t *testing.T) {
	t.Parallel()
	state := mgcStateWithPending("UI-5", 26)
	tracker := &mgcTrackerFake{states: map[string]string{"UI-5": "In Review"}}
	scm := &mgcSCMFake{fn: func(int, string, string) (domain.PRMergeStatus, error) { return mergedStatus("sha-ui"), nil }}
	params := mgcParams(newMGCStore(), scm, tracker)
	params.ReviewGate = planReviewGate(gateConfig(), &labelTracker{labels: []string{config.ReviewUILabel}})

	reconcileMergeCompletion(state, params, discardLogger(), context.Background(), &domain.NoopMetrics{})

	if len(tracker.transitionCalls) != 1 || tracker.transitionCalls[0].target != "done" {
		t.Errorf("transitions = %+v, want the story moved to done", tracker.transitionCalls)
	}
}
