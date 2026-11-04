package orchestrator

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
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

// uiMergeTracker is a fake tracker holding a ui story that records the
// comments written on it.
type uiMergeTracker struct {
	labelTracker
	mu       sync.Mutex
	comments []string
}

func (u *uiMergeTracker) CommentIssue(_ context.Context, _ string, body string) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.comments = append(u.comments, body)
	return nil
}

func (u *uiMergeTracker) written() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return slices.Clone(u.comments)
}

const uiHeadSHA = "0123456789abcdef0123456789abcdef01234567"

// uiMergeForge is a fake forge whose pull request is clean, approved and
// green at uiHeadSHA, and counts its merges.
func uiMergeForge(merges *int) *controlledSCMAdapter {
	return &controlledSCMAdapter{
		getMergeabilityFn: func(context.Context, int, string, string) (domain.PRMergeStatus, error) {
			return domain.PRMergeStatus{Mergeability: domain.MergeabilityClean, HeadSHA: uiHeadSHA}, nil
		},
		getReviewDecisionFn: func(context.Context, int, string, string) (domain.ReviewDecision, error) {
			return domain.ReviewDecisionNotRequired, nil
		},
		getCIStatusFn: func(context.Context, int, string, string) (string, error) { return "success", nil },
		mergePRFn: func(_ context.Context, _ int, _, _ string, _ domain.MergeStrategy, _, _, expected string) (domain.MergeResult, error) {
			if expected != uiHeadSHA {
				return domain.MergeResult{}, errors.New("unexpected head")
			}
			*merges++
			return domain.MergeResult{SHA: "m"}, nil
		},
	}
}

// uiMergeRun runs one auto-merge pass over a ui story's entry, after a run
// of rule, with the given conversation on its pull request.
func uiMergeRun(t *testing.T, state *State, rule string, conversation []domain.ReviewComment, tracker *uiMergeTracker, now time.Time) (merges int) {
	t.Helper()
	state.PendingReactions[ReactionKey("AM-UI", ReactionKindAutoMerge)].RuleName = rule
	params := autoMergeParams(&reviewReconcileStore{}, uiMergeForge(&merges), tracker)
	params.ReviewGate = planReviewGate(gateConfig(), tracker, allMet{})
	params.ClickProofs = planClickProofs(gateConfig(), &commentForge{conversation: conversation})
	params.AutoMergePendingTTL = time.Minute
	params.NowFunc = func() time.Time { return now }
	reconcileAutoMerge(state, params, discardLogger(), context.Background(), newAutoMergeMetricsSpy())
	state.TrackerOpsWg.Wait()
	return merges
}

func clickProofsComment(sha string) domain.ReviewComment {
	return domain.ReviewComment{
		ID: "issuecomment-7", Reviewer: "reviewer",
		Body: "Click-proofs on https://pr-45.preview.example: all pass.\n\n`" + ClickProofsMarker + " " + sha + "`",
	}
}

// Scenarios "A built UI story" and "A UI story from spec to merge": once
// its criteria review and CI passed and its pull request records the
// click-proofs on the preview, Sortie merges a ui story, and nobody is
// asked to approve it.
func TestUIStory_MergesAfterReviewCIAndClickProofs(t *testing.T) {
	t.Parallel()
	state := stateWithAutoMergePending(t, "AM-UI", 45)
	tracker := &uiMergeTracker{labelTracker: labelTracker{labels: []string{config.ReviewUILabel}}}

	merges := uiMergeRun(t, state, "check", []domain.ReviewComment{clickProofsComment(uiHeadSHA[:12])}, tracker, autoMergeBaseTime)

	if merges != 1 {
		t.Fatalf("merges = %d, want Sortie to merge the ui story", merges)
	}
	if _, ok := state.PendingReactions[ReactionKey("AM-UI", ReactionKindAutoMerge)]; ok {
		t.Error("auto-merge entry kept after the merge")
	}
	if got := tracker.written(); len(got) != 0 {
		t.Errorf("tracker comments = %q, want none asking anyone", got)
	}
}

// Without a record of the click-proofs Sortie does not merge a ui story:
// it asks for them once per head commit and keeps waiting, past the watch
// window, until a record names the head commit.
func TestUIStory_NoMergeWithoutClickProofs(t *testing.T) {
	t.Parallel()
	state := stateWithAutoMergePending(t, "AM-UI", 45)
	key := ReactionKey("AM-UI", ReactionKindAutoMerge)
	tracker := &uiMergeTracker{labelTracker: labelTracker{labels: []string{config.ReviewUILabel}}}
	operator := domain.ReviewComment{ID: "issuecomment-1", Reviewer: "ana", Body: "Looks fine to me, approve."}
	stale := clickProofsComment("fedcba9876543210")

	if merges := uiMergeRun(t, state, "check", []domain.ReviewComment{operator, stale}, tracker, autoMergeBaseTime); merges != 0 {
		t.Fatalf("merges = %d without a click-proofs record for the head", merges)
	}
	if _, ok := state.PendingReactions[key]; !ok {
		t.Fatal("auto-merge entry dropped, want it waiting for the click-proofs")
	}
	got := tracker.written()
	if len(got) != 1 || !strings.Contains(got[0], "click-proofs on its preview") || !strings.Contains(got[0], ClickProofsMarker+" "+uiHeadSHA) {
		t.Fatalf("tracker comments = %q, want one asking for the click-proofs on the head", got)
	}

	// Long past the watch window, it still waits and does not ask again.
	later := autoMergeBaseTime.Add(48 * time.Hour)
	state.PendingReactions[key].PendingRetryAt = time.Time{}
	if merges := uiMergeRun(t, state, "check", nil, tracker, later); merges != 0 {
		t.Fatalf("merges = %d without a click-proofs record", merges)
	}
	if _, ok := state.PendingReactions[key]; !ok {
		t.Fatal("auto-merge entry aged out, want it held for the click-proofs")
	}
	if got := tracker.written(); len(got) != 1 {
		t.Errorf("tracker comments = %d, want the request made once per head", len(got))
	}

	state.PendingReactions[key].PendingRetryAt = time.Time{}
	if merges := uiMergeRun(t, state, "check", []domain.ReviewComment{clickProofsComment(uiHeadSHA)}, tracker, later.Add(time.Hour)); merges != 1 {
		t.Errorf("merges = %d once the record names the head, want 1", merges)
	}
}

// A ui story whose pull request was opened by a run that was not its
// criteria review is not merged, records or not.
func TestUIStory_NoMergeWithoutCriteriaReview(t *testing.T) {
	t.Parallel()
	state := stateWithAutoMergePending(t, "AM-UI", 45)
	tracker := &uiMergeTracker{labelTracker: labelTracker{labels: []string{config.ReviewUILabel}}}
	if merges := uiMergeRun(t, state, "implement", []domain.ReviewComment{clickProofsComment(uiHeadSHA)}, tracker, autoMergeBaseTime); merges != 0 {
		t.Errorf("merges = %d after a run that was not the criteria review", merges)
	}
}

// A story that is not a ui story needs no click-proofs.
func TestPlanClickProofs_OnlyUIStories(t *testing.T) {
	t.Parallel()
	forge := &commentForge{}
	check := planClickProofs(gateConfig(), forge)
	ok, _, err := check(context.Background(), &PendingReaction{}, &AutoMergeReactionData{story: storyOther}, uiHeadSHA)
	if !ok || err != nil || forge.commentCalls != 0 {
		t.Errorf("other story = %v, %v after %d reads, want allowed without a read", ok, err, forge.commentCalls)
	}
	ok, _, _ = planClickProofs(gateConfig(), nil)(context.Background(), &PendingReaction{}, &AutoMergeReactionData{story: storyUI}, uiHeadSHA)
	if ok {
		t.Error("ui story allowed on a forge that cannot read its record")
	}
	forge.commentErr = &domain.SCMError{Kind: domain.ErrSCMTransport, Message: "down"}
	if _, _, err := check(context.Background(), &PendingReaction{}, &AutoMergeReactionData{story: storyUI}, uiHeadSHA); err == nil {
		t.Error("failed read reported no error")
	}
	if planClickProofs(config.ServiceConfig{}, forge) != nil {
		t.Error("click-proofs check set with plan control off")
	}
}

func TestClickProofsRecord(t *testing.T) {
	t.Parallel()
	tests := []struct {
		body string
		sha  string
		ok   bool
	}{
		{"sortie:click-proofs passed abcdef1", "abcdef1", true},
		{"Done.\n> `Sortie:Click-Proofs PASSED ABCDEF12`\n", "abcdef12", true},
		{"sortie:click-proofs passed abc", "", false},
		{"sortie:click-proofs failed abcdef1", "", false},
		{"sortie:click-proofs passed not-a-sha", "", false},
		{"the click-proofs passed", "", false},
	}
	for _, tt := range tests {
		sha, ok := clickProofsRecord(tt.body)
		if sha != tt.sha || ok != tt.ok {
			t.Errorf("clickProofsRecord(%q) = %q, %v, want %q, %v", tt.body, sha, ok, tt.sha, tt.ok)
		}
	}
}

// A click-proofs record on a ui story's pull request is not review
// feedback: it starts no fix run.
func TestUIStory_ClickProofsRecordIsNoFeedback(t *testing.T) {
	t.Parallel()
	state := stateWithReviewReaction(t, "UI-6", 27)
	record := clickProofsComment(uiHeadSHA)
	record.SubmittedAt = reviewBaseTime.Add(-5 * time.Minute)
	forge := &commentForge{conversation: []domain.ReviewComment{record}}
	forge.decision = domain.ReviewDecisionNotRequired
	reconcileReviewComments(state, uiReviewParams(forge, &labelTracker{labels: []string{config.ReviewUILabel}}), discardLogger(), context.Background(), newReviewMetricsSpy())
	if _, ok := state.RetryAttempts["UI-6"]; ok {
		t.Error("fix run scheduled for a click-proofs record")
	}
}

// A comment on the pull request of a ui story starts a fix run on the
// branch that carries it.
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

// A ui story with no comment keeps its review watch past the watch
// window, and the watch ends when its pull request merges.
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

// The merge of its pull request moves the ui story to Done.
func TestUIStory_MergeIsDone(t *testing.T) {
	t.Parallel()
	state := mgcStateWithPending("UI-5", 26)
	tracker := &mgcTrackerFake{states: map[string]string{"UI-5": "In Review"}}
	scm := &mgcSCMFake{fn: func(int, string, string) (domain.PRMergeStatus, error) { return mergedStatus("sha-ui"), nil }}
	params := mgcParams(newMGCStore(), scm, tracker)
	params.ReviewGate = planReviewGate(gateConfig(), &labelTracker{labels: []string{config.ReviewUILabel}}, allMet{})

	reconcileMergeCompletion(state, params, discardLogger(), context.Background(), &domain.NoopMetrics{})

	if len(tracker.transitionCalls) != 1 || tracker.transitionCalls[0].target != "done" {
		t.Errorf("transitions = %+v, want the story moved to done", tracker.transitionCalls)
	}
}

// The auto-merge watch of a ui story, held past its window, ends when its
// pull request closes unmerged.
func TestUIStory_ClosedPullRequestEndsTheWatch(t *testing.T) {
	t.Parallel()
	state := stateWithAutoMergePending(t, "AM-UI", 45)
	state.PendingReactions[ReactionKey("AM-UI", ReactionKindAutoMerge)].RuleName = "check"
	tracker := &uiMergeTracker{labelTracker: labelTracker{labels: []string{config.ReviewUILabel}}}
	merges := 0
	forge := uiMergeForge(&merges)
	forge.getMergeabilityFn = func(context.Context, int, string, string) (domain.PRMergeStatus, error) {
		return domain.PRMergeStatus{Closed: true, Mergeability: domain.MergeabilityUnknown, HeadSHA: uiHeadSHA}, nil
	}
	params := autoMergeParams(&reviewReconcileStore{}, forge, tracker)
	params.ReviewGate = planReviewGate(gateConfig(), tracker, allMet{})
	params.ClickProofs = planClickProofs(gateConfig(), &commentForge{})
	reconcileAutoMerge(state, params, discardLogger(), context.Background(), newAutoMergeMetricsSpy())
	state.TrackerOpsWg.Wait()
	if _, ok := state.PendingReactions[ReactionKey("AM-UI", ReactionKindAutoMerge)]; ok || merges != 0 {
		t.Errorf("entry kept %v, merges %d, want the watch ended without a merge", ok, merges)
	}
}
