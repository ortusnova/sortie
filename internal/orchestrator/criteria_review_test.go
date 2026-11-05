package orchestrator

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
)

// staleCriteria is a review recorded before the after-deploy reason
// existed: its live criterion carries no reason.
const staleCriteria = `[{"criterion":"the dashboard shows the queue","verdict":"met","evidence":"web test"},` +
	`{"criterion":"p1 serves the new route","verdict":"live-proof","evidence":"needs the deployed host"}]`

// MarkPullRequestReadyForReview marks pull request n ready, as GitHub's
// markPullRequestReadyForReview does.
func (f *proveForge) MarkPullRequestReadyForReview(_ context.Context, n int, _, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := f.pulls[n]
	p.Draft, p.Mergeability = false, domain.MergeabilityClean
	f.readied = append(f.readied, n)
	return nil
}

func (f *proveForge) readiedPulls() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.readied)
}

func (f *proveForge) setHead(n int, head string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pulls[n].HeadSHA = head
}

func (f *proveForge) draft(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pulls[n].Draft, f.pulls[n].Mergeability = true, domain.MergeabilityBlocked
}

func (f *proveFixture) requestedHead(t *testing.T, id string) string {
	t.Helper()
	head, _, err := f.store.GetReactionFingerprint(t.Context(), id, criteriaReviewKind)
	if err != nil {
		t.Fatal(err)
	}
	return head
}

// assertReviewDispatches checks that ONS-4 waits in the release state
// with labels that dispatch the criteria rule on the next poll.
func (f *proveFixture) assertReviewDispatches(t *testing.T) {
	const id = "id-4"
	t.Helper()
	issue := f.stories.issue(id)
	cfg := f.o.workflowManager.Config()
	if issue.State != cfg.Tracker.ReleaseState {
		t.Errorf("%s state = %q, want %q for its review", id, issue.State, cfg.Tracker.ReleaseState)
	}
	if got := ResolveRule(issue, cfg.Dispatch, HopRoute{}, cfg.Agent.Kind, "").RuleName; got != "check" {
		t.Errorf("%s with labels %v dispatches rule %q, want the criteria rule check", id, issue.Labels, got)
	}
}

// assertNoPersonAsked checks that nothing parked a story, raised a
// question, or notified anyone.
func (f *proveFixture) assertNoPersonAsked(t *testing.T) {
	t.Helper()
	var parked, faults int
	onLoop(t, f.o, func() { parked, faults = len(f.o.state.Parked), len(f.o.state.PlatformFaults) })
	if parked != 0 || faults != 0 {
		t.Errorf("parked = %d, open questions = %d, want none", parked, faults)
	}
	if sent := f.spy.notifications(); len(sent) != 0 {
		t.Errorf("notifications = %+v, want none", sent)
	}
}

// Scenario "A review from before after-deploy existed": merge-and-prove of
// a story whose review left a live criterion with no reason answers
// live_proof_not_after_deploy with a review requested, and the story goes
// back for a fresh review of its head with no person asked. A second
// request on the same head requests none; a new head requests one.
func TestMergeAndProve_StaleReviewRequestsAFreshOne(t *testing.T) {
	t.Parallel()
	f := newProveFixture(t)
	f.addStory(t, 4, staleCriteria, CriteriaOutcomeLiveProof)
	sup := f.o.Supervisor()

	got := sup.MergeAndProve(t.Context(), "ONS-4")
	if got.Status != SupervisorPrecondition || got.Code != "live_proof_not_after_deploy" || got.ReviewRequested == nil || !*got.ReviewRequested {
		t.Fatalf("merge-and-prove of ONS-4 = %+v, want live_proof_not_after_deploy with a review requested", got)
	}
	if !strings.Contains(got.Message, "requested a fresh criteria review of its head "+headSHA(4)) || got.PullRequest != "o/kit#4" {
		t.Errorf("refusal = %+v, want it to name the review of the head and the pull request", got)
	}
	f.assertReviewDispatches(t)
	if head := f.requestedHead(t, "id-4"); head != headSHA(4) {
		t.Errorf("requested head = %q, want %q", head, headSHA(4))
	}
	if c := f.stories.lastComment("id-4"); !strings.Contains(c, "fresh criteria review of o/kit#4 at its head "+headSHA(4)) {
		t.Errorf("story comment = %q, want the review request", c)
	}

	again := sup.MergeAndProve(t.Context(), "ONS-4")
	if again.Code != "live_proof_not_after_deploy" || again.ReviewRequested == nil || *again.ReviewRequested ||
		!strings.Contains(again.Message, "already requested") {
		t.Errorf("second merge-and-prove on the same head = %+v, want the refusal with no review requested", again)
	}
	if moves := len(f.stories.moves); moves != 1 {
		t.Errorf("moves = %d, want the one move of the first request", moves)
	}

	// The review ran at that head, and a fix then pushed a new head.
	f.stories.setState("id-4", "In Review")
	f.forge.setHead(4, headSHA(44))
	fresh := sup.MergeAndProve(t.Context(), "ONS-4")
	if fresh.ReviewRequested == nil || !*fresh.ReviewRequested {
		t.Errorf("merge-and-prove on a new head = %+v, want a review requested", fresh)
	}
	f.assertReviewDispatches(t)
	if head := f.requestedHead(t, "id-4"); head != headSHA(44) {
		t.Errorf("requested head = %q, want the new head", head)
	}
	f.assertNoPersonAsked(t)
	if snap := f.snapshot(t); len(snap.Queue) != 0 || snap.InFlight != nil {
		t.Errorf("merge-and-prove = %+v, want nothing queued", snap)
	}
}

// The supervisor's review route requests a fresh review of the story's
// current head, answers review_running while one runs or waits for its
// dispatch, and requests another once it ended.
func TestRequestReview(t *testing.T) {
	t.Parallel()
	f := newProveFixture(t)
	f.addStory(t, 4, staleCriteria, CriteriaOutcomeLiveProof)
	sup := f.o.Supervisor()

	got := sup.RequestReview(t.Context(), "ONS-4")
	if got.Status != SupervisorDone || got.PullRequest != "o/kit#4" || got.Identifier != "ONS-4" {
		t.Fatalf("review of ONS-4 = %+v, want it requested on o/kit#4", got)
	}
	f.assertReviewDispatches(t)
	if head := f.requestedHead(t, "id-4"); head != headSHA(4) {
		t.Errorf("requested head = %q, want %q", head, headSHA(4))
	}

	if got := sup.RequestReview(t.Context(), "ONS-4"); got.Code != "review_running" {
		t.Errorf("review while the first waits for its dispatch = %+v, want review_running", got)
	}

	onLoop(t, f.o, func() {
		f.o.state.Running["id-4"] = &RunningEntry{Identifier: "ONS-4", RuleName: "check"}
	})
	if got := sup.RequestReview(t.Context(), "ONS-4"); got.Code != "review_running" {
		t.Errorf("review while one runs = %+v, want review_running", got)
	}
	onLoop(t, f.o, func() {
		f.o.state.Running["id-4"] = &RunningEntry{Identifier: "ONS-4", RuleName: "check", ReactionKind: ReactionKindReview}
	})
	if got := sup.RequestReview(t.Context(), "ONS-4"); got.Code != "running" {
		t.Errorf("review while a fix run runs = %+v, want running", got)
	}

	// The review ended and handed the story off: the supervisor may ask
	// for another at the same head.
	onLoop(t, f.o, func() { delete(f.o.state.Running, "id-4") })
	f.stories.setState("id-4", "In Review")
	if got := sup.RequestReview(t.Context(), "ONS-4"); got.Status != SupervisorDone {
		t.Errorf("review after the last one ended = %+v, want it requested", got)
	}
	f.assertReviewDispatches(t)

	f.forge.mu.Lock()
	f.forge.pulls[4].Merged = true
	f.forge.mu.Unlock()
	if got := sup.RequestReview(t.Context(), "ONS-4"); got.Code != "pull_request_closed" {
		t.Errorf("review of a merged pull request = %+v, want pull_request_closed", got)
	}
	if got := sup.RequestReview(t.Context(), "ONS-404"); got.Status != SupervisorUnknownIssue {
		t.Errorf("review of an unknown story = %+v, want unknown", got)
	}
	f.assertNoPersonAsked(t)
}

// Scenario "A draft": a story merge-and-prove accepted with a draft pull
// request is marked ready for review, then merged.
func TestMergeAndProve_MarksADraftReadyAndMergesIt(t *testing.T) {
	t.Parallel()
	f := newProveFixture(t)
	f.o.prReadier = f.forge
	f.forge.draft(1)
	f.accept(t, "ONS-1")

	f.tick(t, f.t0)
	if got := f.forge.readiedPulls(); !slices.Equal(got, []int{1}) {
		t.Fatalf("readied = %v, want o/kit#1 marked ready for review", got)
	}
	if got := f.snapshot(t).InFlight; got == nil || got.Phase != MergeAndProveMerging || !strings.Contains(got.Waiting, "ready for review") {
		t.Fatalf("in flight after the first tick = %+v, want merging, the draft marked ready", got)
	}

	f.tick(t, f.t0.Add(30*time.Second))
	if got := f.forge.mergedPulls(); !slices.Equal(got, []int{1}) {
		t.Fatalf("merged = %v, want o/kit#1 merged once ready", got)
	}
	if got := f.snapshot(t).InFlight; got == nil || got.Phase != MergeAndProveDeploying || got.MergeCommit != mergeSHA(1) {
		t.Errorf("in flight after the merge = %+v, want deploying", got)
	}
	if got := f.forge.readiedPulls(); len(got) != 1 {
		t.Errorf("readied = %v, want once", got)
	}
}

// A forge that cannot mark a draft ready for review is capability_missing
// for a draft, and accepts a pull request that is not one.
func TestMergeAndProve_DraftNeedsTheReadyCapability(t *testing.T) {
	t.Parallel()
	f := newProveFixture(t)
	f.forge.draft(1)
	got := f.o.Supervisor().MergeAndProve(t.Context(), "ONS-1")
	if got.Status != SupervisorUnsupported || !strings.Contains(got.Message, "ready for review") {
		t.Errorf("merge-and-prove of a draft with no ready capability = %+v, want unsupported", got)
	}
	if rows, _ := f.store.LoadMergeAndProve(t.Context()); len(rows) != 0 {
		t.Errorf("stored = %+v, want nothing queued", rows)
	}
	f.accept(t, "ONS-2")
}
