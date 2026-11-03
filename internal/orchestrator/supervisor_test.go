package orchestrator

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// supervisorForge is a fake forge: one pull request whose status, review
// decision and CI conclusion the test sets, which records merges and
// drafts.
type supervisorForge struct {
	controlledSCMAdapter
	status   domain.PRMergeStatus
	decision domain.ReviewDecision
	ci       string
	merges   []string // expected head of each merge
	drafts   int
}

var _ domain.PullRequestDrafter = (*supervisorForge)(nil)

func newSupervisorForge() *supervisorForge {
	f := &supervisorForge{
		status:   domain.PRMergeStatus{Mergeability: domain.MergeabilityClean, HeadSHA: "head-1"},
		decision: domain.ReviewDecisionApproved,
		ci:       "success",
	}
	f.getMergeabilityFn = func(context.Context, int, string, string) (domain.PRMergeStatus, error) { return f.status, nil }
	f.getReviewDecisionFn = func(context.Context, int, string, string) (domain.ReviewDecision, error) { return f.decision, nil }
	f.getCIStatusFn = func(context.Context, int, string, string) (string, error) { return f.ci, nil }
	f.mergePRFn = func(_ context.Context, _ int, _, _ string, _ domain.MergeStrategy, _, _, head string) (domain.MergeResult, error) {
		f.merges = append(f.merges, head)
		f.status.Merged, f.status.MergeCommitSHA = true, "merge-1"
		return domain.MergeResult{SHA: "merge-1", Merged: true}, nil
	}
	return f
}

func (f *supervisorForge) MarkPullRequestDraft(context.Context, int, string, string) error {
	f.drafts++
	f.status.Draft = true
	return nil
}

// supervisorTracker is a fake tracker that reads an issue's labels and
// creates issues.
type supervisorTracker struct {
	labelTracker
	created []domain.NewIssue
}

var _ domain.IssueCreator = (*supervisorTracker)(nil)

func (s *supervisorTracker) CreateIssue(_ context.Context, issue domain.NewIssue) (domain.CreatedIssue, error) {
	s.created = append(s.created, issue)
	return domain.CreatedIssue{ID: "new-id", Identifier: "ONS-200"}, nil
}

// supervisorHarness is an orchestrator whose event loop only runs the
// actions sent to it.
type supervisorHarness struct {
	o       *Orchestrator
	store   *persistence.Store
	forge   *supervisorForge
	tracker *supervisorTracker
	polls   atomic.Int64
}

func newSupervisorHarness(t *testing.T, state *State) *supervisorHarness {
	t.Helper()
	h := &supervisorHarness{store: openRepeatedFailureStore(t), forge: newSupervisorForge(), tracker: &supervisorTracker{}}
	cfg := gateConfig()
	h.o = &Orchestrator{
		state:                    state,
		logger:                   discardLogger(),
		trackerAdapter:           h.tracker,
		workflowManager:          &stubWorkflowManager{config: cfg},
		store:                    h.store,
		scmAdapter:               h.forge,
		prDrafter:                h.forge,
		reviewReactionConfigured: true,
		actionCh:                 make(chan *loopAction),
		refreshCh:                make(chan struct{}, 1),
		stopping:                 make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case <-h.o.refreshCh:
				h.polls.Add(1)
			case action := <-h.o.actionCh:
				if action.state.CompareAndSwap(loopActionPending, loopActionTaken) {
					action.apply(ctx)
					close(action.done)
				}
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return h
}

// stateWithParked returns a state with one parked issue.
func stateWithParked(issueID, identifier string) *State {
	s := NewState(5000, 4, 0, nil, AgentTotals{})
	s.Parked[issueID] = &ParkedEntry{Identifier: identifier, Reason: parkReasonAgentBlocked, ParkedAt: time.Now().Add(-time.Hour)}
	return s
}

// Scenario "An action and its evidence": the supervisor records the merge
// of a parked story. Sortie checks the forge shows the pull request merged
// at that commit before it writes the plan_satisfaction row.
func TestSupervisor_RecordMergeOfAParkedStory(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, stateWithParked("id-7", "ONS-7"))
	h.forge.status = domain.PRMergeStatus{Merged: true, Closed: true, MergeCommitSHA: "abc123"}
	sup := h.o.Supervisor()
	ctx := t.Context()

	if got := sup.RecordMerge(ctx, "ONS-7", "o/r#12", "def456"); got.Status != SupervisorPrecondition || got.Code != "commit_mismatch" {
		t.Errorf("record at another commit = %+v, want commit_mismatch", got)
	}
	if got := sup.RecordMerge(ctx, "ONS-7", "o/r", "abc123"); got.Status != SupervisorPrecondition || got.Code != "invalid_request" {
		t.Errorf("record without a number = %+v, want invalid_request", got)
	}
	if got := sup.RecordMerge(ctx, "ONS-404", "o/r#12", "abc123"); got.Status != SupervisorUnknownIssue {
		t.Errorf("record for an unknown issue = %+v, want unknown issue", got)
	}
	assertNoPlanRecord(t, h.store, "id-7")

	got := sup.RecordMerge(ctx, "ONS-7", "o/r#12", "ABC123")
	want := SupervisorResult{Status: SupervisorDone, IssueID: "id-7", Identifier: "ONS-7", PullRequest: "o/r#12", MergeCommit: "abc123"}
	if got != want {
		t.Fatalf("record = %+v, want %+v", got, want)
	}
	rows, err := h.store.LoadPlanSatisfaction(ctx, []string{"id-7"})
	if err != nil || len(rows["id-7"]) != 1 {
		t.Fatalf("plan satisfaction = %+v, %v, want one row", rows, err)
	}
	r := rows["id-7"][0]
	if r.Kind != persistence.PlanSatisfactionMerged || r.Ref != "abc123" || r.PR != "o/r#12" || r.RecordedAt.IsZero() {
		t.Errorf("row = %+v, want merged at abc123 by o/r#12 with a time", r)
	}
}

// A merge the forge does not show is not recorded.
func TestSupervisor_RecordMergeNeedsTheForgesMerge(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, stateWithParked("id-8", "ONS-8"))
	h.forge.status = domain.PRMergeStatus{Closed: true}
	if got := h.o.Supervisor().RecordMerge(t.Context(), "ONS-8", "o/r#12", "abc123"); got.Status != SupervisorPrecondition || got.Code != "not_merged" {
		t.Errorf("record of an unmerged pull request = %+v, want not_merged", got)
	}
	assertNoPlanRecord(t, h.store, "id-8")

	// A watched issue's merge must be of the pull request Sortie watches.
	state := stateWithReviewReaction(t, "id-9", 31)
	h = newSupervisorHarness(t, state)
	h.forge.status = domain.PRMergeStatus{Merged: true, MergeCommitSHA: "abc123"}
	if got := h.o.Supervisor().RecordMerge(t.Context(), "id-9-ident", "o/r#12", "abc123"); got.Code != "pull_request_mismatch" {
		t.Errorf("record of another pull request = %+v, want pull_request_mismatch", got)
	}
}

func assertNoPlanRecord(t *testing.T, store *persistence.Store, issueID string) {
	t.Helper()
	rows, err := store.LoadPlanSatisfaction(t.Context(), []string{issueID})
	if err != nil || len(rows[issueID]) != 0 {
		t.Fatalf("plan satisfaction = %+v, %v, want none", rows, err)
	}
}

func TestSupervisor_ReleasePark(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, stateWithParked("id-10", "ONS-10"))
	sup := h.o.Supervisor()

	got := sup.ReleasePark(t.Context(), "ONS-10")
	if got.Status != SupervisorDone || got.IssueID != "id-10" {
		t.Fatalf("release = %+v, want done", got)
	}
	if _, parked := h.o.state.Parked["id-10"]; parked {
		t.Error("issue still parked after the release")
	}
	release := h.o.state.ParkReleases["id-10"]
	if release == nil || release.ReleaseGesture != unparkTriggerReleased {
		t.Errorf("park release = %+v, want the released gesture", release)
	}
	if again := sup.ReleasePark(t.Context(), "ONS-10"); again.Code != "issue_not_parked" {
		t.Errorf("second release = %+v, want issue_not_parked", again)
	}
}

func TestSupervisor_Merge(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		labels []string
		change func(f *supervisorForge)
		code   string
	}{
		{name: "review and CI passed", labels: []string{config.ReviewCriteriaLabel}},
		{name: "a ui story waits for the operator", labels: []string{config.ReviewUILabel}, code: "ui_story"},
		{name: "review not passed", change: func(f *supervisorForge) { f.decision = domain.ReviewDecisionChangesRequested }, code: "review_not_passed"},
		{name: "review required", change: func(f *supervisorForge) { f.decision = domain.ReviewDecisionReviewRequired }, code: "review_not_passed"},
		{name: "CI failing", change: func(f *supervisorForge) { f.ci = "failure" }, code: "ci_not_passed"},
		{name: "CI pending", change: func(f *supervisorForge) { f.ci = "pending" }, code: "ci_not_passed"},
		{name: "no CI", change: func(f *supervisorForge) { f.ci = "" }, code: "ci_not_passed"},
		{name: "a draft", change: func(f *supervisorForge) { f.status.Draft = true }, code: "draft"},
		{name: "conflicts", change: func(f *supervisorForge) { f.status.Mergeability = domain.MergeabilityDirty }, code: "not_mergeable"},
		{name: "already merged", change: func(f *supervisorForge) { f.status.Merged = true }, code: "already_merged"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newSupervisorHarness(t, stateWithReviewReaction(t, "id-11", 41))
			h.tracker.labels = tt.labels
			if tt.change != nil {
				tt.change(h.forge)
			}
			got := h.o.Supervisor().Merge(t.Context(), "id-11-ident")
			if tt.code != "" {
				if got.Status != SupervisorPrecondition || got.Code != tt.code {
					t.Errorf("merge = %+v, want %s", got, tt.code)
				}
				if len(h.forge.merges) != 0 {
					t.Errorf("merges = %v, want none", h.forge.merges)
				}
				return
			}
			want := SupervisorResult{Status: SupervisorDone, IssueID: "id-11", Identifier: "id-11-ident", PullRequest: "owner/repo#41", MergeCommit: "merge-1"}
			if got != want {
				t.Fatalf("merge = %+v, want %+v", got, want)
			}
			if len(h.forge.merges) != 1 || h.forge.merges[0] != "head-1" {
				t.Errorf("merges = %v, want one at head-1", h.forge.merges)
			}
			rows, err := h.store.LoadPlanSatisfaction(t.Context(), []string{"id-11"})
			if err != nil || len(rows["id-11"]) != 1 || rows["id-11"][0].Ref != "merge-1" {
				t.Errorf("plan satisfaction = %+v, %v, want the merge recorded", rows, err)
			}
		})
	}
}

// supervisorComments is a forge's conversation comments on the pull
// request.
type supervisorComments []domain.ReviewComment

func (c supervisorComments) FetchPullRequestComments(context.Context, int, string, string) ([]domain.ReviewComment, error) {
	return c, nil
}

// The supervisor's merge follows the story's latest criteria verdict: a
// review that found every criterion met merges; one that left a
// criterion to a live proof merges only once a live-proof job recorded
// it proven at the pull request's head; one that found a criterion not
// met, or recorded no verdict, never merges. CI alone is not enough:
// before, a pull request whose live criterion was never proven merged
// once its CI passed.
func TestSupervisor_MergeFollowsTheCriteriaVerdict(t *testing.T) {
	t.Parallel()
	const head = "4d2c1aa9e1f0b7c3d5e6f7a8b9c0d1e2f3a4b5c6"
	proven := func(sha string) domain.ReviewComment {
		return domain.ReviewComment{ID: "c1", Reviewer: "ortus-kit", Body: "<!-- live-proof-result -->\nResult: proven\n" + LiveProofMarker + " " + sha + "\n"}
	}
	for _, tt := range []struct {
		name     string
		outcome  string
		comments supervisorComments
		noReader bool
		code     string
	}{
		{name: "every criterion met", outcome: CriteriaOutcomeMet},
		{name: "a live criterion proven at the head", outcome: CriteriaOutcomeLiveProof, comments: supervisorComments{proven(head[:12])}},
		{name: "a live criterion never proven", outcome: CriteriaOutcomeLiveProof, code: "live_proof_unproven"},
		{name: "a live criterion proven at an earlier head", outcome: CriteriaOutcomeLiveProof, comments: supervisorComments{proven("0123456789abcdef")}, code: "live_proof_unproven"},
		{name: "a live criterion on a forge without comments", outcome: CriteriaOutcomeLiveProof, noReader: true, code: "live_proof_unproven"},
		{name: "a criterion not met", outcome: CriteriaOutcomeRefused, comments: supervisorComments{proven(head)}, code: "criteria_not_met"},
		{name: "no verdict", outcome: CriteriaOutcomeRefused, code: "criteria_not_met"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newSupervisorHarness(t, stateWithReviewReaction(t, "id-11", 41))
			h.tracker.labels = []string{config.ReviewCriteriaLabel}
			h.forge.status.HeadSHA = head
			if !tt.noReader {
				h.o.prCommentReader = tt.comments
			}
			if err := h.store.UpsertCriteriaVerdict(t.Context(), persistence.CriteriaVerdict{
				IssueID: "id-11", Outcome: tt.outcome, RecordedAt: time.Now(),
			}); err != nil {
				t.Fatal(err)
			}
			got := h.o.Supervisor().Merge(t.Context(), "id-11-ident")
			if tt.code != "" {
				if got.Status != SupervisorPrecondition || got.Code != tt.code || len(h.forge.merges) != 0 {
					t.Errorf("merge = %+v with merges %v, want %s and none", got, h.forge.merges, tt.code)
				}
				return
			}
			if got.Status != SupervisorDone || len(h.forge.merges) != 1 || h.forge.merges[0] != head {
				t.Errorf("merge = %+v with merges %v, want one at the head", got, h.forge.merges)
			}
		})
	}
}

func TestSupervisor_MarkDraft(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, stateWithReviewReaction(t, "id-12", 51))
	got := h.o.Supervisor().MarkDraft(t.Context(), "id-12-ident")
	if got.Status != SupervisorDone || got.PullRequest != "owner/repo#51" || h.forge.drafts != 1 {
		t.Fatalf("draft = %+v with %d drafts, want done once", got, h.forge.drafts)
	}

	h = newSupervisorHarness(t, stateWithParked("id-13", "ONS-13"))
	if got := h.o.Supervisor().MarkDraft(t.Context(), "ONS-13"); got.Code != "no_pull_request" {
		t.Errorf("draft without a pull request = %+v, want no_pull_request", got)
	}
	h = newSupervisorHarness(t, stateWithReviewReaction(t, "id-14", 52))
	h.o.prDrafter = nil
	if got := h.o.Supervisor().MarkDraft(t.Context(), "id-14-ident"); got.Status != SupervisorUnsupported {
		t.Errorf("draft on a forge without drafts = %+v, want unsupported", got)
	}
}

func TestSupervisor_FixRunCarriesTheMessage(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, stateWithReviewReaction(t, "id-15", 61))
	got := h.o.Supervisor().FixRun(t.Context(), "id-15-ident", "The worker column is missing.")
	if got.Status != SupervisorDone || got.PullRequest != "owner/repo#61" {
		t.Fatalf("fix run = %+v, want done", got)
	}
	var retry *RetryEntry
	onLoop(t, h.o, func() { retry = h.o.state.RetryAttempts["id-15"] })
	if retry == nil || retry.ReactionKind != ReactionKindReview {
		t.Fatalf("retry = %+v, want a review fix run", retry)
	}
	comments, _ := retry.ContinuationContext[reviewCommentsKey].([]map[string]any)
	if len(comments) != 1 || comments[0]["body"] != "The worker column is missing." || comments[0]["reviewer"] != "supervisor" {
		t.Errorf("review_comments = %#v, want the supervisor's message", retry.ContinuationContext[reviewCommentsKey])
	}
	if again := h.o.Supervisor().FixRun(t.Context(), "id-15-ident", "again"); again.Status != SupervisorPrecondition {
		t.Errorf("second fix run = %+v, want refused", again)
	}
	if empty := h.o.Supervisor().FixRun(t.Context(), "id-15-ident", " "); empty.Code != "invalid_request" {
		t.Errorf("fix run without a message = %+v, want invalid_request", empty)
	}
}

// onLoop runs read on o's event loop.
func onLoop(t *testing.T, o *Orchestrator, read func()) {
	t.Helper()
	if o.runOnLoop(t.Context(), func(context.Context) { read() }) != loopRan {
		t.Fatal("the loop did not run the read")
	}
}

func TestSupervisor_FileIssueWaitsInTheBacklog(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, NewState(5000, 4, 0, nil, AgentTotals{}))
	got := h.o.Supervisor().FileIssue(t.Context(), "Flaky seed step", "Seen on ONS-7.")
	if got.Status != SupervisorDone || got.Identifier != "ONS-200" {
		t.Fatalf("file = %+v, want ONS-200", got)
	}
	if len(h.tracker.created) != 1 {
		t.Fatalf("created = %d, want 1", len(h.tracker.created))
	}
	c := h.tracker.created[0]
	if c.Title != "Flaky seed step" || c.State != "Backlog" || c.Kind != domain.IssueKindStory || c.PlanKey == "" {
		t.Errorf("created = %+v, want a story in Backlog with a plan key", c)
	}
	if blank := h.o.Supervisor().FileIssue(t.Context(), " ", ""); blank.Code != "invalid_request" {
		t.Errorf("file without a title = %+v, want invalid_request", blank)
	}
}

func TestSupervisor_RequestLiveProof(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, stateWithReviewReaction(t, "id-16", 71))
	if got := h.o.Supervisor().RequestLiveProof(t.Context(), "id-16-ident"); got.Status != SupervisorDone || got.PullRequest != "owner/repo#71" {
		t.Errorf("live proof = %+v, want done on owner/repo#71", got)
	}
	if got := h.o.Supervisor().RequestLiveProof(t.Context(), "ONS-404"); got.Status != SupervisorUnknownIssue {
		t.Errorf("live proof of an unknown issue = %+v, want unknown issue", got)
	}
}
