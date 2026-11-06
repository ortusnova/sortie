package orchestrator

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/notify/route"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// staleCriteria is a review recorded before the after-deploy reason
// existed: its live criterion carries no reason.
const staleCriteria = `[{"criterion":"the dashboard shows the queue","verdict":"met","evidence":"web test"},` +
	`{"criterion":"p1 serves the new route","verdict":"live-proof","evidence":"needs the deployed host"}]`

// proveForge is a fake forge of the repository o/kit: pull requests by
// number with their status.
type proveForge struct {
	controlledSCMAdapter
	mu    sync.Mutex
	pulls map[int]*domain.PRMergeStatus
}

func headSHA(n int) string { return fmt.Sprintf("b%039x", n) }

func newProveForge() *proveForge {
	f := &proveForge{pulls: map[int]*domain.PRMergeStatus{}}
	f.getMergeabilityFn = func(_ context.Context, n int, _, _ string) (domain.PRMergeStatus, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		p, ok := f.pulls[n]
		if !ok {
			return domain.PRMergeStatus{}, &domain.SCMError{Kind: domain.ErrSCMNotFound, Message: "no such pull request"}
		}
		return *p, nil
	}
	return f
}

func (f *proveForge) open(n int, head string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pulls[n] = &domain.PRMergeStatus{Mergeability: domain.MergeabilityClean, HeadSHA: head, BranchName: fmt.Sprintf("sortie/ONS-%d", n)}
}

// storyTracker is a fake tracker of the stories ONS-<n>: their state and
// labels, and the comments Sortie writes.
type storyTracker struct {
	domain.TrackerAdapter
	mu       sync.Mutex
	issues   map[string]*domain.Issue
	comments map[string][]string
}

func newStoryTracker() *storyTracker {
	return &storyTracker{issues: map[string]*domain.Issue{}, comments: map[string][]string{}}
}

func (s *storyTracker) add(id, identifier, state string, labels ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.issues[id] = &domain.Issue{ID: id, Identifier: identifier, Title: identifier, State: state, Labels: labels}
}

func (s *storyTracker) FetchIssueByID(_ context.Context, id string) (domain.Issue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if is, ok := s.issues[id]; ok {
		return *is, nil
	}
	return domain.Issue{}, fmt.Errorf("issue %s not found", id)
}

func (s *storyTracker) TransitionIssue(_ context.Context, id, target string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.issues[id].State = target
	return nil
}

func (s *storyTracker) AddLabel(_ context.Context, id, label string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !slices.Contains(s.issues[id].Labels, label) {
		s.issues[id].Labels = append(s.issues[id].Labels, label)
	}
	return nil
}

func (s *storyTracker) issue(id string) domain.Issue {
	s.mu.Lock()
	defer s.mu.Unlock()
	is := *s.issues[id]
	is.Labels = slices.Clone(is.Labels)
	return is
}

func (s *storyTracker) setState(id, state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.issues[id].State = state
}

func (s *storyTracker) CommentIssue(_ context.Context, id, body string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.comments[id] = append(s.comments[id], body)
	return nil
}

// proveFixture is an orchestrator whose supervisor reviews the stories of
// a fake tracker with their pull requests on a fake forge.
type proveFixture struct {
	*supervisorHarness
	forge   *proveForge
	stories *storyTracker
	spy     *notifierSpy
	t0      time.Time
}

func newProveFixture(t *testing.T) *proveFixture {
	t.Helper()
	state := NewState(5000, 4, 0, nil, AgentTotals{})
	h := newSupervisorHarness(t, state)
	f := &proveFixture{supervisorHarness: h, forge: newProveForge(), stories: newStoryTracker(), spy: &notifierSpy{},
		t0: time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)}
	cfg := gateConfig()
	cfg.Tracker.HandoffState = "In Review"
	cfg.Tracker.ActiveStates = []string{"Todo"}
	cfg.Tracker.TerminalStates = []string{"Done", "Canceled"}
	h.o.workflowManager = &stubWorkflowManager{config: cfg}
	h.o.scmAdapter = f.forge
	h.o.trackerAdapter = f.stories
	h.o.router = mustRouter(t, nil, spyLookup(f.spy), route.Inputs{})
	return f
}

// addStory adds ONS-<n> in review with its watched pull request o/kit#<n>
// and its criteria verdict.
func (f *proveFixture) addStory(t *testing.T, n int, criteria, outcome string, labels ...string) {
	t.Helper()
	id, identifier := fmt.Sprintf("id-%d", n), fmt.Sprintf("ONS-%d", n)
	f.stories.add(id, identifier, "In Review", labels...)
	f.forge.open(n, headSHA(n))
	f.o.state.PendingReactions[ReactionKey(id, ReactionKindAutoMerge)] = &PendingReaction{
		IssueID: id, Identifier: identifier, Kind: ReactionKindAutoMerge, Attempt: 1,
		KindData:  &AutoMergeReactionData{PRNumber: n, Owner: "o", Repo: "kit", Branch: "sortie/" + identifier, SHA: headSHA(n)},
		AgentKind: "claude-code", RuleName: "check",
	}
	if err := f.store.UpsertCriteriaVerdict(t.Context(), persistence.CriteriaVerdict{
		IssueID: id, Outcome: outcome, Criteria: criteria, RecordedAt: f.t0,
	}); err != nil {
		t.Fatal(err)
	}
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
