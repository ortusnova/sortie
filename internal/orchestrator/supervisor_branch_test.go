package orchestrator

import (
	"strings"
	"testing"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// homelabPullRequest is the forge's open pull request of ONS-39's branch in
// the repository the story routes to.
var homelabPullRequest = supervisorFinder{"o/homelab:sortie/ONS-39": {Number: 155}}

// Scenario "A parked story with a conflicting pull request": Sortie
// watches no pull request of the parked story, and its branch has an open
// pull request that conflicts with its base. The supervisor's fix request
// starts a fix run on that pull request, and the park ends. Before, it
// answered no_pull_request, as on sortie#58.
func TestSupervisor_FixRunOfAParkedStoryWithAConflictingPullRequest(t *testing.T) {
	t.Parallel()
	h := parkedStoryHarness(t, homelabPullRequest)
	h.forge.status.Mergeability = domain.MergeabilityDirty
	if err := h.store.UpsertParkedIssue(t.Context(), parkedIssueRow("id-39", h.o.state.Parked["id-39"])); err != nil {
		t.Fatal(err)
	}

	got := h.o.Supervisor().FixRun(t.Context(), "ONS-39", "Rebase on master and resolve the conflict in hosts.yml.")
	want := SupervisorResult{Status: SupervisorDone, IssueID: "id-39", Identifier: "ONS-39", PullRequest: "o/homelab#155"}
	if got != want {
		t.Fatalf("fix run = %+v, want %+v", got, want)
	}
	var retry *RetryEntry
	var parked, released bool
	onLoop(t, h.o, func() {
		retry = h.o.state.RetryAttempts["id-39"]
		_, parked = h.o.state.Parked["id-39"]
		_, released = h.o.state.ParkReleases["id-39"]
	})
	if parked || released {
		t.Errorf("parked = %v, release pending = %v, want the park ended", parked, released)
	}
	rows, err := h.store.ListParkedIssues(t.Context())
	if err != nil || len(rows) != 0 {
		t.Errorf("park rows = %+v, %v, want none", rows, err)
	}
	if retry == nil || retry.ReactionKind != ReactionKindReview {
		t.Fatalf("retry = %+v, want a review fix run", retry)
	}
	if retry.Continuation != (ContinuationTarget{PRNumber: 155, Owner: "o", Repo: "homelab", Attempt: 1}) {
		t.Errorf("continuation = %+v, want o/homelab#155", retry.Continuation)
	}
	if retry.RuleName != "review" || retry.AgentKind != "agent-client-protocol" || retry.TemplateID != "review-template" {
		t.Errorf("retry runs %q/%q/%q, want the parked run's rule, agent and template", retry.RuleName, retry.AgentKind, retry.TemplateID)
	}
	comments, _ := retry.ContinuationContext[reviewCommentsKey].([]map[string]any)
	if len(comments) != 1 {
		t.Fatalf("review_comments = %#v, want the supervisor's request", retry.ContinuationContext[reviewCommentsKey])
	}
	body, _ := comments[0]["body"].(string)
	for _, want := range []string{"o/homelab#155", "branch sortie/ONS-39", "resolve the conflict in hosts.yml"} {
		if !strings.Contains(body, want) {
			t.Errorf("request body = %q, want it to hold %q", body, want)
		}
	}

	if again := h.o.Supervisor().FixRun(t.Context(), "ONS-39", "again"); again.Code != "retry_scheduled" {
		t.Errorf("second fix run = %+v, want retry_scheduled", again)
	}
}

// runningStoryState returns a state in which ONS-39 has a running worker
// and Sortie watches no pull request of it.
func runningStoryState() *State {
	s := NewState(5000, 4, 0, nil, AgentTotals{})
	s.Running["id-39"] = &RunningEntry{Identifier: "ONS-39"}
	return s
}

// A running story Sortie watches no pull request of has its open pull
// request found by its branch: a fix run is refused for the running worker,
// not for a missing pull request, and the live proof, draft and merge act
// on it.
func TestSupervisor_RunningStoryWithoutAWatch(t *testing.T) {
	t.Parallel()
	h := routedStoryHarness(t, runningStoryState(), homelabPullRequest)
	sup := h.o.Supervisor()

	if got := sup.FixRun(t.Context(), "ONS-39", "fix it"); got.Status != SupervisorPrecondition || got.Code != "running" {
		t.Errorf("fix run = %+v, want running", got)
	}
	if got := sup.RequestLiveProof(t.Context(), "ONS-39", "criterion"); got.Status != SupervisorDone || got.PullRequest != "o/homelab#155" {
		t.Errorf("live proof = %+v, want done on o/homelab#155", got)
	}
	if got := sup.Merge(t.Context(), "ONS-39"); got.Status != SupervisorDone || got.PullRequest != "o/homelab#155" || len(h.forge.merges) != 1 {
		t.Errorf("merge = %+v with merges %v, want o/homelab#155 merged once", got, h.forge.merges)
	}

	h = routedStoryHarness(t, runningStoryState(), homelabPullRequest)
	if got := h.o.Supervisor().MarkDraft(t.Context(), "ONS-39"); got.Status != SupervisorDone || got.PullRequest != "o/homelab#155" || h.forge.drafts != 1 {
		t.Errorf("draft = %+v with %d drafts, want o/homelab#155 a draft", got, h.forge.drafts)
	}
}

// A story whose run has ended, released from its park and watched by no
// reaction, gets a fix run on its open pull request.
func TestSupervisor_FixRunOfAStoryWhoseRunEnded(t *testing.T) {
	t.Parallel()
	state := stateWithParked("id-39", "ONS-39")
	release := state.Parked["id-39"]
	delete(state.Parked, "id-39")
	release.ReleaseGesture = unparkTriggerReleased
	state.ParkReleases["id-39"] = release
	h := routedStoryHarness(t, state, homelabPullRequest)

	if got := h.o.Supervisor().FixRun(t.Context(), "ONS-39", "fix it"); got.Status != SupervisorDone || got.PullRequest != "o/homelab#155" {
		t.Fatalf("fix run = %+v, want done on o/homelab#155", got)
	}
	var retry *RetryEntry
	onLoop(t, h.o, func() { retry = h.o.state.RetryAttempts["id-39"] })
	if retry == nil || retry.ReactionKind != ReactionKindReview || retry.Continuation.PRNumber != 155 {
		t.Errorf("retry = %+v, want a review fix run on #155", retry)
	}
}

// A story whose branch has no open pull request gets no_pull_request
// naming the branch and the repository searched from every route, and
// nothing changes: no run, no draft, no merge, and the park stays.
func TestSupervisor_NoOpenPullRequestOnTheBranch(t *testing.T) {
	t.Parallel()
	h := parkedStoryHarness(t, supervisorFinder{"o/sortie:sortie/ONS-39": {Number: 9}})
	if err := h.store.UpsertCriteriaVerdict(t.Context(), persistence.CriteriaVerdict{IssueID: "id-39", Outcome: CriteriaOutcomeMet}); err != nil {
		t.Fatal(err)
	}
	sup := h.o.Supervisor()
	for name, got := range map[string]SupervisorResult{
		"fix":        sup.FixRun(t.Context(), "ONS-39", "fix it"),
		"draft":      sup.MarkDraft(t.Context(), "ONS-39"),
		"merge":      sup.Merge(t.Context(), "ONS-39"),
		"live-proof": sup.RequestLiveProof(t.Context(), "ONS-39", "criterion"),
	} {
		if got.Status != SupervisorPrecondition || got.Code != "no_pull_request" ||
			!strings.Contains(got.Message, "sortie/ONS-39") || !strings.Contains(got.Message, "o/homelab") {
			t.Errorf("%s = %+v, want no_pull_request naming the branch and repository", name, got)
		}
	}
	var retry *RetryEntry
	var park *ParkedEntry
	onLoop(t, h.o, func() { retry, park = h.o.state.RetryAttempts["id-39"], h.o.state.Parked["id-39"] })
	if retry != nil || park == nil {
		t.Errorf("retry = %+v, park = %+v, want no run and the park kept", retry, park)
	}
	if h.forge.drafts != 0 || len(h.forge.merges) != 0 {
		t.Errorf("drafts = %d, merges = %v, want none", h.forge.drafts, h.forge.merges)
	}
}
