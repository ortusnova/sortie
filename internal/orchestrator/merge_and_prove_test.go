package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/notify/route"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// proveForge is a fake forge of the repository o/kit: pull requests by
// number with their status, CI and conversation comments, which merges,
// opens reverts, compares commits and lists merged reverts.
type proveForge struct {
	controlledSCMAdapter
	mu       sync.Mutex
	pulls    map[int]*domain.PRMergeStatus
	ci       map[int]string
	comments map[int][]domain.ReviewComment
	contains map[string]bool // deployed commit -> holds the merge commit
	merges   []int
	reverts  []int // the pull requests a revert was opened of
	merged   []domain.MergedRevert
}

func mergeSHA(n int) string { return fmt.Sprintf("%040x", 0xabc000+n) }

func headSHA(n int) string { return fmt.Sprintf("b%039x", n) }

func newProveForge() *proveForge {
	f := &proveForge{
		pulls: map[int]*domain.PRMergeStatus{}, ci: map[int]string{},
		comments: map[int][]domain.ReviewComment{}, contains: map[string]bool{},
	}
	f.getMergeabilityFn = func(_ context.Context, n int, _, _ string) (domain.PRMergeStatus, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		p, ok := f.pulls[n]
		if !ok {
			return domain.PRMergeStatus{}, &domain.SCMError{Kind: domain.ErrSCMNotFound, Message: "no such pull request"}
		}
		return *p, nil
	}
	f.getCIStatusFn = func(_ context.Context, n int, _, _ string) (string, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.ci[n], nil
	}
	f.mergePRFn = func(_ context.Context, n int, _, _ string, _ domain.MergeStrategy, _, _, head string) (domain.MergeResult, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		p := f.pulls[n]
		if p.HeadSHA != head {
			return domain.MergeResult{}, &domain.SCMError{Kind: domain.ErrSCMConflict, Message: "head moved"}
		}
		p.Merged, p.Closed, p.MergeCommitSHA = true, true, mergeSHA(n)
		f.merges = append(f.merges, n)
		return domain.MergeResult{SHA: p.MergeCommitSHA, Merged: true}, nil
	}
	return f
}

func (f *proveForge) open(n int, head string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pulls[n] = &domain.PRMergeStatus{Mergeability: domain.MergeabilityClean, HeadSHA: head, BranchName: fmt.Sprintf("sortie/ONS-%d", n)}
	f.ci[n] = "success"
}

func (f *proveForge) comment(n int, id, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.comments[n] = append(f.comments[n], domain.ReviewComment{ID: id, Body: body})
}

func (f *proveForge) setCI(n int, ci string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ci[n] = ci
}

func (f *proveForge) holds(commit string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.contains[commit] = true
}

func (f *proveForge) mergedPulls() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.merges)
}

func (f *proveForge) revertsOpened() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.reverts)
}

func (f *proveForge) FetchPullRequestComments(_ context.Context, n int, _, _ string) ([]domain.ReviewComment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.comments[n]), nil
}

func (f *proveForge) RevertPullRequest(_ context.Context, n int, _, _, _, _ string) (domain.PullRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	number := 100 + n
	f.reverts = append(f.reverts, n)
	f.pulls[number] = &domain.PRMergeStatus{Mergeability: domain.MergeabilityClean, HeadSHA: fmt.Sprintf("revert-head-%d", n)}
	f.ci[number] = "pending"
	return domain.PullRequest{Number: number, Created: true}, nil
}

func (f *proveForge) CommitContains(_ context.Context, _, _, commit, _ string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.contains[commit], nil
}

func (f *proveForge) FindMergedReverts(context.Context, string, string, time.Time) ([]domain.MergedRevert, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.merged), nil
}

// storyTracker is a fake tracker of the stories ONS-<n>: their state and
// labels, the moves Sortie makes and the comments it writes.
type storyTracker struct {
	domain.TrackerAdapter
	mu       sync.Mutex
	issues   map[string]*domain.Issue
	moves    []string
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
	s.moves = append(s.moves, id+" "+target)
	return nil
}

func (s *storyTracker) CommentIssue(_ context.Context, id, body string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.comments[id] = append(s.comments[id], body)
	return nil
}

func (s *storyTracker) stateOf(id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.issues[id].State
}

func (s *storyTracker) lastComment(id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.comments[id]
	if len(c) == 0 {
		return ""
	}
	return c[len(c)-1]
}

// proveFixture is an orchestrator with stories ONS-1 and ONS-2 in review,
// their pull requests o/kit#1 and o/kit#2 watched by auto-merge, each
// with a criteria verdict that leaves one criterion to a proof after
// deploy, and the two hosts p1 and taichi whose deploy state files the
// test writes.
type proveFixture struct {
	*supervisorHarness
	forge   *proveForge
	stories *storyTracker
	spy     *notifierSpy
	dir     string
	t0      time.Time
}

const (
	afterDeployCriteria = `[{"criterion":"the dashboard shows the queue","verdict":"met","evidence":"web test"},` +
		`{"criterion":"p1 serves the new route after deploy","verdict":"live-proof","evidence":"needs the deployed host","reason":"after-deploy"}]`
)

func newProveFixture(t *testing.T) *proveFixture {
	t.Helper()
	state := NewState(5000, 4, 0, nil, AgentTotals{})
	h := newSupervisorHarness(t, state)
	f := &proveFixture{supervisorHarness: h, forge: newProveForge(), stories: newStoryTracker(), spy: &notifierSpy{}, dir: t.TempDir(),
		t0: time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)}
	cfg := gateConfig()
	cfg.Tracker.HandoffState = "In Review"
	cfg.Tracker.ActiveStates = []string{"Todo"}
	cfg.Tracker.TerminalStates = []string{"Done", "Canceled"}
	cfg.MergeAndProve = config.MergeAndProveConfig{Deadline: 30 * time.Minute, Hosts: []config.DeployHost{
		{Name: "p1", DeployState: filepath.Join(f.dir, "p1.json")},
		{Name: "taichi", DeployState: filepath.Join(f.dir, "taichi.json")},
	}}
	h.o.workflowManager = &stubWorkflowManager{config: cfg}
	h.o.scmAdapter = f.forge
	h.o.prCommentReader = f.forge
	h.o.prReverter = f.forge
	h.o.commitComparer = f.forge
	h.o.revertFinder = f.forge
	h.o.trackerAdapter = f.stories
	h.o.router = mustRouter(t, nil, spyLookup(f.spy), route.Inputs{Entries: []config.NotificationBackend{subscribe("webhook", domain.EventStoryReverted)}})
	for _, n := range []int{1, 2} {
		f.addStory(t, n, afterDeployCriteria, CriteriaOutcomeLiveProof)
	}
	f.deploy(t, "p1", "old-p1")
	f.deploy(t, "taichi", "old-taichi")
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

// deploy writes the deploy state file of host naming commit.
func (f *proveFixture) deploy(t *testing.T, host, commit string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"status": "succeeded", "commit": commit, "main": commit, "updated_at": f.t0})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.dir, host+".json")
	if err := os.WriteFile(path+".tmp", body, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		t.Fatal(err)
	}
}

// tick runs merge-and-prove's and the revert scan's part of the poll tick
// at now on the event loop, and waits for the notifications it sent.
func (f *proveFixture) tick(t *testing.T, now time.Time) {
	t.Helper()
	onLoop(t, f.o, func() {
		cfg := f.o.workflowManager.Config()
		f.o.advanceMergeAndProve(t.Context(), cfg, now)
		f.o.detectReverts(t.Context(), cfg, now)
	})
	f.o.state.TrackerOpsWg.Wait()
}

func (f *proveFixture) accept(t *testing.T, identifier string) {
	t.Helper()
	if got := f.o.Supervisor().MergeAndProve(t.Context(), identifier); got.Status != SupervisorDone {
		t.Fatalf("merge-and-prove of %s = %+v, want accepted", identifier, got)
	}
}

func (f *proveFixture) snapshot(t *testing.T) SnapshotMergeAndProve {
	t.Helper()
	var snap SnapshotMergeAndProve
	onLoop(t, f.o, func() { snap = snapshotMergeAndProve(f.o.state) })
	return snap
}

// restart replaces the runtime state with what a new process restores
// from the store, as startup does.
func (f *proveFixture) restart(t *testing.T) {
	t.Helper()
	rows, err := f.store.LoadMergeAndProve(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	onLoop(t, f.o, func() {
		f.o.state = NewState(5000, 4, 0, nil, AgentTotals{})
		PopulateMergeAndProve(f.o.state, rows, discardLogger())
	})
}

func (f *proveFixture) planMerge(t *testing.T, id string) (persistence.PlanSatisfaction, bool) {
	t.Helper()
	rows, err := f.store.LoadPlanSatisfaction(t.Context(), []string{id})
	if err != nil {
		t.Fatal(err)
	}
	return mergedRow(rows[id])
}

func liveProofScheduled(state *State, id string) bool {
	r := state.RetryAttempts[id]
	return r != nil && r.ReactionKind == ReactionKindLiveProof
}

// Scenario "Proven in time": a kit story merges through the route, both
// hosts deploy it, and its live proof is proven 12 minutes later. The
// story is Done, and the next queued story's merge starts.
func TestMergeAndProve_ProvenInTime(t *testing.T) {
	t.Parallel()
	f := newProveFixture(t)
	f.accept(t, "ONS-1")
	f.accept(t, "ONS-2")

	snap := f.snapshot(t)
	if snap.InFlight != nil || len(snap.Queue) != 2 || snap.Queue[0].Identifier != "ONS-1" || snap.Queue[1].Position != 2 {
		t.Fatalf("snapshot before the tick = %+v, want both queued in order", snap)
	}

	f.tick(t, f.t0)
	merge := mergeSHA(1)
	if got := f.forge.mergedPulls(); !slices.Equal(got, []int{1}) {
		t.Fatalf("merged = %v, want only o/kit#1: one merge-and-prove at a time", got)
	}
	snap = f.snapshot(t)
	if snap.InFlight == nil || snap.InFlight.Phase != MergeAndProveDeploying || snap.InFlight.MergeCommit != merge ||
		!snap.InFlight.Deadline.Equal(f.t0.Add(30*time.Minute)) || len(snap.Queue) != 1 || snap.Queue[0].Identifier != "ONS-2" {
		t.Fatalf("snapshot after the merge = %+v, want ONS-1 deploying until t0+30m and ONS-2 queued", snap)
	}
	if _, watched := f.o.state.PendingReactions[ReactionKey("id-1", ReactionKindAutoMerge)]; watched {
		t.Error("auto-merge still watches the merged pull request")
	}
	if _, found := f.planMerge(t, "id-1"); found {
		t.Error("merge recorded before the proof: dependents would start")
	}

	// p1 deploys the merge, taichi does not yet.
	f.forge.holds("new-main")
	f.deploy(t, "p1", "new-main")
	f.tick(t, f.t0.Add(3*time.Minute))
	if snap = f.snapshot(t); snap.InFlight.Phase != MergeAndProveDeploying || !strings.Contains(snap.InFlight.Waiting, "taichi deployed old-taichi") {
		t.Fatalf("in flight with one host deployed = %+v, want it deploying, waiting on taichi", snap.InFlight)
	}

	f.deploy(t, "taichi", "new-main")
	deployed := f.t0.Add(5 * time.Minute)
	f.tick(t, deployed)
	snap = f.snapshot(t)
	if snap.InFlight.Phase != MergeAndProveProving || !snap.InFlight.Deadline.Equal(deployed.Add(30*time.Minute)) ||
		!slices.Equal(snap.InFlight.Deployed, []DeployedHost{{"p1", "new-main"}, {"taichi", "new-main"}}) {
		t.Fatalf("in flight once both hosts deployed = %+v, want proving with its own deadline", snap.InFlight)
	}
	if !liveProofScheduled(f.o.state, "id-1") {
		t.Fatal("no live-proof job scheduled against the deployed main")
	}
	request := f.o.state.RetryAttempts["id-1"]
	if request.Continuation.PRNumber != 1 {
		t.Errorf("live-proof job continuation = %+v, want o/kit#1", request.Continuation)
	}
	onLoop(t, f.o, func() { f.o.state.RetryAttempts = map[string]*RetryEntry{} }) // the job ran

	f.forge.comment(1, "c-1", "Proven on p1 and taichi.\n\n`"+LiveProofMarker+" "+merge[:12]+"`")
	f.tick(t, deployed.Add(12*time.Minute))
	if got := f.stories.stateOf("id-1"); got != "Done" {
		t.Errorf("ONS-1 state = %q, want Done", got)
	}
	if row, found := f.planMerge(t, "id-1"); !found || row.Ref != merge || row.PR != "o/kit#1" {
		t.Errorf("ONS-1 merge record = %+v, %v, want o/kit#1 at the merge commit", row, found)
	}
	if got := f.forge.mergedPulls(); !slices.Equal(got, []int{1, 2}) {
		t.Errorf("merged = %v, want ONS-2's merge started once ONS-1 was proven", got)
	}
	if snap = f.snapshot(t); snap.InFlight == nil || snap.InFlight.Identifier != "ONS-2" || len(snap.Queue) != 0 {
		t.Errorf("snapshot after the proof = %+v, want ONS-2 in flight", snap)
	}
	if rows, _ := f.store.LoadMergeAndProve(t.Context()); len(rows) != 1 || rows[0].IssueID != "id-2" {
		t.Errorf("stored merge-and-prove = %+v, want only ONS-2", rows)
	}
}

// Scenario "Not proven": the deadline passes with no proven record. Sortie
// merges a revert pull request once its CI passes, the story returns to
// Todo with the reason, and the operator's channel receives the event.
func TestMergeAndProve_NotProvenReverts(t *testing.T) {
	t.Parallel()
	f := newProveFixture(t)
	f.accept(t, "ONS-1")
	f.tick(t, f.t0)
	f.forge.holds("new-main")
	f.deploy(t, "p1", "new-main")
	f.deploy(t, "taichi", "new-main")
	deployed := f.t0.Add(4 * time.Minute)
	f.tick(t, deployed)

	f.tick(t, deployed.Add(29*time.Minute))
	if got := f.snapshot(t).InFlight; got.Phase != MergeAndProveProving {
		t.Fatalf("before the deadline = %+v, want still proving", got)
	}
	f.tick(t, deployed.Add(30*time.Minute))
	got := f.snapshot(t).InFlight
	if got.Phase != MergeAndProveReverting || got.RevertPR != "o/kit#101" || !strings.Contains(got.Reason, "by the deadline") {
		t.Fatalf("at the deadline = %+v, want reverting with o/kit#101 and the reason", got)
	}
	if !strings.Contains(got.Waiting, "pending") {
		t.Errorf("waiting = %q, want the revert's pending CI", got.Waiting)
	}
	if f.stories.stateOf("id-1") != "In Review" {
		t.Error("the story moved before the revert merged")
	}

	f.forge.setCI(101, "success")
	f.tick(t, deployed.Add(33*time.Minute))
	if merged := f.forge.mergedPulls(); !slices.Equal(merged, []int{1, 101}) {
		t.Fatalf("merged = %v, want the story and then its revert", merged)
	}
	if state := f.stories.stateOf("id-1"); state != "Todo" {
		t.Errorf("ONS-1 state = %q, want Todo", state)
	}
	if c := f.stories.lastComment("id-1"); !strings.Contains(c, "o/kit#101") || !strings.Contains(c, "no comment on o/kit#1 recorded the live proof proven") {
		t.Errorf("ONS-1 comment = %q, want the revert and the unproven result as its reason", c)
	}
	if _, found := f.planMerge(t, "id-1"); found {
		t.Error("a reverted story keeps a merge record")
	}
	sent := f.spy.notifications()
	if len(sent) != 1 || sent[0].Envelope.EventType != domain.EventStoryReverted || sent[0].Envelope.Identifier != "ONS-1" {
		t.Fatalf("notifications = %+v, want one story.reverted for ONS-1", sent)
	}
	facts := sent[0].Message.Facts.(domain.StoryRevertedFacts)
	if !facts.BySortie || facts.RevertPullRequest != "o/kit#101" || facts.State != "Todo" || facts.Reason == "" {
		t.Errorf("facts = %+v, want Sortie's revert back to Todo with the reason", facts)
	}
	if snap := f.snapshot(t); snap.InFlight != nil {
		t.Errorf("in flight after the revert = %+v, want none", snap.InFlight)
	}
}

// A host that does not deploy the merge by the deadline reverts it, and
// the reason names the host.
func TestMergeAndProve_HostThatDoesNotDeployReverts(t *testing.T) {
	t.Parallel()
	f := newProveFixture(t)
	f.accept(t, "ONS-1")
	f.tick(t, f.t0)
	f.forge.holds("main-2")
	f.deploy(t, "p1", "main-2")
	f.tick(t, f.t0.Add(30*time.Minute))
	got := f.snapshot(t).InFlight
	if got.Phase != MergeAndProveReverting || !strings.Contains(got.Reason, "taichi deployed old-taichi") {
		t.Fatalf("at the deploy deadline = %+v, want reverting naming taichi", got)
	}
	if liveProofScheduled(f.o.state, "id-1") {
		t.Error("live-proof job scheduled though taichi never deployed")
	}
}

// A record that the proof failed reverts at once, with the record as the
// reason.
func TestMergeAndProve_UnprovenRecordReverts(t *testing.T) {
	t.Parallel()
	f := newProveFixture(t)
	f.accept(t, "ONS-1")
	f.tick(t, f.t0)
	f.forge.holds("new-main")
	f.deploy(t, "p1", "new-main")
	f.deploy(t, "taichi", "new-main")
	f.tick(t, f.t0.Add(time.Minute))
	f.forge.comment(1, "c-2", LiveProofUnprovenMarker+" "+mergeSHA(1)+"\np1 answers 404 on /api/v1/state")
	f.tick(t, f.t0.Add(2*time.Minute))
	got := f.snapshot(t).InFlight
	if got.Phase != MergeAndProveReverting || !strings.Contains(got.Reason, "p1 answers 404") {
		t.Fatalf("after the unproven record = %+v, want reverting with its text", got)
	}
}

// Any story but one whose verdict leaves only criteria a deploy settles,
// whose CI passed at its head, and that is no ui story awaiting its
// click-proofs, is refused with the precondition naming the criterion.
func TestMergeAndProve_Refusals(t *testing.T) {
	t.Parallel()
	f := newProveFixture(t)
	f.addStory(t, 3, `[{"criterion":"the route answers","verdict":"not-met","evidence":"404"},`+
		`{"criterion":"p1 serves it","verdict":"live-proof","reason":"after-deploy"}]`, CriteriaOutcomeRefused)
	f.addStory(t, 4, `[{"criterion":"the VM boots nested","verdict":"live-proof","reason":"needs kvm"}]`, CriteriaOutcomeLiveProof)
	f.addStory(t, 5, `[{"criterion":"it builds","verdict":"met","evidence":"make test"}]`, CriteriaOutcomeMet)
	f.addStory(t, 6, afterDeployCriteria, CriteriaOutcomeLiveProof, config.ReviewUILabel)
	f.addStory(t, 7, afterDeployCriteria, CriteriaOutcomeLiveProof)
	f.forge.setCI(7, "failure")

	sup := f.o.Supervisor()
	for _, tt := range []struct {
		identifier, code, names string
	}{
		{"ONS-3", "criteria_not_met", "criterion 1"},
		{"ONS-4", "live_proof_not_after_deploy", "the VM boots nested"},
		{"ONS-5", "no_after_deploy_criterion", "merge"},
		{"ONS-6", "click_proofs_missing", "sortie:click-proofs passed"},
		{"ONS-7", "ci_not_passed", "failure"},
	} {
		got := sup.MergeAndProve(t.Context(), tt.identifier)
		if got.Status != SupervisorPrecondition || got.Code != tt.code || !strings.Contains(got.Message, tt.names) {
			t.Errorf("merge-and-prove of %s = %+v, want %s naming %q", tt.identifier, got, tt.code, tt.names)
		}
	}
	f.forge.comment(6, "c-6", ClickProofsMarker+" "+headSHA(6))
	if got := sup.MergeAndProve(t.Context(), "ONS-6"); got.Status != SupervisorDone {
		t.Errorf("ui story with its click-proofs at the head = %+v, want accepted", got)
	}
	f.accept(t, "ONS-1")
	if got := sup.MergeAndProve(t.Context(), "ONS-1"); got.Code != "already_queued" {
		t.Errorf("second merge-and-prove of ONS-1 = %+v, want already_queued", got)
	}
	if got := sup.MergeAndProve(t.Context(), "ONS-404"); got.Status != SupervisorUnknownIssue {
		t.Errorf("unknown story = %+v, want unknown", got)
	}
	if rows, _ := f.store.LoadMergeAndProve(t.Context()); len(rows) != 2 {
		t.Errorf("stored = %+v, want only ONS-6 and ONS-1", rows)
	}
	if merged := f.forge.mergedPulls(); len(merged) != 0 {
		t.Errorf("merged = %v, want nothing merged by a refusal or an acceptance", merged)
	}
}

// A restart in each phase goes on from that phase: a queued story stays
// queued, a merge the forge made before the record moved on is taken, the
// deploy wait reads the hosts anew, the live-proof job is scheduled again,
// and a revert opened before the restart is merged rather than opened
// again.
func TestMergeAndProve_RestartInEachPhase(t *testing.T) {
	t.Parallel()
	f := newProveFixture(t)
	f.accept(t, "ONS-1")
	f.accept(t, "ONS-2")

	// Queued.
	f.restart(t)
	if snap := f.snapshot(t); snap.InFlight != nil || len(snap.Queue) != 2 || snap.Queue[0].Identifier != "ONS-1" {
		t.Fatalf("restored queue = %+v, want both queued in order", snap)
	}

	// Merging: the forge merged, the process stopped before the record
	// moved on.
	onLoop(t, f.o, func() {
		f.o.setMergeAndProvePhase(t.Context(), f.o.state.MergeAndProve["id-1"], MergeAndProveMerging, f.t0, time.Time{})
	})
	if _, err := f.forge.MergePR(t.Context(), 1, "o", "kit", domain.StrategySquash, "", "", headSHA(1)); err != nil {
		t.Fatal(err)
	}
	f.restart(t)
	if got := f.snapshot(t).InFlight; got == nil || got.Phase != MergeAndProveMerging {
		t.Fatalf("restored in flight = %+v, want merging", got)
	}
	f.tick(t, f.t0)
	if got := f.snapshot(t).InFlight; got.Phase != MergeAndProveDeploying || got.MergeCommit != mergeSHA(1) {
		t.Fatalf("after the restart in merging = %+v, want deploying with the forge's merge", got)
	}
	if merged := f.forge.mergedPulls(); len(merged) != 1 {
		t.Errorf("merges = %v, want the one before the restart only", merged)
	}

	// Deploying.
	f.restart(t)
	f.forge.holds("new-main")
	f.deploy(t, "p1", "new-main")
	f.deploy(t, "taichi", "new-main")
	f.tick(t, f.t0.Add(5*time.Minute))
	if got := f.snapshot(t).InFlight; got.Phase != MergeAndProveProving || !got.Deadline.Equal(f.t0.Add(35*time.Minute)) {
		t.Fatalf("after the restart in deploying = %+v, want proving until t0+35m", got)
	}

	// Proving: the job was runtime-only, so it is scheduled again.
	f.restart(t)
	if got := f.snapshot(t).InFlight; got.Phase != MergeAndProveProving || len(got.Deployed) != 2 {
		t.Fatalf("restored in flight = %+v, want proving with both hosts", got)
	}
	f.tick(t, f.t0.Add(10*time.Minute))
	if !liveProofScheduled(f.o.state, "id-1") {
		t.Error("live-proof job not scheduled again after a restart in proving")
	}

	// Reverting: the revert opened before the restart is the one merged.
	f.tick(t, f.t0.Add(35*time.Minute))
	if got := f.snapshot(t).InFlight; got.Phase != MergeAndProveReverting {
		t.Fatalf("at the deadline = %+v, want reverting", got)
	}
	f.restart(t)
	f.forge.setCI(101, "success")
	f.tick(t, f.t0.Add(40*time.Minute))
	if opened := f.forge.revertsOpened(); !slices.Equal(opened, []int{1}) {
		t.Errorf("reverts opened = %v, want one", opened)
	}
	if state := f.stories.stateOf("id-1"); state != "Todo" {
		t.Errorf("ONS-1 after the restart in reverting = %q, want Todo", state)
	}
	if got := f.snapshot(t).InFlight; got == nil || got.Identifier != "ONS-2" {
		t.Errorf("in flight after ONS-1's revert = %+v, want ONS-2", got)
	}
}

// Scenario "A hand revert": a person merges a revert of a story's pull
// request, as ao#144 reverted ao#142 for ONS-121. Within one sync the
// story no longer reads Done, its merge record is gone, and its dependents
// wait for it again.
func TestStoryRevert_HandRevert(t *testing.T) {
	t.Parallel()
	f := newProveFixture(t)
	f.stories.add("id-121", "ONS-121", "Done")
	if err := f.store.UpsertPlanSatisfaction(t.Context(), persistence.PlanSatisfaction{
		IssueID: "id-121", Kind: persistence.PlanSatisfactionMerged, Ref: mergeSHA(142), PR: "o/kit#142", RecordedAt: f.t0,
	}); err != nil {
		t.Fatal(err)
	}
	dependent := domain.BlockerRef{ID: "id-121", Identifier: "ONS-121", State: "Done", PlanKey: "kit-121"}
	judge := func() string {
		cfg := f.o.workflowManager.Config()
		j := NewPlanJudge(t.Context(), f.store, cfg.Tracker.CancelState, cfg.Tracker.DoneState, discardLogger())
		dependent.State = f.stories.stateOf("id-121")
		return j.Unsatisfied(dependent, stateSet(cfg.Tracker.TerminalStates))
	}
	if why := judge(); why != "" {
		t.Fatalf("dependent before the revert = %q, want it satisfied", why)
	}

	f.tick(t, f.t0)
	if f.stories.stateOf("id-121") != "Done" {
		t.Fatal("a story with no revert left Done")
	}

	f.forge.mu.Lock()
	f.forge.merged = []domain.MergedRevert{
		{Number: 143, Commits: []string{"1111111"}},
		{Number: 144, MergedAt: f.t0.Add(time.Hour), PullRequests: []int{142}},
	}
	f.forge.mu.Unlock()
	f.tick(t, f.t0.Add(time.Hour))

	if state := f.stories.stateOf("id-121"); state != "Todo" {
		t.Errorf("ONS-121 after the hand revert = %q, want Todo", state)
	}
	if _, found := f.planMerge(t, "id-121"); found {
		t.Error("ONS-121 keeps its merge record after the revert")
	}
	if why := judge(); why == "" {
		t.Error("ONS-121's dependent is not blocked again")
	}
	if c := f.stories.lastComment("id-121"); !strings.Contains(c, "o/kit#144") || !strings.Contains(c, "o/kit#142") {
		t.Errorf("comment = %q, want the revert and the reverted pull request", c)
	}
	sent := f.spy.notifications()
	if len(sent) != 1 || sent[0].Message.Facts.(domain.StoryRevertedFacts).BySortie {
		t.Errorf("notifications = %+v, want one story.reverted merged by hand", sent)
	}

	// The next sync changes nothing more.
	f.tick(t, f.t0.Add(2*time.Hour))
	if moves := len(f.stories.moves); moves != 1 {
		t.Errorf("moves = %d, want the one move", moves)
	}
}

// A hand revert by commit of a story merge-and-prove has merged and not
// yet proven ends its run as reverted, and the next story starts.
func TestStoryRevert_HandRevertInFlight(t *testing.T) {
	t.Parallel()
	f := newProveFixture(t)
	f.accept(t, "ONS-1")
	f.tick(t, f.t0)
	f.forge.mu.Lock()
	f.forge.merged = []domain.MergedRevert{{Number: 150, Commits: []string{mergeSHA(1)[:10]}}}
	f.forge.mu.Unlock()
	f.tick(t, f.t0.Add(time.Minute))
	if state := f.stories.stateOf("id-1"); state != "Todo" {
		t.Errorf("ONS-1 = %q, want Todo", state)
	}
	if snap := f.snapshot(t); snap.InFlight != nil {
		t.Errorf("in flight = %+v, want none", snap.InFlight)
	}
	if opened := f.forge.revertsOpened(); len(opened) != 0 {
		t.Errorf("reverts opened = %v, want none: a person reverted it", opened)
	}
}

// The merge-completion reaction leaves a story merge-and-prove merged to
// merge-and-prove, which ends it done only once proven.
func TestMergeAndProve_HoldsMergeCompletion(t *testing.T) {
	t.Parallel()
	state := NewState(5000, 4, 0, nil, AgentTotals{})
	state.MergeAndProve["id-1"] = &MergeAndProveEntry{IssueID: "id-1", Phase: MergeAndProveQueued}
	if mergeAndProveHolds(state, "id-1") {
		t.Error("a queued story holds merge completion")
	}
	state.MergeAndProve["id-1"].Phase = MergeAndProveDeploying
	if !mergeAndProveHolds(state, "id-1") || mergeAndProveHolds(state, "id-2") {
		t.Error("a merged story in flight does not hold merge completion, or another story does")
	}
}
