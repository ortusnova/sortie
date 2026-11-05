package orchestrator

import (
	"strings"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// openRevertQuestion returns a copy of the open revert question that
// holds issueID, nil when none does.
func openRevertQuestion(t *testing.T, f *proveFixture, issueID string) *SnapshotPlatformFault {
	t.Helper()
	var snap *SnapshotPlatformFault
	onLoop(t, f.o, func() {
		if q := revertQuestionOf(f.o.state, issueID); q != nil {
			s := snapshotPlatformFault(q)
			snap = &s
		}
	})
	return snap
}

// faultHeld reports whether an open platform fault holds issueID out of
// dispatch.
func faultHeld(t *testing.T, f *proveFixture, issueID string) bool {
	t.Helper()
	var held bool
	onLoop(t, f.o, func() { _, held = f.o.state.FaultHolds[issueID] })
	return held
}

const (
	ao42     = "ortusnova/agentic-orchestration#57"
	ao42Hand = "ortusnova/agentic-orchestration#65"
)

// newONS42Fixture is a prove fixture with ONS-42 approved and Done, its
// merge of ortusnova/agentic-orchestration#57 recorded, and a person's
// revert of it, ortusnova/agentic-orchestration#65, merged on the forge.
func newONS42Fixture(t *testing.T) *proveFixture {
	t.Helper()
	f := newProveFixture(t)
	f.stories.add("id-42", "ONS-42", "Done")
	if err := f.store.UpsertPlanSatisfaction(t.Context(), persistence.PlanSatisfaction{
		IssueID: "id-42", Kind: persistence.PlanSatisfactionMerged, Ref: mergeSHA(57), PR: ao42, RecordedAt: f.t0,
	}); err != nil {
		t.Fatal(err)
	}
	f.forge.mu.Lock()
	f.forge.merged = []domain.MergedRevert{{Number: 65, MergedAt: f.t0.Add(time.Hour), PullRequests: []int{57}}}
	f.forge.mu.Unlock()
	return f
}

// answer answers the open revert question of issueID as by.
func answer(t *testing.T, f *proveFixture, issueID, by, choice string) PlatformFaultDoneResult {
	t.Helper()
	q := openRevertQuestion(t, f, issueID)
	if q == nil {
		t.Fatalf("no open revert question holds %s", issueID)
	}
	return f.o.PlatformFaultAnswerFunc()(t.Context(), q.ID, by, choice)
}

// Scenario "A rejected story reverted by hand": ONS-42's merge,
// ortusnova/agentic-orchestration#57, was reverted by a person with
// ortusnova/agentic-orchestration#65. The story
// leaves Done, is parked with the question "re-queue, or cancel?" naming
// both pull requests, and is never re-released: no run starts, and the
// supervisor can neither rebuild, merge nor release it.
func TestRevertQuestion_HandRevertParksTheStory(t *testing.T) {
	t.Parallel()
	f := newONS42Fixture(t)
	f.tick(t, f.t0.Add(time.Hour))

	if state := f.stories.stateOf("id-42"); state != "In Review" {
		t.Errorf("ONS-42 after the hand revert = %q, want In Review: out of Done, never Todo", state)
	}
	for _, move := range f.stories.moves {
		if strings.HasSuffix(move, " Todo") {
			t.Errorf("moves = %v, want ONS-42 never moved to tracker.release_state", f.stories.moves)
		}
	}
	if _, found := f.planMerge(t, "id-42"); found {
		t.Error("ONS-42 keeps its merge record after the revert")
	}
	q := openRevertQuestion(t, f, "id-42")
	if q == nil {
		t.Fatal("ONS-42 is not parked with a question")
	}
	if q.QuestionKind != "decision" || q.Class != PlatformFaultClassRevertQuestion || q.RaisedBy != QuestionRaisedBySortie {
		t.Errorf("question = %+v, want a decision Sortie raised", q)
	}
	if q.Action == nil || !strings.Contains(q.Action.Action, "re-queue, or cancel?") ||
		!strings.Contains(q.Action.Action, ao42) || !strings.Contains(q.Action.Action, ao42Hand) {
		t.Errorf("question action = %+v, want re-queue, or cancel? naming %s and %s", q.Action, ao42, ao42Hand)
	}
	if !faultHeld(t, f, "id-42") {
		t.Error("ONS-42 is not held out of dispatch")
	}
	c := f.stories.lastComment("id-42")
	if !strings.Contains(c, ao42) || !strings.Contains(c, ao42Hand) || !strings.Contains(c, q.ID) {
		t.Errorf("comment = %q, want both pull requests and the question", c)
	}
	sent := f.spy.notifications()
	if len(sent) != 1 {
		t.Fatalf("notifications = %+v, want one story.reverted", sent)
	}
	if facts := sent[0].Message.Facts.(domain.StoryRevertedFacts); facts.BySortie || facts.Question != q.ID || facts.RevertPullRequest != ao42Hand {
		t.Errorf("facts = %+v, want a hand revert waiting on question %s", facts, q.ID)
	}

	// A person moving it back to Todo by hand still starts no run while
	// the question is open, and the periodic check does not clear it.
	f.stories.add("id-42", "ONS-42", "Todo")
	cfg := f.o.workflowManager.Config()
	issue := domain.Issue{ID: "id-42", Identifier: "ONS-42", Title: "ONS-42", State: "Todo"}
	var dispatchable bool
	onLoop(t, f.o, func() {
		dispatchable = ShouldDispatch(issue, f.o.state, cfg.Tracker.ActiveStates, cfg.Tracker.TerminalStates)
	})
	if dispatchable {
		t.Error("ONS-42 is dispatchable while its question is open")
	}
	if err := f.o.runFaultCheck(t.Context(), PlatformFaultCheck{FaultID: q.ID, Class: q.Class, PersonAction: true}); err == nil {
		t.Error("the check of an unanswered question passed")
	}

	sup := f.o.Supervisor()
	for name, got := range map[string]SupervisorResult{
		"fix run":         sup.FixRun(t.Context(), "ONS-42", "rebuild it", ""),
		"release":         sup.ReleasePark(t.Context(), "ONS-42"),
		"merge":           sup.Merge(t.Context(), "ONS-42"),
		"merge and prove": sup.MergeAndProve(t.Context(), "ONS-42"),
		"live proof":      sup.RequestLiveProof(t.Context(), "ONS-42", "p1 serves it", ""),
		"merge record":    sup.RecordMerge(t.Context(), "ONS-42", "ortusnova/agentic-orchestration#158", mergeSHA(158)),
	} {
		if got.Status != SupervisorPrecondition || got.Code != "revert_question" {
			t.Errorf("supervisor %s while the question is open = %+v, want refused revert_question", name, got)
		}
	}

	// The next scans change nothing more.
	f.tick(t, f.t0.Add(2*time.Hour))
	if moves := len(f.stories.moves); moves != 1 {
		t.Errorf("moves = %v, want the one move out of Done", f.stories.moves)
	}
	var open int
	onLoop(t, f.o, func() { open = len(f.o.state.PlatformFaults) })
	if open != 1 {
		t.Errorf("open faults = %d, want the one question", open)
	}
}

// Scenario "A revert after a failed proof", by the scan: merge-and-prove's
// own revert, merged on the forge while it waits for the revert's CI,
// returns the story to Todo with its reason and asks nothing.
func TestRevertQuestion_SortiesOwnRevertReleases(t *testing.T) {
	t.Parallel()
	f := newProveFixture(t)
	f.accept(t, "ONS-1")
	f.tick(t, f.t0)
	f.forge.holds("new-main")
	f.deploy(t, "p1", "new-main")
	f.deploy(t, "taichi", "new-main")
	f.tick(t, f.t0.Add(4*time.Minute))
	f.tick(t, f.t0.Add(34*time.Minute))
	if got := f.snapshot(t).InFlight; got == nil || got.Phase != MergeAndProveReverting || got.RevertPR != "o/kit#101" {
		t.Fatalf("at the deadline = %+v, want reverting with o/kit#101", got)
	}
	f.forge.mu.Lock()
	f.forge.merged = []domain.MergedRevert{{Number: 101, PullRequests: []int{1}}}
	f.forge.mu.Unlock()
	f.tick(t, f.t0.Add(34*time.Minute+config.DefaultRevertScanInterval))

	if state := f.stories.stateOf("id-1"); state != "Todo" {
		t.Errorf("ONS-1 after Sortie's revert = %q, want Todo", state)
	}
	if q := openRevertQuestion(t, f, "id-1"); q != nil || faultHeld(t, f, "id-1") {
		t.Errorf("Sortie's own revert parked the story with %+v, want no question", q)
	}
	sent := f.spy.notifications()
	if len(sent) != 1 {
		t.Fatalf("notifications = %+v, want one story.reverted", sent)
	}
	if facts := sent[0].Message.Facts.(domain.StoryRevertedFacts); !facts.BySortie || facts.State != "Todo" || facts.Reason == "" || facts.Question != "" {
		t.Errorf("facts = %+v, want Sortie's revert back to Todo with the reason", facts)
	}
}

// Scenario: answering re-queue returns the story to tracker.release_state,
// ends the park, and records who answered.
func TestRevertQuestion_RequeueReleases(t *testing.T) {
	t.Parallel()
	f := newONS42Fixture(t)
	f.tick(t, f.t0.Add(time.Hour))

	got := answer(t, f, "id-42", "taichi", "re-queue")
	if got.Status != PlatformFaultDoneChecked || !got.Resolved {
		t.Fatalf("answer re-queue = %+v, want checked and resolved", got)
	}
	if got.Fault.Answer != RevertAnswerRequeue || got.Fault.ActionDoneBy != "taichi" {
		t.Errorf("answered question = %+v, want requeue by taichi", got.Fault)
	}
	if state := f.stories.stateOf("id-42"); state != "Todo" {
		t.Errorf("ONS-42 after re-queue = %q, want Todo", state)
	}
	if faultHeld(t, f, "id-42") || openRevertQuestion(t, f, "id-42") != nil {
		t.Error("ONS-42 is still held after the answer")
	}
	if c := f.stories.lastComment("id-42"); !strings.Contains(c, "taichi answered requeue") || !strings.Contains(c, "Todo") {
		t.Errorf("comment = %q, want who answered and the move", c)
	}
}

// Scenario "A cancelled story": answering cancel moves the story to
// tracker.cancel_state, and no later revert, sync or supervisor action
// brings it back.
func TestRevertQuestion_CancelIsFinal(t *testing.T) {
	t.Parallel()
	f := newONS42Fixture(t)
	f.tick(t, f.t0.Add(time.Hour))

	got := answer(t, f, "id-42", "taichi", "cancel")
	if got.Status != PlatformFaultDoneChecked || !got.Resolved || got.Fault.Answer != RevertAnswerCancel || got.Fault.ActionDoneBy != "taichi" {
		t.Fatalf("answer cancel = %+v, want resolved, cancel by taichi", got)
	}
	if state := f.stories.stateOf("id-42"); state != "Canceled" {
		t.Fatalf("ONS-42 after cancel = %q, want Canceled", state)
	}
	cfg := f.o.workflowManager.Config()
	moves := len(f.stories.moves)

	// A later revert of a merge of it: the record goes, the story stays
	// cancelled, and nothing is asked.
	if err := f.store.UpsertPlanSatisfaction(t.Context(), persistence.PlanSatisfaction{
		IssueID: "id-42", Kind: persistence.PlanSatisfactionMerged, Ref: mergeSHA(158), PR: "ortusnova/agentic-orchestration#158", RecordedAt: f.t0.Add(2 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	f.forge.mu.Lock()
	f.forge.merged = append(f.forge.merged, domain.MergedRevert{Number: 160, MergedAt: f.t0.Add(3 * time.Hour), PullRequests: []int{158}})
	f.forge.mu.Unlock()
	f.tick(t, f.t0.Add(3*time.Hour))
	if state := f.stories.stateOf("id-42"); state != "Canceled" {
		t.Errorf("ONS-42 after a later revert = %q, want Canceled", state)
	}
	if q := openRevertQuestion(t, f, "id-42"); q != nil {
		t.Errorf("a later revert of a cancelled story asked %+v, want nothing", q)
	}
	if c := f.stories.lastComment("id-42"); !strings.Contains(c, "cancelled") {
		t.Errorf("comment = %q, want the story left cancelled", c)
	}

	// Merge-and-prove's release of a reverted story skips it.
	if moved, err := releaseStoryOnTracker(t.Context(), cfg, f.stories, "id-42", true); err != nil || moved != "" {
		t.Errorf("releaseStoryOnTracker(cancelled) = %q, %v, want it left", moved, err)
	}

	// Plan control: pull-back refuses it, cancel leaves it.
	target := planTarget{IssueID: "id-42", Identifier: "ONS-42", State: "Canceled"}
	var pullBack, cancel ControlReply
	onLoop(t, f.o, func() {
		pullBack = applyPullBack(t.Context(), f.o.state, target, planParams(f.stories))
		cancel = applyCancel(t.Context(), f.o.state, target, planParams(f.stories))
	})
	if pullBack.Outcome == ControlDone || !strings.Contains(pullBack.Message, ReasonCancelled) {
		t.Errorf("pull-back of a cancelled story = %+v, want refused as cancelled", pullBack)
	}
	if cancel.Outcome != ControlDone || !strings.Contains(cancel.Message, "cancelled") {
		t.Errorf("cancel of a cancelled story = %+v, want already cancelled", cancel)
	}

	// The supervisor's routes refuse it with the reason cancelled.
	f.addStory(t, 42, afterDeployCriteria, CriteriaOutcomeLiveProof)
	f.stories.add("id-42", "ONS-42", "Canceled")
	sup := f.o.Supervisor()
	for name, r := range map[string]SupervisorResult{
		"fix run":         sup.FixRun(t.Context(), "ONS-42", "rebuild it", ""),
		"merge":           sup.Merge(t.Context(), "ONS-42"),
		"merge and prove": sup.MergeAndProve(t.Context(), "ONS-42"),
		"live proof":      sup.RequestLiveProof(t.Context(), "ONS-42", "p1 serves it", ""),
		"merge record":    sup.RecordMerge(t.Context(), "ONS-42", "o/kit#42", mergeSHA(42)),
	} {
		if r.Status != SupervisorPrecondition || r.Code != ReasonCancelled {
			t.Errorf("supervisor %s of a cancelled story = %+v, want refused cancelled", name, r)
		}
	}
	if len(f.stories.moves) != moves {
		t.Errorf("moves after the cancel = %v, want none", f.stories.moves[moves:])
	}
}

// Merge-and-prove does not merge a story cancelled while it waited in
// the queue.
func TestRevertQuestion_MergeAndProveSkipsACancelledStory(t *testing.T) {
	t.Parallel()
	f := newProveFixture(t)
	f.accept(t, "ONS-1")
	f.stories.add("id-1", "ONS-1", "Canceled")
	f.tick(t, f.t0)
	if merged := f.forge.mergedPulls(); len(merged) != 0 {
		t.Errorf("merged = %v, want a cancelled story left unmerged", merged)
	}
	if state := f.stories.stateOf("id-1"); state != "Canceled" {
		t.Errorf("ONS-1 = %q, want Canceled", state)
	}
	if c := f.stories.lastComment("id-1"); !strings.Contains(c, ReasonCancelled) {
		t.Errorf("comment = %q, want the reason cancelled", c)
	}
}

// Neither the supervisor nor a malformed answer clears the question: a
// withdrawal is forbidden, and an answer that is not re-queue or cancel is
// refused with nothing changed.
func TestRevertQuestion_OnlyAPersonAnswers(t *testing.T) {
	t.Parallel()
	f := newONS42Fixture(t)
	f.tick(t, f.t0.Add(time.Hour))
	q := openRevertQuestion(t, f, "id-42")
	if q == nil {
		t.Fatal("ONS-42 is not parked with a question")
	}

	withdrawn := f.o.Supervisor().WithdrawQuestion(t.Context(), SupervisorWithdrawal{QuestionID: q.ID, Evidence: "the story was rejected"})
	if withdrawn.Status != SupervisorForbidden {
		t.Errorf("supervisor withdrawal = %+v, want forbidden", withdrawn.SupervisorResult)
	}
	for _, choice := range []string{"", "maybe"} {
		if got := f.o.PlatformFaultAnswerFunc()(t.Context(), q.ID, "taichi", choice); got.Status != PlatformFaultDoneInvalid || !strings.Contains(got.Message, "re-queue, or cancel?") {
			t.Errorf("answer %q = %+v, want refused as invalid", choice, got)
		}
	}
	if after := openRevertQuestion(t, f, "id-42"); after == nil || after.Answer != "" || !after.ActionDoneAt.IsZero() {
		t.Errorf("question after the refusals = %+v, want it open and unanswered", after)
	}
	if !faultHeld(t, f, "id-42") || f.stories.stateOf("id-42") != "In Review" {
		t.Error("a refused answer moved or released ONS-42")
	}
}

// A restart with the question open restores it and its hold from the
// store, and the answer after the restart is carried out.
func TestRevertQuestion_SurvivesARestart(t *testing.T) {
	t.Parallel()
	f := newONS42Fixture(t)
	f.tick(t, f.t0.Add(time.Hour))
	before := openRevertQuestion(t, f, "id-42")
	if before == nil {
		t.Fatal("ONS-42 is not parked with a question")
	}

	rows, err := f.store.LoadOpenPlatformFaults(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	onLoop(t, f.o, func() {
		f.o.state = NewState(5000, 4, 0, nil, AgentTotals{})
		PopulatePlatformFaults(f.o.state, rows, discardLogger())
	})
	after := openRevertQuestion(t, f, "id-42")
	if after == nil || after.ID != before.ID || after.QuestionKind != "decision" || after.Action == nil || after.Action.Action != before.Action.Action {
		t.Fatalf("question after the restart = %+v, want %+v", after, before)
	}
	if !faultHeld(t, f, "id-42") {
		t.Error("ONS-42 is not held after the restart")
	}
	f.tick(t, f.t0.Add(2*time.Hour))
	if state := f.stories.stateOf("id-42"); state != "In Review" {
		t.Errorf("ONS-42 after a restart and a scan = %q, want In Review", state)
	}

	if got := answer(t, f, "id-42", "taichi", "requeue"); !got.Resolved {
		t.Fatalf("answer after the restart = %+v, want resolved", got)
	}
	if state := f.stories.stateOf("id-42"); state != "Todo" {
		t.Errorf("ONS-42 after the answer = %q, want Todo", state)
	}
}
