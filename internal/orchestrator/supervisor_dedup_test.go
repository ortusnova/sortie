package orchestrator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/persistence"
)

// stateWithParkedStories returns a state in which each identifier, keyed by
// its issue ID, is parked.
func stateWithParkedStories(stories map[string]string) *State {
	s := NewState(5000, 4, 0, nil, AgentTotals{})
	for id, identifier := range stories {
		s.Parked[id] = &ParkedEntry{Identifier: identifier, Reason: parkReasonAgentBlocked, ParkedAt: time.Now().Add(-time.Hour)}
	}
	return s
}

// liveProofQuestion is the supervisor's question that a person prove
// o/r#12 of ONS-77 live.
func liveProofQuestion() SupervisorQuestion {
	return SupervisorQuestion{
		Kind:     "live_proof",
		Action:   "Run the live proof of o/r#12 on the homelab and record it",
		Where:    "the homelab, gitea.homelab.lan",
		Stories:  []string{"ONS-77"},
		Checked:  "the review left criterion 3 to a live proof",
		Evidence: "run 4 of ONS-77 could not reach gitea.homelab.lan",
		RaisedBy: QuestionRaisedBySupervisor,
	}
}

// onLoop runs fn on the harness's event loop.
func (h *supervisorHarness) onLoop(t *testing.T, fn func(ctx context.Context)) {
	t.Helper()
	if _, ok := h.o.Supervisor().onLoop(t.Context(), fn); !ok {
		t.Fatal("the loop did not run the read")
	}
}

// Scenario "A question the forge overtook": a question asks a person to
// prove a pull request that the forge shows merged and reverted; the
// supervisor withdraws it with that evidence, and the story is free for
// its next action.
func TestScenario_AQuestionTheForgeOvertook(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, stateWithParked("id-77", "ONS-77"))
	sup := h.o.Supervisor()

	asked := sup.AskPerson(t.Context(), liveProofQuestion())
	if asked.Status != SupervisorDone || asked.Fault.QuestionKind != "live_proof" || asked.Fault.RaisedBy != QuestionRaisedBySupervisor {
		t.Fatalf("question = %+v, want a live_proof question the supervisor raised", asked)
	}
	polls := h.polls.Load()

	const evidence = "the forge shows o/r#12 merged at abc123 and reverted by o/r#15"
	got := sup.WithdrawQuestion(t.Context(), SupervisorWithdrawal{QuestionID: asked.Fault.ID, Evidence: evidence})
	if got.Status != SupervisorDone || got.Identifier != "ONS-77" || got.Withdrawn.ID != asked.Fault.ID || got.Replacement != nil {
		t.Fatalf("withdrawal = %+v, want the question withdrawn for ONS-77", got)
	}
	h.onLoop(t, func(context.Context) {
		if _, open := h.o.state.PlatformFaults[asked.Fault.ID]; open {
			t.Error("the withdrawn question is still open")
		}
		if held, ok := h.o.state.FaultHolds["id-77"]; ok {
			t.Errorf("ONS-77 is still held by %s", held)
		}
	})
	if h.polls.Load() == polls {
		t.Error("the withdrawal queued no poll for the released story")
	}
	open, err := h.store.LoadOpenPlatformFaults(t.Context())
	if err != nil || len(open) != 0 {
		t.Errorf("stored open faults = %+v, %v, want the question resolved", open, err)
	}

	// The story is free: a new question of the same kind is a new one.
	again := sup.AskPerson(t.Context(), liveProofQuestion())
	if again.Status != SupervisorDone || again.Existing || again.Fault.ID == asked.Fault.ID {
		t.Errorf("question after the withdrawal = %+v, want a new question", again)
	}
}

func TestSupervisor_WithdrawWithAReplacement(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, stateWithParked("id-77", "ONS-77"))
	sup := h.o.Supervisor()
	asked := sup.AskPerson(t.Context(), liveProofQuestion())

	replacement := SupervisorQuestion{
		Kind:     "decision",
		Action:   "Decide whether ONS-77 lands again after the revert",
		Where:    "the workbench, ONS-77",
		Stories:  []string{"ONS-77"},
		Checked:  "o/r#15 reverted o/r#12, so the proof has nothing to prove",
		RaisedBy: QuestionRaisedBySupervisor,
	}
	got := sup.WithdrawQuestion(t.Context(), SupervisorWithdrawal{
		QuestionID: asked.Fault.ID, Evidence: "o/r#12 merged and was reverted", Replacement: &replacement,
	})
	if got.Status != SupervisorDone || got.Replacement == nil || got.Replacement.Status != SupervisorDone || got.Replacement.Existing {
		t.Fatalf("withdrawal = %+v, want the question withdrawn and a new one raised", got)
	}
	r := got.Replacement.Fault
	if r.ID == asked.Fault.ID || r.QuestionKind != "decision" || r.Action == nil || r.Action.Action != replacement.Action || len(r.Holds) != 1 || r.Holds[0].Identifier != "ONS-77" {
		t.Errorf("replacement = %+v, want the decision holding ONS-77", r)
	}
	h.onLoop(t, func(context.Context) {
		if len(h.o.state.PlatformFaults) != 1 || h.o.state.FaultHolds["id-77"] != r.ID {
			t.Errorf("faults = %v, holds = %v, want only the replacement holding ONS-77", h.o.state.PlatformFaults, h.o.state.FaultHolds)
		}
	})

	// A replacement that would be refused leaves the question in place.
	bad := replacement
	bad.Stories = []string{"ONS-404"}
	if got := sup.WithdrawQuestion(t.Context(), SupervisorWithdrawal{QuestionID: r.ID, Evidence: "stale", Replacement: &bad}); got.Status != SupervisorUnknownIssue {
		t.Errorf("withdrawal with an unknown story's replacement = %+v, want unknown issue", got)
	}
	bad.Kind = ""
	if got := sup.WithdrawQuestion(t.Context(), SupervisorWithdrawal{QuestionID: r.ID, Evidence: "stale", Replacement: &bad}); got.Code != "invalid_request" {
		t.Errorf("withdrawal with a replacement of no kind = %+v, want invalid_request", got)
	}
	h.onLoop(t, func(context.Context) {
		if _, open := h.o.state.PlatformFaults[r.ID]; !open || h.o.state.FaultHolds["id-77"] != r.ID {
			t.Error("a refused withdrawal changed the question")
		}
	})
}

// The supervisor withdraws only a question it raised: a person's question,
// a fault a story's agent or Sortie recorded, an unknown ID and a
// withdrawal without evidence are refused, and nothing changes.
func TestSupervisor_RefusedWithdrawal(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, stateWithParkedStories(map[string]string{"id-77": "ONS-77", "id-39": "ONS-39"}))
	sup := h.o.Supervisor()

	persons := liveProofQuestion()
	persons.RaisedBy = QuestionRaisedByOperator
	byPerson := sup.AskPerson(t.Context(), persons)
	var agentFault string
	h.onLoop(t, func(ctx context.Context) {
		f := &PlatformFault{
			ID: "pf-handoff", Signature: "handoff: ONS-39/live-chain", Class: PlatformFaultClassHandoffPerson, Step: "handoff",
			RecordedAt: time.Now(), NextCheckAt: time.Now().Add(time.Hour),
		}
		h.o.state.PlatformFaults[f.ID] = f
		holdUnderPlatformFault(ctx, h.o.state, h.o.store, f, PlatformFaultHold{IssueID: "id-39", Identifier: "ONS-39", HeldAt: time.Now()}, discardLogger())
		agentFault = f.ID
	})

	for name, tt := range map[string]struct {
		w      SupervisorWithdrawal
		status SupervisorStatus
		code   string
	}{
		"a person's question":   {SupervisorWithdrawal{QuestionID: byPerson.Fault.ID, Evidence: "stale"}, SupervisorPrecondition, "not_supervisor_question"},
		"an agent's hand-off":   {SupervisorWithdrawal{QuestionID: agentFault, Evidence: "stale"}, SupervisorPrecondition, "not_supervisor_question"},
		"an unknown question":   {SupervisorWithdrawal{QuestionID: "pf-404", Evidence: "stale"}, SupervisorUnknownIssue, "question_not_found"},
		"a withdrawal unproven": {SupervisorWithdrawal{QuestionID: byPerson.Fault.ID, Evidence: " "}, SupervisorPrecondition, "invalid_request"},
	} {
		if got := sup.WithdrawQuestion(t.Context(), tt.w); got.Status != tt.status || got.Code != tt.code {
			t.Errorf("%s: %+v, want status %d %s", name, got, tt.status, tt.code)
		}
	}
	h.onLoop(t, func(context.Context) {
		if len(h.o.state.PlatformFaults) != 2 || h.o.state.FaultHolds["id-77"] != byPerson.Fault.ID || h.o.state.FaultHolds["id-39"] != agentFault {
			t.Errorf("faults = %v, holds = %v, want both in place", h.o.state.PlatformFaults, h.o.state.FaultHolds)
		}
	})
}

// A question whose story and kind match an open one joins it whatever its
// wording, and the open one keeps the new evidence; ONS-39 asked five
// times is one question. Another kind for the same story is its own
// question.
func TestSupervisor_QuestionsJoinByStoryAndKind(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, stateWithParkedStories(map[string]string{"id-77": "ONS-77", "id-39": "ONS-39"}))
	sup := h.o.Supervisor()

	first := sup.AskPerson(t.Context(), liveProofQuestion())
	reworded := SupervisorQuestion{
		Kind:     "live_proof",
		Action:   "Please prove ONS-77 against the real Gitea",
		Where:    "gitea on the homelab",
		Stories:  []string{"ONS-77"},
		Checked:  "criterion 3 needs the real Gitea API",
		Evidence: "run 5 was refused by egress too",
		RaisedBy: QuestionRaisedBySupervisor,
	}
	second := sup.AskPerson(t.Context(), reworded)
	if second.Status != SupervisorDone || !second.Existing || second.Fault.ID != first.Fault.ID {
		t.Fatalf("reworded question = %+v, want it joined to %s", second, first.Fault.ID)
	}
	for _, want := range []string{first.Fault.Detail, reworded.Checked, reworded.Evidence} {
		if !strings.Contains(second.Fault.Detail, want) {
			t.Errorf("detail = %q, want it to keep %q", second.Fault.Detail, want)
		}
	}
	open, err := h.store.LoadOpenPlatformFaults(t.Context())
	if err != nil || len(open) != 1 || open[0].Detail != second.Fault.Detail || open[0].QuestionKind != "live_proof" {
		t.Errorf("stored = %+v, %v, want the joined question with its evidence", open, err)
	}

	approval := reworded
	approval.Kind = "approval"
	if other := sup.AskPerson(t.Context(), approval); other.Existing || other.Fault.ID == first.Fault.ID {
		t.Errorf("question of another kind = %+v, want its own question", other)
	}

	var ids []string
	for i := range 5 {
		q := SupervisorQuestion{
			Kind:     "decision",
			Action:   "Decide ONS-39, wording " + strings.Repeat("x", i+1),
			Where:    "place " + strings.Repeat("y", i+1),
			Stories:  []string{"ONS-39"},
			Checked:  "check " + strings.Repeat("z", i+1),
			RaisedBy: QuestionRaisedBySupervisor,
		}
		ids = append(ids, sup.AskPerson(t.Context(), q).Fault.ID)
	}
	for _, id := range ids[1:] {
		if id != ids[0] {
			t.Errorf("ONS-39's questions = %v, want one", ids)
			break
		}
	}
	h.onLoop(t, func(context.Context) {
		if n := len(h.o.state.PlatformFaults); n != 3 {
			t.Errorf("open questions = %d, want 3: ONS-77's proof and approval, and ONS-39's decision", n)
		}
	})
}

// Scenario "The same defect three times": the supervisor files the same
// deploy-state defect three times in fifteen minutes with different
// titles; one issue exists, with the three pieces of evidence.
func TestScenario_TheSameDefectThreeTimes(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, NewState(5000, 4, 0, nil, AgentTotals{}))
	sup := h.o.Supervisor()

	filings := []SupervisorIssue{
		{Title: "deploy.json is stale", Kind: "platform_defect", Signature: "health | deploy_state | stale", Evidence: "deploy.json is 61 minutes old at 03:00Z"},
		{Title: "Deploy state not refreshed", Kind: "platform_defect", Signature: "Health | deploy_state  | STALE", Evidence: "still 66 minutes old at 03:05Z"},
		{Title: "stage0 deploy state frozen", Kind: "platform_defect", Signature: "health | deploy_state | stale", Body: "The timer looks dead.", Evidence: "76 minutes old at 03:15Z"},
	}
	var results []SupervisorIssueResult
	for _, f := range filings {
		results = append(results, sup.FileIssue(t.Context(), f))
	}
	if len(h.tracker.created) != 1 {
		t.Fatalf("created = %d issues, want one", len(h.tracker.created))
	}
	for i, r := range results {
		if r.Status != SupervisorDone || r.Identifier != "ONS-200" || r.Existing != (i > 0) {
			t.Errorf("filing %d = %+v, want ONS-200, existing %v", i, r, i > 0)
		}
	}
	all := h.tracker.created[0].Body + "\n" + strings.Join(h.tracker.comments["new-id"], "\n")
	for _, f := range filings {
		if !strings.Contains(all, f.Evidence) {
			t.Errorf("ONS-200 and its comments lack the evidence %q:\n%s", f.Evidence, all)
		}
	}
	if len(h.tracker.comments["new-id"]) != 2 || !strings.Contains(h.tracker.comments["new-id"][1], "stage0 deploy state frozen") {
		t.Errorf("comments = %q, want the two later filings with their titles", h.tracker.comments["new-id"])
	}
}

// An issue joins an open one with the same stories and kind; another kind,
// other stories, or a closed issue files a new one.
func TestSupervisor_IssuesJoinByStoriesAndKind(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, NewState(5000, 4, 0, nil, AgentTotals{}))
	sup := h.o.Supervisor()
	file := func(issue SupervisorIssue) SupervisorIssueResult {
		t.Helper()
		r := sup.FileIssue(t.Context(), issue)
		if r.Status != SupervisorDone {
			t.Fatalf("file %+v = %+v, want done", issue, r)
		}
		return r
	}

	first := file(SupervisorIssue{Title: "Port the seed fix", Kind: "follow_up", Stories: []string{"ONS-77", "ONS-39"}, Evidence: "e1"})
	if again := file(SupervisorIssue{Title: "Seed fix follow-up", Kind: "follow_up", Stories: []string{"ONS-39", " ONS-77", "ONS-77"}, Evidence: "e2"}); !again.Existing || again.Identifier != first.Identifier {
		t.Errorf("same stories and kind = %+v, want it joined to %s", again, first.Identifier)
	}
	if defect := file(SupervisorIssue{Title: "Seed defect", Kind: "platform_defect", Stories: []string{"ONS-77", "ONS-39"}}); defect.Existing {
		t.Errorf("another kind = %+v, want a new issue", defect)
	}
	if fewer := file(SupervisorIssue{Title: "Port the seed fix", Kind: "follow_up", Stories: []string{"ONS-77"}}); fewer.Existing {
		t.Errorf("other stories = %+v, want a new issue", fewer)
	}

	h.tracker.mu.Lock()
	h.tracker.states[first.IssueID] = "Done"
	h.tracker.mu.Unlock()
	if after := file(SupervisorIssue{Title: "Port the seed fix", Kind: "follow_up", Stories: []string{"ONS-39", "ONS-77"}}); after.Existing || after.Identifier == first.Identifier {
		t.Errorf("filing after the issue closed = %+v, want a new issue", after)
	}
	if n := len(h.tracker.created); n != 4 {
		t.Errorf("created = %d issues, want 4", n)
	}
	if c := h.tracker.comments[first.IssueID]; len(c) != 1 || !strings.Contains(c[0], "e2") {
		t.Errorf("comments on %s = %q, want the joined filing's evidence", first.Identifier, c)
	}
}

// A question restored at startup keeps its kind and who raised it, so the
// supervisor can still withdraw it and later questions still join it.
func TestSupervisor_RestoredQuestionKeepsItsKindAndRaiser(t *testing.T) {
	t.Parallel()
	state := stateWithParked("id-77", "ONS-77")
	PopulatePlatformFaults(state, []persistence.PlatformFault{{
		ID: "pf-old", Signature: questionSignature("prove it", "homelab"), Class: PlatformFaultClassSupervisorQuestion, Step: supervisorQuestionStep,
		Action: "prove it", ActionWhere: "homelab", RecordedAt: "2026-11-02T03:31:00Z", QuestionKind: "live_proof", RaisedBy: QuestionRaisedBySupervisor,
		Holds: []persistence.PlatformFaultHold{{IssueID: "id-77", Identifier: "ONS-77", HeldAt: "2026-11-02T03:31:00Z"}},
	}}, discardLogger())
	h := newSupervisorHarness(t, state)
	sup := h.o.Supervisor()

	if joined := sup.AskPerson(t.Context(), liveProofQuestion()); !joined.Existing || joined.Fault.ID != "pf-old" {
		t.Errorf("question = %+v, want it joined to the restored one", joined)
	}
	if got := sup.WithdrawQuestion(t.Context(), SupervisorWithdrawal{QuestionID: "pf-old", Evidence: "merged and reverted"}); got.Status != SupervisorDone {
		t.Errorf("withdrawal of the restored question = %+v, want done", got)
	}
}
