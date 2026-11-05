package orchestrator

import (
	"context"
	"testing"
	"time"
)

// Scenario "A question the supervisor settled itself": the supervisor
// raised a question, did its action through its own routes, and marks the
// action done; Sortie checks the fault at once, and the check releases the
// story.
func TestScenario_AQuestionTheSupervisorSettledItself(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, stateWithParked("id-77", "ONS-77"))
	asked := h.o.Supervisor().AskPerson(t.Context(), liveProofQuestion())
	if asked.Status != SupervisorDone || asked.Fault.RaisedBy != QuestionRaisedBySupervisor {
		t.Fatalf("question = %+v, want one the supervisor raised", asked)
	}

	got := h.o.PlatformFaultDoneFunc()(t.Context(), asked.Fault.ID, "supervisor", QuestionRaisedBySupervisor)
	if got.Status != PlatformFaultDoneChecked || !got.Resolved {
		t.Fatalf("action done = %+v, want the fault checked at once and resolved", got)
	}
	if got.Fault.ActionDoneBy != "supervisor" || got.Fault.ActionDoneAt.IsZero() {
		t.Errorf("fault = %+v, want its action done by the supervisor", got.Fault)
	}
	h.onLoop(t, func(context.Context) {
		if _, open := h.o.state.PlatformFaults[asked.Fault.ID]; open {
			t.Error("the question is still open after its check passed")
		}
		if held, ok := h.o.state.FaultHolds["id-77"]; ok {
			t.Errorf("ONS-77 is still held by %s", held)
		}
	})
}

// Scenario "A fault Sortie raised": the supervisor marks done the action
// of a fault a failed run raised, or a question a person raised; it is
// refused, nothing changes, and the fault waits for a person, whose mark
// still works.
func TestScenario_AFaultSortieRaised(t *testing.T) {
	t.Parallel()
	h := newSupervisorHarness(t, stateWithParkedStories(map[string]string{"id-77": "ONS-77", "id-49": "ONS-49"}))

	persons := liveProofQuestion()
	persons.RaisedBy = QuestionRaisedByOperator
	byPerson := h.o.Supervisor().AskPerson(t.Context(), persons)
	if byPerson.Status != SupervisorDone {
		t.Fatalf("person's question = %+v", byPerson)
	}
	runFault := &PlatformFault{
		ID: "pf-run", Signature: "export | git_push | refused", Class: "token_scope", Step: "export",
		RecordedAt: time.Now(), NextCheckAt: time.Now().Add(time.Hour),
	}
	h.onLoop(t, func(ctx context.Context) {
		h.o.state.PlatformFaults[runFault.ID] = runFault
		holdUnderPlatformFault(ctx, h.o.state, h.o.store, runFault, PlatformFaultHold{IssueID: "id-49", Identifier: "ONS-49", HeldAt: time.Now()}, discardLogger())
	})

	done := h.o.PlatformFaultDoneFunc()
	for name, id := range map[string]string{"a failed run's fault": runFault.ID, "a person's question": byPerson.Fault.ID} {
		got := done(t.Context(), id, "supervisor", QuestionRaisedBySupervisor)
		if got.Status != PlatformFaultDoneNotRaisedBy || got.Fault.ID != id || len(got.Fault.Holds) != 1 {
			t.Errorf("%s: %+v, want it refused with the fault and its story", name, got)
		}
	}
	if got := done(t.Context(), "pf-404", "supervisor", QuestionRaisedBySupervisor); got.Status != PlatformFaultDoneNotFound {
		t.Errorf("unknown fault: %+v, want not found", got)
	}
	h.onLoop(t, func(context.Context) {
		for _, id := range []string{runFault.ID, byPerson.Fault.ID} {
			f := h.o.state.PlatformFaults[id]
			if f == nil || !f.ActionDoneAt.IsZero() || f.ActionDoneBy != "" || f.checking {
				t.Errorf("fault %s = %+v, want it open and untouched", id, f)
			}
		}
	})

	if got := done(t.Context(), runFault.ID, "taichi", ""); got.Status != PlatformFaultDoneChecked || got.Fault.ActionDoneBy != "taichi" {
		t.Errorf("a person's mark = %+v, want it marked done by taichi and checked", got)
	}
}
