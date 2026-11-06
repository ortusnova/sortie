package orchestrator

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/notify/render"
	"github.com/sortie-ai/sortie/internal/notify/route"
	"github.com/sortie-ai/sortie/internal/registry"
)

// personEvents are the events meant for a person: what a person's
// notifications destination lists.
func personEvents() []domain.EventType {
	var events []domain.EventType
	for _, t := range domain.EventTypes() {
		if t.ForPerson() {
			events = append(events, t)
		}
	}
	return events
}

// personAndOpsLookup builds the person's destination, kind webhook, on
// person, and every other kind on ops.
func personAndOpsLookup(person, ops *notifierSpy) route.NotifierLookup {
	return func(kind string) (registry.NotifierConstructor, error) {
		spy := ops
		if kind == "webhook" {
			spy = person
		}
		return func(map[string]any) (domain.Notifier, error) { return spy, nil }, nil
	}
}

// personRouter routes the person's events to person, the fake
// notifications destination of a person.
func personRouter(t *testing.T, person *notifierSpy) *route.Router {
	t.Helper()
	return mustRouter(t, nil, spyLookup(person), route.Inputs{Entries: []config.NotificationBackend{subscribe("webhook", personEvents()...)}})
}

// credentialQuestion is the supervisor's question that a person give
// ONS-77's token the workflow scope.
func credentialQuestion() SupervisorQuestion {
	return SupervisorQuestion{
		Kind:     "credential",
		Action:   `Give taichi's GitHub token "Workflows: Read and write"`,
		Where:    "GitHub, Settings > Developer settings > Personal access tokens",
		Stories:  []string{"ONS-77"},
		Checked:  "both pushes of ONS-77 were refused for the workflow scope",
		RaisedBy: QuestionRaisedBySupervisor,
	}
}

// questionHarness is a supervisor harness whose notifications go to a
// fake destination of a person.
func questionHarness(t *testing.T, state *State) (*supervisorHarness, *notifierSpy) {
	t.Helper()
	person := &notifierSpy{}
	h := newSupervisorHarness(t, state)
	h.o.router = personRouter(t, person)
	return h, person
}

// sent waits for every detached delivery of h and returns what person
// received.
func sent(h *supervisorHarness, person *notifierSpy) []domain.Notification {
	h.o.state.TrackerOpsWg.Wait()
	return person.notifications()
}

// questionOf returns the question facts n carries.
func questionOf(t *testing.T, n domain.Notification) domain.QuestionFacts {
	t.Helper()
	q, ok := n.Message.Facts.(domain.QuestionFacts)
	if !ok {
		t.Fatalf("notification %s carries %T, want question facts", n.Envelope.EventType, n.Message.Facts)
	}
	return q
}

// Scenario "A credential question": the supervisor raises a question of
// class credential, and the person is notified of it once.
func TestScenario_ACredentialQuestion(t *testing.T) {
	t.Parallel()
	h, person := questionHarness(t, stateWithParked("id-77", "ONS-77"))

	asked := h.o.Supervisor().AskPerson(t.Context(), credentialQuestion())
	if asked.Status != SupervisorDone || asked.Existing {
		t.Fatalf("question = %+v, want a new question", asked)
	}
	got := sent(h, person)
	if len(got) != 1 || got[0].Envelope.EventType != domain.EventQuestionForPerson {
		t.Fatalf("notifications = %+v, want one question.for_person", got)
	}
	n := got[0]
	q := questionOf(t, n)
	want := domain.QuestionFacts{
		QuestionID: asked.Fault.ID, Kind: "credential", Text: credentialQuestion().Action, Where: credentialQuestion().Where,
		RaisedBy: QuestionRaisedBySupervisor, Stories: []string{"ONS-77"},
	}
	if q.QuestionID != want.QuestionID || q.Kind != want.Kind || q.Text != want.Text || q.Where != want.Where ||
		q.RaisedBy != want.RaisedBy || !slices.Equal(q.Stories, want.Stories) || q.Closed != "" {
		t.Errorf("facts = %+v, want %+v", q, want)
	}
	if n.Envelope.IssueID != "id-77" || n.Envelope.Identifier != "ONS-77" || n.Message.Severity != "warning" {
		t.Errorf("envelope %+v severity %q, want ONS-77 at warning", n.Envelope, n.Message.Severity)
	}
	if body := render.Notification(n).Body; !strings.Contains(body, "A question needs a person (credential): "+want.Text) || !strings.Contains(body, "Question ID: "+asked.Fault.ID) {
		t.Errorf("body = %q, want the question, its kind and its ID", body)
	}
}

// A question that joins an open one publishes nothing; a question of a
// kind the supervisor follows itself is not sent to a person.
func TestQuestionForPerson_OncePerQuestion(t *testing.T) {
	t.Parallel()
	h, person := questionHarness(t, stateWithParkedStories(map[string]string{"id-77": "ONS-77", "id-39": "ONS-39"}))
	sup := h.o.Supervisor()

	first := sup.AskPerson(t.Context(), credentialQuestion())
	reworded := credentialQuestion()
	reworded.Action = "Add the workflow scope to the token"
	reworded.Stories = []string{"ONS-77", "ONS-39"}
	if joined := sup.AskPerson(t.Context(), reworded); !joined.Existing || joined.Fault.ID != first.Fault.ID {
		t.Fatalf("second question = %+v, want it joined to %s", joined, first.Fault.ID)
	}
	same := credentialQuestion()
	same.Stories = []string{"ONS-39"}
	if joined := sup.AskPerson(t.Context(), same); !joined.Existing {
		t.Fatalf("question with the same action and place = %+v, want it joined", joined)
	}
	proof := liveProofQuestion()
	proof.Stories = []string{"ONS-39"}
	if other := sup.AskPerson(t.Context(), proof); other.Existing || other.Status != SupervisorDone {
		t.Fatalf("live-proof question = %+v, want a new question", other)
	}

	got := sent(h, person)
	if len(got) != 1 || questionOf(t, got[0]).QuestionID != first.Fault.ID {
		t.Fatalf("notifications = %+v, want only the first question's question.for_person", got)
	}
}

// Every kind classed for a person is sent; the others are not.
func TestQuestionForPerson_Kinds(t *testing.T) {
	t.Parallel()
	for _, kind := range QuestionKinds {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			h, person := questionHarness(t, stateWithParked("id-77", "ONS-77"))
			q := credentialQuestion()
			q.Kind = kind
			if asked := h.o.Supervisor().AskPerson(t.Context(), q); asked.Status != SupervisorDone {
				t.Fatalf("question = %+v, want done", asked)
			}
			wantSent := slices.Contains([]string{"credential", "key", "approval", "decision"}, kind)
			if got := sent(h, person); (len(got) == 1) != wantSent || len(got) > 1 {
				t.Errorf("notifications = %+v, want sent %v", got, wantSent)
			}
		})
	}
}

// A question closes when a person marks its action done and its check
// passes.
func TestQuestionClosed_Answered(t *testing.T) {
	t.Parallel()
	h, person := questionHarness(t, stateWithParked("id-77", "ONS-77"))
	asked := h.o.Supervisor().AskPerson(t.Context(), credentialQuestion())

	done := h.o.PlatformFaultDoneFunc()(t.Context(), asked.Fault.ID, "taichi", "")
	if done.Status != PlatformFaultDoneChecked || !done.Resolved {
		t.Fatalf("action done = %+v, want the question resolved", done)
	}
	got := sent(h, person)
	if len(got) != 2 || got[1].Envelope.EventType != domain.EventQuestionClosed {
		t.Fatalf("notifications = %+v, want question.for_person then question.closed", got)
	}
	q := questionOf(t, got[1])
	if q.QuestionID != asked.Fault.ID || q.Closed != domain.QuestionClosedAnswered || q.Kind != "credential" || !slices.Equal(q.Stories, []string{"ONS-77"}) {
		t.Errorf("closed facts = %+v, want %s answered for ONS-77", q, asked.Fault.ID)
	}
	if got[1].Message.Severity != "info" || got[1].Envelope.Identifier != "ONS-77" {
		t.Errorf("closed event %+v, want info for ONS-77", got[1])
	}
}

// A question the supervisor withdraws closes; its replacement, a new
// question, is sent.
func TestQuestionClosed_Withdrawn(t *testing.T) {
	t.Parallel()
	h, person := questionHarness(t, stateWithParked("id-77", "ONS-77"))
	sup := h.o.Supervisor()
	asked := sup.AskPerson(t.Context(), credentialQuestion())

	replacement := credentialQuestion()
	replacement.Kind = "decision"
	replacement.Action = "Decide whether ONS-77 may change .github/workflows"
	got := sup.WithdrawQuestion(t.Context(), SupervisorWithdrawal{QuestionID: asked.Fault.ID, Evidence: "the token gained the scope", Replacement: &replacement})
	if got.Status != SupervisorDone || got.Replacement == nil {
		t.Fatalf("withdrawal = %+v, want done with a replacement", got)
	}
	events := sent(h, person)
	if len(events) != 3 {
		t.Fatalf("notifications = %+v, want the question, its close and the replacement", events)
	}
	if closed := questionOf(t, events[1]); events[1].Envelope.EventType != domain.EventQuestionClosed || closed.QuestionID != asked.Fault.ID || closed.Closed != domain.QuestionClosedWithdrawn {
		t.Errorf("second event = %s %+v, want the question closed as withdrawn", events[1].Envelope.EventType, closed)
	}
	if raised := questionOf(t, events[2]); events[2].Envelope.EventType != domain.EventQuestionForPerson || raised.QuestionID != got.Replacement.Fault.ID || raised.Kind != "decision" {
		t.Errorf("third event = %s %+v, want the replacement's question.for_person", events[2].Envelope.EventType, raised)
	}
}

// A question closes once its story leaves the state that raised it, read
// from the poll's candidates or, for a story the candidates omit, from
// the tracker. A story still in that state keeps the question open.
func TestQuestionClosed_StoryLeftTheState(t *testing.T) {
	t.Parallel()
	state := stateWithParkedStories(map[string]string{"id-77": "ONS-77", "id-39": "ONS-39"})
	state.Parked["id-77"].ParkedState = "In Progress"
	h, person := questionHarness(t, state)
	h.tracker.states = map[string]string{"id-39": "Todo"}
	q := credentialQuestion()
	q.Stories = []string{"ONS-77", "ONS-39"}
	asked := h.o.Supervisor().AskPerson(t.Context(), q)

	candidates := []domain.Issue{{ID: "id-77", Identifier: "ONS-77", State: "In Progress"}}
	h.onLoop(t, func(ctx context.Context) { h.o.closeQuestionsOfLeftStories(ctx, candidates) })
	h.onLoop(t, func(context.Context) {
		f := h.o.state.PlatformFaults[asked.Fault.ID]
		if f == nil || len(f.Holds) != 2 || f.Holds[0].State != "In Progress" || f.Holds[1].State != "Todo" {
			t.Fatalf("question = %+v, want it open holding ONS-77 in In Progress and ONS-39 in Todo", f)
		}
	})

	// ONS-77 moves on: its hold ends, and the question still holds ONS-39.
	candidates[0].State = "In Review"
	h.onLoop(t, func(ctx context.Context) { h.o.closeQuestionsOfLeftStories(ctx, candidates) })
	h.onLoop(t, func(context.Context) {
		f := h.o.state.PlatformFaults[asked.Fault.ID]
		if f == nil || len(f.Holds) != 1 || f.Holds[0].Identifier != "ONS-39" {
			t.Fatalf("question = %+v, want it open holding only ONS-39", f)
		}
		if _, held := h.o.state.FaultHolds["id-77"]; held {
			t.Error("ONS-77 is still held by the question")
		}
	})
	if got := sent(h, person); len(got) != 1 {
		t.Fatalf("notifications = %+v, want only the question while a story stays", got)
	}

	// ONS-39 leaves Todo: the question closes.
	h.tracker.mu.Lock()
	h.tracker.states["id-39"] = "Done"
	h.tracker.mu.Unlock()
	h.onLoop(t, func(ctx context.Context) { h.o.closeQuestionsOfLeftStories(ctx, candidates) })
	h.onLoop(t, func(context.Context) {
		if _, open := h.o.state.PlatformFaults[asked.Fault.ID]; open {
			t.Error("the question is still open")
		}
		if len(h.o.state.FaultHolds) != 0 {
			t.Errorf("fault holds = %v, want none", h.o.state.FaultHolds)
		}
	})
	got := sent(h, person)
	if len(got) != 2 || got[1].Envelope.EventType != domain.EventQuestionClosed {
		t.Fatalf("notifications = %+v, want the question and its close", got)
	}
	if closed := questionOf(t, got[1]); closed.Closed != domain.QuestionClosedStateChanged || !slices.Equal(closed.Stories, []string{"ONS-39"}) || got[1].Envelope.Identifier != "ONS-39" {
		t.Errorf("closed facts = %+v, want closed by the state change of ONS-39", closed)
	}
	open, err := h.store.LoadOpenPlatformFaults(t.Context())
	if err != nil || len(open) != 0 {
		t.Errorf("stored open faults = %+v, %v, want the question resolved", open, err)
	}

	// A question of a kind the supervisor follows closes the same way,
	// and no person hears of it.
	proof := liveProofQuestion()
	proofAsked := h.o.Supervisor().AskPerson(t.Context(), proof)
	h.onLoop(t, func(ctx context.Context) {
		h.o.closeQuestionsOfLeftStories(ctx, candidates)
		if _, open := h.o.state.PlatformFaults[proofAsked.Fault.ID]; open {
			t.Error("the live-proof question is still open after its story left its state")
		}
	})
	if got := sent(h, person); len(got) != 2 {
		t.Errorf("notifications = %+v, want nothing for a live-proof question", got)
	}
}

// A restart with a question open sends nothing again: the restored
// question keeps the state that raised it, and its close is sent once.
func TestQuestionForPerson_RestartWithAQuestionOpen(t *testing.T) {
	t.Parallel()
	store := openRepeatedFailureStore(t)
	first := stateWithParked("id-77", "ONS-77")
	first.Parked["id-77"].ParkedState = "In Progress"
	h, person := questionHarness(t, first)
	h.store = store
	h.o.store = store
	asked := h.o.Supervisor().AskPerson(t.Context(), credentialQuestion())
	if got := sent(h, person); len(got) != 1 {
		t.Fatalf("notifications before the restart = %+v, want the question", got)
	}

	rows, err := store.LoadOpenPlatformFaults(t.Context())
	if err != nil || len(rows) != 1 || len(rows[0].Holds) != 1 || rows[0].Holds[0].State != "In Progress" {
		t.Fatalf("stored faults = %+v, %v, want the question holding ONS-77 in In Progress", rows, err)
	}
	restarted := stateWithParked("id-77", "ONS-77")
	PopulatePlatformFaults(restarted, rows, discardLogger())
	h2, person2 := questionHarness(t, restarted)
	h2.store, h2.o.store = store, store

	candidates := []domain.Issue{{ID: "id-77", Identifier: "ONS-77", State: "In Progress"}}
	h2.onLoop(t, func(ctx context.Context) { h2.o.closeQuestionsOfLeftStories(ctx, candidates) })
	if joined := h2.o.Supervisor().AskPerson(t.Context(), credentialQuestion()); !joined.Existing || joined.Fault.ID != asked.Fault.ID {
		t.Fatalf("question after the restart = %+v, want it joined to the restored one", joined)
	}
	if got := sent(h2, person2); len(got) != 0 {
		t.Fatalf("notifications after the restart = %+v, want none", got)
	}

	candidates[0].State = "Done"
	h2.onLoop(t, func(ctx context.Context) { h2.o.closeQuestionsOfLeftStories(ctx, candidates) })
	h2.onLoop(t, func(ctx context.Context) { h2.o.closeQuestionsOfLeftStories(ctx, candidates) })
	got := sent(h2, person2)
	if len(got) != 1 || got[0].Envelope.EventType != domain.EventQuestionClosed || questionOf(t, got[0]).QuestionID != asked.Fault.ID {
		t.Fatalf("notifications = %+v, want the restored question's close once", got)
	}
}

// Scenario "A run that failed": a story's run fails, the supervisor is
// woken, and no notification reaches the person. A run that stops wakes
// it too. Both events are still published to a destination that lists
// them.
func TestScenario_ARunThatFailed(t *testing.T) {
	t.Parallel()
	person, ops := &notifierSpy{}, &notifierSpy{}
	router := mustRouter(t, nil, personAndOpsLookup(person, ops), route.Inputs{Entries: []config.NotificationBackend{
		subscribe("webhook", personEvents()...),
		subscribe("slack", domain.EventSessionStopped, domain.EventSessionFailed),
	}})
	state := NewState(5000, 4, 0, nil, AgentTotals{})
	state.supervisor = newSupervisorState()
	state.supervisor.enabled = true
	store := openRepeatedFailureStore(t)

	endRun(t, state, store, router, WorkerResult{IssueID: "id-8", Identifier: "ONS-8", ExitKind: WorkerExitError, Error: errors.New("run: the disk sang")})
	endRun(t, state, store, router, WorkerResult{
		IssueID: "id-9", Identifier: "ONS-9", ExitKind: WorkerExitNormal, TurnsCompleted: 1,
		SoftStop: true, SoftStopReason: "needs-human-review", ObservedIssueState: "In Progress",
	})
	state.TrackerOpsWg.Wait()

	if got := person.notifications(); len(got) != 0 {
		t.Errorf("person's notifications = %+v, want none", got)
	}
	var events []domain.EventType
	for _, n := range ops.notifications() {
		events = append(events, n.Envelope.EventType)
	}
	slices.Sort(events)
	if !slices.Equal(events, []domain.EventType{domain.EventSessionFailed, domain.EventSessionStopped}) {
		t.Errorf("published events = %v, want session.failed and session.stopped", events)
	}
	if got, want := triggerKinds(state.supervisor.pending), []string{"failure ONS-8", "park ONS-9"}; !slices.Equal(got, want) {
		t.Errorf("supervisor triggers = %v, want %v", got, want)
	}
	if !strings.Contains(state.supervisor.pending[0].Detail, "run failed") || !strings.Contains(state.supervisor.pending[1].Detail, "needs-human-review") {
		t.Errorf("trigger details = %+v, want the failure and the stop", state.supervisor.pending)
	}
}

// A blocked stop that parks keeps the park's own trigger.
func TestRunEndTrigger_ParkKeepsItsTrigger(t *testing.T) {
	t.Parallel()
	state := NewState(5000, 4, 0, nil, AgentTotals{})
	state.supervisor = newSupervisorState()
	state.supervisor.enabled = true
	noteSupervisorTrigger(state, SupervisorTrigger{Kind: SupervisorTriggerPark, IssueID: "id-9", Identifier: "ONS-9", Detail: "parked: agent_blocked"})
	noteRunEndTrigger(state, "id-9", "ONS-9", domain.SessionStoppedFacts{Reason: "blocked"}, baseTime)
	if len(state.supervisor.pending) != 1 || state.supervisor.pending[0].Detail != "parked: agent_blocked" {
		t.Errorf("triggers = %+v, want the park's own trigger", state.supervisor.pending)
	}
	noteRunEndTrigger(state, "id-9", "ONS-9", domain.SessionCompletedFacts{}, baseTime)
	if len(state.supervisor.pending) != 1 {
		t.Errorf("a completed run added a trigger: %+v", state.supervisor.pending)
	}
}

// endRun ends the running run of result's issue through the worker exit.
func endRun(t *testing.T, state *State, store WorkerExitStore, router *route.Router, result WorkerResult) {
	t.Helper()
	state.Running[result.IssueID] = &RunningEntry{
		Identifier: result.Identifier,
		Issue:      domain.Issue{ID: result.IssueID, Identifier: result.Identifier, State: "In Progress"},
		StartedAt:  baseTime,
	}
	state.Claimed[result.IssueID] = struct{}{}
	result.AgentAdapter = "mock"
	HandleWorkerExit(state, result, HandleWorkerExitParams{
		Store:             store,
		Router:            router,
		MaxRetryBackoffMS: 300_000,
		OnRetryFire:       noopRetryFire,
		NowFunc:           func() time.Time { return baseTime.Add(60 * time.Second) },
		Logger:            discardLogger(),
		Ctx:               context.Background(),
	})
}
