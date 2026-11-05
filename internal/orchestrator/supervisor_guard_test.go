package orchestrator

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/notify/route"
	"github.com/sortie-ai/sortie/internal/waittest"
)

// supervisorEvents are the supervisor's events a fake destination
// subscribes to.
var supervisorEvents = []domain.EventType{
	domain.EventSupervisorLoopSuspected, domain.EventSupervisorUnavailable, domain.EventSupervisorAvailable,
}

// withSupervisorSpy subscribes a fake notifications destination to the
// supervisor's events and returns it.
func withSupervisorSpy(h *supHarness) *notifierSpy {
	spy := &notifierSpy{}
	h.cfg.Notifications.Backends = []config.NotificationBackend{subscribe("webhook", supervisorEvents...)}
	h.params = func(p *OrchestratorParams) { p.Router = route.NewRouter(nil, spyLookup(spy)) }
	return spy
}

// spendingTurn returns a turn that reports tokens of usage and completes.
func spendingTurn(tokens int64) func(context.Context, string, func(domain.AgentEvent)) (domain.TurnResult, error) {
	return func(_ context.Context, _ string, onEvent func(domain.AgentEvent)) (domain.TurnResult, error) {
		onEvent(domain.AgentEvent{Type: domain.EventTokenUsage, Usage: domain.TokenUsage{InputTokens: tokens, TotalTokens: tokens}})
		return domain.TurnResult{ExitReason: domain.EventTurnCompleted}, nil
	}
}

// eventTypes returns the event type of each notification.
func eventTypes(sent []domain.Notification) []domain.EventType {
	out := make([]domain.EventType, 0, len(sent))
	for _, n := range sent {
		out = append(out, n.Envelope.EventType)
	}
	return out
}

// waitEvents waits until spy received exactly the events of want, and
// returns them in want's order. Each delivery is detached, so events
// published on one pass of the loop may arrive in either order.
func waitEvents(t *testing.T, spy *notifierSpy, want ...domain.EventType) []domain.Notification {
	t.Helper()
	sorted := slices.Sorted(slices.Values(want))
	var sent []domain.Notification
	waittest.Until(t, func() error {
		sent = spy.notifications()
		if got := slices.Sorted(slices.Values(eventTypes(sent))); !slices.Equal(got, sorted) {
			return fmt.Errorf("notifications = %v, want %v", eventTypes(sent), want)
		}
		return nil
	})
	out := make([]domain.Notification, 0, len(want))
	used := make([]bool, len(sent))
	for _, w := range want {
		for i, n := range sent {
			if !used[i] && n.Envelope.EventType == w {
				used[i] = true
				out = append(out, n)
				break
			}
		}
	}
	return out
}

func unavailableFacts(t *testing.T, n domain.Notification) domain.SupervisorUnavailableFacts {
	t.Helper()
	facts, ok := n.Message.Facts.(domain.SupervisorUnavailableFacts)
	if !ok {
		t.Fatalf("notification facts = %T, want SupervisorUnavailableFacts", n.Message.Facts)
	}
	return facts
}

// Scenario "A runaway session": a session that reaches the per-session
// token cap is stopped with the outcome session_cap, and its tokens count
// against the daily budget.
func TestScenario_ARunawaySession(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, func(cfg *config.ServiceConfig) { cfg.Supervisor.SessionTokens = 1000 })
	h.sup.setTurn(func(ctx context.Context, _ string, onEvent func(domain.AgentEvent)) (domain.TurnResult, error) {
		onEvent(domain.AgentEvent{Type: domain.EventTokenUsage, Usage: domain.TokenUsage{InputTokens: 1100, OutputTokens: 100, TotalTokens: 1200}})
		<-ctx.Done()
		return domain.TurnResult{ExitReason: domain.EventTurnCancelled}, ctx.Err()
	})
	h.run()

	h.park("id-7", "ONS-7")
	snap := h.waitEnded()
	if snap.LastSession.Outcome != SupervisorOutcomeSessionCap || snap.LastSession.Tokens != 1200 || !strings.Contains(snap.LastSession.Error, "supervisor.session_tokens") {
		t.Errorf("last session = %+v, want stopped on the session cap at 1200 tokens", snap.LastSession)
	}
	if snap.Budget.TokensUsed != 1200 || snap.Budget.SessionTokensLimit != 1000 || snap.Status != SupervisorStatusIdle || !snap.Available {
		t.Errorf("status %s budget %+v available %v, want 1200 tokens counted against the day and the supervisor idle", snap.Status, snap.Budget, snap.Available)
	}
	latest, ok, err := h.store.LatestSupervisorSession(t.Context())
	if err != nil || !ok || latest.Outcome != SupervisorOutcomeSessionCap || latest.Tokens != 1200 {
		t.Errorf("stored session = %+v, %v, %v, want the session_cap outcome with its tokens", latest, ok, err)
	}
}

// Scenario "The last fifth of the budget": with 15 % of the daily budget
// left, a free slot starts no session and is recorded as deferred, and a
// park that follows still starts one.
func TestScenario_TheLastFifthOfTheBudget(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, nil)
	h.run()
	h.onLoop(func(context.Context) { h.o.state.supervisor.tokensUsed = 850_000 })

	h.trigger(SupervisorTriggerIdleSlot, func(s *State) { s.FaultHolds["id-5"] = "pf-held" })
	snap := h.snapshot()
	if h.sup.sessions() != 0 || len(snap.Pending) != 0 || !snap.Budget.Reserve {
		t.Fatalf("free slot in the reserve: %d sessions, pending %v, reserve %v, want no session", h.sup.sessions(), triggerKinds(snap.Pending), snap.Budget.Reserve)
	}
	if len(snap.Deferred) != 1 || snap.Deferred[0].Kind != SupervisorTriggerIdleSlot || snap.Deferred[0].Guard != SupervisorGuardReserve {
		t.Fatalf("deferred = %+v, want the idle slot deferred by the reserve", snap.Deferred)
	}

	h.park("id-7", "ONS-7")
	snap = h.waitSessions(1)
	if got := triggerKinds(snap.LastSession.Triggers); !slices.Equal(got, []string{"park ONS-7"}) {
		t.Errorf("session triggers = %v, want the park alone", got)
	}
	if len(snap.Deferred) != 1 || snap.Deferred[0].Kind != SupervisorTriggerIdleSlot {
		t.Errorf("deferred after the park = %+v, want the idle slot still deferred", snap.Deferred)
	}
	if !snap.Available {
		t.Errorf("supervisor = %+v, want available: the reserve still starts parks", snap)
	}
}

// The reserve admits a platform fault and a sweep, and a new UTC day
// gives back what it deferred.
func TestSupervisor_TheReserveAdmitsFaultsAndSweeps(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{SupervisorTriggerUnclassifiedFailure, SupervisorTriggerSweep} {
		if !reserveAdmits(kind) {
			t.Errorf("reserveAdmits(%s) = false, want true", kind)
		}
	}
	for _, kind := range []string{SupervisorTriggerIdleSlot, SupervisorTriggerStalledReaction} {
		if reserveAdmits(kind) {
			t.Errorf("reserveAdmits(%s) = true, want false", kind)
		}
	}

	h := newSupHarness(t, func(cfg *config.ServiceConfig) {
		cfg.Supervisor.SweepIntervalMS = int(48 * time.Hour / time.Millisecond)
	})
	h.run()
	h.onLoop(func(context.Context) { h.o.state.supervisor.tokensUsed = 850_000 })
	h.trigger(SupervisorTriggerStalledReaction, func(s *State) { s.FaultHolds["id-5"] = "pf-held" })
	if snap := h.snapshot(); len(snap.Deferred) != 1 || h.sup.sessions() != 0 {
		t.Fatalf("deferred = %+v with %d sessions, want the stalled reaction deferred", snap.Deferred, h.sup.sessions())
	}
	h.trigger(SupervisorTriggerUnclassifiedFailure, func(s *State) { s.FaultHolds["id-6"] = "pf-held" })
	if snap := h.waitSessions(1); snap.LastSession.Triggers[0].Kind != SupervisorTriggerUnclassifiedFailure {
		t.Fatalf("session = %+v, want the unclassified failure's", snap.LastSession)
	}

	tomorrow := utcDay(time.Now()).Add(24*time.Hour + time.Minute)
	h.onLoop(func(ctx context.Context) {
		h.o.state.FaultHolds["id-7"] = "pf-held"
		h.o.superviseOnLoop(ctx, tomorrow)
	})
	snap := h.waitSessions(2)
	if snap.LastSession.Triggers[0].Kind != SupervisorTriggerStalledReaction || len(snap.Deferred) != 0 {
		t.Errorf("next day: session %+v deferred %+v, want the deferred stalled reaction started", snap.LastSession, snap.Deferred)
	}
}

// Scenario "A spend velocity": once the last hour's tokens exceed
// supervisor.velocity_factor times the even hourly pace, Sortie publishes
// supervisor.loop_suspected with the hourly spend and supervisor.unavailable
// with reason velocity; only a sweep starts a session until the hourly
// spend is back under the pace, when the deferred park starts one and
// supervisor.available follows.
func TestScenario_ASpendVelocity(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, func(cfg *config.ServiceConfig) {
		cfg.Supervisor.MaxTokensPerDay, cfg.Supervisor.VelocityFactor = 2_400_000, 3
		cfg.Supervisor.SessionTokens = 1_000_000
		cfg.Supervisor.SweepIntervalMS = int(48 * time.Hour / time.Millisecond)
	})
	spy := withSupervisorSpy(h)
	h.sup.setTurn(spendingTurn(200_000))
	h.run()

	h.park("id-7", "ONS-7")
	h.waitSessions(1)
	if sent := spy.notifications(); len(sent) != 0 {
		t.Fatalf("after 200000 tokens: %v, want nothing published under the 300000 limit", eventTypes(sent))
	}
	h.park("id-8", "ONS-8")
	snap := h.waitSessions(2)
	sent := waitEvents(t, spy, domain.EventSupervisorLoopSuspected, domain.EventSupervisorUnavailable)
	loop, ok := sent[0].Message.Facts.(domain.SupervisorLoopSuspectedFacts)
	if !ok || loop.HourlyTokens != 400_000 || loop.HourlyLimit != 300_000 || loop.Pace != 100_000 || !loop.Velocity() {
		t.Errorf("loop suspicion = %+v, want 400000 tokens over 300000 at a pace of 100000", sent[0].Message.Facts)
	}
	gone := unavailableFacts(t, sent[1])
	if gone.Reason != domain.SupervisorUnavailableVelocity || gone.ResetsAt.IsZero() {
		t.Errorf("unavailable = %+v, want velocity with when it resets", gone)
	}
	if snap.Available || snap.Reason != domain.SupervisorUnavailableVelocity || !snap.Budget.Velocity || snap.Budget.HourTokensUsed != 400_000 {
		t.Errorf("supervisor = %+v budget %+v, want unavailable for velocity at 400000 tokens", snap, snap.Budget)
	}

	h.sup.setTurn(nil)
	h.park("id-9", "ONS-9")
	snap = h.snapshot()
	if h.sup.sessions() != 2 || len(snap.Deferred) != 1 || snap.Deferred[0].Identifier != "ONS-9" || snap.Deferred[0].Guard != SupervisorGuardVelocity {
		t.Fatalf("park under velocity: %d sessions, deferred %+v, want it deferred", h.sup.sessions(), snap.Deferred)
	}
	h.trigger(SupervisorTriggerSweep, func(s *State) { s.FaultHolds["id-5"] = "pf-held" })
	snap = h.waitSessions(3)
	if got := triggerKinds(snap.LastSession.Triggers); !slices.Equal(got, []string{"sweep "}) || len(snap.Deferred) != 1 {
		t.Errorf("under velocity: session triggers %v deferred %+v, want the sweep alone and the park still deferred", got, snap.Deferred)
	}

	h.onLoop(func(ctx context.Context) {
		h.o.state.FaultHolds["id-6"] = "pf-held"
		h.o.superviseOnLoop(ctx, time.Now().Add(time.Hour+time.Minute))
	})
	snap = h.waitSessions(4)
	if got := triggerKinds(snap.LastSession.Triggers); !slices.Equal(got, []string{"park ONS-9"}) || len(snap.Deferred) != 0 {
		t.Errorf("back under the pace: session triggers %v deferred %+v, want the deferred park started", got, snap.Deferred)
	}
	sent = waitEvents(t, spy, domain.EventSupervisorLoopSuspected, domain.EventSupervisorUnavailable, domain.EventSupervisorAvailable)
	if back, ok := sent[2].Message.Facts.(domain.SupervisorAvailableFacts); !ok || back.Reason != domain.SupervisorUnavailableVelocity {
		t.Errorf("available = %+v, want after velocity", sent[2].Message.Facts)
	}
	if snap := h.snapshot(); !snap.Available || snap.Reason != "" || snap.Budget.Velocity {
		t.Errorf("supervisor = %+v, want available again", snap)
	}
}

// Scenario "Budget gone at night": the daily budget runs out, Sortie
// publishes supervisor.unavailable with reason budget_exhausted and
// resets_at the next UTC midnight, the state serves both, and the next
// UTC day publishes supervisor.available.
func TestScenario_BudgetGoneAtNight(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, func(cfg *config.ServiceConfig) {
		cfg.Supervisor.MaxTokensPerDay, cfg.Supervisor.VelocityFactor = 1000, 1000
		cfg.Supervisor.SweepIntervalMS = int(48 * time.Hour / time.Millisecond)
	})
	spy := withSupervisorSpy(h)
	h.sup.setTurn(func(ctx context.Context, _ string, onEvent func(domain.AgentEvent)) (domain.TurnResult, error) {
		onEvent(domain.AgentEvent{Type: domain.EventTokenUsage, Usage: domain.TokenUsage{InputTokens: 1500, TotalTokens: 1500}})
		<-ctx.Done()
		return domain.TurnResult{ExitReason: domain.EventTurnCancelled}, ctx.Err()
	})
	h.run()

	h.park("id-7", "ONS-7")
	snap := h.waitEnded()
	sent := waitEvents(t, spy, domain.EventSupervisorUnavailable)
	gone := unavailableFacts(t, sent[0])
	midnight := utcDay(snap.LastSession.StartedAt).Add(24 * time.Hour)
	if gone.Reason != domain.SupervisorUnavailableBudget || !gone.ResetsAt.Equal(midnight) || gone.Since.IsZero() {
		t.Errorf("unavailable = %+v, want budget_exhausted resetting at %v", gone, midnight)
	}
	if sent[0].Envelope.Identifier != "" || sent[0].Message.Severity != "warning" {
		t.Errorf("notification = %+v, want a warning about no story", sent[0])
	}
	snap = h.snapshot()
	if snap.Available || snap.Reason != domain.SupervisorUnavailableBudget || !snap.ResetsAt.Equal(midnight) {
		t.Errorf("supervisor available %v reason %q resets %v, want budget_exhausted until %v", snap.Available, snap.Reason, snap.ResetsAt, midnight)
	}

	h.sup.setTurn(nil)
	h.park("id-8", "ONS-8")
	if n := len(spy.notifications()); n != 1 || h.sup.sessions() != 1 {
		t.Errorf("a park on a spent budget: %d notifications, %d sessions, want the one announcement and no session", n, h.sup.sessions())
	}

	h.onLoop(func(ctx context.Context) { h.o.superviseOnLoop(ctx, midnight.Add(time.Second)) })
	sent = waitEvents(t, spy, domain.EventSupervisorUnavailable, domain.EventSupervisorAvailable)
	if back, ok := sent[1].Message.Facts.(domain.SupervisorAvailableFacts); !ok || back.Reason != domain.SupervisorUnavailableBudget || sent[1].Message.Severity != "info" {
		t.Errorf("available = %+v, want after budget_exhausted", sent[1].Message.Facts)
	}
}

// A session that fails on its credential records the agent kind's
// credential fault: Sortie publishes supervisor.unavailable with reason
// credential, starts no session while it is open, and once a person marks
// it done publishes supervisor.available and starts the session that
// waited.
func TestSupervisor_ACredentialFailure(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, nil)
	spy := withSupervisorSpy(h)
	h.sup.startErr = &domain.AgentError{Kind: domain.ErrCredentialUnverified, Message: "the runtime is not logged in"}
	h.run()

	h.park("id-7", "ONS-7")
	snap := h.waitEnded()
	if snap.LastSession.Outcome != SupervisorOutcomeFailed || !strings.Contains(snap.LastSession.Error, "not logged in") {
		t.Fatalf("last session = %+v, want failed on its credential", snap.LastSession)
	}
	sent := waitEvents(t, spy, domain.EventSupervisorUnavailable)
	if gone := unavailableFacts(t, sent[0]); gone.Reason != domain.SupervisorUnavailableCredential || !gone.ResetsAt.IsZero() ||
		!strings.Contains(gone.Detail, "not logged in") {
		t.Errorf("unavailable = %+v, want credential with no reset", gone)
	}
	var faultID string
	h.onLoop(func(context.Context) {
		if f := supervisorCredentialFault(h.o.state, testSupervisorKind); f != nil {
			faultID = f.ID
		}
	})
	if faultID == "" {
		t.Fatal("no credential fault of the supervisor's agent kind")
	}

	h.park("id-8", "ONS-8")
	if snap := h.snapshot(); h.sup.sessions() != 1 || len(snap.Pending) != 1 || snap.Reason != domain.SupervisorUnavailableCredential {
		t.Fatalf("a park while the credential is refused: %d sessions, pending %v, reason %q, want it waiting", h.sup.sessions(), triggerKinds(snap.Pending), snap.Reason)
	}

	if done := h.o.PlatformFaultDoneFunc()(t.Context(), faultID, "taichi"); done.Status != PlatformFaultDoneChecked || !done.Resolved {
		t.Fatalf("action done = %+v, want the fault resolved", done)
	}
	snap = h.waitSessions(2)
	if got := triggerKinds(snap.LastSession.Triggers); !slices.Contains(got, "park ONS-8") || snap.LastSession.Outcome != SupervisorOutcomeCompleted {
		t.Errorf("session after the sign-in = %+v, want the park of ONS-8 completed", snap.LastSession)
	}
	waitEvents(t, spy, domain.EventSupervisorUnavailable, domain.EventSupervisorAvailable)
}

// Pause and resume each publish once: supervisor.unavailable with reason
// paused and no reset, and supervisor.available.
func TestSupervisor_PauseAndResumeAreAnnounced(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, nil)
	spy := withSupervisorSpy(h)
	h.run()
	pause := h.o.SupervisorPauseFunc()

	pause(t.Context(), true, "taichi", "rewriting the prompt")
	pause(t.Context(), true, "someone", "")
	sent := waitEvents(t, spy, domain.EventSupervisorUnavailable)
	if gone := unavailableFacts(t, sent[0]); gone.Reason != domain.SupervisorUnavailablePaused || !gone.ResetsAt.IsZero() ||
		gone.Detail != "paused by taichi: rewriting the prompt" {
		t.Errorf("unavailable = %+v, want paused by taichi", gone)
	}
	if snap := h.snapshot(); snap.Available || snap.Reason != domain.SupervisorUnavailablePaused || !snap.ResetsAt.IsZero() || snap.Since.IsZero() {
		t.Errorf("supervisor = %+v, want unavailable for the pause", snap)
	}

	pause(t.Context(), false, "", "")
	pause(t.Context(), false, "", "")
	sent = waitEvents(t, spy, domain.EventSupervisorUnavailable, domain.EventSupervisorAvailable)
	if back, ok := sent[1].Message.Facts.(domain.SupervisorAvailableFacts); !ok || back.Reason != domain.SupervisorUnavailablePaused || back.Since.IsZero() {
		t.Errorf("available = %+v, want after the pause", sent[1].Message.Facts)
	}
	if snap := h.snapshot(); !snap.Available || snap.Reason != "" || !snap.Since.IsZero() {
		t.Errorf("supervisor = %+v, want available", snap)
	}
}

func TestSupervisorSpend_ResetsAt(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	s := newSupervisorState()
	s.noteSpend(now.Add(-2*time.Hour), 500)
	s.noteSpend(now.Add(-30*time.Minute), 200)
	s.noteSpend(now.Add(-50*time.Minute), 300)
	s.noteSpend(now.Add(-10*time.Minute), 100)
	if got := s.hourSpend(now); got != 600 {
		t.Errorf("hourSpend = %d, want 600", got)
	}
	if got, want := s.spendResetsAt(now, 350), now.Add(10*time.Minute); !got.Equal(want) {
		t.Errorf("spendResetsAt(350) = %v, want %v once the -50m sample leaves the hour", got, want)
	}
	if got, want := s.spendResetsAt(now, 250), now.Add(30*time.Minute); !got.Equal(want) {
		t.Errorf("spendResetsAt(250) = %v, want %v once the -50m and -30m samples leave the hour", got, want)
	}
	if got := s.spendResetsAt(now, 700); !got.IsZero() {
		t.Errorf("spendResetsAt(700) = %v, want zero: already under", got)
	}
	s.pruneSpend(now)
	if len(s.spend) != 3 {
		t.Errorf("after pruning %d samples, want the 3 of the last hour", len(s.spend))
	}
}
