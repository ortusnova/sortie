package orchestrator

import (
	"context"
	"fmt"
	"maps"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/waittest"
)

// grantHarness runs an orchestrator over a real store on one active issue
// whose every session ends after one turn, so the issue runs again until
// its session budget holds it.
type grantHarness struct {
	o        *Orchestrator
	store    *persistence.Store
	sessions atomic.Int64 // sessions started by this orchestrator
	polls    atomic.Int64
	before   int // sessions in run history before it started
}

const (
	grantIssueID    = "id-68"
	grantIdentifier = "ONS-68"
)

func newGrantHarness(t *testing.T, store *persistence.Store, maxSessions, before int) *grantHarness {
	t.Helper()
	h := &grantHarness{store: store, before: before}

	cfg := defaultWorkerConfig(t.TempDir())
	cfg.Tracker.Kind = "mock"
	cfg.Polling = config.PollingConfig{IntervalMS: int(time.Hour / time.Millisecond)}
	cfg.Agent.MaxConcurrentAgents = 1
	cfg.Agent.MaxTurns = 1
	cfg.Agent.MaxSessions = maxSessions

	issue := domain.Issue{ID: grantIssueID, Identifier: grantIdentifier, Title: "held story", State: "To Do"}
	agent := &mockAgentAdapter{
		startSessionFn: func(context.Context, domain.StartSessionParams) (domain.Session, error) {
			n := h.sessions.Add(1)
			return domain.Session{ID: fmt.Sprintf("sess-%d", n)}, nil
		},
		runTurnFn: func(_ context.Context, session domain.Session, _ domain.RunTurnParams) (domain.TurnResult, error) {
			return domain.TurnResult{SessionID: session.ID, ExitReason: domain.EventTurnCompleted}, nil
		},
	}
	tracker := &candidateTrackerAdapter{
		mockTrackerAdapter: &mockTrackerAdapter{
			fetchIssueByIDFn: func(context.Context, string) (domain.Issue, error) { return issue, nil },
			fetchStatesFn: func(_ context.Context, ids []string) (map[string]string, error) {
				out := make(map[string]string, len(ids))
				for _, id := range ids {
					out[id] = issue.State
				}
				return out, nil
			},
		},
		fetchCandidatesFn: func(context.Context) ([]domain.Issue, error) {
			defer h.polls.Add(1)
			return []domain.Issue{issue}, nil
		},
	}

	pf := passingPreflightRegistries()
	pf.ReloadWorkflow = func() error { return nil }
	pf.ConfigFunc = func() config.ServiceConfig { return cfg }
	state := NewState(cfg.Polling.IntervalMS, cfg.Agent.MaxConcurrentAgents, 0, nil, AgentTotals{})
	h.o = NewOrchestrator(OrchestratorParams{
		State:           state,
		Logger:          discardLogger(),
		TrackerAdapter:  tracker,
		AgentAdapter:    agent,
		WorkflowManager: &stubWorkflowManager{config: cfg, template: mustParseTemplate(t, "do {{ .issue.identifier }}")},
		Store:           store,
		PreflightParams: pf,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		h.o.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return h
}

// heldAt waits until the session budget holds the issue after sessions
// sessions, at a budget of budget that counts granted sessions.
func (h *grantHarness) heldAt(t *testing.T, sessions, budget, granted int) {
	t.Helper()
	snapshot := h.o.SnapshotFunc()
	waittest.Until(t, func() error {
		snap, err := snapshot()
		if err != nil {
			return err
		}
		if len(snap.BudgetExhausted) != 1 || len(snap.Running) != 0 || len(snap.Retrying) != 0 {
			return fmt.Errorf("held %d, running %d, retrying %d, want only the hold", len(snap.BudgetExhausted), len(snap.Running), len(snap.Retrying))
		}
		held := snap.BudgetExhausted[0]
		if held.Reason != budgetReasonSession || held.UsedSessions != sessions || held.BudgetSessions != budget || held.GrantedSessions != granted {
			return fmt.Errorf("hold = %s at %d of %d with %d granted, want session_budget at %d of %d with %d granted",
				held.Reason, held.UsedSessions, held.BudgetSessions, held.GrantedSessions, sessions, budget, granted)
		}
		return nil
	})
	if got := h.sessions.Load(); got != int64(sessions-h.before) {
		t.Fatalf("sessions started = %d, want %d", got, sessions-h.before)
	}
}

// tick asks for a poll and returns once the loop has run it.
func (h *grantHarness) tick(t *testing.T) {
	t.Helper()
	before := h.polls.Load()
	h.o.RefreshFunc()()
	waittest.Until(t, func() error {
		if h.polls.Load() <= before {
			return fmt.Errorf("no poll since the refresh")
		}
		return nil
	})
}

func TestSessionGrant_HeldIssueRunsTheGrantedSessionsThenHoldsAgain(t *testing.T) {
	t.Parallel()

	store := openRepeatedFailureStore(t)
	h := newGrantHarness(t, store, 2, 0)
	h.heldAt(t, 2, 2, 0)
	h.tick(t)
	if got := h.sessions.Load(); got != 2 {
		t.Fatalf("sessions after a poll while held = %d, want 2", got)
	}

	grant := h.o.SessionGrantFunc()
	got := grant(t.Context(), SessionGrantRequest{Identifier: grantIdentifier, Add: 4, By: "brian", Reason: "one more try"})
	want := SessionGrantResult{Status: SessionGrantDone, IssueID: grantIssueID, Identifier: grantIdentifier, UsedSessions: 2, BudgetSessions: 6, GrantedSessions: 4}
	if got != want {
		t.Fatalf("grant = %+v, want %+v", got, want)
	}

	// The grant asks for a poll itself; the poll interval is an hour.
	h.heldAt(t, 6, 6, 4)
	h.tick(t)
	if got := h.sessions.Load(); got != 6 {
		t.Fatalf("sessions after a poll while held again = %d, want 6", got)
	}
	if snap, _ := h.o.SnapshotFunc()(); snap.SessionGrants[grantIssueID] != 4 {
		t.Errorf("snapshot SessionGrants = %v, want 4 for %s", snap.SessionGrants, grantIssueID)
	}

	if unknown := grant(t.Context(), SessionGrantRequest{Identifier: "ONS-404", Add: 1, By: "brian"}); unknown.Status != SessionGrantUnknownIssue {
		t.Errorf("grant to an unknown issue = %+v, want unknown issue", unknown)
	}
	totals, err := store.SessionGrantTotals(t.Context())
	if err != nil || len(totals) != 1 || totals[grantIssueID] != 4 {
		t.Errorf("SessionGrantTotals() = %v, %v, want only %s at 4", totals, err, grantIssueID)
	}
}

// A grant made before a restart still raises the budget after it: the new
// orchestrator reads it from the store.
func TestSessionGrant_OutlivesARestart(t *testing.T) {
	t.Parallel()

	store := openRepeatedFailureStore(t)
	ctx := context.Background()
	for i := range 2 {
		if _, err := store.AppendRunHistory(ctx, persistence.RunHistory{
			IssueID: grantIssueID, Identifier: grantIdentifier, Attempt: i + 1, AgentAdapter: "mock",
			Workspace: "/ws", StartedAt: "2026-11-02T09:00:00Z", CompletedAt: "2026-11-02T09:01:00Z", Status: "succeeded",
		}); err != nil {
			t.Fatalf("AppendRunHistory: %v", err)
		}
	}
	if _, err := store.AddSessionGrant(ctx, persistence.SessionGrant{IssueID: grantIssueID, Identifier: grantIdentifier, Sessions: 1, GrantedBy: "brian"}); err != nil {
		t.Fatalf("AddSessionGrant: %v", err)
	}

	h := newGrantHarness(t, store, 2, 2)
	h.heldAt(t, 3, 3, 1)
}

// grantingRetryStore is a retry store that also holds session grants.
type grantingRetryStore struct {
	*mockRetryStore
	grants map[string]int
}

func (s *grantingRetryStore) AddSessionGrant(_ context.Context, g persistence.SessionGrant) (int, error) {
	s.grants[g.IssueID] += g.Sessions
	return s.grants[g.IssueID], nil
}

func (s *grantingRetryStore) SessionGrantTotals(context.Context) (map[string]int, error) {
	return maps.Clone(s.grants), nil
}

func (s *grantingRetryStore) SessionGrantTotal(_ context.Context, issueID string) (int, error) {
	return s.grants[issueID], nil
}

// A retry that comes due at max_sessions runs when the issue has a grant.
func TestSessionGrant_RetryGateCountsTheGrant(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name       string
		granted    int
		wantHeld   bool
		wantBudget int
	}{
		{name: "no grant holds at max_sessions", wantHeld: true, wantBudget: 3},
		{name: "a grant lets the retry run", granted: 1},
		{name: "the sessions a grant adds are counted too", granted: 1, wantHeld: true, wantBudget: 4},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			used := 3
			if tt.wantHeld && tt.granted > 0 {
				used = 4
			}
			store := &grantingRetryStore{mockRetryStore: &mockRetryStore{runHistoryCount: used}, grants: map[string]int{}}
			if tt.granted > 0 {
				store.grants[grantIssueID] = tt.granted
			}
			state := retryState(t, grantIssueID, grantIdentifier, 1)
			params := defaultRetryParams(t, store.mockRetryStore, &mockRetryTracker{fetchedIssue: candidateIssue(grantIssueID, grantIdentifier, "To Do")})
			params.Store = store
			params.MaxSessions = 3

			HandleRetryTimer(state, grantIssueID, params)

			held, isHeld := state.BudgetExhausted[grantIssueID]
			if isHeld != tt.wantHeld {
				t.Fatalf("held = %v (%+v), want %v", isHeld, held, tt.wantHeld)
			}
			if isHeld && held.BudgetSessions != tt.wantBudget {
				t.Errorf("held budget = %d, want %d", held.BudgetSessions, tt.wantBudget)
			}
		})
	}
}

// A grant with a cap is refused when the issue's granted sessions would pass
// it, and nothing is granted.
func TestSessionGrant_CapBoundsTheIssuesGrants(t *testing.T) {
	t.Parallel()

	store := openRepeatedFailureStore(t)
	h := newGrantHarness(t, store, 2, 0)
	h.heldAt(t, 2, 2, 0)

	grant := h.o.SessionGrantFunc()
	over := grant(t.Context(), SessionGrantRequest{Identifier: grantIdentifier, Add: 4, By: "supervisor", Cap: 3})
	if over.Status != SessionGrantOverCap || over.GrantedSessions != 0 {
		t.Fatalf("grant past the cap = %+v, want refused with 0 granted", over)
	}
	if total, err := store.SessionGrantTotal(t.Context(), grantIssueID); err != nil || total != 0 {
		t.Fatalf("granted after a refusal = %d, %v, want 0", total, err)
	}
	within := grant(t.Context(), SessionGrantRequest{Identifier: grantIdentifier, Add: 3, By: "supervisor", Cap: 3})
	if within.Status != SessionGrantDone || within.GrantedSessions != 3 {
		t.Fatalf("grant up to the cap = %+v, want done with 3 granted", within)
	}
	h.heldAt(t, 5, 5, 3)
	if again := grant(t.Context(), SessionGrantRequest{Identifier: grantIdentifier, Add: 1, By: "supervisor", Cap: 3}); again.Status != SessionGrantOverCap {
		t.Errorf("grant once the cap is reached = %+v, want refused", again)
	}
}
