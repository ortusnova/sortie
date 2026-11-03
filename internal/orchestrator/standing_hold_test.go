package orchestrator

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/notify/render"
	"github.com/sortie-ai/sortie/internal/notify/route"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// fakeStandingHoldStore keeps standing hold rows in memory.
type fakeStandingHoldStore struct {
	mu   sync.Mutex
	rows map[string]persistence.StandingHold
}

func (f *fakeStandingHoldStore) UpsertStandingHold(_ context.Context, hold persistence.StandingHold) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rows == nil {
		f.rows = map[string]persistence.StandingHold{}
	}
	f.rows[hold.IssueID] = hold
	return nil
}

func (f *fakeStandingHoldStore) DeleteStandingHold(_ context.Context, issueID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.rows, issueID)
	return nil
}

func (f *fakeStandingHoldStore) list() []persistence.StandingHold {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]persistence.StandingHold, 0, len(f.rows))
	for _, row := range f.rows {
		out = append(out, row)
	}
	return out
}

// parkedHoldState returns a state with ONS-54 parked.
func parkedHoldState() *State {
	state := NewState(5000, 4, 0, nil, AgentTotals{})
	state.Parked["id-54"] = &ParkedEntry{Identifier: "ONS-54", Reason: parkReasonAgentBlocked, ParkedAt: reconcileBaseTime}
	return state
}

func supervisorHold(evidence string) HoldRequest {
	return HoldRequest{Holder: "supervisor", Reason: "full vm chain proof running", Evidence: evidence}
}

func TestStandingHold_CreateNeedsEvidenceAndBoundsTheLease(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := reconcileBaseTime
	tests := []struct {
		name string
		req  HoldRequest
		want string // "" for a hold placed
	}{
		{"no evidence", HoldRequest{Holder: "supervisor"}, "needs evidence"},
		{"blank evidence", HoldRequest{Holder: "supervisor", Evidence: "  "}, "needs evidence"},
		{"until in the past", HoldRequest{Evidence: "e", Until: now.Add(-time.Minute)}, "must end after now"},
		{"longer than the maximum", HoldRequest{Evidence: "e", For: MaxHoldLease + time.Minute}, "at most"},
		{"until beyond the maximum", HoldRequest{Evidence: "e", Until: now.Add(5 * time.Hour)}, "at most"},
		{"the maximum", HoldRequest{Evidence: "e", For: MaxHoldLease}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			state := parkedHoldState()
			reply := applyHold(ctx, state, nil, "ONS-54", tt.req, now, discardLogger())
			if tt.want == "" {
				if reply.Outcome != ControlDone || state.StandingHolds["id-54"] == nil {
					t.Fatalf("hold = %+v, want a standing hold placed", reply)
				}
				return
			}
			if reply.Outcome != ControlInvalid || !strings.Contains(reply.Message, tt.want) {
				t.Errorf("hold = %+v, want ControlInvalid saying %q", reply, tt.want)
			}
			if len(state.StandingHolds) != 0 {
				t.Errorf("standing holds = %+v, want none", state.StandingHolds)
			}
		})
	}
}

func TestStandingHold_CreateRenewExpireRelease(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := &fakeStandingHoldStore{}
	state := parkedHoldState()
	now := reconcileBaseTime

	created := applyHold(ctx, state, store, "ONS-54", supervisorHold("sandbox hl-ons-54-proof-1"), now, discardLogger())
	if created.Outcome != ControlDone || created.Hold.Lease == nil {
		t.Fatalf("hold = %+v, want a lease placed", created)
	}
	lease := created.Hold.Lease
	if lease.Holder != "supervisor" || lease.Evidence != "sandbox hl-ons-54-proof-1" || !lease.Until.Equal(now.Add(DefaultHoldLease)) || !lease.RenewedAt.Equal(now) {
		t.Errorf("lease = %+v, want supervisor's with the evidence, renewed now, until %v", lease, now.Add(DefaultHoldLease))
	}
	if state.Parked["id-54"] == nil {
		t.Error("the hold unparked the issue, want the park kept")
	}
	if rows := store.list(); len(rows) != 1 || rows[0].Until != now.Add(DefaultHoldLease).Format(time.RFC3339Nano) {
		t.Fatalf("stored rows = %+v, want the lease with its expiry", rows)
	}

	later := now.Add(50 * time.Minute)
	same := applyHold(ctx, state, store, "ONS-54", supervisorHold("sandbox hl-ons-54-proof-1"), later, discardLogger())
	if same.Outcome != ControlInvalid || !strings.Contains(same.Message, "new evidence") {
		t.Errorf("renewal with the same evidence = %+v, want ControlInvalid asking for new evidence", same)
	}
	renewed := applyHold(ctx, state, store, "ONS-54", HoldRequest{Evidence: "step k8s-up of hl-ons-54-proof-1", For: 30 * time.Minute}, later, discardLogger())
	if renewed.Outcome != ControlDone {
		t.Fatalf("renewal = %+v, want done", renewed)
	}
	h := state.StandingHolds["id-54"]
	if !h.Since.Equal(now) || !h.RenewedAt.Equal(later) || !h.Until.Equal(later.Add(30*time.Minute)) || h.Holder != "supervisor" || h.Reason != "full vm chain proof running" {
		t.Errorf("renewed lease = %+v, want start kept, renewed at %v until %v, holder and reason kept", h, later, later.Add(30*time.Minute))
	}

	spy := &notifierSpy{}
	p := standingHoldExpiry{
		Store:  store,
		Router: mustRouter(t, nil, spyLookup(spy), route.Inputs{Entries: []config.NotificationBackend{subscribe("webhook", domain.EventHoldExpired)}}),
		Wg:     &sync.WaitGroup{},
		Ctx:    ctx,
		Log:    discardLogger(),
	}
	if n := expireStandingHolds(state, h.Until.Add(-time.Second), p); n != 0 {
		t.Fatalf("expired %d leases before their end, want 0", n)
	}
	if n := expireStandingHolds(state, h.Until, p); n != 1 {
		t.Fatalf("expired %d leases at their end, want 1", n)
	}
	p.Wg.Wait()
	if !h.Expired() || !h.ExpiredAt.Equal(h.Until) || state.StandingHolds["id-54"] == nil {
		t.Errorf("lease after its end = %+v, want it kept and expired at its end", h)
	}
	if rows := store.list(); len(rows) != 1 || rows[0].ExpiredAt == "" {
		t.Errorf("stored rows = %+v, want the expiry persisted", rows)
	}
	if n := expireStandingHolds(state, h.Until.Add(time.Hour), p); n != 0 {
		t.Errorf("expired %d leases again, want 0", n)
	}
	p.Wg.Wait()
	sent := spy.notifications()
	if len(sent) != 1 || sent[0].Envelope.EventType != domain.EventHoldExpired || sent[0].Envelope.Identifier != "ONS-54" {
		t.Fatalf("notifications = %+v, want one hold.expired for ONS-54", sent)
	}
	if body := render.Notification(sent[0]).Body; !strings.Contains(body, "Last evidence: step k8s-up of hl-ons-54-proof-1") {
		t.Errorf("hold.expired body = %q, want the last evidence", body)
	}

	snap := RuntimeSnapshotResult{Holds: snapshotStandingHolds(state), Parked: []SnapshotParkedEntry{{IssueID: "id-54", Identifier: "ONS-54"}}}
	causes := AttentionCauses(snap, nil)
	if len(causes) != 2 || !hasCause(causes, AttentionKindHoldExpired) || !hasCause(causes, AttentionKindParked) {
		t.Errorf("attention causes after the expiry = %+v, want hold_expired and the park", causes)
	}

	renewExpired := applyHold(ctx, state, store, "ONS-54", supervisorHold("sandbox hl-ons-54-proof-2"), h.Until.Add(time.Hour), discardLogger())
	if renewExpired.Outcome != ControlDone || state.StandingHolds["id-54"].Expired() || !state.StandingHolds["id-54"].Since.Equal(h.Until.Add(time.Hour)) {
		t.Errorf("hold after the expiry = %+v, want a new live lease", renewExpired)
	}

	released := applyRelease(ctx, state, store, "ONS-54", h.Until.Add(2*time.Hour), nil, discardLogger())
	if released.Outcome != ControlDone || released.Hold.Held || len(state.StandingHolds) != 0 || len(store.list()) != 0 {
		t.Errorf("release = %+v, holds %+v, rows %+v, want the lease gone", released, state.StandingHolds, store.list())
	}
	if got := applyRelease(ctx, state, store, "ONS-54", h.Until.Add(2*time.Hour), nil, discardLogger()).Outcome; got != ControlNotRunning {
		t.Errorf("second release = %v, want ControlNotRunning", got)
	}
}

func hasCause(causes []AttentionCause, kind string) bool {
	for _, c := range causes {
		if c.Kind == kind {
			return true
		}
	}
	return false
}

func TestStandingHold_LiveLeaseHidesTheParkFromAttention(t *testing.T) {
	t.Parallel()

	state := parkedHoldState()
	applyHold(context.Background(), state, nil, "ONS-54", supervisorHold("sandbox"), reconcileBaseTime, discardLogger())
	snap := RuntimeSnapshotResult{Holds: snapshotStandingHolds(state), Parked: []SnapshotParkedEntry{{IssueID: "id-54", Identifier: "ONS-54"}}}
	if causes := AttentionCauses(snap, nil); len(causes) != 0 {
		t.Errorf("attention causes under a live lease = %+v, want none", causes)
	}
}

func TestStandingHold_RestartKeepsTheLeaseAndItsExpiry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := &fakeStandingHoldStore{}
	state := parkedHoldState()
	applyHold(ctx, state, store, "ONS-54", supervisorHold("sandbox hl-ons-54-proof-1"), reconcileBaseTime, discardLogger())
	want := *state.StandingHolds["id-54"]

	restarted := NewState(5000, 4, 0, nil, AgentTotals{})
	PopulateStandingHolds(restarted, store.list(), discardLogger())
	got := restarted.StandingHolds["id-54"]
	if got == nil || *got != want {
		t.Fatalf("lease after a restart = %+v, want %+v", got, want)
	}
	if n := expireStandingHolds(restarted, want.Until.Add(time.Minute), standingHoldExpiry{Store: store, Ctx: ctx, Log: discardLogger()}); n != 1 || !got.Expired() {
		t.Errorf("expired %d leases after a restart past the end, lease %+v, want 1", n, got)
	}
}

func TestStandingHold_KeepsTheIssueOutOfDispatchUntilReleased(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	h := newHeldHarness(t, nil, blockedCandidate("In Progress"))
	h.o.handleTick(ctx)
	requireHeld(t, h, "id-1", SkipBlockedBy)

	if reply := h.o.handleControl(ctx, controlRequest{Kind: controlHold, Identifier: "A-1", Hold: supervisorHold("sandbox")}); reply.Outcome != ControlDone {
		t.Fatalf("hold = %+v, want done", reply)
	}
	h.tracker.candidates = []domain.Issue{blockedCandidate("Done")}
	h.o.handleTick(ctx)
	got := requireHeld(t, h, "id-1", SkipStandingHold)
	if got.Holder != "supervisor" || got.Note != "full vm chain proof running" {
		t.Errorf("held entry = %+v, want supervisor's hold and its reason", got)
	}

	h.state.StandingHolds["id-1"].Until = time.Now().Add(-time.Second)
	h.o.handleTick(ctx)
	requireHeld(t, h, "id-1", SkipStandingHoldExpired)

	if reply := h.o.handleControl(ctx, controlRequest{Kind: controlRelease, Identifier: "A-1"}); reply.Outcome != ControlDone {
		t.Fatalf("release = %+v, want done", reply)
	}
	h.o.handleTick(ctx)
	h.waitForSessions(1)
}
