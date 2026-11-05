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
	"github.com/sortie-ai/sortie/internal/failcause"
	"github.com/sortie-ai/sortie/internal/notify/route"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/waittest"
)

// readyReviewEntry is a review watch of owner/repo#number whose last
// check found nothing to address at head.
func readyReviewEntry(issueID string, number int, head string) *PendingReaction {
	p := newReviewPendingEntry(issueID, number)
	p.LastCheck = &ReactionCheck{ObservedAt: time.Now(), HeadSHA: head, Comments: &CommentCheck{}}
	return p
}

func TestSupervisorWake_DigestTracksWhatTheSupervisorCanActOn(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	state := NewState(1000, 2, 0, nil, AgentTotals{})
	state.supervisor = newSupervisorState()
	state.Held = map[string]*HeldEntry{}
	if state.StandingHolds == nil {
		state.StandingHolds = map[string]*StandingHold{}
	}
	state.Held["id-5"] = &HeldEntry{Identifier: "ONS-5", Reason: SkipBlockedBy}
	state.StandingHolds["id-6"] = &StandingHold{Identifier: "ONS-6", Holder: "supervisor", RenewedAt: now, Until: now.Add(time.Hour)}
	base := supervisorStateDigest(state, now)

	unchanged := map[string]func(){
		"a slot fills":          func() { state.Held["id-9"] = &HeldEntry{Identifier: "ONS-9", Reason: SkipAgentSlotsFull} },
		"a blocker is not read": func() { state.Held["id-5"].Reason = SkipBlockersNotRead },
		"a lease is renewed": func() {
			state.StandingHolds["id-6"].RenewedAt, state.StandingHolds["id-6"].Until = now, now.Add(2*time.Hour)
		},
		"a story run starts": func() { state.Running["id-8"] = &RunningEntry{Identifier: "ONS-8"} },
		"a reaction waits on CI": func() {
			state.PendingReactions["id-7:ci"] = &PendingReaction{IssueID: "id-7", Kind: ReactionKindCI, KindData: &CIReactionData{PRNumber: 7, Owner: "o", Repo: "r"}}
		},
		"a hold's since moves on": func() { state.Held["id-5"].Since = now },
		"a story is held by a blocker": func() {
			state.Held["id-10"] = &HeldEntry{Identifier: "ONS-10", Reason: SkipBlockedBy, Blockers: []HeldBlocker{{BlockerRef: domain.BlockerRef{ID: "id-5"}}}}
		},
		"a fix job runs": func() { state.Running["id-11"] = &RunningEntry{Identifier: "ONS-11", ReactionKind: ReactionKindCI} },
		"a live-proof job runs": func() {
			state.Running["id-12"] = &RunningEntry{Identifier: "ONS-12", ReactionKind: ReactionKindLiveProof}
		},
		"a fix job ends": func() { delete(state.Running, "id-11") },
	}
	for name, change := range unchanged {
		change()
		if got := supervisorStateDigest(state, now); got != base {
			t.Errorf("%s changed the digest", name)
		}
	}

	changed := []struct {
		name   string
		change func()
	}{
		{"a story is held for another reason", func() { state.Held["id-5"].Reason = SkipEgressInvalid }},
		{"a lease passes its expiry", func() { state.StandingHolds["id-6"].Until = now.Add(-time.Second) }},
		{"a story parks", func() {
			state.Parked["id-4"] = &ParkedEntry{Identifier: "ONS-4", Reason: "agent_blocked", ParkedAt: now}
		}},
		{"its live-proof job ends", func() { noteLiveProofEnded(state, "id-4") }},
		{"a pull request is ready", func() { state.PendingReactions["id-3:review"] = readyReviewEntry("id-3", 3, "head-1") }},
		{"its head moves", func() { state.PendingReactions["id-3:review"].LastCheck.HeadSHA = "head-2" }},
		{"it conflicts", func() {
			state.PendingReactions["id-3:merge-conflict"] = &PendingReaction{
				IssueID: "id-3", Kind: ReactionKindMergeConflict, KindData: &MergeConflictReactionData{PRNumber: 3, Owner: "owner", Repo: "repo"},
				LastCheck: &ReactionCheck{ObservedAt: now, HeadSHA: "head-2", Mergeability: &MergeabilityCheck{Mergeable: new(false)}},
			}
		}},
		{"a fault opens", func() {
			state.PlatformFaults["pf-1"] = &PlatformFault{ID: "pf-1", Class: PlatformFaultClassSupervisorQuestion, RecordedAt: now, Action: &failcause.PersonAction{Action: "approve", Where: "here"}}
		}},
		{"a person marks it done", func() { state.PlatformFaults["pf-1"].ActionDoneAt = now }},
		{"another question opens", func() {
			state.PlatformFaults["pf-2"] = &PlatformFault{ID: "pf-2", Class: PlatformFaultClassSupervisorQuestion, RecordedAt: now, Action: &failcause.PersonAction{Action: "renew", Where: "there"}}
		}},
	}
	seen := map[string]string{base: "the start"}
	for _, c := range changed {
		c.change()
		got := supervisorStateDigest(state, now)
		if prev, ok := seen[got]; ok {
			t.Errorf("%s left the digest of %s", c.name, prev)
		}
		seen[got] = c.name
	}

	// No supervisor rule acts on how long a question waits: only a person
	// answers it, and the park or the question already changed the digest.
	last := supervisorStateDigest(state, now)
	for name, change := range map[string]func(){
		"a parked story's question ages": func() { state.Parked["id-4"].ParkedAt = now.Add(-5 * time.Hour) },
		"a question for a person ages":   func() { state.PlatformFaults["pf-2"].RecordedAt = now.Add(-5 * time.Hour) },
		"the parked story's proof starts": func() {
			state.Running["id-4"] = &RunningEntry{Identifier: "ONS-4", ReactionKind: ReactionKindLiveProof}
		},
	} {
		change()
		if got := supervisorStateDigest(state, now); got != last {
			t.Errorf("%s changed the digest", name)
		}
	}
}

// waitSessions waits until n supervisor sessions have started and none
// runs.
func (h *supHarness) waitSessions(n int) *SupervisorSnapshot {
	h.t.Helper()
	var snap *SupervisorSnapshot
	waittest.Until(h.t, func() error {
		snap = h.snapshot()
		if got := h.sup.sessions(); got != n || snap == nil || snap.Running != nil || snap.LastSession == nil {
			return fmt.Errorf("%d sessions, supervisor %+v, want %d ended", got, snap, n)
		}
		return nil
	})
	return snap
}

// trigger notes a trigger of kind on the event loop after change; the
// loop then decides whether a session starts.
func (h *supHarness) trigger(kind string, change func(s *State)) {
	h.t.Helper()
	h.onLoop(func(context.Context) {
		if change != nil {
			change(h.o.state)
		}
		noteSupervisorTrigger(h.o.state, SupervisorTrigger{Kind: kind, Detail: "test"})
	})
}

// pending returns the triggers waiting for a session.
func (h *supHarness) pending() []SupervisorTrigger {
	h.t.Helper()
	var pending []SupervisorTrigger
	h.onLoop(func(context.Context) { pending = slices.Clone(h.o.state.supervisor.pending) })
	return pending
}

// Scenario "A slot that blinks busy": a free slot with nothing to
// dispatch starts a session; a story then takes the one slot for a
// dispatch pass and ends; the next pass finds the same story held for the
// same reason, and no session starts.
func TestScenario_ASlotThatBlinksBusy(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, func(cfg *config.ServiceConfig) { cfg.Agent.MaxConcurrentAgents = 1 })
	release := make(chan struct{})
	h.story = &mockAgentAdapter{runTurnFn: func(ctx context.Context, session domain.Session, _ domain.RunTurnParams) (domain.TurnResult, error) {
		select {
		case <-release:
			return domain.TurnResult{SessionID: session.ID, ExitReason: domain.EventTurnCompleted}, nil
		case <-ctx.Done():
			return domain.TurnResult{SessionID: session.ID, ExitReason: domain.EventTurnCancelled}, ctx.Err()
		}
	}}
	h.run()

	five := domain.Issue{ID: "id-5", Identifier: "ONS-5", Title: "five", State: "To Do"}
	h.tracker.setCandidates(five)
	h.onLoop(func(context.Context) { h.o.state.FaultHolds["id-5"] = "pf-held" })
	h.refresh()
	first := h.waitSessions(1)
	if kind := first.LastSession.Triggers[0].Kind; kind != SupervisorTriggerIdleSlot || first.LastSession.Digest == "" {
		t.Fatalf("first session = %+v, want an idle_slot session with a digest", first.LastSession)
	}

	h.tracker.setCandidates(five, domain.Issue{ID: "id-6", Identifier: "ONS-6", Title: "six", State: "To Do"})
	h.refresh()
	running := func() bool {
		var ok bool
		h.onLoop(func(context.Context) { _, ok = h.o.state.Running["id-6"] })
		return ok
	}
	waittest.Until(t, func() error {
		if !running() {
			return fmt.Errorf("ONS-6 is not running")
		}
		return nil
	})
	h.tracker.finish("id-6")
	close(release)
	waittest.Until(t, func() error {
		if running() {
			return fmt.Errorf("ONS-6 still runs")
		}
		return nil
	})

	h.refresh()
	var held bool
	h.onLoop(func(context.Context) { held = h.o.state.FaultHolds["id-5"] == "pf-held" })
	if pending := h.pending(); !held || len(pending) != 0 || h.sup.sessions() != 1 {
		t.Errorf("after the blink: held %v, pending %v, %d sessions, want ONS-5 held and no new session", held, triggerKinds(pending), h.sup.sessions())
	}
	if snap := h.snapshot(); snap.LastTrigger == nil || snap.LastTrigger.Kind != SupervisorTriggerIdleSlot || !snap.LastTrigger.At.After(first.LastSession.StartedAt) {
		t.Errorf("last trigger = %+v, want the idle slot noted again and dropped", snap.LastTrigger)
	}
}

// Scenario "A ready pull request ages": nothing changes but a story pull
// request stays ready past 30 minutes; the digest changes then, and one
// session starts.
func TestScenario_AReadyPullRequestAges(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, nil)
	h.run()

	start := time.Now()
	h.trigger(SupervisorTriggerIdleSlot, func(s *State) {
		s.FaultHolds["id-5"] = "pf-held"
		s.PendingReactions[ReactionKey("id-41", ReactionKindReview)] = readyReviewEntry("id-41", 41, "head-41")
	})
	first := h.waitSessions(1)

	for _, at := range []time.Duration{29 * time.Minute, 31 * time.Minute, 45 * time.Minute} {
		h.onLoop(func(ctx context.Context) {
			noteSupervisorTrigger(h.o.state, SupervisorTrigger{Kind: SupervisorTriggerIdleSlot, Detail: "test"})
			h.o.superviseOnLoop(ctx, start.Add(at))
		})
		want := 1
		if at > supervisorReadyAge {
			want = 2
		}
		snap := h.waitSessions(want)
		if at > supervisorReadyAge && snap.LastSession.Digest == first.LastSession.Digest {
			t.Errorf("at %v the session's digest %s is the first's, want it changed", at, snap.LastSession.Digest)
		}
	}
}

// Scenario "Three no-ops in a row": three consecutive idle_slot sessions
// take no action on one digest; idle_slot then starts no session on that
// digest, Sortie publishes supervisor.loop_suspected, and the state shows
// the suspended kind. A changed digest lifts it.
func TestScenario_ThreeNoOpsInARow(t *testing.T) {
	t.Parallel()
	spy := &notifierSpy{}
	h := newSupHarness(t, func(cfg *config.ServiceConfig) {
		cfg.Notifications.Backends = []config.NotificationBackend{subscribe("webhook", domain.EventSupervisorLoopSuspected)}
	})
	h.params = func(p *OrchestratorParams) { p.Router = route.NewRouter(nil, spyLookup(spy)) }
	h.run()

	held := func(ids ...string) func(s *State) {
		return func(s *State) {
			clear(s.FaultHolds)
			for _, id := range ids {
				s.FaultHolds[id] = "pf-held"
			}
		}
	}
	sessions := 0
	var stateA string
	// Between two idle_slot sessions on state A, a stalled reaction starts
	// one on another state, so the last session's digest is never A's.
	for i, other := range []string{"id-6", "id-7", "id-8"} {
		h.trigger(SupervisorTriggerIdleSlot, held("id-5"))
		sessions++
		snap := h.waitSessions(sessions)
		if i == 0 {
			stateA = snap.LastSession.Digest
		}
		if snap.LastSession.Triggers[0].Kind != SupervisorTriggerIdleSlot || snap.LastSession.Digest != stateA || snap.LastSession.Actions != 0 {
			t.Fatalf("session %d = %+v, want a no-op idle_slot session on %s", sessions, snap.LastSession, stateA)
		}
		h.trigger(SupervisorTriggerStalledReaction, held("id-5", other))
		sessions++
		h.waitSessions(sessions)
	}

	snap := h.snapshot()
	if len(snap.Suspended) != 1 || snap.Suspended[0].Kind != SupervisorTriggerIdleSlot || snap.Suspended[0].Digest != stateA || snap.Suspended[0].Sessions != 3 {
		t.Fatalf("suspended = %+v, want idle_slot on %s after 3 sessions", snap.Suspended, stateA)
	}
	waittest.Until(t, func() error {
		sent := spy.notifications()
		if len(sent) != 1 {
			return fmt.Errorf("notifications = %d, want 1", len(sent))
		}
		facts, ok := sent[0].Message.Facts.(domain.SupervisorLoopSuspectedFacts)
		if sent[0].Envelope.EventType != domain.EventSupervisorLoopSuspected || !ok || facts.TriggerKind != SupervisorTriggerIdleSlot || facts.Digest != stateA {
			return fmt.Errorf("notification = %+v, want supervisor.loop_suspected for idle_slot on %s", sent[0], stateA)
		}
		return nil
	})

	h.trigger(SupervisorTriggerIdleSlot, held("id-5"))
	if pending := h.pending(); len(pending) != 0 || h.sup.sessions() != sessions {
		t.Fatalf("suspended idle_slot on its digest: pending %v, %d sessions, want none started", triggerKinds(pending), h.sup.sessions())
	}

	h.trigger(SupervisorTriggerIdleSlot, held("id-5", "id-9"))
	sessions++
	snap = h.waitSessions(sessions)
	if snap.LastSession.Triggers[0].Kind != SupervisorTriggerIdleSlot || snap.LastSession.Digest == stateA || len(snap.Suspended) != 0 {
		t.Errorf("after the digest changed: last %+v, suspended %+v, want an idle_slot session and no suspension", snap.LastSession, snap.Suspended)
	}
	if n := len(spy.notifications()); n != 1 {
		t.Errorf("notifications = %d, want the one loop suspicion", n)
	}
}

// A session that took an action ends its kind's no-op streak.
func TestSupervisor_AnActionEndsTheNoOpStreak(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, func(cfg *config.ServiceConfig) { cfg.Supervisor.NoopLimit = 2 })
	acted := false
	h.sup.setTurn(func(ctx context.Context, _ string, _ func(domain.AgentEvent)) (domain.TurnResult, error) {
		if !acted {
			acted = true
			if _, err := h.store.InsertSupervisorAction(ctx, persistence.SupervisorAction{
				Action: "release", Route: "POST /api/v1/ONS-5/unpark", Result: persistence.SupervisorDone, Status: 200,
			}); err != nil {
				return domain.TurnResult{}, err
			}
		}
		return domain.TurnResult{ExitReason: domain.EventTurnCompleted}, nil
	})
	h.run()

	h.trigger(SupervisorTriggerIdleSlot, func(s *State) { s.FaultHolds["id-5"] = "pf-held" })
	if snap := h.waitSessions(1); snap.LastSession.Actions != 1 {
		t.Fatalf("first session actions = %d, want 1", snap.LastSession.Actions)
	}
	h.trigger(SupervisorTriggerIdleSlot, func(s *State) { s.FaultHolds["id-6"] = "pf-held" })
	if snap := h.waitSessions(2); snap.LastSession.Actions != 0 || len(snap.Suspended) != 0 {
		t.Fatalf("second session = %+v, suspended %+v, want a no-op and no suspension", snap.LastSession, snap.Suspended)
	}
	latest, ok, err := h.store.LatestSupervisorSession(t.Context())
	if err != nil || !ok || latest.Actions != 0 || latest.Digest != h.snapshot().LastSession.Digest {
		t.Errorf("stored session = %+v, %v, %v, want its digest and no actions", latest, ok, err)
	}
}

// Scenario "A restart keeps the last digest": the digest a session started
// on is stored with it; after a restart the state API serves it, and the
// same state starts no session.
func TestScenario_ARestartKeepsTheLastDigest(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, nil)
	h.run()
	h.trigger(SupervisorTriggerIdleSlot, func(s *State) { s.FaultHolds["id-5"] = "pf-held" })
	digest := h.waitSessions(1).LastSession.Digest
	h.stop()

	latest, ok, err := h.store.LatestSupervisorSession(t.Context())
	if err != nil || !ok || latest.Digest != digest || latest.Actions != 0 {
		t.Fatalf("stored session = %+v, %v, %v, want digest %s", latest, ok, err, digest)
	}

	again := newSupHarness(t, nil)
	again.store = h.store
	again.run()
	again.onLoop(func(context.Context) {})
	snap := again.snapshot()
	if snap.LastSession == nil || snap.LastSession.Digest != digest || snap.LastSession.ID != latest.ID {
		t.Fatalf("last session after the restart = %+v, want session %d on %s", snap.LastSession, latest.ID, digest)
	}
	again.trigger(SupervisorTriggerIdleSlot, func(s *State) { s.FaultHolds["id-5"] = "pf-held" })
	if pending := again.pending(); len(pending) != 0 || again.sup.sessions() != 0 {
		t.Errorf("the same state after a restart: pending %v, %d sessions, want none", triggerKinds(pending), again.sup.sessions())
	}
	again.trigger(SupervisorTriggerIdleSlot, func(s *State) { s.FaultHolds["id-6"] = "pf-held" })
	if snap := again.waitSessions(1); snap.LastSession.Digest == digest {
		t.Errorf("a changed state started a session on the old digest %s", digest)
	}
}

// A no-op streak and its suspension outlive a restart.
func TestSupervisor_ASuspensionOutlivesARestart(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, nil)
	at := time.Now().Add(-time.Hour)
	for i, row := range []persistence.SupervisorSession{
		{TriggerKind: SupervisorTriggerIdleSlot, Digest: "aaaa"},
		{TriggerKind: SupervisorTriggerPark, Digest: "bbbb"},
		{TriggerKind: SupervisorTriggerIdleSlot, Digest: "aaaa"},
		{TriggerKind: SupervisorTriggerPark, Digest: "cccc"},
		{TriggerKind: SupervisorTriggerIdleSlot, Digest: "aaaa"},
	} {
		row.StartedAt = at.Add(time.Duration(i) * time.Minute)
		id, err := h.store.StartSupervisorSession(t.Context(), row)
		if err != nil {
			t.Fatal(err)
		}
		if err := h.store.EndSupervisorSession(t.Context(), id, row.StartedAt.Add(time.Second), SupervisorOutcomeCompleted, "", 10, 0); err != nil {
			t.Fatal(err)
		}
	}
	h.run()
	h.onLoop(func(context.Context) {})
	snap := h.snapshot()
	if len(snap.Suspended) != 1 || snap.Suspended[0].Kind != SupervisorTriggerIdleSlot || snap.Suspended[0].Digest != "aaaa" {
		t.Errorf("suspended after a restart = %+v, want idle_slot on aaaa", snap.Suspended)
	}
	if snap.LastSession == nil || snap.LastSession.Digest != "aaaa" {
		t.Errorf("last session = %+v, want the stored one on aaaa", snap.LastSession)
	}
}

// The quiet sweep: sweeps on an unchanged digest start no session until
// supervisor.quiet_sweep_ms has passed since the last one started; then
// one runs, and quiet sweeps never suspend the sweep.
func TestSupervisor_TheQuietSweep(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, func(cfg *config.ServiceConfig) { cfg.Supervisor.SweepIntervalMS = 1 })
	h.run()

	first := h.waitSessions(1)
	if first.LastSession.Triggers[0].Kind != SupervisorTriggerSweep {
		t.Fatalf("first session = %+v, want the sweep", first.LastSession)
	}
	// Sweeps come due every millisecond; none starts a session.
	for range 5 {
		h.refresh()
	}
	if n := h.sup.sessions(); n != 1 {
		t.Fatalf("sessions = %d on an unchanged digest, want 1", n)
	}

	quiet := time.Duration(h.cfg.Supervisor.QuietSweepMS) * time.Millisecond
	at := first.LastSession.StartedAt
	for i := range 4 {
		at = at.Add(quiet + time.Minute)
		h.onLoop(func(ctx context.Context) { h.o.superviseOnLoop(ctx, at) })
		snap := h.waitSessions(i + 2)
		last := snap.LastSession
		if last.Digest != first.LastSession.Digest || !strings.HasPrefix(last.Triggers[0].Detail, "the quiet sweep") {
			t.Fatalf("quiet sweep %d = %+v, want a quiet sweep on %s", i+1, last, first.LastSession.Digest)
		}
		if len(snap.Suspended) != 0 {
			t.Fatalf("quiet sweeps suspended %+v, want none", snap.Suspended)
		}
	}
}

// blockedHold is the hold of a story that waits on the blockers ids.
func blockedHold(identifier string, ids ...string) *HeldEntry {
	e := &HeldEntry{Identifier: identifier, Reason: SkipBlockedBy}
	for _, id := range ids {
		e.Blockers = append(e.Blockers, HeldBlocker{BlockerRef: domain.BlockerRef{ID: id, Identifier: strings.ToUpper(id), State: "In Progress"}})
	}
	return e
}

// conflictEntry is a merge-conflict watch of owner/repo#number whose last
// check found it conflicting at head.
func conflictEntry(issueID string, number int, head string, at time.Time) *PendingReaction {
	return &PendingReaction{
		IssueID: issueID, Kind: ReactionKindMergeConflict, KindData: &MergeConflictReactionData{PRNumber: number, Owner: "owner", Repo: "repo"},
		LastCheck: &ReactionCheck{ObservedAt: at, HeadSHA: head, Mergeability: &MergeabilityCheck{Mergeable: new(false)}},
	}
}

// A story held only by its blockers is not one the supervisor can act on,
// so it starts no idle_slot session, until a blocker's pull request
// conflicts or waits ready past supervisorReadyAge.
func TestSupervisor_IdleSlotIgnoresStoriesHeldOnlyByBlockers(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 5, 4, 0, 0, 0, time.UTC)
	state := NewState(1000, 4, 0, nil, AgentTotals{})
	state.supervisor = newSupervisorState()
	state.supervisor.enabled = true
	state.Held = map[string]*HeldEntry{
		"id-5": blockedHold("ONS-5", "id-b1"),
		"id-6": blockedHold("ONS-6", "id-b1", "id-b2"),
		"id-7": {Identifier: "ONS-7", Reason: SkipBlockersNotRead},
		"id-8": {Identifier: "ONS-8", Reason: SkipBlockersUnresolved},
		"id-9": {Identifier: "ONS-9", Reason: SkipBlockersIncomplete},
	}
	idle := func(at time.Time) []SupervisorTrigger {
		state.supervisor.pending = nil
		noteIdleSlot(state, 0, at)
		return state.supervisor.pending
	}

	if got := idle(now); len(got) != 0 {
		t.Fatalf("stories held only by blockers noted %v, want nothing", triggerKinds(got))
	}
	state.PendingReactions[ReactionKey("id-b1", ReactionKindReview)] = readyReviewEntry("id-b1", 1, "head-1")
	if got := idle(now.Add(time.Minute)); len(got) != 0 {
		t.Fatalf("a blocker's pull request ready for a minute noted %v, want nothing", triggerKinds(got))
	}
	got := idle(now.Add(time.Minute + supervisorReadyAge))
	if len(got) != 1 || got[0].Kind != SupervisorTriggerIdleSlot || !strings.Contains(got[0].Detail, "2 stories are held") {
		t.Fatalf("a blocker's pull request ready past %v noted %+v, want idle_slot for its 2 stories", supervisorReadyAge, got)
	}

	delete(state.PendingReactions, ReactionKey("id-b1", ReactionKindReview))
	if got := idle(now.Add(time.Hour)); len(got) != 0 {
		t.Fatalf("after the pull request left: noted %v, want nothing", triggerKinds(got))
	}
	state.PendingReactions[ReactionKey("id-b2", ReactionKindMergeConflict)] = conflictEntry("id-b2", 2, "head-2", now)
	if got := idle(now.Add(time.Hour)); len(got) != 1 || !strings.Contains(got[0].Detail, "1 stories are held") {
		t.Fatalf("a blocker's conflicting pull request noted %+v, want idle_slot for its one story", got)
	}

	delete(state.PendingReactions, ReactionKey("id-b2", ReactionKindMergeConflict))
	state.Held["id-10"] = &HeldEntry{Identifier: "ONS-10", Reason: SkipEgressInvalid}
	if got := idle(now.Add(time.Hour)); len(got) != 1 || !strings.Contains(got[0].Detail, "1 stories are held") {
		t.Fatalf("a story held for another reason noted %+v, want idle_slot for it alone", got)
	}
}

// Scenario "Blocked stories and runs that come and go" replays p1 on
// 2026-11-05: up to twelve stories wait on four blockers that are in
// progress or in review, a slot is free, the blockers' runs and fix jobs
// start and end, and the questions for a person grow old. None of that
// starts a session. The state first seen starts one, the one actual change
// (a blocker's pull request that conflicts) one more, and on an unchanged
// state the quiet sweep runs once per supervisor.quiet_sweep_ms.
func TestScenario_BlockedStoriesAndRunsThatComeAndGo(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, func(cfg *config.ServiceConfig) { cfg.Agent.MaxConcurrentAgents = 8 })
	h.run()

	start := time.Now()
	blockers := []string{"id-b1", "id-b2", "id-b3", "id-b4"}
	h.onLoop(func(context.Context) {
		s := h.o.state
		for i := range 7 {
			s.Held[fmt.Sprintf("id-%d", 20+i)] = blockedHold(fmt.Sprintf("ONS-%d", 20+i), blockers[i%len(blockers)])
		}
		s.PendingReactions[ReactionKey("id-b3", ReactionKindCI)] = &PendingReaction{
			IssueID: "id-b3", Kind: ReactionKindCI, CreatedAt: start, KindData: &CIReactionData{PRNumber: 3, Owner: "owner", Repo: "repo"},
		}
		s.PlatformFaults["pf-1"] = &PlatformFault{
			ID: "pf-1", Class: PlatformFaultClassSupervisorQuestion, RecordedAt: start, Action: &failcause.PersonAction{Action: "approve", Where: "here"},
		}
		s.FaultHolds["id-q1"] = "pf-1"
		s.Parked["id-p1"] = &ParkedEntry{Identifier: "ONS-P1", Reason: "agent_blocked", Statement: "approve the spec", ParkedAt: start}
	})

	// sessions waits until no session runs and returns the sessions started.
	sessions := func() int {
		t.Helper()
		var n int
		waittest.Until(t, func() error {
			var running bool
			h.onLoop(func(context.Context) { running, n = h.o.state.supervisor.run != nil, h.o.state.supervisor.sessionsUsed })
			if running || h.sup.sessions() != n {
				return fmt.Errorf("a session of %d runs", n)
			}
			return nil
		})
		return n
	}
	// pass is a dispatch pass at at that left a slot free and dispatched
	// nothing, after change.
	pass := func(at time.Time, change func(s *State)) {
		t.Helper()
		h.onLoop(func(ctx context.Context) {
			if change != nil {
				change(h.o.state)
			}
			noteIdleSlot(h.o.state, 0, at)
			h.o.superviseOnLoop(ctx, at)
		})
	}
	held := 7
	churn := []func(s *State){
		func(s *State) { s.Running["id-b1"] = &RunningEntry{Identifier: "ONS-B1"} },
		func(s *State) { s.Running["id-b2"] = &RunningEntry{Identifier: "ONS-B2", ReactionKind: ReactionKindCI} },
		func(s *State) {
			s.Running["id-b4"] = &RunningEntry{Identifier: "ONS-B4", ReactionKind: ReactionKindMergeConflict}
		},
		func(s *State) { delete(s.Running, "id-b2") },
		func(s *State) {
			s.PendingReactions[ReactionKey("id-b3", ReactionKindCI)].LastCheck = &ReactionCheck{ObservedAt: time.Now(), HeadSHA: "head-3", CI: &CICheck{Status: string(domain.CIStatusPending)}}
		},
		func(s *State) { delete(s.Running, "id-b4") },
		func(s *State) { delete(s.Running, "id-b1") },
		func(s *State) {
			if held < 12 {
				s.Held[fmt.Sprintf("id-%d", 20+held)] = blockedHold(fmt.Sprintf("ONS-%d", 20+held), blockers[held%len(blockers)])
				held++
			}
		},
		func(s *State) {
			s.Running["id-b3"] = &RunningEntry{Identifier: "ONS-B3", ReactionKind: ReactionKindReview}
		},
		func(s *State) { delete(s.Running, "id-b3") },
	}
	replay := func(from time.Time, passes int) {
		t.Helper()
		for i := range passes {
			pass(from.Add(time.Duration(i)*time.Minute), churn[i%len(churn)])
		}
	}

	// The first pass starts a session on the state it first sees: a
	// question holds a story.
	pass(start, nil)
	if n := sessions(); n != 1 {
		t.Fatalf("the first pass: %d sessions, want 1", n)
	}
	// The sweep at an hour finds the same state before the quiet sweep is due.
	replay(start.Add(time.Minute), 70)
	if n := sessions(); n != 1 {
		t.Fatalf("blocked stories and runs that come and go: %d sessions, want 1", n)
	}

	pass(start.Add(80*time.Minute), func(s *State) {
		s.PendingReactions[ReactionKey("id-b4", ReactionKindMergeConflict)] = conflictEntry("id-b4", 4, "head-4", time.Now())
	})
	if n := sessions(); n != 2 {
		t.Fatalf("a blocker's pull request conflicts: %d sessions, want 2", n)
	}
	replay(start.Add(81*time.Minute), 20)
	if n := sessions(); n != 2 {
		t.Fatalf("churn after the conflict: %d sessions, want 2", n)
	}

	// Past four hours the question and the park are old; that starts no
	// session, and the quiet sweep runs when it is due.
	replay(start.Add(4*time.Hour), 20)
	if n := sessions(); n != 3 {
		t.Fatalf("the questions grow old: %d sessions, want the one quiet sweep, 3", n)
	}
	replay(start.Add(5*time.Hour), 40)
	if n := sessions(); n != 3 {
		t.Fatalf("within supervisor.quiet_sweep_ms of the quiet sweep: %d sessions, want 3", n)
	}
	replay(start.Add(7*time.Hour), 10)
	if n := sessions(); n != 4 {
		t.Fatalf("the next quiet sweep: %d sessions, want 4", n)
	}
	h.onLoop(func(context.Context) {
		last := h.o.state.supervisor.last
		if last == nil || last.Triggers[0].Kind != SupervisorTriggerSweep || !strings.HasPrefix(last.Triggers[0].Detail, "the quiet sweep") {
			t.Errorf("last session = %+v, want a quiet sweep", last)
		}
	})
}

// A ready or conflicting pull request of a parked or fault-held story is
// left out of the digest, with its ready age, while the hold stands; its
// wait goes on, so it comes back old when the hold ends.
func TestSupervisorWake_APullRequestOfAHeldStoryIsLeftOut(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 5, 4, 0, 0, 0, time.UTC)
	state := NewState(1000, 2, 0, nil, AgentTotals{})
	state.Parked["id-3"] = &ParkedEntry{Identifier: "ONS-3", Reason: "agent_blocked", ParkedAt: now}
	state.FaultHolds["id-4"] = "pf-1"
	base := supervisorStateDigest(state, now)

	state.PendingReactions[ReactionKey("id-3", ReactionKindReview)] = readyReviewEntry("id-3", 3, "head-3")
	state.PendingReactions[ReactionKey("id-4", ReactionKindMergeConflict)] = conflictEntry("id-4", 4, "head-4", now)
	for _, at := range []time.Duration{0, supervisorReadyAge + time.Minute} {
		if got := supervisorStateDigest(state, now.Add(at)); got != base {
			t.Errorf("at %v the held stories' pull requests changed the digest", at)
		}
	}

	delete(state.Parked, "id-3")
	delete(state.FaultHolds, "id-4")
	w := observeSupervisorWake(state, now.Add(supervisorReadyAge+2*time.Minute))
	if len(w.Pulls) != 2 || !w.Pulls[0].Old || w.Pulls[1].State != wakePullConflicting {
		t.Errorf("pulls after the holds ended = %+v, want the ready one old and the conflicting one", w.Pulls)
	}
}

// Scenario "A parked story's pull request waits ready": the supervisor
// merges nothing of a parked story, so its pull request ready past 30
// minutes starts no session; releasing the park starts one.
func TestScenario_AParkedStorysPullRequestWaitsReady(t *testing.T) {
	t.Parallel()
	h := newSupHarness(t, nil)
	h.run()

	start := time.Now()
	h.onLoop(func(context.Context) {
		h.o.state.PendingReactions[ReactionKey("id-41", ReactionKindReview)] = readyReviewEntry("id-41", 41, "head-41")
	})
	h.park("id-41", "ONS-41")
	first := h.waitSessions(1)

	idle := func(at time.Duration, change func(s *State)) {
		h.onLoop(func(ctx context.Context) {
			if change != nil {
				change(h.o.state)
			}
			noteSupervisorTrigger(h.o.state, SupervisorTrigger{Kind: SupervisorTriggerIdleSlot, Detail: "test"})
			h.o.superviseOnLoop(ctx, start.Add(at))
		})
	}
	for _, at := range []time.Duration{29 * time.Minute, 31 * time.Minute, 45 * time.Minute} {
		idle(at, nil)
		if snap := h.waitSessions(1); snap.LastSession.Digest != first.LastSession.Digest {
			t.Fatalf("at %v the digest moved to %s, want the parked story's pull request left out", at, snap.LastSession.Digest)
		}
	}

	idle(50*time.Minute, func(s *State) { delete(s.Parked, "id-41") })
	if snap := h.waitSessions(2); snap.LastSession.Digest == first.LastSession.Digest {
		t.Errorf("releasing the park left the digest %s, want it changed", first.LastSession.Digest)
	}
	idle(55*time.Minute, nil)
	h.waitSessions(2)
}
