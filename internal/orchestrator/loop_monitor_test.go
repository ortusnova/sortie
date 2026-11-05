package orchestrator

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/notify/route"
	"github.com/sortie-ai/sortie/internal/waittest"
)

// fakeLoopClock is a clock a test moves by hand.
type fakeLoopClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeLoopClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeLoopClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newFakeLoopClock() *fakeLoopClock {
	return &fakeLoopClock{now: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)}
}

func TestLoopMonitor_NamesTheSlowestPhase(t *testing.T) {
	t.Parallel()

	clock := newFakeLoopClock()
	m := newLoopMonitor(clock.Now)

	m.begin(loopHandlerTick)
	clock.advance(10 * time.Millisecond)
	m.enter("reconcile.ci_status")
	clock.advance(20 * time.Millisecond)
	m.enter("reconcile.review_comments")
	clock.advance(3 * time.Second)

	busy := m.status()
	if busy.BusyNow != 3030*time.Millisecond || busy.BusyHandler != "tick/reconcile.review_comments" {
		t.Fatalf("status while busy = %+v, want 3.03s in tick/reconcile.review_comments", busy)
	}

	m.enter("dispatch")
	clock.advance(5 * time.Millisecond)
	hold, slow, notice := m.end()
	if hold.Handler != "tick/reconcile.review_comments" || hold.Duration != 3035*time.Millisecond {
		t.Fatalf("hold = %+v, want 3.035s named for the review pass", hold)
	}
	if slow != 0 || notice {
		t.Fatalf("slow, notice = %d, %v; want no slow hold under 5s", slow, notice)
	}

	idle := m.status()
	if idle.BusyNow != 0 || idle.BusyHandler != "" {
		t.Fatalf("status while idle = %+v, want no busy hold", idle)
	}
	if idle.MaxHold != 3035*time.Millisecond || idle.SlowestHandler != "tick/reconcile.review_comments" {
		t.Fatalf("status window = %+v, want the 3.035s hold", idle)
	}
}

func TestLoopMonitor_HandlerBodyNamesTheHold(t *testing.T) {
	t.Parallel()

	clock := newFakeLoopClock()
	m := newLoopMonitor(clock.Now)

	m.begin(loopHandlerWorkerExit)
	clock.advance(200 * time.Millisecond)
	m.enter("supervise")
	clock.advance(time.Millisecond)
	if hold, _, _ := m.end(); hold.Handler != loopHandlerWorkerExit {
		t.Fatalf("hold handler = %q, want %q", hold.Handler, loopHandlerWorkerExit)
	}
}

func TestLoopMonitor_WindowForgetsOldHolds(t *testing.T) {
	t.Parallel()

	clock := newFakeLoopClock()
	m := newLoopMonitor(clock.Now)

	m.begin(loopHandlerTick)
	clock.advance(time.Second)
	m.end()
	m.begin(loopHandlerAgentEvent)
	clock.advance(time.Millisecond)
	m.end()

	clock.advance(loopStatusWindow)
	if st := m.status(); st.MaxHold != 0 || st.SlowestHandler != "" {
		t.Fatalf("status after the window = %+v, want no hold", st)
	}
}

func TestLoopMonitor_RepeatedSlowHoldsNoticeOncePerWindow(t *testing.T) {
	t.Parallel()

	clock := newFakeLoopClock()
	m := newLoopMonitor(clock.Now)
	slowHold := func() (int, bool) {
		m.begin(loopHandlerTick)
		m.enter("reconcile.review_comments")
		clock.advance(6 * time.Second)
		_, slow, notice := m.end()
		return slow, notice
	}

	if slow, notice := slowHold(); slow != 1 || notice {
		t.Fatalf("first slow hold: slow, notice = %d, %v; want 1, false", slow, notice)
	}
	clock.advance(time.Minute)
	if slow, notice := slowHold(); slow != 2 || !notice {
		t.Fatalf("second slow hold: slow, notice = %d, %v; want 2, true", slow, notice)
	}
	clock.advance(time.Minute)
	if slow, notice := slowHold(); slow != 3 || notice {
		t.Fatalf("third slow hold in the window: slow, notice = %d, %v; want 3, false", slow, notice)
	}
	if st := m.status(); st.SlowHolds != 3 {
		t.Fatalf("status slow holds = %d, want 3", st.SlowHolds)
	}

	clock.advance(loopSlowWindow)
	if slow, notice := slowHold(); slow != 1 || notice {
		t.Fatalf("slow hold after the window: slow, notice = %d, %v; want 1, false", slow, notice)
	}
	clock.advance(time.Minute)
	if slow, notice := slowHold(); slow != 2 || !notice {
		t.Fatalf("repeat after the window: slow, notice = %d, %v; want 2, true", slow, notice)
	}
}

func TestLoopMonitor_EnterOutsideAHoldDoesNothing(t *testing.T) {
	t.Parallel()

	m := newLoopMonitor(nil)
	m.enter("reconcile.stalled")
	if hold, _, _ := m.end(); hold.Handler != "" {
		t.Fatalf("hold = %+v, want none", hold)
	}
	if st := m.status(); st.BusyHandler != "" {
		t.Fatalf("status = %+v, want idle", st)
	}
}

func TestEndLoopHold_WarnsMeasuresAndNotices(t *testing.T) {
	t.Parallel()

	spy := &notifierSpy{}
	metrics := &spyMetrics{}
	lb, logger := textLogger()
	wm := budgetTickConfig(0)
	wm.config.Notifications.Backends = []config.NotificationBackend{subscribe("webhook", domain.EventPlatformLoopSlow)}
	o := NewOrchestrator(OrchestratorParams{
		State:           NewState(60000, 4, 0, nil, AgentTotals{}),
		Logger:          logger,
		TrackerAdapter:  &mockTrackerAdapter{},
		Router:          route.NewRouter(nil, spyLookup(spy)),
		AgentAdapter:    &mockAgentAdapter{},
		WorkflowManager: wm,
		Store:           &stubStore{},
		Metrics:         metrics,
	})
	o.updateRouter(wm.Config())
	clock := newFakeLoopClock()
	o.loop = newLoopMonitor(clock.Now)

	hold := func(d time.Duration) {
		o.loop.begin(loopHandlerTick)
		o.loop.enter("reconcile.review_comments")
		clock.advance(d)
		o.endLoopHold(t.Context())
	}

	hold(time.Second)
	if strings.Contains(lb.String(), "event loop held") {
		t.Fatalf("a 1s hold logged a warning:\n%s", lb.String())
	}
	hold(2500 * time.Millisecond)
	line := findLogLine(lb.String(), "event loop held")
	if !strings.Contains(line, "level=WARN") || !strings.Contains(line, "handler=tick/reconcile.review_comments") || !strings.Contains(line, "hold_ms=2500") {
		t.Fatalf("warning = %q, want a WARN naming the handler and the 2500 ms hold", line)
	}

	hold(6 * time.Second)
	hold(7 * time.Second)
	o.state.TrackerOpsWg.Wait()

	metrics.mu.Lock()
	holds := slices.Clone(metrics.loopHolds)
	metrics.mu.Unlock()
	want := []loopHoldCall{
		{"tick/reconcile.review_comments", 1},
		{"tick/reconcile.review_comments", 2.5},
		{"tick/reconcile.review_comments", 6},
		{"tick/reconcile.review_comments", 7},
	}
	if !slices.Equal(holds, want) {
		t.Fatalf("observed holds = %v, want %v", holds, want)
	}

	sent := spy.notifications()
	if len(sent) != 1 {
		t.Fatalf("notifications = %d, want 1", len(sent))
	}
	facts, ok := sent[0].Message.Facts.(domain.PlatformLoopSlowFacts)
	if sent[0].Envelope.EventType != domain.EventPlatformLoopSlow || !ok {
		t.Fatalf("notification = %+v, want platform.loop_slow", sent[0])
	}
	if facts.Handler != "tick/reconcile.review_comments" || facts.Hold != 7*time.Second || facts.SlowHolds != 2 || facts.Window != loopSlowWindow {
		t.Fatalf("facts = %+v, want the 7s review hold, the second in 10 minutes", facts)
	}
	if sent[0].Message.Severity != "warning" {
		t.Fatalf("severity = %q, want warning", sent[0].Message.Severity)
	}
}

func TestServedSnapshot_AnswersFromTheLastSnapshotWithinTheBound(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		o := &Orchestrator{snapshotCh: make(chan snapshotRequest, 4), loop: newLoopMonitor(time.Now)}
		served := o.ServedSnapshotFunc()

		start := time.Now()
		if _, err := served(); err == nil {
			t.Fatal("served() with no snapshot built = nil error, want the snapshot unavailable")
		}
		if waited := time.Since(start); waited != snapshotReplyBound {
			t.Fatalf("served() with no snapshot waited %v, want %v", waited, snapshotReplyBound)
		}

		built := time.Now()
		o.lastSnapshot.Store(&RuntimeSnapshotResult{GeneratedAt: built, MaxSessions: 7})
		o.loop.begin(loopHandlerTick)
		o.loop.enter("reconcile.review_comments")
		time.Sleep(2 * time.Second)

		start = time.Now()
		snap, err := served()
		if err != nil {
			t.Fatalf("served() = %v, want the last snapshot", err)
		}
		if waited := time.Since(start); waited != snapshotReplyBound {
			t.Fatalf("served() waited %v, want %v", waited, snapshotReplyBound)
		}
		if !snap.Stale || snap.MaxSessions != 7 || !snap.GeneratedAt.Equal(built) {
			t.Fatalf("served() = stale %v, max sessions %d, generated %v; want the last snapshot, stale", snap.Stale, snap.MaxSessions, snap.GeneratedAt)
		}
		if snap.Loop.BusyHandler != "tick/reconcile.review_comments" || snap.Loop.BusyNow != 2*time.Second+snapshotReplyBound {
			t.Fatalf("served() loop = %+v, want the review pass busy for %v", snap.Loop, 2*time.Second+snapshotReplyBound)
		}
	})
}

func TestServedSnapshot_FreshWhenTheLoopAnswers(t *testing.T) {
	t.Parallel()

	o := budgetOrchestrator(NewState(60000, 4, 0, nil, AgentTotals{}), budgetTickConfig(0), &stubStore{}, &candidateTrackerAdapter{mockTrackerAdapter: &mockTrackerAdapter{}})
	o.drainTimeout = 100 * time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	var running sync.WaitGroup
	running.Go(func() { o.Run(ctx) })
	t.Cleanup(func() {
		cancel()
		running.Wait()
	})

	snap, err := o.ServedSnapshotFunc()()
	if err != nil {
		t.Fatalf("served() = %v", err)
	}
	if snap.Stale {
		t.Fatal("served() from an idle loop is stale, want fresh")
	}
	if kept := o.lastSnapshot.Load(); kept == nil || !kept.GeneratedAt.Equal(snap.GeneratedAt) {
		t.Fatalf("last snapshot = %v, want the one served", kept)
	}
}

// blockingForge blocks FetchPendingReviews until release closes, as a
// forge read that hangs does. Any other call panics on the nil adapter.
type blockingForge struct {
	domain.SCMAdapter
	entered chan struct{}
	release chan struct{}
}

func (f *blockingForge) FetchPendingReviews(ctx context.Context, _ int, _, _ string) ([]domain.ReviewComment, error) {
	f.entered <- struct{}{}
	select {
	case <-f.release:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *blockingForge) GetReviewDecision(context.Context, int, string, string) (domain.ReviewDecision, error) {
	return "", nil
}

// TestServedSnapshot_BlockedForgeServesTheLastSnapshot is the hold the
// state route saw in production: the poll tick's review pass waits on the
// forge, and the served snapshot still answers, stale, naming the pass.
func TestServedSnapshot_BlockedForgeServesTheLastSnapshot(t *testing.T) {
	t.Parallel()

	forge := &blockingForge{entered: make(chan struct{}, 1), release: make(chan struct{})}
	metrics := &spyMetrics{}
	wm := budgetTickConfig(0)
	regs := passingPreflightRegistries()
	regs.ReloadWorkflow = func() error { return nil }
	regs.ConfigFunc = wm.Config
	tracker := &candidateTrackerAdapter{mockTrackerAdapter: &mockTrackerAdapter{}}
	o := NewOrchestrator(OrchestratorParams{
		State:           NewState(60000, 4, 0, nil, AgentTotals{}),
		Logger:          discardLogger(),
		TrackerAdapter:  tracker,
		Router:          route.NewRouter(tracker, nil),
		AgentAdapter:    &mockAgentAdapter{},
		WorkflowManager: wm,
		Store:           &stubStore{},
		PreflightParams: regs,
		SCMAdapter:      forge,
		Metrics:         metrics,
	})
	o.drainTimeout = 100 * time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	var running sync.WaitGroup
	running.Go(func() { o.Run(ctx) })
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(forge.release) }) }
	t.Cleanup(func() {
		release()
		cancel()
		running.Wait()
	})

	served := o.ServedSnapshotFunc()
	before, err := served()
	if err != nil || before.Stale {
		t.Fatalf("served() before the hold = stale %v, %v; want fresh", before.Stale, err)
	}

	added := make(chan struct{})
	if outcome := o.runOnLoop(ctx, func(context.Context) {
		key := ReactionKey("iss-review", ReactionKindReview)
		o.state.PendingReactions[key] = &PendingReaction{
			IssueID:        "iss-review",
			Identifier:     "TEST-REVIEW",
			Kind:           ReactionKindReview,
			CreatedAt:      time.Now().UTC(),
			PullRequestURL: "https://example.test/acme/web/pull/7",
			KindData:       &ReviewReactionData{PRNumber: 7, Owner: "acme", Repo: "web"},
		}
		close(added)
	}); outcome != loopRan {
		t.Fatalf("runOnLoop() = %v, want ran", outcome)
	}
	<-added
	o.RefreshFunc()()
	waittest.Receive(t, forge.entered, "the review pass reading the forge")

	snap, err := served()
	if err != nil {
		t.Fatalf("served() while the forge blocks = %v, want the last snapshot", err)
	}
	if !snap.Stale {
		t.Fatal("served() while the forge blocks is fresh, want stale")
	}
	if snap.Loop.BusyHandler != "tick/reconcile.review_comments" {
		t.Fatalf("busy handler = %q, want tick/reconcile.review_comments", snap.Loop.BusyHandler)
	}
	if snap.Loop.BusyNow < snapshotReplyBound {
		t.Fatalf("busy now = %v, want at least the %v bound", snap.Loop.BusyNow, snapshotReplyBound)
	}
	if age := time.Since(snap.GeneratedAt); age < snap.Loop.BusyNow {
		t.Fatalf("snapshot age %v is below the hold %v, want a snapshot built before the hold", age, snap.Loop.BusyNow)
	}

	release()
	waittest.Until(t, func() error {
		metrics.mu.Lock()
		defer metrics.mu.Unlock()
		for _, h := range metrics.loopHolds {
			if h.handler == "tick/reconcile.review_comments" && h.seconds >= snapshotReplyBound.Seconds() {
				return nil
			}
		}
		return fmt.Errorf("observed holds = %v, want the review pass hold", metrics.loopHolds)
	})
	if st := o.loop.status(); st.SlowestHandler != "tick/reconcile.review_comments" || st.MaxHold < snapshotReplyBound {
		t.Fatalf("loop status = %+v, want the review pass as the slowest hold", st)
	}
	after, err := served()
	if err != nil || after.Stale {
		t.Fatalf("served() after the hold = stale %v, %v; want fresh", after.Stale, err)
	}
}
