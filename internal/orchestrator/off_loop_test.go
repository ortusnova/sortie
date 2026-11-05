package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/environment"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/waittest"
)

// prSubject is pull request o/kit#n as an off-loop subject.
func prSubject(n int) offLoopSubject {
	return offLoopSubject{Kind: offLoopPullRequest, Key: "o/kit#" + strconv.Itoa(n)}
}

// appliedResults counts, on the event loop, the results applied per
// subject. Only the loop reads or writes it.
type appliedResults map[offLoopSubject]int

// ciCall reads pull request n's CI status from the harness's forge and
// counts its result in applied.
func (h *loopHarness) ciCall(n int, applied appliedResults) offLoopCall {
	subject := prSubject(n)
	return offLoopCall{
		Subject: subject,
		Kind:    "ci_status",
		Run: func(ctx context.Context) func(context.Context) {
			if _, err := h.forge.GetCIStatus(ctx, n, "o", "kit"); err != nil {
				return nil
			}
			return func(context.Context) { applied[subject]++ }
		},
	}
}

// appliedTo returns, read on the loop, how many results subject has had
// applied.
func (h *loopHarness) appliedTo(t *testing.T, applied appliedResults, subject offLoopSubject) int {
	t.Helper()
	var n int
	h.onLoop(t, func(context.Context) { n = applied[subject] })
	return n
}

// awaitIdle waits until the runner holds no call, queued or running.
func (h *loopHarness) awaitIdle(t *testing.T) {
	t.Helper()
	waittest.Until(t, func() error {
		got := h.getState(t).OffLoop
		if len(got.InFlight) != 0 || got.QueueLength != 0 {
			return fmt.Errorf("off-loop runner holds %d running and %d queued calls", len(got.InFlight), got.QueueLength)
		}
		return nil
	})
}

func inFlightSubjects(snap SnapshotOffLoop) map[string]int {
	got := map[string]int{}
	for _, c := range snap.InFlight {
		got[c.Subject]++
	}
	return got
}

// One call per subject: while a subject's call blocks, a second start
// of it reports in_flight and makes no adapter call, and the state API
// lists the call with its subject, kind and start. Calls of every adapter
// family block at once without holding the loop.
func TestOffLoop_SingleFlightPerSubject(t *testing.T) {
	t.Parallel()
	gate := newBlockGate()
	h := newLoopHarness(t, openRepeatedFailureStore(t), gate, config.DefaultOffLoopWorkers)
	applied := appliedResults{}

	story := offLoopSubject{Kind: offLoopStory, Key: "id-7"}
	repo := offLoopSubject{Kind: offLoopRepository, Key: "o/kit"}
	check := offLoopSubject{Kind: offLoopCheck, Key: "fault-1"}
	calls := []offLoopCall{
		h.ciCall(1, applied),
		{Subject: story, Kind: "fetch_issue", Run: func(ctx context.Context) func(context.Context) {
			_, err := h.tracker.FetchIssueByID(ctx, "id-7")
			if err != nil {
				return nil
			}
			return func(context.Context) { applied[story]++ }
		}},
		{Subject: repo, Kind: "git_refresh", Run: func(ctx context.Context) func(context.Context) {
			if h.git.Refresh(ctx, nil, environment.Workspace{Key: "ONS-7"}) != nil {
				return nil
			}
			return func(context.Context) { applied[repo]++ }
		}},
		{Subject: check, Kind: "fault_check", Run: func(ctx context.Context) func(context.Context) {
			if h.command.checkFault(ctx, PlatformFaultCheck{FaultID: "fault-1"}) != nil {
				return nil
			}
			return func(context.Context) { applied[check]++ }
		}},
	}
	h.stories.add("id-7", "ONS-7", "Todo")

	tick := func(want offLoopStart) harnessState {
		return h.assertLoopFree(t, func(ctx context.Context) {
			for _, c := range calls {
				if got := h.o.startOffLoop(ctx, c); got != want {
					t.Errorf("start %s = %s, want %s", c.Subject, got, want)
				}
			}
		})
	}

	tick(offLoopStarted)
	gate.awaitEntered(t, len(calls))
	state := tick(offLoopInFlight)

	if len(state.OffLoop.InFlight) != len(calls) {
		t.Fatalf("in flight = %+v, want one call per subject", state.OffLoop.InFlight)
	}
	for subject, n := range inFlightSubjects(state.OffLoop) {
		if n != 1 {
			t.Errorf("subject %s has %d calls in flight, want 1", subject, n)
		}
	}
	for _, c := range state.OffLoop.InFlight {
		if c.Kind == "" || c.StartedAt.IsZero() {
			t.Errorf("in-flight call %+v lacks its kind or start", c)
		}
	}
	if state.OffLoop.QueueLength != 0 {
		t.Errorf("queue length = %d, want 0", state.OffLoop.QueueLength)
	}
	for _, name := range []string{forgeCall("GetCIStatus", 1), "tracker.FetchIssueByID", "git.Refresh ONS-7", "command fault-check fault-1"} {
		if n := gate.count(name); n != 1 {
			t.Errorf("%s made %d times, want 1", name, n)
		}
	}

	gate.release()
	h.awaitIdle(t)
	for _, c := range calls {
		if n := h.appliedTo(t, applied, c.Subject); n != 1 {
			t.Errorf("results applied to %s = %d, want 1", c.Subject, n)
		}
	}

	// Once applied, the subject starts again.
	var again offLoopStart
	h.onLoop(t, func(ctx context.Context) { again = h.o.startOffLoop(ctx, h.ciCall(1, applied)) })
	if again != offLoopStarted {
		t.Errorf("start after the result applied = %s, want started", again)
	}
	h.awaitIdle(t)
	if n := h.appliedTo(t, applied, prSubject(1)); n != 2 {
		t.Errorf("results applied to %s = %d, want 2", prSubject(1), n)
	}
}

// The pool runs at most off_loop.workers calls at once; the rest wait in
// the queue, which the state API counts, and run as workers free up.
func TestOffLoop_PoolBound(t *testing.T) {
	t.Parallel()
	gate := newBlockGate()
	const workers, calls = 2, 5
	h := newLoopHarness(t, openRepeatedFailureStore(t), gate, workers)
	applied := appliedResults{}

	var running, most atomic.Int32
	bounded := func(n int) offLoopCall {
		c := h.ciCall(n, applied)
		run := c.Run
		c.Run = func(ctx context.Context) func(context.Context) {
			now := running.Add(1)
			defer running.Add(-1)
			for prev := most.Load(); now > prev && !most.CompareAndSwap(prev, now); prev = most.Load() {
			}
			return run(ctx)
		}
		return c
	}

	var starts []offLoopStart
	state := h.assertLoopFree(t, func(ctx context.Context) {
		for n := 1; n <= calls; n++ {
			starts = append(starts, h.o.startOffLoop(ctx, bounded(n)))
		}
	})
	want := []offLoopStart{offLoopStarted, offLoopStarted, offLoopQueued, offLoopQueued, offLoopQueued}
	if fmt.Sprint(starts) != fmt.Sprint(want) {
		t.Errorf("starts = %v, want %v", starts, want)
	}
	if len(state.OffLoop.InFlight) != workers || state.OffLoop.QueueLength != calls-workers {
		t.Errorf("state: %d in flight, queue %d; want %d and %d", len(state.OffLoop.InFlight), state.OffLoop.QueueLength, workers, calls-workers)
	}
	if state.OffLoop.Workers != workers {
		t.Errorf("state workers = %d, want %d", state.OffLoop.Workers, workers)
	}

	gate.awaitEntered(t, workers)
	if n := gate.blockedNow(); n != workers {
		t.Errorf("calls blocked at the forge = %d, want %d", n, workers)
	}
	// A queued subject is in flight too: it is not started twice.
	var again offLoopStart
	h.onLoop(t, func(ctx context.Context) { again = h.o.startOffLoop(ctx, bounded(calls)) })
	if again != offLoopInFlight {
		t.Errorf("start of a queued subject = %s, want in_flight", again)
	}

	gate.release()
	h.awaitIdle(t)
	for n := 1; n <= calls; n++ {
		if got := h.appliedTo(t, applied, prSubject(n)); got != 1 {
			t.Errorf("results applied to %s = %d, want 1", prSubject(n), got)
		}
	}
	if m := most.Load(); m > workers {
		t.Errorf("calls running at once = %d, want at most %d", m, workers)
	}
}

// A call that outlives its timeout is recorded with its subject and kind
// on the state API, applies nothing, and its subject starts again on the
// next tick.
func TestOffLoop_Timeout(t *testing.T) {
	t.Parallel()
	gate := newBlockGate()
	h := newLoopHarness(t, openRepeatedFailureStore(t), gate, config.DefaultOffLoopWorkers)
	applied := appliedResults{}

	call := h.ciCall(3, applied)
	call.Timeout = time.Millisecond
	h.assertLoopFree(t, func(ctx context.Context) {
		if got := h.o.startOffLoop(ctx, call); got != offLoopStarted {
			t.Errorf("start = %s, want started", got)
		}
	})

	var state harnessState
	waittest.Until(t, func() error {
		state = h.getState(t)
		if len(state.OffLoop.Timeouts) == 0 || len(state.OffLoop.InFlight) != 0 {
			return fmt.Errorf("timeouts %+v, in flight %+v", state.OffLoop.Timeouts, state.OffLoop.InFlight)
		}
		return nil
	})
	got := state.OffLoop.Timeouts[0]
	if got.Subject != prSubject(3).String() || got.Kind != "ci_status" || got.StartedAt.IsZero() || got.TimedOutAt.Before(got.StartedAt) {
		t.Errorf("timeout record = %+v, want %s ci_status with its start and end", got, prSubject(3))
	}
	if n := h.appliedTo(t, applied, prSubject(3)); n != 0 {
		t.Errorf("results applied after the timeout = %d, want 0", n)
	}

	call.Timeout = 0
	h.assertLoopFree(t, func(ctx context.Context) {
		if got := h.o.startOffLoop(ctx, call); got != offLoopStarted {
			t.Errorf("start on the tick after the timeout = %s, want started", got)
		}
	})
	gate.release()
	h.awaitIdle(t)
	if n := h.appliedTo(t, applied, prSubject(3)); n != 1 {
		t.Errorf("results applied after the restart = %d, want 1", n)
	}
}

// The default timeout ends a call that never returns, and frees its
// worker for the queue, without the clock of the test advancing it.
func TestOffLoopRunner_DefaultTimeoutFreesTheWorker(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var wg sync.WaitGroup
		loop := make(chan func(context.Context))
		r := newOffLoopRunner(1, &wg, slog.New(slog.DiscardHandler), func(_ context.Context, apply func(context.Context)) bool {
			loop <- apply
			return true
		})
		ctx := t.Context()
		start := time.Now()
		hang := offLoopCall{Subject: prSubject(1), Kind: "hang", Run: func(ctx context.Context) func(context.Context) {
			<-ctx.Done()
			return func(context.Context) { t.Error("a timed-out call's result was applied") }
		}}
		ran := false
		next := offLoopCall{Subject: prSubject(2), Kind: "next", Run: func(context.Context) func(context.Context) {
			return func(context.Context) { ran = true }
		}}
		if got := r.start(ctx, hang); got != offLoopStarted {
			t.Fatalf("start = %s, want started", got)
		}
		if got := r.start(ctx, next); got != offLoopQueued {
			t.Fatalf("start with the worker busy = %s, want queued", got)
		}

		// The timed-out call frees its subject, and the queued call runs on
		// the freed worker; their results reach the loop in either order.
		(<-loop)(ctx)
		if took := time.Since(start); took != defaultOffLoopTimeout {
			t.Errorf("the call ended after %v, want its default timeout %v", took, defaultOffLoopTimeout)
		}
		(<-loop)(ctx)
		snap := r.snapshot()
		if len(snap.Timeouts) != 1 || snap.Timeouts[0].Subject != prSubject(1).String() || snap.Timeouts[0].Kind != "hang" {
			t.Errorf("timeouts = %+v, want the hung call", snap.Timeouts)
		}
		if r.inFlight(prSubject(1)) {
			t.Error("the timed-out subject is still in flight")
		}
		if !ran {
			t.Error("the queued call did not run after the timeout")
		}
		wg.Wait()
	})
}

// reviewReactionCall reads the review comments of key's pull request off
// the loop, started on its head; applied counts each result applied.
func (h *loopHarness) reviewReactionCall(key string, applied appliedResults) (offLoopCall, bool) {
	pr := h.o.state.PendingReactions[key]
	data, ok := pr.KindData.(*ReviewReactionData)
	if !ok {
		return offLoopCall{}, false
	}
	subject := prSubject(data.PRNumber)
	head := func() any {
		if cur := h.o.state.PendingReactions[key]; cur != nil {
			if d, ok := cur.KindData.(*ReviewReactionData); ok {
				return d.SHA
			}
		}
		return nil
	}
	return offLoopCall{
		Subject: subject,
		Kind:    "review_comments",
		Version: data.SHA,
		Current: head,
		Run: func(ctx context.Context) func(context.Context) {
			comments, err := h.forge.FetchPendingReviews(ctx, data.PRNumber, "o", "kit")
			if err != nil {
				return nil
			}
			return func(context.Context) { applied[subject] += len(comments) }
		},
	}, true
}

// addReviewReaction adds ONS-<n> running with a pending review-comment
// reaction on o/kit#<n> at its head.
func (h *loopHarness) addReviewReaction(n int) string {
	id, identifier := fmt.Sprintf("id-%d", n), fmt.Sprintf("ONS-%d", n)
	key := ReactionKey(id, ReactionKindReview)
	h.o.state.PendingReactions[key] = &PendingReaction{
		IssueID: id, Identifier: identifier, Kind: ReactionKindReview, Attempt: 1,
		KindData: &ReviewReactionData{PRNumber: n, Owner: "o", Repo: "kit", Branch: "sortie/" + identifier, SHA: headSHA(n)},
	}
	return key
}

// A result whose subject's version changed while its call ran is dropped
// on the loop: the pull request's head moved, so the comments read at the
// old head are not applied.
func TestOffLoop_StaleResultDropped(t *testing.T) {
	t.Parallel()
	gate := newBlockGate()
	h := newLoopHarness(t, openRepeatedFailureStore(t), gate, config.DefaultOffLoopWorkers)
	applied := appliedResults{}
	var stale, fresh string
	h.onLoop(t, func(context.Context) {
		stale, fresh = h.addReviewReaction(4), h.addReviewReaction(5)
	})

	h.assertLoopFree(t, func(ctx context.Context) {
		for _, key := range []string{stale, fresh} {
			call, _ := h.reviewReactionCall(key, applied)
			h.o.startOffLoop(ctx, call)
		}
	})
	gate.awaitEntered(t, 2)
	h.onLoop(t, func(context.Context) {
		h.o.state.PendingReactions[stale].KindData.(*ReviewReactionData).SHA = headSHA(40)
	})

	gate.release()
	h.awaitIdle(t)
	if n := h.appliedTo(t, applied, prSubject(4)); n != 0 {
		t.Errorf("comments applied after the head moved = %d, want the result dropped", n)
	}
	if n := h.appliedTo(t, applied, prSubject(5)); n != 1 {
		t.Errorf("comments applied at an unchanged head = %d, want 1", n)
	}
}

// Scenario: Many pending reactions. 25 running stories each have a
// pending review-comment reaction; no tick holds the loop while their
// calls block, and each result is applied when its call returns.
func TestOffLoop_ManyPendingReactions(t *testing.T) {
	t.Parallel()
	gate := newBlockGate()
	h := newLoopHarness(t, openRepeatedFailureStore(t), gate, config.DefaultOffLoopWorkers)
	applied := appliedResults{}
	const stories = 25
	var keys []string
	h.onLoop(t, func(context.Context) {
		for n := 1; n <= stories; n++ {
			keys = append(keys, h.addReviewReaction(n))
		}
	})
	tick := func(ctx context.Context) {
		for _, key := range keys {
			if call, ok := h.reviewReactionCall(key, applied); ok {
				h.o.startOffLoop(ctx, call)
			}
		}
	}

	for range 3 {
		state := h.assertLoopFree(t, tick)
		if got := len(state.OffLoop.InFlight) + state.OffLoop.QueueLength; got != stories {
			t.Errorf("calls held = %d, want one per story, %d", got, stories)
		}
		if len(state.OffLoop.InFlight) != config.DefaultOffLoopWorkers {
			t.Errorf("calls running = %d, want the %d workers", len(state.OffLoop.InFlight), config.DefaultOffLoopWorkers)
		}
	}

	gate.release()
	h.awaitIdle(t)
	for n := 1; n <= stories; n++ {
		if got := h.appliedTo(t, applied, prSubject(n)); got != 1 {
			t.Errorf("results applied to %s = %d, want 1", prSubject(n), got)
		}
	}
	for n := 1; n <= stories; n++ {
		if got := gate.count(forgeCall("FetchPendingReviews", n)); got != 1 {
			t.Errorf("forge reads of #%d = %d, want 1", n, got)
		}
	}
}

// addRevertedMerge records ONS-121 Done with its merge of o/kit#142, and
// a forge revert of it.
func (h *loopHarness) addRevertedMerge(t *testing.T) {
	t.Helper()
	h.stories.add("id-121", "ONS-121", "Done")
	if err := h.store.UpsertPlanSatisfaction(t.Context(), persistence.PlanSatisfaction{
		IssueID: "id-121", Kind: persistence.PlanSatisfactionMerged, Ref: mergeSHA(142), PR: "o/kit#142", RecordedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	h.forge.mu.Lock()
	h.forge.reverts = []domain.MergedRevert{{Number: 144, PullRequests: []int{142}}}
	h.forge.mu.Unlock()
}

// Scenario: A slow forge. While every forge call blocks, GET
// /api/v1/state answers at once with the loop's state, and each blocked
// subject has one call in flight, however many ticks start them.
func TestOffLoop_SlowForge(t *testing.T) {
	t.Parallel()
	gate := newBlockGate()
	h := newLoopHarness(t, openRepeatedFailureStore(t), gate, config.DefaultOffLoopWorkers)
	h.addRevertedMerge(t)
	applied := appliedResults{}
	cfg := h.o.workflowManager.Config()
	now := time.Now()

	tick := func(at time.Time) func(context.Context) {
		return func(ctx context.Context) {
			h.o.detectReverts(ctx, cfg, at)
			for n := 1; n <= 3; n++ {
				h.o.startOffLoop(ctx, h.ciCall(n, applied))
			}
		}
	}
	h.assertLoopFree(t, tick(now))
	gate.awaitEntered(t, 4)
	var state harnessState
	for i := range 3 {
		state = h.assertLoopFree(t, tick(now.Add(time.Duration(i+1)*time.Hour)))
	}
	want := map[string]int{revertScanSubject.String(): 1}
	for n := 1; n <= 3; n++ {
		want[prSubject(n).String()] = 1
	}
	if got := inFlightSubjects(state.OffLoop); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("in flight = %v, want %v", got, want)
	}
	if n := gate.count("forge.FindMergedReverts o/kit"); n != 1 {
		t.Errorf("revert listings = %d, want 1", n)
	}
	if h.stories.stateOf("id-121") != "Done" {
		t.Error("the story moved before the forge answered")
	}

	gate.release()
	h.awaitIdle(t)
	if state := h.stories.stateOf("id-121"); state != "Todo" {
		t.Errorf("ONS-121 after the forge answered = %q, want Todo", state)
	}
}

// A restart with calls in flight: shutdown cancels the blocked calls and
// applies none of their results, and the restarted orchestrator, on the
// same store, starts the same subjects again.
func TestOffLoop_RestartWithCallsInFlight(t *testing.T) {
	t.Parallel()
	store := openRepeatedFailureStore(t)
	first := newLoopHarness(t, store, newBlockGate(), config.DefaultOffLoopWorkers)
	first.addRevertedMerge(t)
	applied := appliedResults{}
	cfg := first.o.workflowManager.Config()
	first.assertLoopFree(t, func(ctx context.Context) {
		first.o.detectReverts(ctx, cfg, time.Now())
		first.o.startOffLoop(ctx, first.ciCall(1, applied))
	})
	first.gate.awaitEntered(t, 2)

	first.stop()
	if n := first.gate.blockedNow(); n != 0 {
		t.Errorf("calls still blocked after shutdown = %d, want them cancelled", n)
	}
	if got := first.o.offLoop.start(t.Context(), first.ciCall(2, applied)); got != offLoopStopped {
		t.Errorf("start after shutdown = %s, want stopped", got)
	}
	if len(applied) != 0 {
		t.Errorf("results applied across shutdown = %v, want none", applied)
	}
	if state := first.stories.stateOf("id-121"); state != "Done" {
		t.Errorf("ONS-121 after the cancelled scan = %q, want Done", state)
	}

	gate := newBlockGate()
	second := newLoopHarness(t, store, gate, config.DefaultOffLoopWorkers)
	second.addRevertedMerge(t)
	state := second.assertLoopFree(t, func(ctx context.Context) {
		second.o.detectReverts(ctx, cfg, time.Now())
		if got := second.o.startOffLoop(ctx, second.ciCall(1, applied)); got != offLoopStarted {
			t.Errorf("start after the restart = %s, want started", got)
		}
	})
	if got := inFlightSubjects(state.OffLoop); got[revertScanSubject.String()] != 1 || got[prSubject(1).String()] != 1 {
		t.Errorf("in flight after the restart = %v, want the revert scan and o/kit#1", got)
	}
	gate.release()
	second.awaitIdle(t)
	if state := second.stories.stateOf("id-121"); state != "Todo" {
		t.Errorf("ONS-121 after the restarted scan = %q, want Todo", state)
	}
	if n := second.appliedTo(t, applied, prSubject(1)); n != 1 {
		t.Errorf("results applied after the restart = %d, want 1", n)
	}
}
