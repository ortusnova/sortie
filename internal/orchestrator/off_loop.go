package orchestrator

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
)

// The kinds of subject an off-loop call is single-flight on.
const (
	offLoopStory       = "story"
	offLoopPullRequest = "pull_request"
	offLoopRepository  = "repository"
	offLoopCheck       = "check"
)

// defaultOffLoopTimeout bounds an off-loop call that names no timeout.
const defaultOffLoopTimeout = 30 * time.Second

// offLoopTimeoutsKept is how many timed-out calls the runner remembers.
const offLoopTimeoutsKept = 32

// offLoopSubject is what an off-loop call reads or writes: a story, a
// pull request, a repository or a check. At most one call per subject is
// queued or running.
type offLoopSubject struct {
	Kind string
	Key  string
}

func (s offLoopSubject) String() string { return s.Kind + ":" + s.Key }

// offLoopCall is one forge, tracker, git or command call run off the
// event loop.
type offLoopCall struct {
	Subject offLoopSubject

	// Kind names what the call does, such as "park_record".
	Kind string

	// Version is the subject's version the call starts on, such as a pull
	// request's head or a story's state. It must be comparable.
	Version any

	// Current returns the subject's version now; it runs on the event loop
	// before the result is applied, and a result whose version differs
	// from Version is dropped. Nil never drops a result.
	Current func() any

	// Timeout bounds the call; zero is [defaultOffLoopTimeout].
	Timeout time.Duration

	// Run makes the call off the event loop under a context that ends at
	// the timeout, and returns what to apply on the loop; nil applies
	// nothing. It must not read or write runtime state.
	Run func(ctx context.Context) func(loopCtx context.Context)
}

// offLoopStart is what [offLoopRunner.start] did with a call.
type offLoopStart string

const (
	// offLoopStarted: a worker runs the call.
	offLoopStarted offLoopStart = "started"
	// offLoopQueued: the call waits for a worker.
	offLoopQueued offLoopStart = "queued"
	// offLoopInFlight: a call for the subject is queued or running, so
	// this one is not started.
	offLoopInFlight offLoopStart = "in_flight"
	// offLoopStopped: Sortie is stopping, so no call starts.
	offLoopStopped offLoopStart = "stopped"
)

// offLoopTask is a call the runner holds, queued or running.
type offLoopTask struct {
	call      offLoopCall
	ctx       context.Context
	queuedAt  time.Time
	startedAt time.Time // zero while queued
	cancel    context.CancelFunc
}

// offLoopRunner runs forge, tracker, git and command calls off the event
// loop: at most workers at once, the rest queued in order, one per
// subject, each under its timeout. A finished call's result returns to the
// event loop through deliver and is applied there unless its subject's
// version changed. Its methods are safe from any goroutine.
type offLoopRunner struct {
	// deliver runs apply on the event loop and reports whether it ran.
	deliver func(ctx context.Context, apply func(loopCtx context.Context)) bool
	wg      *sync.WaitGroup
	logger  *slog.Logger
	now     func() time.Time

	mu       sync.Mutex
	workers  int
	running  int
	queue    []*offLoopTask
	subjects map[offLoopSubject]*offLoopTask
	timeouts []SnapshotOffLoopTimeout
	stopped  bool
}

func newOffLoopRunner(workers int, wg *sync.WaitGroup, logger *slog.Logger,
	deliver func(ctx context.Context, apply func(loopCtx context.Context)) bool) *offLoopRunner {
	return &offLoopRunner{
		deliver:  deliver,
		wg:       wg,
		logger:   logger,
		now:      time.Now,
		workers:  cmp.Or(workers, config.DefaultOffLoopWorkers),
		subjects: map[offLoopSubject]*offLoopTask{},
	}
}

// offLoopRunner returns the orchestrator's runner, made on first use with
// the configured workers. Must run on the event loop.
func (o *Orchestrator) offLoopRunner() *offLoopRunner {
	if o.offLoop == nil {
		workers := config.DefaultOffLoopWorkers
		if o.workflowManager != nil {
			workers = o.workflowManager.Config().OffLoop.Workers
		}
		o.offLoop = newOffLoopRunner(workers, &o.state.TrackerOpsWg, o.logger, func(ctx context.Context, apply func(context.Context)) bool {
			return o.runOnLoop(ctx, apply) == loopRan
		})
	}
	return o.offLoop
}

// startOffLoop starts call on the orchestrator's runner. Must run on the
// event loop.
func (o *Orchestrator) startOffLoop(ctx context.Context, call offLoopCall) offLoopStart {
	return o.offLoopRunner().start(ctx, call)
}

// start queues call, or runs it at once when a worker is free. A call
// whose subject has one queued or running is not started and reports
// offLoopInFlight. ctx bounds the call with its timeout.
func (r *offLoopRunner) start(ctx context.Context, call offLoopCall) offLoopStart {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return offLoopStopped
	}
	if _, busy := r.subjects[call.Subject]; busy {
		return offLoopInFlight
	}
	task := &offLoopTask{call: call, ctx: ctx, queuedAt: r.now()}
	r.subjects[call.Subject] = task
	if r.running >= r.workers {
		r.queue = append(r.queue, task)
		return offLoopQueued
	}
	r.launchLocked(task)
	return offLoopStarted
}

// inFlight reports whether a call for subject is queued or running.
func (r *offLoopRunner) inFlight(subject offLoopSubject) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, busy := r.subjects[subject]
	return busy
}

// setWorkers changes how many calls run at once. A lower bound lets the
// running calls finish; a higher one starts queued calls at once.
func (r *offLoopRunner) setWorkers(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workers = cmp.Or(n, config.DefaultOffLoopWorkers)
	r.launchQueuedLocked()
}

// stop cancels every running call, drops the queue and starts no call
// after it. A running call's result is not applied once the loop drains.
func (r *offLoopRunner) stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopped = true
	for _, task := range r.queue {
		delete(r.subjects, task.call.Subject)
	}
	r.queue = nil
	for _, task := range r.subjects {
		if task.cancel != nil {
			task.cancel()
		}
	}
}

// launchLocked runs task on a worker. r.mu must be held.
func (r *offLoopRunner) launchLocked(task *offLoopTask) {
	r.running++
	task.startedAt = r.now()
	timeout := cmp.Or(task.call.Timeout, defaultOffLoopTimeout)
	ctx, cancel := context.WithTimeout(task.ctx, timeout)
	task.cancel = cancel
	r.wg.Go(func() {
		defer cancel()
		r.run(ctx, task)
	})
}

// launchQueuedLocked runs queued tasks while a worker is free. r.mu must
// be held.
func (r *offLoopRunner) launchQueuedLocked() {
	for !r.stopped && len(r.queue) > 0 && r.running < r.workers {
		task := r.queue[0]
		r.queue = r.queue[1:]
		r.launchLocked(task)
	}
}

// run makes task's call and returns its result to the event loop. A call
// that outlives its timeout frees its worker and its subject at once; what
// it returns later is dropped.
func (r *offLoopRunner) run(ctx context.Context, task *offLoopTask) {
	done := make(chan func(context.Context), 1)
	r.wg.Go(func() { done <- task.call.Run(ctx) })
	var apply func(context.Context)
	select {
	case apply = <-done:
	case <-ctx.Done():
	}
	// A call that returns as its context ends has timed out all the same.
	timedOut := errors.Is(ctx.Err(), context.DeadlineExceeded)
	if timedOut {
		apply = nil
	}
	r.mu.Lock()
	r.running--
	if timedOut {
		r.recordTimeoutLocked(task)
	}
	r.launchQueuedLocked()
	r.mu.Unlock()

	log := r.logger.With(slog.String("subject", task.call.Subject.String()), slog.String("call", task.call.Kind))
	if timedOut {
		log.Warn("off-loop call timed out; its subject can start again",
			slog.Duration("timeout", cmp.Or(task.call.Timeout, defaultOffLoopTimeout)))
	}
	delivered := r.deliver(task.ctx, func(loopCtx context.Context) {
		r.release(task)
		if apply == nil {
			return
		}
		if task.call.Current != nil && task.call.Current() != task.call.Version {
			log.Debug("off-loop result dropped: its subject changed while the call ran")
			return
		}
		apply(loopCtx)
	})
	if !delivered {
		r.release(task)
	}
}

// release frees task's subject, so its next call can start.
func (r *offLoopRunner) release(task *offLoopTask) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.subjects[task.call.Subject] == task {
		delete(r.subjects, task.call.Subject)
	}
}

// recordTimeoutLocked keeps task's timeout, the newest last. r.mu must be
// held.
func (r *offLoopRunner) recordTimeoutLocked(task *offLoopTask) {
	r.timeouts = append(r.timeouts, SnapshotOffLoopTimeout{
		Subject: task.call.Subject.String(), Kind: task.call.Kind, StartedAt: task.startedAt, TimedOutAt: r.now(),
	})
	if over := len(r.timeouts) - offLoopTimeoutsKept; over > 0 {
		r.timeouts = slices.Delete(r.timeouts, 0, over)
	}
}

// snapshot returns the runner's running calls, oldest first, its queue
// length and the calls that timed out.
func (r *offLoopRunner) snapshot() SnapshotOffLoop {
	r.mu.Lock()
	defer r.mu.Unlock()
	snap := SnapshotOffLoop{
		Workers:     r.workers,
		InFlight:    []SnapshotOffLoopCall{},
		QueueLength: len(r.queue),
		Timeouts:    slices.Clone(r.timeouts),
	}
	if snap.Timeouts == nil {
		snap.Timeouts = []SnapshotOffLoopTimeout{}
	}
	for _, task := range r.subjects {
		if task.startedAt.IsZero() {
			continue
		}
		snap.InFlight = append(snap.InFlight, SnapshotOffLoopCall{
			Subject: task.call.Subject.String(), Kind: task.call.Kind, StartedAt: task.startedAt,
		})
	}
	slices.SortFunc(snap.InFlight, func(a, b SnapshotOffLoopCall) int {
		return cmp.Or(a.StartedAt.Compare(b.StartedAt), cmp.Compare(a.Subject, b.Subject))
	})
	return snap
}

// SnapshotOffLoop is the off-loop runner at capture: its workers, the
// calls they run, how many calls wait for one, and the calls that timed
// out, the newest last.
type SnapshotOffLoop struct {
	Workers     int
	InFlight    []SnapshotOffLoopCall
	QueueLength int
	Timeouts    []SnapshotOffLoopTimeout
}

// SnapshotOffLoopCall is one call a worker runs: its subject, such as
// "story:<issue id>" or "pull_request:owner/repo#12", what it does, and
// when it started.
type SnapshotOffLoopCall struct {
	Subject   string
	Kind      string
	StartedAt time.Time
}

// SnapshotOffLoopTimeout is one call that outlived its timeout.
type SnapshotOffLoopTimeout struct {
	Subject    string
	Kind       string
	StartedAt  time.Time
	TimedOutAt time.Time
}
