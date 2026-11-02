package orchestrator

import (
	"context"
	"log/slog"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
)

// DispatchPause is a pause of dispatch an operator asked for. While it
// holds, the instance starts no run: no first dispatch, retry,
// continuation or stage hop. Runs already going continue to their end.
// The pause lives in memory only, so a restart clears it.
type DispatchPause struct {
	Since  time.Time // UTC: when the first pause request was taken
	By     string    // who asked for the pause
	Reason string    // why, as the request said; may be empty
}

// dispatchPauseState is the event loop's record of a pause: the pause
// and the retries it held, which resume makes due again.
type dispatchPauseState struct {
	DispatchPause
	heldRetries map[string]struct{}
}

// DispatchPauseStatus is what a request to pause or resume dispatch did.
type DispatchPauseStatus int

const (
	DispatchPauseDone      DispatchPauseStatus = iota + 1 // the loop took the request
	DispatchPauseDraining                                 // shutdown began before the loop took the request; nothing changed
	DispatchPauseWithdrawn                                // the caller's context ended before the loop took the request; nothing changed
)

// DispatchPauseResult is the answer to a request to pause or resume
// dispatch. Pause is the pause in force after the request, nil when
// dispatch runs; it is set only with [DispatchPauseDone].
type DispatchPauseResult struct {
	Status DispatchPauseStatus
	Pause  *DispatchPause
}

// DispatchPauseFunc returns a function that pauses dispatch when pause is
// true and resumes it otherwise. Pausing a paused instance keeps the first
// pause, and resuming a running one changes nothing. The function is safe
// for concurrent use and returns once the event loop has taken the
// request, ctx has ended, or shutdown has begun.
func (o *Orchestrator) DispatchPauseFunc() func(ctx context.Context, pause bool, by, reason string) DispatchPauseResult {
	return func(ctx context.Context, pause bool, by, reason string) DispatchPauseResult {
		var result DispatchPauseResult
		switch o.runOnLoop(ctx, func(context.Context) {
			if pause {
				o.pauseDispatchOnLoop(by, reason)
			} else {
				o.resumeDispatchOnLoop()
			}
			result = DispatchPauseResult{Status: DispatchPauseDone, Pause: o.state.dispatchPause.snapshot()}
		}) {
		case loopRan:
			return result
		case loopDraining:
			return DispatchPauseResult{Status: DispatchPauseDraining}
		default:
			return DispatchPauseResult{Status: DispatchPauseWithdrawn}
		}
	}
}

// pauseDispatchOnLoop records a pause unless one holds. Must run on the
// event loop.
func (o *Orchestrator) pauseDispatchOnLoop(by, reason string) {
	if o.state.dispatchPause != nil {
		return
	}
	o.state.dispatchPause = &dispatchPauseState{
		DispatchPause: DispatchPause{Since: time.Now().UTC(), By: by, Reason: reason},
		heldRetries:   map[string]struct{}{},
	}
	o.logger.Info("dispatch paused",
		slog.String("by", by),
		slog.String("reason", reason),
		slog.Int("running", len(o.state.Running)),
	)
	o.notifyObservers()
}

// resumeDispatchOnLoop ends the pause, makes each retry it held due now,
// and asks for a tick so waiting candidates dispatch at once. Must run on
// the event loop.
func (o *Orchestrator) resumeDispatchOnLoop() {
	paused := o.state.dispatchPause
	if paused == nil {
		return
	}
	o.state.dispatchPause = nil
	now := time.Now()
	for issueID := range paused.heldRetries {
		entry, ok := o.state.RetryAttempts[issueID]
		if !ok {
			continue
		}
		if entry.TimerHandle != nil {
			entry.TimerHandle.Stop()
		}
		entry.scheduledAt = now
		entry.scheduledDelayMS = 0
		entry.DueAtMS = now.UnixMilli()
		entry.TimerHandle = time.AfterFunc(0, func() { o.onRetryFire(issueID) })
	}
	o.logger.Info("dispatch resumed",
		slog.String("paused_by", paused.By),
		slog.Duration("paused_for", now.Sub(paused.Since)),
		slog.Int("retries_due", len(paused.heldRetries)),
	)
	o.RefreshFunc()()
	o.notifyObservers()
}

// snapshot returns a copy of the pause, or nil when dispatch runs.
func (p *dispatchPauseState) snapshot() *DispatchPause {
	if p == nil {
		return nil
	}
	return new(p.DispatchPause)
}

// pausedDispatchInput returns no candidate while dispatch is paused, and
// input otherwise.
func pausedDispatchInput(state *State, input []domain.Issue) []domain.Issue {
	if state.dispatchPause != nil {
		return nil
	}
	return input
}

// holdRetryForDispatchPause re-arms the popped retry of issueID after
// recheck when dispatch is paused, records it for resume, and reports
// whether it did. Must run on the event loop.
func holdRetryForDispatchPause(state *State, issueID string, recheck time.Duration, rearmHeld func(time.Time), log *slog.Logger) bool {
	paused := state.dispatchPause
	if paused == nil {
		return false
	}
	rearmHeld(time.Now().Add(recheck))
	paused.heldRetries[issueID] = struct{}{}
	log.Debug("retry held by dispatch pause",
		slog.String("paused_by", paused.By),
	)
	return true
}
