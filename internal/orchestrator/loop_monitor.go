package orchestrator

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
)

const (
	// snapshotReplyBound is how long a served snapshot waits for the event
	// loop before it answers with the last snapshot the loop built.
	snapshotReplyBound = 750 * time.Millisecond

	// loopHoldFloor is the shortest hold the monitor keeps for its
	// windows; shorter holds only reach the metrics.
	loopHoldFloor = 100 * time.Millisecond

	// loopHoldWarn is the hold above which the loop logs a warning.
	loopHoldWarn = 2 * time.Second

	// loopHoldSlow is the hold above which a hold counts as slow.
	loopHoldSlow = 5 * time.Second

	// loopSlowWindow is the window in which a second slow hold publishes
	// platform.loop_slow, and the least time between two of them.
	loopSlowWindow = 10 * time.Minute

	// loopStatusWindow is the window of [LoopStatus.MaxHold].
	loopStatusWindow = 5 * time.Minute

	// loopHoldsKept caps the holds the monitor keeps.
	loopHoldsKept = 256
)

// Event loop handlers, the first part of a hold's name.
const (
	// loopHandlerTick is the poll tick, timed or asked for by a refresh.
	loopHandlerTick             = "tick"
	loopHandlerWorkerExit       = "worker_exit"
	loopHandlerRetryTimer       = "retry_timer"
	loopHandlerAgentEvent       = "agent_event"
	loopHandlerSelfReview       = "self_review"
	loopHandlerTurnStarted      = "turn_started"
	loopHandlerIssueRead        = "issue_read"
	loopHandlerUnaccountedTurns = "unaccounted_turns"
	loopHandlerSnapshot         = "snapshot"
	loopHandlerAction           = "action"
	loopHandlerControl          = "control"
	loopHandlerLeaseSweep       = "lease_sweep"
	loopHandlerStreamBroken     = "stream_broken"
)

// loopHold is one hold of the event loop: when it ended, the name of what
// held it longest, and how long the whole hold lasted.
type loopHold struct {
	At       time.Time
	Handler  string
	Duration time.Duration
}

// LoopStatus is how long the event loop holds its work, as the monitor saw
// it when the status was read.
type LoopStatus struct {
	// BusyNow is how long the hold in progress has lasted; zero while the
	// loop waits for work.
	BusyNow time.Duration

	// BusyHandler names the handler and the phase holding the loop now;
	// empty while the loop waits.
	BusyHandler string

	// MaxHold is the longest hold of at least 100 ms that ended in the last
	// 5 minutes, and SlowestHandler what held it longest; zero and empty
	// when there was none.
	MaxHold        time.Duration
	SlowestHandler string

	// SlowHolds counts the holds over 5 seconds that ended in the last 10
	// minutes.
	SlowHolds int
}

// loopMonitor measures how long each event loop iteration holds the loop.
// Only the loop calls begin, enter and end; status is safe from any
// goroutine.
type loopMonitor struct {
	now func() time.Time

	mu sync.Mutex

	handler      string
	phase        string
	started      time.Time
	phaseStarted time.Time

	// slowPhase is the phase that held the hold in progress longest, and
	// slowPhaseFor how long; slowPhase is empty for the handler's own body.
	slowPhase    string
	slowPhaseFor time.Duration

	holds          []loopHold
	lastSlowNotice time.Time
}

func newLoopMonitor(now func() time.Time) *loopMonitor {
	if now == nil {
		now = time.Now
	}
	return &loopMonitor{now: now}
}

// begin starts the hold of one loop iteration run by handler.
func (m *loopMonitor) begin(handler string) {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handler, m.phase = handler, ""
	m.started, m.phaseStarted = now, now
	m.slowPhase, m.slowPhaseFor = "", -1
}

// enter starts the named phase of the hold in progress. It does nothing
// while no hold is in progress, so a handler run outside the loop can
// mark its phases too.
func (m *loopMonitor) enter(phase string) {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started.IsZero() {
		return
	}
	m.closePhase(now)
	m.phase, m.phaseStarted = phase, now
}

func (m *loopMonitor) closePhase(now time.Time) {
	if d := now.Sub(m.phaseStarted); d > m.slowPhaseFor {
		m.slowPhase, m.slowPhaseFor = m.phase, d
	}
}

func (m *loopMonitor) nameOf(phase string) string {
	if phase == "" {
		return m.handler
	}
	return m.handler + "/" + phase
}

// end finishes the hold in progress and returns it, with the count of slow
// holds in the slow window when it is slow. notice reports that this hold
// is the second slow one in the window and no notice was given within it.
func (m *loopMonitor) end() (hold loopHold, slowHolds int, notice bool) {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started.IsZero() {
		return loopHold{}, 0, false
	}
	m.closePhase(now)
	hold = loopHold{At: now, Handler: m.nameOf(m.slowPhase), Duration: now.Sub(m.started)}
	m.started = time.Time{}

	m.trim(now)
	if hold.Duration >= loopHoldFloor {
		if len(m.holds) == loopHoldsKept {
			m.holds = append(m.holds[:0], m.holds[1:]...)
		}
		m.holds = append(m.holds, hold)
	}
	if hold.Duration <= loopHoldSlow {
		return hold, 0, false
	}
	for _, h := range m.holds {
		if h.Duration > loopHoldSlow {
			slowHolds++
		}
	}
	if slowHolds >= 2 && (m.lastSlowNotice.IsZero() || now.Sub(m.lastSlowNotice) >= loopSlowWindow) {
		m.lastSlowNotice = now
		notice = true
	}
	return hold, slowHolds, notice
}

// trim drops the holds that ended before the slow window.
func (m *loopMonitor) trim(now time.Time) {
	keep := 0
	for keep < len(m.holds) && now.Sub(m.holds[keep].At) > loopSlowWindow {
		keep++
	}
	if keep > 0 {
		m.holds = append(m.holds[:0], m.holds[keep:]...)
	}
}

// status reports the hold in progress and the recent holds.
func (m *loopMonitor) status() LoopStatus {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	var st LoopStatus
	if !m.started.IsZero() {
		st.BusyNow = max(now.Sub(m.started), 0)
		st.BusyHandler = m.nameOf(m.phase)
	}
	for _, h := range m.holds {
		age := now.Sub(h.At)
		if age <= loopStatusWindow && h.Duration > st.MaxHold {
			st.MaxHold, st.SlowestHandler = h.Duration, h.Handler
		}
		if age <= loopSlowWindow && h.Duration > loopHoldSlow {
			st.SlowHolds++
		}
	}
	return st
}

// endLoopHold finishes the hold of the iteration in progress: it records
// the hold in the metrics, warns of a long one, and publishes
// platform.loop_slow when slow holds repeat.
func (o *Orchestrator) endLoopHold(ctx context.Context) {
	hold, slowHolds, notice := o.loop.end()
	if hold.Handler == "" {
		return
	}
	o.metrics.ObserveLoopHold(hold.Handler, hold.Duration.Seconds())
	if hold.Duration > loopHoldWarn {
		o.logger.Warn("event loop held",
			slog.String("handler", hold.Handler),
			slog.Int64("hold_ms", hold.Duration.Milliseconds()),
		)
	}
	if notice {
		o.publishLoopSlow(ctx, hold, slowHolds)
	}
}

// publishLoopSlow routes platform.loop_slow. The event names no story, so
// it never goes to tracker_comment.
func (o *Orchestrator) publishLoopSlow(ctx context.Context, hold loopHold, slowHolds int) {
	o.logger.Warn("event loop slow: holds over 5 seconds repeat",
		slog.String("handler", hold.Handler),
		slog.Int64("hold_ms", hold.Duration.Milliseconds()),
		slog.Int("slow_holds", slowHolds),
	)
	if o.router == nil {
		return
	}
	_, delivery := o.router.Route(orchestratorNotification(domain.EventPlatformLoopSlow, "", domain.NotificationEnvelope{},
		domain.PlatformLoopSlowFacts{
			Handler: hold.Handler, Hold: hold.Duration, SlowHolds: slowHolds, Window: loopSlowWindow,
		})).SplitTrackerComment()
	if delivery.Empty() {
		o.logger.Info("event loop slowness had no destination", slog.String("event_type", string(domain.EventPlatformLoopSlow)))
		return
	}
	deliverDetached(ctx, &o.state.TrackerOpsWg, delivery, o.logger, nil)
}
