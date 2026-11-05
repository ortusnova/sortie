package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
)

// What defers a supervisor trigger.
const (
	SupervisorGuardReserve  = "reserve"  // less than a fifth of the day's tokens is left
	SupervisorGuardVelocity = "velocity" // the hourly spend is over its limit
)

// supervisorSpendWindow is the span the hourly spend counts.
const supervisorSpendWindow = time.Hour

// supervisorReserveDivisor sets the reserve: once less than one part in
// this many of the daily token budget is left, only the triggers
// reserveAdmits starts a session.
const supervisorReserveDivisor = 5

// SupervisorDeferral is a trigger a spend guard keeps from starting a
// session: Guard is SupervisorGuardReserve or SupervisorGuardVelocity.
// It starts one once the guard lifts.
type SupervisorDeferral struct {
	SupervisorTrigger
	Guard string
}

// spendSample is the tokens the supervisor spent by at.
type spendSample struct {
	at     time.Time
	tokens int64
}

// supervisorAvailability is whether the supervisor can start the
// sessions it would start: reason is empty while it can.
type supervisorAvailability struct {
	reason   string
	detail   string
	since    time.Time
	resetsAt time.Time
}

// noteSpend adds tokens spent at to the hourly spend.
func (s *supervisorState) noteSpend(at time.Time, tokens int64) {
	if tokens > 0 {
		s.spend = append(s.spend, spendSample{at: at.UTC(), tokens: tokens})
	}
}

// hourSpend returns the tokens spent in the hour before now, without the
// running session's tokens not yet sampled.
func (s *supervisorState) hourSpend(now time.Time) int64 {
	var sum int64
	for _, x := range s.spend {
		if now.Sub(x.at) < supervisorSpendWindow {
			sum += x.tokens
		}
	}
	return sum
}

// pruneSpend drops the samples older than the hour before now.
func (s *supervisorState) pruneSpend(now time.Time) {
	s.spend = slices.DeleteFunc(s.spend, func(x spendSample) bool { return now.Sub(x.at) >= supervisorSpendWindow })
}

// spendResetsAt returns when the hourly spend falls under pace if
// nothing more is spent; zero when it already is.
func (s *supervisorState) spendResetsAt(now time.Time, pace int64) time.Time {
	window := make([]spendSample, 0, len(s.spend))
	var sum int64
	for _, x := range s.spend {
		if now.Sub(x.at) < supervisorSpendWindow {
			window = append(window, x)
			sum += x.tokens
		}
	}
	slices.SortFunc(window, func(a, b spendSample) int { return a.at.Compare(b.at) })
	for _, x := range window {
		if sum < pace {
			break
		}
		sum -= x.tokens
		if sum < pace {
			return x.at.Add(supervisorSpendWindow)
		}
	}
	return time.Time{}
}

// hourlyPace returns the even hourly pace of sc's daily token budget and
// the hourly spend over which only sweeps start a session.
func hourlyPace(sc config.SupervisorConfig) (pace, limit int64) {
	pace = int64(sc.MaxTokensPerDay) / 24
	return pace, pace * int64(max(sc.VelocityFactor, 1))
}

// inReserve reports whether less than a fifth of the day's token budget
// is left after used.
func inReserve(sc config.SupervisorConfig, used int64) bool {
	limit := int64(sc.MaxTokensPerDay)
	return (limit-used)*supervisorReserveDivisor < limit
}

// reserveAdmits reports whether a trigger of kind starts a session while
// the reserve holds: a park, a platform fault, and a sweep.
func reserveAdmits(kind string) bool {
	switch kind {
	case SupervisorTriggerPark, SupervisorTriggerUnclassifiedFailure, SupervisorTriggerSweep:
		return true
	}
	return false
}

// guardBlocks reports whether guard keeps a trigger of kind from starting
// a session.
func guardBlocks(guard, kind string) bool {
	switch guard {
	case SupervisorGuardVelocity:
		return kind != SupervisorTriggerSweep
	case SupervisorGuardReserve:
		return !reserveAdmits(kind)
	}
	return false
}

// sampleSupervisorSpend adds what the running session spent since the
// last sample to the hourly spend. Must run on the event loop.
func (o *Orchestrator) sampleSupervisorSpend(now time.Time) {
	s := o.state.supervisor
	if s.run == nil {
		return
	}
	if tokens := s.run.tokens.Load(); tokens > s.run.sampled {
		s.noteSpend(now, tokens-s.run.sampled)
		s.run.sampled = tokens
	}
}

// guardSupervisorSpend applies the spend guards to the waiting triggers.
// When the hourly spend goes over supervisor.velocity_factor times the
// even hourly pace of the daily budget, it publishes
// supervisor.loop_suspected, and only sweeps start a session until the
// hourly spend is back under the pace; while less than a fifth of the
// day's tokens is left, only reserveAdmits's triggers do. It defers every
// other waiting trigger, and gives back to the waiting triggers each
// deferred one its guard no longer blocks. While the day's budget is spent
// it defers nothing and drops what it deferred. Must run on the event
// loop.
func (o *Orchestrator) guardSupervisorSpend(ctx context.Context, sc config.SupervisorConfig, now time.Time) {
	s := o.state.supervisor
	s.pruneSpend(now)
	pace, limit := hourlyPace(sc)
	hour := s.hourSpend(now)
	switch {
	case !s.velocity && hour > limit:
		s.velocity = true
		o.logger.Warn("supervisor loop suspected: the hourly spend is over its limit, only sweeps start a session",
			slog.Int64("hour_tokens", hour), slog.Int64("hour_tokens_limit", limit), slog.Int64("pace", pace))
		o.publishSupervisorEvent(ctx, domain.EventSupervisorLoopSuspected, domain.SupervisorLoopSuspectedFacts{
			HourlyTokens: hour, HourlyLimit: limit, Pace: pace,
		}, o.logger)
		o.notifyObservers()
	case s.velocity && hour < pace:
		s.velocity = false
		o.logger.Info("supervisor hourly spend is back under the pace, every trigger starts a session again",
			slog.Int64("hour_tokens", hour), slog.Int64("pace", pace))
		o.notifyObservers()
	}

	if s.budgetStop(sc) != "" {
		if len(s.deferred) > 0 {
			s.deferred = nil
			o.notifyObservers()
		}
		return
	}
	guard := ""
	switch {
	case s.velocity:
		guard = SupervisorGuardVelocity
	case inReserve(sc, s.tokensUsed):
		guard = SupervisorGuardReserve
	}
	changed := false
	kept := s.deferred[:0]
	for _, d := range s.deferred {
		if guardBlocks(guard, d.Kind) {
			changed = changed || d.Guard != guard
			d.Guard = guard
			kept = append(kept, d)
			continue
		}
		s.pending = keepTrigger(s.pending, d.SupervisorTrigger)
		changed = true
	}
	s.deferred = kept
	if guard != "" {
		pending := s.pending[:0]
		var deferred []string
		for _, t := range s.pending {
			if !guardBlocks(guard, t.Kind) {
				pending = append(pending, t)
				continue
			}
			s.deferred = keepDeferral(s.deferred, SupervisorDeferral{SupervisorTrigger: t, Guard: guard})
			deferred = append(deferred, t.Kind)
		}
		s.pending = pending
		if len(deferred) > 0 {
			changed = true
			o.logger.Info("supervisor triggers deferred by a spend guard",
				slog.String("guard", guard), slog.Any("supervisor_triggers", deferred))
		}
	}
	if changed {
		o.notifyObservers()
	}
}

// keepDeferral returns deferred with d kept as keepTrigger keeps a
// trigger.
func keepDeferral(deferred []SupervisorDeferral, d SupervisorDeferral) []SupervisorDeferral {
	for i, p := range deferred {
		if p.Kind == d.Kind && p.IssueID == d.IssueID {
			deferred[i] = d
			return deferred
		}
	}
	if len(deferred) >= maxPendingSupervisorTriggers {
		deferred = slices.Delete(deferred, 0, 1)
	}
	return append(deferred, d)
}

// supervisorCredentialFault returns the oldest open fault of a refused
// credential of the agent kind, or nil.
func supervisorCredentialFault(state *State, kind string) *PlatformFault {
	var fault *PlatformFault
	for _, f := range state.PlatformFaults {
		if f.Step == healthStep && healthKeyOf(f.Signature) == healthKeyAgentCredential+kind && (fault == nil || f.RecordedAt.Before(fault.RecordedAt)) {
			fault = f
		}
	}
	return fault
}

// supervisorUnavailable returns why the supervisor cannot start the
// sessions it would start, with a detail and when it resets (zero when
// not known); an empty reason when it can.
func (o *Orchestrator) supervisorUnavailable(sc config.SupervisorConfig, now time.Time) (reason, detail string, resetsAt time.Time) {
	s := o.state.supervisor
	if p := s.pause; p != nil {
		detail = "paused by " + p.By
		if p.Reason != "" {
			detail += ": " + p.Reason
		}
		return domain.SupervisorUnavailablePaused, detail, time.Time{}
	}
	if f := supervisorCredentialFault(o.state, sc.Agent); f != nil {
		return domain.SupervisorUnavailableCredential, f.Cause, time.Time{}
	}
	if stop := s.budgetStop(sc); stop != "" {
		return domain.SupervisorUnavailableBudget, stop, s.day.Add(24 * time.Hour)
	}
	if s.velocity {
		pace, limit := hourlyPace(sc)
		detail = fmt.Sprintf("the supervisor spent %d tokens in the last hour, over its limit of %d; only sweeps start a session until it is back under %d",
			s.hourSpend(now), limit, pace)
		return domain.SupervisorUnavailableVelocity, detail, s.spendResetsAt(now, pace)
	}
	return "", "", time.Time{}
}

// noteSupervisorAvailability publishes supervisor.unavailable when the
// reason the supervisor cannot start a session changes to a new one, and
// supervisor.available when it can start one again. Must run on the
// event loop.
func (o *Orchestrator) noteSupervisorAvailability(ctx context.Context, sc config.SupervisorConfig, now time.Time) {
	s := o.state.supervisor
	reason, detail, resetsAt := o.supervisorUnavailable(sc, now)
	if reason == s.avail.reason {
		if detail != s.avail.detail || !resetsAt.Equal(s.avail.resetsAt) {
			s.avail.detail, s.avail.resetsAt = detail, resetsAt
			o.notifyObservers()
		}
		return
	}
	prev := s.avail
	s.avail = supervisorAvailability{reason: reason, detail: detail, resetsAt: resetsAt}
	if reason == "" {
		o.logger.Info("supervisor available", slog.String("was", prev.reason), slog.Duration("unavailable_for", now.Sub(prev.since)))
		o.publishSupervisorEvent(ctx, domain.EventSupervisorAvailable, domain.SupervisorAvailableFacts{Reason: prev.reason, Since: prev.since}, o.logger)
	} else {
		s.avail.since = now.UTC()
		attrs := []any{slog.String("reason", reason), slog.String("detail", detail)}
		if !resetsAt.IsZero() {
			attrs = append(attrs, slog.Time("resets_at", resetsAt))
		}
		o.logger.Warn("supervisor unavailable", attrs...)
		o.publishSupervisorEvent(ctx, domain.EventSupervisorUnavailable, domain.SupervisorUnavailableFacts{
			Reason: reason, Detail: detail, Since: s.avail.since, ResetsAt: resetsAt,
		}, o.logger)
	}
	o.notifyObservers()
}
