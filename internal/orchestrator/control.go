package orchestrator

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/logging"
	"github.com/sortie-ai/sortie/internal/notify/route"
	"github.com/sortie-ai/sortie/internal/parkanswer"
	"github.com/sortie-ai/sortie/internal/redact"
)

// ControlOutcome is the event loop's verdict on a control request.
type ControlOutcome int

const (
	// ControlDone means the request was applied, or had nothing to change.
	ControlDone ControlOutcome = iota

	// ControlAccepted means the request was accepted for delivery.
	ControlAccepted

	// ControlUnknownIssue means no issue with the identifier is known.
	ControlUnknownIssue

	// ControlNotRunning means the issue is known but has no running worker.
	ControlNotRunning

	// ControlInvalid means the request itself is malformed.
	ControlInvalid

	// ControlRateLimited means the request came too soon after the last
	// one of its kind for the same issue.
	ControlRateLimited

	// ControlUnavailable means the event loop did not answer, because it is
	// shutting down or busy past the reply bound.
	ControlUnavailable

	// ControlNotParked means the issue holds no park to answer.
	ControlNotParked
)

// HoldState is a run's hold as the control routes report it.
type HoldState struct {
	Identifier string
	IssueID    string
	Held       bool
	Reason     string
	Holder     string
	HeldSince  time.Time
	HeldTotal  time.Duration

	// Lease is the standing hold, a copy, for a hold on an issue that is
	// not running; nil for a running run's hold and after a release.
	Lease *StandingHold
}

// ControlReply is the event loop's answer to a control request.
type ControlReply struct {
	Outcome ControlOutcome
	Hold    HoldState
	Message string
	Broker  BrokerAddress
	Answer  ParkAnswer
}

type controlKind int

const (
	controlHold controlKind = iota
	controlRelease
	controlEscalate
	controlPullBack
	controlCancel
	controlBrokerReport
	controlBrokerRead
	controlAnswer
)

// controlRequest asks the event loop to change one issue's run.
type controlRequest struct {
	Kind       controlKind
	Identifier string
	Hold       HoldRequest
	Escalation EscalationRequest
	Plan       planTarget
	Broker     BrokerAddress
	Answer     parkanswer.Answer
	ReplyCh    chan ControlReply
}

// controlReplyBound bounds how long a control call waits for the event
// loop, both to accept the request and to answer it.
const controlReplyBound = 5 * time.Second

// findIssue resolves identifier to the running entry serving it, and
// reports whether the identifier names any issue the runtime state
// knows: running, waiting to retry, held out by a budget ceiling, parked,
// released from a park, held back by the latest dispatch pass, or held by
// a standing hold.
func findIssue(state *State, identifier string) (issueID string, entry *RunningEntry, known bool) {
	for id, e := range state.Running {
		if e.Identifier == identifier {
			return id, e, true
		}
	}
	for id, r := range state.RetryAttempts {
		if r.Identifier == identifier {
			return id, nil, true
		}
	}
	for id, b := range state.BudgetExhausted {
		if b.Identifier == identifier {
			return id, nil, true
		}
	}
	for _, parks := range []map[string]*ParkedEntry{state.Parked, state.ParkReleases} {
		for id, p := range parks {
			if p.Identifier == identifier {
				return id, nil, true
			}
		}
	}
	for id, h := range state.Held {
		if h.Identifier == identifier {
			return id, nil, true
		}
	}
	for id, h := range state.StandingHolds {
		if h.Identifier == identifier {
			return id, nil, true
		}
	}
	return "", nil, false
}

// holdStateOf returns the hold view of a running entry.
func holdStateOf(issueID string, entry *RunningEntry, now time.Time) HoldState {
	total := entry.HeldTotal
	if !entry.HeldSince.IsZero() {
		total += now.Sub(entry.HeldSince)
	}
	return HoldState{
		Identifier: entry.Identifier,
		IssueID:    issueID,
		Held:       !entry.HeldSince.IsZero(),
		Reason:     entry.HoldReason,
		Holder:     entry.Holder,
		HeldSince:  entry.HeldSince,
		HeldTotal:  total,
	}
}

// applyHold holds the running entry for identifier at now, or places a
// standing hold on a known issue that is not running. A hold already in
// effect keeps its start, reason, and holder. store persists a standing
// hold; nil keeps it in memory only.
func applyHold(ctx context.Context, state *State, store StandingHoldStore, identifier string, req HoldRequest, now time.Time, log *slog.Logger) ControlReply {
	reason, holder := req.Reason, req.Holder
	issueID, entry, known := findIssue(state, identifier)
	switch {
	case !known:
		return ControlReply{Outcome: ControlUnknownIssue, Message: fmt.Sprintf("issue %q is not known", identifier)}
	case entry == nil:
		return placeStandingHold(ctx, state, store, issueID, identifier, req, now, log)
	}
	if entry.HeldSince.IsZero() {
		entry.HeldSince = now
		entry.HoldReason = reason
		entry.Holder = holder
		entry.TurnClock.hold(now)
		logging.WithIssue(log, issueID, entry.Identifier).Info("run held",
			slog.String("hold_reason", reason),
			slog.String("holder", holder),
		)
	}
	return ControlReply{Outcome: ControlDone, Hold: holdStateOf(issueID, entry, now)}
}

// applyRelease ends the hold of the running entry for identifier at now,
// or removes the standing hold of an issue that is not running. Releasing
// a run that is not held changes nothing.
func applyRelease(ctx context.Context, state *State, store StandingHoldStore, identifier string, now time.Time, metrics domain.Metrics, log *slog.Logger) ControlReply {
	issueID, entry, known := findIssue(state, identifier)
	switch {
	case !known:
		return ControlReply{Outcome: ControlUnknownIssue, Message: fmt.Sprintf("issue %q is not known", identifier)}
	case entry == nil && state.StandingHolds[issueID] != nil:
		return removeStandingHold(ctx, state, store, issueID, now, log)
	case entry == nil:
		return ControlReply{Outcome: ControlNotRunning, Message: fmt.Sprintf("issue %q has no running worker", identifier)}
	}
	ended := endHoldRecorded(entry, now, metrics)
	if ended > 0 {
		logging.WithIssue(log, issueID, entry.Identifier).Info("run released",
			slog.Int64("held_ms", ended.Milliseconds()),
			slog.Int64("held_total_ms", entry.HeldTotal.Milliseconds()),
		)
	}
	return ControlReply{Outcome: ControlDone, Hold: holdStateOf(issueID, entry, now)}
}

// endHoldRecorded ends the entry's hold at now, as [endHold] does, and
// counts the hold it ended toward the run's rule in metrics.
func endHoldRecorded(entry *RunningEntry, now time.Time, metrics domain.Metrics) time.Duration {
	ended := endHold(entry, now)
	if ended > 0 && metrics != nil {
		metrics.AddHoldSeconds(normalizeDispatchRuleName(entry.RuleName), ended.Seconds())
	}
	return ended
}

// endHold ends the entry's hold at now, if one is in effect: it adds the
// hold to the run's total, restarts the stall clock at now, and credits
// the turn clock. It returns the duration of the hold it ended.
func endHold(entry *RunningEntry, now time.Time) time.Duration {
	if entry.HeldSince.IsZero() {
		return 0
	}
	ended := max(now.Sub(entry.HeldSince), 0)
	entry.HeldTotal += ended
	entry.HeldSince = time.Time{}
	entry.HoldReason = ""
	entry.Holder = ""
	entry.HoldReleasedAt = now
	entry.TurnClock.release(now)
	return ended
}

// heldRuns counts the running entries currently held.
func heldRuns(state *State) int {
	n := 0
	for _, e := range state.Running {
		if !e.HeldSince.IsZero() {
			n++
		}
	}
	return n
}

// EscalationRequest is a caller's request to call people about a running
// issue.
type EscalationRequest struct {
	// Message is the text people receive. It must hold a non-space
	// character.
	Message string

	// Kind is "question", "permission", or "other"; empty means "other".
	Kind string

	// URL optionally links to the session.
	URL string
}

// escalationWindow is the shortest interval between two escalations
// delivered for one issue.
const escalationWindow = 60 * time.Second

// sessionEscalationKinds are the accepted values of [EscalationRequest.Kind].
var sessionEscalationKinds = []string{"question", "permission", "other"}

// escalationParams carries what [applyEscalate] needs besides the request.
type escalationParams struct {
	Router *route.Router
	Wg     *sync.WaitGroup
	Ctx    context.Context
	Logger *slog.Logger
}

// applyEscalate validates req, checks that identifier has a running
// worker and that the issue's escalation window has passed, masks the
// text, and routes one session.escalated event. An event that reaches no
// destination is accepted and logged once.
func applyEscalate(state *State, identifier string, req EscalationRequest, now time.Time, p escalationParams) ControlReply {
	kind := cmp.Or(strings.TrimSpace(req.Kind), "other")
	switch {
	case strings.TrimSpace(req.Message) == "":
		return ControlReply{Outcome: ControlInvalid, Message: "message must not be empty"}
	case !slices.Contains(sessionEscalationKinds, kind):
		return ControlReply{Outcome: ControlInvalid, Message: fmt.Sprintf("kind %q is not one of question, permission, or other", req.Kind)}
	}

	issueID, entry, known := findIssue(state, identifier)
	switch {
	case !known:
		return ControlReply{Outcome: ControlUnknownIssue, Message: fmt.Sprintf("issue %q is not known", identifier)}
	case entry == nil:
		return ControlReply{Outcome: ControlNotRunning, Message: fmt.Sprintf("issue %q has no running worker", identifier)}
	}
	if last, ok := state.LastEscalation[issueID]; ok && now.Sub(last) < escalationWindow {
		return ControlReply{Outcome: ControlRateLimited, Message: fmt.Sprintf("issue %q was escalated less than %s ago", identifier, escalationWindow)}
	}
	if state.LastEscalation == nil {
		state.LastEscalation = make(map[string]time.Time)
	}
	state.LastEscalation[issueID] = now

	log := logging.WithIssue(p.Logger, issueID, entry.Identifier)
	url := redact.Mask(strings.TrimSpace(req.URL))
	n := sessionEvent{
		IssueID:    issueID,
		Identifier: entry.Identifier,
		DisplayID:  entry.Issue.DisplayID,
		DispatchID: entry.DispatchID,
		SessionID:  entry.SessionID,
		Attempt:    entry.RetryAttempt,
		Agent:      entry.AgentKind,
		AgentText:  redact.Mask(strings.TrimSpace(req.Message)),
	}.notification(domain.EventSessionEscalated, "", domain.SessionEscalatedFacts{Kind: kind, Rule: entry.RuleName, URL: url})
	n.Envelope.Escalation = domain.SessionEscalation{Kind: kind, Rule: entry.RuleName, URL: url}

	delivery := p.Router.Route(n)
	if delivery.Empty() {
		log.Info("session escalation had no destination",
			slog.String("event_type", string(domain.EventSessionEscalated)),
			slog.String("kind", kind),
		)
		return ControlReply{Outcome: ControlAccepted}
	}
	log.Info("session escalated", slog.String("kind", kind))
	deliverDetached(p.Ctx, p.Wg, delivery, log, func(received bool, err error) {
		if received && err != nil {
			log.Warn("session escalation tracker comment failed", slog.Any("error", err))
		}
	})
	return ControlReply{Outcome: ControlAccepted}
}
