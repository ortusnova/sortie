package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sortie-ai/sortie/internal/logging"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// SessionGrantStore is the optional store capability that keeps the
// sessions operators grant issues beyond agent.max_sessions.
// [persistence.Store] implements it; with a store without it no issue has
// a grant and no grant can be made.
type SessionGrantStore interface {
	AddSessionGrant(ctx context.Context, g persistence.SessionGrant) (int, error)
	SessionGrantTotals(ctx context.Context) (map[string]int, error)
	SessionGrantTotal(ctx context.Context, issueID string) (int, error)
}

var _ SessionGrantStore = (*persistence.Store)(nil)

// sessionGrantCache holds the store's grant totals once they have been read.
type sessionGrantCache struct {
	read   bool
	totals map[string]int
}

// sessionGrantsOf returns the sessions granted to each issue, reading the
// store the first time. A failed read is retried on the next call and
// meanwhile counts no grant, so the budget fails closed. Must run on the
// event loop.
func sessionGrantsOf(ctx context.Context, state *State, store any, log *slog.Logger) map[string]int {
	if state.sessionGrants.read {
		return state.sessionGrants.totals
	}
	grants, ok := store.(SessionGrantStore)
	if !ok {
		return nil
	}
	totals, err := grants.SessionGrantTotals(ctx)
	if err != nil {
		if log != nil {
			log.Warn("session grant read failed, counting no grant", slog.Any("error", err))
		}
		return nil
	}
	state.sessionGrants = sessionGrantCache{read: true, totals: totals}
	return totals
}

// sessionBudget returns the session budget of an issue granted granted
// sessions: maxSessions plus the grant, or 0 (no budget) when maxSessions
// is 0.
func sessionBudget(maxSessions, granted int) int {
	if maxSessions <= 0 {
		return 0
	}
	return maxSessions + granted
}

// SessionGrantRequest asks for more sessions for one issue.
type SessionGrantRequest struct {
	Identifier string
	Add        int
	By         string
	Reason     string
	// Cap, when positive, bounds the sessions granted to the issue in
	// total, this grant included; a grant that would pass it is refused.
	Cap int
}

// SessionGrantStatus is what a session grant request did.
type SessionGrantStatus int

const (
	SessionGrantDone         SessionGrantStatus = iota + 1 // the grant is recorded
	SessionGrantUnknownIssue                               // no issue with the identifier is known; nothing changed
	SessionGrantUnavailable                                // the store could not record the grant; nothing changed
	SessionGrantDraining                                   // shutdown began before the loop took the request; nothing changed
	SessionGrantWithdrawn                                  // the caller's context ended before the loop took the request; nothing changed
	SessionGrantOverCap                                    // the grant would pass the request's cap; nothing changed
)

// SessionGrantResult is the answer to a session grant request. The counts
// are set only with [SessionGrantDone]; BudgetSessions is 0 while
// agent.max_sessions sets no budget.
type SessionGrantResult struct {
	Status          SessionGrantStatus
	IssueID         string
	Identifier      string
	UsedSessions    int
	BudgetSessions  int
	GrantedSessions int
	Message         string
}

// SessionGrantFunc returns a function that grants an issue more sessions
// and releases it at once when the session budget held it. The grant is
// stored, so it outlives a restart. The function is safe for concurrent
// use and returns once the event loop has taken the request, ctx has
// ended, or shutdown has begun.
func (o *Orchestrator) SessionGrantFunc() func(ctx context.Context, req SessionGrantRequest) SessionGrantResult {
	return func(ctx context.Context, req SessionGrantRequest) SessionGrantResult {
		var result SessionGrantResult
		switch o.runOnLoop(ctx, func(loopCtx context.Context) {
			result = o.grantSessionsOnLoop(loopCtx, req)
		}) {
		case loopRan:
			return result
		case loopDraining:
			return SessionGrantResult{Status: SessionGrantDraining}
		default:
			return SessionGrantResult{Status: SessionGrantWithdrawn}
		}
	}
}

// grantSessionsOnLoop records the grant and, when the session budget held
// the issue, raises the held record's budget and asks for a tick, so the
// issue dispatches without waiting for the poll interval. Must run on the
// event loop.
func (o *Orchestrator) grantSessionsOnLoop(ctx context.Context, req SessionGrantRequest) SessionGrantResult {
	issueID, _, known := findIssue(o.state, req.Identifier)
	if !known {
		issueID, _, known = findParked(o.state, req.Identifier)
	}
	if !known {
		return SessionGrantResult{Status: SessionGrantUnknownIssue, Message: "issue " + req.Identifier + " is not known"}
	}
	store, ok := o.store.(SessionGrantStore)
	if !ok {
		return SessionGrantResult{Status: SessionGrantUnavailable, Message: "session grants are not available"}
	}
	log := logging.WithIssue(o.logger, issueID, req.Identifier)
	if req.Cap > 0 {
		total, err := store.SessionGrantTotal(ctx, issueID)
		if err != nil {
			log.Error("session grant read failed", slog.Any("error", err))
			return SessionGrantResult{Status: SessionGrantUnavailable, Message: "the issue's grants could not be read"}
		}
		if total+req.Add > req.Cap {
			return SessionGrantResult{
				Status: SessionGrantOverCap, IssueID: issueID, Identifier: req.Identifier, GrantedSessions: total,
				Message: fmt.Sprintf("issue %s has %d granted sessions; %d more would pass the cap of %d", req.Identifier, total, req.Add, req.Cap),
			}
		}
	}
	sessionGrantsOf(ctx, o.state, o.store, o.logger)
	granted, err := store.AddSessionGrant(ctx, persistence.SessionGrant{
		IssueID:    issueID,
		Identifier: req.Identifier,
		Sessions:   req.Add,
		GrantedBy:  req.By,
		Reason:     req.Reason,
		GrantedAt:  time.Now(),
	})
	if err != nil {
		log.Error("session grant failed", slog.Any("error", err))
		return SessionGrantResult{Status: SessionGrantUnavailable, Message: "the grant could not be recorded"}
	}
	if o.state.sessionGrants.read {
		if o.state.sessionGrants.totals == nil {
			o.state.sessionGrants.totals = map[string]int{}
		}
		o.state.sessionGrants.totals[issueID] = granted
	}

	budget := sessionBudget(o.workflowManager.Config().Agent.MaxSessions, granted)
	held := o.state.BudgetExhausted[issueID]
	used, countErr := o.store.CountRunHistoryByIssue(ctx, issueID)
	if countErr != nil {
		log.Warn("session count failed after a grant", slog.Any("error", countErr))
		if held != nil {
			used = held.UsedSessions
		}
	}
	released := false
	if held != nil {
		held.BudgetSessions = budget
		released = held.Reason == budgetReasonSession && (budget == 0 || used < budget)
	}
	log.Info("sessions granted",
		slog.Int("added", req.Add),
		slog.Int("granted_sessions", granted),
		slog.Int("budget_sessions", budget),
		slog.Int("used_sessions", used),
		slog.String("by", req.By),
		slog.String("reason", req.Reason),
		slog.Bool("released", released),
	)
	if released {
		o.RefreshFunc()()
	}
	o.notifyObservers()
	return SessionGrantResult{
		Status:          SessionGrantDone,
		IssueID:         issueID,
		Identifier:      req.Identifier,
		UsedSessions:    used,
		BudgetSessions:  budget,
		GrantedSessions: granted,
	}
}
