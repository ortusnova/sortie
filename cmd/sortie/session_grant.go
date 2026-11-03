package main

import (
	"context"
	"log/slog"

	"github.com/sortie-ai/sortie/internal/persistence"
)

// grantedSessionBudget returns the session budget cost_budget reports for
// the issue: agent.max_sessions plus the sessions granted to the issue, or
// 0 (unlimited) when agent.max_sessions is 0. A failed read of the grants
// reports agent.max_sessions alone.
func grantedSessionBudget(ctx context.Context, logger *slog.Logger, store *persistence.Store, params SessionToolParams) int {
	if params.MaxSessions <= 0 {
		return params.MaxSessions
	}
	granted, err := store.SessionGrantTotal(ctx, params.IssueID)
	if err != nil {
		logger.Warn("session grant read failed, reporting agent.max_sessions alone", slog.Any("error", err))
		return params.MaxSessions
	}
	return params.MaxSessions + granted
}
