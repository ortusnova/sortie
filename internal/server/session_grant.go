package server

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/sortie-ai/sortie/internal/orchestrator"
)

// SessionGrantFunc grants one issue more sessions beyond
// agent.max_sessions. ctx is the request's context.
type SessionGrantFunc func(ctx context.Context, req orchestrator.SessionGrantRequest) orchestrator.SessionGrantResult

const (
	// maxSessionGrant bounds the sessions one request may add.
	maxSessionGrant = 64

	// defaultGrantedBy names who granted sessions when the request does not.
	defaultGrantedBy = "operator"
)

type sessionGrantRequest struct {
	Add    *int   `json:"add"`
	By     string `json:"by"`
	Reason string `json:"reason"`
}

// sessionGrantResponse answers POST /api/v1/{identifier}/sessions with the
// issue's session budget after the grant. BudgetSessions is null while
// agent.max_sessions sets no budget.
type sessionGrantResponse struct {
	IssueIdentifier string `json:"issue_identifier"`
	IssueID         string `json:"issue_id"`
	UsedSessions    int    `json:"used_sessions"`
	BudgetSessions  *int   `json:"budget_sessions"`
	GrantedSessions int    `json:"granted_sessions"`
}

func (s *Server) handleSessionGrant(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if s.sessionGrantFn == nil {
		writeErrorJSON(w, s.logger, http.StatusServiceUnavailable, "orchestrator_unavailable", "session grants are not available")
		return
	}
	var req sessionGrantRequest
	if err := decodeControlBody(r, &req); err != nil || req.Add == nil || *req.Add < 1 || *req.Add > maxSessionGrant {
		writeErrorJSON(w, s.logger, http.StatusBadRequest, "invalid_request", "request body must be a JSON object with add from 1 to 64, and optional by and reason")
		return
	}
	if req.By == "" {
		req.By = defaultGrantedBy
	}
	result := s.sessionGrantFn(r.Context(), orchestrator.SessionGrantRequest{
		Identifier: r.PathValue("identifier"),
		Add:        *req.Add,
		By:         req.By,
		Reason:     req.Reason,
	})
	status := http.StatusOK
	switch result.Status {
	case orchestrator.SessionGrantDone:
		var budget *int
		if result.BudgetSessions > 0 {
			budget = new(result.BudgetSessions)
		}
		writeJSON(w, s.logger, status, sessionGrantResponse{
			IssueIdentifier: result.Identifier,
			IssueID:         result.IssueID,
			UsedSessions:    result.UsedSessions,
			BudgetSessions:  budget,
			GrantedSessions: result.GrantedSessions,
		})
	case orchestrator.SessionGrantUnknownIssue:
		status = http.StatusNotFound
		writeErrorJSON(w, s.logger, status, "issue_not_found", result.Message)
	default:
		status = http.StatusServiceUnavailable
		message := result.Message
		if message == "" {
			message = "the orchestrator did not take the request"
		}
		writeErrorJSON(w, s.logger, status, "orchestrator_unavailable", message)
	}
	s.logger.Debug("request served",
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.Int("status", status),
		slog.Duration("duration", time.Since(start)),
	)
}

// grantedSessionsOf returns the sessions granted to issueID in snap and
// its session budget, nil while agent.max_sessions sets no budget.
func grantedSessionsOf(snap orchestrator.RuntimeSnapshotResult, issueID string) (granted int, budget *int) {
	granted = snap.SessionGrants[issueID]
	if snap.MaxSessions > 0 {
		budget = new(snap.MaxSessions + granted)
	}
	return granted, budget
}
