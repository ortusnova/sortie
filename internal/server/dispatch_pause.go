package server

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/sortie-ai/sortie/internal/orchestrator"
)

// DispatchPauseFunc pauses dispatch when pause is true and resumes it
// otherwise; by and reason describe a pause. ctx is the request's context.
type DispatchPauseFunc func(ctx context.Context, pause bool, by, reason string) orchestrator.DispatchPauseResult

// defaultPausedBy names who paused dispatch when the request does not.
const defaultPausedBy = "operator"

type dispatchPauseRequest struct {
	By     string `json:"by"`
	Reason string `json:"reason"`
}

// dispatchPausedResponse is a pause of dispatch, as /api/v1/state and the
// pause and resume routes serve it.
type dispatchPausedResponse struct {
	Since  time.Time `json:"since"`
	By     string    `json:"by"`
	Reason string    `json:"reason"`
}

// dispatchPauseResponse answers the pause and resume routes.
// DispatchPaused is null once dispatch runs.
type dispatchPauseResponse struct {
	DispatchPaused *dispatchPausedResponse `json:"dispatch_paused"`
}

func toDispatchPausedResponse(p *orchestrator.DispatchPause) *dispatchPausedResponse {
	if p == nil {
		return nil
	}
	return &dispatchPausedResponse{Since: p.Since.UTC(), By: p.By, Reason: p.Reason}
}

func (s *Server) handleDispatchPause(w http.ResponseWriter, r *http.Request) {
	var req dispatchPauseRequest
	if err := decodeControlBody(r, &req); err != nil {
		writeErrorJSON(w, s.logger, http.StatusBadRequest, "invalid_request", "request body must be a JSON object with optional by and reason")
		return
	}
	if req.By == "" {
		req.By = defaultPausedBy
	}
	s.serveDispatchPause(w, r, true, req.By, req.Reason)
}

func (s *Server) handleDispatchResume(w http.ResponseWriter, r *http.Request) {
	s.serveDispatchPause(w, r, false, "", "")
}

func (s *Server) serveDispatchPause(w http.ResponseWriter, r *http.Request, pause bool, by, reason string) {
	start := time.Now()
	if s.dispatchPauseFn == nil {
		writeErrorJSON(w, s.logger, http.StatusServiceUnavailable, "orchestrator_unavailable", "dispatch pause is not available")
		return
	}
	result := s.dispatchPauseFn(r.Context(), pause, by, reason)
	if result.Status != orchestrator.DispatchPauseDone {
		writeErrorJSON(w, s.logger, http.StatusServiceUnavailable, "orchestrator_unavailable", "the orchestrator did not take the request")
		return
	}
	writeJSON(w, s.logger, http.StatusOK, dispatchPauseResponse{DispatchPaused: toDispatchPausedResponse(result.Pause)})
	s.logger.Debug("request served",
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.Int("status", http.StatusOK),
		slog.Duration("duration", time.Since(start)),
	)
}
