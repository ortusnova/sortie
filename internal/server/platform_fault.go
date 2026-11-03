package server

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/sortie-ai/sortie/internal/orchestrator"
)

// PlatformFaultDoneFunc marks the person action of the open platform
// fault faultID done by by and checks the fault again at once. ctx is the
// request's context.
type PlatformFaultDoneFunc func(ctx context.Context, faultID, by string) orchestrator.PlatformFaultDoneResult

// defaultActionDoneBy names who marked a person action done when the
// request does not.
const defaultActionDoneBy = "operator"

type platformFaultDoneRequest struct {
	By string `json:"by"`
}

// personActionResponse is what a person does to clear a platform fault,
// where, and whether someone marked it done.
type personActionResponse struct {
	Action string     `json:"action"`
	Where  string     `json:"where"`
	DoneAt *time.Time `json:"done_at"`
	DoneBy string     `json:"done_by"`
}

// platformFaultStoryResponse is one story a platform fault holds.
type platformFaultStoryResponse struct {
	IssueID           string    `json:"issue_id"`
	IssueIdentifier   string    `json:"issue_identifier"`
	DisplayIdentifier string    `json:"display_identifier,omitempty"`
	HeldAt            time.Time `json:"held_at"`
}

// platformFaultResponse is an open platform fault, as /api/v1/state and
// the action-done route serve it. PersonAction is null for a fault
// Sortie's own check clears.
type platformFaultResponse struct {
	ID             string                       `json:"id"`
	Class          string                       `json:"class"`
	Step           string                       `json:"step"`
	Signature      string                       `json:"signature"`
	Cause          string                       `json:"cause"`
	Detail         string                       `json:"detail"`
	PersonAction   *personActionResponse        `json:"person_action"`
	Stories        []platformFaultStoryResponse `json:"stories"`
	RecordedAt     time.Time                    `json:"recorded_at"`
	LastCheckAt    *time.Time                   `json:"last_check_at"`
	LastCheckError string                       `json:"last_check_error"`
	NextCheckAt    time.Time                    `json:"next_check_at"`
	Checking       bool                         `json:"checking"`
}

// platformFaultDoneResponse answers the action-done route. Resolved
// reports that the check passed and released the held stories.
type platformFaultDoneResponse struct {
	Resolved      bool                  `json:"resolved"`
	PlatformFault platformFaultResponse `json:"platform_fault"`
}

func faultTimeOrNil(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return new(t.UTC())
}

func toPlatformFaultResponse(f orchestrator.SnapshotPlatformFault) platformFaultResponse {
	resp := platformFaultResponse{
		ID:             f.ID,
		Class:          f.Class,
		Step:           f.Step,
		Signature:      f.Signature,
		Cause:          f.Cause,
		Detail:         f.Detail,
		Stories:        make([]platformFaultStoryResponse, 0, len(f.Holds)),
		RecordedAt:     f.RecordedAt.UTC(),
		LastCheckAt:    faultTimeOrNil(f.LastCheckAt),
		LastCheckError: f.LastCheckError,
		NextCheckAt:    f.NextCheckAt.UTC(),
		Checking:       f.Checking,
	}
	if f.Action != nil {
		resp.PersonAction = &personActionResponse{
			Action: f.Action.Action,
			Where:  f.Action.Where,
			DoneAt: faultTimeOrNil(f.ActionDoneAt),
			DoneBy: f.ActionDoneBy,
		}
	}
	for _, h := range f.Holds {
		resp.Stories = append(resp.Stories, platformFaultStoryResponse{
			IssueID:           h.IssueID,
			IssueIdentifier:   h.Identifier,
			DisplayIdentifier: h.DisplayID,
			HeldAt:            h.HeldAt.UTC(),
		})
	}
	return resp
}

func toPlatformFaultResponses(faults []orchestrator.SnapshotPlatformFault) []platformFaultResponse {
	out := make([]platformFaultResponse, 0, len(faults))
	for _, f := range faults {
		out = append(out, toPlatformFaultResponse(f))
	}
	return out
}

func (s *Server) handlePlatformFaultDone(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	status := s.servePlatformFaultDone(w, r)
	s.logger.Debug("request served",
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.Int("status", status),
		slog.Duration("duration", time.Since(start)),
	)
}

func (s *Server) servePlatformFaultDone(w http.ResponseWriter, r *http.Request) int {
	var req platformFaultDoneRequest
	if err := decodeControlBody(r, &req); err != nil {
		writeErrorJSON(w, s.logger, http.StatusBadRequest, "invalid_request", "request body must be a JSON object with an optional by")
		return http.StatusBadRequest
	}
	if req.By == "" {
		req.By = defaultActionDoneBy
	}
	if s.platformFaultDoneFn == nil {
		writeErrorJSON(w, s.logger, http.StatusServiceUnavailable, "orchestrator_unavailable", "platform faults are not available")
		return http.StatusServiceUnavailable
	}
	result := s.platformFaultDoneFn(r.Context(), r.PathValue("id"), req.By)

	// The check runs before the answer, so the server's own deadline may
	// have lapsed by now.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(s.httpServer.WriteTimeout))

	switch result.Status {
	case orchestrator.PlatformFaultDoneChecked, orchestrator.PlatformFaultDoneBusy:
		writeJSON(w, s.logger, http.StatusOK, platformFaultDoneResponse{
			Resolved:      result.Resolved,
			PlatformFault: toPlatformFaultResponse(result.Fault),
		})
		return http.StatusOK
	case orchestrator.PlatformFaultDoneNotFound:
		writeErrorJSON(w, s.logger, http.StatusNotFound, "platform_fault_not_found", "no open platform fault has this id")
		return http.StatusNotFound
	default:
		writeErrorJSON(w, s.logger, http.StatusServiceUnavailable, "orchestrator_unavailable", "the orchestrator did not take the request")
		return http.StatusServiceUnavailable
	}
}
