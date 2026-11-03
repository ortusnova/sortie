package server

import (
	"cmp"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/sortie-ai/sortie/internal/orchestrator"
	"github.com/sortie-ai/sortie/internal/parkanswer"
)

// HoldFunc holds the running issue named by identifier, or places a
// standing hold on a known issue that is not running. It must be safe to
// call from any goroutine.
type HoldFunc func(identifier string, req orchestrator.HoldRequest) orchestrator.ControlReply

// ReleaseFunc releases the hold on the running issue named by
// identifier, or its standing hold. It must be safe to call from any
// goroutine.
type ReleaseFunc func(identifier string) orchestrator.ControlReply

// maxControlBodyBytes bounds the JSON body of a control request.
const maxControlBodyBytes = 64 << 10

// holdRequest is the body of a hold request. By is another name for
// Holder; Holder wins when both are set. Evidence, Until (RFC 3339), and
// Minutes apply to a hold on an issue that is not running: a lease that
// must be renewed with new evidence before it ends. Until wins over
// Minutes; with neither the lease lasts the default.
type holdRequest struct {
	Reason   string `json:"reason"`
	Holder   string `json:"holder"`
	By       string `json:"by"`
	Evidence string `json:"evidence"`
	Until    string `json:"until"`
	Minutes  int    `json:"minutes"`
}

// runHoldResponse is the reply of the hold and release routes. Lease is
// the standing hold after a hold or renewal of an issue that is not
// running, and null otherwise.
type runHoldResponse struct {
	IssueIdentifier string `json:"issue_identifier"`
	IssueID         string `json:"issue_id"`
	holdFields
	Lease *standingHoldResponse `json:"lease"`
}

// controlRoute returns a handler that answers 405 with the JSON error
// envelope for any method but POST, and otherwise applies the control
// authentication of scope before next.
func (s *Server) controlRoute(scope controlScope, next http.HandlerFunc) http.HandlerFunc {
	guarded := s.requireControl(scope, next)
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			s.methodNotAllowed(w, r, http.MethodPost)
			return
		}
		guarded(w, r)
	}
}

// decodeControlBody decodes the request's JSON body into v. An empty body
// leaves v unchanged.
func decodeControlBody(r *http.Request, v any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxControlBodyBytes+1))
	if err != nil {
		return err
	}
	if len(body) > maxControlBodyBytes {
		return errors.New("request body too large")
	}
	if len(body) == 0 {
		return nil
	}
	return json.Unmarshal(body, v)
}

// writeControlReply writes reply with the status its outcome maps to.
func (s *Server) writeControlReply(w http.ResponseWriter, r *http.Request, start time.Time, reply orchestrator.ControlReply, body any) {
	var status int
	var code string
	switch reply.Outcome {
	case orchestrator.ControlDone:
		status = http.StatusOK
	case orchestrator.ControlAccepted:
		status = http.StatusAccepted
	case orchestrator.ControlUnknownIssue:
		status, code = http.StatusNotFound, "issue_not_found"
	case orchestrator.ControlNotRunning:
		status, code = http.StatusConflict, "issue_not_running"
	case orchestrator.ControlNotParked:
		status, code = http.StatusConflict, "issue_not_parked"
	case orchestrator.ControlInvalid:
		status, code = http.StatusBadRequest, "invalid_request"
	case orchestrator.ControlRateLimited:
		status, code = http.StatusTooManyRequests, "rate_limited"
	case orchestrator.ControlAlreadyClaimed:
		status, code = http.StatusConflict, "already_claimed"
	case orchestrator.ControlNotReleased:
		status, code = http.StatusConflict, "not_released"
	case orchestrator.ControlAlreadyTerminal:
		status, code = http.StatusConflict, "already_terminal"
	case orchestrator.ControlPlanOff:
		status, code = http.StatusConflict, "plan_control_off"
	case orchestrator.ControlTrackerFailed:
		status, code = http.StatusBadGateway, "tracker_error"
	default:
		status, code = http.StatusServiceUnavailable, "orchestrator_unavailable"
	}
	if code != "" {
		writeErrorJSON(w, s.logger, status, code, reply.Message)
	} else {
		writeJSON(w, s.logger, status, body)
	}
	s.logger.Debug("request served",
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.Int("status", status),
		slog.Duration("duration", time.Since(start)),
	)
}

func toRunHoldResponse(h orchestrator.HoldState) runHoldResponse {
	resp := runHoldResponse{
		IssueIdentifier: h.Identifier,
		IssueID:         h.IssueID,
		holdFields:      newHoldFields(h.Held, h.Reason, h.Holder, h.HeldSince, h.HeldTotal),
	}
	if l := h.Lease; l != nil {
		resp.Lease = new(toStandingHoldResponse(orchestrator.SnapshotStandingHold{
			IssueID: h.IssueID, Identifier: l.Identifier, Holder: l.Holder, Reason: l.Reason, Evidence: l.Evidence,
			Since: l.Since, RenewedAt: l.RenewedAt, Until: l.Until, ExpiredAt: l.ExpiredAt,
		}))
	}
	return resp
}

func (s *Server) handleHold(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if s.holdFn == nil {
		writeErrorJSON(w, s.logger, http.StatusServiceUnavailable, "orchestrator_unavailable", "holds are not available")
		return
	}
	var req holdRequest
	if err := decodeControlBody(r, &req); err != nil {
		writeErrorJSON(w, s.logger, http.StatusBadRequest, "invalid_request", "request body must be a JSON object with optional reason, holder (or by), evidence, until, and minutes")
		return
	}
	hold := orchestrator.HoldRequest{
		Reason:   req.Reason,
		Holder:   cmp.Or(req.Holder, req.By),
		Evidence: req.Evidence,
		For:      time.Duration(req.Minutes) * time.Minute,
	}
	if req.Until != "" {
		until, err := time.Parse(time.RFC3339, req.Until)
		if err != nil {
			writeErrorJSON(w, s.logger, http.StatusBadRequest, "invalid_request", "until must be an RFC 3339 time")
			return
		}
		hold.Until = until
	}
	if req.Minutes < 0 {
		writeErrorJSON(w, s.logger, http.StatusBadRequest, "invalid_request", "minutes must not be negative")
		return
	}
	reply := s.holdFn(r.PathValue("identifier"), hold)
	s.writeControlReply(w, r, start, reply, toRunHoldResponse(reply.Hold))
}

func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if s.releaseFn == nil {
		writeErrorJSON(w, s.logger, http.StatusServiceUnavailable, "orchestrator_unavailable", "holds are not available")
		return
	}
	reply := s.releaseFn(r.PathValue("identifier"))
	s.writeControlReply(w, r, start, reply, toRunHoldResponse(reply.Hold))
}

// EscalateFunc escalates the running issue named by identifier to people.
// It must be safe to call from any goroutine.
type EscalateFunc func(identifier string, req orchestrator.EscalationRequest) orchestrator.ControlReply

type escalateRequest struct {
	Message string `json:"message"`
	Kind    string `json:"kind"`
	URL     string `json:"url"`
}

type escalateResponse struct {
	IssueIdentifier string `json:"issue_identifier"`
	EventType       string `json:"event_type"`
	Accepted        bool   `json:"accepted"`
}

func (s *Server) handleEscalate(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if s.escalateFn == nil {
		writeErrorJSON(w, s.logger, http.StatusServiceUnavailable, "orchestrator_unavailable", "escalation is not available")
		return
	}
	var req escalateRequest
	if err := decodeControlBody(r, &req); err != nil {
		writeErrorJSON(w, s.logger, http.StatusBadRequest, "invalid_request", "request body must be a JSON object with message, and optional kind and url")
		return
	}
	identifier := r.PathValue("identifier")
	reply := s.escalateFn(identifier, orchestrator.EscalationRequest{Message: req.Message, Kind: req.Kind, URL: req.URL})
	s.writeControlReply(w, r, start, reply, escalateResponse{IssueIdentifier: identifier, EventType: "session.escalated", Accepted: true})
}

// AnswerFunc answers the parked issue named by identifier. It must be safe
// to call from any goroutine.
type AnswerFunc func(identifier string, answer parkanswer.Answer) orchestrator.ControlReply

type answerRequest struct {
	Text   string `json:"text"`
	Author string `json:"author"`
}

type answerResponse struct {
	IssueIdentifier string    `json:"issue_identifier"`
	IssueID         string    `json:"issue_id"`
	Author          string    `json:"author"`
	Reason          string    `json:"reason"`
	Statement       string    `json:"statement"`
	ParkedAt        time.Time `json:"parked_at"`
	ReleasedAt      time.Time `json:"released_at"`
	Gesture         string    `json:"gesture"`
}

func (s *Server) handleAnswer(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if s.answerFn == nil {
		writeErrorJSON(w, s.logger, http.StatusServiceUnavailable, "orchestrator_unavailable", "answers are not available")
		return
	}
	var req answerRequest
	if err := decodeControlBody(r, &req); err != nil {
		writeErrorJSON(w, s.logger, http.StatusBadRequest, "invalid_request", "request body must be a JSON object with text and author")
		return
	}
	reply := s.answerFn(r.PathValue("identifier"), parkanswer.Answer{Text: req.Text, Author: req.Author})
	a := reply.Answer
	s.writeControlReply(w, r, start, reply, answerResponse{
		IssueIdentifier: a.Identifier,
		IssueID:         a.IssueID,
		Author:          a.Author,
		Reason:          a.Reason,
		Statement:       a.Statement,
		ParkedAt:        a.ParkedAt.UTC(),
		ReleasedAt:      a.ReleasedAt.UTC(),
		Gesture:         "answered",
	})
}
