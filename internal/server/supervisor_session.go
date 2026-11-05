package server

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/sortie-ai/sortie/internal/orchestrator"
)

// SupervisorPauseFunc pauses the supervisor when pause is true and
// resumes it otherwise; by and reason describe a pause. ctx is the
// request's context.
type SupervisorPauseFunc func(ctx context.Context, pause bool, by, reason string) orchestrator.SupervisorPauseResult

// supervisorPauseResponse answers the supervisor pause and resume routes.
// SupervisorPaused is null once the supervisor runs.
type supervisorPauseResponse struct {
	SupervisorPaused *dispatchPausedResponse `json:"supervisor_paused"`
}

// supervisorTriggerResponse is one event that asked for a supervisor
// session.
type supervisorTriggerResponse struct {
	Kind            string    `json:"kind"`
	IssueIdentifier *string   `json:"issue_identifier"`
	Detail          string    `json:"detail"`
	At              time.Time `json:"at"`
}

// supervisorSessionResponse is one supervisor session. EndedAt, Outcome
// and Actions are null while it runs; ID is null for a session that was
// not stored, and Digest for one stored before digests were.
type supervisorSessionResponse struct {
	ID           *int64                      `json:"id"`
	StartedAt    time.Time                   `json:"started_at"`
	EndedAt      *time.Time                  `json:"ended_at"`
	Outcome      *string                     `json:"outcome"`
	Error        *string                     `json:"error"`
	Tokens       int64                       `json:"tokens"`
	TriggerCount int                         `json:"trigger_count"`
	Triggers     []supervisorTriggerResponse `json:"triggers"`
	Digest       *string                     `json:"digest"`
	Actions      *int                        `json:"actions"`
}

// supervisorDigestResponse is the state digest of the last supervisor
// session that ran, and that session.
type supervisorDigestResponse struct {
	Digest           string    `json:"digest"`
	SessionID        *int64    `json:"session_id"`
	SessionStartedAt time.Time `json:"session_started_at"`
	TriggerKind      string    `json:"trigger_kind"`
}

// supervisorSuspensionResponse is a trigger kind that starts no session
// until the state digest changes.
type supervisorSuspensionResponse struct {
	Kind        string    `json:"kind"`
	Digest      string    `json:"digest"`
	Sessions    int       `json:"sessions"`
	Since       time.Time `json:"since"`
	SuspendedAt time.Time `json:"suspended_at"`
}

// supervisorBudgetResponse is the supervisor's spend in the current UTC
// day against its daily budget.
type supervisorBudgetResponse struct {
	Day           time.Time `json:"day"`
	ResetsAt      time.Time `json:"resets_at"`
	SessionsUsed  int       `json:"sessions_used"`
	SessionsLimit int       `json:"sessions_limit"`
	TokensUsed    int64     `json:"tokens_used"`
	TokensLimit   int64     `json:"tokens_limit"`
}

// supervisorResponse is the supervisor's state as GET /api/v1/state
// serves it.
type supervisorResponse struct {
	Status          string                         `json:"status"`
	Agent           string                         `json:"agent"`
	Unavailable     *string                        `json:"unavailable"`
	Paused          *dispatchPausedResponse        `json:"paused"`
	Running         *supervisorSessionResponse     `json:"running"`
	LastSession     *supervisorSessionResponse     `json:"last_session"`
	LastTrigger     *supervisorTriggerResponse     `json:"last_trigger"`
	PendingTriggers []supervisorTriggerResponse    `json:"pending_triggers"`
	Budget          supervisorBudgetResponse       `json:"budget"`
	NextSweepAt     *time.Time                     `json:"next_sweep_at"`
	LastDigest      *supervisorDigestResponse      `json:"last_digest"`
	Suspended       []supervisorSuspensionResponse `json:"suspended_triggers"`
}

func toSupervisorPausedResponse(p *orchestrator.SupervisorPause) *dispatchPausedResponse {
	if p == nil {
		return nil
	}
	return &dispatchPausedResponse{Since: p.Since.UTC(), By: p.By, Reason: p.Reason}
}

func toSupervisorTriggerResponse(t orchestrator.SupervisorTrigger) supervisorTriggerResponse {
	return supervisorTriggerResponse{Kind: t.Kind, IssueIdentifier: nonEmpty(t.Identifier), Detail: t.Detail, At: t.At.UTC()}
}

func toSupervisorTriggerResponses(triggers []orchestrator.SupervisorTrigger) []supervisorTriggerResponse {
	out := make([]supervisorTriggerResponse, 0, len(triggers))
	for _, t := range triggers {
		out = append(out, toSupervisorTriggerResponse(t))
	}
	return out
}

func toSupervisorSessionResponse(s *orchestrator.SupervisorSession) *supervisorSessionResponse {
	if s == nil {
		return nil
	}
	resp := &supervisorSessionResponse{
		ID:           positiveOrNil(s.ID),
		StartedAt:    s.StartedAt.UTC(),
		EndedAt:      faultTimeOrNil(s.EndedAt),
		Outcome:      nonEmpty(s.Outcome),
		Error:        nonEmpty(s.Error),
		Tokens:       s.Tokens,
		TriggerCount: s.TriggerCount,
		Triggers:     toSupervisorTriggerResponses(s.Triggers),
		Digest:       nonEmpty(s.Digest),
	}
	if s.Actions >= 0 && !s.EndedAt.IsZero() {
		resp.Actions = new(s.Actions)
	}
	return resp
}

func positiveOrNil(id int64) *int64 {
	if id <= 0 {
		return nil
	}
	return &id
}

// toSupervisorDigestResponse returns the digest of the running session, or
// else of the last session; nil while neither carries one.
func toSupervisorDigestResponse(s *orchestrator.SupervisorSnapshot) *supervisorDigestResponse {
	session := s.Running
	if session == nil {
		session = s.LastSession
	}
	if session == nil || session.Digest == "" {
		return nil
	}
	resp := &supervisorDigestResponse{Digest: session.Digest, SessionID: positiveOrNil(session.ID), SessionStartedAt: session.StartedAt.UTC()}
	if len(session.Triggers) > 0 {
		resp.TriggerKind = session.Triggers[0].Kind
	}
	return resp
}

func toSupervisorResponse(s *orchestrator.SupervisorSnapshot) *supervisorResponse {
	if s == nil {
		return nil
	}
	resp := &supervisorResponse{
		Status:          s.Status,
		Agent:           s.Agent,
		Unavailable:     nonEmpty(s.Unavailable),
		Paused:          toSupervisorPausedResponse(s.Pause),
		Running:         toSupervisorSessionResponse(s.Running),
		LastSession:     toSupervisorSessionResponse(s.LastSession),
		PendingTriggers: toSupervisorTriggerResponses(s.Pending),
		Budget: supervisorBudgetResponse{
			Day:           s.Budget.Day.UTC(),
			ResetsAt:      s.Budget.ResetsAt.UTC(),
			SessionsUsed:  s.Budget.SessionsUsed,
			SessionsLimit: s.Budget.SessionsLimit,
			TokensUsed:    s.Budget.TokensUsed,
			TokensLimit:   s.Budget.TokensLimit,
		},
		NextSweepAt: faultTimeOrNil(s.NextSweepAt),
		LastDigest:  toSupervisorDigestResponse(s),
		Suspended:   make([]supervisorSuspensionResponse, 0, len(s.Suspended)),
	}
	for _, sus := range s.Suspended {
		resp.Suspended = append(resp.Suspended, supervisorSuspensionResponse{
			Kind: sus.Kind, Digest: sus.Digest, Sessions: sus.Sessions, Since: sus.Since.UTC(), SuspendedAt: sus.At.UTC(),
		})
	}
	if s.LastTrigger != nil {
		t := toSupervisorTriggerResponse(*s.LastTrigger)
		resp.LastTrigger = &t
	}
	return resp
}

func (s *Server) handleSupervisorPause(w http.ResponseWriter, r *http.Request) {
	var req dispatchPauseRequest
	if err := decodeControlBody(r, &req); err != nil {
		writeErrorJSON(w, s.logger, http.StatusBadRequest, "invalid_request", "request body must be a JSON object with optional by and reason")
		return
	}
	if req.By == "" {
		req.By = defaultPausedBy
	}
	s.serveSupervisorPause(w, r, true, req.By, req.Reason)
}

func (s *Server) handleSupervisorResume(w http.ResponseWriter, r *http.Request) {
	s.serveSupervisorPause(w, r, false, "", "")
}

func (s *Server) serveSupervisorPause(w http.ResponseWriter, r *http.Request, pause bool, by, reason string) {
	start := time.Now()
	status := http.StatusOK
	defer func() {
		s.logger.Debug("request served",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", status),
			slog.Duration("duration", time.Since(start)),
		)
	}()
	if s.supervisorPauseFn == nil {
		status = http.StatusServiceUnavailable
		writeErrorJSON(w, s.logger, status, "orchestrator_unavailable", "the supervisor pause is not available")
		return
	}
	result := s.supervisorPauseFn(r.Context(), pause, by, reason)
	switch result.Status {
	case orchestrator.SupervisorPauseDone:
		writeJSON(w, s.logger, status, supervisorPauseResponse{SupervisorPaused: toSupervisorPausedResponse(result.Pause)})
	case orchestrator.SupervisorPauseNotConfigured:
		status = http.StatusConflict
		writeErrorJSON(w, s.logger, status, "supervisor_not_configured", "the workflow configures no supervisor section")
	default:
		status = http.StatusServiceUnavailable
		writeErrorJSON(w, s.logger, status, "orchestrator_unavailable", "the orchestrator did not take the request")
	}
}

// supervisorQuestionRequest is a question as POST
// /api/v1/supervisor/questions and a withdrawal's replacement carry it.
type supervisorQuestionRequest struct {
	Evidence string   `json:"evidence"`
	Kind     string   `json:"kind"`
	Action   string   `json:"action"`
	Where    string   `json:"where"`
	Stories  []string `json:"stories"`
	Checked  string   `json:"checked"`
	Cause    string   `json:"cause"`
}

// question returns q as the orchestrator takes it, raised by the caller
// of r.
func (q supervisorQuestionRequest) question(r *http.Request, evidence string) orchestrator.SupervisorQuestion {
	raisedBy := orchestrator.QuestionRaisedByOperator
	if supervisorCaller(r) {
		raisedBy = orchestrator.QuestionRaisedBySupervisor
	}
	return orchestrator.SupervisorQuestion{
		Kind: q.Kind, Action: q.Action, Where: q.Where, Stories: q.Stories, Checked: q.Checked, Cause: q.Cause,
		Evidence: evidence, RaisedBy: raisedBy,
	}
}

// supervisorQuestionResponse answers POST /api/v1/supervisor/questions.
// Existing reports that an open question of the same story and kind, or
// the same action and place, took the stories and the evidence.
type supervisorQuestionResponse struct {
	Action        string                `json:"action"`
	Existing      bool                  `json:"existing"`
	PlatformFault platformFaultResponse `json:"platform_fault"`
}

// supervisorWithdrawRequest is the body of POST
// /api/v1/supervisor/questions/{id}/withdraw.
type supervisorWithdrawRequest struct {
	Evidence    string                     `json:"evidence"`
	Replacement *supervisorQuestionRequest `json:"replacement"`
}

// supervisorReplacementResponse is the question raised in place of a
// withdrawn one.
type supervisorReplacementResponse struct {
	Existing      bool                  `json:"existing"`
	PlatformFault platformFaultResponse `json:"platform_fault"`
}

// supervisorWithdrawResponse answers POST
// /api/v1/supervisor/questions/{id}/withdraw: the question as it was
// withdrawn, and its replacement, null when none was asked.
type supervisorWithdrawResponse struct {
	Action        string                         `json:"action"`
	PlatformFault platformFaultResponse          `json:"platform_fault"`
	Replacement   *supervisorReplacementResponse `json:"replacement"`
}

// supervisorIssueRequest is the body of POST /api/v1/issues.
type supervisorIssueRequest struct {
	Evidence  string   `json:"evidence"`
	Title     string   `json:"title"`
	Body      string   `json:"body"`
	Kind      string   `json:"kind"`
	Stories   []string `json:"stories"`
	Signature string   `json:"signature"`
}

// supervisorIssueResponse answers POST /api/v1/issues. Existing reports
// that an open issue of the same stories and kind, or the same fault
// signature, took the filing as a comment.
type supervisorIssueResponse struct {
	Action          string  `json:"action"`
	IssueIdentifier string  `json:"issue_identifier"`
	IssueID         string  `json:"issue_id"`
	Existing        bool    `json:"existing"`
	PullRequest     *string `json:"pull_request"`
	MergeCommit     *string `json:"merge_commit"`
}

// serveSupervisor decodes the body of a supervisor route into req and
// returns false, having answered, when it cannot.
func (s *Server) serveSupervisor(w http.ResponseWriter, r *http.Request, req any) bool {
	if s.supervisor == nil {
		writeErrorJSON(w, s.logger, http.StatusServiceUnavailable, "orchestrator_unavailable", "supervisor actions are not available")
		return false
	}
	if err := decodeControlBody(r, req); err != nil {
		writeErrorJSON(w, s.logger, http.StatusBadRequest, "invalid_request", "request body must be a JSON object")
		return false
	}
	return true
}

func (s *Server) logServed(r *http.Request, start time.Time, status int) {
	s.logger.Debug("request served",
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.Int("status", status),
		slog.Duration("duration", time.Since(start)),
	)
}

func (s *Server) handleSupervisorQuestion(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var req supervisorQuestionRequest
	if !s.serveSupervisor(w, r, &req) {
		return
	}
	result := s.supervisor.AskPerson(r.Context(), req.question(r, req.Evidence))
	if result.Status != orchestrator.SupervisorDone {
		s.writeSupervisorResult(w, r, start, supervisorActionAsk, result.SupervisorResult)
		return
	}
	noteSupervised(r, result.Identifier, "", "")
	writeJSON(w, s.logger, http.StatusOK, supervisorQuestionResponse{
		Action:        supervisorActionAsk,
		Existing:      result.Existing,
		PlatformFault: toPlatformFaultResponse(result.Fault),
	})
	s.logServed(r, start, http.StatusOK)
}

func (s *Server) handleWithdrawQuestion(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var req supervisorWithdrawRequest
	if !s.serveSupervisor(w, r, &req) {
		return
	}
	withdrawal := orchestrator.SupervisorWithdrawal{QuestionID: r.PathValue("id"), Evidence: req.Evidence}
	if req.Replacement != nil {
		evidence := req.Replacement.Evidence
		if evidence == "" {
			evidence = req.Evidence
		}
		q := req.Replacement.question(r, evidence)
		withdrawal.Replacement = &q
	}
	result := s.supervisor.WithdrawQuestion(r.Context(), withdrawal)
	if result.Status != orchestrator.SupervisorDone {
		s.writeSupervisorResult(w, r, start, supervisorActionWithdraw, result.SupervisorResult)
		return
	}
	noteSupervised(r, result.Identifier, "", "")
	resp := supervisorWithdrawResponse{
		Action:        supervisorActionWithdraw,
		PlatformFault: toPlatformFaultResponse(result.Withdrawn),
	}
	if result.Replacement != nil {
		resp.Replacement = &supervisorReplacementResponse{
			Existing:      result.Replacement.Existing,
			PlatformFault: toPlatformFaultResponse(result.Replacement.Fault),
		}
	}
	writeJSON(w, s.logger, http.StatusOK, resp)
	s.logServed(r, start, http.StatusOK)
}

func (s *Server) handleFileIssue(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var req supervisorIssueRequest
	if !s.serveSupervisor(w, r, &req) {
		return
	}
	result := s.supervisor.FileIssue(r.Context(), orchestrator.SupervisorIssue{
		Title: req.Title, Body: req.Body, Kind: req.Kind, Stories: req.Stories, Signature: req.Signature, Evidence: req.Evidence,
	})
	if result.Status != orchestrator.SupervisorDone {
		s.writeSupervisorResult(w, r, start, supervisorActionFileIssue, result.SupervisorResult)
		return
	}
	noteSupervised(r, result.Identifier, "", "")
	writeJSON(w, s.logger, http.StatusOK, supervisorIssueResponse{
		Action:          supervisorActionFileIssue,
		IssueIdentifier: result.Identifier,
		IssueID:         result.IssueID,
		Existing:        result.Existing,
	})
	s.logServed(r, start, http.StatusOK)
}
