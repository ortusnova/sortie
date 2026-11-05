package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/sortie-ai/sortie/internal/orchestrator"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// SupervisorActions carries out the supervisor actions besides answering
// a park and granting sessions. [orchestrator.Supervisor] implements it.
// Each method must be safe to call from any goroutine.
type SupervisorActions interface {
	ReleasePark(ctx context.Context, identifier string) orchestrator.SupervisorResult
	RecordMerge(ctx context.Context, identifier, pullRequest, commit string) orchestrator.SupervisorResult
	FixRun(ctx context.Context, identifier, message, profile string) orchestrator.SupervisorResult
	MarkDraft(ctx context.Context, identifier string) orchestrator.SupervisorResult
	Merge(ctx context.Context, identifier string) orchestrator.SupervisorResult
	FileIssue(ctx context.Context, issue orchestrator.SupervisorIssue) orchestrator.SupervisorIssueResult
	RequestLiveProof(ctx context.Context, identifier, criterion, profile string) orchestrator.SupervisorResult
	MergeAndProve(ctx context.Context, identifier string) orchestrator.SupervisorResult
	AskPerson(ctx context.Context, q orchestrator.SupervisorQuestion) orchestrator.SupervisorQuestionResult
	WithdrawQuestion(ctx context.Context, w orchestrator.SupervisorWithdrawal) orchestrator.SupervisorWithdrawResult
}

var _ SupervisorActions = orchestrator.Supervisor{}

// SupervisorLog stores what the supervisor token did or tried.
// [persistence.Store] implements it.
type SupervisorLog interface {
	InsertSupervisorAction(ctx context.Context, a persistence.SupervisorAction) (int64, error)
	RecentSupervisorActions(ctx context.Context, limit int) ([]persistence.SupervisorAction, error)
}

var _ SupervisorLog = (*persistence.Store)(nil)

const (
	// supervisorActionsServed is how many supervisor actions
	// GET /api/v1/state serves, newest first.
	supervisorActionsServed = 50

	// supervisorEvidenceMaxRunes caps the evidence text stored with an
	// action.
	supervisorEvidenceMaxRunes = 8192

	// supervisorRecordBound bounds the write of one action record.
	supervisorRecordBound = 5 * time.Second

	// supervisorGrantedBy names the grantor of the sessions the
	// supervisor grants.
	supervisorGrantedBy = "supervisor"
)

// The names of the supervisor's actions, as supervisor_actions[].action
// serves them.
const (
	supervisorActionAnswer      = "answer"
	supervisorActionRelease     = "release"
	supervisorActionGrant       = "grant_sessions"
	supervisorActionRecordMerge = "record_merge"
	supervisorActionFixRun      = "fix_run"
	supervisorActionDraft       = "mark_draft"
	supervisorActionMerge       = "merge"
	supervisorActionFileIssue   = "file_issue"
	supervisorActionLiveProof   = "live_proof"
	supervisorActionMergeProve  = "merge_and_prove"
	supervisorActionHold        = "hold"
	supervisorActionReleaseHold = "release_hold"
	supervisorActionAsk         = "ask_person"
	supervisorActionWithdraw    = "withdraw_question"
)

// supervisorCtxKey marks the context of a request the supervisor token
// made; its value is the record the request fills.
type supervisorCtxKey struct{}

// supervisorNote is what a handler adds to the record of a supervisor
// request.
type supervisorNote struct {
	identifier  string
	pullRequest string
	mergeCommit string
	profile     string
}

// supervisorCaller reports whether the supervisor token made r.
func supervisorCaller(r *http.Request) bool {
	_, ok := r.Context().Value(supervisorCtxKey{}).(*supervisorNote)
	return ok
}

// noteSupervised adds what the action named to the record of a supervisor
// request; it does nothing for any other caller.
func noteSupervised(r *http.Request, identifier, pullRequest, mergeCommit string) {
	note, ok := r.Context().Value(supervisorCtxKey{}).(*supervisorNote)
	if !ok {
		return
	}
	if identifier != "" {
		note.identifier = identifier
	}
	note.pullRequest, note.mergeCommit = pullRequest, mergeCommit
}

// noteProfile adds the job profile the action named to the record of a
// supervisor request; it does nothing for any other caller.
func noteProfile(r *http.Request, profile string) {
	if note, ok := r.Context().Value(supervisorCtxKey{}).(*supervisorNote); ok {
		note.profile = profile
	}
}

// isSupervisorToken reports whether token is the configured supervisor
// token.
func (s *Server) isSupervisorToken(token string) bool {
	return s.supervisorToken != "" && constantTimeEqual(token, s.supervisorToken)
}

// supervisorRoute registers next as an operator route the supervisor
// token may also use, as action.
func (s *Server) supervisorRoute(action string, next http.HandlerFunc) http.HandlerFunc {
	return s.controlRoute(scopeSupervisor, s.recordSupervised(action, next))
}

// recordSupervised runs next and, for a request the supervisor token made,
// records it with its evidence and result. Such a request must carry
// evidence: one without it is answered 400 and recorded as rejected.
func (s *Server) recordSupervised(action string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !supervisorCaller(r) {
			next(w, r)
			return
		}
		evidence, bodyErr := readEvidence(r)
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		switch {
		case bodyErr != nil:
			writeErrorJSON(rec, s.logger, http.StatusBadRequest, "invalid_request", "request body must be a JSON object of at most 64 KiB")
		case strings.TrimSpace(evidence) == "":
			writeErrorJSON(rec, s.logger, http.StatusBadRequest, "evidence_required", "a supervisor action must carry the evidence it is based on in evidence")
		default:
			next(rec, r)
		}
		note, _ := r.Context().Value(supervisorCtxKey{}).(*supervisorNote)
		s.recordSupervisorAction(r, action, evidence, rec, note)
	}
}

// refuseSupervisor answers 403 to the supervisor token on a route outside
// its scope, and records the refusal.
func (s *Server) refuseSupervisor(w http.ResponseWriter, r *http.Request) {
	evidence, _ := readEvidence(r)
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	s.rejectControl(rec, r, http.StatusForbidden, "forbidden", "the supervisor token may only answer, release, grant sessions, hold or release a hold, record a merge, start a fix run, mark a draft, merge, file an issue, request a live proof, merge and prove, ask a person, or withdraw its own question")
	action := r.Method + " " + r.URL.Path
	if r.Pattern != "" {
		action = r.Pattern
		if !strings.Contains(action, " ") {
			action = r.Method + " " + action
		}
	}
	s.recordSupervisorAction(r, action, evidence, rec, &supervisorNote{identifier: r.PathValue("identifier")})
}

// recordSupervisorAction stores one supervisor request. A failed write is
// logged; the answer already went out.
func (s *Server) recordSupervisorAction(r *http.Request, action, evidence string, rec *statusRecorder, note *supervisorNote) {
	a := persistence.SupervisorAction{
		Action:   action,
		Route:    r.Method + " " + r.URL.Path,
		Evidence: s.maskText(truncateRunes(strings.TrimSpace(evidence), supervisorEvidenceMaxRunes)),
		Status:   rec.status,
		Result:   supervisorResultOf(rec.status),
		ActedAt:  time.Now().UTC(),
	}
	if note != nil {
		a.IssueIdentifier, a.PullRequest, a.MergeCommit, a.Profile = note.identifier, note.pullRequest, note.mergeCommit, note.profile
	}
	if a.IssueIdentifier == "" {
		a.IssueIdentifier = r.PathValue("identifier")
	}
	if rec.status >= http.StatusBadRequest {
		var env errorResponse
		if json.Unmarshal(rec.body.Bytes(), &env) == nil && env.Error.Code != "" {
			a.Detail = s.maskText(env.Error.Code + ": " + env.Error.Message)
		}
	}
	log := s.logger.With(slog.String("action", a.Action), slog.String("result", a.Result), slog.Int("status", a.Status))
	if a.IssueIdentifier != "" {
		log = log.With(slog.String("issue_identifier", a.IssueIdentifier))
	}
	if s.supervisorLog == nil {
		log.Warn("supervisor action not recorded: no store")
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), supervisorRecordBound)
	defer cancel()
	if _, err := s.supervisorLog.InsertSupervisorAction(ctx, a); err != nil {
		log.Error("supervisor action record failed", slog.Any("error", err))
		return
	}
	log.Info("supervisor action recorded")
	s.OnStateChange()
}

func supervisorResultOf(status int) string {
	switch {
	case status < http.StatusBadRequest:
		return persistence.SupervisorDone
	case status == http.StatusForbidden:
		return persistence.SupervisorRefused
	case status < http.StatusInternalServerError:
		return persistence.SupervisorRejected
	default:
		return persistence.SupervisorFailed
	}
}

func (s *Server) maskText(text string) string {
	if s.maskErrorFn == nil {
		return text
	}
	return s.maskErrorFn(text)
}

func truncateRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n])
}

// readEvidence reads the request body, puts it back for the handler, and
// returns its evidence member. A body that is not a JSON object has none.
func readEvidence(r *http.Request) (string, error) {
	if r.Body == nil {
		return "", nil
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxControlBodyBytes+1))
	r.Body = io.NopCloser(bytes.NewReader(body))
	if err != nil || len(body) > maxControlBodyBytes {
		return "", errBodyTooLarge
	}
	var v struct {
		Evidence string `json:"evidence"`
	}
	if len(body) == 0 || json.Unmarshal(body, &v) != nil {
		return "", nil
	}
	return v.Evidence, nil
}

type bodyError string

func (e bodyError) Error() string { return string(e) }

const errBodyTooLarge = bodyError("request body too large or unreadable")

// statusRecorder keeps the status and body a handler writes.
type statusRecorder struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

func (w *statusRecorder) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusRecorder) Write(b []byte) (int, error) {
	if w.body.Len() < maxControlBodyBytes {
		w.body.Write(b)
	}
	return w.ResponseWriter.Write(b)
}

// supervisorActionResponse is one supervisor action as
// GET /api/v1/state serves it.
type supervisorActionResponse struct {
	ID              int64     `json:"id"`
	Action          string    `json:"action"`
	Route           string    `json:"route"`
	IssueIdentifier *string   `json:"issue_identifier"`
	Evidence        string    `json:"evidence"`
	Result          string    `json:"result"`
	Status          int       `json:"status"`
	Detail          *string   `json:"detail"`
	PullRequest     *string   `json:"pull_request"`
	MergeCommit     *string   `json:"merge_commit"`
	Profile         *string   `json:"profile"`
	At              time.Time `json:"at"`
}

// supervisorView holds the member of GET /api/v1/state that lists the
// supervisor's recent actions.
type supervisorView struct {
	SupervisorActions []supervisorActionResponse `json:"supervisor_actions"`
}

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// attachSupervisorActions serves the newest supervisor actions on resp. A
// failed read serves none.
func (s *Server) attachSupervisorActions(ctx context.Context, resp *stateResponse) {
	resp.SupervisorActions = []supervisorActionResponse{}
	if s.supervisorLog == nil {
		return
	}
	actions, err := s.supervisorLog.RecentSupervisorActions(ctx, supervisorActionsServed)
	if err != nil {
		s.logger.Warn("supervisor action read failed", slog.Any("error", err))
		return
	}
	for _, a := range actions {
		resp.SupervisorActions = append(resp.SupervisorActions, supervisorActionResponse{
			ID:              a.ID,
			Action:          a.Action,
			Route:           a.Route,
			IssueIdentifier: nonEmpty(a.IssueIdentifier),
			Evidence:        a.Evidence,
			Result:          a.Result,
			Status:          a.Status,
			Detail:          nonEmpty(a.Detail),
			PullRequest:     nonEmpty(a.PullRequest),
			MergeCommit:     nonEmpty(a.MergeCommit),
			Profile:         nonEmpty(a.Profile),
			At:              a.ActedAt.UTC(),
		})
	}
}

// supervisorResultResponse answers a supervisor action route.
type supervisorResultResponse struct {
	Action          string  `json:"action"`
	IssueIdentifier string  `json:"issue_identifier"`
	IssueID         string  `json:"issue_id"`
	PullRequest     *string `json:"pull_request"`
	MergeCommit     *string `json:"merge_commit"`
}

// writeSupervisorResult writes result with the status its outcome maps
// to, and notes it on the record of a supervisor request.
func (s *Server) writeSupervisorResult(w http.ResponseWriter, r *http.Request, start time.Time, action string, result orchestrator.SupervisorResult) {
	noteSupervised(r, result.Identifier, result.PullRequest, result.MergeCommit)
	status := http.StatusOK
	switch result.Status {
	case orchestrator.SupervisorDone:
		writeJSON(w, s.logger, status, supervisorResultResponse{
			Action:          action,
			IssueIdentifier: result.Identifier,
			IssueID:         result.IssueID,
			PullRequest:     nonEmpty(result.PullRequest),
			MergeCommit:     nonEmpty(result.MergeCommit),
		})
	case orchestrator.SupervisorUnknownIssue:
		status = http.StatusNotFound
		code := "issue_not_found"
		if result.Code != "" {
			code = result.Code
		}
		writeErrorJSON(w, s.logger, status, code, result.Message)
	case orchestrator.SupervisorPrecondition:
		status = http.StatusConflict
		if result.Code == "invalid_request" {
			status = http.StatusBadRequest
		}
		writeErrorJSON(w, s.logger, status, result.Code, result.Message)
	case orchestrator.SupervisorUnsupported:
		status = http.StatusUnprocessableEntity
		writeErrorJSON(w, s.logger, status, "capability_missing", result.Message)
	case orchestrator.SupervisorUpstreamFailed:
		status = http.StatusBadGateway
		writeErrorJSON(w, s.logger, status, "upstream_error", result.Message)
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

// supervisorRequest is the body of the supervisor action routes; each
// route reads the members it names.
type supervisorRequest struct {
	Evidence    string `json:"evidence"`
	PullRequest string `json:"pull_request"`
	Commit      string `json:"commit"`
	Message     string `json:"message"`
	Profile     string `json:"profile"`
}

// supervisorHandler returns the handler of one supervisor action route,
// which decodes the body and calls do.
func (s *Server) supervisorHandler(action string, do func(ctx context.Context, sup SupervisorActions, identifier string, req supervisorRequest) orchestrator.SupervisorResult) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		if s.supervisor == nil {
			writeErrorJSON(w, s.logger, http.StatusServiceUnavailable, "orchestrator_unavailable", "supervisor actions are not available")
			return
		}
		var req supervisorRequest
		if err := decodeControlBody(r, &req); err != nil {
			writeErrorJSON(w, s.logger, http.StatusBadRequest, "invalid_request", "request body must be a JSON object")
			return
		}
		req.Profile = strings.TrimSpace(req.Profile)
		noteProfile(r, req.Profile)
		s.writeSupervisorResult(w, r, start, action, do(r.Context(), s.supervisor, r.PathValue("identifier"), req))
	}
}

// registerSupervisorRoutes registers the supervisor action routes that
// have no operator counterpart elsewhere.
func (s *Server) registerSupervisorRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/{identifier}/unpark", s.supervisorRoute(supervisorActionRelease, s.supervisorHandler(supervisorActionRelease,
		func(ctx context.Context, sup SupervisorActions, id string, _ supervisorRequest) orchestrator.SupervisorResult {
			return sup.ReleasePark(ctx, id)
		})))
	mux.HandleFunc("/api/v1/{identifier}/merge-record", s.supervisorRoute(supervisorActionRecordMerge, s.supervisorHandler(supervisorActionRecordMerge,
		func(ctx context.Context, sup SupervisorActions, id string, req supervisorRequest) orchestrator.SupervisorResult {
			return sup.RecordMerge(ctx, id, req.PullRequest, req.Commit)
		})))
	mux.HandleFunc("/api/v1/{identifier}/fix", s.supervisorRoute(supervisorActionFixRun, s.supervisorHandler(supervisorActionFixRun,
		func(ctx context.Context, sup SupervisorActions, id string, req supervisorRequest) orchestrator.SupervisorResult {
			return sup.FixRun(ctx, id, req.Message, req.Profile)
		})))
	mux.HandleFunc("/api/v1/{identifier}/draft", s.supervisorRoute(supervisorActionDraft, s.supervisorHandler(supervisorActionDraft,
		func(ctx context.Context, sup SupervisorActions, id string, _ supervisorRequest) orchestrator.SupervisorResult {
			return sup.MarkDraft(ctx, id)
		})))
	mux.HandleFunc("/api/v1/{identifier}/merge", s.supervisorRoute(supervisorActionMerge, s.supervisorHandler(supervisorActionMerge,
		func(ctx context.Context, sup SupervisorActions, id string, _ supervisorRequest) orchestrator.SupervisorResult {
			return sup.Merge(ctx, id)
		})))
	mux.HandleFunc("/api/v1/{identifier}/live-proof", s.supervisorRoute(supervisorActionLiveProof, s.supervisorHandler(supervisorActionLiveProof,
		func(ctx context.Context, sup SupervisorActions, id string, req supervisorRequest) orchestrator.SupervisorResult {
			return sup.RequestLiveProof(ctx, id, req.Evidence, req.Profile)
		})))
	mux.HandleFunc("/api/v1/{identifier}/merge-and-prove", s.supervisorRoute(supervisorActionMergeProve, s.supervisorHandler(supervisorActionMergeProve,
		func(ctx context.Context, sup SupervisorActions, id string, _ supervisorRequest) orchestrator.SupervisorResult {
			return sup.MergeAndProve(ctx, id)
		})))
	mux.HandleFunc("/api/v1/supervisor/questions", s.supervisorRoute(supervisorActionAsk, s.handleSupervisorQuestion))
	mux.HandleFunc("/api/v1/supervisor/questions/{id}/withdraw", s.supervisorRoute(supervisorActionWithdraw, s.handleWithdrawQuestion))
	mux.HandleFunc("/api/v1/issues", s.supervisorRoute(supervisorActionFileIssue, s.handleFileIssue))
}

// supervisorGrantCap returns the cap of a session grant r asks for: the
// supervisor's configured cap for the supervisor token, and none for any
// other caller.
func (s *Server) supervisorGrantCap(r *http.Request) (capped bool, limit int) {
	if !supervisorCaller(r) {
		return false, 0
	}
	return true, s.supervisorSessionCap
}

// overCapMessage says why a supervisor grant was refused.
func overCapMessage(limit int) string {
	if limit <= 0 {
		return "the supervisor may grant no sessions: server.supervisor_max_sessions is 0"
	}
	return "the supervisor may grant an issue at most " + strconv.Itoa(limit) + " sessions in total"
}
