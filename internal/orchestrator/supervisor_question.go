package orchestrator

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/failcause"
	"github.com/sortie-ai/sortie/internal/logging"
	"github.com/sortie-ai/sortie/internal/redact"
)

// PlatformFaultClassSupervisorQuestion is the class of the platform fault
// that carries a question the supervisor raised for a person.
const PlatformFaultClassSupervisorQuestion = "supervisor_question"

// supervisorQuestionStep is the step of a supervisor question's fault.
const supervisorQuestionStep = "supervisor"

// Who raised a question through the supervisor's question route.
const (
	QuestionRaisedBySupervisor = "supervisor"
	QuestionRaisedByOperator   = "operator"
)

// QuestionKinds are the action kinds a supervisor question takes: what a
// person is asked to provide or do.
var QuestionKinds = []string{"credential", "key", "approval", "decision", "host_action", "live_proof"}

// SupervisorQuestion is a question the supervisor raises for a person:
// its action kind, the exact action, where to take it, the stories it
// holds, and what the supervisor already checked. Cause says why the
// action is needed and Evidence what the request is based on; either may
// be empty. RaisedBy is [QuestionRaisedBySupervisor] or
// [QuestionRaisedByOperator].
type SupervisorQuestion struct {
	Kind     string
	Action   string
	Where    string
	Stories  []string
	Checked  string
	Cause    string
	Evidence string
	RaisedBy string
}

// SupervisorQuestionResult is the answer to [Supervisor.AskPerson]. Fault
// is the platform fault that carries the question; Existing reports that
// an open question already asked it, so the stories and the evidence
// joined that one instead of a second question.
type SupervisorQuestionResult struct {
	SupervisorResult
	Fault    SnapshotPlatformFault
	Existing bool
}

// questionSignature is the signature that makes one question of one
// decision: its action and place, case and spacing folded.
func questionSignature(action, where string) string {
	return PlatformFaultClassSupervisorQuestion + ": " + foldText(action) + " @ " + foldText(where)
}

// foldText folds case and spacing.
func foldText(s string) string { return strings.ToLower(strings.Join(strings.Fields(s), " ")) }

// normalizeQuestion trims q and its stories, dropping repeats, and
// reports the refusal of a question that lacks a member.
func normalizeQuestion(q SupervisorQuestion) (SupervisorQuestion, SupervisorResult, bool) {
	q.Kind = strings.TrimSpace(q.Kind)
	q.Action, q.Where, q.Checked = strings.TrimSpace(q.Action), strings.TrimSpace(q.Where), strings.TrimSpace(q.Checked)
	q.Cause, q.Evidence = strings.TrimSpace(q.Cause), strings.TrimSpace(q.Evidence)
	stories := make([]string, 0, len(q.Stories))
	for _, id := range q.Stories {
		if id = strings.TrimSpace(id); id != "" && !slices.Contains(stories, id) {
			stories = append(stories, id)
		}
	}
	q.Stories = stories
	if q.RaisedBy == "" {
		q.RaisedBy = QuestionRaisedByOperator
	}
	switch {
	case q.Action == "" || q.Where == "" || q.Checked == "" || len(stories) == 0:
		return q, SupervisorResult{
			Status: SupervisorPrecondition, Code: "invalid_request",
			Message: "a question names its kind, the action, where to take it, the stories it holds, and what the supervisor checked",
		}, false
	case !slices.Contains(QuestionKinds, q.Kind):
		return q, SupervisorResult{
			Status: SupervisorPrecondition, Code: "invalid_request",
			Message: "a question's kind is one of " + strings.Join(QuestionKinds, ", "),
		}, false
	}
	return q, SupervisorResult{}, true
}

// AskPerson raises q for a person through the person-action path: a
// platform fault whose person action is q's action and place, whose
// detail is what the supervisor checked, and which holds q's stories out
// of dispatch until a person marks the action done and its check passes.
// A question that shares a story and its kind with an open question, or
// names the action and place of one, joins that one whatever its wording:
// its stories are added and what it checked and its evidence are appended
// to the open question's detail. A story with a running worker is
// refused, since the hold would take its claim.
func (s Supervisor) AskPerson(ctx context.Context, q SupervisorQuestion) SupervisorQuestionResult {
	q, refusal, ok := normalizeQuestion(q)
	if !ok {
		return SupervisorQuestionResult{SupervisorResult: refusal}
	}
	var result SupervisorQuestionResult
	if r, ok := s.onLoop(ctx, func(loopCtx context.Context) {
		issues, refusal, ok := questionStories(s.o.state, q.Stories)
		if !ok {
			result = SupervisorQuestionResult{SupervisorResult: refusal}
			return
		}
		result = s.raiseOnLoop(loopCtx, q, issues, time.Now().UTC())
	}); !ok {
		return SupervisorQuestionResult{SupervisorResult: r}
	}
	return result
}

// questionStories looks up each story a question holds, and refuses an
// unknown one or one with a running worker.
func questionStories(state *State, stories []string) ([]supervisedIssue, SupervisorResult, bool) {
	issues := make([]supervisedIssue, 0, len(stories))
	for _, identifier := range stories {
		issue, known := lookupSupervised(state, identifier)
		switch {
		case !known:
			return nil, unknownIssue(identifier), false
		case issue.running:
			return nil, precondition(issue, "running", "issue "+identifier+" has a running worker"), false
		}
		issues = append(issues, issue)
	}
	return issues, SupervisorResult{}, true
}

// openQuestionFor returns the open supervisor question q joins: the
// oldest one of q's kind that holds one of issues, else the one with q's
// signature; nil when there is none.
func openQuestionFor(state *State, q SupervisorQuestion, issues []supervisedIssue) *PlatformFault {
	var byKind, bySignature *PlatformFault
	signature := questionSignature(q.Action, q.Where)
	for _, f := range state.PlatformFaults {
		if f.Class != PlatformFaultClassSupervisorQuestion {
			continue
		}
		if f.Signature == signature && (bySignature == nil || f.RecordedAt.Before(bySignature.RecordedAt)) {
			bySignature = f
		}
		holds := slices.ContainsFunc(issues, func(issue supervisedIssue) bool {
			return slices.ContainsFunc(f.Holds, func(h PlatformFaultHold) bool { return h.IssueID == issue.issueID })
		})
		if f.QuestionKind == q.Kind && holds && (byKind == nil || f.RecordedAt.Before(byKind.RecordedAt)) {
			byKind = f
		}
	}
	if byKind != nil {
		return byKind
	}
	return bySignature
}

// questionEvidence is what a question adds to the detail of the question
// it joins: what it checked, and its evidence.
func questionEvidence(q SupervisorQuestion) string {
	text := q.Checked
	if q.Evidence != "" && q.Evidence != q.Checked {
		text += "\nEvidence: " + q.Evidence
	}
	return text
}

// raiseOnLoop records q, which holds issues, as a new question or joins
// the open one it matches. Must run on the event loop.
func (s Supervisor) raiseOnLoop(ctx context.Context, q SupervisorQuestion, issues []supervisedIssue, now time.Time) SupervisorQuestionResult {
	state := s.o.state
	fault := openQuestionFor(state, q, issues)
	existing := fault != nil
	if existing {
		fault.Detail += "\n\n" + redact.Mask(questionEvidence(q))
	} else {
		cause := q.Cause
		if cause == "" {
			cause = "The supervisor asks a person to act."
		}
		signature := questionSignature(q.Action, q.Where)
		fault = &PlatformFault{
			ID:           platformFaultID(signature, now),
			Signature:    signature,
			Class:        PlatformFaultClassSupervisorQuestion,
			Step:         supervisorQuestionStep,
			Cause:        redact.Mask(cause),
			Detail:       redact.Mask(questionEvidence(q)),
			Action:       &failcause.PersonAction{Action: q.Action, Where: q.Where},
			RecordedAt:   now,
			NextCheckAt:  now.Add(PlatformFaultCheckInterval),
			QuestionKind: q.Kind,
			RaisedBy:     q.RaisedBy,
		}
		state.PlatformFaults[fault.ID] = fault
	}
	log := s.o.logger.With(slog.String("platform_fault_id", fault.ID))
	for _, issue := range issues {
		holdUnderPlatformFault(ctx, state, s.o.store, fault, PlatformFaultHold{
			IssueID: issue.issueID, Identifier: issue.identifier, HeldAt: now,
		}, logging.WithIssue(log, issue.issueID, issue.identifier))
	}
	if existing {
		// A story the joined question already held adds no hold, so
		// the appended evidence is saved here.
		savePlatformFault(ctx, s.o.store, fault, time.Time{}, log)
	}
	log.Info("supervisor raised a question for a person",
		slog.String("question_kind", q.Kind),
		slog.String("raised_by", q.RaisedBy),
		slog.String("person_action", redact.Mask(q.Action)),
		slog.String("where", redact.Mask(q.Where)),
		slog.Int("stories", len(fault.Holds)),
		slog.Bool("existing", existing),
	)
	s.o.notifyObservers()
	return SupervisorQuestionResult{
		SupervisorResult: SupervisorResult{Status: SupervisorDone, IssueID: issues[0].issueID, Identifier: issues[0].identifier},
		Fault:            snapshotPlatformFault(fault),
		Existing:         existing,
	}
}

// SupervisorWithdrawal withdraws the question QuestionID on Evidence, and
// raises Replacement in its place when it is not nil.
type SupervisorWithdrawal struct {
	QuestionID  string
	Evidence    string
	Replacement *SupervisorQuestion
}

// SupervisorWithdrawResult is the answer to [Supervisor.WithdrawQuestion]:
// the question as it was withdrawn, and the replacement when one was
// raised.
type SupervisorWithdrawResult struct {
	SupervisorResult
	Withdrawn   SnapshotPlatformFault
	Replacement *SupervisorQuestionResult
}

// WithdrawQuestion withdraws an open question the supervisor raised:
// the question leaves the open platform faults, is stored resolved with
// the evidence, and releases the stories it holds, so they are free for
// their next action, and a poll is queued. A replacement is raised in the
// same event-loop turn, as [Supervisor.AskPerson] raises a question; when
// it would be refused, nothing changes. A fault that is not a supervisor
// question, or a question a person raised, is refused.
func (s Supervisor) WithdrawQuestion(ctx context.Context, w SupervisorWithdrawal) SupervisorWithdrawResult {
	w.QuestionID, w.Evidence = strings.TrimSpace(w.QuestionID), strings.TrimSpace(w.Evidence)
	if w.Evidence == "" {
		return SupervisorWithdrawResult{SupervisorResult: SupervisorResult{
			Status: SupervisorPrecondition, Code: "invalid_request", Message: "a withdrawal carries the evidence the question is stale",
		}}
	}
	var replacement SupervisorQuestion
	if w.Replacement != nil {
		q, refusal, ok := normalizeQuestion(*w.Replacement)
		if !ok {
			refusal.Message = "replacement: " + refusal.Message
			return SupervisorWithdrawResult{SupervisorResult: refusal}
		}
		replacement = q
	}
	var result SupervisorWithdrawResult
	if r, ok := s.onLoop(ctx, func(loopCtx context.Context) {
		result = s.withdrawOnLoop(loopCtx, w, replacement, time.Now().UTC())
	}); !ok {
		return SupervisorWithdrawResult{SupervisorResult: r}
	}
	return result
}

func (s Supervisor) withdrawOnLoop(ctx context.Context, w SupervisorWithdrawal, replacement SupervisorQuestion, now time.Time) SupervisorWithdrawResult {
	state := s.o.state
	f := state.PlatformFaults[w.QuestionID]
	switch {
	case f == nil:
		return SupervisorWithdrawResult{SupervisorResult: SupervisorResult{
			Status: SupervisorUnknownIssue, Code: "question_not_found", Message: "no open question has the ID " + w.QuestionID,
		}}
	case f.Class != PlatformFaultClassSupervisorQuestion:
		return SupervisorWithdrawResult{SupervisorResult: questionRefusal(f,
			"platform fault "+f.ID+" is not a question the supervisor raised: Sortie or a story's agent recorded it ("+f.Class+"), and its check or a person clears it")}
	case f.RaisedBy != QuestionRaisedBySupervisor:
		return SupervisorWithdrawResult{SupervisorResult: questionRefusal(f,
			"question "+f.ID+" was raised by a person ("+f.RaisedBy+"), and only they or its check clear it")}
	}
	var issues []supervisedIssue
	if w.Replacement != nil {
		var refusal SupervisorResult
		var ok bool
		if issues, refusal, ok = questionStories(state, replacement.Stories); !ok {
			refusal.Message = "replacement: " + refusal.Message
			return SupervisorWithdrawResult{SupervisorResult: refusal}
		}
	}

	log := s.o.logger.With(slog.String("platform_fault_id", f.ID))
	f.Withdrawn = redact.Mask(w.Evidence)
	delete(state.PlatformFaults, f.ID)
	for _, h := range f.Holds {
		if state.FaultHolds[h.IssueID] == f.ID {
			delete(state.FaultHolds, h.IssueID)
		}
	}
	savePlatformFault(ctx, s.o.store, f, now, log)
	log.Info("supervisor withdrew its question, releasing held stories",
		slog.String("question_kind", f.QuestionKind),
		slog.Int("released", len(f.Holds)),
	)
	result := SupervisorWithdrawResult{
		SupervisorResult: SupervisorResult{Status: SupervisorDone},
		Withdrawn:        snapshotPlatformFault(f),
	}
	if len(f.Holds) > 0 {
		result.IssueID, result.Identifier = f.Holds[0].IssueID, f.Holds[0].Identifier
	}
	if w.Replacement != nil {
		raised := s.raiseOnLoop(ctx, replacement, issues, now)
		result.Replacement = &raised
	}
	s.o.RefreshFunc()()
	s.o.notifyObservers()
	return result
}

// questionRefusal refuses the withdrawal of f.
func questionRefusal(f *PlatformFault, message string) SupervisorResult {
	r := SupervisorResult{Status: SupervisorPrecondition, Code: "not_supervisor_question", Message: message}
	if len(f.Holds) > 0 {
		r.IssueID, r.Identifier = f.Holds[0].IssueID, f.Holds[0].Identifier
	}
	return r
}
