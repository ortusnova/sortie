package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/failcause"
	"github.com/sortie-ai/sortie/internal/logging"
	"github.com/sortie-ai/sortie/internal/redact"
)

// PlatformFaultClassRevertQuestion is the class of the question Sortie
// asks a person when a merged revert it did not make undoes a story's
// merge: re-queue the story, or cancel it.
const PlatformFaultClassRevertQuestion = "revert_question"

// revertQuestionStep is the step of a revert question's fault.
const revertQuestionStep = "revert"

// QuestionRaisedBySortie is who raised a question Sortie asks on its own,
// such as a revert question.
const QuestionRaisedBySortie = "sortie"

// The answers a revert question takes.
const (
	// RevertAnswerRequeue returns the story to tracker.release_state.
	RevertAnswerRequeue = "requeue"
	// RevertAnswerCancel moves the story to tracker.cancel_state.
	RevertAnswerCancel = "cancel"
)

// ReasonCancelled is the reason revert detection, merge-and-prove, plan
// control and the supervisor routes give when they leave a story in
// tracker.cancel_state where it is.
const ReasonCancelled = "cancelled"

// isCancelledState reports whether state is the tracker's cancel state.
func isCancelledState(state string, t config.TrackerConfig) bool {
	return t.CancelState != "" && strings.EqualFold(strings.TrimSpace(state), t.CancelState)
}

// storyCancelled reads the story issueID and reports whether it is in
// tracker.cancel_state, with that state. A failed read reports false: the
// caller's own tracker writes still go to the tracker.
func storyCancelled(ctx context.Context, cfg config.ServiceConfig, tracker domain.TrackerAdapter, issueID string) (string, bool) {
	if cfg.Tracker.CancelState == "" || tracker == nil {
		return "", false
	}
	rctx, cancel := context.WithTimeout(ctx, planControlWriteBound)
	defer cancel()
	issue, err := tracker.FetchIssueByID(rctx, issueID)
	if err != nil || !isCancelledState(issue.State, cfg.Tracker) {
		return "", false
	}
	return issue.State, true
}

// revertQuestionSignature is the signature of the revert question of the
// story issueID: one open question per story.
func revertQuestionSignature(issueID string) string {
	return PlatformFaultClassRevertQuestion + ": " + issueID
}

// revertQuestionOf returns the open revert question that holds issueID,
// nil when none does.
func revertQuestionOf(state *State, issueID string) *PlatformFault {
	signature := revertQuestionSignature(issueID)
	for _, f := range state.PlatformFaults {
		if f.Class == PlatformFaultClassRevertQuestion && f.Signature == signature {
			return f
		}
	}
	return nil
}

// answerFits returns answer as a question of f takes it, or why it does
// not fit: a revert question takes requeue (or re-queue) or cancel, and
// any other fault no answer.
func answerFits(f *PlatformFault, answer string) (string, string, bool) {
	answer = strings.ToLower(strings.TrimSpace(answer))
	if f.Class != PlatformFaultClassRevertQuestion {
		if answer != "" {
			return "", "platform fault " + f.ID + " takes no answer: mark its action done without one", false
		}
		return "", "", true
	}
	switch answer {
	case RevertAnswerRequeue, "re-queue":
		return RevertAnswerRequeue, "", true
	case RevertAnswerCancel:
		return RevertAnswerCancel, "", true
	}
	return "", "question " + f.ID + " asks re-queue, or cancel?: answer " + RevertAnswerRequeue + " or " + RevertAnswerCancel, false
}

// holdRevertedOnTracker reads the story whose merge a revert Sortie did
// not make undid, and moves it out of its done state to
// tracker.handoff_state, where it waits for a person's answer; never to
// tracker.release_state. A story in tracker.cancel_state is left where it
// is and reported cancelled, as is a story in any state but a done or
// terminal one. It returns the story's identifier and the state it moved
// the story to, empty when it left it. It reads only its arguments, so it
// may run off the event loop.
func holdRevertedOnTracker(ctx context.Context, cfg config.ServiceConfig, tracker domain.TrackerAdapter, issueID string) (identifier, moved string, cancelled bool, err error) {
	if tracker == nil {
		return "", "", false, nil
	}
	rctx, cancel := context.WithTimeout(ctx, mergeAndProveBound)
	issue, err := tracker.FetchIssueByID(rctx, issueID)
	cancel()
	if err != nil {
		return "", "", false, fmt.Errorf("tracker read: %w", err)
	}
	if isCancelledState(issue.State, cfg.Tracker) {
		return issue.Identifier, "", true, nil
	}
	target := cfg.Tracker.HandoffState
	done := isTerminalState(issue.State, cfg.Tracker.TerminalStates) || strings.EqualFold(issue.State, cfg.Tracker.DoneState)
	if target == "" || !done || strings.EqualFold(issue.State, target) {
		return issue.Identifier, "", false, nil
	}
	wctx, wcancel := context.WithTimeout(ctx, planControlWriteBound)
	err = tracker.TransitionIssue(wctx, issueID, target)
	wcancel()
	if err != nil {
		return issue.Identifier, "", false, fmt.Errorf("move to %s: %w", target, err)
	}
	return issue.Identifier, target, false, nil
}

// revertAsked is what a revert question asks about: the story, its
// reverted merge and the revert that undid it.
type revertAsked struct {
	issueID, identifier string
	merged              string // the reverted merge, as "its merge of owner/repo#N (sha)"
	revert              string // the reverting pull request, owner/repo#N
	moved               string // the state the story was moved to, empty when left
}

// raiseRevertQuestion asks a person whether the story a revert Sortie did
// not make undid lands again: a platform fault of class
// [PlatformFaultClassRevertQuestion] and kind decision that holds the
// story out of dispatch, retries and fix runs until a person answers it.
// A story with an open revert question keeps it, and the new revert is
// added to its detail. Must run on the event loop.
func (o *Orchestrator) raiseRevertQuestion(ctx context.Context, cfg config.ServiceConfig, a revertAsked, now time.Time) *PlatformFault {
	f := revertQuestionOf(o.state, a.issueID)
	detail := fmt.Sprintf("%s merged, which reverts %s. Sortie removed the story's merge record, so the stories it blocks wait for it again", a.revert, a.merged)
	if a.moved != "" {
		detail += ", and moved it to " + a.moved
	}
	detail += "."
	if f != nil {
		f.Detail += "\n\n" + redact.Mask(detail)
	} else {
		signature := revertQuestionSignature(a.issueID)
		id := platformFaultID(signature, now)
		release, cancelState := cmpOrUnset(cfg.Tracker.ReleaseState), cmpOrUnset(cfg.Tracker.CancelState)
		f = &PlatformFault{
			ID:        id,
			Signature: signature,
			Class:     PlatformFaultClassRevertQuestion,
			Step:      revertQuestionStep,
			Cause: redact.Mask(fmt.Sprintf("%s merged on the forge and reverts %s's %s. Sortie did not make this revert, so it does not know whether the story should land again.",
				a.revert, a.identifier, strings.TrimPrefix(a.merged, "its "))),
			Detail: redact.Mask(detail),
			Action: &failcause.PersonAction{
				Action: redact.Mask(fmt.Sprintf("%s: re-queue, or cancel? %s reverted %s. Answer %s to return the story to %s, or %s to move it to %s.",
					a.identifier, a.revert, a.merged, RevertAnswerRequeue, release, RevertAnswerCancel, cancelState)),
				Where: "Sortie's API with the control token: POST /api/v1/platform-faults/" + id + `/action-done {"by": "<your name>", "answer": "requeue" or "cancel"}`,
			},
			RecordedAt:   now,
			NextCheckAt:  now.Add(PlatformFaultCheckInterval),
			QuestionKind: "decision",
			RaisedBy:     QuestionRaisedBySortie,
		}
		o.state.PlatformFaults[f.ID] = f
	}
	log := logging.WithIssue(o.logger, a.issueID, a.identifier).With(slog.String("platform_fault_id", f.ID))
	holdUnderPlatformFault(ctx, o.state, o.store, f, PlatformFaultHold{IssueID: a.issueID, Identifier: a.identifier, HeldAt: now}, log)
	noteSupervisorTrigger(o.state, SupervisorTrigger{
		Kind: SupervisorTriggerPark, IssueID: a.issueID, Identifier: a.identifier,
		Detail: "parked: " + a.revert + " reverted its merge; a person answers re-queue, or cancel?", At: now,
	})
	log.Warn("story parked with a question: a revert Sortie did not make undid its merge",
		slog.String("revert", a.revert), slog.String("question_kind", f.QuestionKind), slog.String("state", a.moved))
	return f
}

// cmpOrUnset returns state, or a placeholder naming that it is not set.
func cmpOrUnset(state string) string {
	if state == "" {
		return "(not set)"
	}
	return state
}

// checkRevertQuestion carries out the answer a person gave a revert
// question: requeue moves each story it holds to tracker.release_state,
// cancel to tracker.cancel_state, and either comments the answer and who
// gave it on the story. A story in tracker.cancel_state is never moved
// back: it is left as it is, with the reason cancelled. The check fails,
// so the question stays open, until a person answers and while a tracker
// call fails. It runs off the event loop.
func (o *Orchestrator) checkRevertQuestion(ctx context.Context, check PlatformFaultCheck) error {
	if check.Answer == "" {
		return errAwaitingPersonAction
	}
	if o.trackerAdapter == nil || o.workflowManager == nil {
		return errors.New("no tracker is configured to carry out the answer")
	}
	cfg := o.workflowManager.Config()
	target, field := cfg.Tracker.ReleaseState, "tracker.release_state"
	if check.Answer == RevertAnswerCancel {
		target, field = cfg.Tracker.CancelState, "tracker.cancel_state"
	}
	if target == "" {
		return fmt.Errorf("the answer %s moves the story to %s, which the workflow does not set", check.Answer, field)
	}
	by := check.ActionDoneBy
	if by == "" {
		by = "a person"
	}
	for _, h := range check.Stories {
		log := logging.WithIssue(o.logger, h.IssueID, h.Identifier).With(slog.String("platform_fault_id", check.FaultID),
			slog.String("answer", check.Answer), slog.String("by", by))
		rctx, cancel := context.WithTimeout(ctx, mergeAndProveBound)
		issue, err := o.trackerAdapter.FetchIssueByID(rctx, h.IssueID)
		cancel()
		if err != nil {
			return fmt.Errorf("tracker read of %s: %w", h.Identifier, err)
		}
		body := fmt.Sprintf("%s answered %s to Sortie's question %s, re-queue, or cancel?, after the revert of this story's merge.", by, check.Answer, check.FaultID)
		switch {
		case isCancelledState(issue.State, cfg.Tracker):
			log.Info("revert question answered; the story is cancelled, so Sortie leaves it", slog.String("reason", ReasonCancelled), slog.String("state", issue.State))
			body += " The story is cancelled (" + issue.State + "), so Sortie leaves it there."
		case strings.EqualFold(issue.State, target):
			log.Info("revert question answered; the story is already in its state", slog.String("state", issue.State))
			body += " The story is already in " + issue.State + "."
		default:
			wctx, wcancel := context.WithTimeout(ctx, planControlWriteBound)
			err := o.trackerAdapter.TransitionIssue(wctx, h.IssueID, target)
			wcancel()
			if err != nil {
				return fmt.Errorf("move %s to %s: %w", h.Identifier, target, err)
			}
			log.Info("revert question answered; story moved", slog.String("state", target))
			body += " Sortie moved the story to " + target + "."
		}
		wctx, wcancel := context.WithTimeout(ctx, planControlWriteBound)
		if err := o.trackerAdapter.CommentIssue(wctx, h.IssueID, body); err != nil {
			log.Warn("revert answer comment failed", slog.Any("error", err))
		}
		wcancel()
	}
	return nil
}

// refuseRevertHeld refuses a supervisor action on a story an open revert
// question holds: only a person's answer releases it.
func refuseRevertHeld(state *State, issue supervisedIssue) (SupervisorResult, bool) {
	if f := revertQuestionOf(state, issue.issueID); f != nil {
		return precondition(issue, "revert_question", "issue "+issue.identifier+" waits for a person's answer to question "+f.ID+
			", re-queue, or cancel?, after a revert Sortie did not make"), false
	}
	return SupervisorResult{}, true
}

// refuseCancelled reads the story's tracker state and refuses a
// supervisor action on a story in tracker.cancel_state, with the reason
// cancelled. A failed read refuses nothing: the action's own reads and
// writes still go to the tracker.
func (s Supervisor) refuseCancelled(ctx context.Context, issue supervisedIssue) (SupervisorResult, bool) {
	state, cancelled := storyCancelled(ctx, s.o.workflowManager.Config(), s.o.trackerAdapter, issue.issueID)
	if !cancelled {
		return SupervisorResult{}, true
	}
	return cancelledRefusal(issue, state), false
}

// refuseHeldOrCancelled refuses a supervisor action that would run,
// merge or release a story an open revert question holds, or a story in
// tracker.cancel_state.
func (s Supervisor) refuseHeldOrCancelled(ctx context.Context, issue supervisedIssue) (SupervisorResult, bool) {
	refusal, ok := SupervisorResult{}, true
	if r, ran := s.onLoop(ctx, func(context.Context) { refusal, ok = refuseRevertHeld(s.o.state, issue) }); !ran {
		return r, false
	}
	if !ok {
		return refusal, false
	}
	return s.refuseCancelled(ctx, issue)
}

// cancelledRefusal refuses an action on a story in the cancel state.
func cancelledRefusal(issue supervisedIssue, state string) SupervisorResult {
	return precondition(issue, ReasonCancelled, "issue "+issue.identifier+" is cancelled ("+state+"): Sortie does not move a cancelled story")
}
