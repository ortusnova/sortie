package orchestrator

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/redact"
)

// personQuestionKinds are the question kinds classed for a person: only
// a person holds a credential or a key, approves, or decides for the
// operator. A question of another kind is the supervisor's to follow, so
// no person is notified of it.
var personQuestionKinds = []string{"credential", "key", "approval", "decision"}

// forPerson reports whether f is a question classed for a person.
func (f *PlatformFault) forPerson() bool {
	return f.Class == PlatformFaultClassSupervisorQuestion && slices.Contains(personQuestionKinds, f.QuestionKind)
}

// questionHoldState returns the tracker state Sortie last knew issueID
// in, the state a question that takes it now was raised in; empty when
// it knows none, which the first observation fills.
func questionHoldState(state *State, issueID string) string {
	if p := state.Parked[issueID]; p != nil && p.ParkedState != "" {
		return p.ParkedState
	}
	if r := state.RetryAttempts[issueID]; r != nil {
		return r.IssueIdentity.State
	}
	return ""
}

// questionNotification builds the question event t of f, which holds
// holds. closed is how it closed, empty on question.for_person.
func questionNotification(t domain.EventType, f *PlatformFault, holds []PlatformFaultHold, closed string) domain.Notification {
	var envelope domain.NotificationEnvelope
	stories := make([]string, 0, len(holds))
	for _, h := range holds {
		stories = append(stories, h.Identifier)
	}
	if len(holds) > 0 {
		envelope.IssueID, envelope.Identifier, envelope.DisplayID = holds[0].IssueID, holds[0].Identifier, holds[0].DisplayID
	}
	facts := domain.QuestionFacts{
		QuestionID: f.ID,
		Kind:       f.QuestionKind,
		RaisedBy:   f.RaisedBy,
		Stories:    stories,
		Closed:     closed,
	}
	if f.Action != nil {
		facts.Text, facts.Where = redact.Mask(f.Action.Action), redact.Mask(f.Action.Where)
	}
	return orchestratorNotification(t, "", envelope, facts)
}

// publishQuestionEvent publishes the question event t of f, which holds
// holds, when f is a question classed for a person; closed is how it
// closed, empty on question.for_person.
func (o *Orchestrator) publishQuestionEvent(ctx context.Context, t domain.EventType, f *PlatformFault, holds []PlatformFaultHold, closed string) {
	if !f.forPerson() {
		return
	}
	log := o.logger.With(slog.String("platform_fault_id", f.ID), slog.String("question_kind", f.QuestionKind))
	if o.router == nil {
		return
	}
	delivery := o.router.Route(questionNotification(t, f, holds, closed))
	if delivery.Empty() {
		log.Info("question event had no destination", slog.String("event_type", string(t)))
		return
	}
	deliverDetached(ctx, &o.state.TrackerOpsWg, delivery, log, func(received bool, err error) {
		if received && err != nil {
			log.Warn("question tracker comment failed", slog.String("event_type", string(t)), slog.Any("error", err))
		}
	})
}

// closeQuestionsOfLeftStories releases each story a question holds once
// the tracker shows it out of the state the question took it in, and
// closes a question that holds no story then. A story's state comes from
// this tick's candidates, else one batched read; a hold that knows no
// state takes the one observed. Must run on the event loop.
func (o *Orchestrator) closeQuestionsOfLeftStories(ctx context.Context, candidates []domain.Issue) {
	var questions []*PlatformFault
	for _, f := range o.state.PlatformFaults {
		if f.Class == PlatformFaultClassSupervisorQuestion && len(f.Holds) > 0 {
			questions = append(questions, f)
		}
	}
	if len(questions) == 0 {
		return
	}
	slices.SortFunc(questions, func(a, b *PlatformFault) int { return a.RecordedAt.Compare(b.RecordedAt) })

	observed := make(map[string]string, len(candidates))
	for _, issue := range candidates {
		observed[issue.ID] = issue.State
	}
	var missing []string
	for _, f := range questions {
		for _, h := range f.Holds {
			if _, ok := observed[h.IssueID]; !ok && !slices.Contains(missing, h.IssueID) {
				missing = append(missing, h.IssueID)
			}
		}
	}
	if len(missing) > 0 && o.trackerAdapter != nil {
		slices.Sort(missing)
		states, err := o.trackerAdapter.FetchIssueStatesByIDs(ctx, missing)
		if err != nil {
			o.logger.Warn("question story state read failed, keeping the questions", slog.Any("error", err))
		}
		maps.Copy(observed, states)
	}

	now := time.Now().UTC()
	changed := false
	for _, f := range questions {
		if o.observeQuestionStates(ctx, f, observed, now) {
			changed = true
		}
	}
	if changed {
		o.notifyObservers()
	}
}

// observeQuestionStates applies the observed tracker states to the
// stories f holds, and reports whether f lost one.
func (o *Orchestrator) observeQuestionStates(ctx context.Context, f *PlatformFault, observed map[string]string, now time.Time) bool {
	log := o.logger.With(slog.String("platform_fault_id", f.ID), slog.String("question_kind", f.QuestionKind))
	backfilled := false
	var kept, left []PlatformFaultHold
	for _, h := range f.Holds {
		s := observed[h.IssueID]
		switch {
		case s == "":
			kept = append(kept, h)
		case h.State == "":
			h.State, backfilled = s, true
			kept = append(kept, h)
		case strings.EqualFold(s, h.State):
			kept = append(kept, h)
		default:
			left = append(left, h)
		}
	}
	f.Holds = kept
	if len(left) == 0 {
		if backfilled {
			savePlatformFault(ctx, o.store, f, time.Time{}, log)
		}
		return false
	}
	for _, h := range left {
		if o.state.FaultHolds[h.IssueID] == f.ID {
			delete(o.state.FaultHolds, h.IssueID)
		}
		log.Info("question story left the state that raised the question, releasing it",
			slog.String("issue_identifier", h.Identifier),
			slog.String("held_state", h.State),
			slog.String("observed_state", observed[h.IssueID]),
		)
	}
	if len(kept) > 0 {
		savePlatformFault(ctx, o.store, f, time.Time{}, log)
		return true
	}
	delete(o.state.PlatformFaults, f.ID)
	f.Holds = left
	savePlatformFault(ctx, o.store, f, now, log)
	log.Info("question closed: every story it held left the state that raised it")
	o.publishQuestionEvent(ctx, domain.EventQuestionClosed, f, left, domain.QuestionClosedStateChanged)
	return true
}
