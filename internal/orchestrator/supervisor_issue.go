package orchestrator

import (
	"context"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/logging"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/redact"
)

// IssueKinds are the kinds of issue the supervisor files: a defect of the
// platform, or follow-up work of stories.
var IssueKinds = []string{"platform_defect", "follow_up"}

// SupervisorIssue is an issue the supervisor files: its title and body,
// its kind, and the stories or the fault signature it concerns. Evidence
// is what the filing is based on; it may be empty.
type SupervisorIssue struct {
	Title     string
	Body      string
	Kind      string
	Stories   []string
	Signature string
	Evidence  string
}

// SupervisorIssueResult is the answer to [Supervisor.FileIssue]. Existing
// reports that an open issue matched, so the filing joined it as a comment.
type SupervisorIssueResult struct {
	SupervisorResult
	Existing bool
}

// supervisorIssueStore keeps the issues the supervisor filed.
// [persistence.Store] implements it.
type supervisorIssueStore interface {
	SaveSupervisorIssue(ctx context.Context, issue persistence.SupervisorIssue) error
	SupervisorIssues(ctx context.Context) ([]persistence.SupervisorIssue, error)
}

var _ supervisorIssueStore = (*persistence.Store)(nil)

// normalizeIssue trims issue, sorts its stories dropping repeats, folds
// its signature, and reports the refusal of an issue that lacks a member.
func normalizeIssue(issue SupervisorIssue) (SupervisorIssue, SupervisorResult, bool) {
	issue.Title, issue.Kind, issue.Evidence = strings.TrimSpace(issue.Title), strings.TrimSpace(issue.Kind), strings.TrimSpace(issue.Evidence)
	issue.Signature = foldText(issue.Signature)
	stories := make([]string, 0, len(issue.Stories))
	for _, id := range issue.Stories {
		if id = strings.TrimSpace(id); id != "" && !slices.Contains(stories, id) {
			stories = append(stories, id)
		}
	}
	slices.Sort(stories)
	issue.Stories = stories
	refuse := func(message string) (SupervisorIssue, SupervisorResult, bool) {
		return issue, SupervisorResult{Status: SupervisorPrecondition, Code: "invalid_request", Message: message}, false
	}
	switch {
	case issue.Title == "":
		return refuse("title must not be empty")
	case !slices.Contains(IssueKinds, issue.Kind):
		return refuse("an issue's kind is one of " + strings.Join(IssueKinds, ", "))
	case len(stories) == 0 && issue.Signature == "":
		return refuse("an issue names the stories or the fault signature it concerns")
	}
	return issue, SupervisorResult{}, true
}

// sameIssue reports whether the filed issue rec is what issue files again:
// the same stories and kind, or the same fault signature.
func sameIssue(rec persistence.SupervisorIssue, issue SupervisorIssue) bool {
	if issue.Signature != "" && rec.Signature == issue.Signature {
		return true
	}
	return len(issue.Stories) > 0 && rec.Kind == issue.Kind && slices.Equal(rec.Stories, issue.Stories)
}

// withEvidence returns text followed by the masked evidence, when there
// is any.
func withEvidence(text, evidence string) string {
	if evidence == "" {
		return text
	}
	return strings.TrimRight(text, "\n") + "\n\nEvidence: " + redact.Mask(evidence)
}

// FileIssue files issue in the tracker, unless an open issue the
// supervisor filed has the same stories and kind, or the same fault
// signature, whatever its title: then it joins that issue as a comment
// that carries its title, body and evidence. A new issue is an open story
// in the tracker's project, in tracker.backlog_state when it is set, so it
// waits for a person and is not dispatched; its plan key starts with
// "supervisor-". A filed issue is open while the tracker returns it in a
// state that is not terminal.
func (s Supervisor) FileIssue(ctx context.Context, issue SupervisorIssue) SupervisorIssueResult {
	issue, refusal, ok := normalizeIssue(issue)
	if !ok {
		return SupervisorIssueResult{SupervisorResult: refusal}
	}
	s.o.supervisorIssueMu.Lock()
	defer s.o.supervisorIssueMu.Unlock()
	fctx, cancel := context.WithTimeout(ctx, supervisorForgeBound)
	defer cancel()

	store, _ := s.o.store.(supervisorIssueStore)
	if store != nil {
		open, r, ok := s.openFiledIssue(fctx, store, issue)
		if !ok {
			return SupervisorIssueResult{SupervisorResult: r}
		}
		if open != nil {
			return s.joinIssue(fctx, *open, issue)
		}
	}

	creator, ok := s.o.trackerAdapter.(domain.IssueCreator)
	if !ok {
		return SupervisorIssueResult{SupervisorResult: SupervisorResult{Status: SupervisorUnsupported, Message: "the tracker adapter cannot create issues"}}
	}
	now := time.Now()
	created, err := creator.CreateIssue(fctx, domain.NewIssue{
		Title:   issue.Title,
		Body:    withEvidence(issue.Body, issue.Evidence),
		Kind:    domain.IssueKindStory,
		PlanKey: "supervisor-" + strconv.FormatInt(now.UnixNano(), 36),
		State:   s.o.workflowManager.Config().Tracker.BacklogState,
	})
	if err != nil {
		return SupervisorIssueResult{SupervisorResult: SupervisorResult{Status: SupervisorUpstreamFailed, Message: "the tracker refused the issue: " + err.Error()}}
	}
	log := logging.WithIssue(s.o.logger, created.ID, created.Identifier).With(slog.String("issue_kind", issue.Kind))
	if store != nil {
		if err := store.SaveSupervisorIssue(context.WithoutCancel(ctx), persistence.SupervisorIssue{
			IssueID: created.ID, Identifier: created.Identifier, Kind: issue.Kind,
			Stories: issue.Stories, Signature: issue.Signature, FiledAt: now,
		}); err != nil {
			log.Error("supervisor issue record failed: a later filing of it opens another", slog.Any("error", err))
		}
	}
	log.Info("issue filed by the supervisor")
	return SupervisorIssueResult{SupervisorResult: SupervisorResult{Status: SupervisorDone, IssueID: created.ID, Identifier: created.Identifier}}
}

// openFiledIssue returns the newest open issue the supervisor filed that
// issue files again; nil when there is none.
func (s Supervisor) openFiledIssue(ctx context.Context, store supervisorIssueStore, issue SupervisorIssue) (*persistence.SupervisorIssue, SupervisorResult, bool) {
	filed, err := store.SupervisorIssues(ctx)
	if err != nil {
		return nil, SupervisorResult{Status: SupervisorUnavailable, Message: "the record of filed issues could not be read: " + err.Error()}, false
	}
	var candidates []persistence.SupervisorIssue
	for _, rec := range filed {
		if sameIssue(rec, issue) {
			candidates = append(candidates, rec)
		}
	}
	if len(candidates) == 0 {
		return nil, SupervisorResult{}, true
	}
	ids := make([]string, 0, len(candidates))
	for _, rec := range candidates {
		ids = append(ids, rec.IssueID)
	}
	states, err := s.o.trackerAdapter.FetchIssueStatesByIDs(ctx, ids)
	if err != nil {
		return nil, SupervisorResult{Status: SupervisorUpstreamFailed, Message: "the tracker read of the filed issues failed: " + err.Error()}, false
	}
	tracker := s.o.workflowManager.Config().Tracker
	closed := slices.Clone(tracker.TerminalStates)
	for _, st := range []string{tracker.DoneState, tracker.CancelState} {
		if st != "" {
			closed = append(closed, st)
		}
	}
	for _, rec := range candidates {
		if st, ok := states[rec.IssueID]; ok && !isTerminalState(st, closed) {
			return &rec, SupervisorResult{}, true
		}
	}
	return nil, SupervisorResult{}, true
}

// joinIssue adds issue to the open filed issue rec as a comment.
func (s Supervisor) joinIssue(ctx context.Context, rec persistence.SupervisorIssue, issue SupervisorIssue) SupervisorIssueResult {
	comment := "The supervisor filed this again as \"" + issue.Title + "\"."
	if body := strings.TrimSpace(issue.Body); body != "" {
		comment += "\n\n" + body
	}
	comment = withEvidence(comment, issue.Evidence)
	if err := s.o.trackerAdapter.CommentIssue(ctx, rec.IssueID, comment); err != nil {
		return SupervisorIssueResult{SupervisorResult: SupervisorResult{
			Status: SupervisorUpstreamFailed, IssueID: rec.IssueID, Identifier: rec.Identifier,
			Message: "the tracker refused the comment on " + rec.Identifier + ": " + err.Error(),
		}}
	}
	logging.WithIssue(s.o.logger, rec.IssueID, rec.Identifier).Info("supervisor filing joined an open issue",
		slog.String("issue_kind", issue.Kind))
	return SupervisorIssueResult{
		SupervisorResult: SupervisorResult{Status: SupervisorDone, IssueID: rec.IssueID, Identifier: rec.Identifier},
		Existing:         true,
	}
}
