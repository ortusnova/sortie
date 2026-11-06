package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/logging"
)

// criteriaReviewKind keys, among the reaction fingerprints, the head of
// the pull request whose fresh criteria review Sortie last requested for
// a story.
const criteriaReviewKind = "criteria-review"

// criteriaReviewStore keeps the head each story's fresh criteria review
// was last requested at.
type criteriaReviewStore interface {
	GetReactionFingerprint(ctx context.Context, issueID, kind string) (fingerprint string, dispatched bool, err error)
	UpsertReactionFingerprint(ctx context.Context, issueID, kind, fingerprint string) error
	DeleteReactionFingerprint(ctx context.Context, issueID, kind string) error
}

// RequestReview requests a fresh criteria review of the current head of
// the pull request of the story named by identifier
// ([Supervisor.pullRequestOf]), as [Supervisor.requestCriteriaReview]
// does. A review of the story that runs, is scheduled, or waits for its
// dispatch at that head is review_running; a parked story is released
// for its review by unpark instead.
func (s Supervisor) RequestReview(ctx context.Context, identifier string) SupervisorResult {
	issue, r, ok := s.lookupOnLoop(ctx, identifier)
	if !ok {
		return r
	}
	if issue.parked {
		return precondition(issue, "issue_parked", "issue "+identifier+" is parked: release it for a new review with POST /api/v1/"+identifier+"/unpark")
	}
	if r, ok := s.reviewRunAllowed(ctx, issue); !ok {
		return r
	}
	if s.o.scmAdapter == nil || s.o.trackerAdapter == nil {
		return SupervisorResult{Status: SupervisorUnavailable, Message: "no forge or tracker is configured"}
	}
	fctx, cancel := context.WithTimeout(ctx, supervisorForgeBound)
	defer cancel()
	pr, r, ok := s.pullRequestOf(fctx, issue)
	if !ok {
		return r
	}
	status, err := s.o.scmAdapter.GetMergeability(fctx, pr.Number, pr.Owner, pr.Repo)
	switch {
	case err != nil:
		return upstreamFailed(issue, "the forge read failed", err)
	case status.Merged || status.Closed:
		return precondition(issue, "pull_request_closed", formatPullRequest(pr)+" is no longer open")
	case status.HeadSHA == "":
		return precondition(issue, "no_head", "the forge reports no head commit for "+formatPullRequest(pr))
	}
	if _, r, ok := s.requestCriteriaReview(ctx, fctx, issue, pr, status.HeadSHA, false, "the supervisor requested it"); !ok {
		return r
	}
	return SupervisorResult{Status: SupervisorDone, IssueID: issue.issueID, Identifier: identifier, PullRequest: formatPullRequest(pr)}
}

// reviewRunAllowed reports whether no run of issue is going or scheduled,
// and the refusal when one is: review_running when it is a criteria
// review of the story.
func (s Supervisor) reviewRunAllowed(ctx context.Context, issue supervisedIssue) (SupervisorResult, bool) {
	if !issue.running && !issue.retrying {
		return SupervisorResult{}, true
	}
	review := false
	if r, ok := s.onLoop(ctx, func(context.Context) {
		dispatch := s.o.workflowManager.Config().Dispatch
		if e := s.o.state.Running[issue.issueID]; e != nil {
			review = e.ReactionKind == "" && isGateRule(dispatch, e.RuleName)
		} else if e := s.o.state.RetryAttempts[issue.issueID]; e != nil {
			review = e.ReactionKind == "" && isGateRule(dispatch, e.RuleName)
		}
	}); !ok {
		return r, false
	}
	if review {
		return precondition(issue, "review_running", "a criteria review of issue "+issue.identifier+" is running"), false
	}
	return fixRunAllowed(issue)
}

func isGateRule(dispatch config.DispatchConfig, name string) bool {
	rule, ok := dispatch.RuleByName(name)
	return ok && rule.Gate == config.GateCriteria
}

// requestCriteriaReview requests a fresh criteria review of pr at head:
// the story gets the stage label of its criteria rule and moves to
// tracker.release_state, so the next poll dispatches that rule on its
// branch, as a failed check's on_fail returns a story to its rework
// stage, and Sortie comments why on the story. The head is kept, so with
// once a review already requested at head is not requested again and
// requested is false. Without once, a review requested at head that still
// waits for its dispatch is review_running.
func (s Supervisor) requestCriteriaReview(ctx, fctx context.Context, issue supervisedIssue, pr pullRequestRef, head string, once bool, why string) (requested bool, result SupervisorResult, ok bool) {
	cfg := s.o.workflowManager.Config()
	if !cfg.Tracker.PlanControl() {
		return false, precondition(issue, "plan_control_off", "a fresh criteria review returns the story to tracker.release_state, which the workflow does not set"), false
	}
	store, isStore := s.o.store.(criteriaReviewStore)
	if !isStore {
		return false, SupervisorResult{Status: SupervisorUnavailable, Message: "review requests are not available"}, false
	}
	tracked, err := s.o.trackerAdapter.FetchIssueByID(fctx, issue.issueID)
	if err != nil {
		return false, upstreamFailed(issue, "the tracker read failed", err), false
	}
	current := ""
	if issue.watch != nil {
		current = issue.watch.RuleName
	}
	rule, found := cfg.Dispatch.RuleByName(criteriaRuleName(cfg.Dispatch, current))
	if !found || rule.Gate != config.GateCriteria {
		return false, precondition(issue, "no_review_rule", "the workflow has no rule with gate: criteria to review issue "+issue.identifier), false
	}
	routed := tracked
	routed.Labels = append([]string(nil), tracked.Labels...)
	if rule.Stage != "" && !carriesLabel(routed.Labels, rule.Stage) {
		routed.Labels = append(routed.Labels, rule.Stage)
	}
	if got := ResolveRule(routed, cfg.Dispatch, HopRoute{}, cfg.Agent.Kind, "").RuleName; got != rule.Name {
		return false, precondition(issue, "no_review_rule", fmt.Sprintf("issue %s would dispatch rule %q, not its criteria rule %q, which carries no stage label", issue.identifier, got, rule.Name)), false
	}
	waiting := strings.EqualFold(tracked.State, cfg.Tracker.ReleaseState)
	var prev string
	var refusal SupervisorResult
	already := false
	if r, ran := s.onLoop(ctx, func(loopCtx context.Context) {
		stored, _, err := store.GetReactionFingerprint(loopCtx, issue.issueID, criteriaReviewKind)
		switch {
		case err != nil:
			refusal = SupervisorResult{Status: SupervisorUnavailable, Message: "the review request read failed: " + err.Error(), IssueID: issue.issueID, Identifier: issue.identifier}
			return
		case strings.EqualFold(stored, head) && once:
			already = true
			return
		case strings.EqualFold(stored, head) && waiting:
			refusal = precondition(issue, "review_running", fmt.Sprintf("a criteria review of issue %s at %s waits for its dispatch in %s", issue.identifier, head, tracked.State))
			return
		}
		prev = stored
		if err := store.UpsertReactionFingerprint(loopCtx, issue.issueID, criteriaReviewKind, head); err != nil {
			refusal = SupervisorResult{Status: SupervisorUnavailable, Message: "the review request could not be recorded: " + err.Error(), IssueID: issue.issueID, Identifier: issue.identifier}
		}
	}); !ran {
		return false, r, false
	}
	switch {
	case refusal.Status != 0:
		return false, refusal, false
	case already:
		return false, SupervisorResult{}, true
	}
	log := logging.WithIssue(s.o.logger, issue.issueID, issue.identifier)
	if err := s.returnForReview(fctx, tracked, rule, cfg.Tracker.ReleaseState); err != nil {
		s.forgetReviewRequest(ctx, store, issue.issueID, prev, log)
		return false, upstreamFailed(issue, "the tracker refused the review's move", err), false
	}
	s.commentReview(fctx, issue.issueID, fmt.Sprintf("Sortie requested a fresh criteria review of %s at its head %s (rule %s), because %s. The story is back in %s for it.",
		formatPullRequest(pr), head, rule.Name, why, cfg.Tracker.ReleaseState), log)
	log.Info("fresh criteria review requested",
		slog.String("pull_request", formatPullRequest(pr)), slog.String("head_sha", head),
		slog.String("rule_name", rule.Name), slog.String("reason", why))
	s.o.notifyObservers()
	s.o.RefreshFunc()()
	return true, SupervisorResult{}, true
}

// returnForReview gives the story the stage label of rule and moves it to
// releaseState, each unless it already holds.
func (s Supervisor) returnForReview(ctx context.Context, tracked domain.Issue, rule config.DispatchRule, releaseState string) error {
	if rule.Stage != "" && !carriesLabel(tracked.Labels, rule.Stage) {
		if err := s.o.trackerAdapter.AddLabel(ctx, tracked.ID, rule.Stage); err != nil {
			return fmt.Errorf("add label %s: %w", rule.Stage, err)
		}
	}
	if !strings.EqualFold(tracked.State, releaseState) {
		if err := s.o.trackerAdapter.TransitionIssue(ctx, tracked.ID, releaseState); err != nil {
			return fmt.Errorf("move to %s: %w", releaseState, err)
		}
	}
	return nil
}

// forgetReviewRequest restores the head kept before a request that did
// not happen.
func (s Supervisor) forgetReviewRequest(ctx context.Context, store criteriaReviewStore, issueID, prev string, log *slog.Logger) {
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), planControlWriteBound)
	defer cancel()
	var err error
	if prev == "" {
		err = store.DeleteReactionFingerprint(wctx, issueID, criteriaReviewKind)
	} else {
		err = store.UpsertReactionFingerprint(wctx, issueID, criteriaReviewKind, prev)
	}
	if err != nil {
		log.Warn("review request record not restored", slog.Any("error", err))
	}
}

func (s Supervisor) commentReview(ctx context.Context, issueID, body string, log *slog.Logger) {
	wctx, cancel := context.WithTimeout(ctx, planControlWriteBound)
	defer cancel()
	if err := s.o.trackerAdapter.CommentIssue(wctx, issueID, body); err != nil {
		log.Warn("review request comment failed", slog.Any("error", err))
	}
}
