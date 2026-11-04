package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/failcause"
	"github.com/sortie-ai/sortie/internal/logging"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/redact"
)

// supervisorForgeBound bounds the tracker and forge calls of one
// supervisor action.
const supervisorForgeBound = 30 * time.Second

// SupervisorStatus is what a supervisor action did.
type SupervisorStatus int

const (
	SupervisorDone           SupervisorStatus = iota + 1 // the action is done
	SupervisorUnknownIssue                               // no issue with the identifier is known; nothing changed
	SupervisorPrecondition                               // a precondition does not hold; nothing changed
	SupervisorUnsupported                                // the tracker or forge cannot do it; nothing changed
	SupervisorUpstreamFailed                             // the tracker or forge call failed
	SupervisorUnavailable                                // the store, the loop, or an adapter is unavailable; nothing changed
)

// SupervisorResult is the answer to a supervisor action. Code names a
// [SupervisorPrecondition] that does not hold.
type SupervisorResult struct {
	Status      SupervisorStatus
	Code        string
	Message     string
	IssueID     string
	Identifier  string
	PullRequest string // owner/repo#number
	MergeCommit string
}

// Supervisor carries out the actions a supervisor may take besides
// answering a park and granting sessions. Each method is safe for
// concurrent use and touches the runtime state only on the event loop.
type Supervisor struct {
	o *Orchestrator
}

// Supervisor returns the supervisor actions of o.
func (o *Orchestrator) Supervisor() Supervisor { return Supervisor{o: o} }

// supervisedIssue is what the runtime state knows of one issue.
type supervisedIssue struct {
	issueID    string
	identifier string
	parked     bool
	running    bool
	retrying   bool
	// watch is a copy of the issue's pending reaction that records a pull
	// request, the review watch when there is one; nil when none does.
	watch *PendingReaction
	pr    pullRequestRef
}

// lookupSupervised returns what state knows of the issue named by
// identifier, and false when it knows nothing.
func lookupSupervised(state *State, identifier string) (supervisedIssue, bool) {
	var s supervisedIssue
	if id, entry, ok := findIssue(state, identifier); ok {
		s.issueID = id
		s.running = entry != nil
		_, s.retrying = state.RetryAttempts[id]
	}
	if id, _, ok := findParked(state, identifier); ok {
		s.issueID, s.parked = id, true
	}
	if s.issueID == "" {
		for id, e := range state.ParkReleases {
			if e.Identifier == identifier {
				s.issueID = id
			}
		}
	}
	if s.issueID == "" {
		for _, f := range state.PlatformFaults {
			for _, h := range f.Holds {
				if h.Identifier == identifier {
					s.issueID = h.IssueID
				}
			}
		}
	}
	keys := make([]string, 0, len(state.PendingReactions))
	for key, p := range state.PendingReactions {
		if p.Identifier == identifier && (s.issueID == "" || p.IssueID == s.issueID) {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	for _, key := range keys {
		p := state.PendingReactions[key]
		if s.issueID == "" {
			s.issueID = p.IssueID
		}
		pr := pullRequestOf(p)
		if pr.Number <= 0 || (s.watch != nil && s.watch.Kind == ReactionKindReview) {
			continue
		}
		watch := *p
		s.watch, s.pr = &watch, pr
	}
	if s.issueID == "" {
		return supervisedIssue{}, false
	}
	s.identifier = identifier
	return s, true
}

// lookupOnLoop runs lookupSupervised on the event loop.
func (s Supervisor) lookupOnLoop(ctx context.Context, identifier string) (supervisedIssue, SupervisorResult, bool) {
	var issue supervisedIssue
	var known bool
	if r, ok := s.onLoop(ctx, func(context.Context) { issue, known = lookupSupervised(s.o.state, identifier) }); !ok {
		return issue, r, false
	}
	if !known {
		return issue, unknownIssue(identifier), false
	}
	return issue, SupervisorResult{}, true
}

// onLoop runs apply on the event loop, and reports false with the result
// to answer when it did not run.
func (s Supervisor) onLoop(ctx context.Context, apply func(loopCtx context.Context)) (SupervisorResult, bool) {
	switch s.o.runOnLoop(ctx, apply) {
	case loopRan:
		return SupervisorResult{}, true
	case loopDraining:
		return SupervisorResult{Status: SupervisorUnavailable, Message: "Sortie is shutting down"}, false
	default:
		return SupervisorResult{Status: SupervisorUnavailable, Message: "the request ended before the orchestrator took it"}, false
	}
}

func unknownIssue(identifier string) SupervisorResult {
	return SupervisorResult{Status: SupervisorUnknownIssue, Message: "issue " + identifier + " is not known"}
}

func precondition(issue supervisedIssue, code, message string) SupervisorResult {
	return SupervisorResult{Status: SupervisorPrecondition, Code: code, Message: message, IssueID: issue.issueID, Identifier: issue.identifier}
}

func upstreamFailed(issue supervisedIssue, what string, err error) SupervisorResult {
	return SupervisorResult{Status: SupervisorUpstreamFailed, Message: what + ": " + err.Error(), IssueID: issue.issueID, Identifier: issue.identifier}
}

// formatPullRequest returns the owner/repo#number form of pr.
func formatPullRequest(pr pullRequestRef) string {
	return fmt.Sprintf("%s/%s#%d", pr.Owner, pr.Repo, pr.Number)
}

// parsePullRequest parses the owner/repo#number form.
func parsePullRequest(s string) (pullRequestRef, bool) {
	project, num, ok := strings.Cut(strings.TrimSpace(s), "#")
	if !ok {
		return pullRequestRef{}, false
	}
	owner, repo, ok := splitProject(project)
	n, err := strconv.Atoi(num)
	if !ok || err != nil || n <= 0 {
		return pullRequestRef{}, false
	}
	return pullRequestRef{Number: n, Owner: owner, Repo: repo}, true
}

// ReleasePark releases the park of the parked issue named by identifier
// as a person's release does, so its next run resumes with the park's
// statement and the comments made since, and queues a poll.
func (s Supervisor) ReleasePark(ctx context.Context, identifier string) SupervisorResult {
	var result SupervisorResult
	if r, ok := s.onLoop(ctx, func(loopCtx context.Context) {
		issue, known := lookupSupervised(s.o.state, identifier)
		switch {
		case !known:
			result = unknownIssue(identifier)
		case !issue.parked:
			result = precondition(issue, "issue_not_parked", "issue "+identifier+" is not parked")
		default:
			log := logging.WithIssue(s.o.logger, issue.issueID, identifier)
			unparkIssue(loopCtx, s.o.state, issue.issueID, unparkTriggerReleased, s.o.store, log)
			s.o.notifyObservers()
			result = SupervisorResult{Status: SupervisorDone, IssueID: issue.issueID, Identifier: identifier}
		}
	}); !ok {
		return r
	}
	if result.Status == SupervisorDone {
		s.o.RefreshFunc()()
	}
	return result
}

// RecordMerge records that the issue's work merged as pullRequest
// (owner/repo#number) at commit, once the forge shows that pull request
// merged at that commit. The record is the plan_satisfaction row a merge
// Sortie watched writes. A pull request other than the one the issue's
// watch records is refused.
func (s Supervisor) RecordMerge(ctx context.Context, identifier, pullRequest, commit string) SupervisorResult {
	pr, ok := parsePullRequest(pullRequest)
	commit = strings.TrimSpace(commit)
	if !ok || commit == "" {
		return SupervisorResult{Status: SupervisorPrecondition, Code: "invalid_request", Message: "pull_request must be owner/repo#number and commit must be set"}
	}
	issue, r, ok := s.lookupOnLoop(ctx, identifier)
	if !ok {
		return r
	}
	if issue.watch != nil && (issue.pr.Number != pr.Number || !strings.EqualFold(issue.pr.Owner, pr.Owner) || !strings.EqualFold(issue.pr.Repo, pr.Repo)) {
		return precondition(issue, "pull_request_mismatch", "the issue's pull request is "+formatPullRequest(issue.pr)+", not "+formatPullRequest(pr))
	}
	if s.o.scmAdapter == nil {
		return SupervisorResult{Status: SupervisorUnavailable, Message: "no forge is configured"}
	}
	recorder, ok := s.o.store.(PlanSatisfactionRecorder)
	if !ok {
		return SupervisorResult{Status: SupervisorUnavailable, Message: "merge records are not available"}
	}
	fctx, cancel := context.WithTimeout(ctx, supervisorForgeBound)
	defer cancel()
	status, err := s.o.scmAdapter.GetMergeability(fctx, pr.Number, pr.Owner, pr.Repo)
	if err != nil {
		return upstreamFailed(issue, "the forge read failed", err)
	}
	if !status.Merged {
		return precondition(issue, "not_merged", "the forge does not show "+formatPullRequest(pr)+" merged")
	}
	if !strings.EqualFold(status.MergeCommitSHA, commit) {
		return precondition(issue, "commit_mismatch", fmt.Sprintf("the forge shows %s merged at %q, not %q", formatPullRequest(pr), status.MergeCommitSHA, commit))
	}
	if err := recorder.UpsertPlanSatisfaction(ctx, persistence.PlanSatisfaction{
		IssueID: issue.issueID, Kind: persistence.PlanSatisfactionMerged,
		Ref: status.MergeCommitSHA, PR: formatPullRequest(pr), RecordedAt: time.Now(),
	}); err != nil {
		logging.WithIssue(s.o.logger, issue.issueID, identifier).Error("supervisor merge record failed", slog.Any("error", err))
		return SupervisorResult{Status: SupervisorUnavailable, Message: "the merge could not be recorded", IssueID: issue.issueID, Identifier: identifier}
	}
	logging.WithIssue(s.o.logger, issue.issueID, identifier).Info("merge recorded by the supervisor",
		slog.String("pull_request", formatPullRequest(pr)), slog.String("merge_commit", status.MergeCommitSHA))
	return SupervisorResult{
		Status: SupervisorDone, IssueID: issue.issueID, Identifier: identifier,
		PullRequest: formatPullRequest(pr), MergeCommit: status.MergeCommitSHA,
	}
}

// FixRun schedules a fix run on the issue's pull request with message as
// the one review comment it answers, as a review comment on the pull
// request does. The issue needs a watched pull request, no running worker
// and no scheduled retry, and the review reaction must be configured.
func (s Supervisor) FixRun(ctx context.Context, identifier, message string) SupervisorResult {
	message = strings.TrimSpace(message)
	if message == "" {
		return SupervisorResult{Status: SupervisorPrecondition, Code: "invalid_request", Message: "message must not be empty"}
	}
	var result SupervisorResult
	if r, ok := s.onLoop(ctx, func(loopCtx context.Context) {
		result = s.fixRunOnLoop(loopCtx, identifier, message, time.Now().UTC())
	}); !ok {
		return r
	}
	return result
}

func (s Supervisor) fixRunOnLoop(ctx context.Context, identifier, message string, now time.Time) SupervisorResult {
	issue, known := lookupSupervised(s.o.state, identifier)
	switch {
	case !known:
		return unknownIssue(identifier)
	case !s.o.reviewReactionConfigured:
		return precondition(issue, "review_reaction_off", "a fix run needs the review reaction configured")
	case issue.watch == nil:
		return precondition(issue, "no_pull_request", "Sortie watches no pull request of issue "+identifier)
	case issue.running:
		return precondition(issue, "running", "issue "+identifier+" has a running worker")
	case issue.retrying:
		return precondition(issue, "retry_scheduled", "issue "+identifier+" already has a run scheduled")
	case issue.parked:
		return precondition(issue, "issue_parked", "issue "+identifier+" is parked")
	}
	pending := s.o.state.PendingReactions[ReactionKey(issue.issueID, issue.watch.Kind)]
	rkey := ReactionKey(issue.issueID, ReactionKindReview)
	turn := s.o.state.ReactionAttempts[rkey] + 1
	log := logging.WithIssue(s.o.logger, issue.issueID, identifier)
	comment := domain.ReviewComment{
		ID:          "supervisor-" + strconv.FormatInt(now.UnixNano(), 10),
		Reviewer:    "supervisor",
		Body:        message,
		SubmittedAt: now,
	}
	scheduleReactionContinuation(ctx, s.o.state, s.o.store, ScheduleRetryParams{
		IssueID:     pending.IssueID,
		Identifier:  pending.Identifier,
		DisplayID:   pending.DisplayID,
		Attempt:     pending.Attempt,
		DelayMS:     continuationDelayMS,
		LastSSHHost: pending.LastSSHHost,
		ContinuationContext: map[string]any{
			reviewCommentsKey: buildReviewTemplateMap([]domain.ReviewComment{comment}),
		},
		ReactionKind:        ReactionKindReview,
		Continuation:        continuationTargetOf(pending, turn),
		AgentKind:           pending.AgentKind,
		ConfiguredModel:     pending.ConfiguredModel,
		RuleName:            pending.RuleName,
		StageLineage:        pending.StageLineage,
		RuleSettingsApplied: pending.RuleSettingsApplied,
		TemplateID:          pending.TemplateID,
		Logger:              log,

		IssueIdentity: pending.IssueIdentity,
	}, s.o.onRetryFire, now)
	s.o.state.ReactionAttempts[rkey] = turn
	if pending.Kind == ReactionKindReview {
		delete(s.o.state.PendingReactions, ReactionKey(issue.issueID, ReactionKindReview))
	}
	log.Info("fix run scheduled by the supervisor", slog.String("pull_request", formatPullRequest(issue.pr)))
	s.o.notifyObservers()
	return SupervisorResult{Status: SupervisorDone, IssueID: issue.issueID, Identifier: identifier, PullRequest: formatPullRequest(issue.pr)}
}

// MarkDraft makes the issue's watched pull request a draft, which keeps
// auto-merge from merging it.
func (s Supervisor) MarkDraft(ctx context.Context, identifier string) SupervisorResult {
	issue, r, ok := s.lookupOnLoop(ctx, identifier)
	if !ok {
		return r
	}
	if issue.watch == nil {
		return precondition(issue, "no_pull_request", "Sortie watches no pull request of issue "+identifier)
	}
	drafter := s.o.prDrafter
	if drafter == nil || s.o.scmAdapter == nil {
		return SupervisorResult{Status: SupervisorUnsupported, Message: "the forge adapter cannot mark a pull request a draft", IssueID: issue.issueID, Identifier: identifier}
	}
	fctx, cancel := context.WithTimeout(ctx, supervisorForgeBound)
	defer cancel()
	status, err := s.o.scmAdapter.GetMergeability(fctx, issue.pr.Number, issue.pr.Owner, issue.pr.Repo)
	if err != nil {
		return upstreamFailed(issue, "the forge read failed", err)
	}
	if status.Merged || status.Closed {
		return precondition(issue, "pull_request_closed", formatPullRequest(issue.pr)+" is no longer open")
	}
	if err := drafter.MarkPullRequestDraft(fctx, issue.pr.Number, issue.pr.Owner, issue.pr.Repo); err != nil {
		return upstreamFailed(issue, "the forge refused the draft", err)
	}
	logging.WithIssue(s.o.logger, issue.issueID, identifier).Info("pull request marked a draft by the supervisor",
		slog.String("pull_request", formatPullRequest(issue.pr)))
	return SupervisorResult{Status: SupervisorDone, IssueID: issue.issueID, Identifier: identifier, PullRequest: formatPullRequest(issue.pr)}
}

// Merge merges the issue's watched pull request when it is open, not a
// draft, mergeable, its review decision is approved or requires no
// review, its CI conclusion is success, the story is not a ui story,
// whose merge the operator keeps, and its criteria verdict lets it merge
// ([Supervisor.criteriaLetMerge]). The merge records the plan merge as
// [Supervisor.RecordMerge] does.
func (s Supervisor) Merge(ctx context.Context, identifier string) SupervisorResult {
	issue, r, ok := s.lookupOnLoop(ctx, identifier)
	if !ok {
		return r
	}
	if issue.watch == nil {
		return precondition(issue, "no_pull_request", "Sortie watches no pull request of issue "+identifier)
	}
	if s.o.scmAdapter == nil || s.o.trackerAdapter == nil {
		return SupervisorResult{Status: SupervisorUnavailable, Message: "no forge or tracker is configured"}
	}
	fctx, cancel := context.WithTimeout(ctx, supervisorForgeBound)
	defer cancel()
	pr := issue.pr
	tracked, err := s.o.trackerAdapter.FetchIssueByID(fctx, issue.issueID)
	if err != nil {
		return upstreamFailed(issue, "the tracker read failed", err)
	}
	if slices.ContainsFunc(tracked.Labels, isUIReviewLabel) {
		return precondition(issue, "ui_story", "a ui story waits for the operator, who merges it")
	}
	status, err := s.o.scmAdapter.GetMergeability(fctx, pr.Number, pr.Owner, pr.Repo)
	if err != nil {
		return upstreamFailed(issue, "the forge read failed", err)
	}
	switch {
	case status.Merged:
		return precondition(issue, "already_merged", formatPullRequest(pr)+" is already merged")
	case status.Closed:
		return precondition(issue, "pull_request_closed", formatPullRequest(pr)+" is closed")
	case status.Draft:
		return precondition(issue, "draft", formatPullRequest(pr)+" is a draft")
	case status.Mergeability != domain.MergeabilityClean && status.Mergeability != domain.MergeabilityUnstable:
		return precondition(issue, "not_mergeable", fmt.Sprintf("%s is not mergeable: %s", formatPullRequest(pr), status.Mergeability))
	case status.HeadSHA == "":
		return precondition(issue, "not_mergeable", "the forge reports no head commit for "+formatPullRequest(pr))
	}
	decision, err := s.o.scmAdapter.GetReviewDecision(fctx, pr.Number, pr.Owner, pr.Repo)
	if err != nil {
		return upstreamFailed(issue, "the review read failed", err)
	}
	if decision != domain.ReviewDecisionApproved && decision != domain.ReviewDecisionNotRequired {
		return precondition(issue, "review_not_passed", fmt.Sprintf("the review decision of %s is %s", formatPullRequest(pr), decision))
	}
	ci, err := s.o.scmAdapter.GetCIStatus(fctx, pr.Number, pr.Owner, pr.Repo)
	if err != nil {
		return upstreamFailed(issue, "the CI read failed", err)
	}
	if ci != "success" {
		if ci == "" {
			ci = "no checks"
		}
		return precondition(issue, "ci_not_passed", fmt.Sprintf("the CI of %s is %s", formatPullRequest(pr), ci))
	}
	if r, ok := s.criteriaLetMerge(fctx, issue, status.HeadSHA); !ok {
		return r
	}
	strategy := s.o.autoMergeConfig.Strategy
	if strategy == "" {
		strategy = domain.StrategySquash
	}
	merged, err := s.o.scmAdapter.MergePR(fctx, pr.Number, pr.Owner, pr.Repo, strategy, "", "", status.HeadSHA)
	if err != nil {
		var scmErr *domain.SCMError
		if errors.As(err, &scmErr) && scmErr.Kind == domain.ErrSCMConflict {
			return precondition(issue, "merge_refused", "the forge refused the merge: "+scmErr.Message)
		}
		return upstreamFailed(issue, "the merge failed", err)
	}
	log := logging.WithIssue(s.o.logger, issue.issueID, identifier)
	log.Info("pull request merged by the supervisor",
		slog.String("pull_request", formatPullRequest(pr)), slog.String("merge_commit", merged.SHA))
	if merged.SHA != "" {
		if err := recordPlanMerge(s.o.store, issue.issueID, merged.SHA, &MergeCompletionReactionData{PRNumber: pr.Number, Owner: pr.Owner, Repo: pr.Repo}, ctx); err != nil {
			log.Error("plan merge record failed after the supervisor's merge", slog.Any("error", err))
		}
	}
	return SupervisorResult{
		Status: SupervisorDone, IssueID: issue.issueID, Identifier: identifier,
		PullRequest: formatPullRequest(pr), MergeCommit: merged.SHA,
	}
}

// FileIssue creates an open story in the tracker's project, in
// tracker.backlog_state when it is set, so it waits for a person and is
// not dispatched. Its plan key starts with "supervisor-".
func (s Supervisor) FileIssue(ctx context.Context, title, body string) SupervisorResult {
	title = strings.TrimSpace(title)
	if title == "" {
		return SupervisorResult{Status: SupervisorPrecondition, Code: "invalid_request", Message: "title must not be empty"}
	}
	creator, ok := s.o.trackerAdapter.(domain.IssueCreator)
	if !ok {
		return SupervisorResult{Status: SupervisorUnsupported, Message: "the tracker adapter cannot create issues"}
	}
	fctx, cancel := context.WithTimeout(ctx, supervisorForgeBound)
	defer cancel()
	created, err := creator.CreateIssue(fctx, domain.NewIssue{
		Title:   title,
		Body:    body,
		Kind:    domain.IssueKindStory,
		PlanKey: "supervisor-" + strconv.FormatInt(time.Now().UnixNano(), 36),
		State:   s.o.workflowManager.Config().Tracker.BacklogState,
	})
	if err != nil {
		return SupervisorResult{Status: SupervisorUpstreamFailed, Message: "the tracker refused the issue: " + err.Error()}
	}
	logging.WithIssue(s.o.logger, created.ID, created.Identifier).Info("issue filed by the supervisor")
	return SupervisorResult{Status: SupervisorDone, IssueID: created.ID, Identifier: created.Identifier}
}

// criteriaLetMerge reports whether the story's latest criteria verdict
// lets the supervisor merge its pull request at head, and the refusal
// when it does not. A story no criteria review judged is not held. A
// verdict that every criterion is met lets it merge; one that leaves
// criteria to a live proof lets it merge only once a pull request
// comment records them proven at head ([LiveProofMarker]); any other
// verdict never does.
func (s Supervisor) criteriaLetMerge(ctx context.Context, issue supervisedIssue, head string) (SupervisorResult, bool) {
	loader, ok := s.o.store.(CriteriaVerdictLoader)
	if !ok {
		return SupervisorResult{}, true
	}
	v, found, err := loader.LoadCriteriaVerdict(ctx, issue.issueID)
	switch {
	case err != nil:
		return upstreamFailed(issue, "the criteria verdict read failed", err), false
	case !found || v.Outcome == CriteriaOutcomeMet:
		return SupervisorResult{}, true
	case v.Outcome != CriteriaOutcomeLiveProof:
		return precondition(issue, "criteria_not_met", "the story's review did not find every criterion met ("+v.Outcome+"): decide how each is met, then release it for a new review"), false
	}
	pr := issue.pr
	unproven := precondition(issue, "live_proof_unproven", fmt.Sprintf(
		"the story's review left criteria to a live proof, and no comment on %s records them proven at its head %s (%s %s): request the live proof", formatPullRequest(pr), head, LiveProofMarker, head))
	if s.o.prCommentReader == nil {
		return unproven, false
	}
	comments, err := s.o.prCommentReader.FetchPullRequestComments(ctx, pr.Number, pr.Owner, pr.Repo)
	if err != nil {
		return upstreamFailed(issue, "the pull request comments read failed", err), false
	}
	h := strings.ToLower(head)
	for _, c := range comments {
		if sha, ok := liveProofRecord(c.Body); ok && strings.HasPrefix(h, sha) {
			return SupervisorResult{}, true
		}
	}
	return unproven, false
}

// RequestLiveProof checks that the issue named by identifier is known.
// Sortie runs no live proof itself: the request is the supervisor's
// recorded action, which a person or the workbench acts on.
func (s Supervisor) RequestLiveProof(ctx context.Context, identifier string) SupervisorResult {
	issue, r, ok := s.lookupOnLoop(ctx, identifier)
	if !ok {
		return r
	}
	logging.WithIssue(s.o.logger, issue.issueID, identifier).Info("live proof requested by the supervisor")
	res := SupervisorResult{Status: SupervisorDone, IssueID: issue.issueID, Identifier: identifier}
	if issue.watch != nil {
		res.PullRequest = formatPullRequest(issue.pr)
	}
	return res
}

// PlatformFaultClassSupervisorQuestion is the class of the platform fault
// that carries a question the supervisor raised for a person.
const PlatformFaultClassSupervisorQuestion = "supervisor_question"

// supervisorQuestionStep is the step of a supervisor question's fault.
const supervisorQuestionStep = "supervisor"

// SupervisorQuestion is a question the supervisor raises for a person:
// the exact action, where to take it, the stories it holds, and what the
// supervisor already checked. Cause says why the action is needed; it may
// be empty.
type SupervisorQuestion struct {
	Action  string
	Where   string
	Stories []string
	Checked string
	Cause   string
}

// SupervisorQuestionResult is the answer to [Supervisor.AskPerson]. Fault
// is the platform fault that carries the question; Existing reports that
// an open question named the same action and place already, so the
// stories joined it instead of a second question.
type SupervisorQuestionResult struct {
	SupervisorResult
	Fault    SnapshotPlatformFault
	Existing bool
}

// questionSignature is the signature that makes one question of one
// decision: its action and place, case and spacing folded.
func questionSignature(action, where string) string {
	fold := func(s string) string { return strings.ToLower(strings.Join(strings.Fields(s), " ")) }
	return PlatformFaultClassSupervisorQuestion + ": " + fold(action) + " @ " + fold(where)
}

// AskPerson raises q for a person through the person-action path: a
// platform fault whose person action is q's action and place, whose
// detail is what the supervisor checked, and which holds q's stories out
// of dispatch until a person marks the action done and its check passes.
// A question with the action and place of an open one adds its stories to
// that one. A story with a running worker is refused, since the hold
// would take its claim.
func (s Supervisor) AskPerson(ctx context.Context, q SupervisorQuestion) SupervisorQuestionResult {
	q.Action, q.Where, q.Checked, q.Cause = strings.TrimSpace(q.Action), strings.TrimSpace(q.Where), strings.TrimSpace(q.Checked), strings.TrimSpace(q.Cause)
	stories := make([]string, 0, len(q.Stories))
	for _, id := range q.Stories {
		if id = strings.TrimSpace(id); id != "" && !slices.Contains(stories, id) {
			stories = append(stories, id)
		}
	}
	if q.Action == "" || q.Where == "" || q.Checked == "" || len(stories) == 0 {
		return SupervisorQuestionResult{SupervisorResult: SupervisorResult{
			Status: SupervisorPrecondition, Code: "invalid_request",
			Message: "a question names the action, where to take it, the stories it holds, and what the supervisor checked",
		}}
	}
	var result SupervisorQuestionResult
	if r, ok := s.onLoop(ctx, func(loopCtx context.Context) {
		result = s.askOnLoop(loopCtx, q, stories, time.Now().UTC())
	}); !ok {
		return SupervisorQuestionResult{SupervisorResult: r}
	}
	return result
}

func (s Supervisor) askOnLoop(ctx context.Context, q SupervisorQuestion, stories []string, now time.Time) SupervisorQuestionResult {
	state := s.o.state
	issues := make([]supervisedIssue, 0, len(stories))
	for _, identifier := range stories {
		issue, known := lookupSupervised(state, identifier)
		switch {
		case !known:
			return SupervisorQuestionResult{SupervisorResult: unknownIssue(identifier)}
		case issue.running:
			return SupervisorQuestionResult{SupervisorResult: precondition(issue, "running", "issue "+identifier+" has a running worker")}
		}
		issues = append(issues, issue)
	}
	signature := questionSignature(q.Action, q.Where)
	var fault *PlatformFault
	for _, f := range state.PlatformFaults {
		if f.Signature == signature {
			fault = f
		}
	}
	existing := fault != nil
	if !existing {
		cause := q.Cause
		if cause == "" {
			cause = "The supervisor asks a person to act."
		}
		fault = &PlatformFault{
			ID:          platformFaultID(signature, now),
			Signature:   signature,
			Class:       PlatformFaultClassSupervisorQuestion,
			Step:        supervisorQuestionStep,
			Cause:       redact.Mask(cause),
			Detail:      redact.Mask(q.Checked),
			Action:      &failcause.PersonAction{Action: q.Action, Where: q.Where},
			RecordedAt:  now,
			NextCheckAt: now.Add(PlatformFaultCheckInterval),
		}
		state.PlatformFaults[fault.ID] = fault
	}
	log := s.o.logger.With(slog.String("platform_fault_id", fault.ID))
	for _, issue := range issues {
		holdUnderPlatformFault(ctx, state, s.o.store, fault, PlatformFaultHold{
			IssueID: issue.issueID, Identifier: issue.identifier, HeldAt: now,
		}, logging.WithIssue(log, issue.issueID, issue.identifier))
	}
	log.Info("supervisor raised a question for a person",
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
