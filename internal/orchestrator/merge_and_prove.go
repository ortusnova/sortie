package orchestrator

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/deploystate"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/logging"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// Phases of a story in merge-and-prove.
const (
	// MergeAndProveQueued: the story waits for the one in flight.
	MergeAndProveQueued = "queued"
	// MergeAndProveMerging: Sortie merges its pull request.
	MergeAndProveMerging = "merging"
	// MergeAndProveDeploying: Sortie waits for every host to deploy a
	// commit that holds the merge commit, until the deadline.
	MergeAndProveDeploying = "deploying"
	// MergeAndProveProving: the live-proof job runs against the deployed
	// main, and Sortie waits for its record until the deadline.
	MergeAndProveProving = "proving"
	// MergeAndProveReverting: Sortie opened a revert of the merge commit
	// and merges it once its CI passes.
	MergeAndProveReverting = "reverting"
)

// LiveProofUnprovenMarker opens the line of a pull request comment that
// records a story's criteria not proven by its live-proof job after the
// deploy, naming the merge commit; the comment says what failed:
//
//	sortie:live-proof unproven <merge sha>
const LiveProofUnprovenMarker = "sortie:live-proof unproven"

// mergeAndProveBound bounds the tracker and forge calls of one step of
// merge-and-prove on the event loop.
const mergeAndProveBound = 20 * time.Second

// maxMergeAndProveReason caps the reason a revert carries.
const maxMergeAndProveReason = 1000

// DeployedHost is the commit one host deployed.
type DeployedHost struct {
	Host   string `json:"host"`
	Commit string `json:"commit"`
}

// MergeAndProveEntry is one story in merge-and-prove: queued, or in flight
// with its phase.
type MergeAndProveEntry struct {
	IssueID     string
	Identifier  string
	Seq         int64
	Phase       string
	PullRequest pullRequestRef
	Head        string // the head whose CI passed when the story was accepted
	Criteria    string // the criteria left to the proof after deploy
	MergeCommit string
	Deployed    []DeployedHost
	QueuedAt    time.Time
	PhaseAt     time.Time
	Deadline    time.Time // zero in the queue and while merging
	RevertPR    int
	Reason      string

	AgentKind       string
	ConfiguredModel string
	RuleName        string
	TemplateID      string

	// Waiting says what the phase waits on now. Runtime-only.
	Waiting string
	// jobScheduled reports that this process scheduled the live-proof
	// job of the proving phase.
	jobScheduled bool
	// contains caches whether a commit holds the merge commit.
	contains map[string]bool
}

// mergeAndProveStore persists merge-and-prove.
type mergeAndProveStore interface {
	SaveMergeAndProve(ctx context.Context, r persistence.MergeAndProve) error
	DeleteMergeAndProve(ctx context.Context, issueID string) error
}

// planSatisfactionDeleter removes a plan_satisfaction row.
type planSatisfactionDeleter interface {
	DeletePlanSatisfaction(ctx context.Context, issueID, kind string) error
}

func formatOptionalTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func mergeAndProveRow(e *MergeAndProveEntry) persistence.MergeAndProve {
	deployed := make([]string, 0, len(e.Deployed))
	for _, d := range e.Deployed {
		deployed = append(deployed, d.Host+"="+d.Commit)
	}
	return persistence.MergeAndProve{
		IssueID: e.IssueID, Identifier: e.Identifier, Seq: e.Seq, Phase: e.Phase,
		PullRequest: formatPullRequest(e.PullRequest), Branch: e.PullRequest.Branch, Head: e.Head,
		Criteria: e.Criteria, MergeCommit: e.MergeCommit, Deployed: strings.Join(deployed, ","),
		QueuedAt: formatOptionalTime(e.QueuedAt), PhaseAt: formatOptionalTime(e.PhaseAt), Deadline: formatOptionalTime(e.Deadline),
		RevertPR: e.RevertPR, Reason: e.Reason,
		AgentKind: e.AgentKind, ConfiguredModel: e.ConfiguredModel, RuleName: e.RuleName, TemplateID: e.TemplateID,
	}
}

// PopulateMergeAndProve restores merge-and-prove's queue and its story in
// flight at startup. The story in flight goes on from its phase: a merge
// is read again, the deploy state read anew, and the live-proof job,
// which does not outlive the process that scheduled it, is scheduled
// again when no run of the story is going.
func PopulateMergeAndProve(state *State, rows []persistence.MergeAndProve, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	for _, r := range rows {
		pr, ok := parsePullRequest(r.PullRequest)
		if r.IssueID == "" || !ok {
			log.Warn("skipping malformed merge-and-prove record", slog.String("issue_id", r.IssueID))
			continue
		}
		pr.Branch = r.Branch
		e := &MergeAndProveEntry{
			IssueID: r.IssueID, Identifier: r.Identifier, Seq: r.Seq, Phase: r.Phase, PullRequest: pr, Head: r.Head,
			Criteria: r.Criteria, MergeCommit: r.MergeCommit, RevertPR: r.RevertPR, Reason: r.Reason,
			QueuedAt: parseFaultTime(r.QueuedAt), PhaseAt: parseFaultTime(r.PhaseAt), Deadline: parseFaultTime(r.Deadline),
			AgentKind: r.AgentKind, ConfiguredModel: r.ConfiguredModel, RuleName: r.RuleName, TemplateID: r.TemplateID,
		}
		for pair := range strings.SplitSeq(r.Deployed, ",") {
			if host, commit, ok := strings.Cut(pair, "="); ok {
				e.Deployed = append(e.Deployed, DeployedHost{Host: host, Commit: commit})
			}
		}
		state.MergeAndProve[e.IssueID] = e
		state.MergeAndProveSeq = max(state.MergeAndProveSeq, e.Seq)
	}
}

// saveMergeAndProve persists e. A failed write is logged: the runtime
// state stays authoritative for this process.
func (o *Orchestrator) saveMergeAndProve(ctx context.Context, e *MergeAndProveEntry) {
	store, ok := o.store.(mergeAndProveStore)
	if !ok {
		return
	}
	if err := store.SaveMergeAndProve(ctx, mergeAndProveRow(e)); err != nil {
		logging.WithIssue(o.logger, e.IssueID, e.Identifier).Error("failed to persist merge-and-prove", slog.Any("error", err))
	}
}

// endMergeAndProve removes e from the queue, so the next story's merge
// may start.
func (o *Orchestrator) endMergeAndProve(ctx context.Context, e *MergeAndProveEntry) {
	delete(o.state.MergeAndProve, e.IssueID)
	if store, ok := o.store.(mergeAndProveStore); ok {
		if err := store.DeleteMergeAndProve(ctx, e.IssueID); err != nil {
			logging.WithIssue(o.logger, e.IssueID, e.Identifier).Error("failed to delete merge-and-prove record", slog.Any("error", err))
		}
	}
	o.notifyObservers()
}

// mergeAndProveInFlight returns the story past the queue, nil when none.
func mergeAndProveInFlight(state *State) *MergeAndProveEntry {
	for _, e := range state.MergeAndProve {
		if e.Phase != MergeAndProveQueued {
			return e
		}
	}
	return nil
}

// mergeAndProveQueue returns the queued stories in order.
func mergeAndProveQueue(state *State) []*MergeAndProveEntry {
	var out []*MergeAndProveEntry
	for _, e := range state.MergeAndProve {
		if e.Phase == MergeAndProveQueued {
			out = append(out, e)
		}
	}
	slices.SortFunc(out, func(a, b *MergeAndProveEntry) int {
		return cmp.Or(cmp.Compare(a.Seq, b.Seq), cmp.Compare(a.IssueID, b.IssueID))
	})
	return out
}

// mergeAndProveHolds reports whether merge-and-prove has merged, or is
// merging, the issue's pull request: its done state and its merge record
// are merge-and-prove's to write, after the proof.
func mergeAndProveHolds(state *State, issueID string) bool {
	e := state.MergeAndProve[issueID]
	return e != nil && e.Phase != MergeAndProveQueued
}

// MergeAndProve accepts the story named by identifier into merge-and-prove
// when its latest criteria verdict leaves only criteria a live proof
// settles after deploy, its pull request's CI passed at its head, and, for
// a ui story, its click-proofs are recorded at that head. The story queues
// behind the one in flight; the poll tick merges it, waits for every host
// to deploy it, runs the live-proof job against the deployed main, and
// ends it done once proven, or reverts the merge.
func (s Supervisor) MergeAndProve(ctx context.Context, identifier string) SupervisorResult {
	issue, r, ok := s.lookupOnLoop(ctx, identifier)
	if !ok {
		return r
	}
	var open []*HandoffPart
	var queued bool
	if r, ok := s.onLoop(ctx, func(context.Context) {
		open = openHandoffParts(s.o.state, issue.issueID)
		_, queued = s.o.state.MergeAndProve[issue.issueID]
	}); !ok {
		return r
	}
	cfg := s.o.workflowManager.Config()
	switch {
	case queued:
		return precondition(issue, "already_queued", "issue "+identifier+" is already in merge-and-prove")
	case issue.running:
		return precondition(issue, "running", "issue "+identifier+" has a running worker")
	case issue.retrying:
		return precondition(issue, "retry_scheduled", "issue "+identifier+" already has a run scheduled")
	case len(open) > 0:
		return precondition(issue, "handoff_open", fmt.Sprintf("issue %s has %d open hand-off part(s), %s first", identifier, len(open), open[0].ID))
	case !cfg.Tracker.PlanControl():
		return precondition(issue, "plan_control_off", "merge-and-prove returns a reverted story to tracker.release_state, which the workflow does not set")
	case len(cfg.DeployHosts(s.o.hostName)) == 0:
		return precondition(issue, "no_deploy_state", "merge-and-prove reads each host's deploy state, and the workflow sets neither merge_and_prove.hosts nor health.deploy_state")
	case !s.o.reviewReactionConfigured:
		return precondition(issue, "review_reaction_off", "the live-proof job needs the review reaction configured")
	}
	if s.o.scmAdapter == nil || s.o.trackerAdapter == nil || s.o.prCommentReader == nil {
		return SupervisorResult{Status: SupervisorUnavailable, Message: "no forge or tracker is configured"}
	}
	if s.o.prReverter == nil || s.o.commitComparer == nil {
		return SupervisorResult{Status: SupervisorUnsupported, Message: "the forge adapter cannot revert a pull request or compare commits", IssueID: issue.issueID, Identifier: identifier}
	}
	fctx, cancel := context.WithTimeout(ctx, supervisorForgeBound)
	defer cancel()
	criteria, r, ok := s.afterDeployCriteria(fctx, issue)
	if !ok {
		return r
	}
	pr, r, ok := s.pullRequestOf(fctx, issue)
	if !ok {
		return r
	}
	tracked, err := s.o.trackerAdapter.FetchIssueByID(fctx, issue.issueID)
	if err != nil {
		return upstreamFailed(issue, "the tracker read failed", err)
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
		return precondition(issue, "ci_not_passed", fmt.Sprintf("the CI of %s at its head %s is %s", formatPullRequest(pr), status.HeadSHA, cmp.Or(ci, "no checks")))
	}
	if slices.ContainsFunc(tracked.Labels, isUIReviewLabel) {
		if r, ok := s.clickProofsLetMerge(fctx, issue, pr, status.HeadSHA); !ok {
			return r
		}
	}
	if pr.Branch == "" {
		pr.Branch = status.BranchName
	}
	var result SupervisorResult
	if r, ok := s.onLoop(ctx, func(loopCtx context.Context) {
		result = s.enqueueMergeAndProve(loopCtx, identifier, pr, status.HeadSHA, criteria, time.Now().UTC())
	}); !ok {
		return r
	}
	if result.Status == SupervisorDone {
		s.o.RefreshFunc()()
	}
	return result
}

// afterDeployCriteria returns the criteria the story's latest verdict
// leaves to a proof after deploy, or the refusal naming the criterion that
// keeps it out of merge-and-prove.
func (s Supervisor) afterDeployCriteria(ctx context.Context, issue supervisedIssue) (string, SupervisorResult, bool) {
	loader, ok := s.o.store.(CriteriaVerdictLoader)
	if !ok {
		return "", SupervisorResult{Status: SupervisorUnavailable, Message: "criteria verdicts are not available"}, false
	}
	stored, found, err := loader.LoadCriteriaVerdict(ctx, issue.issueID)
	switch {
	case err != nil:
		return "", upstreamFailed(issue, "the criteria verdict read failed", err), false
	case !found:
		return "", precondition(issue, "no_criteria_verdict", "the story's review recorded no verdict per criterion, so no criterion is known to wait for a deploy"), false
	}
	v := evaluateCriteria([]byte(`{"criteria":` + cmp.Or(stored.Criteria, "[]") + `}`))
	left, blocking, ok := v.afterDeployCriteria()
	if ok && stored.Outcome == CriteriaOutcomeLiveProof {
		return left, SupervisorResult{}, true
	}
	if blocking == 0 {
		return "", precondition(issue, "no_after_deploy_criterion", "the story's review left no criterion to a live proof after deploy ("+stored.Outcome+"): merge it with POST /api/v1/"+issue.identifier+"/merge instead"), false
	}
	c := v.Criteria[blocking-1]
	verdict := strings.ToLower(strings.TrimSpace(c.Verdict))
	code, why := "criteria_not_met", "its verdict is "+verdict
	if verdict == CriterionLiveProof {
		code = "live_proof_not_after_deploy"
		why = "it waits for a live proof whose reason is " + strconv.Quote(c.Reason) + ", not " + CriterionReasonAfterDeploy
	}
	return "", precondition(issue, code, fmt.Sprintf("criterion %d of the story's review keeps it from merge-and-prove: %s: %s",
		blocking, why, verdictLine(c.Criterion))), false
}

// enqueueMergeAndProve queues the story named by identifier, unless it
// queued, started a run or left meanwhile.
func (s Supervisor) enqueueMergeAndProve(ctx context.Context, identifier string, pr pullRequestRef, head, criteria string, now time.Time) SupervisorResult {
	issue, known := lookupSupervised(s.o.state, identifier)
	switch {
	case !known:
		return unknownIssue(identifier)
	case s.o.state.MergeAndProve[issue.issueID] != nil:
		return precondition(issue, "already_queued", "issue "+identifier+" is already in merge-and-prove")
	case issue.running:
		return precondition(issue, "running", "issue "+identifier+" has a running worker")
	case issue.retrying:
		return precondition(issue, "retry_scheduled", "issue "+identifier+" already has a run scheduled")
	}
	s.o.state.MergeAndProveSeq++
	e := &MergeAndProveEntry{
		IssueID: issue.issueID, Identifier: identifier, Seq: s.o.state.MergeAndProveSeq, Phase: MergeAndProveQueued,
		PullRequest: pr, Head: head, Criteria: criteria, QueuedAt: now, PhaseAt: now,
	}
	switch {
	case issue.watch != nil:
		e.AgentKind, e.ConfiguredModel, e.RuleName, e.TemplateID = issue.watch.AgentKind, issue.watch.ConfiguredModel, issue.watch.RuleName, issue.watch.TemplateID
	case issue.parked:
		park := s.o.state.Parked[issue.issueID]
		e.AgentKind, e.ConfiguredModel, e.RuleName, e.TemplateID = park.AgentKind, park.ConfiguredModel, park.RuleName, park.TemplateID
	}
	s.o.state.MergeAndProve[e.IssueID] = e
	s.o.saveMergeAndProve(ctx, e)
	logging.WithIssue(s.o.logger, e.IssueID, identifier).Info("story queued for merge-and-prove",
		slog.String("pull_request", formatPullRequest(pr)), slog.String("head_sha", head), slog.Int("ahead", len(mergeAndProveQueue(s.o.state))-1))
	s.o.notifyObservers()
	return SupervisorResult{Status: SupervisorDone, IssueID: e.IssueID, Identifier: identifier, PullRequest: formatPullRequest(pr)}
}

// advanceMergeAndProve takes merge-and-prove's story in flight one step,
// and starts the next queued story's merge once none is in flight. Runs
// on the event loop, on the poll tick.
func (o *Orchestrator) advanceMergeAndProve(ctx context.Context, cfg config.ServiceConfig, now time.Time) {
	for range 3 {
		e := mergeAndProveInFlight(o.state)
		if e == nil {
			queue := mergeAndProveQueue(o.state)
			if len(queue) == 0 {
				return
			}
			e = queue[0]
			o.setMergeAndProvePhase(ctx, e, MergeAndProveMerging, now, time.Time{})
		}
		switch e.Phase {
		case MergeAndProveMerging:
			o.mergeStep(ctx, cfg, e, now)
		case MergeAndProveDeploying:
			o.deployStep(ctx, cfg, e, now)
		case MergeAndProveProving:
			o.proveStep(ctx, cfg, e, now)
		case MergeAndProveReverting:
			o.revertStep(ctx, cfg, e)
		default:
			logging.WithIssue(o.logger, e.IssueID, e.Identifier).Warn("merge-and-prove record in an unknown phase, ending it", slog.String("phase", e.Phase))
			o.endMergeAndProve(ctx, e)
		}
		if o.state.MergeAndProve[e.IssueID] == e {
			return
		}
	}
}

// setMergeAndProvePhase moves e to phase, persists it and tells observers.
func (o *Orchestrator) setMergeAndProvePhase(ctx context.Context, e *MergeAndProveEntry, phase string, now, deadline time.Time) {
	e.Phase, e.PhaseAt, e.Deadline, e.Waiting = phase, now, deadline, ""
	o.saveMergeAndProve(ctx, e)
	logging.WithIssue(o.logger, e.IssueID, e.Identifier).Info("merge-and-prove phase",
		slog.String("phase", phase), slog.String("pull_request", formatPullRequest(e.PullRequest)),
		slog.String("deadline", formatOptionalTime(deadline)))
	o.notifyObservers()
}

// commentStory comments body on the story, bounded; a failure is logged.
func (o *Orchestrator) commentStory(ctx context.Context, e *MergeAndProveEntry, body string) {
	if o.trackerAdapter == nil {
		return
	}
	wctx, cancel := context.WithTimeout(ctx, planControlWriteBound)
	defer cancel()
	if err := o.trackerAdapter.CommentIssue(wctx, e.IssueID, body); err != nil {
		logging.WithIssue(o.logger, e.IssueID, e.Identifier).Warn("merge-and-prove comment failed", slog.Any("error", err))
	}
}

// mergeStep merges the pull request at the head whose CI passed, or takes
// the merge someone else made, and starts the wait for the deploy. A pull
// request that closed, moved its head, or that the forge refuses to merge
// ends the story's run with nothing merged.
func (o *Orchestrator) mergeStep(ctx context.Context, cfg config.ServiceConfig, e *MergeAndProveEntry, now time.Time) {
	log := logging.WithIssue(o.logger, e.IssueID, e.Identifier)
	pr := e.PullRequest
	fctx, cancel := context.WithTimeout(ctx, mergeAndProveBound)
	defer cancel()
	status, err := o.scmAdapter.GetMergeability(fctx, pr.Number, pr.Owner, pr.Repo)
	if err != nil {
		e.Waiting = "the forge read failed: " + err.Error()
		log.Warn("merge-and-prove pull request read failed", slog.Any("error", err))
		return
	}
	commit := status.MergeCommitSHA
	switch {
	case status.Merged:
		if commit == "" {
			e.Waiting = "the forge shows " + formatPullRequest(pr) + " merged without its merge commit yet"
			return
		}
	case status.Closed:
		o.giveUpMergeAndProve(ctx, e, formatPullRequest(pr)+" was closed before Sortie merged it")
		return
	case !strings.EqualFold(status.HeadSHA, e.Head):
		o.giveUpMergeAndProve(ctx, e, fmt.Sprintf("the head of %s moved from %s to %s after the story was accepted, so its CI is not known to pass", formatPullRequest(pr), e.Head, status.HeadSHA))
		return
	default:
		strategy := o.autoMergeConfig.Strategy
		if strategy == "" {
			strategy = domain.StrategySquash
		}
		merged, err := o.scmAdapter.MergePR(fctx, pr.Number, pr.Owner, pr.Repo, strategy, "", "", e.Head)
		if err != nil {
			if scmErr, ok := errors.AsType[*domain.SCMError](err); ok && scmErr.Kind == domain.ErrSCMConflict && !strings.Contains(strings.ToLower(scmErr.Message), "already merged") {
				o.giveUpMergeAndProve(ctx, e, "the forge refused the merge of "+formatPullRequest(pr)+": "+scmErr.Message)
				return
			}
			e.Waiting = "the merge failed: " + err.Error()
			log.Warn("merge-and-prove merge failed, trying again on the next tick", slog.Any("error", err))
			return
		}
		if merged.SHA == "" {
			e.Waiting = "the forge merged " + formatPullRequest(pr) + " without naming its merge commit"
			return
		}
		commit = merged.SHA
	}
	e.MergeCommit = strings.ToLower(commit)
	dropPullRequestWatches(o.state, e.IssueID)
	recordIssueOutcome(ctx, o.store, mergedOutcome(e.IssueID, "merge-and-prove", pr.Number, pr.Owner, pr.Repo, now), log)
	o.setMergeAndProvePhase(ctx, e, MergeAndProveDeploying, now, now.Add(cfg.MergeAndProve.Deadline))
	log.Info("merge-and-prove merged the pull request", slog.String("merge_commit", e.MergeCommit))
	o.commentStory(ctx, e, fmt.Sprintf("Sortie merged %s as %s through merge-and-prove. It waits until %s for every host to deploy it, then runs the live proof of these criteria against the deployed main:\n\n%s\nWithout a proven record by its deadline, Sortie reverts the merge and returns the story to %s.",
		formatPullRequest(pr), e.MergeCommit, e.Deadline.Format(time.RFC3339), e.Criteria, cfg.Tracker.ReleaseState))
}

// dropPullRequestWatches removes the issue's pull request watches once
// merge-and-prove merged it: the story's done state is merge-and-prove's
// to set, after the proof.
func dropPullRequestWatches(state *State, issueID string) {
	for _, kind := range []string{ReactionKindCI, ReactionKindReview, ReactionKindBotReview, ReactionKindAutoMerge, ReactionKindMergeConflict, ReactionKindMergeCompletion} {
		delete(state.PendingReactions, ReactionKey(issueID, kind))
	}
}

// giveUpMergeAndProve ends a story's run before anything merged: the story
// stays where it is and the next story's merge starts.
func (o *Orchestrator) giveUpMergeAndProve(ctx context.Context, e *MergeAndProveEntry, reason string) {
	logging.WithIssue(o.logger, e.IssueID, e.Identifier).Warn("merge-and-prove ended without a merge", slog.String("reason", reason))
	o.commentStory(ctx, e, "Sortie did not merge this story through merge-and-prove: "+reason+".")
	o.endMergeAndProve(ctx, e)
}

// deployStep reads each host's deploy state and starts the live proof
// once every host deployed a commit that holds the merge commit. A host
// still without it at the deadline reverts the merge.
func (o *Orchestrator) deployStep(ctx context.Context, cfg config.ServiceConfig, e *MergeAndProveEntry, now time.Time) {
	fctx, cancel := context.WithTimeout(ctx, mergeAndProveBound)
	defer cancel()
	hosts := cfg.DeployHosts(o.hostName)
	var behind []string
	var deployed []DeployedHost
	for _, h := range hosts {
		st, err := deploystate.Read(h.DeployState)
		switch {
		case err != nil:
			behind = append(behind, h.Name+": its deploy state is unreadable: "+err.Error())
			continue
		case st.Commit == "":
			behind = append(behind, h.Name+": its deploy state names no commit ("+st.Status+")")
			continue
		}
		holds, err := o.commitHolds(fctx, e, st.Commit)
		switch {
		case err != nil:
			behind = append(behind, h.Name+": the forge could not compare "+st.Commit+": "+err.Error())
		case !holds:
			behind = append(behind, h.Name+" deployed "+st.Commit+" ("+st.Status+"), which does not hold "+e.MergeCommit)
		default:
			deployed = append(deployed, DeployedHost{Host: h.Name, Commit: strings.ToLower(st.Commit)})
		}
	}
	if len(hosts) == 0 {
		behind = append(behind, "no host's deploy state is configured")
	}
	if len(behind) == 0 {
		e.Deployed = deployed
		o.setMergeAndProvePhase(ctx, e, MergeAndProveProving, now, now.Add(cfg.MergeAndProve.Deadline))
		o.proveStep(ctx, cfg, e, now)
		return
	}
	e.Waiting = strings.Join(behind, "; ")
	if !now.Before(e.Deadline) {
		o.startRevert(ctx, cfg, e, now, "not every host deployed merge commit "+e.MergeCommit+" by the deadline "+e.Deadline.Format(time.RFC3339)+": "+e.Waiting)
	}
}

// commitHolds reports whether commit holds e's merge commit, asking the
// forge once per commit.
func (o *Orchestrator) commitHolds(ctx context.Context, e *MergeAndProveEntry, commit string) (bool, error) {
	commit = strings.ToLower(commit)
	if strings.HasPrefix(commit, e.MergeCommit) || (len(commit) >= minClickProofsSHA && strings.HasPrefix(e.MergeCommit, commit)) {
		return true, nil
	}
	if holds, ok := e.contains[commit]; ok {
		return holds, nil
	}
	holds, err := o.commitComparer.CommitContains(ctx, e.PullRequest.Owner, e.PullRequest.Repo, commit, e.MergeCommit)
	if err != nil {
		return false, err
	}
	if e.contains == nil {
		e.contains = map[string]bool{}
	}
	e.contains[commit] = holds
	return holds, nil
}

// proveStep schedules the live-proof job when this process has not, and
// reads the pull request for its record: a proven record ends the story
// done, an unproven one or the deadline reverts the merge.
func (o *Orchestrator) proveStep(ctx context.Context, cfg config.ServiceConfig, e *MergeAndProveEntry, now time.Time) {
	log := logging.WithIssue(o.logger, e.IssueID, e.Identifier)
	if !e.jobScheduled && !workerHolds(o.state, e.IssueID) {
		o.scheduleAfterDeployProof(ctx, e, now)
	}
	fctx, cancel := context.WithTimeout(ctx, mergeAndProveBound)
	defer cancel()
	pr := e.PullRequest
	comments, err := o.prCommentReader.FetchPullRequestComments(fctx, pr.Number, pr.Owner, pr.Repo)
	if err != nil {
		e.Waiting = "the pull request comments read failed: " + err.Error()
		log.Warn("merge-and-prove record read failed", slog.Any("error", err))
	} else {
		for _, c := range comments {
			if sha, ok := recordLine(c.Body, LiveProofMarker); ok && o.recordNamesMerge(fctx, e, sha) {
				o.proven(ctx, cfg, e, c, now)
				return
			}
			if sha, ok := recordLine(c.Body, LiveProofUnprovenMarker); ok && o.recordNamesMerge(fctx, e, sha) {
				o.startRevert(ctx, cfg, e, now, "the live proof after deploy is unproven: "+capReason(c.Body))
				return
			}
		}
		e.Waiting = fmt.Sprintf("no comment on %s records the live proof proven at %s (%s %s)", formatPullRequest(pr), e.MergeCommit, LiveProofMarker, e.MergeCommit)
	}
	if !now.Before(e.Deadline) {
		o.startRevert(ctx, cfg, e, now, fmt.Sprintf("no comment on %s recorded the live proof proven at merge commit %s by the deadline %s", formatPullRequest(pr), e.MergeCommit, e.Deadline.Format(time.RFC3339)))
	}
}

// recordNamesMerge reports whether sha, as a record names it, is the merge
// commit, a commit a host deployed it in, or another commit that holds it.
func (o *Orchestrator) recordNamesMerge(ctx context.Context, e *MergeAndProveEntry, sha string) bool {
	if strings.HasPrefix(e.MergeCommit, sha) {
		return true
	}
	for _, d := range e.Deployed {
		if strings.HasPrefix(d.Commit, sha) {
			return true
		}
	}
	holds, err := o.commitHolds(ctx, e, sha)
	return err == nil && holds
}

func capReason(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > maxMergeAndProveReason {
		s = s[:maxMergeAndProveReason] + "…"
	}
	return s
}

// afterDeployProofRequest is the request the live-proof job answers after
// the deploy.
func afterDeployProofRequest(e *MergeAndProveEntry) string {
	deployed := make([]string, 0, len(e.Deployed))
	for _, d := range e.Deployed {
		deployed = append(deployed, d.Host+" at "+d.Commit)
	}
	return LiveProofRequestMarker + "\n" +
		"Live proof of " + e.Identifier + " after deploy, requested by Sortie's merge-and-prove.\n" +
		"Pull request: " + formatPullRequest(e.PullRequest) + " (merged)\n" +
		"Merge commit: " + e.MergeCommit + "\n" +
		"Deployed: " + strings.Join(deployed, ", ") + "\n" +
		"Deadline: " + e.Deadline.UTC().Format(time.RFC3339) + "\n" +
		"Prove these criteria against the deployed hosts, running main as deployed, not against a checkout:\n" + e.Criteria +
		"Then comment on the pull request `" + LiveProofMarker + " " + e.MergeCommit + "` when every one is proven, or `" +
		LiveProofUnprovenMarker + " " + e.MergeCommit + "` with what failed. Without a proven record by the deadline, Sortie reverts the merge."
}

// scheduleAfterDeployProof schedules the live-proof job against the
// deployed main.
func (o *Orchestrator) scheduleAfterDeployProof(ctx context.Context, e *MergeAndProveEntry, now time.Time) {
	log := logging.WithIssue(o.logger, e.IssueID, e.Identifier)
	comment := domain.ReviewComment{
		ID:          "merge-and-prove-" + strconv.FormatInt(now.UnixNano(), 10),
		Reviewer:    "sortie",
		Body:        afterDeployProofRequest(e),
		SubmittedAt: now,
	}
	pr := e.PullRequest
	scheduleReactionContinuation(ctx, o.state, o.store, ScheduleRetryParams{
		IssueID:    e.IssueID,
		Identifier: e.Identifier,
		DelayMS:    continuationDelayMS,
		ContinuationContext: map[string]any{
			reviewCommentsKey: buildReviewTemplateMap([]domain.ReviewComment{comment}),
		},
		ReactionKind:    ReactionKindLiveProof,
		Continuation:    ContinuationTarget{PRNumber: pr.Number, Owner: pr.Owner, Repo: pr.Repo},
		AgentKind:       e.AgentKind,
		ConfiguredModel: e.ConfiguredModel,
		RuleName:        e.RuleName,
		TemplateID:      e.TemplateID,
		Logger:          log,
	}, o.onRetryFire, now)
	e.jobScheduled = true
	log.Info("merge-and-prove scheduled the live-proof job against the deployed main",
		slog.String("merge_commit", e.MergeCommit), slog.String("deadline", formatOptionalTime(e.Deadline)))
}

// proven ends the story done: its merge record is written, so its
// dependents start, and it moves to the done state.
func (o *Orchestrator) proven(ctx context.Context, cfg config.ServiceConfig, e *MergeAndProveEntry, record domain.ReviewComment, now time.Time) {
	log := logging.WithIssue(o.logger, e.IssueID, e.Identifier)
	pr := e.PullRequest
	if err := recordPlanMerge(o.store, e.IssueID, e.MergeCommit, &MergeCompletionReactionData{PRNumber: pr.Number, Owner: pr.Owner, Repo: pr.Repo}, ctx); err != nil {
		e.Waiting = "the merge record could not be written: " + err.Error()
		log.Error("merge-and-prove merge record failed, trying again on the next tick", slog.Any("error", err))
		return
	}
	done := o.doneState(cfg)
	if done != "" && o.trackerAdapter != nil {
		wctx, cancel := context.WithTimeout(ctx, planControlWriteBound)
		err := o.trackerAdapter.TransitionIssue(wctx, e.IssueID, done)
		cancel()
		if err != nil {
			e.Waiting = "the story could not be moved to " + done + ": " + err.Error()
			log.Warn("merge-and-prove could not move the proven story to the done state, trying again on the next tick", slog.Any("error", err))
			return
		}
	}
	log.Info("merge-and-prove proven: the story is done",
		slog.String("merge_commit", e.MergeCommit), slog.String("record", record.ID), slog.String("done_state", done),
		slog.Duration("after_deploy", now.Sub(e.PhaseAt)))
	o.commentStory(ctx, e, fmt.Sprintf("The live proof after deploy of %s is proven at %s (comment %s). The story is done.", formatPullRequest(pr), e.MergeCommit, record.ID))
	o.endMergeAndProve(ctx, e)
}

// doneState is the state a proven story moves to: the merge-completion
// target when that reaction is configured, else tracker.done_state.
func (o *Orchestrator) doneState(cfg config.ServiceConfig) string {
	if o.mergeCompletionReactionConfigured && o.mergeCompletionConfig.TargetState != "" {
		return o.mergeCompletionConfig.TargetState
	}
	return cfg.Tracker.DoneState
}

// startRevert moves e to the revert of its merge commit, for reason.
func (o *Orchestrator) startRevert(ctx context.Context, cfg config.ServiceConfig, e *MergeAndProveEntry, now time.Time, reason string) {
	e.Reason = capReason(reason)
	logging.WithIssue(o.logger, e.IssueID, e.Identifier).Warn("merge-and-prove reverts the merge", slog.String("reason", e.Reason))
	o.setMergeAndProvePhase(ctx, e, MergeAndProveReverting, now, time.Time{})
	o.revertStep(ctx, cfg, e)
}

// revertStep opens the revert pull request of the merge once, and merges
// it once its CI passes. A revert with no checks yet waits: the forge
// may not have started them when it was just opened.
func (o *Orchestrator) revertStep(ctx context.Context, cfg config.ServiceConfig, e *MergeAndProveEntry) {
	log := logging.WithIssue(o.logger, e.IssueID, e.Identifier)
	pr := e.PullRequest
	fctx, cancel := context.WithTimeout(ctx, mergeAndProveBound)
	defer cancel()
	if e.RevertPR <= 0 {
		opened, err := o.prReverter.RevertPullRequest(fctx, pr.Number, pr.Owner, pr.Repo,
			"Revert "+e.Identifier+": not proven after deploy",
			fmt.Sprintf("Reverts %s, merged as %s by Sortie's merge-and-prove, because its change was not proven live in time.\n\nReason: %s", formatPullRequest(pr), e.MergeCommit, e.Reason))
		if err != nil {
			e.Waiting = "the revert pull request could not be opened: " + err.Error()
			log.Warn("merge-and-prove revert pull request failed, trying again on the next tick", slog.Any("error", err))
			return
		}
		e.RevertPR = opened.Number
		o.saveMergeAndProve(ctx, e)
		log.Info("merge-and-prove opened the revert pull request", slog.Int("revert_pr_number", opened.Number))
	}
	revert := pullRequestRef{Number: e.RevertPR, Owner: pr.Owner, Repo: pr.Repo}
	status, err := o.scmAdapter.GetMergeability(fctx, revert.Number, revert.Owner, revert.Repo)
	if err != nil {
		e.Waiting = "the revert pull request read failed: " + err.Error()
		return
	}
	switch {
	case status.Merged:
		o.finishRevert(ctx, cfg, e, formatPullRequest(revert), true)
		return
	case status.Closed:
		log.Warn("merge-and-prove revert pull request closed without a merge; the story is left to a person", slog.Int("revert_pr_number", revert.Number))
		o.commentStory(ctx, e, fmt.Sprintf("Sortie opened %s to revert %s (%s), because: %s\nThe revert was closed without a merge, so the change stays on main and Sortie leaves the story to a person.",
			formatPullRequest(revert), formatPullRequest(pr), e.MergeCommit, e.Reason))
		o.endMergeAndProve(ctx, e)
		return
	case status.Mergeability != domain.MergeabilityClean && status.Mergeability != domain.MergeabilityUnstable || status.HeadSHA == "":
		e.Waiting = fmt.Sprintf("the revert %s is not mergeable yet: %s", formatPullRequest(revert), status.Mergeability)
		return
	}
	ci, err := o.scmAdapter.GetCIStatus(fctx, revert.Number, revert.Owner, revert.Repo)
	if err != nil {
		e.Waiting = "the revert's CI read failed: " + err.Error()
		return
	}
	if ci != "success" {
		e.Waiting = fmt.Sprintf("the CI of the revert %s is %s", formatPullRequest(revert), cmp.Or(ci, "not started"))
		return
	}
	strategy := o.autoMergeConfig.Strategy
	if strategy == "" {
		strategy = domain.StrategySquash
	}
	if _, err := o.scmAdapter.MergePR(fctx, revert.Number, revert.Owner, revert.Repo, strategy, "", "", status.HeadSHA); err != nil {
		e.Waiting = "the revert's merge failed: " + err.Error()
		log.Warn("merge-and-prove revert merge failed, trying again on the next tick", slog.Any("error", err))
		return
	}
	o.finishRevert(ctx, cfg, e, formatPullRequest(revert), true)
}

// finishRevert ends a story whose merge a merged revert undid: it is not
// done, so its merge record goes, it returns to tracker.release_state with
// the reason, and the operator's channel hears of it.
func (o *Orchestrator) finishRevert(ctx context.Context, cfg config.ServiceConfig, e *MergeAndProveEntry, revert string, bySortie bool) {
	moved, err := o.unsatisfyStory(ctx, cfg, e.IssueID, true)
	o.endReverted(ctx, e, revert, bySortie, moved, err)
}

// endReverted ends e as reverted by revert once its story left the
// tracker's done state for moved, or err said why it could not. It does no
// tracker or forge call on the loop: the comment is written detached.
func (o *Orchestrator) endReverted(ctx context.Context, e *MergeAndProveEntry, revert string, bySortie bool, moved string, err error) {
	log := logging.WithIssue(o.logger, e.IssueID, e.Identifier)
	reason := cmp.Or(e.Reason, "the merge was reverted by "+revert)
	if err != nil {
		e.Waiting = "the reverted story could not be returned: " + err.Error()
		log.Warn("merge-and-prove could not return the reverted story, trying again on the next tick", slog.Any("error", err))
		return
	}
	log.Warn("merge-and-prove reverted the story's merge; the story is not done",
		slog.String("revert", revert), slog.String("merge_commit", e.MergeCommit), slog.String("reason", reason), slog.String("state", moved))
	o.commentDetached(ctx, e.IssueID, fmt.Sprintf("Sortie reverted %s (%s) with %s, so the story is not done and is back in %s.\n\nReason: %s",
		formatPullRequest(e.PullRequest), e.MergeCommit, revert, cmp.Or(moved, "its state"), reason), log)
	o.publishStoryReverted(ctx, e.IssueID, e.Identifier, domain.StoryRevertedFacts{
		PullRequest: formatPullRequest(e.PullRequest), MergeCommit: e.MergeCommit, RevertPullRequest: revert,
		BySortie: bySortie, Reason: reason, State: moved,
	}, log)
	o.endMergeAndProve(ctx, e)
}

// commentDetached comments body on the issue off the event loop, bounded;
// a failure is logged.
func (o *Orchestrator) commentDetached(ctx context.Context, issueID, body string, log *slog.Logger) {
	tracker := o.trackerAdapter
	if tracker == nil {
		return
	}
	o.state.TrackerOpsWg.Go(func() {
		wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), planControlWriteBound)
		defer cancel()
		if err := tracker.CommentIssue(wctx, issueID, body); err != nil {
			log.Warn("reverted story comment failed", slog.Any("error", err))
		}
	})
}

// unsatisfyStory removes the story's merge record, so the stories it
// blocks wait for it again, and moves it to tracker.release_state as
// [releaseStoryOnTracker] does. It returns the state it moved the story
// to, empty when it left the state as it was.
func (o *Orchestrator) unsatisfyStory(ctx context.Context, cfg config.ServiceConfig, issueID string, always bool) (string, error) {
	moved, err := releaseStoryOnTracker(ctx, cfg, o.trackerAdapter, issueID, always)
	if err != nil {
		return "", err
	}
	return moved, o.deleteMergeRecord(ctx, issueID)
}

// releaseStoryOnTracker moves the story to tracker.release_state when it
// is done or handed off (or, with always, from any state but that and the
// cancel state). It returns the state it moved the story to, empty when it
// left the state as it was. It reads only its arguments, so it may run off
// the event loop.
func releaseStoryOnTracker(ctx context.Context, cfg config.ServiceConfig, tracker domain.TrackerAdapter, issueID string, always bool) (string, error) {
	target := cfg.Tracker.ReleaseState
	if target == "" || tracker == nil {
		return "", nil
	}
	rctx, cancel := context.WithTimeout(ctx, mergeAndProveBound)
	issue, err := tracker.FetchIssueByID(rctx, issueID)
	cancel()
	if err != nil {
		return "", fmt.Errorf("tracker read: %w", err)
	}
	leave := strings.EqualFold(issue.State, target) || strings.EqualFold(issue.State, cfg.Tracker.CancelState)
	done := isTerminalState(issue.State, cfg.Tracker.TerminalStates) || strings.EqualFold(issue.State, cfg.Tracker.DoneState) ||
		strings.EqualFold(issue.State, cfg.Tracker.HandoffState)
	if leave || (!always && !done) {
		return "", nil
	}
	wctx, wcancel := context.WithTimeout(ctx, planControlWriteBound)
	err = tracker.TransitionIssue(wctx, issueID, target)
	wcancel()
	if err != nil {
		return "", fmt.Errorf("move to %s: %w", target, err)
	}
	return target, nil
}

// deleteMergeRecord removes the story's merge record.
func (o *Orchestrator) deleteMergeRecord(ctx context.Context, issueID string) error {
	if del, ok := o.store.(planSatisfactionDeleter); ok {
		if err := del.DeletePlanSatisfaction(ctx, issueID, persistence.PlanSatisfactionMerged); err != nil {
			return fmt.Errorf("merge record delete: %w", err)
		}
	}
	return nil
}

// publishStoryReverted routes the story.reverted event.
func (o *Orchestrator) publishStoryReverted(ctx context.Context, issueID, identifier string, facts domain.StoryRevertedFacts, log *slog.Logger) {
	if o.router == nil {
		return
	}
	n := orchestratorNotification(domain.EventStoryReverted, "", domain.NotificationEnvelope{IssueID: issueID, Identifier: identifier}, facts)
	delivery := o.router.Route(n)
	if delivery.Empty() {
		log.Info("story revert had no destination", slog.String("event_type", string(domain.EventStoryReverted)))
		return
	}
	deliverDetached(ctx, &o.state.TrackerOpsWg, delivery, log, func(received bool, err error) {
		if received && err != nil {
			log.Warn("story revert tracker comment failed", slog.Any("error", err))
		}
	})
}

// SnapshotMergeAndProveEntry is a read-only view of one story in
// merge-and-prove. Deadline is zero in the queue and while merging.
type SnapshotMergeAndProveEntry struct {
	IssueID     string
	Identifier  string
	Position    int // 0 in flight, 1 first in the queue
	Phase       string
	PullRequest string
	Head        string
	MergeCommit string
	Deployed    []DeployedHost
	QueuedAt    time.Time
	PhaseAt     time.Time
	Deadline    time.Time
	RevertPR    string
	Reason      string
	Waiting     string
}

// SnapshotMergeAndProve is merge-and-prove's story in flight, nil when
// none, and its queue in order.
type SnapshotMergeAndProve struct {
	InFlight *SnapshotMergeAndProveEntry
	Queue    []SnapshotMergeAndProveEntry
}

func snapshotMergeAndProveEntry(e *MergeAndProveEntry, position int) SnapshotMergeAndProveEntry {
	s := SnapshotMergeAndProveEntry{
		IssueID: e.IssueID, Identifier: e.Identifier, Position: position, Phase: e.Phase,
		PullRequest: formatPullRequest(e.PullRequest), Head: e.Head, MergeCommit: e.MergeCommit,
		Deployed: slices.Clone(e.Deployed), QueuedAt: e.QueuedAt, PhaseAt: e.PhaseAt, Deadline: e.Deadline,
		Reason: e.Reason, Waiting: e.Waiting,
	}
	if e.RevertPR > 0 {
		s.RevertPR = formatPullRequest(pullRequestRef{Number: e.RevertPR, Owner: e.PullRequest.Owner, Repo: e.PullRequest.Repo})
	}
	return s
}

func snapshotMergeAndProve(state *State) SnapshotMergeAndProve {
	snap := SnapshotMergeAndProve{Queue: []SnapshotMergeAndProveEntry{}}
	if e := mergeAndProveInFlight(state); e != nil {
		v := snapshotMergeAndProveEntry(e, 0)
		snap.InFlight = &v
	}
	for i, e := range mergeAndProveQueue(state) {
		snap.Queue = append(snap.Queue, snapshotMergeAndProveEntry(e, i+1))
	}
	return snap
}

// MergeAndProvePhases returns every phase of merge-and-prove, in order.
// The slice is new on each call.
func MergeAndProvePhases() []string {
	return []string{MergeAndProveQueued, MergeAndProveMerging, MergeAndProveDeploying, MergeAndProveProving, MergeAndProveReverting}
}
