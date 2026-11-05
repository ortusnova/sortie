package orchestrator

import (
	"cmp"
	"context"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/logging"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// parkRecordReadBound bounds the tracker and forge reads of one parked
// story's record check, off the event loop.
const parkRecordReadBound = 20 * time.Second

// releaseParksByRecord reads, under plan control, the pull request of each
// parked story at most once per auto-merge poll interval, and releases the
// park when that pull request holds, at its current head, every record the
// story's latest criteria verdict leaves open ([recordsNeeded]). A released
// park starts no session: the story moves to tracker.handoff_state, as a
// passed review's does, and Sortie seeds the pull request's watches at that
// head, so auto-merge's own checks decide the merge. A park whose story has
// no criteria verdict, a refused one, or one no record settles is never
// released here, nor is one whose record names another commit. The reads
// run off the event loop ([Orchestrator.startParkRecordCheck]). Must run
// on the event loop.
func (o *Orchestrator) releaseParksByRecord(ctx context.Context, cfg config.ServiceConfig, now time.Time) {
	if len(o.state.Parked) == 0 && len(o.state.ParkRecordChecked) == 0 {
		return
	}
	for id := range o.state.ParkRecordChecked {
		if _, parked := o.state.Parked[id]; !parked {
			delete(o.state.ParkRecordChecked, id)
		}
	}
	if !cfg.Tracker.PlanControl() || cfg.Tracker.HandoffState == "" || !o.autoMergeReactionConfigured ||
		o.scmAdapter == nil || o.prCommentReader == nil || o.trackerAdapter == nil {
		return
	}
	loader, ok := o.store.(CriteriaVerdictLoader)
	if !ok {
		return
	}
	interval := max(time.Duration(o.autoMergeConfig.PollIntervalMS)*time.Millisecond, 30*time.Second)
	ids := make([]string, 0, len(o.state.Parked))
	for id := range o.state.Parked {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		if last, checked := o.state.ParkRecordChecked[id]; checked && now.Sub(last) < interval {
			continue
		}
		if workerHolds(o.state, id) {
			continue
		}
		if o.parkRecordReads[id] {
			continue
		}
		o.state.ParkRecordChecked[id] = now
		o.startParkRecordCheck(ctx, cfg, loader, id, now)
	}
}

// parkRecordFound is what a park record read found off the event loop:
// the story's pull request at its head, moved on the tracker to the
// handoff state, and the comments that record what its review left open.
type parkRecordFound struct {
	pr         pullRequestRef
	commentIDs []string
}

// startParkRecordCheck reads, off the event loop, whether the pull request
// of issueID's park holds the records its verdict leaves open at its head,
// and releases the park on the loop when it does. At most one read of an
// issue is in flight. Must run on the event loop.
func (o *Orchestrator) startParkRecordCheck(ctx context.Context, cfg config.ServiceConfig, loader CriteriaVerdictLoader, issueID string, now time.Time) {
	park := o.state.Parked[issueID]
	issue, known := lookupSupervised(o.state, park.Identifier)
	if !known {
		return
	}
	if o.parkRecordReads == nil {
		o.parkRecordReads = map[string]bool{}
	}
	o.parkRecordReads[issueID] = true
	log := logging.WithIssue(o.logger, issueID, park.Identifier)
	o.state.TrackerOpsWg.Go(func() {
		found, ok := o.readParkRecord(ctx, cfg, loader, issueID, issue, log)
		o.runOnLoop(ctx, func(loopCtx context.Context) {
			delete(o.parkRecordReads, issueID)
			if ok && o.state.Parked[issueID] == park {
				o.releaseParkByRecord(loopCtx, cfg, issueID, park, found, now, log)
			}
		})
	})
}

// readParkRecord reads whether the pull request of issueID holds the
// records its verdict leaves open at its head, and when it does moves the
// story to the handoff state. It reads no runtime state, so it runs off
// the event loop.
func (o *Orchestrator) readParkRecord(ctx context.Context, cfg config.ServiceConfig, loader CriteriaVerdictLoader, issueID string, issue supervisedIssue, log *slog.Logger) (parkRecordFound, bool) {
	rctx, cancel := context.WithTimeout(ctx, parkRecordReadBound)
	defer cancel()

	v, found, err := loader.LoadCriteriaVerdict(rctx, issueID)
	if err != nil {
		log.Warn("parked story's criteria verdict read failed", slog.Any("error", err))
		return parkRecordFound{}, false
	}
	if !found || (v.Outcome != CriteriaOutcomeMet && v.Outcome != CriteriaOutcomeLiveProof) {
		return parkRecordFound{}, false
	}
	pr, refused, ok := o.Supervisor().pullRequestOf(rctx, issue)
	if !ok {
		log.Debug("parked story's pull request not found", slog.String("reason", refused.Message))
		return parkRecordFound{}, false
	}
	tracked, err := o.trackerAdapter.FetchIssueByID(rctx, issueID)
	if err != nil {
		log.Warn("parked story's tracker read failed", slog.Any("error", err))
		return parkRecordFound{}, false
	}
	if isTerminalState(tracked.State, cfg.Tracker.TerminalStates) {
		return parkRecordFound{}, false
	}
	needs := recordsNeeded(slices.ContainsFunc(tracked.Labels, isUIReviewLabel), v.Outcome)
	if len(needs) == 0 {
		return parkRecordFound{}, false
	}
	status, err := o.scmAdapter.GetMergeability(rctx, pr.Number, pr.Owner, pr.Repo)
	if err != nil {
		log.Warn("parked story's pull request read failed", slog.Int("pr_number", pr.Number), slog.Any("error", err))
		return parkRecordFound{}, false
	}
	if status.Merged || status.Closed || status.HeadSHA == "" {
		return parkRecordFound{}, false
	}
	comments, err := o.prCommentReader.FetchPullRequestComments(rctx, pr.Number, pr.Owner, pr.Repo)
	if err != nil {
		log.Warn("parked story's pull request comments read failed", slog.Int("pr_number", pr.Number), slog.Any("error", err))
		return parkRecordFound{}, false
	}
	commentIDs, missing := recordsAtHead(comments, status.HeadSHA, needs)
	if missing != "" {
		log.Debug("parked story's pull request lacks a record at its head",
			slog.Int("pr_number", pr.Number), slog.String("head_sha", status.HeadSHA), slog.String("record", missing))
		return parkRecordFound{}, false
	}
	pr.HeadSHA = status.HeadSHA
	if pr.Branch == "" {
		pr.Branch = status.BranchName
	}

	if !strings.EqualFold(tracked.State, cfg.Tracker.HandoffState) {
		wctx, wcancel := context.WithTimeout(ctx, planControlWriteBound)
		err := o.trackerAdapter.TransitionIssue(wctx, issueID, cfg.Tracker.HandoffState)
		wcancel()
		if err != nil {
			log.Warn("park release by record could not move the story to the handoff state, keeping the park",
				slog.String("handoff_state", cfg.Tracker.HandoffState), slog.Any("error", err))
			return parkRecordFound{}, false
		}
	}
	return parkRecordFound{pr: pr, commentIDs: commentIDs}, true
}

// releaseParkByRecord releases park, whose story readParkRecord found
// recorded at its pull request's head and moved to the handoff state, and
// seeds the pull request's watches. Must run on the event loop.
func (o *Orchestrator) releaseParkByRecord(ctx context.Context, cfg config.ServiceConfig, issueID string, park *ParkedEntry, found parkRecordFound, now time.Time, log *slog.Logger) {
	pr, commentIDs := found.pr, found.commentIDs
	record := ParkRecord{PullRequest: formatPullRequest(pr), Head: pr.HeadSHA, Comments: commentIDs}
	park.Record = &record
	unparkIssue(ctx, o.state, issueID, unparkTriggerRecord, o.store, log)
	delete(o.state.ParkRecordChecked, issueID)
	recordIssueOutcome(ctx, o.store, persistence.IssueOutcome{
		IssueID:        issueID,
		Kind:           persistence.IssueOutcomeHandedOff,
		ObservedAt:     now.UTC().Format(time.RFC3339),
		State:          cfg.Tracker.HandoffState,
		PRNumber:       pr.Number,
		PRRepository:   pr.Owner + "/" + pr.Repo,
		ReleaseHead:    record.Head,
		ReleaseComment: strings.Join(record.Comments, ","),
	}, log)
	identity := park.IssueIdentity
	identity.State = cfg.Tracker.HandoffState
	seeded := o.seedRecordWatches(issueID, park, identity, pr, now)
	recordReactionActivity(ctx, o.store, issueID, park.StageLineage, persistence.ReactionActivityWatch, seeded, now, log)
	log.Info("park released by its pull request record",
		slog.String("pull_request", record.PullRequest),
		slog.String("head_sha", record.Head),
		slog.String("comments", strings.Join(record.Comments, ",")),
		slog.String("handoff_state", cfg.Tracker.HandoffState),
		slog.Any("watches", seeded),
	)
	o.notifyObservers()
}

// seedRecordWatches seeds the watches a passed review's exit seeds for the
// pull request at its head, for each kind the workflow configures: CI,
// merge conflicts, auto-merge, and merge completion. It replaces no entry
// that already watches the pull request, and returns the kinds it seeded.
func (o *Orchestrator) seedRecordWatches(issueID string, park *ParkedEntry, identity IssueIdentity, pr pullRequestRef, now time.Time) []string {
	rule := criteriaRuleName(o.workflowManager.Config().Dispatch, park.RuleName)
	var seeded []string
	seed := func(kind string, data any) {
		key := ReactionKey(issueID, kind)
		if _, exists := o.state.PendingReactions[key]; exists {
			return
		}
		o.state.PendingReactions[key] = &PendingReaction{
			IssueID:         issueID,
			Identifier:      park.Identifier,
			DisplayID:       park.DisplayID,
			Attempt:         1,
			Kind:            kind,
			CreatedAt:       now,
			KindData:        data,
			AgentKind:       park.AgentKind,
			ConfiguredModel: park.ConfiguredModel,
			RuleName:        rule,
			StageLineage:    park.StageLineage,
			TemplateID:      park.TemplateID,
			IssueIdentity:   identity,
		}
		seeded = append(seeded, kind)
	}
	if o.ciProvider != nil && pr.Branch != "" {
		seed(ReactionKindCI, &CIReactionData{PRNumber: pr.Number, Owner: pr.Owner, Repo: pr.Repo, Branch: pr.Branch, SHA: pr.HeadSHA})
	}
	if o.mergeConflictReactionConfigured && pr.Branch != "" {
		seed(ReactionKindMergeConflict, &MergeConflictReactionData{PRNumber: pr.Number, Owner: pr.Owner, Repo: pr.Repo, Branch: pr.Branch, SHA: pr.HeadSHA})
	}
	seed(ReactionKindAutoMerge, &AutoMergeReactionData{PRNumber: pr.Number, Owner: pr.Owner, Repo: pr.Repo, Branch: pr.Branch, SHA: pr.HeadSHA})
	if o.mergeCompletionReactionConfigured {
		seed(ReactionKindMergeCompletion, &MergeCompletionReactionData{PRNumber: pr.Number, Owner: pr.Owner, Repo: pr.Repo})
	}
	return seeded
}

// criteriaRuleName returns the name of the rule whose criteria gate the
// watches of a released park answer to: current when it carries the gate,
// else the first rule that does.
func criteriaRuleName(dispatch config.DispatchConfig, current string) string {
	if r, ok := dispatch.RuleByName(current); ok && r.Gate == config.GateCriteria {
		return current
	}
	for _, r := range dispatch.Rules {
		if r.Gate == config.GateCriteria {
			return r.Name
		}
	}
	return current
}

// SnapshotParkRelease is a read-only view of one released park whose next
// primary dispatch has not yet consumed it, produced by [RuntimeSnapshot].
// PullRequest, Head, and Comments name the pull request record that
// released it, gesture "record", and are empty for every other gesture.
type SnapshotParkRelease struct {
	IssueID     string    `json:"issue_id"`
	Identifier  string    `json:"issue_identifier"`
	DisplayID   string    `json:"display_identifier,omitempty"`
	Reason      string    `json:"reason"`
	ParkedAt    time.Time `json:"parked_at"`
	ReleasedAt  time.Time `json:"released_at"`
	Gesture     string    `json:"gesture"`
	PullRequest string    `json:"pull_request,omitempty"`
	Head        string    `json:"head,omitempty"`
	Comments    []string  `json:"comments,omitempty"`
}

// snapshotParkReleases returns the view of each pending park release,
// sorted by identifier.
func snapshotParkReleases(state *State) []SnapshotParkRelease {
	out := make([]SnapshotParkRelease, 0, len(state.ParkReleases))
	for id, e := range state.ParkReleases {
		row := SnapshotParkRelease{
			IssueID: id, Identifier: e.Identifier, DisplayID: e.DisplayID, Reason: e.Reason,
			ParkedAt: e.ParkedAt, ReleasedAt: e.ReleasedAt, Gesture: e.ReleaseGesture,
		}
		if r := e.Record; r != nil {
			row.PullRequest, row.Head, row.Comments = r.PullRequest, r.Head, slices.Clone(r.Comments)
		}
		out = append(out, row)
	}
	slices.SortFunc(out, func(a, b SnapshotParkRelease) int {
		return cmp.Or(cmp.Compare(a.Identifier, b.Identifier), cmp.Compare(a.IssueID, b.IssueID))
	})
	return out
}
