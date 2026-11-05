package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/logging"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// revertScanLookback is how far before the oldest merge record of a
// repository the first scan after a start reads merged pull requests, so a
// revert merged before its story's merge was recorded is still found.
const revertScanLookback = 7 * 24 * time.Hour

// revertScanOverlap is how far before the previous scan's start the next
// one reads, so a pull request that merged while the scan ran is read.
const revertScanOverlap = 10 * time.Minute

// planMergeLister lists the merge records.
type planMergeLister interface {
	ListPlanSatisfaction(ctx context.Context, kind string) ([]persistence.PlanSatisfaction, error)
}

// revertedMerge is one story's merge record, or merge-and-prove's merge
// of a story not yet proven, that a revert may name.
type revertedMerge struct {
	issueID     string
	pullRequest pullRequestRef // zero for a record without its pull request
	commit      string
	inFlight    *MergeAndProveEntry
}

// detectReverts reads, on every poll tick, the merged pull requests of
// each repository Sortie holds a merge record of, and undoes each record a
// merged revert names, whoever merged it: the record is deleted, so the
// stories it blocks wait for it again, and the story leaves its done
// state. A story merge-and-prove merged and has not yet proven ends as
// reverted. Runs on the event loop.
func (o *Orchestrator) detectReverts(ctx context.Context, cfg config.ServiceConfig, now time.Time) {
	if o.revertFinder == nil || o.once != nil {
		return
	}
	lister, ok := o.store.(planMergeLister)
	if !ok {
		return
	}
	lctx, cancel := context.WithTimeout(ctx, mergeAndProveBound)
	rows, err := lister.ListPlanSatisfaction(lctx, persistence.PlanSatisfactionMerged)
	cancel()
	if err != nil {
		o.logger.Warn("merge records unreadable, no revert scan this tick", slog.Any("error", err))
		return
	}
	merges := make([]revertedMerge, 0, len(rows))
	oldest := map[string]time.Time{}
	note := func(repo string, at time.Time) {
		if first, seen := oldest[repo]; !seen || at.Before(first) {
			oldest[repo] = at
		}
	}
	for _, r := range rows {
		m := revertedMerge{issueID: r.IssueID, commit: strings.ToLower(r.Ref)}
		if pr, ok := parsePullRequest(r.PR); ok {
			m.pullRequest = pr
			note(strings.ToLower(pr.Owner+"/"+pr.Repo), r.RecordedAt)
		}
		merges = append(merges, m)
	}
	if e := mergeAndProveInFlight(o.state); e != nil && e.MergeCommit != "" {
		merges = append(merges, revertedMerge{issueID: e.IssueID, pullRequest: e.PullRequest, commit: e.MergeCommit, inFlight: e})
		note(strings.ToLower(e.PullRequest.Owner+"/"+e.PullRequest.Repo), e.PhaseAt)
	}
	if len(merges) == 0 {
		return
	}
	// A record kept without its pull request is matched by its commit in
	// every repository the workflow dispatches.
	earliest := now
	for _, r := range rows {
		if r.RecordedAt.Before(earliest) && !r.RecordedAt.IsZero() {
			earliest = r.RecordedAt
		}
	}
	for _, project := range repositoryProjects(cfg) {
		note(strings.ToLower(project), earliest)
	}
	for _, repo := range slices.Sorted(maps.Keys(oldest)) {
		owner, name, ok := splitProject(repo)
		if !ok {
			continue
		}
		since, scanned := o.state.RevertScans[repo]
		if !scanned {
			since = oldest[repo].Add(-revertScanLookback)
		}
		fctx, fcancel := context.WithTimeout(ctx, mergeAndProveBound)
		reverts, err := o.revertFinder.FindMergedReverts(fctx, owner, name, since)
		fcancel()
		if err != nil {
			o.logger.Warn("merged reverts unreadable", slog.String("repository", repo), slog.Any("error", err))
			continue
		}
		handled := true
		for _, rv := range reverts {
			for i := range merges {
				m := &merges[i]
				if m.issueID == "" || !revertNames(rv, owner, name, *m) {
					continue
				}
				if !o.undoRevertedMerge(ctx, cfg, *m, pullRequestRef{Number: rv.Number, Owner: owner, Repo: name}) {
					handled = false
				}
				m.issueID = ""
			}
		}
		if handled {
			o.state.RevertScans[repo] = now.Add(-revertScanOverlap)
		}
	}
}

// revertNames reports whether the merged revert rv of owner/repo undoes m:
// it names m's pull request, or m's merge commit, which a record without
// its pull request matches in any repository.
func revertNames(rv domain.MergedRevert, owner, repo string, m revertedMerge) bool {
	sameRepo := m.pullRequest.Number > 0 && strings.EqualFold(m.pullRequest.Owner, owner) && strings.EqualFold(m.pullRequest.Repo, repo)
	if sameRepo && slices.Contains(rv.PullRequests, m.pullRequest.Number) {
		return true
	}
	if !sameRepo && m.pullRequest.Number > 0 {
		return false
	}
	for _, c := range rv.Commits {
		if len(c) >= minClickProofsSHA && m.commit != "" && strings.HasPrefix(m.commit, strings.ToLower(c)) {
			return true
		}
	}
	return false
}

// undoRevertedMerge ends m's story as reverted by revert. It reports
// whether that is done, false when a write failed and the next scan must
// see the revert again.
func (o *Orchestrator) undoRevertedMerge(ctx context.Context, cfg config.ServiceConfig, m revertedMerge, revert pullRequestRef) bool {
	if e := m.inFlight; e != nil {
		if o.state.MergeAndProve[e.IssueID] != e {
			return true
		}
		bySortie := e.RevertPR == revert.Number
		if e.Reason == "" {
			e.Reason = "the merge was reverted by " + formatPullRequest(revert) + " before its live proof after deploy was proven"
		}
		o.finishRevert(ctx, cfg, e, formatPullRequest(revert), bySortie)
		return o.state.MergeAndProve[e.IssueID] != e
	}
	identifier := m.issueID
	if e := o.state.MergeAndProve[m.issueID]; e != nil {
		identifier = e.Identifier
	}
	log := logging.WithIssue(o.logger, m.issueID, identifier)
	merged := "its merge"
	if m.pullRequest.Number > 0 {
		merged = "its merge of " + formatPullRequest(m.pullRequest)
	}
	if m.commit != "" {
		merged += " (" + m.commit + ")"
	}
	moved, err := o.unsatisfyStory(ctx, cfg, m.issueID, false)
	if err != nil {
		log.Warn("reverted story could not be undone, trying again on the next scan", slog.String("revert", formatPullRequest(revert)), slog.Any("error", err))
		return false
	}
	reason := fmt.Sprintf("%s merged, which reverts %s", formatPullRequest(revert), merged)
	log.Warn("story's merge reverted on the forge; the story is not done",
		slog.String("revert", formatPullRequest(revert)), slog.String("merge_commit", m.commit), slog.String("state", moved))
	if o.trackerAdapter != nil {
		body := "Sortie found " + reason + ". The story is not done: Sortie removed its merge record, so the stories it blocks wait for it again"
		if moved != "" {
			body += ", and moved it to " + moved
		}
		wctx, cancel := context.WithTimeout(ctx, planControlWriteBound)
		if err := o.trackerAdapter.CommentIssue(wctx, m.issueID, body+"."); err != nil {
			log.Warn("reverted story comment failed", slog.Any("error", err))
		}
		cancel()
	}
	pr := ""
	if m.pullRequest.Number > 0 {
		pr = formatPullRequest(m.pullRequest)
	}
	o.publishStoryReverted(ctx, m.issueID, identifier, domain.StoryRevertedFacts{
		PullRequest: pr, MergeCommit: m.commit, RevertPullRequest: formatPullRequest(revert), Reason: reason, State: moved,
	}, log)
	o.notifyObservers()
	return true
}

// repositoryProjects returns the owner/repo of each repository the
// workflow dispatches.
func repositoryProjects(cfg config.ServiceConfig) []string {
	if !configuresRepository(cfg.Environment) {
		return nil
	}
	var out []string
	for _, r := range cfg.Environment.RepositorySet().AllRepositories() {
		if _, _, ok := splitProject(r.Project); ok {
			out = append(out, r.Project)
		}
	}
	return out
}
