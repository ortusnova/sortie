package orchestrator

import (
	"cmp"
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

// revertScanSchedule is when the revert scan runs. Only the event loop
// reads or writes it.
type revertScanSchedule struct {
	next time.Time // no scan starts before it
}

// revertedMerge is one story's merge record, or merge-and-prove's merge
// of a story not yet proven, that a revert may name.
type revertedMerge struct {
	issueID     string
	pullRequest pullRequestRef // zero for a record without its pull request
	commit      string
	inFlight    *MergeAndProveEntry // read only on the event loop
}

// revertScanRepo is one repository a revert scan reads, from since.
type revertScanRepo struct {
	key, owner, name string
	since            time.Time
}

// revertUndo is one merge a merged revert names, with what the scan did
// on the tracker: the state it moved the story to, or why it could not.
type revertUndo struct {
	repo   string
	merge  revertedMerge
	revert pullRequestRef
	moved  string
	err    error
}

// revertScanResult is what a scan found off the loop: each merge undone on
// the tracker, and each repository read without an error.
type revertScanResult struct {
	undos []revertUndo
	read  map[string]bool
}

// revertScanEvery is how often the revert scan runs.
func revertScanEvery(cfg config.ServiceConfig) time.Duration {
	return cmp.Or(cfg.MergeAndProve.RevertScanInterval, config.DefaultRevertScanInterval)
}

// detectReverts starts, once per merge_and_prove.revert_scan_interval and
// never while one is in flight, a scan of the merged pull requests of each
// repository Sortie holds a merge record of. The scan runs off the event
// loop: it reads the forge, and for each record a merged revert names,
// whoever merged it, moves the story out of its done state on the
// tracker. The loop then deletes the record, so the stories it blocks wait
// for it again; a story merge-and-prove merged and has not yet proven ends
// as reverted. Must run on the event loop.
func (o *Orchestrator) detectReverts(ctx context.Context, cfg config.ServiceConfig, now time.Time) {
	if o.revertFinder == nil || o.once != nil || now.Before(o.revertScan.next) || o.offLoopRunner().inFlight(revertScanSubject) {
		return
	}
	merges, repos, ok := o.planRevertScan(ctx, cfg, now)
	if !ok {
		return
	}
	o.revertScan.next = now.Add(revertScanEvery(cfg))
	if len(merges) == 0 {
		return
	}
	finder, tracker := o.revertFinder, o.trackerAdapter
	o.startOffLoop(ctx, offLoopCall{
		Subject: revertScanSubject,
		Kind:    revertScanCallKind,
		Timeout: revertScanTimeout(len(repos), len(merges)),
		Run: func(ctx context.Context) func(context.Context) {
			found := scanReverts(ctx, cfg, finder, tracker, slices.Clone(merges), repos, o.logger)
			return func(loopCtx context.Context) { o.applyRevertScan(loopCtx, now, found) }
		},
	})
}

// revertScanCallKind names the revert scan on the off-loop runner.
const revertScanCallKind = "revert_scan"

// revertScanSubject is the subject of the revert scan, one at a time.
var revertScanSubject = offLoopSubject{Kind: offLoopCheck, Key: revertScanCallKind}

// revertScanTimeout bounds a scan of repos repositories for merges: each
// forge read and each tracker move has its own bound.
func revertScanTimeout(repos, merges int) time.Duration {
	return time.Duration(repos+merges) * mergeAndProveBound
}

// planRevertScan reads the merge records and returns the merges a revert
// may name and the repositories to read, each from its cursor. Must run on
// the event loop.
func (o *Orchestrator) planRevertScan(ctx context.Context, cfg config.ServiceConfig, now time.Time) ([]revertedMerge, []revertScanRepo, bool) {
	lister, ok := o.store.(planMergeLister)
	if !ok {
		return nil, nil, false
	}
	lctx, cancel := context.WithTimeout(ctx, mergeAndProveBound)
	rows, err := lister.ListPlanSatisfaction(lctx, persistence.PlanSatisfactionMerged)
	cancel()
	if err != nil {
		o.logger.Warn("merge records unreadable, no revert scan this tick", slog.Any("error", err))
		return nil, nil, false
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
		return nil, nil, true
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
	repos := make([]revertScanRepo, 0, len(oldest))
	for _, repo := range slices.Sorted(maps.Keys(oldest)) {
		owner, name, ok := splitProject(repo)
		if !ok {
			continue
		}
		since, scanned := o.state.RevertScans[repo]
		if !scanned {
			since = oldest[repo].Add(-revertScanLookback)
		}
		repos = append(repos, revertScanRepo{key: repo, owner: owner, name: name, since: since})
	}
	return merges, repos, true
}

// scanReverts reads each repository's merged reverts and, for each merge
// one names, moves its story out of its done state on the tracker. It
// reads only its arguments, so it runs off the event loop.
func scanReverts(ctx context.Context, cfg config.ServiceConfig, finder domain.RevertFinder, tracker domain.TrackerAdapter,
	merges []revertedMerge, repos []revertScanRepo, log *slog.Logger) revertScanResult {
	found := revertScanResult{read: map[string]bool{}}
	for _, repo := range repos {
		fctx, fcancel := context.WithTimeout(ctx, mergeAndProveBound)
		reverts, err := finder.FindMergedReverts(fctx, repo.owner, repo.name, repo.since)
		fcancel()
		if err != nil {
			log.Warn("merged reverts unreadable", slog.String("repository", repo.key), slog.Any("error", err))
			continue
		}
		found.read[repo.key] = true
		for _, rv := range reverts {
			for i := range merges {
				m := &merges[i]
				if m.issueID == "" || !revertNames(rv, repo.owner, repo.name, *m) {
					continue
				}
				u := revertUndo{repo: repo.key, merge: *m, revert: pullRequestRef{Number: rv.Number, Owner: repo.owner, Repo: repo.name}}
				u.moved, u.err = releaseStoryOnTracker(ctx, cfg, tracker, m.issueID, m.inFlight != nil)
				found.undos = append(found.undos, u)
				m.issueID = ""
			}
		}
	}
	return found
}

// applyRevertScan applies a scan started at now: it ends each undone
// merge, and moves the cursor of each repository whose reverts were all
// handled. Must run on the event loop.
func (o *Orchestrator) applyRevertScan(ctx context.Context, now time.Time, found revertScanResult) {
	handled := found.read
	for _, u := range found.undos {
		if !o.undoRevertedMerge(ctx, u) {
			handled[u.repo] = false
		}
	}
	for repo, ok := range handled {
		if ok {
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

// undoRevertedMerge ends u's story as reverted by its revert, once the
// scan moved it on the tracker. It reports whether that is done, false
// when a step failed and the next scan must see the revert again. Must run
// on the event loop; its comment is written detached.
func (o *Orchestrator) undoRevertedMerge(ctx context.Context, u revertUndo) bool {
	m, revert := u.merge, u.revert
	if e := m.inFlight; e != nil {
		if o.state.MergeAndProve[e.IssueID] != e {
			// It ended while the scan ran: read it again, as a record if
			// it was proven meanwhile.
			return false
		}
		if e.Reason == "" {
			e.Reason = "the merge was reverted by " + formatPullRequest(revert) + " before its live proof after deploy was proven"
		}
		err := u.err
		if err == nil {
			err = o.deleteMergeRecord(ctx, e.IssueID)
		}
		o.endReverted(ctx, e, formatPullRequest(revert), e.RevertPR == revert.Number, u.moved, err)
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
	err := u.err
	if err == nil {
		err = o.deleteMergeRecord(ctx, m.issueID)
	}
	if err != nil {
		log.Warn("reverted story could not be undone, trying again on the next scan", slog.String("revert", formatPullRequest(revert)), slog.Any("error", err))
		return false
	}
	moved := u.moved
	reason := fmt.Sprintf("%s merged, which reverts %s", formatPullRequest(revert), merged)
	log.Warn("story's merge reverted on the forge; the story is not done",
		slog.String("revert", formatPullRequest(revert)), slog.String("merge_commit", m.commit), slog.String("state", moved))
	body := "Sortie found " + reason + ". The story is not done: Sortie removed its merge record, so the stories it blocks wait for it again"
	if moved != "" {
		body += ", and moved it to " + moved
	}
	o.commentDetached(ctx, m.issueID, body+".", log)
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
