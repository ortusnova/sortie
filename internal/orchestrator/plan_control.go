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
	"github.com/sortie-ai/sortie/internal/logging"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/redact"
	"github.com/sortie-ai/sortie/internal/workspace"
)

// dispatchBlockTTL bounds how long a dispatch block waits for the tracker
// to confirm a pull-back or cancel, so a tracker that never shows the move
// cannot hold an issue forever.
const dispatchBlockTTL = 10 * time.Minute

// planControlWriteBound bounds the tracker write a pull-back or cancel
// makes on the event loop, inside the control reply bound.
const planControlWriteBound = 4 * time.Second

// DispatchBlock keeps an issue out of dispatch after a plan pull-back or
// cancel moved it to Target, until a poll no longer reports it active or
// the block expires.
type DispatchBlock struct {
	Target  string
	Since   time.Time
	Expires time.Time
	Reason  string // "pull_back" or "cancel"
}

// clearDispatchBlocks drops each block whose issue the candidate read no
// longer reports, because the tracker then confirms it left the active
// states, and each block that expired. It runs on the event loop after
// every candidate fetch.
func clearDispatchBlocks(state *State, candidates []domain.Issue, now time.Time, log *slog.Logger) {
	if len(state.DispatchBlocks) == 0 {
		return
	}
	active := make(map[string]bool, len(candidates))
	for _, c := range candidates {
		active[c.ID] = true
	}
	for id, b := range state.DispatchBlocks {
		switch {
		case !active[id]:
			delete(state.DispatchBlocks, id)
		case now.After(b.Expires):
			delete(state.DispatchBlocks, id)
			log.Warn("plan dispatch block expired while the tracker still reports the issue active",
				slog.String("issue_id", id),
				slog.String("target_state", b.Target),
				slog.String("reason", b.Reason),
			)
		}
	}
}

// Plan control outcomes beyond the generic ones.
const (
	// ControlAlreadyClaimed means a worker claimed the issue first.
	ControlAlreadyClaimed ControlOutcome = iota + 100
	// ControlNotReleased means the issue is not in the release state.
	ControlNotReleased
	// ControlAlreadyTerminal means the issue already reached another
	// terminal state.
	ControlAlreadyTerminal
	// ControlPlanOff means plan control is not configured.
	ControlPlanOff
	// ControlTrackerFailed means the tracker write failed.
	ControlTrackerFailed
)

// planTarget is a plan story resolved, outside the event loop, from its
// plan key.
type planTarget struct {
	IssueID    string
	Identifier string
	State      string
}

// resolvePlanKey finds the issue a plan key names through the tracker.
// It runs on the caller's goroutine, never on the event loop.
func (o *Orchestrator) resolvePlanKey(ctx context.Context, planKey string) (planTarget, ControlReply, bool) {
	cfg := o.workflowManager.Config()
	if !cfg.Tracker.PlanControl() {
		return planTarget{}, ControlReply{Outcome: ControlPlanOff, Message: "plan control is off: tracker.release_state is not set"}, false
	}
	finder, ok := o.trackerAdapter.(domain.PlanKeyFinder)
	if !ok {
		return planTarget{}, ControlReply{Outcome: ControlPlanOff, Message: "the tracker cannot find issues by plan key"}, false
	}
	pi, err := finder.FindIssueByPlanKey(ctx, planKey)
	if err != nil {
		if te, ok := errors.AsType[*domain.TrackerError](err); ok && te.Kind == domain.ErrTrackerNotFound {
			return planTarget{}, ControlReply{Outcome: ControlUnknownIssue, Message: fmt.Sprintf("no issue carries plan key %q", planKey)}, false
		}
		return planTarget{}, ControlReply{Outcome: ControlTrackerFailed, Message: err.Error()}, false
	}
	return planTarget{IssueID: pi.ID, Identifier: pi.Identifier, State: pi.State}, ControlReply{}, true
}

// PullBackFunc returns the function the HTTP server calls to pull a
// released plan story back. Safe to call from any goroutine.
func (o *Orchestrator) PullBackFunc() func(ctx context.Context, planKey string) ControlReply {
	return func(ctx context.Context, planKey string) ControlReply {
		target, reply, ok := o.resolvePlanKey(ctx, planKey)
		if !ok {
			return reply
		}
		return o.sendControl(controlRequest{Kind: controlPullBack, Identifier: target.Identifier, Plan: target})
	}
}

// PlanCancelFunc returns the function the HTTP server calls to cancel a
// plan story. Safe to call from any goroutine.
func (o *Orchestrator) PlanCancelFunc() func(ctx context.Context, planKey string) ControlReply {
	return func(ctx context.Context, planKey string) ControlReply {
		target, reply, ok := o.resolvePlanKey(ctx, planKey)
		if !ok {
			return reply
		}
		return o.sendControl(controlRequest{Kind: controlCancel, Identifier: target.Identifier, Plan: target})
	}
}

// planControlParams carries what pull-back and cancel need on the loop.
type planControlParams struct {
	Release, Backlog, Cancel string
	Terminal                 []string
	Tracker                  domain.TrackerAdapter
	Store                    retryEntryDeleter
	Metrics                  domain.Metrics
	Now                      time.Time
	Log                      *slog.Logger
}

// workerHolds reports whether a worker runs, retries, or claims the issue.
func workerHolds(state *State, issueID string) bool {
	_, running := state.Running[issueID]
	_, claimed := state.Claimed[issueID]
	_, retrying := state.RetryAttempts[issueID]
	return running || claimed || retrying
}

// applyPullBack moves a released story back to the backlog state, on the
// event loop, only while no worker holds it. The claim check and the block
// happen before the write, with no tick between them.
func applyPullBack(ctx context.Context, state *State, t planTarget, p planControlParams) ControlReply {
	log := logging.WithIssue(p.Log, t.IssueID, t.Identifier)
	if workerHolds(state, t.IssueID) {
		return ControlReply{Outcome: ControlAlreadyClaimed, Message: fmt.Sprintf("a worker already claimed %s", t.Identifier)}
	}
	if !strings.EqualFold(t.State, p.Release) {
		return ControlReply{Outcome: ControlNotReleased, Message: fmt.Sprintf("%s is in %q, not in the release state %q", t.Identifier, t.State, p.Release)}
	}
	state.DispatchBlocks[t.IssueID] = DispatchBlock{Target: p.Backlog, Since: p.Now, Expires: p.Now.Add(dispatchBlockTTL), Reason: "pull_back"}
	wctx, cancel := context.WithTimeout(ctx, planControlWriteBound)
	defer cancel()
	if err := p.Tracker.TransitionIssue(wctx, t.IssueID, p.Backlog); err != nil {
		delete(state.DispatchBlocks, t.IssueID)
		log.Warn("plan pull-back failed", slog.Any("error", err))
		return ControlReply{Outcome: ControlTrackerFailed, Message: err.Error()}
	}
	log.Info("plan story pulled back", slog.String("state", p.Backlog))
	return ControlReply{Outcome: ControlDone}
}

// applyCancel stops a plan story's worker, if one runs, and moves the issue
// to the cancel state, on the event loop. The cancel state is terminal, so
// neither reconcile nor retry starts the issue again.
func applyCancel(ctx context.Context, state *State, t planTarget, p planControlParams) ControlReply {
	log := logging.WithIssue(p.Log, t.IssueID, t.Identifier)
	if strings.EqualFold(t.State, p.Cancel) {
		return ControlReply{Outcome: ControlDone, Message: "already cancelled"}
	}
	if _, terminal := stateSet(p.Terminal)[strings.ToLower(t.State)]; terminal {
		return ControlReply{Outcome: ControlAlreadyTerminal, Message: fmt.Sprintf("%s is already in the terminal state %q", t.Identifier, t.State)}
	}
	if entry, running := state.Running[t.IssueID]; running {
		if !entry.PendingCleanup {
			if entry.CancelFunc != nil {
				entry.CancelFunc()
			}
			endHoldRecorded(entry, p.Now, p.Metrics)
			entry.PendingCleanup = true
			entry.ObservedTerminalState = p.Cancel
		}
		log.Info("stopping worker for cancelled plan story")
	}
	releaseIssueRuntimeState(ctx, state, p.Store, t.IssueID, log)
	state.DispatchBlocks[t.IssueID] = DispatchBlock{Target: p.Cancel, Since: p.Now, Expires: p.Now.Add(dispatchBlockTTL), Reason: "cancel"}

	wctx, cancel := context.WithTimeout(ctx, planControlWriteBound)
	defer cancel()
	if err := p.Tracker.TransitionIssue(wctx, t.IssueID, p.Cancel); err != nil {
		// The worker is stopped either way. The block stays, so the issue
		// is not dispatched again before the cancel is retried.
		log.Warn("plan cancel could not move the issue", slog.Any("error", err))
		return ControlReply{Outcome: ControlTrackerFailed, Message: err.Error()}
	}
	log.Info("plan story cancelled", slog.String("state", p.Cancel))
	return ControlReply{Outcome: ControlDone}
}

// ReviewGateFunc decides whether auto-merge may merge a pending entry's
// pull request. It reports why not, or an error when it could not tell.
type ReviewGateFunc func(ctx context.Context, pending *PendingReaction) (allowed bool, reason string, err error)

// planReviewGate returns the auto-merge gate of plan control, or nil when
// plan control is off. Under plan control Sortie merges only a plan story,
// one that carries a review label, and only after a run of a criteria rule.
// The workflow's rules decide which stories reach a criteria rule: a human
// story whose chain ends in none is merged by a person in the forge, and a
// workflow that routes every story through the check lets Sortie merge
// either kind.
func planReviewGate(cfg config.ServiceConfig, tracker domain.TrackerAdapter) ReviewGateFunc {
	if !cfg.Tracker.PlanControl() {
		return nil
	}
	dispatch := cfg.Dispatch
	return func(ctx context.Context, pending *PendingReaction) (bool, string, error) {
		rule, ok := dispatch.RuleByName(pending.RuleName)
		if !ok || rule.Gate != config.GateCriteria {
			return false, "the run that opened the pull request was not a criteria check", nil
		}
		issue, err := tracker.FetchIssueByID(ctx, pending.IssueID)
		if err != nil {
			return false, "", err
		}
		for _, l := range issue.Labels {
			if strings.EqualFold(l, config.ReviewCriteriaLabel) || strings.EqualFold(l, config.ReviewHumanLabel) {
				return true, "", nil
			}
		}
		return false, "the story carries neither " + config.ReviewCriteriaLabel + " nor " + config.ReviewHumanLabel, nil
	}
}

// criteriaCheckFailed reports whether the run was a criteria check, under
// plan control, that ended blocked or asking for human review: a refusal.
func criteriaCheckFailed(params HandleWorkerExitParams, entry *RunningEntry, result WorkerResult) bool {
	if params.PlanReleaseState == "" || entry == nil || !result.SoftStop {
		return false
	}
	if result.SoftStopReason != string(workspace.StatusBlocked) && result.SoftStopReason != string(workspace.StatusNeedsHumanReview) {
		return false
	}
	rule, ok := params.Dispatch.RuleByName(entry.RuleName)
	return ok && rule.Gate == config.GateCriteria
}

// maxCriteriaSummary bounds the agent text a failed check posts.
const maxCriteriaSummary = 4000

// handleCriteriaFailure reports a failed criteria check on the story and
// either sends it back to its rework stage (on_fail names a rule) or
// escalates it to people. The pull request is not merged: the exit seeds
// no reaction for it.
func handleCriteriaFailure(ctx context.Context, state *State, params HandleWorkerExitParams, entry *RunningEntry, result WorkerResult, log *slog.Logger) {
	rule, _ := params.Dispatch.RuleByName(entry.RuleName)
	summary := strings.TrimSpace(redact.Mask(entry.LastAgentMessage))
	if len(summary) > maxCriteriaSummary {
		summary = summary[:maxCriteriaSummary] + "…"
	}
	if summary == "" {
		summary = "(the check left no summary)"
	}
	comment := fmt.Sprintf("The acceptance-criteria check (rule %s) did not pass, so the pull request is not merged.\n\n%s", rule.Name, summary)
	if params.TrackerAdapter != nil {
		if err := params.TrackerAdapter.CommentIssue(ctx, result.IssueID, comment); err != nil {
			log.Warn("criteria failure comment failed", slog.Any("error", err))
		}
	}

	if rule.OnFail != "" && rule.OnFail != config.OnFailEscalate {
		target, ok := params.Dispatch.RuleByName(rule.OnFail)
		if ok && params.TrackerAdapter != nil {
			if err := params.TrackerAdapter.AddLabel(ctx, result.IssueID, target.Stage); err != nil {
				log.Warn("criteria failure could not add the rework stage label", slog.String("label", target.Stage), slog.Any("error", err))
			} else {
				if rule.Stage != "" {
					_ = params.TrackerAdapter.RemoveLabel(ctx, result.IssueID, rule.Stage)
				}
				if err := params.TrackerAdapter.TransitionIssue(ctx, result.IssueID, params.PlanReleaseState); err != nil {
					log.Warn("criteria failure could not return the story to the release state", slog.Any("error", err))
				}
				log.Info("criteria check failed, story sent back for rework", slog.String("on_fail", target.Name))
				return
			}
		}
	}

	n := sessionEvent{
		IssueID:    result.IssueID,
		Identifier: entry.Identifier,
		DisplayID:  entry.Issue.DisplayID,
		DispatchID: entry.DispatchID,
		SessionID:  entry.SessionID,
		Attempt:    entry.RetryAttempt,
		Agent:      entry.AgentKind,
		AgentText:  summary,
	}.notification(domain.EventSessionEscalated, "", domain.SessionEscalatedFacts{Kind: domain.SessionEscalationCriteriaFailed, Rule: rule.Name})
	n.Envelope.Escalation = domain.SessionEscalation{Kind: domain.SessionEscalationCriteriaFailed, Rule: rule.Name}
	if params.Router == nil {
		log.Info("criteria check failed, escalation had no router")
		return
	}
	delivery := params.Router.Route(n)
	if delivery.Empty() {
		log.Info("criteria check failed, escalation had no destination")
		return
	}
	log.Info("criteria check failed, escalated")
	deliverDetached(ctx, &state.TrackerOpsWg, delivery, log, func(received bool, err error) {
		if received && err != nil {
			log.Warn("criteria failure escalation failed", slog.Any("error", err))
		}
	})
}

// warnCriteriaNeedsReview warns once per story when a criteria story that
// passed its check waits for a review the forge requires: branch protection
// that requires an approving review keeps auto-merge from ever merging it,
// and such repositories should use review: human.
func warnCriteriaNeedsReview(state *State, params ReconcileParams, pending *PendingReaction, decision domain.ReviewDecision, prNumber int, log *slog.Logger) {
	if params.ReviewGate == nil || decision != domain.ReviewDecisionReviewRequired {
		return
	}
	if _, logged := state.CriteriaReviewWarned[pending.IssueID]; logged {
		return
	}
	state.CriteriaReviewWarned[pending.IssueID] = struct{}{}
	log.Warn("criteria story waits for a review the forge requires: branch protection requires an approving review, so auto-merge cannot merge it; use review: human for this repository",
		slog.Int("pr_number", prNumber),
	)
}

// NoChangeVerdict is what the worker found when it checked a no-change
// claim's citation. Zero when no claim was checked.
type NoChangeVerdict struct {
	// Checked is true when a plan-control run ended no-change-needed.
	Checked bool
	// Ref is commit:<sha> or plan:<key> for a confirmed claim.
	Ref string
	// Cites is the issue id of a cited plan story.
	Cites string
	// Unverified is the reason the claim satisfies nothing, empty when
	// it is confirmed.
	Unverified string
}

// commitConfirmer is the capability of a repository to tell whether a
// commit is on its base branch, as gitsync's mirror does.
type commitConfirmer interface {
	CommitOnBase(ctx context.Context, commit string) (bool, error)
}

// Reasons a no-change claim is unverified, beyond the file's own.
const (
	NoChangeNoMirror         = "no_mirror"
	NoChangeCommitNotOnBase  = "commit_not_on_base"
	NoChangeUnknownPlanKey   = "unknown_plan_key"
	NoChangeCycle            = "cycle"
	NoChangeCheckFailed      = "check_failed"
	noChangeConfirmTimeLimit = 30 * time.Second
)

// confirmNoChange reads the claim's evidence from the workspace and
// confirms its citation on the host: a commit against the repository's
// mirror, a plan key against the tracker. It runs on the worker goroutine,
// never on the event loop, and never runs git in the workspace.
func confirmNoChange(ctx context.Context, tracker domain.TrackerAdapter, repo any, wsPath, issueID string, log *slog.Logger) NoChangeVerdict {
	v := NoChangeVerdict{Checked: true}
	ev, why := workspace.ReadEvidenceFile(wsPath, log)
	if why != "" {
		v.Unverified = why
		return v
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), noChangeConfirmTimeLimit)
	defer cancel()
	switch ev.Kind {
	case workspace.EvidenceCommit:
		confirmer, ok := repo.(commitConfirmer)
		if !ok || repo == nil {
			v.Unverified = NoChangeNoMirror
			return v
		}
		onBase, err := confirmer.CommitOnBase(ctx, ev.SHA)
		switch {
		case err != nil:
			log.Warn("no-change commit check failed", slog.Any("error", err))
			v.Unverified = NoChangeCheckFailed
		case !onBase:
			v.Unverified = NoChangeCommitNotOnBase
		default:
			v.Ref = "commit:" + ev.SHA
		}
	case workspace.EvidencePlanKey:
		finder, ok := tracker.(domain.PlanKeyFinder)
		if !ok {
			v.Unverified = NoChangeUnknownPlanKey
			return v
		}
		cited, err := finder.FindIssueByPlanKey(ctx, ev.PlanKey)
		switch {
		case err != nil:
			v.Unverified = NoChangeUnknownPlanKey
		case cited.ID == issueID:
			v.Unverified = NoChangeCycle
		default:
			v.Ref, v.Cites = "plan:"+ev.PlanKey, cited.ID
		}
	}
	return v
}

// recordNoChange records a checked claim on the event loop: confirmed, as
// satisfaction, or unverified, with its reason. A plan-key claim whose
// cited story itself cites this one is a cycle and satisfies nothing.
func recordNoChange(ctx context.Context, store any, issueID string, v NoChangeVerdict, log *slog.Logger) {
	if !v.Checked {
		return
	}
	rec, ok := store.(PlanSatisfactionRecorder)
	if !ok {
		return
	}
	if v.Unverified == "" && v.Cites != "" {
		if loader, ok := store.(PlanSatisfactionLoader); ok {
			if rows, err := loader.LoadPlanSatisfaction(ctx, []string{v.Cites}); err == nil {
				for _, r := range rows[v.Cites] {
					if r.Kind == persistence.PlanSatisfactionNoChange && r.Cites == issueID {
						v.Unverified, v.Ref, v.Cites = NoChangeCycle, "", ""
					}
				}
			}
		}
	}
	row := persistence.PlanSatisfaction{IssueID: issueID, Kind: persistence.PlanSatisfactionNoChange, Ref: v.Ref, Cites: v.Cites}
	if v.Unverified != "" {
		row = persistence.PlanSatisfaction{IssueID: issueID, Kind: persistence.PlanSatisfactionUnverified, Ref: v.Unverified}
	}
	if err := rec.UpsertPlanSatisfaction(ctx, row); err != nil {
		log.Warn("no-change claim record failed", slog.Any("error", err))
		return
	}
	log.Info("no-change claim recorded", slog.String("kind", row.Kind), slog.String("ref", row.Ref))
}
