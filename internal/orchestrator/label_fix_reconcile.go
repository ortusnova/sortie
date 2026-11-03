package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/logging"
)

// reconcileLabelFixCommands polls the label-event journal for each
// label-fix entry in state.PendingReactions and dispatches at most one
// full fix session per confirmed labeling gesture. Called from
// [ReconcileRunningIssues] after [reconcileLabelReviewCommands]. Skipped
// entirely when the SCM adapter is nil or the label-fix feature is not
// configured.
//
// The metrics parameter is accepted for signature parity with the sibling
// reconcile passes invoked positionally from [ReconcileRunningIssues]; this
// pass records no counter in this slice.
func reconcileLabelFixCommands(state *State, params ReconcileParams, log *slog.Logger, ctx context.Context, _ domain.Metrics) {
	if params.SCMAdapter == nil || !params.LabelFixReactionConfigured {
		return
	}

	now := time.Now().UTC()
	if params.NowFunc != nil {
		now = params.NowFunc().UTC()
	}

	pollInterval := max(time.Duration(params.LabelFixConfig.PollIntervalMS)*time.Millisecond, labelReviewPendingBackoffBase)

	for key, pending := range state.PendingReactions {
		if pending.Kind != ReactionKindLabelFix {
			continue
		}
		delete(state.PendingReactions, key)

		data, ok := pending.KindData.(*LabelFixReactionData)
		if !ok {
			log.ErrorContext(ctx, "unexpected KindData type for label-fix reaction",
				slog.String("issue_id", pending.IssueID),
				slog.String("type", fmt.Sprintf("%T", pending.KindData)),
			)
			continue
		}

		entryLog := logging.WithIssue(log, pending.IssueID, pending.Identifier)

		// A human gesture stays actionable regardless of age, so this kind
		// has no TTL and no drop-on-age branch.
		if reactionWaits(pending, now) {
			state.PendingReactions[key] = pending
			continue
		}

		readPullRequestURL(ctx, params.SCMAdapter, pending, entryLog)

		// A command run of this kind waiting in the retry slot has not yet
		// recorded its command handled: the pass leaves the journal and the
		// mark alone until the run starts, so it neither queues the command
		// twice nor records it handled before it runs.
		if commandQueued(state, pending.IssueID, ReactionKindLabelFix) {
			pending.PendingRetryAt = now.Add(pollInterval)
			requeueReaction(state, key, pending, waitRetrySlot)
			continue
		}

		// The dispatched flag is not consulted for this kind. The read error
		// is: once-per-command rests solely on the mark, and this kind has no
		// turn cap to bound a duplicate, so when the mark cannot be read the
		// pass backs off rather than risk re-dispatching an already
		// acknowledged command with storedMark treated as empty.
		storedMark, _, fpErr := params.Store.GetReactionFingerprint(ctx, pending.IssueID, ReactionKindLabelFix)
		if fpErr != nil {
			pending.PendingAttempts++
			delay := max(computeReactionPendingDelay(pending.PendingAttempts), pollInterval)
			pending.PendingRetryAt = now.Add(delay)
			requeueReaction(state, key, pending, waitBackoff)
			entryLog.Warn("label-fix fingerprint read failed, backing off without dispatch",
				slog.Int("pr_number", data.PRNumber),
				slog.Any("error", fpErr),
				slog.Int("pending_attempts", pending.PendingAttempts),
				slog.Int64("retry_after_ms", int64(delay/time.Millisecond)),
			)
			continue
		}

		events, err := params.SCMAdapter.ListLabelEvents(ctx, data.PRNumber, data.Owner, data.Repo)
		if err != nil {
			pending.PendingAttempts++
			delay := max(computeReactionPendingDelay(pending.PendingAttempts), pollInterval)
			pending.PendingRetryAt = now.Add(delay)
			requeueReaction(state, key, pending, waitBackoff)
			entryLog.Warn("label-fix journal fetch failed, retrying with backoff",
				slog.Int("pr_number", data.PRNumber),
				slog.Any("error", err),
				slog.Int("pending_attempts", pending.PendingAttempts),
				slog.Int64("retry_after_ms", int64(delay/time.Millisecond)),
			)
			continue
		}

		// Events strictly newer than the stored position. An empty
		// storedMark sorts before every real mark.
		var newEvents []domain.LabelEvent
		for _, e := range events {
			if labelReviewMark(e) > storedMark {
				newEvents = append(newEvents, e)
			}
		}
		if len(newEvents) == 0 {
			pending.PendingRetryAt = now.Add(pollInterval)
			requeueReaction(state, key, pending, waitPollInterval)
			continue
		}

		newestMark := labelReviewMark(latestLabelEvent(newEvents))

		// Burst collapse: all matching labeled events in the batch collapse
		// to at most one command. A self-authored-event filter is not run in
		// this slice because no orchestrator identity is wired; the structural
		// guarantee that Sortie never applies command labels makes its absence
		// a no-op.
		var matches []domain.LabelEvent
		for _, e := range newEvents {
			if e.Added && strings.EqualFold(e.Label, params.LabelFixConfig.FixLabel) {
				matches = append(matches, e)
			}
		}

		labelPresentNow := labelPresent(events, params.LabelFixConfig.FixLabel)
		commandConfirmed := len(matches) > 0 && labelPresentNow

		running := state.Running[pending.IssueID]
		incumbent := retrySlotIncumbent(state, pending.IssueID)

		// A session or retry of another kind defers before the same-kind
		// collapse check runs: evaluating the collapse arm first would
		// advance the mark past the labeling event and swallow the gesture.
		if commandConfirmed && incumbent != nil && incumbent.ReactionKind != ReactionKindLabelFix {
			logRetrySlotDeferral(entryLog, ReactionKindLabelFix, incumbent)
			requeueReaction(state, key, pending, waitRetrySlot)
			continue
		}
		if commandConfirmed && running != nil && running.ReactionKind != ReactionKindLabelFix {
			requeueReaction(state, key, pending, waitRetrySlot)
			continue
		}

		sameKindQueuedOrRunning := (running != nil && running.ReactionKind == ReactionKindLabelFix) ||
			(incumbent != nil && incumbent.ReactionKind == ReactionKindLabelFix)

		if commandConfirmed && !sameKindQueuedOrRunning {
			// The mark and the label are left as they are: the retry
			// handler records the command handled and removes the label
			// when the run starts ([acknowledgeLabelCommand]). A queued
			// retry is runtime-only, so a restart before the run starts
			// leaves the command standing and the rebuilt watch queues it
			// again; a run that started is never repeated for it.
			newestMatch := latestLabelEvent(matches)
			data.HighWaterMark = newestMark
			data.LastActor = newestMatch.Actor

			scheduleReactionContinuation(ctx, state, params.Store, ScheduleRetryParams{
				IssueID:    pending.IssueID,
				Identifier: pending.Identifier,
				DisplayID:  pending.DisplayID,
				Attempt:    pending.Attempt,
				DelayMS:    continuationDelayMS,
				ContinuationContext: map[string]any{
					"label_fix": buildLabelFixMap(data, newestMatch.Actor, newestMatch.At),
				},
				ReactionKind:        ReactionKindLabelFix,
				Continuation:        continuationTargetOf(pending, 0),
				Command:             LabelCommand{Label: params.LabelFixConfig.FixLabel, Mark: newestMark},
				AgentKind:           pending.AgentKind,
				ConfiguredModel:     pending.ConfiguredModel,
				RuleName:            pending.RuleName,
				StageLineage:        pending.StageLineage,
				RuleSettingsApplied: pending.RuleSettingsApplied,
				TemplateID:          pending.TemplateID,
				Logger:              entryLog,

				IssueIdentity: pending.IssueIdentity,
			}, params.OnRetryFire, now)

			entryLog.Info("label-fix dispatched",
				slog.Int("pr_number", data.PRNumber),
				slog.String("actor", data.LastActor),
			)

			// Re-enqueue so the detection loop survives the dispatch,
			// decoupled from the per-issue workspace the session runs in.
			pending.PendingRetryAt = now.Add(pollInterval)
			requeueReaction(state, key, pending, waitPollInterval)
			continue
		}

		// No dispatch (retraction, foreign labels, unlabeled-only, or
		// collapse into an outstanding command): advance the mark past the
		// examined events and re-enqueue at the next poll.
		if upErr := params.Store.UpsertReactionFingerprint(ctx, pending.IssueID, ReactionKindLabelFix, newestMark); upErr != nil {
			entryLog.Warn("failed to upsert label-fix reaction fingerprint",
				slog.Any("error", upErr),
			)
		}
		data.HighWaterMark = newestMark
		pending.PendingRetryAt = now.Add(pollInterval)
		requeueReaction(state, key, pending, waitPollInterval)
	}
}

// buildLabelFixMap builds the continuation template context injected under
// the key "label_fix". It mirrors [buildLabelReviewMap] and adds branch, the
// PR head branch the fix session checks out and pushes to. Every documented
// field is always present so a template referencing any sub-field renders
// under missingkey=error.
func buildLabelFixMap(data *LabelFixReactionData, actor string, requestedAt time.Time) map[string]any {
	return map[string]any{
		"pr_number":    data.PRNumber,
		"owner":        data.Owner,
		"repo":         data.Repo,
		"branch":       data.Branch,
		"actor":        actor,
		"requested_at": requestedAt.UTC().Format(time.RFC3339),
	}
}
