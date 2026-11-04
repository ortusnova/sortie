package orchestrator

import (
	"context"
	"log/slog"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/logging"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// completeMerged applies [completeMergedCandidates] with the
// merge-completion target state as the done state. Without a configured
// merge-completion reaction there is no done state, and under a single
// once run reactions are skipped, so issues pass unchanged.
func (o *Orchestrator) completeMerged(ctx context.Context, cfg config.ServiceConfig, issues []domain.Issue) []domain.Issue {
	if !o.mergeCompletionReactionConfigured || o.once != nil {
		return issues
	}
	return completeMergedCandidates(ctx, o.state, o.trackerAdapter, o.store,
		o.mergeCompletionConfig.TargetState, cfg.Tracker.TerminalStates, issues, o.logger)
}

// completeMergedCandidates moves to the merge-completion target state
// every candidate that Sortie has a merge record for (a merged
// plan_satisfaction row) but whose tracker state is not terminal. Such an
// issue's pull request merged outside the merge-completion reaction (by a
// person, or before Sortie watched it), so nothing else would end it, and
// each dispatch would review an already merged branch again.
//
// It runs before dispatch on every tick, the first tick after a start
// included. A moved candidate keeps its slot in the result with the target
// state, so the park and budget bookkeeping of this tick sees it end; a
// candidate whose move failed is left out of this tick and tried again on
// the next. A candidate with an open hand-off part is left out of the tick
// without a move until its parts close. A running issue is left to its
// run. A store without the
// records, or a failed read, changes nothing.
func completeMergedCandidates(
	ctx context.Context,
	state *State,
	tracker domain.TrackerAdapter,
	store any,
	targetState string,
	terminalStates []string,
	candidates []domain.Issue,
	log *slog.Logger,
) []domain.Issue {
	loader, ok := store.(PlanSatisfactionLoader)
	if !ok || tracker == nil || targetState == "" || len(candidates) == 0 {
		return candidates
	}

	var ids []string
	for _, issue := range candidates {
		if isTerminalState(issue.State, terminalStates) {
			continue
		}
		if _, running := state.Running[issue.ID]; running {
			continue
		}
		ids = append(ids, issue.ID)
	}
	if len(ids) == 0 {
		return candidates
	}
	rows, err := loader.LoadPlanSatisfaction(ctx, ids)
	if err != nil {
		log.Warn("merge record read failed, dispatching without the merged check", slog.Any("error", err))
		return candidates
	}

	out := make([]domain.Issue, 0, len(candidates))
	for _, issue := range candidates {
		merged, found := mergedRow(rows[issue.ID])
		if !found || isTerminalState(issue.State, terminalStates) {
			out = append(out, issue)
			continue
		}
		if _, running := state.Running[issue.ID]; running {
			out = append(out, issue)
			continue
		}
		issueLog := logging.WithIssue(log, issue.ID, issue.Identifier)
		if handoffOpen(state, issue.ID) {
			issueLog.Debug("merged issue kept out of the done state and this tick: a hand-off part is open",
				slog.String("pull_request", merged.PR))
			continue
		}
		if err := tracker.TransitionIssue(ctx, issue.ID, targetState); err != nil {
			issueLog.Warn("merged issue could not be moved to the done state, holding it until the next tick",
				slog.String("target_state", targetState),
				slog.String("pull_request", merged.PR),
				slog.Any("error", err),
			)
			continue
		}
		issueLog.Info("merged issue moved to the done state instead of dispatching it",
			slog.String("from_state", issue.State),
			slog.String("target_state", targetState),
			slog.String("pull_request", merged.PR),
			slog.String("merge_commit_sha", merged.Ref),
		)
		issue.State = targetState
		out = append(out, issue)
	}
	return out
}

func mergedRow(rows []persistence.PlanSatisfaction) (persistence.PlanSatisfaction, bool) {
	for _, r := range rows {
		if r.Kind == persistence.PlanSatisfactionMerged {
			return r, true
		}
	}
	return persistence.PlanSatisfaction{}, false
}
