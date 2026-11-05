package render

import (
	"cmp"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/pricing"
)

// startedBody is the text of every session dispatch, for every agent kind
// and every tracker kind alike: a business reader of the issue needs only
// that a session started, not an internal integration name or a dispatch
// counter.
const startedBody = "Sortie session started."

// completionBody returns the text for a normal exit. RetryScheduled
// distinguishes "completed (re-queuing)" from "completed". The text carries
// no session identifier: a reader of the issue cannot act on it, and a
// public issue would publish it.
func completionBody(f domain.SessionCompletedFacts) string {
	headline := "Sortie session completed."
	if f.RetryScheduled {
		headline = "Sortie session completed (re-queuing)."
	}
	return fmt.Sprintf("%s\nDuration: %s\nTurns: %d",
		headline, f.Elapsed.Truncate(time.Second).String(), f.Turns)
}

// failureBody returns the text for an error exit. It carries neither the
// error text nor the session identifier: the issue may be public, and the
// cause stays in the log, the run history, and the dashboard.
func failureBody(f domain.SessionFailedFacts) string {
	retryLine := "Retry: no (not retryable)"
	if f.RetryScheduled {
		retryLine = fmt.Sprintf("Retry: yes (attempt %d)", f.NextAttempt)
	}
	return fmt.Sprintf("Sortie session failed.\nDuration: %s\n%s",
		f.Elapsed.Truncate(time.Second).String(), retryLine)
}

// softStopBody returns the text for an exit triggered by a recognized A2O
// status signal.
func softStopBody(f domain.SessionStoppedFacts) string {
	return fmt.Sprintf("Sortie session completed (agent signaled: %s).\nDuration: %s\nTurns: %d",
		f.Reason, f.Elapsed.Truncate(time.Second).String(), f.Turns)
}

// ciEscalationBody returns the plain-text escalation for CI failures that
// exceeded the retry budget.
func ciEscalationBody(f domain.CIFailureFacts) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CI fix retries exhausted\n\n")
	fmt.Fprintf(&b, "Sortie attempted %d CI-fix continuation(s) on ref %s but CI is still failing.\n\n", f.Attempts, f.Ref)

	if f.FailingCount > 0 {
		fmt.Fprintf(&b, "Failing checks: %d\n", f.FailingCount)
	}

	for _, check := range f.Checks {
		if !check.Failing {
			continue
		}
		fmt.Fprintf(&b, "- %s: %s", check.Name, check.Conclusion)
		if check.DetailsURL != "" {
			fmt.Fprintf(&b, " (details: %s)", check.DetailsURL)
		}
		b.WriteString("\n")
	}

	b.WriteString("\nManual intervention required.")
	return b.String()
}

// triageEscalationBody returns the operator-facing text for an escalation a
// triage command requested. The subject
// is the short phrase naming what was triaged: "ref <head sha>" for CI, and
// "PR #<number>" for the three pull-request kinds, the same subjects the
// budget escalations name.
func triageEscalationBody(eventType domain.EventType, f domain.TriageEscalationFacts) string {
	kind, subject := triageKind(eventType), "PR #"+strconv.Itoa(f.PRNumber)
	if eventType == domain.EventEscalationCIFailure {
		subject = "ref " + f.Ref
	}
	return fmt.Sprintf(
		"The %s triage command requested human attention for %s.\n\n"+
			"Sortie ran the configured triage command before dispatching a continuation "+
			"and the command answered \"escalate\", so no continuation was dispatched and "+
			"no retry or continuation budget was spent. Manual intervention required.",
		kind, subject)
}

func triageKind(eventType domain.EventType) string {
	switch eventType {
	case domain.EventEscalationCIFailure:
		return "ci"
	case domain.EventEscalationReviewComments:
		return "review"
	case domain.EventEscalationBotReview:
		return "bot-review"
	case domain.EventEscalationMergeConflicts:
		return "merge-conflict"
	default:
		return string(eventType)
	}
}

// reviewEscalationBody returns the plain-text escalation for review fixes
// that exceeded the turn budget.
func reviewEscalationBody(f domain.ReviewEscalationFacts) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Review fix continuation turns exhausted for PR #%d on branch %s.\n", f.PRNumber, f.Branch)
	fmt.Fprintf(&b, "%d continuation turns attempted. Remaining review comments require human attention.", f.Turns)
	return b.String()
}

// botReviewEscalationBody returns the escalation used when the bot review
// continuation turn budget is exhausted.
func botReviewEscalationBody(f domain.BotReviewEscalationFacts) string {
	return fmt.Sprintf(
		"Sortie attempted %d bot-review continuation turn(s) and could not resolve the automated review comments for PR #%d. Remaining bot review comments require human attention.",
		f.Turns,
		f.PRNumber,
	)
}

// mergeConflictEscalationBody returns the plain-text escalation used when
// the per-episode retry budget is exhausted.
func mergeConflictEscalationBody(f domain.MergeConflictFacts) string {
	// The observation that trips the retry cap dispatches no continuation, so
	// the number of resolution turns actually dispatched is attempts-1. At
	// max_retries 0 this is 0: the reaction escalates on first detection
	// without attempting a rebase.
	dispatched := f.Attempts - 1
	return fmt.Sprintf(
		"Sortie attempted %d merge-conflict resolution turn(s) and could not clear the conflicts for PR #%d. Manual rebase required.",
		dispatched,
		f.PRNumber,
	)
}

// autoMergeEscalationBody returns the escalation used when MaxRetries is
// exhausted.
func autoMergeEscalationBody(f domain.AutoMergeEscalationFacts) string {
	return fmt.Sprintf(
		"Sortie auto_merge attempted %d time(s) and could not complete the merge for PR #%d. Manual merge required.",
		f.Turns,
		f.PRNumber,
	)
}

// autoMergedBody returns the text used on a successful auto-merge. The
// merge-commit line appears only when the merge result carries a commit
// identifier.
func autoMergedBody(f domain.AutoMergedFacts) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Sortie auto-merged PR #%d (%s/%s).\n", f.PRNumber, f.Owner, f.Repo)
	fmt.Fprintf(&b, "Strategy: %s.", string(f.Strategy))
	if f.SHA != "" {
		fmt.Fprintf(&b, "\nMerge commit: %s.", f.SHA)
	}
	return b.String()
}

// mergeCompletionBody returns the plain-text escalation used when a
// merge-completion transition cannot be performed. It names the pull
// request number, repository, configured target state, and the number of
// transition calls made, and no internal symbol, table, or file path.
func mergeCompletionBody(f domain.MergeCompletionFacts) string {
	return fmt.Sprintf(
		"Sortie attempted %d time(s) and could not transition the issue to %q after PR #%d (%s/%s) merged. Manual transition required.",
		f.Attempts,
		f.TargetState,
		f.PRNumber,
		f.Owner,
		f.Repo,
	)
}

// mergeCompletionMissingSHABody returns the operator-facing explanation
// used after the missing-SHA grace period. It includes the affected
// repository and pull request, elapsed wait, escalation reason, and the
// manual follow-up.
func mergeCompletionMissingSHABody(f domain.MergeCompletionMissingSHAFacts) string {
	return fmt.Sprintf(
		"Sortie waited %s for %s/%s PR #%d: the pull request is reported merged but no merge commit identifier was provided. Sortie keeps watching and transitions this issue to %q if the forge reports the commit. Verify the merge in the forge and transition this issue manually if appropriate.",
		f.Waited.Round(time.Second),
		f.Owner,
		f.Repo,
		f.PRNumber,
		f.TargetState,
	)
}

// budgetHoldCeilingWords names each budget-hold reason in the words the
// notice uses. A reason absent from this map renders no Ceiling: line at
// all, rather than a machine value or an invented phrase.
var budgetHoldCeilingWords = map[string]string{
	domain.BudgetHoldReasonSession: "session budget",
	domain.BudgetHoldReasonToken:   "token budget",
	domain.BudgetHoldReasonMoney:   "money budget",
}

// budgetHoldTokenSpend words the token sum for the notice: unknown spend is
// never printed as zero, and a sum that leaves spend out is marked as a
// lower bound. UsedTokens must be non-nil.
func budgetHoldTokenSpend(f domain.BudgetHeldFacts) string {
	used := *f.UsedTokens
	switch {
	case f.TokensComplete:
		return strconv.FormatInt(used, 10)
	case used == 0:
		return "not measured"
	default:
		return fmt.Sprintf("at least %d", used)
	}
}

// budgetHoldMoneySpend renders the estimated cost: exact when the figure is
// complete, else a lower bound, or unknown when nothing priced. UsedUSD must
// be non-nil.
func budgetHoldMoneySpend(f domain.BudgetHeldFacts) string {
	used := *f.UsedUSD
	switch {
	case f.UsedUSDComplete != nil && *f.UsedUSDComplete:
		return pricing.FormatUSD(used)
	case used == 0:
		return "unknown"
	default:
		return "at least " + pricing.FormatUSD(used)
	}
}

// budgetHeldBody renders the plain-text notice body from the facts alone.
// The body is a past-tense event record and MUST NOT assert a present state, use
// imperative or sufficiency-claiming phrasing, or interpolate the issue's
// identifier, which the comment is already attached to.
func budgetHeldBody(f domain.BudgetHeldFacts) string {
	lines := []string{
		"Sortie stopped dispatching this issue.",
		fmt.Sprintf("Recorded: %s", f.ExhaustedAt.Format(time.RFC3339)),
	}

	word, hasWord := budgetHoldCeilingWords[f.Reason]
	setting := f.CeilingSetting
	hasSetting := setting != ""
	if hasWord && hasSetting {
		lines = append(lines, fmt.Sprintf("Ceiling: %s (%s)", word, setting))
	}

	if f.BudgetUSD > 0 && f.UsedUSD != nil {
		lines = append(lines, fmt.Sprintf("Estimated cost: %s of %s", budgetHoldMoneySpend(f), pricing.FormatUSD(f.BudgetUSD)))
	}
	if f.BudgetTokens > 0 && f.UsedTokens != nil {
		lines = append(lines, fmt.Sprintf("Tokens: %s of %d", budgetHoldTokenSpend(f), f.BudgetTokens))
	}
	if f.BudgetSessions > 0 {
		lines = append(lines, fmt.Sprintf("Sessions: %d of %d", f.UsedSessions, f.BudgetSessions))
	}
	if f.StoppedInFlight != nil && *f.StoppedInFlight > 0 {
		word := "session"
		if *f.StoppedInFlight != 1 {
			word = "sessions"
		}
		lines = append(lines, fmt.Sprintf("Stopped in flight: %d %s", *f.StoppedInFlight, word))
	}

	if hasSetting {
		lines = append(lines, fmt.Sprintf("Raising %s raises this ceiling.", setting))
	}

	return strings.Join(lines, "\n")
}

// stageBody returns the public text of a stage event. It names no error,
// session, or agent, so a tracker comment may post it.
func stageBody(eventType domain.EventType, stage domain.StageTransition) string {
	headline := "Sortie advanced the issue to the next stage."
	lines := []string{"From: " + stage.SourceRule, "To: " + stage.TargetRule}
	if eventType == domain.EventStageNotAdvanced {
		headline = "Sortie did not advance the issue to the next stage."
		lines = append(lines, "Reason: "+stageNotAdvancedPhrase(stage.Reason))
	}
	lines = append(lines, "Hop count: "+strconv.Itoa(stage.HopCount), "Chain: "+stage.ChainID)
	return strings.Join(append([]string{headline}, lines...), "\n")
}

func stageNotAdvancedPhrase(reason string) string {
	if reason == domain.StageReasonCeiling {
		return "the issue reached its limit of consecutive stage moves (dispatch.max_consecutive_hops)"
	}
	return "the next stage's label could not be added to the issue"
}

// sessionEscalatedBody returns the text of a session.escalated event: who
// asks for a person, or which check did not pass, and the session's viewer
// link when there is one.
func sessionEscalatedBody(f domain.SessionEscalatedFacts) string {
	var body string
	switch {
	case f.Kind == domain.SessionEscalationCriteriaFailed:
		body = "The acceptance-criteria check of rule " + f.Rule + " did not pass."
	case f.Rule != "":
		body = "The running session on rule " + f.Rule + " asks for a person (" + f.Kind + ")."
	default:
		body = "The running session asks for a person (" + f.Kind + ")."
	}
	if f.URL != "" {
		body += "\nSession: " + f.URL
	}
	return body
}

// holdExpiredBody returns the text of a hold.expired event: whose hold ran
// out, when, why it was placed, and the last evidence it carried.
func holdExpiredBody(f domain.HoldExpiredFacts) string {
	by := cmp.Or(f.By, "its holder")
	lines := []string{"The hold by " + by + " expired at " + f.Until.UTC().Format(time.RFC3339) + " without a renewal. The issue waits for a person."}
	if f.Reason != "" {
		lines = append(lines, "Reason: "+f.Reason)
	}
	if f.Evidence != "" {
		lines = append(lines, "Last evidence: "+f.Evidence)
	}
	if !f.RenewedAt.IsZero() {
		lines = append(lines, "Last renewed: "+f.RenewedAt.UTC().Format(time.RFC3339))
	}
	return strings.Join(lines, "\n")
}

// storyRevertedBody returns the text of a story.reverted event: which
// merge was reverted, by which pull request, who reverted it and why, and
// where the story went.
func storyRevertedBody(f domain.StoryRevertedFacts) string {
	merge := "Its merge"
	if f.PullRequest != "" {
		merge = "Its merge of " + f.PullRequest
	}
	if f.MergeCommit != "" {
		merge += " (" + shortSHA(f.MergeCommit) + ")"
	}
	merge += " was reverted"
	if f.RevertPullRequest != "" {
		merge += " by " + f.RevertPullRequest
	}
	if f.BySortie {
		merge += ", which Sortie merged because the change was not proven live in time."
	} else {
		merge += ", merged on the forge."
	}
	lines := []string{merge}
	if f.Reason != "" {
		lines = append(lines, "Reason: "+f.Reason)
	}
	if f.State != "" {
		lines = append(lines, "The story is back in "+f.State+" and is not done; the stories it blocks wait for it again.")
	} else {
		lines = append(lines, "The story is not done; the stories it blocks wait for it again.")
	}
	return strings.Join(lines, "\n")
}

// shortSHA returns the first twelve characters of a commit.
func shortSHA(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}

// loopSuspectedBody returns the text of a supervisor.loop_suspected
// event: which trigger kind is suspended, on which state digest, and since
// when its sessions took no action.
func loopSuspectedBody(f domain.SupervisorLoopSuspectedFacts) string {
	lines := []string{
		fmt.Sprintf("%d consecutive %s supervisor sessions took no action on state digest %s. The %s trigger starts no session until the state digest changes.",
			f.Sessions, f.TriggerKind, f.Digest, f.TriggerKind),
	}
	if !f.Since.IsZero() {
		lines = append(lines, "First of them: "+f.Since.UTC().Format(time.RFC3339))
	}
	return strings.Join(lines, "\n")
}

// loopSlowBody returns the text of a platform.loop_slow event: what held
// Sortie's event loop, for how long, and how often in the window.
func loopSlowBody(f domain.PlatformLoopSlowFacts) string {
	return fmt.Sprintf("Sortie's event loop was held over 5 seconds %d times in the last %d minutes; the latest hold lasted %s, in %s. "+
		"While it is held, Sortie applies no agent event, worker exit or control request, and its API serves the last snapshot it built.",
		f.SlowHolds, int(f.Window.Minutes()), f.Hold.Round(time.Millisecond), f.Handler)
}
