// Package sentence words what each issue is doing or waiting on as plain
// English. One template table turns a runtime snapshot row, a finished
// issue, or a refresh outcome into text, so every API client shows the same
// wording instead of reading Sortie state itself.
//
// Start with [For] for an issue, [NextOutcomes] for what Sortie does next for
// an issue, [Attention] for what needs a person, [PollHealth] for the tracker
// poll check, and [RefreshOutcome], [RetryNowOutcome], [CheckNowOutcome], and
// [AcknowledgeOutcome] for a refresh, a retry-now, a check-now, and an
// acknowledge request. [Hold] words why a candidate is held back and what
// releases it. [Event] words one retained agent event, and [Stall]
// and [StallOff] word a running row's quiet marker and its absence, and
// [RunError] words a finished run's stored error. All are
// safe for concurrent use and never fail: a template that cannot render
// yields a fixed fallback text, or leaves out the one next outcome it
// belongs to, and logs one record.
package sentence

import (
	"cmp"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/orchestrator"
	"github.com/sortie-ai/sortie/internal/redact"
)

// Status is one status sentence: Short for a row or lane, Long for the issue page.
type Status struct {
	Short string
	Long  string
}

// Context is what every sentence reads beside its own input.
type Context struct {
	// Now is the snapshot's generated_at; relative times are measured from it.
	Now time.Time
	// Shape is the workflow shape the request serves; nil omits every clause
	// that reads it.
	Shape *orchestrator.WorkflowShape
}

// Entry holds one runtime entry; [For] words the first non-nil member in
// field order.
type Entry struct {
	Running         *orchestrator.SnapshotRunningEntry
	Retry           *orchestrator.SnapshotRetryEntry
	BudgetHold      *orchestrator.SnapshotBudgetEntry
	Parked          *orchestrator.SnapshotParkedEntry
	PendingReaction *orchestrator.SnapshotPendingReaction
	BetweenStages   *orchestrator.SnapshotStageHop
	Held            *orchestrator.SnapshotHeldEntry
	Finished        *Finished
}

// Finished is an issue no live entry holds, as run history describes it.
type Finished struct {
	Identifier        string
	DisplayIdentifier string
	// Outcome is nil when Sortie recorded no ending.
	Outcome *Outcome
	// StageTarget is the newest run's hop target; empty when its exit reached
	// no hop decision.
	StageTarget string
	// StageResult is advanced, partial, failed, or ceiling; empty with
	// StageTarget.
	StageResult string
}

// Outcome is an issue outcome as the API serves it.
type Outcome struct {
	// Kind is handed_off, merged, or terminal.
	Kind string
	// State is the tracker state of a handed_off or terminal outcome, masked.
	State string
	// MergePath is auto_merge or merge_completion for a merged outcome.
	MergePath string
	// PRNumber is the merged pull request of a merged outcome.
	PRNumber int
}

// For returns the status sentence of the entry e holds, or the zero Status
// when it holds none.
func For(e Entry, c Context) Status {
	switch {
	case e.Running != nil:
		return status(selectRunning(e.Running, c))
	case e.Retry != nil:
		return status(selectRetry(e.Retry, c))
	case e.BudgetHold != nil:
		return status(selectBudgetHold(e.BudgetHold))
	case e.Parked != nil:
		return status(selectParked(e.Parked))
	case e.PendingReaction != nil:
		return status(selectPendingReaction(e.PendingReaction, c))
	case e.BetweenStages != nil:
		return status(selectBetweenStages(e.BetweenStages))
	case e.Held != nil:
		return status(selectHold(&e.Held.Hold, c))
	case e.Finished != nil:
		return status(selectFinished(e.Finished))
	}
	return Status{}
}

// HoldWords is the operator text of a hold: Reason is the short sentence
// without a final period, and Release is one sentence ending with a period.
type HoldWords struct {
	Reason  string
	Release string
}

// Hold returns the words of a hold. Its Reason equals the short form [For]
// serves for a held row, and a reason the table lacks reads as the fallback
// text.
func Hold(h *orchestrator.SnapshotHold, c Context) HoldWords {
	name, data := selectHold(h, c)
	return HoldWords{
		Reason:  render(name, "short", data),
		Release: render(name, "clears", data),
	}
}

// RefreshOutcome returns the outcome sentence of a refresh request.
func RefreshOutcome(queued, coalesced bool) string {
	name := "ctl_refresh"
	switch {
	case !queued:
		name = "ctl_refused"
	case coalesced:
		name = "ctl_refresh_coalesced"
	}
	return render(name, "text", nil)
}

// RetryNowOutcome returns the outcome sentence of a request to run a
// scheduled retry now. queued is false for a request refused because
// Sortie is shutting down, which ignores the other arguments.
func RetryNowOutcome(queued bool, displayID, identifier string, attempt int) string {
	if !queued {
		return render("ctl_refused", "text", nil)
	}
	return render("ctl_retry_now", "text", map[string]any{
		"Key":     issueKey(displayID, identifier),
		"Attempt": attempt,
	})
}

// CheckNowOutcome returns the outcome sentence of a request to check a
// pending pull request reaction now. queued is false for a request refused
// because Sortie is shutting down, which ignores check.
func CheckNowOutcome(queued bool, check orchestrator.CheckNowResult) string {
	if !queued {
		return render("ctl_refused", "text", nil)
	}
	words, known := reactionKindWords[check.Kind]
	if !known {
		words = otherReactionKind
	}
	return render("ctl_check_now", "text", map[string]any{
		"Key":        issueKey(check.DisplayID, check.Identifier),
		"PR":         check.PRNumber,
		"KindInline": words.inline,
	})
}

// AcknowledgeOutcome returns the outcome sentence of a request to acknowledge
// an attention item. acknowledged is false for a request refused because
// Sortie is shutting down, which ignores the other arguments; already is true
// when the item was acknowledged before the request.
func AcknowledgeOutcome(acknowledged, already bool, cause orchestrator.AttentionCause) string {
	if !acknowledged {
		return render("ctl_acknowledge_refused", "text", nil)
	}
	name := "ctl_acknowledge"
	if already {
		name = "ctl_acknowledge_already"
	}
	item := attentionItem(cause, Context{})
	return render(name, "text", map[string]any{
		"Key":    issueKey(item.DisplayIdentifier, item.IssueIdentifier),
		"Reason": item.ReasonText,
	})
}

// Event returns the sentence of a retained agent event. An event type with no
// template reads as the fallback text.
func Event(e orchestrator.RunEvent) string {
	name := "event_" + string(e.Event)
	if _, known := table[name]; !known {
		return redact.Mask(fallbackText)
	}
	message := strings.TrimSpace(e.Message)
	cleaned := strings.TrimSuffix(message, ".")
	if strings.EqualFold(cleaned, strings.ReplaceAll(string(e.Event), "_", " ")) {
		cleaned = ""
	}
	return render(name, "text", map[string]any{
		"Raw":         message,
		"Message":     cleaned,
		"Tool":        ToolLine(e),
		"HasDuration": e.DurationMS > 0,
		"Duration":    float64(e.DurationMS) / 1000,
		"ToolError":   e.ToolError,
	})
}

// ToolLine returns the line text of a tool_result event, and "" for any other
// event. A shell call reads as its command alone; any other call with an
// argument reads as its name and the argument.
func ToolLine(e orchestrator.RunEvent) string {
	if e.Event != domain.EventToolResult {
		return ""
	}
	name := cmp.Or(e.ToolName, toolKindWord(e.ToolKind))
	if e.ToolKind == domain.ToolKindShell && e.ToolArgument != "" {
		return e.ToolArgument
	}
	if e.ToolArgument != "" {
		return name + " " + e.ToolArgument
	}
	return name
}

func toolKindWord(kind domain.ToolKind) string {
	switch kind {
	case domain.ToolKindShell:
		return "Command"
	case domain.ToolKindRead:
		return "Read"
	case domain.ToolKindEdit:
		return "Edit"
	case domain.ToolKindSkill:
		return "Skill"
	}
	return "Tool"
}

// Stall returns a running row's stall sentence, and false when its stall
// detection is off or c.Now is before its quiet instant.
func Stall(e *orchestrator.SnapshotRunningEntry, c Context) (Status, bool) {
	if e.QuietAt.IsZero() || c.Now.Before(e.QuietAt) {
		return Status{}, false
	}
	return status("stall_marker", map[string]any{
		"Silence": c.Now.Sub(e.StallReference).Seconds(),
		"Timeout": e.StallAt.Sub(e.StallReference).Seconds(),
	}), true
}

// StallOff returns a running row's stall-off sentence, and false when its
// stall detection is on.
func StallOff(e *orchestrator.SnapshotRunningEntry) (string, bool) {
	if !e.QuietAt.IsZero() {
		return "", false
	}
	return render("stall_off", "text", nil), true
}

func status(name string, data map[string]any) Status {
	return Status{Short: render(name, "short", data), Long: render(name, "long", data)}
}

// render executes one part of a template. The table is fixed at package
// initialization, so a failure here is a selector that left a key out.
func render(name, part string, data map[string]any) string {
	if text, ok := renderMasked(name, part, data); ok {
		return text
	}
	return redact.Mask(fallbackText)
}

// renderMasked executes one part of a template and masks the result. It
// reports false, after logging the failure, when the template cannot render.
func renderMasked(name, part string, data map[string]any) (string, bool) {
	var out strings.Builder
	if err := templates.ExecuteTemplate(&out, name+"."+part, data); err != nil {
		slog.Default().Error("status sentence render failed",
			slog.String("template", name),
			slog.Any("error", err))
		return "", false
	}
	return redact.Mask(out.String()), true
}

// continuationTemplates names the running template of each reaction kind a
// session can continue.
var continuationTemplates = map[string]string{
	orchestrator.ReactionKindCI:            "run_ci_fix",
	orchestrator.ReactionKindMergeConflict: "run_merge_conflict",
	orchestrator.ReactionKindReview:        "run_review_fix",
	orchestrator.ReactionKindBotReview:     "run_bot_review_fix",
	orchestrator.ReactionKindLabelReview:   "run_label_review",
	orchestrator.ReactionKindLabelFix:      "run_label_fix",
}

// retryTemplates names the retry template of each recorded retry trigger.
var retryTemplates = map[string]string{
	"error":        "retry_error",
	"continuation": "retry_continuation",
	"stall":        "retry_stall",
	"timer":        "retry_timer",
	"ci_fix":       "retry_ci_fix",
	"story_fault":  "retry_story_fault",
}

// issueKey is how a sentence names its issue.
func issueKey(displayID, identifier string) string {
	return cmp.Or(displayID, identifier)
}

// secondsUntil measures the span from now to at; negative once at has passed.
func secondsUntil(now, at time.Time) float64 {
	return at.Sub(now).Seconds()
}

// word returns the vocabulary entry for a served text, or the text as served
// when the vocabulary does not hold it.
func word(vocabulary map[string]string, served string) string {
	served = strings.TrimSpace(served)
	if words, ok := vocabulary[served]; ok {
		return words
	}
	return served
}

func selectRunning(e *orchestrator.SnapshotRunningEntry, c Context) (string, map[string]any) {
	data := map[string]any{
		"Turn":      e.TurnCount,
		"Stage":     "",
		"Pos":       0,
		"Len":       0,
		"Iteration": 0,
		"PR":        0,
		"Attempt":   0,
		"Limit":     0,
		"Label":     "",
	}
	if p := e.StagePosition; p != nil {
		data["Stage"] = strings.TrimSpace(p.Current)
		data["Pos"] = slices.Index(p.Chain, p.Current) + 1
		data["Len"] = len(p.Chain)
	}
	if e.SelfReviewIteration >= 1 {
		data["Iteration"] = e.SelfReviewIteration
	}

	if e.SelfReviewActive {
		return "run_self_review", data
	}
	if name, ok := continuationTemplates[e.ReactionKind]; ok && e.Continuation != nil {
		data["PR"] = e.Continuation.Number
		data["Attempt"] = e.Continuation.Attempt
		if limit, ok := reactionLimit(e.ReactionKind, c.Shape); ok && e.Continuation.Attempt <= limit {
			data["Limit"] = limit
		}
		data["Label"] = commandLabel(e.ReactionKind, c.Shape)
		return name, data
	}
	if e.TurnCount == 0 {
		return "run_starting", data
	}
	return "run_work", data
}

// reactionLimit returns the limit the shape serves for a reaction kind that
// counts tries.
func reactionLimit(kind string, shape *orchestrator.WorkflowShape) (int, bool) {
	if shape == nil {
		return 0, false
	}
	r := shape.Reactions
	switch {
	case kind == orchestrator.ReactionKindCI && r.CIFailure != nil:
		return r.CIFailure.MaxRetries, true
	case kind == orchestrator.ReactionKindMergeConflict && r.MergeConflicts != nil:
		return r.MergeConflicts.MaxRetries, true
	case kind == orchestrator.ReactionKindReview && r.ReviewComments != nil:
		return r.ReviewComments.MaxContinuationTurns, true
	case kind == orchestrator.ReactionKindBotReview && r.BotReview != nil:
		return r.BotReview.MaxContinuationTurns, true
	}
	return 0, false
}

// commandLabel returns the label the shape configures for a label command
// kind, or "" for any other kind or a shape without it.
func commandLabel(kind string, shape *orchestrator.WorkflowShape) string {
	if shape == nil || shape.Reactions.LabelCommands == nil {
		return ""
	}
	switch kind {
	case orchestrator.ReactionKindLabelReview:
		return strings.TrimSpace(shape.Reactions.LabelCommands.ReviewLabel)
	case orchestrator.ReactionKindLabelFix:
		return strings.TrimSpace(shape.Reactions.LabelCommands.FixLabel)
	}
	return ""
}

func selectRetry(e *orchestrator.SnapshotRetryEntry, c Context) (string, map[string]any) {
	data := map[string]any{
		"Attempt": e.Attempt,
		"Due":     float64(e.DueAtMS-c.Now.UnixMilli()) / 1000,
		"Error":   strings.TrimSuffix(RunError(strings.TrimSpace(e.Error)), "."),
		"Reason":  "",
		"Run":     "",
	}

	if e.Paused {
		data["Reason"] = word(pausedReasons, e.Error)
		return "retry_paused", data
	}
	if e.DueAtMS <= c.Now.UnixMilli() {
		return "retry_due_now", data
	}
	if name, ok := retryTemplates[e.Trigger]; ok {
		if name == "retry_timer" {
			data["Reason"] = word(rescheduleReasons, e.Error)
		}
		return name, data
	}
	if phrase, ok := runPhrases[e.ReactionKind]; ok && e.Trigger == "" {
		data["Run"] = phrase.initial
		return "retry_reaction", data
	}
	return "retry_generic", data
}

func selectBudgetHold(e *orchestrator.SnapshotBudgetEntry) (string, map[string]any) {
	used, atLeast := heldTokenSpend(e)
	data := map[string]any{
		"HasBudget": e.BudgetTokens > 0,
		"Budget":    new(e.BudgetTokens),
		"Used":      used,
		"AtLeast":   atLeast,
		"Limit":     e.BudgetSessions,
		"Sessions":  e.UsedSessions,
	}
	switch e.Reason {
	case budgetReasonMoney:
		return "held_budget_money", data
	case budgetReasonTokens:
		return "held_budget_tokens", data
	}
	return "held_budget_sessions", data
}

// heldTokenSpend returns the token figure a budget hold states: nil when
// Sortie holds no figure, and whether the figure is a lower bound. A nil
// result reaches the template as a typed nil, which the tokens function
// words as not measured.
func heldTokenSpend(e *orchestrator.SnapshotBudgetEntry) (used *int64, atLeast bool) {
	if e.UsedTokens == nil {
		return nil, false
	}
	var unmeasured int
	if e.UnmeasuredSessions != nil {
		unmeasured = *e.UnmeasuredSessions
	}
	switch {
	case !orchestrator.SpendIncomplete(unmeasured, e.UnaccountedTurns):
		return e.UsedTokens, false
	case *e.UsedTokens == 0:
		return nil, false
	}
	return e.UsedTokens, true
}

func selectParked(e *orchestrator.SnapshotParkedEntry) (string, map[string]any) {
	data := map[string]any{
		"Label":  strings.TrimSpace(e.Label),
		"Unseen": !e.LabelApplied,
	}
	switch e.Reason {
	case "handoff_absence":
		return "parked_handoff_absence", data
	case "repeated_failure":
		return "parked_repeated_failure", data
	}
	return "parked_agent_blocked", data
}

func selectPendingReaction(e *orchestrator.SnapshotPendingReaction, c Context) (string, map[string]any) {
	words, known := reactionKindWords[e.Kind]
	if !known {
		words = otherReactionKind
	}
	data := map[string]any{
		"PR":            e.PullRequest.Number,
		"KindInitial":   words.initial,
		"KindInline":    words.inline,
		"CountLabel":    "",
		"Used":          0,
		"Max":           0,
		"HasMax":        false,
		"Check":         waitClause(e.Wait),
		"Next":          secondsUntil(c.Now, e.NextPollAt),
		"HasCI":         false,
		"Pending":       0,
		"Passing":       0,
		"FailingChecks": []string{},
		"FailingN":      0,
		"New":           0,
		"Actionable":    0,
		"HasComments":   false,
		"Bots":          "",
		"Strategy":      "",
		"Handoff":       "",
		"Target":        "",
		"Label":         "",
		"Mergeability":  "",
	}
	if e.UsedAttempts != nil && words.count != "" {
		data["CountLabel"] = words.count
		data["Used"] = *e.UsedAttempts
		if e.MaxAttempts != nil {
			data["HasMax"] = true
			data["Max"] = *e.MaxAttempts
		}
	}

	check := e.LastCheck
	ci, comments := checkedCI(check), checkedComments(check)
	if ci != nil {
		data["HasCI"] = true
		data["Pending"] = ci.Pending
		data["Passing"] = ci.Passing
		data["FailingN"] = ci.Failing
		data["FailingChecks"] = trimmed(ci.FailingChecks)
	}
	if comments != nil {
		data["HasComments"] = true
		data["New"] = comments.New
		data["Actionable"] = comments.Actionable
	}
	addShapeFacts(data, e.Kind, c.Shape)

	switch {
	case e.EscalatedAt != nil && e.Kind == orchestrator.ReactionKindCI:
		return "held_escalated_ci", data
	case e.EscalatedAt != nil:
		return "held_escalated_reaction", data
	}

	switch e.Kind {
	case orchestrator.ReactionKindCI:
		switch {
		case ci != nil && ci.Status == "passing":
			return "wait_ci_passing", data
		case ci != nil && ci.Status == "failing":
			return "wait_ci_failing", data
		}
		return "wait_ci", data
	case orchestrator.ReactionKindReview:
		if e.Wait == "debounce" && comments != nil {
			return "wait_review_debounce", data
		}
		if comments != nil && comments.AwaitingApproval {
			return "wait_review_approval", data
		}
		return "wait_review", data
	case orchestrator.ReactionKindBotReview:
		if comments != nil && comments.Handled {
			return "wait_bot_handled", data
		}
		return "wait_bot", data
	case orchestrator.ReactionKindAutoMerge:
		return "wait_merge", data
	case orchestrator.ReactionKindMergeConflict:
		if check != nil && check.Mergeability != nil && check.Mergeability.Mergeable != nil && !*check.Mergeability.Mergeable {
			return "merge_conflict_found", data
		}
		data["Mergeability"] = mergeabilityWords[mergeabilityOf(check)]
		return "wait_merge_conflict", data
	case orchestrator.ReactionKindMergeCompletion:
		return "wait_merge_completion", data
	case orchestrator.ReactionKindLabelReview, orchestrator.ReactionKindLabelFix:
		return "wait_label_command", data
	}
	return "wait_reaction", data
}

// waitClause returns the lead of the next-check clause for an entry's Wait.
func waitClause(wait string) string {
	if clause, ok := waitClauses[wait]; ok {
		return clause
	}
	return otherWaitClause
}

func checkedCI(check *orchestrator.ReactionCheck) *orchestrator.CICheck {
	if check == nil {
		return nil
	}
	return check.CI
}

func checkedComments(check *orchestrator.ReactionCheck) *orchestrator.CommentCheck {
	if check == nil {
		return nil
	}
	return check.Comments
}

func mergeabilityOf(check *orchestrator.ReactionCheck) mergeability {
	switch {
	case check == nil || check.Mergeability == nil:
		return mergeabilityUnchecked
	case check.Mergeability.Mergeable == nil:
		return mergeabilityUnknown
	}
	return mergeabilityClear
}

// trimmed returns names with surrounding white space removed, never nil.
func trimmed(names []string) []string {
	out := make([]string, len(names))
	for i, name := range names {
		out[i] = strings.TrimSpace(name)
	}
	return out
}

// addShapeFacts adds the configuration facts the shape serves for a
// reaction kind; a nil shape adds none.
func addShapeFacts(data map[string]any, kind string, shape *orchestrator.WorkflowShape) {
	if shape == nil {
		return
	}
	data["Handoff"] = strings.TrimSpace(shape.Tracker.HandoffState)
	r := shape.Reactions
	if r.BotReview != nil {
		data["Bots"] = strings.Join(trimmed(r.BotReview.BotUsernames), ", ")
	}
	if r.AutoMerge != nil {
		data["Strategy"] = strings.TrimSpace(r.AutoMerge.Strategy)
	}
	if r.MergeCompletion != nil {
		data["Target"] = strings.TrimSpace(r.MergeCompletion.TargetState)
	}
	data["Label"] = commandLabel(kind, shape)
}

func selectBetweenStages(e *orchestrator.SnapshotStageHop) (string, map[string]any) {
	return "stage_wait", map[string]any{
		"Source": strings.TrimSpace(e.SourceRule),
		"Target": strings.TrimSpace(e.TargetRule),
		"Label":  strings.TrimSpace(e.TargetLabel),
	}
}

// holdTemplates names the template of each hold reason whose words do not
// depend on the hold's details.
var holdTemplates = map[orchestrator.SkipReason]string{
	orchestrator.SkipBlockersUnresolved:  "hold_blockers_unresolved",
	orchestrator.SkipBlockersNotRead:     "hold_blockers_not_read",
	orchestrator.SkipBlockersIncomplete:  "hold_blockers_incomplete",
	orchestrator.SkipSSHHostsFull:        "hold_ssh_hosts",
	orchestrator.SkipNoWorker:            "hold_no_worker",
	orchestrator.SkipDailySpendCeiling:   "hold_spend_daily",
	orchestrator.SkipWeeklySpendCeiling:  "hold_spend_weekly",
	orchestrator.SkipMonthlySpendCeiling: "hold_spend_monthly",
	orchestrator.SkipStageHopUnconfirmed: "hold_stage_hop",
	orchestrator.SkipUsageLimit:          "hold_usage_limit",
	orchestrator.SkipParkCommentsUnread:  "hold_park_comments",
}

func selectHold(h *orchestrator.SnapshotHold, c Context) (string, map[string]any) {
	blockers := make([]string, len(h.Blockers))
	planRule := false
	for i, blocker := range h.Blockers {
		blockers[i] = blockerWords(blocker)
		planRule = planRule || planBlockerReasons[blocker.Reason]
	}
	var slot orchestrator.SnapshotSlotHold
	if h.Slot != nil {
		slot = *h.Slot
	}
	var waiting string
	if h.Worker != nil {
		waiting = strings.Join(h.Worker.Waiting, "; ")
	}
	data := map[string]any{
		"Waiting":   waiting,
		"Blockers":  blockers,
		"Several":   len(blockers) > 1,
		"Plan":      planRule,
		"State":     strings.TrimSpace(slot.State),
		"Used":      slot.Used,
		"Limit":     slot.Limit,
		"HasReset":  h.ResetsAt.After(c.Now),
		"ResetsIn":  secondsUntil(c.Now, h.ResetsAt),
		"AgentKind": h.AgentKind,
		"ModelOnly": h.ModelOnly,
		"Model":     h.Model,
	}

	switch {
	case h.Reason == orchestrator.SkipBlockedBy:
		return "hold_blocked_by", data
	case h.Reason == orchestrator.SkipAgentSlotsFull && slot.Scope == orchestrator.SlotScopeState:
		return "hold_agent_slots_state", data
	case h.Reason == orchestrator.SkipAgentSlotsFull:
		return "hold_agent_slots_global", data
	}
	return holdTemplates[h.Reason], data
}

// planBlockerReasons are the blocker reasons only the plan-control rule
// gives.
var planBlockerReasons = map[string]bool{
	orchestrator.BlockerNotMerged:          true,
	orchestrator.BlockerAwaitingMerge:      true,
	orchestrator.BlockerCanceled:           true,
	orchestrator.BlockerNotDone:            true,
	orchestrator.BlockerUnverifiedNoChange: true,
}

// blockerReasonWords says why a blocker holds its dependent, for the
// reasons its state does not already say.
var blockerReasonWords = map[string]string{
	orchestrator.BlockerNotMerged:          "not merged",
	orchestrator.BlockerAwaitingMerge:      "no merge recorded",
	orchestrator.BlockerCanceled:           "never satisfies the issue",
	orchestrator.BlockerNotDone:            "not done",
	orchestrator.BlockerUnverifiedNoChange: "no-change claim not verified",
}

// blockerWords names a blocker by its key, its state, and why it holds.
func blockerWords(b orchestrator.SnapshotBlocker) string {
	state := strings.TrimSpace(b.State)
	if state == "" {
		state = "state unknown"
	}
	if why := blockerReasonWords[b.Reason]; why != "" {
		state += ", " + why
	}
	return cmp.Or(b.DisplayID, b.Identifier, b.IssueID) + " (" + state + ")"
}

func selectFinished(f *Finished) (string, map[string]any) {
	data := map[string]any{
		"State":     "",
		"PR":        0,
		"Target":    strings.TrimSpace(f.StageTarget),
		"HowBefore": "",
		"HowAfter":  "",
		"Why":       hopResults[f.StageResult],
	}
	o := f.Outcome
	if o == nil {
		return "ended_none", data
	}
	data["State"] = strings.TrimSpace(o.State)
	data["PR"] = o.PRNumber

	switch o.Kind {
	case "merged":
		how, ok := mergePaths[o.MergePath]
		if !ok {
			how = otherMergePath
		}
		data["HowBefore"], data["HowAfter"] = how.before, how.after
		return "ended_merged", data
	case "handed_off":
		if f.StageResult == "failed" || f.StageResult == "ceiling" {
			return "ended_not_advanced", data
		}
		return "ended_handed", data
	}
	return "ended_done", data
}
