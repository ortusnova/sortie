package sentence

import (
	"slices"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/orchestrator"
	"github.com/sortie-ai/sortie/internal/pricing"
)

// NextOutcome is one thing Sortie does next for an issue: the condition that
// leads to it and what Sortie then does.
type NextOutcome struct {
	Condition string // capitalized, no trailing period
	Text      string // one or two sentences, each ending with a period
}

// Situation is what NextOutcomes reads of one issue.
type Situation struct {
	Running          *orchestrator.SnapshotRunningEntry
	Retry            *orchestrator.SnapshotRetryEntry
	BetweenStages    *orchestrator.SnapshotStageHop
	PendingReactions []orchestrator.SnapshotPendingReaction // the issue's rows
	Spend            *Spend                                 // nil when run history was not read
}

// Spend is the issue's completed sessions and their summed uncached tokens,
// the figures the session and token budget gates compare, and the issue's
// estimated cost in USD, which the money budget gate compares.
// GrantedSessions is the sessions an operator granted the issue beyond
// agent.max_sessions.
type Spend struct {
	Sessions        int
	Tokens          int64
	CostUSD         float64
	GrantedSessions int
}

// debounceWait is the Wait value of a review row that waits for comments to
// settle.
const debounceWait = "debounce"

// pairedKinds is the order in which pending reactions yield outcomes.
var pairedKinds = []string{
	orchestrator.ReactionKindCI,
	orchestrator.ReactionKindReview,
	orchestrator.ReactionKindBotReview,
	orchestrator.ReactionKindMergeConflict,
	orchestrator.ReactionKindAutoMerge,
	orchestrator.ReactionKindMergeCompletion,
	orchestrator.ReactionKindLabelReview,
	orchestrator.ReactionKindLabelFix,
}

// NextOutcomes returns what Sortie does next for the issue s describes, in
// selection order; never nil. Safe for concurrent use.
//
// A pair whose template fails to render is left out and logged.
func NextOutcomes(s Situation, c Context) []NextOutcome {
	outcomes := []NextOutcome{}
	if c.Shape == nil {
		return outcomes
	}
	for _, p := range selectOutcomes(s, c) {
		condition, ok := renderMasked(p.condition, "condition", p.data)
		if !ok {
			continue
		}
		text, ok := renderMasked(p.text, "text", p.data)
		if !ok {
			continue
		}
		outcomes = append(outcomes, NextOutcome{Condition: condition, Text: text})
	}
	return outcomes
}

// outcomePair names the condition and text templates of one outcome and holds
// the data both read.
type outcomePair struct {
	condition string
	text      string
	data      map[string]any
}

func selectOutcomes(s Situation, c Context) []outcomePair {
	var pairs []outcomePair
	switch {
	case s.Running != nil:
		if s.Running.ReactionKind == "" {
			pairs = append(pairs, runPairs(s, c)...)
		}
	case s.Retry != nil:
		if !s.Retry.Paused {
			pairs = append(pairs, retryPair(s, c))
		}
	case s.BetweenStages != nil:
		if pair, ok := betweenStagesPair(s, c); ok {
			pairs = append(pairs, pair)
		}
	}
	for _, kind := range pairedKinds {
		for _, row := range s.PendingReactions {
			if row.Kind != kind {
				continue
			}
			if pair, ok := reactionPair(row, s, c); ok {
				pairs = append(pairs, pair)
			}
		}
	}
	return pairs
}

// newData returns the data map of one pair with every key a next outcome
// template reads set to its zero value, since a missing key fails the render.
func newData() map[string]any {
	return map[string]any{
		"PR":              0,
		"Window":          0.0,
		"Settle":          false,
		"Spent":           false,
		"RequireCI":       false,
		"Label":           "",
		"Next":            "",
		"Stage":           "",
		"ActiveState":     false,
		"AtCeiling":       false,
		"Hops":            0,
		"Ceiling":         0,
		"Handoff":         "",
		"NoChange":        "",
		"Watch":           []string{},
		"Budget":          new(int64),
		"MoneyHold":       false,
		"TokenHold":       false,
		"Sessions":        0,
		"MaxSessions":     0,
		"Run":             "",
		"Runs":            "",
		"Restart":         false,
		"N":               0,
		"Used":            0,
		"Max":             0,
		"Debounce":        0.0,
		"Escalation":      "",
		"EscalationLabel": "",
		"ConfigKey":       "",
		"Strategy":        "",
		"DeleteBranch":    false,
		"State":           "",
		"What":            "",
		"Trigger":         "",
		"RunInline":       "",
		"Attempt":         0,
		"Ahead":           "",
		"AheadTrigger":    "",
		"AheadRunInline":  "",
		"AheadAttempt":    0,
	}
}

// budgetHold is the budget gate that would stop a session dispatch: the money
// budget, the token budget, or the session budget with the sessions it counts.
type budgetHold struct {
	money    bool
	tokens   bool
	sessions int
}

// holdOf repeats the gates every session dispatch passes. extra counts the
// sessions that will have landed in run history before the dispatch is gated,
// beyond the running one. The money budget wins over the token budget, which
// wins over the session budget.
func holdOf(s Situation, c Context, extra int) (budgetHold, bool) {
	if s.Spend == nil {
		return budgetHold{}, false
	}
	tokens := s.Spend.Tokens
	sessions := s.Spend.Sessions + extra
	if s.Running != nil {
		sessions++
		if s.Running.UsageMeasured {
			tokens += s.Running.UncachedTokens()
		}
	}
	agent := c.Shape.Agent
	switch {
	case agent.MaxCostUSD != nil && pricing.CeilingReached(s.Spend.CostUSD, *agent.MaxCostUSD):
		return budgetHold{money: true, sessions: sessions}, true
	case agent.MaxTokens != nil && tokens >= int64(*agent.MaxTokens):
		return budgetHold{tokens: true, sessions: sessions}, true
	case agent.MaxSessions != nil && sessions >= *agent.MaxSessions+s.Spend.GrantedSessions:
		return budgetHold{sessions: sessions}, true
	}
	return budgetHold{}, false
}

// textOrHold returns the text template of a dispatch: held, with the hold
// recorded in data, when a budget gate would stop it, else free.
func textOrHold(s Situation, c Context, extra int, data map[string]any, free, held string) string {
	hold, stopped := holdOf(s, c, extra)
	if !stopped {
		return free
	}
	data["MoneyHold"] = hold.money
	data["TokenHold"] = hold.tokens
	data["Sessions"] = hold.sessions
	setBudgets(data, c.Shape)
	if c.Shape.Agent.MaxSessions != nil {
		data["MaxSessions"] = *c.Shape.Agent.MaxSessions + s.Spend.GrantedSessions
	}
	return held
}

// setBudgets records the budgets the shape sets for the templates that name
// them.
func setBudgets(data map[string]any, shape *orchestrator.WorkflowShape) {
	if shape.Agent.MaxTokens != nil {
		data["Budget"] = new(int64(*shape.Agent.MaxTokens))
	}
	if shape.Agent.MaxSessions != nil {
		data["MaxSessions"] = *shape.Agent.MaxSessions
	}
}

func ruleNamed(shape *orchestrator.WorkflowShape, name string) *orchestrator.ShapeRule {
	if name == "" {
		return nil
	}
	for i := range shape.Dispatch.Rules {
		if shape.Dispatch.Rules[i].Name == name {
			return &shape.Dispatch.Rules[i]
		}
	}
	return nil
}

// hopTarget returns the rule that a clean exit of ruleName hops to, or nil
// when the rule names no next rule or that rule carries no stage label.
func hopTarget(shape *orchestrator.WorkflowShape, ruleName string) *orchestrator.ShapeRule {
	source := ruleNamed(shape, ruleName)
	if source == nil || source.Next == "" {
		return nil
	}
	target := ruleNamed(shape, source.Next)
	if target == nil || target.Stage == "" {
		return nil
	}
	return target
}

func runPairs(s Situation, c Context) []outcomePair {
	r := s.Running
	shape := c.Shape
	handoff := strings.TrimSpace(shape.Tracker.HandoffState)
	queued := s.Retry != nil
	target := hopTarget(shape, r.RuleName)
	hops := 0
	if r.StagePosition != nil {
		hops = r.StagePosition.HopCount
	}

	var pairs []outcomePair
	succeeds := outcomePair{condition: "next_if_run_succeeds", data: newData()}
	switch {
	case handoff == "":
		if !queued {
			succeeds.text = textOrHold(s, c, 0, succeeds.data, "next_continue", "next_held")
			pairs = append(pairs, succeeds)
		}
	case target == nil:
		recordHandoff(succeeds.data, shape, handoff)
		succeeds.text = "next_handoff"
		pairs = append(pairs, succeeds)
	case hops >= shape.Dispatch.HopCeiling:
		recordHandoff(succeeds.data, shape, handoff)
		succeeds.data["AtCeiling"] = true
		succeeds.data["Hops"] = hops
		succeeds.data["Ceiling"] = shape.Dispatch.HopCeiling
		succeeds.data["Next"] = strings.TrimSpace(target.Name)
		succeeds.text = "next_handoff"
		pairs = append(pairs, succeeds)
	default:
		succeeds.data["Label"] = strings.TrimSpace(target.Stage)
		succeeds.data["Next"] = strings.TrimSpace(target.Name)
		succeeds.text = textOrHold(s, c, 0, succeeds.data, "next_stage", "next_stage_held")
		pairs = append(pairs, succeeds)
	}

	if !queued {
		fails := outcomePair{condition: "next_if_run_fails", data: newData()}
		fails.text = textOrHold(s, c, 0, fails.data, "next_backoff", "next_held")
		pairs = append(pairs, fails)
	}

	if shape.Agent.MaxCostUSD != nil && r.UsageArrival.ReportsAnyFigure() {
		pairs = append(pairs, outcomePair{condition: "next_if_money_reach", text: "next_money_stop", data: newData()})
	}
	if shape.Agent.MaxTokens != nil && r.UsageArrival.ReportsAnyFigure() {
		stops := outcomePair{condition: "next_if_tokens_reach", text: "next_token_stop", data: newData()}
		setBudgets(stops.data, shape)
		pairs = append(pairs, stops)
	}
	return pairs
}

// recordHandoff records the handoff state, the state for a run that changed
// nothing, and the reactions that watch the pull request a run leaves.
func recordHandoff(data map[string]any, shape *orchestrator.WorkflowShape, handoff string) {
	data["Handoff"] = handoff
	noChange := strings.TrimSpace(shape.Tracker.NoChangeState)
	if !strings.EqualFold(noChange, handoff) {
		data["NoChange"] = noChange
	}
	watch := []string{}
	for _, w := range watchNames {
		if _, present := memberNamed(w.configKey, shape.Reactions); present {
			watch = append(watch, w.name)
		}
	}
	data["Watch"] = watch
}

func retryPair(s Situation, c Context) outcomePair {
	e := s.Retry
	data := newData()
	data["Trigger"] = e.Trigger
	data["RunInline"] = retryRunInline(e)
	data["Attempt"] = e.Attempt
	if p := e.StagePosition; p != nil {
		data["Stage"] = strings.TrimSpace(p.Current)
	}
	data["ActiveState"] = e.ReactionKind == ""
	return outcomePair{
		condition: "next_when_retry_due",
		text:      textOrHold(s, c, 0, data, "next_dispatch", "next_held"),
		data:      data,
	}
}

// retryRunInline returns the inline phrase of the run a reaction retry with no
// trigger starts, or "" for any other retry.
func retryRunInline(e *orchestrator.SnapshotRetryEntry) string {
	if e.Trigger != "" {
		return ""
	}
	return runPhrases[e.ReactionKind].inline
}

func betweenStagesPair(s Situation, c Context) (outcomePair, bool) {
	hop := s.BetweenStages
	rule := strings.TrimSpace(hop.TargetRule)
	label := strings.TrimSpace(hop.TargetLabel)
	holdsRule := slices.ContainsFunc(c.Shape.Dispatch.Rules, func(r orchestrator.ShapeRule) bool {
		return r.Name == rule && strings.EqualFold(strings.TrimSpace(r.Stage), label)
	})
	if !holdsRule {
		return outcomePair{}, false
	}
	data := newData()
	data["Label"] = label
	data["Next"] = rule
	return outcomePair{
		condition: "next_when_label_read",
		text:      textOrHold(s, c, 0, data, "next_stage_start", "next_held"),
		data:      data,
	}, true
}

// reactionMember is the member of the shape's reactions a pending row names.
// The pointer members are set for the kinds that read more than the common
// fields.
type reactionMember struct {
	triage     bool
	debounceMS int
	escalation orchestrator.ShapeEscalation
	autoMerge  *orchestrator.ShapeAutoMerge
	completion *orchestrator.ShapeMergeCompletion
}

// memberNamed returns the reactions member configured under key, and false
// when the shape lacks it.
func memberNamed(key string, r orchestrator.ShapeReactions) (reactionMember, bool) {
	switch key {
	case "ci_failure":
		if m := r.CIFailure; m != nil {
			return reactionMember{triage: m.Triage, escalation: m.ShapeEscalation}, true
		}
	case "review_comments":
		if m := r.ReviewComments; m != nil {
			return reactionMember{triage: m.Triage, debounceMS: m.DebounceMS, escalation: m.ShapeEscalation}, true
		}
	case "bot_review":
		if m := r.BotReview; m != nil {
			return reactionMember{triage: m.Triage, escalation: m.ShapeEscalation}, true
		}
	case "merge_conflicts":
		if m := r.MergeConflicts; m != nil {
			return reactionMember{triage: m.Triage, escalation: m.ShapeEscalation}, true
		}
	case "auto_merge":
		if m := r.AutoMerge; m != nil {
			return reactionMember{escalation: m.ShapeEscalation, autoMerge: m}, true
		}
	case "merge_completion":
		if m := r.MergeCompletion; m != nil {
			return reactionMember{escalation: m.ShapeEscalation, completion: m}, true
		}
	case "label_commands":
		return reactionMember{}, r.LabelCommands != nil
	}
	return reactionMember{}, false
}

// timeLeft returns how long a check can still lead to the row's outcome, and
// false for a row with no watch end. A review set that a check finds is acted
// on only after the debounce and on the next poll tick, so that lag comes off.
func timeLeft(row orchestrator.SnapshotPendingReaction, member reactionMember, c Context) (time.Duration, bool) {
	if row.WatchEndsAt == nil {
		return 0, false
	}
	var lag time.Duration
	if row.Kind == orchestrator.ReactionKindReview {
		lag = time.Duration(c.Shape.Polling.IntervalMS) * time.Millisecond
		if row.Wait != debounceWait {
			lag += time.Duration(member.debounceMS) * time.Millisecond
		}
	}
	return row.WatchEndsAt.Sub(c.Now) - lag, true
}

func reactionPair(row orchestrator.SnapshotPendingReaction, s Situation, c Context) (outcomePair, bool) {
	member, present := memberNamed(row.ConfigKey, c.Shape.Reactions)
	if !present || row.EscalatedAt != nil {
		return outcomePair{}, false
	}
	data := newData()
	if spansPullRequests(s.PendingReactions) {
		data["PR"] = row.PullRequest.Number
	}
	if left, watched := timeLeft(row, member, c); watched {
		if left < time.Second {
			return outcomePair{}, false
		}
		data["Window"] = left.Seconds()
	}

	used := 0
	if row.UsedAttempts != nil {
		used = *row.UsedAttempts
	}
	spent := row.MaxAttempts != nil && used >= *row.MaxAttempts
	data["Used"] = used
	data["N"] = used + 1
	if row.MaxAttempts != nil {
		data["Max"] = *row.MaxAttempts
	}
	recordFixWords(data, row, member)

	switch row.Kind {
	case orchestrator.ReactionKindCI:
		return fixPair("next_if_ci_fails", data, s, c, fixOrder{triageFirst: true, extra: 1, triage: member.triage, spent: spent}), true
	case orchestrator.ReactionKindReview:
		settle := row.Wait == debounceWait
		data["Settle"] = settle
		if !settle {
			data["Debounce"] = float64(member.debounceMS) / 1000
		}
		return fixPair("next_if_review_comments", data, s, c, fixOrder{triage: member.triage, spent: spent}), true
	case orchestrator.ReactionKindBotReview:
		data["Spent"] = spent
		return fixPair("next_if_bot_comments", data, s, c, fixOrder{triage: member.triage, spent: spent}), true
	case orchestrator.ReactionKindMergeConflict:
		return fixPair("next_if_merge_conflict", data, s, c, fixOrder{triageFirst: true, triage: member.triage, spent: spent}), true
	case orchestrator.ReactionKindAutoMerge:
		if spent || member.autoMerge == nil {
			return outcomePair{}, false
		}
		data["RequireCI"] = member.autoMerge.RequireCI
		data["Strategy"] = strings.TrimSpace(member.autoMerge.Strategy)
		data["DeleteBranch"] = member.autoMerge.DeleteBranch
		return outcomePair{condition: "next_when_merge_ready", text: "next_auto_merge", data: data}, true
	case orchestrator.ReactionKindMergeCompletion:
		if member.completion == nil {
			return outcomePair{}, false
		}
		data["State"] = strings.TrimSpace(member.completion.TargetState)
		return outcomePair{condition: "next_when_pr_merges", text: "next_merge_completion", data: data}, true
	case orchestrator.ReactionKindLabelReview, orchestrator.ReactionKindLabelFix:
		return labelCommandPair(row, s, c, data)
	}
	return outcomePair{}, false
}

// spansPullRequests reports whether rows watch more than one pull request,
// the only case where a next step must name which one it means.
func spansPullRequests(rows []orchestrator.SnapshotPendingReaction) bool {
	return slices.ContainsFunc(rows, func(r orchestrator.SnapshotPendingReaction) bool {
		return r.PullRequest.Number != rows[0].PullRequest.Number
	})
}

// recordFixWords records the wording of the fix a kind runs and the escalation
// its pass applies once the tries are used up.
func recordFixWords(data map[string]any, row orchestrator.SnapshotPendingReaction, member reactionMember) {
	words, ok := fixVocabulary[row.Kind]
	if !ok {
		return
	}
	data["Run"] = words.run
	data["Runs"] = words.runs
	data["Restart"] = words.restart
	data["Escalation"] = member.escalation.Escalation
	data["EscalationLabel"] = strings.TrimSpace(member.escalation.EscalationLabel)
	data["ConfigKey"] = row.ConfigKey
}

// fixOrder is what a fix reaction's pass decides before it dispatches: triage
// and the used-up count in the order its pass checks them. extra counts the
// session rows that land before the fix is gated by the session budget.
type fixOrder struct {
	triageFirst bool
	extra       int
	triage      bool
	spent       bool
}

func fixPair(condition string, data map[string]any, s Situation, c Context, order fixOrder) outcomePair {
	var text string
	switch {
	case order.triageFirst && order.triage:
		text = "next_triage"
	case order.spent:
		text = "next_escalate"
	case order.triage:
		text = "next_triage"
	default:
		text = textOrHold(s, c, order.extra, data, "next_fix", "next_held")
	}
	return outcomePair{condition: condition, text: text, data: data}
}

// labelCommandPair words a label command that waits behind the retry or
// session ahead of it, or has no pair when it would collapse into a command of
// its own kind or has no label.
func labelCommandPair(row orchestrator.SnapshotPendingReaction, s Situation, c Context, data map[string]any) (outcomePair, bool) {
	label := commandLabel(row.Kind, c.Shape)
	if label == "" {
		return outcomePair{}, false
	}
	data["Label"] = label
	data["What"] = labelCommandWords[row.Kind]

	extra := 0
	switch {
	case s.Retry != nil:
		if s.Retry.ReactionKind == row.Kind {
			return outcomePair{}, false
		}
		extra = 1
		data["Ahead"] = "retry"
		data["AheadTrigger"] = s.Retry.Trigger
		data["AheadRunInline"] = retryRunInline(s.Retry)
		data["AheadAttempt"] = s.Retry.Attempt
	case s.Running != nil:
		if s.Running.ReactionKind == row.Kind {
			return outcomePair{}, false
		}
		data["Ahead"] = "run"
	}
	return outcomePair{
		condition: "next_if_label_added",
		text:      textOrHold(s, c, extra, data, "next_label_command", "next_held"),
		data:      data,
	}, true
}
