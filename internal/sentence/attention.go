package sentence

import (
	"cmp"
	"slices"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/orchestrator"
)

// AttentionItem is one thing that needs a person, worded and ready to serve.
// Kind is parked, budget_held, reaction_escalated, workflow_reload_failed,
// preflight_failed, spend_ceiling_reached, or hold_expired; Severity is critical or warning. Since is nil only for a
// park whose stored time did not parse. AcknowledgedAt is nil while no person
// has acknowledged the item. The issue fields are empty on a process-wide item.
type AttentionItem struct {
	ID                string
	Kind              string
	Severity          string
	Since             *time.Time
	AcknowledgedAt    *time.Time
	IssueID           string
	IssueIdentifier   string
	DisplayIdentifier string
	IssueTitle        string
	IssueURL          string
	ReasonText        string
	WhyText           string
	ClearsText        string
	Action            AttentionAction
	Evidence          AttentionEvidence
}

// AttentionAction is the one thing a client offers for an item. Kind is
// open_issue, open_pull_request, or acknowledge; URL is set exactly for the
// first two.
type AttentionAction struct {
	Kind string
	URL  string
}

// AttentionEvidence holds the facts behind an item. Each member is set on the
// kinds that carry it and left zero on every other.
type AttentionEvidence struct {
	Reason             string
	Label              string
	UsedSessions       *int
	BudgetSessions     *int
	UsedTokens         *int64
	UsedTokensComplete *bool
	BudgetTokens       *int64
	UsedUSD            *float64
	UsedUSDComplete    *bool
	BudgetUSD          *float64
	ReactionKind       string
	ConfigKey          string
	PullRequest        *orchestrator.SnapshotPullRequest
	EscalationTrigger  string
	UsedAttempts       *int
	MaxAttempts        *int
	LastCheck          *orchestrator.ReactionCheck
	File               string
	Error              string
	ResetsAt           *time.Time
}

const (
	severityCritical = "critical"
	severityWarning  = "warning"
)

const (
	actionOpenIssue       = "open_issue"
	actionOpenPullRequest = "open_pull_request"
	actionAcknowledge     = "acknowledge"
)

const (
	parkReasonHandoffAbsence  = "handoff_absence"
	parkReasonRepeatedFailure = "repeated_failure"
	budgetReasonTokens        = "token_budget"
	budgetReasonMoney         = "money_budget"
	checkVerdictFailing       = "failing"
	checkVerdictPending       = "pending"
)

// escalatedTemplates names the attention template of each escalating reaction
// kind. A kind it lacks renders the fallback text and keeps its item.
var escalatedTemplates = map[string]string{
	orchestrator.ReactionKindCI:              "attn_escalated_ci",
	orchestrator.ReactionKindReview:          "attn_escalated_review",
	orchestrator.ReactionKindBotReview:       "attn_escalated_bot_review",
	orchestrator.ReactionKindMergeConflict:   "attn_escalated_merge_conflict",
	orchestrator.ReactionKindAutoMerge:       "attn_escalated_merge",
	orchestrator.ReactionKindMergeCompletion: "attn_escalated_merge_completion",
}

// Attention returns the items that need a person, ordered by severity, then
// since with unknown times first, then id; the list is never nil. It words the
// causes [orchestrator.AttentionCauses] derives and attaches the time a person
// acknowledged each one. A nil health leaves out the process-wide items. It
// reads only its arguments and is safe for concurrent use.
func Attention(snap orchestrator.RuntimeSnapshotResult, health *orchestrator.SnapshotHealth, c Context) []AttentionItem {
	causes := orchestrator.AttentionCauses(snap, health)
	items := make([]AttentionItem, 0, len(causes))
	for _, cause := range causes {
		item := attentionItem(cause, c)
		if at, found := snap.AttentionAcknowledgements[item.ID]; found {
			item.AcknowledgedAt = new(at)
		}
		items = append(items, item)
	}
	slices.SortFunc(items, compareAttention)
	return items
}

// attentionItem words one cause. A kind it does not know yields an item with
// the cause's identity and no wording.
func attentionItem(cause orchestrator.AttentionCause, c Context) AttentionItem {
	switch cause.Kind {
	case orchestrator.AttentionKindParked:
		return parkedItem(cause)
	case orchestrator.AttentionKindBudgetHeld:
		return budgetItem(cause)
	case orchestrator.AttentionKindReactionEscalated:
		return escalatedReactionItem(cause, c)
	case orchestrator.AttentionKindWorkflowReloadFailed:
		return workflowReloadItem(cause)
	case orchestrator.AttentionKindPreflightFailed:
		return preflightItem(cause)
	case orchestrator.AttentionKindSpendCeilingReached:
		return spendCeilingItem(cause)
	case orchestrator.AttentionKindHoldExpired:
		return holdExpiredItem(cause)
	}
	return AttentionItem{ID: cause.ID, Kind: cause.Kind, Since: cause.Since}
}

func parkedItem(cause orchestrator.AttentionCause) AttentionItem {
	e := cause.Parked
	template := "attn_parked_agent_blocked"
	switch e.Reason {
	case parkReasonHandoffAbsence:
		template = "attn_parked_handoff_absence"
	case parkReasonRepeatedFailure:
		template = "attn_parked_repeated_failure"
	}
	reason, why, clears := attentionTexts(template, map[string]any{
		"Label":  strings.TrimSpace(e.Label),
		"Unseen": !e.LabelApplied,
		"State":  strings.TrimSpace(e.ParkedState),
	})
	return AttentionItem{
		ID:                cause.ID,
		Kind:              cause.Kind,
		Severity:          severityWarning,
		Since:             cause.Since,
		IssueID:           e.IssueID,
		IssueIdentifier:   e.Identifier,
		DisplayIdentifier: e.DisplayID,
		IssueTitle:        e.IssueTitle,
		IssueURL:          e.IssueURL,
		ReasonText:        reason,
		WhyText:           why,
		ClearsText:        clears,
		Action:            issueAction(e.IssueURL),
		Evidence:          AttentionEvidence{Reason: e.Reason, Label: e.Label},
	}
}

func budgetItem(cause orchestrator.AttentionCause) AttentionItem {
	e := cause.Budget
	evidence := AttentionEvidence{Reason: e.Reason}
	var template string
	var data map[string]any
	stopped := 0
	if e.StoppedInFlight != nil {
		stopped = *e.StoppedInFlight
	}
	switch e.Reason {
	case budgetReasonTokens:
		used, atLeast := heldTokenSpend(e)
		template = "attn_budget_tokens"
		data = map[string]any{
			"Used":    used,
			"AtLeast": atLeast,
			"Budget":  new(e.BudgetTokens),
			"Stopped": stopped,
		}
		evidence.UsedTokens = e.UsedTokens
		evidence.UsedTokensComplete = new(!atLeast)
		evidence.BudgetTokens = new(e.BudgetTokens)
	case budgetReasonMoney:
		var used float64
		if e.UsedUSD != nil {
			used = *e.UsedUSD
		}
		complete := e.UsedUSDComplete != nil && *e.UsedUSDComplete
		template = "attn_budget_money"
		data = map[string]any{
			"Used":    used,
			"AtLeast": !complete,
			"Budget":  e.BudgetUSD,
			"Stopped": stopped,
		}
		evidence.UsedUSD = e.UsedUSD
		evidence.UsedUSDComplete = e.UsedUSDComplete
		evidence.BudgetUSD = new(e.BudgetUSD)
	default:
		template = "attn_budget_sessions"
		data = map[string]any{
			"UsedSessions":   e.UsedSessions,
			"BudgetSessions": e.BudgetSessions,
			"Above":          e.UsedSessions - e.GrantedSessions,
		}
		evidence.UsedSessions = new(e.UsedSessions)
		evidence.BudgetSessions = new(e.BudgetSessions)
	}
	reason, why, clears := attentionTexts(template, data)
	return AttentionItem{
		ID:                cause.ID,
		Kind:              cause.Kind,
		Severity:          severityWarning,
		Since:             cause.Since,
		IssueID:           e.IssueID,
		IssueIdentifier:   e.Identifier,
		DisplayIdentifier: e.DisplayID,
		IssueTitle:        e.IssueTitle,
		IssueURL:          e.IssueURL,
		ReasonText:        reason,
		WhyText:           why,
		ClearsText:        clears,
		Action:            AttentionAction{Kind: actionAcknowledge},
		Evidence:          evidence,
	}
}

func escalatedReactionItem(cause orchestrator.AttentionCause, c Context) AttentionItem {
	e := cause.Reaction
	data := map[string]any{
		"PR":            e.PullRequest.Number,
		"Trigger":       e.EscalationTrigger,
		"Tries":         0,
		"HasMax":        false,
		"Max":           0,
		"Latest":        "",
		"FailingChecks": []string{},
		"FailingN":      0,
		"Handoff":       "",
		"Target":        "",
	}
	if e.UsedAttempts != nil {
		data["Tries"] = *e.UsedAttempts
	}
	if e.MaxAttempts != nil {
		data["HasMax"] = true
		data["Max"] = *e.MaxAttempts
	}
	if ci := checkedCI(e.LastCheck); ci != nil {
		if ci.Status == checkVerdictFailing || ci.Status == checkVerdictPending {
			data["Latest"] = ci.Status
		}
		data["FailingChecks"] = trimmed(ci.FailingChecks)
		data["FailingN"] = ci.Failing
	}
	addShapeFacts(data, e.Kind, c.Shape)

	reason, why, clears := attentionTexts(escalatedTemplates[e.Kind], data)
	pullRequest := e.PullRequest
	return AttentionItem{
		ID:                cause.ID,
		Kind:              cause.Kind,
		Severity:          severityWarning,
		Since:             cause.Since,
		IssueID:           e.IssueID,
		IssueIdentifier:   e.Identifier,
		DisplayIdentifier: e.DisplayID,
		IssueTitle:        e.IssueTitle,
		IssueURL:          e.IssueURL,
		ReasonText:        reason,
		WhyText:           why,
		ClearsText:        clears,
		Action:            escalationAction(*e),
		Evidence: AttentionEvidence{
			ReactionKind:      e.Kind,
			ConfigKey:         e.ConfigKey,
			PullRequest:       &pullRequest,
			EscalationTrigger: e.EscalationTrigger,
			UsedAttempts:      e.UsedAttempts,
			MaxAttempts:       e.MaxAttempts,
			LastCheck:         e.LastCheck,
		},
	}
}

func workflowReloadItem(cause orchestrator.AttentionCause) AttentionItem {
	w := cause.Workflow
	return failingCheckItem(cause, "attn_reload",
		map[string]any{"File": w.File, "Error": strings.TrimSpace(w.Error)},
		AttentionEvidence{File: w.File, Error: w.Error})
}

func preflightItem(cause orchestrator.AttentionCause) AttentionItem {
	check := cause.Preflight
	leadless := strings.TrimSpace(strings.TrimPrefix(check.Error, orchestrator.PreflightFailedPrefix))
	return failingCheckItem(cause, "attn_preflight",
		map[string]any{"Error": leadless},
		AttentionEvidence{Error: check.Error})
}

func holdExpiredItem(cause orchestrator.AttentionCause) AttentionItem {
	h := cause.Hold
	reason, why, clears := attentionTexts("attn_hold_expired", map[string]any{
		"By":       strings.TrimSpace(h.Holder),
		"Reason":   strings.TrimSpace(h.Reason),
		"Evidence": strings.TrimSpace(h.Evidence),
		"Until":    h.Until.UTC().Format(time.RFC3339),
	})
	return AttentionItem{
		ID:              cause.ID,
		Kind:            cause.Kind,
		Severity:        severityWarning,
		Since:           cause.Since,
		IssueID:         h.IssueID,
		IssueIdentifier: h.Identifier,
		ReasonText:      reason,
		WhyText:         why,
		ClearsText:      clears,
		Action:          AttentionAction{Kind: actionAcknowledge},
		Evidence:        AttentionEvidence{Reason: h.Reason},
	}
}

func spendCeilingItem(cause orchestrator.AttentionCause) AttentionItem {
	status := cause.Spend
	return failingCheckItem(cause, "attn_spend_"+string(status.Window),
		map[string]any{"Used": status.UsedUSD, "AtLeast": !status.UsedComplete, "Budget": status.BudgetUSD},
		AttentionEvidence{
			Reason:          string(status.Window.SkipReason()),
			ConfigKey:       status.Window.Setting(),
			UsedUSD:         new(status.UsedUSD),
			UsedUSDComplete: new(status.UsedComplete),
			BudgetUSD:       new(status.BudgetUSD),
			ResetsAt:        new(status.ResetsAt.UTC()),
		})
}

// failingCheckItem builds a process-wide item, which no issue owns and only an
// acknowledgement answers.
func failingCheckItem(cause orchestrator.AttentionCause, template string, data map[string]any, evidence AttentionEvidence) AttentionItem {
	reason, why, clears := attentionTexts(template, data)
	return AttentionItem{
		ID:         cause.ID,
		Kind:       cause.Kind,
		Severity:   severityCritical,
		Since:      cause.Since,
		ReasonText: reason,
		WhyText:    why,
		ClearsText: clears,
		Action:     AttentionAction{Kind: actionAcknowledge},
		Evidence:   evidence,
	}
}

// attentionTexts renders the three texts of an attention template. A part that
// fails to render reads as the fallback text, so the item stays.
func attentionTexts(template string, data map[string]any) (reason, why, clears string) {
	return render(template, "reason", data), render(template, "why", data), render(template, "clears", data)
}

// issueAction opens the issue when it has a URL and otherwise asks for an
// acknowledgement.
func issueAction(issueURL string) AttentionAction {
	if issueURL == "" {
		return AttentionAction{Kind: actionAcknowledge}
	}
	return AttentionAction{Kind: actionOpenIssue, URL: issueURL}
}

// escalationAction opens the pull request an escalation names, except for
// merge completion, whose pull request has merged and whose open work is the
// issue.
func escalationAction(e orchestrator.SnapshotPendingReaction) AttentionAction {
	if e.Kind != orchestrator.ReactionKindMergeCompletion && e.PullRequest.URL != "" {
		return AttentionAction{Kind: actionOpenPullRequest, URL: e.PullRequest.URL}
	}
	return issueAction(e.IssueURL)
}

func severityRank(severity string) int {
	if severity == severityCritical {
		return 0
	}
	return 1
}

func compareAttention(a, b AttentionItem) int {
	return cmp.Or(
		cmp.Compare(severityRank(a.Severity), severityRank(b.Severity)),
		compareSince(a.Since, b.Since),
		cmp.Compare(a.ID, b.ID),
	)
}

// compareSince orders an unknown time before every known one.
func compareSince(a, b *time.Time) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	}
	return a.Compare(*b)
}
