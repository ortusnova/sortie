package render

import (
	"cmp"
	"strings"

	"github.com/sortie-ai/sortie/internal/domain"
)

func builtin(n domain.Notification) Text {
	title, body := n.Message.Title, n.Message.Body
	if n.Message.Facts != nil {
		title = string(n.Envelope.EventType)
		if subject := cmp.Or(n.Envelope.DisplayID, n.Envelope.Identifier, n.Envelope.IssueID); subject != "" {
			title = subject + ": " + title
		}
		body = bodyOf(n)
	}

	operator := "[" + strings.ToUpper(n.Message.Severity) + "] " + title + "\n" + body
	public := body
	if agentText := n.Message.AgentText; agentText != "" {
		operator += "\n\n" + agentText
		public += "\n\n" + agentText
	}
	return Text{
		Title:     title,
		Body:      body,
		AgentText: n.Message.AgentText,
		Operator:  operator,
		Public:    public,
	}
}

// bodyOf returns the body for the facts n carries. A facts type with no
// body function renders an empty body.
func bodyOf(n domain.Notification) string {
	switch f := n.Message.Facts.(type) {
	case domain.SessionStartedFacts:
		return startedBody
	case domain.SessionCompletedFacts:
		return completionBody(f)
	case domain.SessionStoppedFacts:
		return softStopBody(f)
	case domain.SessionFailedFacts:
		return failureBody(f)
	case domain.CIFailureFacts:
		return ciEscalationBody(f)
	case domain.TriageEscalationFacts:
		return triageEscalationBody(n.Envelope.EventType, f)
	case domain.ReviewEscalationFacts:
		return reviewEscalationBody(f)
	case domain.BotReviewEscalationFacts:
		return botReviewEscalationBody(f)
	case domain.MergeConflictFacts:
		return mergeConflictEscalationBody(f)
	case domain.AutoMergeEscalationFacts:
		return autoMergeEscalationBody(f)
	case domain.AutoMergedFacts:
		return autoMergedBody(f)
	case domain.MergeCompletionFacts:
		return mergeCompletionBody(f)
	case domain.MergeCompletionMissingSHAFacts:
		return mergeCompletionMissingSHABody(f)
	case domain.BudgetHeldFacts:
		return budgetHeldBody(f)
	case domain.SessionEscalatedFacts:
		return sessionEscalatedBody(f)
	case domain.StoryRevertedFacts:
		return storyRevertedBody(f)
	case domain.HoldExpiredFacts:
		return holdExpiredBody(f)
	case domain.SupervisorLoopSuspectedFacts:
		return loopSuspectedBody(f)
	case domain.SupervisorUnavailableFacts:
		return supervisorUnavailableBody(f)
	case domain.SupervisorAvailableFacts:
		return supervisorAvailableBody(f)
	case domain.StageFacts:
		return stageBody(n.Envelope.EventType, n.Envelope.Stage)
	default:
		return ""
	}
}
