// Package webhook implements [domain.Notifier] for a generic outbound
// HTTP webhook. It posts a normalized notification as a JSON object to
// an operator-configured URL, using the generic notifier vocabulary so
// the body is backend-neutral. This is an outbound POST backend and is
// distinct from inbound tracker webhook ingress.
package webhook

import (
	"context"
	"fmt"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/notify/notifyhttp"
	"github.com/sortie-ai/sortie/internal/notify/render"
	"github.com/sortie-ai/sortie/internal/registry"
	"github.com/sortie-ai/sortie/internal/typeutil"
)

var _ domain.Notifier = (*notifier)(nil)

func init() {
	registry.Notifiers.Register("webhook", newNotifier)
}

// notifier posts notifications to a configured webhook URL.
type notifier struct {
	client *notifyhttp.Client
}

// newNotifier constructs a webhook [domain.Notifier] from the entry's
// pass-through config. It requires a non-empty url; an absent or empty
// value (including a secret reference that resolved to the empty string)
// is rejected.
func newNotifier(config map[string]any) (domain.Notifier, error) {
	endpoint, err := typeutil.RequiredString(config, "url")
	if err != nil {
		return nil, fmt.Errorf("webhook notifier: %w", err)
	}
	client := notifyhttp.NewClient(endpoint)
	return &notifier{client: client}, nil
}

// wirePayload is the JSON object posted to the webhook endpoint. Field
// names use the generic notifier vocabulary so any machine consumer can
// correlate and route without backend-specific knowledge.
type wirePayload struct {
	NotificationID string `json:"notification_id"`
	Timestamp      string `json:"timestamp"`
	Source         string `json:"source"`
	IssueID        string `json:"issue_id"`
	Identifier     string `json:"identifier"`
	DispatchID     string `json:"dispatch_id"`
	SessionID      string `json:"session_id"`
	Attempt        *int   `json:"attempt"`
	Agent          string `json:"agent"`
	Severity       string `json:"severity"`
	Title          string `json:"title"`
	Body           string `json:"body"`
	Category       string `json:"category,omitempty"`

	// EventType and AgentText are set only for an orchestrator event, so
	// an agent message payload keeps its original field set.
	EventType string `json:"event_type,omitempty"`
	AgentText string `json:"agent_text,omitempty"`

	// Stage is set only for a stage event, so every other payload keeps
	// its field set.
	Stage *wireStage `json:"stage,omitempty"`

	// Escalation is set only for a session.escalated event.
	Escalation *wireEscalation `json:"escalation,omitempty"`

	// Supervisor is set only for a supervisor.unavailable,
	// supervisor.available, or supervisor.loop_suspected event.
	Supervisor *wireSupervisor `json:"supervisor,omitempty"`
}

// wireSupervisor is what a supervisor event reports besides its text:
// the reason and reset of a supervisor.unavailable or
// supervisor.available event, and the hourly spend of a
// supervisor.loop_suspected event for a spend velocity.
type wireSupervisor struct {
	Reason       string `json:"reason,omitempty"`
	ResetsAt     string `json:"resets_at,omitempty"`
	TriggerKind  string `json:"trigger_kind,omitempty"`
	HourlyTokens int64  `json:"hourly_tokens,omitempty"`
	HourlyLimit  int64  `json:"hourly_limit,omitempty"`
	Pace         int64  `json:"pace,omitempty"`
}

// supervisorOf returns what the supervisor event facts report, nil for
// any other facts.
func supervisorOf(facts domain.MessageFacts) *wireSupervisor {
	switch f := facts.(type) {
	case domain.SupervisorUnavailableFacts:
		w := &wireSupervisor{Reason: f.Reason}
		if !f.ResetsAt.IsZero() {
			w.ResetsAt = f.ResetsAt.UTC().Format(time.RFC3339)
		}
		return w
	case domain.SupervisorAvailableFacts:
		return &wireSupervisor{Reason: f.Reason}
	case domain.SupervisorLoopSuspectedFacts:
		return &wireSupervisor{TriggerKind: f.TriggerKind, HourlyTokens: f.HourlyTokens, HourlyLimit: f.HourlyLimit, Pace: f.Pace}
	}
	return nil
}

// wireEscalation is what a session.escalated event reports besides its
// text, which is AgentText.
type wireEscalation struct {
	Kind string `json:"kind"`
	Rule string `json:"rule"`
	URL  string `json:"url,omitempty"`
}

// wireStage is the hop decision a stage event reports.
type wireStage struct {
	SourceRule string `json:"source_rule"`
	TargetRule string `json:"target_rule"`
	ChainID    string `json:"chain_id"`
	HopCount   int    `json:"hop_count"`
	Reason     string `json:"reason,omitempty"`
}

// Send posts the notification as a JSON object and returns nil on any
// 2xx response. A non-2xx response or a transport failure yields a
// classified error that omits the URL, request body, and response body.
func (n *notifier) Send(ctx context.Context, notification domain.Notification) error {
	text := render.Notification(notification)
	payload := wirePayload{
		NotificationID: notification.Envelope.NotificationID,
		Timestamp:      notification.Envelope.Timestamp,
		Source:         notification.Envelope.Source,
		IssueID:        notification.Envelope.IssueID,
		Identifier:     notification.Envelope.Identifier,
		DispatchID:     notification.Envelope.DispatchID,
		SessionID:      notification.Envelope.SessionID,
		Attempt:        notification.Envelope.Attempt,
		Agent:          notification.Envelope.Agent,
		Severity:       notification.Message.Severity,
		Title:          text.Title,
		Body:           text.Body,
		Category:       notification.Message.Category,
	}
	if notification.Envelope.EventType.FromOrchestrator() {
		payload.EventType = string(notification.Envelope.EventType)
		payload.AgentText = text.AgentText
	}
	if stage := notification.Envelope.Stage; stage != (domain.StageTransition{}) {
		payload.Stage = &wireStage{
			SourceRule: stage.SourceRule,
			TargetRule: stage.TargetRule,
			ChainID:    stage.ChainID,
			HopCount:   stage.HopCount,
			Reason:     stage.Reason,
		}
	}

	if esc := notification.Envelope.Escalation; esc != (domain.SessionEscalation{}) {
		payload.Escalation = &wireEscalation{Kind: esc.Kind, Rule: esc.Rule, URL: esc.URL}
	}
	payload.Supervisor = supervisorOf(notification.Message.Facts)

	return n.client.PostJSON(ctx, payload)
}
