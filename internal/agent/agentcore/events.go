package agentcore

import (
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/redact"
)

// EmitSessionStarted emits an EventSessionStarted event with the given agent
// process ID and session identifier. agentPID is set directly on AgentPID;
// pass "" when the process ID is unavailable. Subprocess adapters (Claude,
// Copilot) pass strconv.Itoa(cmd.Process.Pid) after cmd.Start; persistent-
// process adapters (Codex) pass session.AgentPID directly.
func EmitSessionStarted(emit func(domain.AgentEvent), agentPID string, sessionID string) {
	emit(domain.AgentEvent{
		Type:      domain.EventSessionStarted,
		Timestamp: time.Now().UTC(),
		AgentPID:  agentPID,
		SessionID: sessionID,
		Message:   "session started",
	})
}

// EmitTurnCompleted emits an EventTurnCompleted event. message is a
// human-readable summary; use "" when unavailable. apiDurationMS is the LLM
// API response wait time in milliseconds for this turn; use 0 when
// unavailable. usage is the session's run-cumulative token usage snapshot;
// the zero value means the caller reports no usage for this event.
// byModel is usage's per-model split, or nil.
func EmitTurnCompleted(emit func(domain.AgentEvent), message string, apiDurationMS int64, usage domain.TokenUsage, byModel map[string]domain.TokenUsage) {
	emit(domain.AgentEvent{
		Type:          domain.EventTurnCompleted,
		Timestamp:     time.Now().UTC(),
		Message:       message,
		APIDurationMS: apiDurationMS,
		Usage:         usage,
		UsageByModel:  byModel,
	})
}

// EmitTurnFailed emits an EventTurnFailed event. message describes the
// failure. apiDurationMS is attached to the event; use 0 when unavailable.
// usage is the session's run-cumulative token usage snapshot; the zero
// value means the caller reports no usage for this event. byModel is
// usage's per-model split, or nil.
func EmitTurnFailed(emit func(domain.AgentEvent), message string, apiDurationMS int64, usage domain.TokenUsage, byModel map[string]domain.TokenUsage) {
	emit(domain.AgentEvent{
		Type:          domain.EventTurnFailed,
		Timestamp:     time.Now().UTC(),
		Message:       message,
		APIDurationMS: apiDurationMS,
		Usage:         usage,
		UsageByModel:  byModel,
	})
}

// EmitTurnCancelled emits an EventTurnCancelled event with the given
// human-readable message. usage is the session's run-cumulative token
// usage snapshot; the zero value means the caller reports no usage for
// this event. byModel is usage's per-model split, or nil.
func EmitTurnCancelled(emit func(domain.AgentEvent), message string, usage domain.TokenUsage, byModel map[string]domain.TokenUsage) {
	emit(domain.AgentEvent{
		Type:         domain.EventTurnCancelled,
		Timestamp:    time.Now().UTC(),
		Message:      message,
		Usage:        usage,
		UsageByModel: byModel,
	})
}

// EmitTurnInputRequired emits an EventTurnInputRequired event with the
// given human-readable message. usage is the session's run-cumulative
// token usage snapshot; the zero value means the caller reports no usage
// for this event. byModel is usage's per-model split, or nil.
func EmitTurnInputRequired(emit func(domain.AgentEvent), message string, usage domain.TokenUsage, byModel map[string]domain.TokenUsage) {
	emit(domain.AgentEvent{
		Type:         domain.EventTurnInputRequired,
		Timestamp:    time.Now().UTC(),
		Message:      message,
		Usage:        usage,
		UsageByModel: byModel,
	})
}

// EmitMalformed emits an EventMalformed event. line is the raw unparseable
// bytes from the agent output stream; it is truncated to 500 Unicode code
// points before inclusion in the event message.
func EmitMalformed(emit func(domain.AgentEvent), line []byte) {
	emit(domain.AgentEvent{
		Type:      domain.EventMalformed,
		Timestamp: time.Now().UTC(),
		Message:   redact.Truncate(string(line), 500),
	})
}

// EmitNotification emits an EventNotification event with the given
// informational message. message may be empty for stall-timer-reset
// notifications.
func EmitNotification(emit func(domain.AgentEvent), message string) {
	emit(domain.AgentEvent{
		Type:      domain.EventNotification,
		Timestamp: time.Now().UTC(),
		Message:   message,
	})
}

// EmitToolStarted emits an EventToolStarted event for the tool call id,
// observed at the given instant. An empty id emits nothing, since no
// tool_result could end the call.
func EmitToolStarted(emit func(domain.AgentEvent), at time.Time, id string, call ToolCall) {
	if id == "" {
		return
	}
	emit(domain.AgentEvent{
		Type:         domain.EventToolStarted,
		Timestamp:    at,
		ToolName:     call.Name,
		ToolKind:     call.Kind,
		ToolArgument: call.Argument,
		ToolCallID:   id,
	})
}

// EmitUsageWindows emits an EventNotification event with an empty message
// that carries the given usage windows, observed at the given instant.
func EmitUsageWindows(emit func(domain.AgentEvent), at time.Time, windows []domain.UsageWindow) {
	emit(domain.AgentEvent{
		Type:         domain.EventNotification,
		Timestamp:    at,
		UsageWindows: windows,
	})
}
