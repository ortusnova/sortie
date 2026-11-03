package clientprotocol

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/sortie-ai/sortie/internal/agent/agentcore"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/redact"
)

// nonTextChunkMessage stands in for a message chunk whose content block is
// not text. The chunk is work the agent did, so the turn still observes
// work, but there is no text to show an operator and an empty notification
// would read as the agent having said nothing.
const nonTextChunkMessage = "agent sent a message chunk this client does not render"

// messageTruncateLimit bounds every agent-produced message this adapter
// emits, matching the shared per-adapter convention.
const messageTruncateLimit = 500

// normalizedUpdate is what applySessionUpdate returns for one recognized
// session/update variant: the event to publish, if any.
type normalizedUpdate struct {
	event        domain.AgentEvent
	hasEvent     bool
	messageChunk bool
	messageID    *messageId
	messageText  string
}

// applySessionUpdate normalizes one decoded session/update variant,
// mutating tracker for the two variants that begin or end a tool call. A
// tool call that begins in progress is published as tool_started, so the
// orchestrator knows the agent is inside it while it emits nothing.
// tracker is the session's own tool tracker; the pump is its only
// caller, so no synchronization is needed here.
func applySessionUpdate(tracker *agentcore.ToolTracker, ev sessionUpdateEvent) normalizedUpdate {
	now := time.Now().UTC()

	switch ev.kind {
	case updateAgentMessageChunk:
		text := chunkText(ev.chunk)
		if text == "" {
			return normalizedUpdate{
				event:        domain.AgentEvent{Type: domain.EventMalformed, Timestamp: now, Message: nonTextChunkMessage},
				hasEvent:     true,
				messageChunk: true,
				messageID:    ev.chunk.MessageID,
			}
		}
		return normalizedUpdate{
			event: domain.AgentEvent{
				Type:      domain.EventNotification,
				Timestamp: now,
			},
			hasEvent:     true,
			messageChunk: true,
			messageID:    ev.chunk.MessageID,
			messageText:  text,
		}

	case updateAgentThoughtChunk:
		return normalizedUpdate{
			event:    domain.AgentEvent{Type: domain.EventOtherMessage, Timestamp: now},
			hasEvent: true,
		}

	case updateUserMessageChunk:
		// No event on the pinned line: this variant is replay evidence
		// for session continuation, which the pump observes directly
		// rather than through this function's return value.
		return normalizedUpdate{}

	case updateToolCall:
		id := string(ev.toolCallBegin.ToolCallID)
		call := toolCallOf(ev.toolCallBegin)
		tracker.Begin(id, call)
		if status := ev.toolCallBegin.Status; status != nil &&
			(*status == toolCallStatusCompleted || *status == toolCallStatusFailed) {
			return normalizedUpdate{}
		}
		return normalizedUpdate{
			event: domain.AgentEvent{
				Type:         domain.EventToolStarted,
				Timestamp:    now,
				ToolName:     call.Name,
				ToolKind:     call.Kind,
				ToolArgument: call.Argument,
				ToolCallID:   id,
			},
			hasEvent: true,
		}

	case updateToolCallUpdate:
		if ev.toolCallUpdate.Status == nil {
			return normalizedUpdate{}
		}
		switch *ev.toolCallUpdate.Status {
		case toolCallStatusCompleted, toolCallStatusFailed:
			call, durationMS, ok := tracker.End(string(ev.toolCallUpdate.ToolCallID))
			if !ok {
				return normalizedUpdate{}
			}
			var title string
			if ev.toolCallUpdate.Title != nil {
				title = *ev.toolCallUpdate.Title
			}
			return normalizedUpdate{
				event: domain.AgentEvent{
					Type:           domain.EventToolResult,
					Timestamp:      now,
					ToolName:       call.Name,
					ToolKind:       call.Kind,
					ToolArgument:   call.Argument,
					ToolDurationMS: durationMS,
					ToolError:      *ev.toolCallUpdate.Status == toolCallStatusFailed,
					ToolCallID:     string(ev.toolCallUpdate.ToolCallID),
					Message:        redact.Truncate(title, messageTruncateLimit),
				},
				hasEvent: true,
			}
		default:
			// Any other status: recorded for the tracker only, per the
			// normalization table; there is nothing further to record
			// here because the tracker's own state already reflects it
			// by construction (begin/end are the only transitions this
			// adapter reads).
			return normalizedUpdate{}
		}

	case updatePlan:
		return normalizedUpdate{
			event:    domain.AgentEvent{Type: domain.EventOtherMessage, Timestamp: now},
			hasEvent: true,
		}

	case updateAvailableCommands, updateCurrentMode, updateConfigOption, updateSessionInfo, updateUsage:
		// Debug log only, per the normalization table; the caller does
		// not emit an event for these.
		return normalizedUpdate{}

	default:
		return normalizedUpdate{}
	}
}

// chunkText extracts the text of a content chunk's content block. It
// returns "" for anything other than the text content block this
// transport reads and writes; the adapter never sends or expects image,
// audio, or resource content.
func chunkText(chunk contentChunk) string {
	if chunk.Content.Type != contentBlockText {
		return ""
	}
	var text textContent
	if err := json.Unmarshal(chunk.Content.Remainder, &text); err != nil {
		return ""
	}
	return text.Text
}

// normalizeToolKind maps a tool call's declared kind to the closed set of
// ACP kinds, substituting "other" for an absent or unrecognized value.
func normalizeToolKind(kind *toolKind) string {
	if kind == nil {
		return string(toolKindOther)
	}
	switch *kind {
	case toolKindRead, toolKindEdit, toolKindDelete, toolKindMove, toolKindSearch,
		toolKindExecute, toolKindThink, toolKindFetch, toolKindSwitchMode, toolKindOther:
		return string(*kind)
	default:
		return string(toolKindOther)
	}
}

// toolCallOf maps a tool_call begin payload to a tool call. The call is named
// by its normalized ACP kind, except other and switch_mode, which name none.
// The main argument is the first location's path for a file kind and the
// title for execute, because the protocol defines no command member.
func toolCallOf(begin toolCall) agentcore.ToolCall {
	kind := normalizeToolKind(begin.Kind)
	call := agentcore.ToolCall{Name: kind}
	switch toolKind(kind) {
	case toolKindOther, toolKindSwitchMode:
		call.Name = ""
	case toolKindRead:
		call.Kind = domain.ToolKindRead
		call.Argument = firstLocationPath(begin.Locations)
	case toolKindEdit, toolKindDelete, toolKindMove:
		call.Kind = domain.ToolKindEdit
		call.Argument = firstLocationPath(begin.Locations)
	case toolKindExecute:
		call.Kind = domain.ToolKindShell
		call.Argument = begin.Title
	}
	return call
}

func firstLocationPath(locations *[]toolCallLocation) string {
	if locations == nil || len(*locations) == 0 {
		return ""
	}
	return (*locations)[0].Path
}

// stopReasonEvidence maps promptResponse.StopReason to the shared turn
// evidence, per the pinned line's five stop reasons plus the two
// conditions that precede this call (a JSON-RPC error response and a
// lost connection, both handled by their own callers). The two
// cancelled-stop-reason rows that depend on who cancelled are resolved
// by the caller through activeTurn.pendingEnd, which overrides whatever
// this function returns; the case handled here is the third row: nobody
// cancelled, which is a nonconformant answer reported as failed.
func stopReasonEvidence(reason stopReason) agentcore.TurnEvidence {
	switch reason {
	case stopReasonEndTurn:
		return agentcore.TurnEvidence{Terminal: agentcore.TerminalSuccess}
	case stopReasonRefusal:
		return agentcore.TurnEvidence{
			Terminal:          agentcore.TerminalFailure,
			TerminalErrorKind: domain.ErrTurnRefused,
		}
	case stopReasonMaxTokens:
		return agentcore.TurnEvidence{
			Terminal:          agentcore.TerminalFailure,
			TerminalErrorKind: domain.ErrTurnTokenLimit,
		}
	case stopReasonMaxTurnRequests:
		return agentcore.TurnEvidence{
			Terminal:          agentcore.TerminalFailure,
			TerminalErrorKind: domain.ErrTurnRequestLimit,
		}
	case stopReasonCancelled:
		return agentcore.TurnEvidence{
			Terminal:          agentcore.TerminalFailure,
			TerminalErrorKind: domain.ErrTurnFailed,
			TerminalMessage:   cancelledWithNoCancellationMessage,
		}
	default:
		return agentcore.TurnEvidence{
			Terminal:          agentcore.TerminalFailure,
			TerminalErrorKind: domain.ErrTurnOutcomeUnknown,
			TerminalMessage: redact.Truncate(
				fmt.Sprintf("agent reported an unrecognized stop reason: %q", string(reason)),
				messageTruncateLimit,
			),
		}
	}
}

// cancelledWithNoCancellationMessage is the compile-time constant
// message for a "cancelled" stop reason that neither the orchestrator
// nor this adapter's own end-attempt sequence induced.
const cancelledWithNoCancellationMessage = "agent reported a cancelled stop reason without a cancellation on either side"
