package clientprotocol

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/sortie-ai/sortie/internal/agent/jsonrpc"
	"github.com/sortie-ai/sortie/internal/redact"
)

// quoteJSONRPCError renders e for display only; no behavior may depend
// on its Message or Data.
func quoteJSONRPCError(e *jsonrpc.Error) string {
	if e == nil {
		return ""
	}
	text := e.Message
	if data := formatErrorData(e.Data); data != "" {
		text += ": " + data
	}
	return redact.Truncate(text, messageTruncateLimit)
}

// errorObjectError is the error object the agent answered a call with,
// as the cause of the agent error the call fails with. Its text
// is the display form; [errorObjectError.FailureOutput] keeps the whole
// object for the run record.
type errorObjectError struct {
	method string
	object *jsonrpc.Error
}

func newErrorObjectError(method string, object *jsonrpc.Error) *errorObjectError {
	return &errorObjectError{method: method, object: object}
}

func (e *errorObjectError) Error() string {
	return fmt.Sprintf("%s error %d: %s", e.method, e.object.Code, quoteJSONRPCError(e.object))
}

// FailureOutput returns the error object as the agent sent it, its data
// member included and uncut. It is not masked.
func (e *errorObjectError) FailureOutput() string {
	encoded, err := json.Marshal(e.object)
	if err != nil {
		encoded = fmt.Appendf(nil, "%+v", *e.object)
	}
	return e.method + " error object: " + string(encoded)
}

func formatErrorData(data json.RawMessage) string {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || string(trimmed) == "null" {
		return ""
	}
	var s string
	if err := json.Unmarshal(trimmed, &s); err == nil {
		return s
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, trimmed); err == nil {
		return compact.String()
	}
	return string(trimmed)
}

// sessionUpdateKind identifies which of the eleven pinned session/update
// variants a decoded notification carried.
type sessionUpdateKind int

const (
	updateUnknown sessionUpdateKind = iota
	updateAgentMessageChunk
	updateAgentThoughtChunk
	updateUserMessageChunk
	updateToolCall
	updateToolCallUpdate
	updatePlan
	updateAvailableCommands
	updateCurrentMode
	updateConfigOption
	updateSessionInfo
	updateUsage
)

// sessionUpdateEvent is the internal, per-variant decoding of one
// session/update notification's payload. Exactly the fields the
// recognized kind needs are populated; the rest hold their zero value.
type sessionUpdateEvent struct {
	kind       sessionUpdateKind
	rawVariant string

	chunk          contentChunk
	toolCallBegin  toolCall
	toolCallUpdate toolCallUpdate
	usage          usageUpdate
}

// parseSessionUpdate decodes the discriminator of raw, the undecoded
// "update" payload of one session/update notification, and then decodes
// only the fields the recognized variant needs. An unrecognized
// discriminator value, or a discriminator that does not decode at all,
// reports found=false rather than an error: the caller records the "any
// other value" row of the normalization table rather than failing the
// message, because the pinned schema itself expects a variant published
// after the pin to arrive undescribed.
//
// A recognized variant whose payload does not decode reports found=false
// for the same reason it reports it for an unrecognized one. Decoding a
// payload partially and reporting success would normalize whatever was
// recovered before the failure: an empty message chunk that still marks
// work observed, or a tool call registered under an empty identifier.
func parseSessionUpdate(raw json.RawMessage) (event sessionUpdateEvent, found bool) {
	var probe struct {
		SessionUpdate string `json:"sessionUpdate"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return sessionUpdateEvent{}, false
	}

	event.rawVariant = probe.SessionUpdate

	switch probe.SessionUpdate {
	case sessionUpdateAgentMessageChunk:
		event.kind = updateAgentMessageChunk
		if err := json.Unmarshal(raw, &event.chunk); err != nil {
			return event, false
		}
	case sessionUpdateAgentThoughtChunk:
		event.kind = updateAgentThoughtChunk
		if err := json.Unmarshal(raw, &event.chunk); err != nil {
			return event, false
		}
	case sessionUpdateUserMessageChunk:
		event.kind = updateUserMessageChunk
		if err := json.Unmarshal(raw, &event.chunk); err != nil {
			return event, false
		}
	case sessionUpdateToolCall:
		event.kind = updateToolCall
		if err := json.Unmarshal(dropMalformedToolCallName(raw), &event.toolCallBegin); err != nil {
			return event, false
		}
	case sessionUpdateToolCallUpdate:
		event.kind = updateToolCallUpdate
		if err := json.Unmarshal(dropMalformedToolCallName(raw), &event.toolCallUpdate); err != nil {
			return event, false
		}
	case sessionUpdatePlan:
		event.kind = updatePlan
	case sessionUpdateAvailableCommandsUpdate:
		event.kind = updateAvailableCommands
	case sessionUpdateCurrentModeUpdate:
		event.kind = updateCurrentMode
	case sessionUpdateConfigOptionUpdate:
		event.kind = updateConfigOption
	case sessionUpdateSessionInfoUpdate:
		event.kind = updateSessionInfo
	case sessionUpdateUsageUpdate:
		event.kind = updateUsage
		if err := json.Unmarshal(raw, &event.usage); err != nil {
			return event, false
		}
	default:
		event.kind = updateUnknown
		return event, false
	}

	return event, true
}

// dropMalformedToolCallName drops a wrong-typed "name" member so it decodes
// as absent rather than failing the tool call that carries it.
func dropMalformedToolCallName(object json.RawMessage) json.RawMessage {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(object, &fields); err != nil {
		return object
	}
	name, present := fields["name"]
	if !present {
		return object
	}
	trimmed := bytes.TrimSpace(name)
	if len(trimmed) > 0 && (trimmed[0] == '"' || string(trimmed) == "null") {
		return object
	}
	delete(fields, "name")
	adapted, err := json.Marshal(fields)
	if err != nil {
		return object
	}
	return adapted
}
