package clientprotocol

import (
	"testing"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/waittest"
)

// A turn whose agent begins a tool call publishes tool_started before the
// call ends, so the orchestrator knows the agent is inside the call while it
// sends nothing, and the tool_result that ends it carries the same call ID.
func TestTurnPublishesToolStartedBeforeItsToolResult(t *testing.T) {
	t.Parallel()

	state, outPr, inPw := newTestSessionWithLogger(t, domain.AgentConfig{}, clientProtocolMaxLineBytes, discardLogger())
	out := newOutboundReader(outPr)
	publishHandshake(state, &fakeUsageReader{})
	markSessionKnown(state)

	started := make(chan domain.AgentEvent, 1)
	var events []domain.AgentEvent
	collect := collectEvents(&events)
	outcomeCh := runTurnAsync(state, domain.RunTurnParams{Prompt: "go", OnEvent: func(e domain.AgentEvent) {
		if e.Type == domain.EventToolStarted {
			started <- e
		}
		collect(e)
	}})

	promptID := out.awaitMethod(t, methodSessionPrompt)
	sendLine(t, inPw, `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sess-test","update":`+
		`{"sessionUpdate":"tool_call","toolCallId":"call_long","title":"make seed","kind":"execute","status":"in_progress"}}}`)

	// The call is still open: nothing ended it yet.
	begun := waittest.Receive(t, started, "tool_started event")
	if begun.ToolCallID != "call_long" || begun.ToolKind != domain.ToolKindShell || begun.ToolArgument != "make seed" {
		t.Errorf("tool_started = %+v, want call_long, a shell call, argument %q", begun, "make seed")
	}

	sendLine(t, inPw, `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"sess-test","update":`+
		`{"sessionUpdate":"tool_call_update","toolCallId":"call_long","status":"completed","title":"make seed"}}}`)
	respondLine(t, inPw, promptID, promptResponse{StopReason: stopReasonEndTurn})
	if outcome := waittest.Receive(t, outcomeCh, "turn outcome"); outcome.err != nil {
		t.Fatalf("RunTurn() error = %v, want nil", outcome.err)
	}

	startedAt, resultAt := -1, -1
	for i, e := range events {
		switch e.Type {
		case domain.EventToolStarted:
			startedAt = i
		case domain.EventToolResult:
			resultAt = i
			if e.ToolCallID != "call_long" {
				t.Errorf("tool_result ToolCallID = %q, want %q", e.ToolCallID, "call_long")
			}
		}
	}
	if startedAt < 0 || resultAt < 0 || startedAt > resultAt {
		t.Errorf("events = %+v, want tool_started (at %d) before tool_result (at %d)", events, startedAt, resultAt)
	}
}
