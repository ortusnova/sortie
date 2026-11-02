package orchestrator

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/failcause"
	"github.com/sortie-ai/sortie/internal/redact"
)

// runtimeErrorObject stands in for an agent runtime's error object as
// the cause of an agent error.
type runtimeErrorObject struct{ object string }

func (e *runtimeErrorObject) Error() string         { return "session/new error -32603: Internal error" }
func (e *runtimeErrorObject) FailureOutput() string { return "session/new error object: " + e.object }

func TestHandleWorkerExit_RecordsTheAgentRuntimesErrorObject(t *testing.T) {
	t.Parallel()

	const secret = "orchestrator-error-object-secret-7f3"
	redact.Add("test.orchestrator error object", secret)
	object := `{"code":-32603,"message":"Internal error","data":{"errorName":"ServiceUnavailableError","key":"` + secret + `","trace":"` + strings.Repeat("t", 2*failcause.MaxBytes) + `"}}`
	cause := &runtimeErrorObject{object: object}
	store := &mockExitStore{}
	state := exitState(t, "CAUSE-1", nil)

	HandleWorkerExit(state, WorkerResult{
		IssueID:      "CAUSE-1",
		Identifier:   "CAUSE-1",
		ExitKind:     WorkerExitError,
		AgentAdapter: "mock",
		Error: fmt.Errorf("agent session start: %w", &domain.AgentError{
			Kind: domain.ErrResponseError, Message: cause.Error(), Err: cause,
		}),
	}, defaultExitParams(t, store))
	t.Cleanup(func() { CancelRetry(state, "CAUSE-1") })

	if len(store.runHistories) != 1 {
		t.Fatalf("run history rows = %d, want 1", len(store.runHistories))
	}
	detail := store.runHistories[0].ErrorDetail
	if !strings.HasPrefix(detail, `session/new error object: {"code":-32603,"message":"Internal error","data":{"errorName":"ServiceUnavailableError","key":"`+redact.Marker+`"`) {
		t.Errorf("ErrorDetail starts %q, want the error object with the key masked", detail[:min(len(detail), 160)])
	}
	if strings.Contains(detail, secret) {
		t.Error("ErrorDetail holds the secret, want it masked")
	}
	if len(detail) > failcause.MaxBytes || !strings.HasSuffix(detail, `"}}`) {
		t.Errorf("len(ErrorDetail) = %d, ends %q; want at most %d bytes ending with the object's end", len(detail), detail[max(0, len(detail)-8):], failcause.MaxBytes)
	}
}

func TestHandleWorkerExit_NoErrorDetailWithoutOutput(t *testing.T) {
	t.Parallel()

	store := &mockExitStore{}
	state := exitState(t, "CAUSE-2", nil)
	HandleWorkerExit(state, WorkerResult{
		IssueID:      "CAUSE-2",
		Identifier:   "CAUSE-2",
		ExitKind:     WorkerExitError,
		AgentAdapter: "mock",
		Error:        errors.New("something transient"),
	}, defaultExitParams(t, store))
	t.Cleanup(func() { CancelRetry(state, "CAUSE-2") })

	if len(store.runHistories) != 1 || store.runHistories[0].ErrorDetail != "" {
		t.Errorf("run history = %+v, want one row without an error detail", store.runHistories)
	}
}
