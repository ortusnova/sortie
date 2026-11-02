package orchestrator

import (
	"context"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/failcause"
	"github.com/sortie-ai/sortie/internal/prompt"
)

// StoryFailure is the failure a story-fault retry hands to the new
// session of the story's agent: the failed step, the first line of the
// error, and the whole output of the failing step, credentials masked.
type StoryFailure struct {
	Step   string
	Error  string
	Output string
}

// storyFailureOf returns the hand-over of a run that failed on err.
func storyFailureOf(err error) *StoryFailure {
	if err == nil {
		return nil
	}
	return &StoryFailure{
		Step:   failureStep(err),
		Error:  failureStatement(err),
		Output: failcause.Of(err),
	}
}

// storyFailureOfRetry returns the hand-over a story-fault retry dispatches
// with: its Failure, or for a retry restored from the store, its error.
// It reports false for any other retry.
func storyFailureOfRetry(e *RetryEntry) (prompt.FailureHandOver, bool) {
	if e == nil || e.OriginTrigger != triggerStoryFault {
		return prompt.FailureHandOver{}, false
	}
	if e.Failure != nil {
		return prompt.FailureHandOver{Step: e.Failure.Step, Error: e.Failure.Error, Output: e.Failure.Output}, true
	}
	line := strings.TrimPrefix(e.Error, WorkerExitedErrorPrefix)
	return prompt.FailureHandOver{Error: failcause.Cap(strings.TrimSpace(line))}, true
}

type failureRenderCtxKey struct{}

// withFailureRender returns a child context carrying the failure the
// worker hands to the first turn of its session.
func withFailureRender(ctx context.Context, f prompt.FailureHandOver) context.Context {
	return context.WithValue(ctx, failureRenderCtxKey{}, f)
}

// failureRenderFrom extracts the value injected by [withFailureRender] and
// reports whether one was injected.
func failureRenderFrom(ctx context.Context) (prompt.FailureHandOver, bool) {
	f, ok := ctx.Value(failureRenderCtxKey{}).(prompt.FailureHandOver)
	return f, ok
}

// faultHoldOf returns the hold of the issue entry ran for, held at now.
func faultHoldOf(entry *RunningEntry, result WorkerResult, now time.Time) PlatformFaultHold {
	return PlatformFaultHold{
		IssueID:    result.IssueID,
		Identifier: result.Identifier,
		DisplayID:  entry.Issue.DisplayID,
		HeldAt:     now,
	}
}
