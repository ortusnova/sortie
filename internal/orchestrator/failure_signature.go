package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"github.com/sortie-ai/sortie/internal/failcause"
	"github.com/sortie-ai/sortie/internal/redact"
)

// failureStep names the step whose error failed a run: "export" for the
// export of the issue branch, which pushes it and opens its pull request,
// and otherwise the text before the error's first ": ", such as "agent
// session start" or "agent turn 3", which the worker puts there.
func failureStep(err error) string {
	if _, ok := errors.AsType[*ExportError](err); ok {
		return "export"
	}
	head, _, found := strings.Cut(err.Error(), ": ")
	if !found {
		return "run"
	}
	return head
}

// runFailureSignature returns the signature of a run that failed on err,
// or the zero signature when err is nil.
func runFailureSignature(err error) failcause.Signature {
	if err == nil {
		return failcause.Signature{}
	}
	return failcause.Sign(failureStep(err), err)
}

// failureSignatureStore reads the signature of an issue's latest run.
type failureSignatureStore interface {
	LatestRunFailureSignature(ctx context.Context, issueID string) (string, error)
}

// repeatsPreviousFailure reports whether signature, of a run of issueID
// about to be recorded, equals the signature of the issue's previous run.
// A transient or zero signature never repeats, so its failure keeps the
// backoff. A failed read keeps it too.
func repeatsPreviousFailure(ctx context.Context, store failureSignatureStore, issueID string, signature failcause.Signature, log *slog.Logger) bool {
	if signature == (failcause.Signature{}) || signature.Transient() {
		return false
	}
	previous, err := store.LatestRunFailureSignature(ctx, issueID)
	if err != nil {
		log.Warn("failure signature read failed, keeping the retry backoff",
			slog.Any("error", err),
		)
		return false
	}
	return previous == signature.String()
}

// failureStatement is what a repeated-failure park keeps as its
// statement: the first line of the run's error, credentials masked.
func failureStatement(err error) string {
	if err == nil {
		return ""
	}
	line, _, _ := strings.Cut(strings.TrimSpace(redact.Mask(err.Error())), "\n")
	return failcause.Cap(strings.TrimSpace(line))
}
