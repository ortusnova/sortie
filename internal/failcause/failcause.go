// Package failcause keeps the whole cause of a failed step for the run
// record: the output the failing step gave, such as a git command's
// porcelain output and standard error or an agent runtime's error
// object, capped in size and with every registered credential masked.
package failcause

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/sortie-ai/sortie/internal/redact"
)

// MaxBytes caps the text [Of] returns.
const MaxBytes = 16 << 10

// Carrier is an error that holds the whole output of the step that
// failed. An error type implements it without importing this package.
type Carrier interface {
	error

	// FailureOutput returns the step's output as the step gave it. It
	// may hold credentials; [Of] masks them.
	FailureOutput() string
}

// Of returns the output of the first [Carrier] in err's chain, with
// every registered credential masked and cut to at most [MaxBytes]
// bytes, keeping its start and its end. It returns "" when no error in
// the chain is a Carrier or the output is blank.
func Of(err error) string {
	if err == nil {
		return ""
	}
	var c Carrier
	if !errors.As(err, &c) {
		return ""
	}
	return Cap(redact.Mask(strings.TrimSpace(c.FailureOutput())))
}

// Cap returns s when it holds at most [MaxBytes] bytes; otherwise its
// start and its end, each cut on a UTF-8 boundary, around a line naming
// how many bytes were left out, MaxBytes bytes at most in all. Mask s
// before capping it, so a cut never splits a credential.
func Cap(s string) string {
	if len(s) <= MaxBytes {
		return s
	}
	// The marker line never exceeds this many bytes.
	const markerBudget = 48
	keep := (MaxBytes - markerBudget) / 2
	head := keep
	for head > 0 && !utf8.RuneStart(s[head]) {
		head--
	}
	tail := len(s) - keep
	for tail < len(s) && !utf8.RuneStart(s[tail]) {
		tail++
	}
	return s[:head] + fmt.Sprintf("\n[%d bytes cut]\n", tail-head) + s[tail:]
}
