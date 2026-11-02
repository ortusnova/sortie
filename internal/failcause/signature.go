package failcause

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"unicode"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/redact"
)

// Class names the kind of a failure: one of the transient classes below,
// the class an error in the chain names for itself, the normalized kind
// of an adapter error, or [ClassOther].
type Class string

// The transient classes. A failure of one of them is expected to clear
// on its own, so a repeat of it keeps the retry backoff.
const (
	// ClassNetworkTimeout is a network operation that timed out: a dial,
	// a TLS handshake, a read, or an HTTP client deadline.
	ClassNetworkTimeout Class = "network_timeout"

	// ClassServerError is a 5xx answer from a server.
	ClassServerError Class = "server_error"

	// ClassRateLimit is a request refused for its rate: an HTTP 429, or
	// an answer that names a rate limit.
	ClassRateLimit Class = "rate_limit"

	// ClassUsageLimit is a provider usage-limit refusal, which has its
	// own wait for the reported reset.
	ClassUsageLimit Class = "usage_limit"
)

// ClassOther is the class of a failure nothing in its chain classifies.
const ClassOther Class = "error"

// TransientClasses lists every transient class.
var TransientClasses = []Class{ClassNetworkTimeout, ClassServerError, ClassRateLimit, ClassUsageLimit}

// Transient reports whether c is one of [TransientClasses].
func (c Class) Transient() bool {
	switch c {
	case ClassNetworkTimeout, ClassServerError, ClassRateLimit, ClassUsageLimit:
		return true
	}
	return false
}

// Classer is an error that names its own failure class, such as
// "git_push". An error type implements it without importing this
// package.
type Classer interface {
	error

	// FailureClass returns the class; empty leaves the error
	// unclassified.
	FailureClass() string
}

var (
	// statusPattern finds a 5xx status beside a word that marks it as one,
	// as in "server error 502", "returned error: 503" or "HTTP 504".
	statusPattern = regexp.MustCompile(`\b(?:status|error|code|http(?:/\d(?:\.\d)?)?)\W{0,3}(5\d\d)\b`)

	// tooManyPattern finds a 429 status beside a word that marks it as one.
	tooManyPattern = regexp.MustCompile(`\b(?:status|error|code|http(?:/\d(?:\.\d)?)?)\W{0,3}429\b`)
)

var serverErrorPhrases = []string{
	"bad gateway", "service unavailable", "serviceunavailable", "service_unavailable",
	"gateway timeout", "gateway time-out", "internal server error",
}

var rateLimitPhrases = []string{"rate limit", "rate-limit", "rate_limit", "ratelimit", "too many requests"}

var timeoutPhrases = []string{
	"i/o timeout", "tls handshake timeout", "connection timed out", "operation timed out",
	"timeout awaiting response headers", "client.timeout exceeded",
}

// Classify returns the class of err. The transient classes come first:
// a usage-limit refusal; an HTTP status a [*domain.TrackerError] in the
// chain carries; a network timeout in the chain; then the error's text,
// for a 5xx answer, a rate limit or a network timeout reported only in
// words. Otherwise it returns the class the first [Classer] in the chain
// names, then the kind of the first agent, SCM, tracker or CI error, then
// [ClassOther]. A nil err is [ClassOther].
func Classify(err error) Class {
	if err == nil {
		return ClassOther
	}
	if agentErr, ok := errors.AsType[*domain.AgentError](err); ok && agentErr.Kind == domain.ErrUsageLimit {
		return ClassUsageLimit
	}
	if trackerErr, ok := errors.AsType[*domain.TrackerError](err); ok {
		switch {
		case trackerErr.Status == http.StatusTooManyRequests:
			return ClassRateLimit
		case trackerErr.Status >= 500 && trackerErr.Status <= 599:
			return ClassServerError
		}
	}
	if isNetworkTimeout(err) {
		return ClassNetworkTimeout
	}
	if class := classifyText(err.Error()); class != "" {
		return class
	}
	if classer, ok := errors.AsType[Classer](err); ok {
		if class := strings.TrimSpace(classer.FailureClass()); class != "" {
			return Class(class)
		}
	}
	if agentErr, ok := errors.AsType[*domain.AgentError](err); ok {
		return Class(agentErr.Kind)
	}
	if scmErr, ok := errors.AsType[*domain.SCMError](err); ok {
		return Class(scmErr.Kind)
	}
	if trackerErr, ok := errors.AsType[*domain.TrackerError](err); ok {
		return Class(trackerErr.Kind)
	}
	if ciErr, ok := errors.AsType[*domain.CIError](err); ok {
		return Class(ciErr.Kind)
	}
	return ClassOther
}

// isNetworkTimeout reports a timed-out network operation in err's chain.
// A bare context deadline is not one: it is how Sortie's own run and turn
// limits end a step.
func isNetworkTimeout(err error) bool {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	netErr, ok := errors.AsType[net.Error](err)
	return ok && netErr.Timeout()
}

// classifyText returns the transient class err's text reports, or "".
func classifyText(text string) Class {
	lower := strings.ToLower(text)
	if containsAny(lower, rateLimitPhrases) || tooManyPattern.MatchString(lower) {
		return ClassRateLimit
	}
	if containsAny(lower, serverErrorPhrases) || statusPattern.MatchString(lower) {
		return ClassServerError
	}
	if containsAny(lower, timeoutPhrases) {
		return ClassNetworkTimeout
	}
	return ""
}

func containsAny(s string, phrases []string) bool {
	for _, p := range phrases {
		if strings.Contains(s, p) {
			return true
		}
	}
	return false
}

// MaxCauseRunes caps the cause a [Signature] keeps.
const MaxCauseRunes = 400

// The patterns [Normalize] removes, in the order it applies them. Each
// replacement is a placeholder no later pattern matches.
var normalizers = []struct {
	pattern *regexp.Regexp
	replace func(match string) string
}{
	{regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.-]*://[^\s"'<>()\[\]]*[^\s"'<>()\[\]:,.;]`), always("<url>")},
	{regexp.MustCompile(`\b\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?)?`), always("<time>")},
	{regexp.MustCompile(`\b\d{1,2}:\d{2}:\d{2}(?:\.\d+)?\b`), always("<time>")},
	{regexp.MustCompile(`\b[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}\b`), always("<id>")},
	{regexp.MustCompile(`(?:^|[\s"'(=\[,])~?/[^\s"'()\[\],;:]+`), absolutePath},
	{regexp.MustCompile(`\b[A-Z][A-Z0-9_]*-\d+\b`), always("<id>")},
	{regexp.MustCompile(`\b(?:\d+(?:\.\d+)?(?:ns|us|µs|ms|h|m|s))+\b`), always("<duration>")},
	{regexp.MustCompile(`\b[0-9A-Fa-f]{7,}\b`), hexID},
	{regexp.MustCompile(`\d{4,}`), always("<n>")},
}

func always(placeholder string) func(string) string {
	return func(string) string { return placeholder }
}

// absolutePath replaces a path, keeping the character that opened it.
func absolutePath(match string) string {
	if r := match[0]; r != '/' && r != '~' {
		return string(r) + "<path>"
	}
	return "<path>"
}

// hexID replaces a hexadecimal identifier, which holds both a digit and
// a letter; a word such as "defaced" and a plain number stay.
func hexID(match string) string {
	if strings.ContainsAny(match, "0123456789") && strings.ContainsAny(strings.ToLower(match), "abcdef") {
		return "<id>"
	}
	return match
}

var spaces = regexp.MustCompile(`\s+`)

// Normalize returns the first non-blank line of text with the values that
// differ from one run to the next removed, so two runs of the same
// failure give the same line: every registered credential is masked;
// URLs, timestamps and clock times, UUIDs, absolute paths, issue keys
// such as ONS-49, durations, hexadecimal identifiers of 7 or more digits
// such as commit hashes, and numbers of 4 or more digits, such as process
// IDs and ports, become placeholders. Short numbers, such as an exit or
// HTTP status, stay. Runs of white space become one space, and the line
// is cut to [MaxCauseRunes] runes.
func Normalize(text string) string {
	line := firstLine(redact.Mask(text))
	for _, n := range normalizers {
		line = n.pattern.ReplaceAllStringFunc(line, n.replace)
	}
	line = strings.TrimSpace(spaces.ReplaceAllString(line, " "))
	if runes := []rune(line); len(runes) > MaxCauseRunes {
		line = string(runes[:MaxCauseRunes])
	}
	return line
}

func firstLine(text string) string {
	for line := range strings.Lines(text) {
		if strings.TrimSpace(line) != "" {
			return strings.TrimSpace(line)
		}
	}
	return ""
}

// Signature identifies a run failure across runs: the step that failed,
// the failure's class, and the first line of its cause with run-specific
// values removed. Two runs that fail the same way have equal signatures.
type Signature struct {
	Step  string
	Class Class
	Cause string
}

// Sign returns the signature of err failing step. The step is normalized
// as the cause is, lower-cased, with its digits removed and its words
// joined by underscores, so "agent turn 3" is "agent_turn".
func Sign(step string, err error) Signature {
	sig := Signature{Step: normalizeStep(step), Class: Classify(err)}
	if err != nil {
		sig.Cause = Normalize(err.Error())
	}
	return sig
}

// String returns the signature as one line, its step, class and cause
// joined by " | ". The zero Signature is "".
func (s Signature) String() string {
	if s == (Signature{}) {
		return ""
	}
	return s.Step + " | " + string(s.Class) + " | " + s.Cause
}

// Transient reports whether the signature's class is transient.
func (s Signature) Transient() bool { return s.Class.Transient() }

var quoted = regexp.MustCompile(`"[^"]*"|'[^']*'|\([^)]*\)`)

func normalizeStep(step string) string {
	step = quoted.ReplaceAllString(step, " ")
	words := strings.FieldsFunc(strings.ToLower(step), func(r rune) bool {
		return !unicode.IsLetter(r)
	})
	if len(words) == 0 {
		return "run"
	}
	return strings.Join(words, "_")
}
