package failcause

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/redact"
)

// refusal is the first line of the error a push refused for a missing
// workflow scope gives, with the run's own branch, URL, commit, path,
// time, and process ID in it.
func refusal(identifier, commit, workspace, at string, pid int) error {
	return fmt.Errorf(
		"export issue branch %q: push sortie/%s to https://github.com/ortusnova/sortie.git: "+
			"[remote rejected] (refusing to allow a Personal Access Token to create or update workflow without `workflow` scope) "+
			"at %s in %s (pid %d, commit %s): git push: exit status 1\nstderr:\nTo github.com:ortusnova/sortie.git",
		"sortie/"+identifier, identifier, at, workspace, pid, commit)
}

func TestSign_TwoRunsOfTheSameRefusalGiveOneSignature(t *testing.T) {
	t.Parallel()

	first := Sign("export", refusal("ONS-49", "4816a7ad", "/var/lib/sortie/ws/ONS-49", "2026-11-02T09:47:54Z", 41237))
	second := Sign("export", refusal("ONS-49", "a4e35353c0ffee", "/var/lib/sortie/ws/ONS-49-retry", "2026-11-02T09:48:35.120+01:00", 51022))

	if first != second {
		t.Fatalf("signatures differ:\n first  %q\n second %q", first, second)
	}
	if first.String() == "" || first.Transient() {
		t.Errorf("signature %q: want a non-empty, non-transient signature", first)
	}
	for _, runSpecific := range []string{"ONS-49", "4816a7ad", "/var/lib", "2026-11-02", "09:47", "41237", "github.com/ortusnova"} {
		if containsFold(first.Cause, runSpecific) {
			t.Errorf("cause %q keeps the run-specific %q", first.Cause, runSpecific)
		}
	}
	if !containsFold(first.Cause, "remote rejected") || !containsFold(first.Cause, "exit status 1") {
		t.Errorf("cause %q lost the refusal itself", first.Cause)
	}
	if containsFold(first.Cause, "stderr") {
		t.Errorf("cause %q goes past the first line", first.Cause)
	}
}

func TestSign_DifferentRefusalsGiveDifferentSignatures(t *testing.T) {
	t.Parallel()

	scope := Sign("export", errors.New("push sortie/ONS-49: [remote rejected] (missing workflow scope)"))
	hook := Sign("export", errors.New("push sortie/ONS-49: [remote rejected] (pre-receive hook declined)"))
	if scope == hook {
		t.Errorf("two refusals give one signature %q", scope)
	}
	if other := Sign("agent session start", errors.New("push sortie/ONS-49: [remote rejected] (missing workflow scope)")); other == scope {
		t.Errorf("two steps give one signature %q", scope)
	}
}

func TestNormalize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, in, want string
	}{
		{"issue key", "branch sortie/ONS-49 moved", "branch sortie/<id> moved"},
		{"absolute path", `open /tmp/ws/ONS-49/.sortie/pr.json: permission denied`, "open <path>: permission denied"},
		{"quoted path", `stat "/home/u/x": no such file`, `stat "<path>": no such file`},
		{"home path", "read ~/cache/x failed", "read <path> failed"},
		{"url", "GET https://api.github.com/repos/o/r/pulls?head=x: server error 502", "GET <url>: server error 502"},
		{"timestamp", "reset at 2026-11-02T09:47:54Z", "reset at <time>"},
		{"date", "on 2026-11-02 only", "on <time> only"},
		{"clock", "at 09:47:54.123 it failed", "at <time> it failed"},
		{"uuid", "session 3f2b8c1e-9d4a-4c7e-8b1f-2a3c4d5e6f70 lost", "session <id> lost"},
		{"commit", "commit a4e35353c0ffee not found", "commit <id> not found"},
		{"hex word stays", "the page was defaced", "the page was defaced"},
		{"duration", "after 1m30s and 250ms", "after <duration> and <duration>"},
		{"long number", "pid 41237 port 8087 code -32603", "pid <n> port <n> code -<n>"},
		{"short number stays", "exit status 128: HTTP 404", "exit status 128: HTTP 404"},
		{"first line", "\n  first line  \nsecond line", "first line"},
		{"white space", "a \t  b", "a b"},
		{"empty", " \n ", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := Normalize(tt.in); got != tt.want {
				t.Errorf("Normalize(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestNormalize_MasksCredentialsAndCaps(t *testing.T) {
	t.Parallel()

	const secret = "ghp_signatureTestSecretValue0001"
	redact.Add("test.failcause signature", secret)
	if got := Normalize("auth with " + secret + " refused"); containsFold(got, secret) {
		t.Errorf("Normalize() = %q, keeps the credential", got)
	}

	long := make([]rune, MaxCauseRunes+50)
	for i := range long {
		long[i] = 'é'
	}
	if got := []rune(Normalize(string(long))); len(got) != MaxCauseRunes {
		t.Errorf("len(Normalize(long)) = %d runes, want %d", len(got), MaxCauseRunes)
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "read tcp: deadline" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

type classedError struct{}

func (classedError) Error() string        { return "git push: exit status 1" }
func (classedError) FailureClass() string { return "git_push" }

func TestClassify_TransientClasses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want Class
	}{
		{"github 502 answer", &domain.SCMError{Kind: domain.ErrSCMAPI, Message: "POST /repos/o/r/pulls: server error 502",
			Err: &domain.TrackerError{Kind: domain.ErrTrackerTransport, Message: "POST /repos/o/r/pulls: server error 502", Status: 502}}, ClassServerError},
		{"5xx status only", &domain.TrackerError{Kind: domain.ErrTrackerAPI, Message: "unexpected", Status: 503}, ClassServerError},
		{"git 502", errors.New("git push: exit status 128: fatal: unable to access 'x': The requested URL returned error: 502"), ClassServerError},
		{"bad gateway", errors.New("502 Bad Gateway"), ClassServerError},
		{"runtime service unavailable", &domain.AgentError{Kind: domain.ErrResponseError,
			Message: `session/new error -32603: Internal error: Internal service failure: {"errorName":"ServiceUnavailableError"}`}, ClassServerError},
		{"429 status", &domain.TrackerError{Kind: domain.ErrTrackerAPI, Message: "rate limited", Status: 429}, ClassRateLimit},
		{"secondary rate limit", &domain.TrackerError{Kind: domain.ErrTrackerAPI, Message: "GET /x: rate limited (secondary)", Status: 403}, ClassRateLimit},
		{"429 text", errors.New("http 429 from upstream"), ClassRateLimit},
		{"too many requests", errors.New("Too Many Requests"), ClassRateLimit},
		{"net timeout", &url.Error{Op: "Get", URL: "https://x", Err: timeoutError{}}, ClassNetworkTimeout},
		{"os deadline", fmt.Errorf("read: %w", os.ErrDeadlineExceeded), ClassNetworkTimeout},
		{"dial timeout", &net.OpError{Op: "dial", Net: "tcp", Err: timeoutError{}}, ClassNetworkTimeout},
		{"timeout text", errors.New("git fetch: fatal: unable to access: Connection timed out after 300000 milliseconds"), ClassNetworkTimeout},
		{"usage limit", &domain.AgentError{Kind: domain.ErrUsageLimit, Message: "limit"}, ClassUsageLimit},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := Classify(fmt.Errorf("wrapped: %w", tt.err))
			if got != tt.want {
				t.Errorf("Classify(%v) = %q, want %q", tt.err, got, tt.want)
			}
			if !got.Transient() {
				t.Errorf("Class %q is not transient", got)
			}
		})
	}
}

func TestClassify_OtherClasses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want Class
	}{
		{"nil", nil, ClassOther},
		{"plain", errors.New("something broke"), ClassOther},
		{"classer", fmt.Errorf("export: %w", classedError{}), "git_push"},
		{"agent kind", &domain.AgentError{Kind: domain.ErrTurnFailed, Message: "turn failed"}, Class(domain.ErrTurnFailed)},
		{"own deadline", &domain.AgentError{Kind: domain.ErrTurnTimeout, Message: "turn", Err: context.DeadlineExceeded}, Class(domain.ErrTurnTimeout)},
		{"scm auth", &domain.SCMError{Kind: domain.ErrSCMAuth, Message: "token lacks scope",
			Err: &domain.TrackerError{Kind: domain.ErrTrackerAuth, Message: "forbidden", Status: 403}}, Class(domain.ErrSCMAuth)},
		{"tracker 404", &domain.TrackerError{Kind: domain.ErrTrackerNotFound, Message: "not found", Status: 404}, Class(domain.ErrTrackerNotFound)},
		{"ci", &domain.CIError{Kind: domain.ErrCIAuth, Message: "denied"}, Class(domain.ErrCIAuth)},
		{"line number is no status", errors.New("parse error at line 500"), ClassOther},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := Classify(tt.err)
			if got != tt.want {
				t.Errorf("Classify(%v) = %q, want %q", tt.err, got, tt.want)
			}
			if got.Transient() {
				t.Errorf("Class %q is transient", got)
			}
		})
	}
}

func TestTransientClasses(t *testing.T) {
	t.Parallel()

	want := []Class{ClassNetworkTimeout, ClassServerError, ClassRateLimit, ClassUsageLimit}
	if !slices.Equal(TransientClasses, want) {
		t.Errorf("TransientClasses = %v, want %v", TransientClasses, want)
	}
	for _, c := range TransientClasses {
		if !c.Transient() {
			t.Errorf("%q.Transient() = false", c)
		}
	}
	if ClassOther.Transient() || Class("git_push").Transient() {
		t.Error("a non-transient class reports transient")
	}
}

func TestSign_Step(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"agent turn 3":                 "agent_turn",
		"prompt render (turn 2)":       "prompt_render",
		`export issue branch "x/ON-1"`: "export_issue_branch",
		"Workspace Preparation":        "workspace_preparation",
		"":                             "run",
	}
	for step, want := range tests {
		if got := Sign(step, errors.New("x")).Step; got != want {
			t.Errorf("Sign(%q).Step = %q, want %q", step, got, want)
		}
	}
	if got := (Signature{}).String(); got != "" {
		t.Errorf("zero Signature.String() = %q, want empty", got)
	}
	if got := Sign("export", errors.New("boom")).String(); got != "export | error | boom" {
		t.Errorf("String() = %q", got)
	}
}

func containsFold(s, sub string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(sub))
}
