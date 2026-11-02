package gitsync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/sortie-ai/sortie/internal/agent/procutil"
)

// safeConfig is set with -c on every git command run on a mirror. The
// command line outranks every configuration file, so neither the host's
// configuration nor anything inside the mirror can turn these off: no
// hook runs, no filesystem monitor starts, and every object received is
// checked. fetch.fsckObjects and receive.fsckObjects are set as well
// because each, when set in a configuration file, overrides
// transfer.fsckObjects. gc.autoDetach keeps an automatic garbage
// collection inside the command, and so inside the mirror's lock.
var safeConfig = []string{
	"core.hooksPath=/dev/null",
	"core.fsmonitor=false",
	"transfer.fsckObjects=true",
	"fetch.fsckObjects=true",
	"receive.fsckObjects=true",
	"gc.autoDetach=false",
}

// droppedEnv names the host variables a mirror's git command never
// sees: those that would point it at another repository, work tree,
// index, or object store, and those that carry command-line
// configuration, which could override safeConfig. The host's
// configuration files and credentials still apply.
var droppedEnv = map[string]bool{
	"GIT_DIR":                          true,
	"GIT_WORK_TREE":                    true,
	"GIT_INDEX_FILE":                   true,
	"GIT_OBJECT_DIRECTORY":             true,
	"GIT_ALTERNATE_OBJECT_DIRECTORIES": true,
	"GIT_COMMON_DIR":                   true,
	"GIT_NAMESPACE":                    true,
	"GIT_CONFIG_PARAMETERS":            true,
	"GIT_CONFIG_COUNT":                 true,
}

// stderrCap bounds the standard error a failed git command reports.
const stderrCap = 4 << 10

// stdoutCap bounds the standard output a failed git command keeps, such
// as the porcelain report of a refused push.
const stdoutCap = 8 << 10

// runner runs host git commands on one bare repository.
type runner struct {
	// binary is the git program; empty is "git" resolved through PATH.
	binary string

	// gitDir is the bare repository every command runs on, and in.
	gitDir string
}

// gitEnv returns environ without the variables of droppedEnv and any
// GIT_CONFIG_KEY_<n> or GIT_CONFIG_VALUE_<n>, with GIT_TERMINAL_PROMPT=0
// so a missing upstream credential fails instead of waiting for input.
func gitEnv(environ []string) []string {
	out := make([]string, 0, len(environ)+1)
	for _, entry := range environ {
		name, _, _ := strings.Cut(entry, "=")
		if droppedEnv[name] || strings.HasPrefix(name, "GIT_CONFIG_KEY_") ||
			strings.HasPrefix(name, "GIT_CONFIG_VALUE_") || name == "GIT_TERMINAL_PROMPT" {
			continue
		}
		out = append(out, entry)
	}
	return append(out, "GIT_TERMINAL_PROMPT=0")
}

// argv returns the full git argument list for args: the repository,
// then safeConfig, then args.
func (r runner) argv(args []string) []string {
	out := make([]string, 0, 1+2*len(safeConfig)+len(args))
	out = append(out, "--git-dir="+r.gitDir)
	for _, kv := range safeConfig {
		out = append(out, "-c", kv)
	}
	return append(out, args...)
}

// run runs git with args on the repository and returns its standard
// output. The repository is named by --git-dir, never found from the
// working directory, and being bare it has no work tree. A failure names
// the git subcommand and carries the tail of its standard error with
// every registered secret masked; it never names the arguments, which
// may hold the upstream URL.
func (r runner) run(ctx context.Context, args ...string) ([]byte, error) {
	binary := r.binary
	if binary == "" {
		binary = "git"
	}
	cmd := exec.CommandContext(ctx, binary, r.argv(args)...) //nolint:gosec // G204: Sortie's own git invocation on its own mirror
	cmd.Env = gitEnv(os.Environ())
	var stdout bytes.Buffer
	stderr := procutil.NewTailBuffer(stderrCap)
	res, err := procutil.RunCapture(ctx, cmd, 0, procutil.CaptureParams{Stdout: &stdout, Stderr: stderr})
	if err == nil {
		err = res.WaitErr
	}
	if err != nil {
		return stdout.Bytes(), &Error{
			Subcommand: args[0],
			ExitCode:   procutil.ExtractExitCode(err),
			Stdout:     strings.TrimSpace(string(tail(stdout.Bytes(), stdoutCap))),
			Stderr:     strings.TrimSpace(string(stderr.Bytes())),
			err:        err,
		}
	}
	return stdout.Bytes(), nil
}

// tail returns the last n bytes of b, or b when it is shorter.
func tail(b []byte, n int) []byte {
	if len(b) <= n {
		return b
	}
	return b[len(b)-n:]
}

// Error reports a failed git command.
type Error struct {
	// Subcommand is the git subcommand that failed, such as "fetch".
	Subcommand string

	// ExitCode is the command's exit status, or -1 when it did not exit
	// on its own.
	ExitCode int

	// Stdout is the tail of the command's standard output, such as the
	// porcelain report of a push, which names why the remote refused
	// each ref. Unlike Stderr it is not masked.
	Stdout string

	// Stderr is the tail of the command's standard error, secrets
	// masked.
	Stderr string

	err error
}

func (e *Error) Error() string {
	if e.Stderr == "" {
		return fmt.Sprintf("git %s: %v", e.Subcommand, e.err)
	}
	return fmt.Sprintf("git %s: %v: %s", e.Subcommand, e.err, e.Stderr)
}

// FailureOutput returns the command's standard output and standard
// error under a line naming the command and how it ended, for the run
// record. The standard output is not masked.
func (e *Error) FailureOutput() string {
	var b strings.Builder
	fmt.Fprintf(&b, "git %s: %v", e.Subcommand, e.err)
	if e.Stdout != "" {
		b.WriteString("\nstdout:\n")
		b.WriteString(e.Stdout)
	}
	if e.Stderr != "" {
		b.WriteString("\nstderr:\n")
		b.WriteString(e.Stderr)
	}
	return b.String()
}

func (e *Error) Unwrap() error { return e.err }

// exitedWith reports whether err is a git command that exited with code.
func exitedWith(err error, code int) bool {
	gitErr, ok := errors.AsType[*Error](err)
	return ok && gitErr.ExitCode == code
}
