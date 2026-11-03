package orchestrator

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/agent/procutil"
	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/environment"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/workspace"
)

// ReactionTriageDisposition is a triage command's answer.
type ReactionTriageDisposition string

const (
	// TriageHandled means the command resolved the subject itself, so
	// no agent continuation is warranted for it.
	TriageHandled ReactionTriageDisposition = "handled"

	// TriageDispatchAgent means the command wants the agent
	// continuation the reaction would have scheduled anyway. It is
	// also the fallback applied to every failure mode.
	TriageDispatchAgent ReactionTriageDisposition = "dispatch-agent"

	// TriageEscalate means the command wants a person, so the kind's
	// configured escalation is applied instead of a continuation.
	TriageEscalate ReactionTriageDisposition = "escalate"
)

// triageInputSchemaVersion is the version of the input document the
// runner writes. A script reads it to tell one document shape from a
// later one.
const triageInputSchemaVersion = 1

// triageMaxResultBytes is the largest result file the runner reads. A
// larger file is rejected rather than parsed, because a triage answer
// is a single small object and an oversized file is a script writing
// something else to that path.
const triageMaxResultBytes = 64 * 1024

// Fallback reasons carried on a [ReactionTriageOutcome] whose
// disposition was defaulted rather than read from the command.
const (
	triageFallbackWorkspaceMissing     = "workspace_missing"
	triageFallbackWorkspacePathInvalid = "workspace_path_invalid"
	triageFallbackStartFailed          = "start_failed"
	triageFallbackTimeout              = "timeout"
	triageFallbackExitStatus           = "exit_status"
	triageFallbackHookRejected         = "hook_rejected"
	triageFallbackNoResult             = "no_result"
	triageFallbackResultTooLarge       = "result_too_large"
	triageFallbackMalformedResult      = "malformed_result"
	triageFallbackUnknownDisposition   = "unknown_disposition"
)

// ReactionTriageOutcome is one completed triage run. It holds no
// captured output: the tail belongs to the completion log record that
// [RunReactionTriage] writes, and an outcome is retained on a pending
// entry for as long as its fingerprint stands, which can be the whole
// watch window.
type ReactionTriageOutcome struct {
	// Disposition is the applied answer. It is [TriageDispatchAgent]
	// whenever Fallback is non-empty.
	Disposition ReactionTriageDisposition

	// Fallback names why the default was applied. Empty when the
	// command produced a recognized disposition.
	Fallback string

	// Elapsed is the command's wall-clock duration.
	Elapsed time.Duration
}

// ReactionTriageRun is an in-flight or finished triage run. It is
// runtime-only and is never persisted.
//
// Outcome is written by the runner goroutine before Done is closed and
// MUST NOT be read until a receive on Done has succeeded. That receive
// is the only synchronization between the goroutine and the reconcile
// pass. Applied and Marked are written afterwards by the event loop
// alone, so they need none. No field is written by both.
type ReactionTriageRun struct {
	// Fingerprint binds the run to the subject it was started for.
	Fingerprint string

	// StartedAt is the UTC time the run began.
	StartedAt time.Time

	// Cancel terminates the subprocess and its process group.
	Cancel context.CancelFunc

	// Done is closed once Outcome is set.
	Done <-chan struct{}

	// Outcome is valid only after a successful receive on Done.
	Outcome *ReactionTriageOutcome

	// Applied reports whether a reconcile pass has already acted on
	// Outcome. Written only by the event loop, and only after a
	// successful receive on Done. A consumed outcome is re-applied from
	// memory rather than re-running the command, which is what bounds a
	// subject to one run.
	Applied bool

	// Marked reports whether the fingerprint write that a handled or
	// escalate outcome requires has succeeded. False after a failed
	// write, which is the one case a later pass retries.
	Marked bool
}

// ReactionTriageRequest carries everything the input document needs.
type ReactionTriageRequest struct {
	// Kind is the runtime reaction discriminator, for example
	// [ReactionKindCI].
	Kind string

	// WorkspaceRoot is the configured workspace root the per-issue
	// directory is derived from.
	WorkspaceRoot string

	// IssueID is the tracker-assigned issue ID.
	IssueID string

	// Identifier is the human-readable ticket key the workspace key is
	// derived from.
	Identifier string

	// DisplayID is the display identifier carried into the document.
	DisplayID string

	// Attempt is the overall run attempt number of the entry.
	Attempt int

	// SSHHost is the host preference carried through to the command's
	// environment, exactly as a workspace hook receives it.
	SSHHost string

	// Fingerprint is the value the kind stores for the current subject.
	Fingerprint string

	// AttemptsUsed is the kind's continuation counter for this
	// reaction key at the moment the run starts.
	AttemptsUsed int

	// MaxAttempts is the kind's own budget field: the continuation-turn
	// cap for review and bot-review, the retry cap for the other two.
	MaxAttempts int

	// Subject is the same map the continuation template receives for
	// this kind, so the command sees what the agent would have seen.
	Subject any

	// Environment is the execution environment the command runs in.
	// Nil runs it as a host process in the workspace directory.
	Environment environment.Environment
}

// triageInputIssue is the issue identity block of the input document.
type triageInputIssue struct {
	ID         string `json:"id"`
	Identifier string `json:"identifier"`
	DisplayID  string `json:"display_id"`
}

// triageInputDocument is the JSON document written to the path named by
// SORTIE_REACTION_INPUT. Externally authored text reaches the command
// only through this document, never through an environment variable or
// a shell word.
type triageInputDocument struct {
	SchemaVersion int              `json:"schema_version"`
	ReactionKind  string           `json:"reaction_kind"`
	Issue         triageInputIssue `json:"issue"`
	Attempt       int              `json:"attempt"`
	Workspace     string           `json:"workspace"`
	Fingerprint   string           `json:"fingerprint"`
	AttemptsUsed  int              `json:"attempts_used"`
	MaxAttempts   int              `json:"max_attempts"`
	Subject       any              `json:"subject"`
}

// triageResultDocument is the JSON document the command writes to the
// path named by SORTIE_REACTION_RESULT. Unknown keys are ignored so a
// later extension can add fields without breaking existing scripts.
type triageResultDocument struct {
	Disposition string `json:"disposition"`
}

// RunReactionTriage executes one triage command and returns its
// outcome. It never returns an error: every failure mode maps to a
// [ReactionTriageOutcome] whose Disposition is [TriageDispatchAgent]
// and whose Fallback names the reason.
//
// It writes exactly one completion log record before it returns, on
// every path including the ones that never start a subprocess. That
// record is the only place the command's captured output tail is
// reported, which is why it is written here rather than by the caller:
// the tail lives on the [workspace.HookResult] or [workspace.HookError]
// and is not carried on the returned outcome.
//
// The input and result files live where the command can reach them and
// outside the workspace: in a fresh host temporary directory, or, for an
// environment that runs elsewhere ([environment.RunsElsewhere]), in a
// per-run directory under [triageGuestRoot] inside it, or under the
// transfer directory the environment names ([environment.TransferDirNamer]),
// which Sortie
// writes, reads, and removes through the environment after resuming its
// instance ([environment.Resumer]).
//
// It blocks for at most cfg.TimeoutMS milliseconds plus the process
// group teardown grace (and, inside an environment, the bounded file
// commands), and MUST be called from a goroutine, never from
// the orchestrator event loop.
func RunReactionTriage(
	ctx context.Context,
	cfg config.ReactionTriageConfig,
	req ReactionTriageRequest,
	log *slog.Logger,
) ReactionTriageOutcome {
	if log == nil {
		log = slog.Default()
	}

	// The record is written through a context stripped of cancellation
	// so a run cancelled at shutdown still reports its outcome.
	logCtx := context.WithoutCancel(ctx)
	finish := func(outcome ReactionTriageOutcome, hookOutput string) ReactionTriageOutcome {
		logTriageCompletion(logCtx, log, req, outcome, hookOutput)
		return outcome
	}

	pathResult, err := workspace.ComputePath(req.WorkspaceRoot, req.Identifier)
	if err != nil {
		return finish(triageFallbackOutcome(triageFallbackWorkspacePathInvalid, 0), "")
	}

	// The directory is never created here: creating it would make the
	// next dispatch see an already-existing workspace, skip the
	// after-create hook, and run the before-run hook in an empty tree.
	info, statErr := os.Stat(pathResult.Path)
	if statErr != nil || !info.IsDir() {
		return finish(triageFallbackOutcome(triageFallbackWorkspaceMissing, 0), "")
	}

	target := workspace.NewTarget(req.Environment, pathResult.Key, pathResult.Path, req.IssueID, req.Identifier)
	workspacePath := pathResult.Path
	var files triageFiles
	if environment.RunsElsewhere(req.Environment) {
		if !resumeForTriage(ctx, log, req, target) {
			return finish(triageFallbackOutcome(triageFallbackStartFailed, 0), "")
		}
		files = newGuestTriageFiles(target, req.Kind, log)
		workspacePath = target.Workspace.GuestPath
	} else {
		hostFiles, filesErr := newHostTriageFiles()
		if filesErr != nil {
			return finish(triageFallbackOutcome(triageFallbackStartFailed, 0), "")
		}
		files = hostFiles
	}
	defer files.remove(ctx, req.Kind, log)

	input, err := encodeTriageInput(req, workspacePath)
	if err != nil {
		return finish(triageFallbackOutcome(triageFallbackStartFailed, 0), "")
	}
	if writeErr := files.writeInput(ctx, input); writeErr != nil {
		return finish(triageFallbackOutcome(triageFallbackStartFailed, 0), "")
	}

	env := workspace.HookEnv(req.IssueID, req.Identifier, pathResult.Path, req.Attempt, req.SSHHost)
	env["SORTIE_REACTION_KIND"] = req.Kind
	env["SORTIE_REACTION_INPUT"] = files.inputPath()
	env["SORTIE_REACTION_RESULT"] = files.resultPath()

	started := time.Now()
	hookResult, hookErr := workspace.RunHook(ctx, workspace.HookParams{
		Script:    cfg.Script,
		Dir:       pathResult.Path,
		Env:       env,
		TimeoutMS: cfg.TimeoutMS,
		Target:    target,
	})
	elapsed := time.Since(started)

	if hookErr != nil {
		output := ""
		op := ""
		terminatedLeftovers := false
		if he, ok := errors.AsType[*workspace.HookError](hookErr); ok {
			output = he.Output
			op = he.Op
			terminatedLeftovers = he.TerminatedLeftovers
		}
		if terminatedLeftovers {
			log.Info(procutil.LeftoversTerminatedMessage, slog.String("reaction_kind", req.Kind)) //nolint:sloglint // procutil.LeftoversTerminatedMessage is a fixed string constant
		}
		return finish(triageFallbackOutcome(fallbackForHookOp(op), elapsed), output)
	}

	if hookResult.TerminatedLeftovers {
		log.Info(procutil.LeftoversTerminatedMessage, slog.String("reaction_kind", req.Kind)) //nolint:sloglint // procutil.LeftoversTerminatedMessage is a fixed string constant
	}
	return finish(files.readResult(ctx, elapsed), "")
}

// triageFiles is the exchange of one triage run with its command: the
// input file Sortie writes and the result file the command writes, at
// paths the command can reach, outside the workspace so a stale result
// file cannot be read as this run's answer and so externally authored
// text stays out of the tree the agent reads.
type triageFiles interface {
	// inputPath and resultPath are the paths as the command sees them.
	inputPath() string
	resultPath() string

	// writeInput writes the input document.
	writeInput(ctx context.Context, data []byte) error

	// readResult reads the result file and returns the outcome it
	// implies.
	readResult(ctx context.Context, elapsed time.Duration) ReactionTriageOutcome

	// remove deletes the run's files, logging a failure.
	remove(ctx context.Context, kind string, log *slog.Logger)
}

// hostTriageFiles is the exchange with a command that runs on this
// host: the input and result files in a fresh host temporary directory.
type hostTriageFiles struct {
	dir    string
	input  string
	result string
}

// newHostTriageFiles creates the run's host temporary directory.
func newHostTriageFiles() (hostTriageFiles, error) {
	dir, err := os.MkdirTemp("", "sortie-triage-")
	if err != nil {
		return hostTriageFiles{}, err
	}
	return hostTriageFiles{
		dir:    dir,
		input:  filepath.Join(dir, "input.json"),
		result: filepath.Join(dir, "result.json"),
	}, nil
}

func (f hostTriageFiles) inputPath() string  { return f.input }
func (f hostTriageFiles) resultPath() string { return f.result }

// writeInput writes the input document at mode 0600.
func (f hostTriageFiles) writeInput(_ context.Context, data []byte) error {
	if err := os.WriteFile(f.input, data, 0o600); err != nil {
		return fmt.Errorf("write triage input: %w", err)
	}
	return nil
}

func (f hostTriageFiles) readResult(_ context.Context, elapsed time.Duration) ReactionTriageOutcome {
	return decodeResult(f.result, elapsed)
}

// remove deletes the run's temporary directory, logging a failure.
func (f hostTriageFiles) remove(_ context.Context, kind string, log *slog.Logger) {
	if rmErr := os.RemoveAll(f.dir); rmErr != nil {
		log.Warn("failed to remove reaction triage temporary directory",
			slog.String("reaction_kind", kind),
			slog.Any("error", rmErr),
		)
	}
}

// triageGuestRoot is the directory, on a tmpfs inside an environment
// that runs elsewhere, under which each triage run gets its own
// directory. Whatever a run leaves there goes when the instance stops.
const triageGuestRoot = "/run/sortie/triage"

// triageGuestFileTimeout bounds each command that writes, reads, or
// removes a triage run's files inside the environment.
const triageGuestFileTimeout = 30 * time.Second

// triageGuestNotFoundStatus is the exit status of the read command when
// the command left no result file.
const triageGuestNotFoundStatus = 3

// guestTriageFiles is the exchange with a command that runs in an
// environment that runs elsewhere: the files in the run's own directory
// under [triageGuestRoot], written, read, and removed by commands the
// environment runs. An environment that names its own transfer
// directory gets the run's directory under it instead.
type guestTriageFiles struct {
	target workspace.Target
	dir    string
	kind   string
	log    *slog.Logger
}

// newGuestTriageFiles names the run's directory: the reaction kind and
// a random suffix, reduced to characters a path segment can hold
// without interpretation.
func newGuestTriageFiles(target workspace.Target, kind string, log *slog.Logger) guestTriageFiles {
	root := triageGuestRoot
	if dir := environment.GuestTransferDir(target.Environment, ""); dir != "" {
		root = path.Join(dir, "triage")
	}
	return guestTriageFiles{target: target, dir: path.Join(root, triageRunID(kind)), kind: kind, log: log}
}

// triageRunID returns a fresh identifier for one triage run of kind,
// made only of A-Za-z0-9, '_', and '-'.
func triageRunID(kind string) string {
	var suffix [8]byte
	_, _ = rand.Read(suffix[:])
	return sanitizeTriagePathSegment(kind) + "-" + hex.EncodeToString(suffix[:])
}

// sanitizeTriagePathSegment replaces every character of s outside
// A-Za-z0-9, '_', and '-' with '_'. An empty s yields "run".
func sanitizeTriagePathSegment(s string) string {
	if s == "" {
		return "run"
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_', r == '-':
			return r
		}
		return '_'
	}, s)
}

func (f guestTriageFiles) inputPath() string  { return path.Join(f.dir, "input.json") }
func (f guestTriageFiles) resultPath() string { return path.Join(f.dir, "result.json") }

// writeInput creates the run's directory, readable only by its owner,
// and pipes data into the input file. A failure is logged with the
// environment's error, which a host write never has.
func (f guestTriageFiles) writeInput(ctx context.Context, data []byte) error {
	_, err := f.run(ctx, bytes.NewReader(data), 0,
		`umask 077 && mkdir -p -- "$1" && cat > "$2"`, f.dir, f.inputPath())
	if err != nil {
		f.log.Warn("failed to write the reaction triage input in the execution environment",
			slog.String("reaction_kind", f.kind),
			slog.String("path", f.inputPath()),
			slog.Any("error", err),
		)
		return fmt.Errorf("write triage input in the environment: %w", err)
	}
	return nil
}

// readResult reads the result file with head, at most one byte more
// than [triageMaxResultBytes], so a larger file is told apart without
// reading it whole. The host keeps at most the cap itself, whatever the
// guest's head does: a result that overflows it is too large.
func (f guestTriageFiles) readResult(ctx context.Context, elapsed time.Duration) ReactionTriageOutcome {
	out, err := f.run(ctx, nil, triageMaxResultBytes,
		fmt.Sprintf(`[ -e "$1" ] || exit %d; exec head -c %d -- "$1"`, triageGuestNotFoundStatus, triageMaxResultBytes+1),
		f.resultPath())
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == triageGuestNotFoundStatus {
			return triageFallbackOutcome(triageFallbackNoResult, elapsed)
		}
		return triageFallbackOutcome(triageFallbackMalformedResult, elapsed)
	}
	if out.Truncated() {
		return triageFallbackOutcome(triageFallbackResultTooLarge, elapsed)
	}
	return parseTriageResult(out.Bytes(), elapsed)
}

// remove deletes the run's directory. It runs even after ctx is
// cancelled, within its own timeout.
func (f guestTriageFiles) remove(ctx context.Context, kind string, log *slog.Logger) {
	if _, err := f.run(context.WithoutCancel(ctx), nil, 0, `rm -rf -- "$1"`, f.dir); err != nil {
		log.Warn("failed to remove reaction triage directory in the environment",
			slog.String("reaction_kind", kind),
			slog.String("dir", f.dir),
			slog.Any("error", err),
		)
	}
}

// run runs script with args as its positional parameters through the
// environment, in the workspace root, feeding it stdin when non-nil, and
// returns the last keep bytes of its standard output, in a buffer that
// reports whether it dropped any, so the host never holds more than
// keep bytes whatever the command writes.
func (f guestTriageFiles) run(ctx context.Context, stdin io.Reader, keep int, script string, args ...string) (*procutil.TailBuffer, error) {
	ctx, cancel := context.WithTimeout(ctx, triageGuestFileTimeout)
	defer cancel()
	argv := append([]string{"sh", "-c", script, "sh"}, args...)
	cmd, err := f.target.Command(ctx, environment.CommandSpec{Argv: argv, Stdin: stdin != nil})
	if err != nil {
		return nil, err
	}
	cmd.Stdin = stdin
	procutil.SetGroupKill(cmd)
	stdout := procutil.NewTailBuffer(max(keep, 1))
	stderr := procutil.NewTailBuffer(512)
	capture, err := procutil.StartCapture(ctx, cmd, procutil.CaptureParams{Stdout: stdout, Stderr: stderr, Logger: f.log})
	if err != nil {
		return nil, err
	}
	result := capture.Wait()
	switch {
	case result.Stopped:
		return nil, fmt.Errorf("%s: %w", strings.Join(args, " "), context.Cause(ctx))
	case result.WaitErr != nil:
		if msg := strings.TrimSpace(string(stderr.Bytes())); msg != "" {
			return nil, fmt.Errorf("%w: %s", result.WaitErr, msg)
		}
		return nil, result.WaitErr
	}
	return stdout, nil
}

// resumeForTriage makes the workspace's environment instance ready to
// run the triage command when the environment stops its instances
// between runs, and reports whether the command can run.
func resumeForTriage(ctx context.Context, log *slog.Logger, req ReactionTriageRequest, target workspace.Target) bool {
	resumer, ok := target.Environment.(environment.Resumer)
	if !ok {
		return true
	}
	ready, err := resumer.Resume(ctx, target.Workspace)
	switch {
	case err != nil:
		log.Warn("reaction triage cannot run: resuming the execution environment failed",
			slog.String("reaction_kind", req.Kind),
			slog.String("environment", target.Environment.Kind()),
			slog.Any("error", err),
		)
		return false
	case !ready:
		log.Warn("reaction triage cannot run: the workspace has no execution environment instance",
			slog.String("reaction_kind", req.Kind),
			slog.String("environment", target.Environment.Kind()),
		)
		return false
	}
	return true
}

// triageFallbackOutcome builds the defaulted outcome for one fallback
// reason.
func triageFallbackOutcome(reason string, elapsed time.Duration) ReactionTriageOutcome {
	return ReactionTriageOutcome{
		Disposition: TriageDispatchAgent,
		Fallback:    reason,
		Elapsed:     elapsed,
	}
}

// fallbackForHookOp maps a [workspace.HookError] Op to the fallback
// reason reported for it. The mapping is total: an Op this function
// does not name still yields a named reason rather than an unlabelled
// fallback.
func fallbackForHookOp(op string) string {
	switch op {
	case "start":
		return triageFallbackStartFailed
	case "timeout":
		return triageFallbackTimeout
	case "run":
		return triageFallbackExitStatus
	default:
		return triageFallbackHookRejected
	}
}

// encodeTriageInput marshals the input document, naming workspacePath
// as the workspace the command runs in.
func encodeTriageInput(req ReactionTriageRequest, workspacePath string) ([]byte, error) {
	doc := triageInputDocument{
		SchemaVersion: triageInputSchemaVersion,
		ReactionKind:  req.Kind,
		Issue: triageInputIssue{
			ID:         req.IssueID,
			Identifier: req.Identifier,
			DisplayID:  req.DisplayID,
		},
		Attempt:      req.Attempt,
		Workspace:    workspacePath,
		Fingerprint:  req.Fingerprint,
		AttemptsUsed: req.AttemptsUsed,
		MaxAttempts:  req.MaxAttempts,
		Subject:      req.Subject,
	}

	encoded, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("marshal triage input: %w", err)
	}
	return encoded, nil
}

// decodeResult reads the result file a successful command left behind
// and returns the outcome it implies. Every rejection yields
// [TriageDispatchAgent] with a named fallback reason.
func decodeResult(path string, elapsed time.Duration) ReactionTriageOutcome {
	info, err := os.Stat(path)
	if err != nil {
		return triageFallbackOutcome(triageFallbackNoResult, elapsed)
	}
	if info.Size() > triageMaxResultBytes {
		return triageFallbackOutcome(triageFallbackResultTooLarge, elapsed)
	}

	raw, err := os.ReadFile(path) //nolint:gosec // the path is orchestrator-generated inside a per-run temporary directory
	if err != nil {
		return triageFallbackOutcome(triageFallbackMalformedResult, elapsed)
	}
	return parseTriageResult(raw, elapsed)
}

// parseTriageResult returns the outcome the result document raw
// implies. Every rejection yields [TriageDispatchAgent] with a named
// fallback reason.
func parseTriageResult(raw []byte, elapsed time.Duration) ReactionTriageOutcome {
	var doc triageResultDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return triageFallbackOutcome(triageFallbackMalformedResult, elapsed)
	}

	switch ReactionTriageDisposition(strings.TrimSpace(doc.Disposition)) {
	case TriageHandled:
		return ReactionTriageOutcome{Disposition: TriageHandled, Elapsed: elapsed}
	case TriageDispatchAgent:
		return ReactionTriageOutcome{Disposition: TriageDispatchAgent, Elapsed: elapsed}
	case TriageEscalate:
		return ReactionTriageOutcome{Disposition: TriageEscalate, Elapsed: elapsed}
	default:
		return triageFallbackOutcome(triageFallbackUnknownDisposition, elapsed)
	}
}

// logTriageCompletion writes the single completion record every run
// emits, whether or not a reconcile pass ever consumes the outcome. It
// is info when the command produced a recognized disposition and warn
// when a fallback applied.
func logTriageCompletion(ctx context.Context, log *slog.Logger, req ReactionTriageRequest, outcome ReactionTriageOutcome, hookOutput string) {
	attrs := []slog.Attr{
		slog.String("reaction_kind", req.Kind),
		slog.String("fingerprint", req.Fingerprint),
		slog.String("disposition", string(outcome.Disposition)),
		slog.Int64("elapsed_ms", outcome.Elapsed.Milliseconds()),
	}
	if outcome.Fallback != "" {
		attrs = append(attrs, slog.String("fallback", outcome.Fallback))
	}
	if hookOutput != "" {
		attrs = append(attrs, slog.String("hook_output", hookOutput))
	}

	level := slog.LevelInfo
	if outcome.Fallback != "" {
		level = slog.LevelWarn
	}
	log.LogAttrs(ctx, level, "reaction triage completed", attrs...)
}

// reactionTriageVerdict tells the calling reconcile pass what to do with
// the entry the gate just advanced.
type reactionTriageVerdict int

const (
	// triageProceed means no triage is configured, or the run returned
	// dispatch-agent. The pass continues to its dispatch block.
	triageProceed reactionTriageVerdict = iota

	// triageWait means a run is in flight or was just started. The
	// caller re-enqueues the entry ready for the next tick and continues
	// to the next entry.
	triageWait

	// triageHandled means the run returned handled. The gate has already
	// marked the fingerprint dispatched. The caller re-enqueues the entry
	// with its poll interval and continues to the next entry.
	triageHandled

	// triageEscalate means the run returned escalate. The gate has
	// already marked the fingerprint dispatched. The caller invokes its
	// own escalation function with [EscalationTriggerTriage] and
	// continues to the next entry.
	triageEscalate
)

// startReactionTriage launches a triage run on its own goroutine,
// registers it with state.TriageWg, counts it in state.TriageInFlight,
// and returns the handle to store on the pending entry. It returns nil
// without starting anything when the in-flight cap is already reached;
// the gate reads a nil handle as a wait.
func startReactionTriage(
	state *State,
	ctx context.Context,
	cfg config.ReactionTriageConfig,
	req ReactionTriageRequest,
	log *slog.Logger,
) *ReactionTriageRun {
	if log == nil {
		log = slog.Default()
	}

	// The cap is sized from the concurrency the operator already chose
	// for this host. It adds to the agent processes that number bounds
	// rather than sharing slots with them.
	limit := int64(max(state.MaxConcurrentAgents, 1))
	if state.TriageInFlight.Load() >= limit {
		log.Debug("reaction triage deferred: in-flight cap reached",
			slog.String("reaction_kind", req.Kind),
			slog.String("fingerprint", req.Fingerprint),
			slog.Int64("cap", limit),
		)
		return nil
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	run := &ReactionTriageRun{
		Fingerprint: req.Fingerprint,
		StartedAt:   time.Now().UTC(),
		Cancel:      cancel,
		Done:        done,
	}

	log.Info("reaction triage started",
		slog.String("reaction_kind", req.Kind),
		slog.String("fingerprint", req.Fingerprint),
		slog.Int("timeout_ms", cfg.TimeoutMS),
	)

	state.TriageInFlight.Add(1)
	state.TriageWg.Go(func() {
		defer state.TriageInFlight.Add(-1)
		outcome := RunReactionTriage(runCtx, cfg, req, log)
		run.Outcome = &outcome
		close(done)
		cancel()
	})

	return run
}

// triageRunFinished reports whether a run's outcome is readable, using a
// non-blocking receive so no reconcile pass ever waits on a subprocess.
func triageRunFinished(run *ReactionTriageRun) bool {
	if run == nil || run.Done == nil {
		return false
	}
	select {
	case <-run.Done:
		return true
	default:
		return false
	}
}

// reactionTriageGate advances a pending entry's triage state for one
// reconcile pass and reports what the pass should do next. It starts,
// cancels, discards, and applies runs, and marks the reaction
// fingerprint dispatched on a handled or escalate answer. It never
// escalates, dispatches, or re-enqueues: those remain the pass's own
// responsibility.
func reactionTriageGate(
	state *State,
	params ReconcileParams,
	pending *PendingReaction,
	cfg config.ReactionTriageConfig,
	req ReactionTriageRequest,
	log *slog.Logger,
	ctx context.Context,
) reactionTriageVerdict {
	if log == nil {
		log = slog.Default()
	}
	if params.WorkspaceRoot == "" || !cfg.Enabled() {
		return triageProceed
	}

	if req.Environment == nil {
		req.Environment = params.Environment
	}

	if pending.Triage == nil {
		if run := startReactionTriage(state, ctx, cfg, req, log); run != nil {
			pending.Triage = run
		}
		return triageWait
	}

	run := pending.Triage
	finished := triageRunFinished(run)

	// A moved fingerprint voids the run bound to the previous subject,
	// whether it has finished or not.
	if run.Fingerprint != req.Fingerprint {
		logTriageDiscarded(ctx, log, run, req, finished)
		if !finished && run.Cancel != nil {
			run.Cancel()
		}
		pending.Triage = nil
		if fresh := startReactionTriage(state, ctx, cfg, req, log); fresh != nil {
			pending.Triage = fresh
		}
		return triageWait
	}

	if !finished {
		return triageWait
	}

	outcome := run.Outcome
	if outcome == nil {
		log.Warn("reaction triage finished without an outcome, dispatching the agent",
			slog.String("reaction_kind", req.Kind),
			slog.String("fingerprint", req.Fingerprint),
		)
		return triageProceed
	}

	if !run.Applied {
		run.Applied = true
		logTriageApplied(ctx, log, req, *outcome)
		recordTriageOutcome(ctx, params, pending, req, *outcome, log)

		switch outcome.Disposition {
		case TriageHandled:
			markTriageDispatched(params, run, req, log, ctx)
			recordTriageAction(ctx, params, pending, log)
			return triageHandled
		case TriageEscalate:
			markTriageDispatched(params, run, req, log, ctx)
			return triageEscalate
		default:
			return triageProceed
		}
	}

	// A consumed outcome is re-applied from memory rather than re-run.
	// A memoized escalate re-applies as handled so a stored answer
	// cannot post a second escalation; the fingerprint write is the one
	// action a later pass repeats, and only while it has failed.
	if outcome.Disposition == TriageDispatchAgent {
		return triageProceed
	}
	if !run.Marked {
		markTriageDispatched(params, run, req, log, ctx)
	}
	return triageHandled
}

// recordTriageAction records that triage handled the entry's reaction in
// place of an agent run.
func recordTriageAction(ctx context.Context, params ReconcileParams, pending *PendingReaction, log *slog.Logger) {
	now := time.Now().UTC()
	if params.NowFunc != nil {
		now = params.NowFunc().UTC()
	}
	recordReactionActivity(ctx, params.Store, pending.IssueID, pending.StageLineage,
		persistence.ReactionActivityAction, []string{pending.Kind}, now, log)
}

// reactionTriageOutcomeStore is the pair of store methods the recorded triage
// answers need. Satisfied by [persistence.Store].
type reactionTriageOutcomeStore interface {
	RecordReactionTriageOutcome(ctx context.Context, outcome persistence.ReactionTriageOutcome) error
	LatestReactionTriageDisposition(ctx context.Context, issueID, kind, fingerprint string) (disposition string, found bool, err error)
}

// recordTriageOutcome appends the answer a pass just applied. A failed write
// is logged and the pass proceeds: the answer has already taken effect.
func recordTriageOutcome(
	ctx context.Context,
	params ReconcileParams,
	pending *PendingReaction,
	req ReactionTriageRequest,
	outcome ReactionTriageOutcome,
	log *slog.Logger,
) {
	if params.Store == nil {
		return
	}
	now := time.Now().UTC()
	if params.NowFunc != nil {
		now = params.NowFunc().UTC()
	}
	if err := params.Store.RecordReactionTriageOutcome(ctx, persistence.ReactionTriageOutcome{
		IssueID:     pending.IssueID,
		Kind:        req.Kind,
		Fingerprint: req.Fingerprint,
		Disposition: string(outcome.Disposition),
		Fallback:    outcome.Fallback,
		Elapsed:     outcome.Elapsed,
		ChainID:     pending.StageLineage.ChainID,
		RecordedAt:  now,
	}); err != nil {
		log.Warn("failed to record reaction triage outcome",
			slog.String("reaction_kind", req.Kind),
			slog.String("fingerprint", req.Fingerprint),
			slog.Any("error", err),
		)
	}
}

// triageHandledFor reports whether the newest answer recorded for the
// fingerprint is handled. It compares the recorded disposition and not the
// gate verdict, because the gate replays a memoized escalate as handled. A
// failed read reads as not handled.
func triageHandledFor(
	ctx context.Context,
	store reactionTriageOutcomeStore,
	pending *PendingReaction,
	fingerprint string,
	log *slog.Logger,
) bool {
	if store == nil {
		return false
	}
	disposition, found, err := store.LatestReactionTriageDisposition(ctx, pending.IssueID, pending.Kind, fingerprint)
	if err != nil {
		log.Warn("failed to read reaction triage outcome",
			slog.String("reaction_kind", pending.Kind),
			slog.Any("error", err),
		)
		return false
	}
	return found && disposition == string(TriageHandled)
}

// markTriageDispatched marks the reaction fingerprint dispatched and
// records the write's success on the run, so a failed write is retried
// by a later pass and a successful one is never repeated.
func markTriageDispatched(
	params ReconcileParams,
	run *ReactionTriageRun,
	req ReactionTriageRequest,
	log *slog.Logger,
	ctx context.Context,
) {
	if params.Store == nil {
		return
	}
	if err := params.Store.MarkReactionDispatched(ctx, req.IssueID, req.Kind); err != nil {
		run.Marked = false
		log.Warn("failed to mark reaction dispatched after triage",
			slog.String("reaction_kind", req.Kind),
			slog.String("fingerprint", req.Fingerprint),
			slog.Any("error", err),
		)
		return
	}
	run.Marked = true
}

// logTriageApplied writes the record reporting that a pass consumed an
// outcome. It is warn when the outcome carried a fallback reason.
func logTriageApplied(ctx context.Context, log *slog.Logger, req ReactionTriageRequest, outcome ReactionTriageOutcome) {
	attrs := []slog.Attr{
		slog.String("reaction_kind", req.Kind),
		slog.String("fingerprint", req.Fingerprint),
		slog.String("disposition", string(outcome.Disposition)),
	}
	level := slog.LevelInfo
	if outcome.Fallback != "" {
		attrs = append(attrs, slog.String("fallback", outcome.Fallback))
		level = slog.LevelWarn
	}
	log.LogAttrs(ctx, level, "reaction triage applied", attrs...)
}

// logTriageDiscarded writes the record reporting that a handle bound to
// a fingerprint the pass no longer computes is being thrown away.
func logTriageDiscarded(ctx context.Context, log *slog.Logger, run *ReactionTriageRun, req ReactionTriageRequest, finished bool) {
	attrs := []slog.Attr{
		slog.String("reaction_kind", req.Kind),
		slog.String("fingerprint", req.Fingerprint),
		slog.String("previous_fingerprint", run.Fingerprint),
	}

	level := slog.LevelInfo
	if !finished {
		attrs = append(attrs, slog.String("discarded", "cancelled_in_flight"))
	} else {
		attrs = append(attrs, slog.String("discarded", "outcome_superseded"))
		if run.Outcome != nil {
			attrs = append(attrs, slog.String("disposition", string(run.Outcome.Disposition)))
			if run.Outcome.Fallback != "" {
				attrs = append(attrs, slog.String("fallback", run.Outcome.Fallback))
				level = slog.LevelWarn
			}
		}
	}

	log.LogAttrs(ctx, level, "reaction triage discarded: subject changed", attrs...)
}

// cancelReactionTriage detaches an entry's triage run, terminating it
// first when it is still in flight. It is called by every path that ends
// the episode the run was started for, whether that path drops the
// pending entry or re-enqueues it, so no subprocess outlives its episode
// and no finished verdict is carried into the next one. Detaching a
// finished run matters as much as cancelling a live one: a retained
// verdict is replayed by the gate whenever the next episode recomputes
// an identical fingerprint. A nil entry and a nil handle are no-ops.
func cancelReactionTriage(pending *PendingReaction) {
	if pending == nil || pending.Triage == nil {
		return
	}
	if !triageRunFinished(pending.Triage) && pending.Triage.Cancel != nil {
		pending.Triage.Cancel()
	}
	pending.Triage = nil
}

// ReactionEscalationTrigger names why an escalation fired. It selects
// the escalation's log message and, for the comment action, its text; it
// changes no action, no metric, and no post-condition.
type ReactionEscalationTrigger string

const (
	// EscalationTriggerBudget is the escalation a kind fires when its
	// own retry or continuation budget is spent.
	EscalationTriggerBudget ReactionEscalationTrigger = "budget_exhausted"

	// EscalationTriggerTriage is the escalation a triage command asked
	// for, with no budget spent on the subject.
	EscalationTriggerTriage ReactionEscalationTrigger = "triage"

	// EscalationTriggerRefused is the escalation a kind fires when the
	// forge or the tracker answered a write with an authentication or
	// payload error, so repeating the write cannot help.
	EscalationTriggerRefused ReactionEscalationTrigger = "refused"

	// EscalationTriggerMissingMergeCommit is the escalation merge
	// completion fires when the grace period for the forge to report the
	// merge commit of a merged pull request has run out.
	EscalationTriggerMissingMergeCommit ReactionEscalationTrigger = "missing_merge_commit"

	// EscalationTriggerUnresolved is the escalation a kind fires when
	// every run it may start on one subject ended and left the subject as
	// it was.
	EscalationTriggerUnresolved ReactionEscalationTrigger = "unresolved"
)
