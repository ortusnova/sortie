package smolvm

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/sortie-ai/sortie/internal/agent/procutil"
)

// DefaultBinary is the smolvm program run when the configuration names
// none.
const DefaultBinary = "smolvm"

// MinimumVersion is the oldest smolvm release this package drives: the
// release every flag, JSON field, and error text the client relies on
// was last measured against. Every later release is accepted.
const MinimumVersion = "1.32.4"

const (
	// stderrTailBytes is how much of a failed invocation's standard
	// error a [CLIError] keeps: the same tail a workspace hook keeps.
	stderrTailBytes = 8 * 1024

	// maxStdoutBytes bounds the standard output the client reads from
	// one invocation. A machine listing takes about 1.5 KiB per machine.
	maxStdoutBytes = 4 << 20
)

var (
	// ErrOutputTooLarge reports that an invocation wrote more standard
	// output than the client reads.
	ErrOutputTooLarge = errors.New("smolvm output exceeds the read limit")

	errOutputIncomplete = errors.New("smolvm output did not complete after the process exited")
)

// CLIError reports a smolvm invocation that failed: it could not be
// started, it was killed, or it exited non-zero.
type CLIError struct {
	// Args holds the subcommand words, such as ["machine", "start"].
	// Flag values never appear in it.
	Args []string

	// Machine names the machine the invocation addressed, if any.
	Machine string

	// ExitCode is the process exit code, or -1 when the process did
	// not exit on its own: it could not be started, or it was killed.
	ExitCode int

	// Stderr holds the last 8 KiB of the invocation's standard error,
	// with every registered secret value masked.
	Stderr string

	// Code and Message are smolvm's own report of the failure, from
	// the error line --error-format json makes it write last on
	// standard error, such as "NOT_FOUND". Both are "" when the
	// last line is not that report, as for a usage error or a failure
	// of the command run in a machine.
	Code    string
	Message string

	// Err is the underlying error from running the process.
	Err error
}

func (e *CLIError) Error() string {
	var b strings.Builder
	b.WriteString("smolvm")
	for _, arg := range e.Args {
		b.WriteByte(' ')
		b.WriteString(arg)
	}
	if e.Machine != "" {
		fmt.Fprintf(&b, " (machine %q)", e.Machine)
	}
	if e.ExitCode >= 0 {
		fmt.Fprintf(&b, ": exit code %d", e.ExitCode)
	} else if e.Err != nil {
		fmt.Fprintf(&b, ": %v", e.Err)
	}
	if e.Code != "" {
		fmt.Fprintf(&b, ": %s: %s", e.Code, e.Message)
	} else if summary := stderrSummary(e.Stderr); summary != "" {
		b.WriteString(": ")
		b.WriteString(summary)
	}
	return b.String()
}

func (e *CLIError) Unwrap() error { return e.Err }

// stderrSummary returns the line of stderr that best describes a
// failure: the last line starting with "Error:", else the last
// non-blank line.
func stderrSummary(stderr string) string {
	lines := strings.Split(strings.TrimSpace(stderr), "\n")
	for _, line := range slices.Backward(lines) {
		if line := strings.TrimSpace(line); strings.HasPrefix(line, "Error:") {
			return line
		}
	}
	return strings.TrimSpace(lines[len(lines)-1])
}

// IsNotFound reports whether err is a [CLIError] from an invocation that
// addressed a machine smolvm does not know. It suits the machine
// lifecycle commands; a command run in a machine writes to the same
// standard error.
func IsNotFound(err error) bool {
	var cliErr *CLIError
	return errors.As(err, &cliErr) && cliErr.ExitCode == 1 && cliErr.Code == codeNotFound
}

// errorFormat selects smolvm's machine-readable failure report. Every
// machine subcommand takes it after its own name.
var errorFormat = []string{"--error-format", "json"}

// machineArgs returns the arguments of the machine subcommand sub with
// rest, selecting the JSON failure report.
func machineArgs(sub string, rest ...string) []string {
	args := make([]string, 0, 4+len(rest))
	args = append(args, "machine", sub)
	args = append(args, errorFormat...)
	return append(args, rest...)
}

// smolvm's failure codes, which it documents as stable, except
// COMMAND_FAILED, which Sortie does not read. Each failure smolvm
// reports itself carries one; a failure of the command run in a
// machine carries none.
const (
	codeNotFound              = "NOT_FOUND"
	codeConflict              = "CONFLICT"
	codeInterrupted           = "INTERRUPTED"
	codeConfig                = "CONFIG_ERROR"
	codeInvalidState          = "INVALID_STATE"
	codeStorage               = "STORAGE_ERROR"
	codeMount                 = "MOUNT_ERROR"
	codeKVMUnavailable        = "KVM_UNAVAILABLE"
	codeHypervisorUnavailable = "HYPERVISOR_UNAVAILABLE"
	codeSmolvm                = "SMOLVM_ERROR"
)

// parseErrorReport returns the code and message of line when it is
// exactly smolvm's JSON failure report: one object whose only key,
// "error", holds exactly a non-empty string "code" and a string
// "message".
func parseErrorReport(line string) (code, message string, ok bool) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "{") {
		return "", "", false
	}
	var report struct {
		Error *struct {
			Code    *string `json:"code"`
			Message *string `json:"message"`
		} `json:"error"`
	}
	dec := json.NewDecoder(strings.NewReader(line))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&report); err != nil || dec.More() {
		return "", "", false
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return "", "", false
	}
	if report.Error == nil || report.Error.Code == nil || report.Error.Message == nil || *report.Error.Code == "" {
		return "", "", false
	}
	return *report.Error.Code, *report.Error.Message, true
}

// lastLine returns the last non-blank line of text, trimmed.
func lastLine(lines []string) string {
	for _, line := range slices.Backward(lines) {
		if line = strings.TrimSpace(line); line != "" {
			return line
		}
	}
	return ""
}

// Machine states reported by the machine listing.
const (
	StateRunning = "running"
	StateStopped = "stopped"
	StateCreated = "created"
)

// Machine is one entry of the machine listing, reduced to the fields
// Sortie reads.
type Machine struct {
	Name   string            `json:"name"`
	State  string            `json:"state"`
	PID    *int              `json:"pid"`
	Labels map[string]string `json:"labels"`

	// Mounts counts the machine's directory mounts. A mounted socket is
	// not counted.
	Mounts int `json:"mounts"`

	// Network reports whether the machine has outbound network access.
	Network bool `json:"network"`
}

// Running reports whether the machine is running.
func (m Machine) Running() bool { return m.State == StateRunning }

// parseMachines parses the output of the machine listing.
func parseMachines(data []byte) ([]Machine, error) {
	var machines []Machine
	if err := json.Unmarshal(data, &machines); err != nil {
		return nil, fmt.Errorf("parse smolvm machine listing: %w", err)
	}
	return machines, nil
}

// Volume is a host directory mounted into a machine.
type Volume struct {
	Host     string
	Guest    string
	ReadOnly bool
}

// SocketMount is a host Unix socket mounted into a machine.
type SocketMount struct {
	Host  string
	Guest string
}

// CredentialBinding binds the host variable Env to the guest variable of
// the same name for Hosts, under the credential name Name. A non-empty
// Header is also set to the real value on every HTTPS request to Hosts.
type CredentialBinding struct {
	Name   string
	Env    string
	Hosts  []string
	Header string
}

// Label is one machine label.
type Label struct {
	Key   string
	Value string
}

// CreateSpec describes a machine to create from a pack.
type CreateSpec struct {
	Name string
	Pack string

	// CPUs, MemoryMiB, and StorageGiB size the machine; 0 keeps the
	// pack's or smolvm's default.
	CPUs       int
	MemoryMiB  int
	StorageGiB int

	Labels []Label

	// User is the user every command in the machine runs as; "" keeps
	// smolvm's own choice.
	User string

	// Nested exposes the host's virtualization extensions in the
	// machine.
	Nested bool

	// NetBackend names the machine's network backend; "" keeps
	// smolvm's own choice.
	NetBackend string

	// AllowHosts lists the egress host patterns. Nothing else opens the
	// network: without it and without Credentials the machine has none.
	AllowHosts []string

	Credentials []CredentialBinding
	SSHAgent    bool
	Volumes     []Volume
	Sockets     []SocketMount
}

// createArgs returns the arguments of the create invocation for spec.
// A path that contains the mount separator ':' is refused, since smolvm
// would split it.
func createArgs(spec CreateSpec) ([]string, error) {
	if spec.Name == "" || spec.Pack == "" {
		return nil, errors.New("smolvm machine create needs a name and a pack")
	}
	args := machineArgs("create", "--name", spec.Name, "--from", spec.Pack)
	for _, sizing := range []struct {
		flag  string
		value int
	}{
		{"--cpus", spec.CPUs},
		{"--mem", spec.MemoryMiB},
		{"--storage", spec.StorageGiB},
	} {
		if sizing.value > 0 {
			args = append(args, sizing.flag, strconv.Itoa(sizing.value))
		}
	}
	for _, label := range spec.Labels {
		args = append(args, "--label", label.Key+"="+label.Value)
	}
	if spec.User != "" {
		// The machine's user owns its workspace: the disk's mount point
		// belongs to root, and git refuses a repository whose directory
		// another user owns. smolvm runs --init commands as root, once.
		args = append(args, "--user", spec.User, "--init", "chown "+spec.User+" "+GuestWorkspace)
	}
	if spec.Nested {
		args = append(args, "--nested")
	}
	if spec.NetBackend != "" {
		args = append(args, "--net-backend", spec.NetBackend)
	}
	for _, host := range spec.AllowHosts {
		args = append(args, "--allow-host-pattern", host)
	}
	for _, cred := range spec.Credentials {
		if len(cred.Hosts) == 0 {
			return nil, fmt.Errorf("credential %q binds no host", cred.Name)
		}
		binding := cred.Name + "=" + cred.Env + "@" + strings.Join(cred.Hosts, ",")
		if cred.Header != "" {
			binding += ":set-header=" + cred.Header
		}
		args = append(args, "--credential", binding)
	}
	if spec.SSHAgent {
		args = append(args, "--ssh-agent")
	}
	for _, v := range spec.Volumes {
		if err := checkMountPaths(v.Host, v.Guest); err != nil {
			return nil, err
		}
		mount := v.Host + ":" + v.Guest
		if v.ReadOnly {
			mount += ":ro"
		}
		args = append(args, "-v", mount)
	}
	for _, s := range spec.Sockets {
		if err := checkMountPaths(s.Host, s.Guest); err != nil {
			return nil, err
		}
		args = append(args, "--mount-socket", s.Host+":"+s.Guest)
	}
	return args, nil
}

func checkMountPaths(paths ...string) error {
	for _, p := range paths {
		if p == "" || strings.Contains(p, ":") {
			return fmt.Errorf("mount path %q is empty or contains ':'", p)
		}
	}
	return nil
}

// ExecOptions configures a command run in a machine.
type ExecOptions struct {
	// Workdir is the guest working directory; "" keeps the machine's.
	Workdir string

	// Env holds KEY=VALUE entries set in the guest command.
	Env []string

	// SecretEnv holds GUEST_VAR=HOST_VAR references smolvm resolves on
	// the host for this command.
	SecretEnv []string

	// Interactive keeps standard input open as a byte-exact pipe; the
	// guest command's output streams as it is written.
	Interactive bool

	// Stream, without Interactive, streams the guest command's output
	// as it is written instead of after the command exits; its
	// standard input is empty.
	Stream bool

	// Detach starts the guest command in the background and returns at
	// once; it keeps running while the machine runs. It excludes
	// Interactive and Stream.
	Detach bool
}

// ExecArgs returns the arguments, without the program, of an invocation
// that runs argv in the machine name.
func ExecArgs(name string, opts ExecOptions, argv []string) []string {
	args := machineArgs("exec", "--name", name)
	switch {
	case opts.Detach:
		args = append(args, "-d")
	case opts.Interactive:
		args = append(args, "-i")
	case opts.Stream:
		args = append(args, "--stream")
	}
	if opts.Workdir != "" {
		args = append(args, "-w", opts.Workdir)
	}
	for _, kv := range opts.Env {
		args = append(args, "-e", kv)
	}
	for _, ref := range opts.SecretEnv {
		args = append(args, "--secret-env", ref)
	}
	args = append(args, "--")
	return append(args, argv...)
}

// CLI drives one smolvm binary. Every invocation inherits this
// process's environment, which is where smolvm reads the values of the
// host variables credential bindings name. Safe for concurrent use.
type CLI struct {
	binary string
}

// NewCLI returns a client for binary; "" selects [DefaultBinary].
func NewCLI(binary string) *CLI {
	if binary == "" {
		binary = DefaultBinary
	}
	return &CLI{binary: binary}
}

// Binary returns the program the client runs.
func (c *CLI) Binary() string { return c.binary }

// run runs the binary with args and returns its standard output.
// subcommand and machine describe the invocation in a [CLIError].
func (c *CLI) run(ctx context.Context, subcommand []string, machine string, args []string) ([]byte, error) {
	return c.runEnv(ctx, subcommand, machine, args, nil)
}

// runEnv is [CLI.run] with env added to the inherited environment; an
// entry of env overrides the inherited variable of the same name.
func (c *CLI) runEnv(ctx context.Context, subcommand []string, machine string, args, env []string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, c.binary, args...) //nolint:gosec // G204: the operator configures the binary; arguments are built by this package.
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}
	stdout := &cappedWriter{max: maxStdoutBytes}
	stderr := procutil.NewTailBuffer(stderrTailBytes)
	result, err := procutil.RunCapture(ctx, cmd, procutil.DefaultStopGrace, procutil.CaptureParams{Stdout: stdout, Stderr: stderr})
	if err == nil {
		err = result.WaitErr
	}
	if err == nil && !result.OutputComplete {
		err = errOutputIncomplete
	}
	if err == nil && stdout.overflow {
		err = ErrOutputTooLarge
	}
	if err == nil {
		return stdout.buf.Bytes(), nil
	}
	cliErr := &CLIError{
		Args:     subcommand,
		Machine:  machine,
		ExitCode: -1,
		Stderr:   string(stderr.Bytes()),
		Err:      err,
	}
	cliErr.Code, cliErr.Message, _ = parseErrorReport(lastLine(strings.Split(cliErr.Stderr, "\n")))
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.Exited() {
		cliErr.ExitCode = exitErr.ExitCode()
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		cliErr.ExitCode = -1
		cliErr.Err = errors.Join(ctxErr, err)
	}
	return nil, cliErr
}

var versionPattern = regexp.MustCompile(`^smolvm v?(\d+)\.(\d+)\.(\d+)(\S*)`)

// Version runs the binary's version command and returns the version it
// prints, such as "1.26.1".
func (c *CLI) Version(ctx context.Context) (string, error) {
	out, err := c.run(ctx, []string{"--version"}, "", []string{"--version"})
	if err != nil {
		return "", err
	}
	line := strings.TrimSpace(strings.SplitN(string(out), "\n", 2)[0])
	m := versionPattern.FindStringSubmatch(line)
	if m == nil {
		return "", fmt.Errorf("smolvm --version printed %q, not a smolvm version", truncate(line, 120))
	}
	return m[1] + "." + m[2] + "." + m[3] + m[4], nil
}

// CheckVersion returns an error naming version and [MinimumVersion]
// when version is older than the minimum or is not a version.
func CheckVersion(version string) error {
	got, ok := parseSemver(version)
	if !ok {
		return fmt.Errorf("smolvm version %q is not a version; minimum supported: %s", version, MinimumVersion)
	}
	minimum, _ := parseSemver(MinimumVersion)
	if got.compare(minimum) < 0 {
		return fmt.Errorf("smolvm version %s is older than the minimum supported version %s", version, MinimumVersion)
	}
	return nil
}

var semverPattern = regexp.MustCompile(`^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$`)

// semver is a parsed release version. Build metadata is dropped because
// it does not order releases.
type semver struct {
	major, minor, patch int
	prerelease          string
}

func parseSemver(s string) (semver, bool) {
	m := semverPattern.FindStringSubmatch(s)
	if m == nil {
		return semver{}, false
	}
	var v semver
	var err error
	if v.major, err = strconv.Atoi(m[1]); err != nil {
		return semver{}, false
	}
	if v.minor, err = strconv.Atoi(m[2]); err != nil {
		return semver{}, false
	}
	if v.patch, err = strconv.Atoi(m[3]); err != nil {
		return semver{}, false
	}
	v.prerelease = strings.TrimPrefix(m[4], "-")
	return v, true
}

// compare orders v and w by major, minor, and patch. A prerelease of a
// version orders before that version; two prereleases of the same
// version compare by their identifiers as text, which is enough to keep
// any prerelease of the minimum below the minimum.
func (v semver) compare(w semver) int {
	if c := cmp.Compare(v.major, w.major); c != 0 {
		return c
	}
	if c := cmp.Compare(v.minor, w.minor); c != 0 {
		return c
	}
	if c := cmp.Compare(v.patch, w.patch); c != 0 {
		return c
	}
	switch {
	case v.prerelease == w.prerelease:
		return 0
	case v.prerelease == "":
		return 1
	case w.prerelease == "":
		return -1
	}
	return strings.Compare(v.prerelease, w.prerelease)
}

// List returns every machine smolvm knows.
func (c *CLI) List(ctx context.Context) ([]Machine, error) {
	out, err := c.run(ctx, []string{"machine", "ls"}, "", machineArgs("ls", "--json"))
	if err != nil {
		return nil, err
	}
	return parseMachines(out)
}

// Create creates the machine spec describes. It does not start it.
func (c *CLI) Create(ctx context.Context, spec CreateSpec) error {
	args, err := createArgs(spec)
	if err != nil {
		return err
	}
	_, err = c.run(ctx, []string{"machine", "create"}, spec.Name, args)
	return err
}

// Start starts the machine name. Each KEY=VALUE entry of env is set in
// smolvm's environment for the start, where smolvm reads the values of
// the host variables the machine's credential bindings name.
func (c *CLI) Start(ctx context.Context, name string, env ...string) error {
	_, err := c.runEnv(ctx, []string{"machine", "start"}, name, machineArgs("start", "--name", name), env)
	return err
}

// Stop stops the machine name. Stopping a machine that is not running
// succeeds.
func (c *CLI) Stop(ctx context.Context, name string) error {
	_, err := c.run(ctx, []string{"machine", "stop"}, name, machineArgs("stop", "--name", name))
	return err
}

// Delete deletes the machine name without asking for confirmation.
func (c *CLI) Delete(ctx context.Context, name string) error {
	_, err := c.run(ctx, []string{"machine", "delete"}, name, machineArgs("delete", "-f", "--name", name))
	return err
}

// Exec runs argv in the machine name and returns its standard output.
// It suits short commands with small output; a long-running command is
// built with [ExecArgs] instead.
func (c *CLI) Exec(ctx context.Context, name string, opts ExecOptions, argv []string) ([]byte, error) {
	if len(argv) == 0 {
		return nil, errors.New("smolvm machine exec needs a command")
	}
	return c.run(ctx, []string{"machine", "exec"}, name, ExecArgs(name, opts, argv))
}

// cappedWriter keeps up to max bytes and records whether more arrived.
type cappedWriter struct {
	buf      bytes.Buffer
	max      int
	overflow bool
}

func (b *cappedWriter) Write(p []byte) (int, error) {
	if room := b.max - b.buf.Len(); len(p) > room {
		b.overflow = true
		if room > 0 {
			b.buf.Write(p[:room]) //nolint:errcheck // bytes.Buffer.Write never returns an error
		}
		return len(p), nil
	}
	b.buf.Write(p) //nolint:errcheck // bytes.Buffer.Write never returns an error
	return len(p), nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
