// Package smolvm registers the smolvm execution environment kind, which
// runs each issue workspace in its own smolvm microVM. It registers
// itself in [environment.Registry] from init.
//
// The package drives smolvm through its command line only, with [CLI];
// it is the one place that knows smolvm's flags, JSON output, and exit
// codes. The kind's host check, [CheckHost], refuses a smolvm release
// older than [MinimumVersion].
//
// [Environment.Prepare] creates and starts the workspace's VM,
// [Environment.AfterCreate] links the guest workspace's .sortie to the
// control directory it shares with the host, and [Environment.Command]
// and [Environment.LaunchArgv] run hooks, Sortie's own commands, and the
// agent in it. [Environment.Release] stops the VM when the worker exits,
// [Environment.Resume] starts it again for the before_remove hook,
// [Environment.Remove] deletes it with its workspace, and
// [Environment.Sweep] deletes the VMs whose workspace directory is gone.
// [Environment.ToolChannel] names each VM's tool channel: a host socket
// mounted in the VM, which a socat listener Prepare and Resume start in
// the guest serves at [GuestToolPort], so an agent reaches Sortie's tool
// server on the host over HTTP.
//
// The package touches only a VM it labeled as managed, for the
// workspace key the VM's name derives from, and for this workflow; any
// other machine is left alone.
package smolvm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/environment"
	"github.com/sortie-ai/sortie/internal/workspace"
	"github.com/sortie-ai/sortie/internal/workspacekit"
)

// Paths inside the VM.
const (
	// GuestWorkspace is the workspace directory on the VM's own disk.
	GuestWorkspace = "/workspace"

	// GuestControlDir is where the host workspace's .sortie directory
	// is mounted, read-write.
	GuestControlDir = config.SmolvmGuestControlDir

	// GuestToolSocket is where the issue's tool channel socket is
	// mounted.
	GuestToolSocket = config.SmolvmGuestToolSocket

	// GuestToolPort is the guest loopback port at which the VM's socat
	// listener joins every connection to [GuestToolSocket].
	GuestToolPort = 47800

	// GuestUserTransferDir is where files Sortie passes into and out of
	// a VM whose workflow sets the machine's user are staged.
	GuestUserTransferDir = config.SmolvmGuestUserTransferDir
)

// Labels Sortie puts on every VM it creates.
const (
	LabelManaged   = "sortie.managed"
	LabelWorkspace = "sortie.workspace"
	LabelPack      = "sortie.pack"
	LabelWorkflow  = "sortie.workflow"
)

// Layout of the environment's directories under the state directory:
// env/<key> holds each workspace's tool channel socket.
const (
	stateEnvDir      = "env"
	toolSocketName   = "tools.sock"
	recordMaxBytes   = 4096
	maxUnixSocketLen = 107 // sun_path holds 108 bytes, one of them the terminating NUL
)

// VM names. smolvm accepts [A-Za-z0-9_-], starting with a letter or
// digit, without a trailing or doubled hyphen, and up to 128
// characters, but a machine whose name exceeds 117 characters cannot
// start. Names stay within a DNS label's 63 characters.
const (
	vmNamePrefix  = "sortie-"
	maxVMNameLen  = 63
	nameHashChars = 10
)

var plainVMNameKey = regexp.MustCompile(`^[A-Za-z0-9_]+(-[A-Za-z0-9_]+)*$`)

func init() {
	environment.Registry.RegisterWithMeta(config.EnvironmentKindSmolvm, New, environment.Meta{
		CheckHost:       CheckHost,
		SweepUnselected: SweepUnselected,
	})
}

// Environment is the smolvm kind. It is safe for concurrent use across
// different workspaces.
type Environment struct {
	cfg          config.SmolvmConfig
	cli          *CLI
	stateDir     string
	workflowHash string
	logger       *slog.Logger

	// machineCache is smolvm's per-machine directory, where it leaves
	// residue for a machine a command addresses that does not exist.
	machineCache string

	packMu   sync.Mutex
	packSums map[string]packSum

	// startedWith holds, for each VM this process started, the
	// fingerprint of the sourced credential values it started with.
	startedMu   sync.Mutex
	startedWith map[string]string
}

// packSum is a pack file's SHA-256 together with the size and
// modification time it was computed for.
type packSum struct {
	size    int64
	modTime time.Time
	sum     string
}

var (
	_ environment.Environment = (*Environment)(nil)
	_ environment.Carrier     = (*Environment)(nil)
	_ environment.Namer       = (*Environment)(nil)

	_ environment.AfterCreateFinisher = (*Environment)(nil)
	_ environment.Resumer             = (*Environment)(nil)
	_ environment.EnvForwarder        = (*Environment)(nil)
	_ environment.ToolChanneler       = (*Environment)(nil)
	_ environment.KeptDirLister       = (*Environment)(nil)
	_ environment.TransferDirNamer    = (*Environment)(nil)
)

// New returns the smolvm environment for params. It succeeds without a
// pack, so that an environment built only to remove VMs of a workflow
// that no longer selects the kind can be built; [Environment.Prepare]
// then fails.
func New(params environment.Params) (environment.Environment, error) {
	logger := params.Logger
	if logger == nil {
		logger = slog.Default()
	}
	env := &Environment{
		cfg:          params.Config.Smolvm,
		cli:          NewCLI(params.Config.Smolvm.Binary),
		stateDir:     params.StateDir,
		machineCache: machineCacheDir(),
		logger:       logger,
		packSums:     map[string]packSum{},
		startedWith:  map[string]string{},
	}
	if params.WorkflowPath != "" {
		abs, err := filepath.Abs(params.WorkflowPath)
		if err != nil {
			return nil, fmt.Errorf("resolve workflow path: %w", err)
		}
		env.workflowHash = shortHash(abs)
	}
	return env, nil
}

// Kind returns [config.EnvironmentKindSmolvm].
func (*Environment) Kind() string { return config.EnvironmentKindSmolvm }

// GuestPath returns [GuestWorkspace]. Every host path handed to it names
// a host workspace directory, and every workspace's guest directory is
// [GuestWorkspace]. A path inside a workspace maps through
// [environment.Workspace.GuestPathOf].
func (*Environment) GuestPath(string) string { return GuestWorkspace }

// GuestTransferDir returns [GuestUserTransferDir] for a workflow that
// sets the machine's user, and "" otherwise. smolvm creates /run/sortie,
// the parent of Sortie's own mounts, for root alone, so a command run as
// another user cannot create a directory there.
func (e *Environment) GuestTransferDir() string {
	if e.cfg.User == "" {
		return ""
	}
	return GuestUserTransferDir
}

// ForwardsEnv reports whether the host variable name reaches the VM: as
// the placeholder of a credential binding, or in plain text through
// pass_env.
func (e *Environment) ForwardsEnv(name string) bool {
	for _, cred := range e.cfg.Credentials {
		if cred.Env == name {
			return true
		}
	}
	return slices.Contains(e.cfg.PassEnv, name)
}

// ToolChannel returns ws's tool channel: the host socket
// <state dir>/env/<key>/tools.sock, mounted at [GuestToolSocket] in ws's
// VM, which reaches it at [GuestToolPort]. Its directory is removed with
// the VM.
func (e *Environment) ToolChannel(ws environment.Workspace) (domain.ToolChannel, error) {
	if e.stateDir == "" {
		return domain.ToolChannel{}, errors.New("smolvm environment: a state directory is required for the tool channel")
	}
	if !validKey(ws.Key) {
		return domain.ToolChannel{}, fmt.Errorf("smolvm environment: workspace key %q cannot name a state directory", ws.Key)
	}
	stateDir, err := filepath.Abs(e.stateDir)
	if err != nil {
		return domain.ToolChannel{}, fmt.Errorf("resolve state directory: %w", err)
	}
	path := filepath.Join(stateDir, stateEnvDir, ws.Key, toolSocketName)
	if len(path) > maxUnixSocketLen {
		return domain.ToolChannel{}, fmt.Errorf("smolvm environment: tool socket path %q is longer than a Unix socket path may be (%d bytes)",
			path, maxUnixSocketLen)
	}
	return domain.ToolChannel{HostSocket: path, GuestPort: GuestToolPort}, nil
}

// InstanceName returns the name of the VM that runs ws.
func (*Environment) InstanceName(ws environment.Workspace) string { return VMName(ws.Key) }

// VMName returns the name of the VM for the workspace key: "sortie-"
// followed by the key. A key smolvm would refuse in a name, or one that
// makes the name too long, is rewritten to the characters smolvm
// accepts, truncated, and followed by a short hash of the whole key, so
// distinct keys keep distinct names.
func VMName(key string) string {
	if plainVMNameKey.MatchString(key) && len(vmNamePrefix)+len(key) <= maxVMNameLen {
		return vmNamePrefix + key
	}
	var b strings.Builder
	lastHyphen := true // drops leading hyphens
	for _, r := range key {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
			lastHyphen = false
		case !lastHyphen:
			b.WriteByte('-')
			lastHyphen = true
		}
	}
	suffix := shortHash(key)[:nameHashChars]
	base := b.String()
	if room := maxVMNameLen - len(vmNamePrefix) - len(suffix) - 1; len(base) > room {
		base = base[:room]
	}
	base = strings.TrimRight(base, "-")
	if base == "" {
		return vmNamePrefix + suffix
	}
	return vmNamePrefix + base + "-" + suffix
}

// shortHash returns the hex SHA-256 of s, cut to 12 characters.
func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:12]
}

// Prepare makes the workspace's VM ready to run commands.
//
// It creates the host workspace's .sortie directory, the state
// directories the VM's mounts name, and each per-workspace directory of
// the workflow's mounts, at mode 0700, which nothing in this package
// removes, then looks the VM up. Without a VM, it creates one from the
// pack and starts it, and reports CreatedNow. A
// stopped VM is started; a running one is left as it is. A VM is reused
// only when the workspace's environment record names it: a VM that
// predates a fresh host workspace directory is deleted and created
// again. The result's image digest is the pack SHA-256 the VM's label
// records. A VM with the workspace's name that Sortie did not label as
// managed, for the workspace key, and for this workflow is never
// touched, and fails preparation. A smolvm
// failure fails preparation with a [*CLIError].
//
// Every credential binding with a file or command source is read anew
// before anything else, and its value given to the VM's start. A source
// that yields no value fails preparation with a [*credsource.Error]. A
// running VM started with other values, or by another process, is
// stopped and started again, because smolvm reads the values only when
// a VM starts.
func (e *Environment) Prepare(ctx context.Context, ws environment.Workspace) (environment.PrepareResult, error) {
	paths, err := e.preparePaths(ws)
	if err != nil {
		return environment.PrepareResult{}, err
	}
	name := VMName(ws.Key)
	credEnv, err := e.credentialEnv(ctx)
	if err != nil {
		return environment.PrepareResult{}, fmt.Errorf("smolvm environment: %w", err)
	}

	machines, err := e.cli.List(ctx)
	if err != nil {
		return environment.PrepareResult{}, err
	}
	var existing *Machine
	for i := range machines {
		if machines[i].Name == name {
			existing = &machines[i]
			break
		}
	}

	if existing != nil {
		// Workspace keys repeat across workflows, so a VM of the same
		// name may be another workflow's: only this workflow's own VM is
		// started, reused, or replaced.
		if !e.owns(*existing, ws.Key) {
			return environment.PrepareResult{}, fmt.Errorf(
				"smolvm machine %q exists but is not this workflow's VM for workspace %q "+
					"(it belongs to another workflow or is not managed by Sortie); remove or rename it", name, ws.Key)
		}
		if recordNames(ws.HostPath, name) {
			// The label names the pack the VM was created from, which a
			// rebuild of the pack file since does not change.
			reused := environment.PrepareResult{ImageDigest: existing.Labels[LabelPack]}
			running := existing.Running()
			if running && e.staleCredentials(name, credEnv) {
				// smolvm reads a binding's value only when the VM
				// starts, so a refreshed value needs a new start.
				e.logger.InfoContext(ctx, "restarting a running VM to give it the credential values read for this launch", slog.String("vm", name))
				if err := e.cli.Stop(ctx, name); err != nil {
					return environment.PrepareResult{}, err
				}
				e.forgetStart(name)
				running = false
			}
			if !running {
				if err := e.startVM(ctx, name, credEnv); err != nil {
					return environment.PrepareResult{}, err
				}
			}
			if err := e.startToolListener(ctx, name); err != nil {
				return environment.PrepareResult{}, err
			}
			return reused, nil
		}
		e.logger.InfoContext(ctx, "deleting a VM that predates its workspace directory",
			slog.String("vm", name), slog.String("workspace", ws.HostPath))
		if err := e.deleteVM(ctx, name); err != nil {
			return environment.PrepareResult{}, err
		}
	}

	spec, err := e.createSpec(name, ws, paths)
	if err != nil {
		return environment.PrepareResult{}, err
	}
	if err := e.cli.Create(ctx, spec); err != nil {
		return environment.PrepareResult{}, err
	}
	if err := e.startVM(ctx, name, credEnv); err != nil {
		return environment.PrepareResult{}, err
	}
	if err := e.startToolListener(ctx, name); err != nil {
		return environment.PrepareResult{}, err
	}
	e.logger.InfoContext(ctx, "created VM", slog.String("vm", name), slog.String("workspace", ws.HostPath))
	return environment.PrepareResult{CreatedNow: true, ImageDigest: labelValue(spec.Labels, LabelPack)}, nil
}

// labelValue returns the value of the label key among labels, or "".
func labelValue(labels []Label, key string) string {
	for _, l := range labels {
		if l.Key == key {
			return l.Value
		}
	}
	return ""
}

// hostPaths holds the host side of a VM's mounts: Sortie's own, and the
// workflow's mounts with each per-workspace directory resolved.
type hostPaths struct {
	control string
	socket  string
	mounts  []Volume
}

// workflowMounts returns the workflow's mounts for the workspace key,
// each per-workspace host directory named for key.
func (e *Environment) workflowMounts(key string) []Volume {
	if len(e.cfg.Mounts) == 0 {
		return nil
	}
	mounts := make([]Volume, 0, len(e.cfg.Mounts))
	for _, m := range e.cfg.Mounts {
		mounts = append(mounts, Volume{Host: m.HostFor(key), Guest: m.Guest, ReadOnly: m.ReadOnly})
	}
	return mounts
}

// KeptDirs returns the host directories of ws's own that its VM
// mounts: the per-workspace directories of the workflow's mounts. Sortie
// creates them before it creates the VM and never removes them, so what
// the guest wrote there outlives the VM and the workspace. A key that
// cannot name a directory has none.
func (e *Environment) KeptDirs(ws environment.Workspace) []string {
	if !validKey(ws.Key) {
		return nil
	}
	var dirs []string
	for _, m := range e.cfg.Mounts {
		if m.PerWorkspace() {
			dirs = append(dirs, m.HostFor(ws.Key))
		}
	}
	return dirs
}

// preparePaths checks the configuration and the workspace, and creates
// the host directories the VM's mounts name.
func (e *Environment) preparePaths(ws environment.Workspace) (hostPaths, error) {
	if e.cfg.Pack == "" {
		return hostPaths{}, &config.ConfigError{Field: "environment.smolvm.pack", Message: "is required to create a smolvm VM"}
	}
	if e.stateDir == "" || e.workflowHash == "" {
		return hostPaths{}, errors.New("smolvm environment: a state directory and a workflow path are required to prepare a VM")
	}
	if ws.Key == "" || ws.Key == "." || !filepath.IsLocal(ws.Key) || strings.ContainsAny(ws.Key, `/\`) {
		return hostPaths{}, fmt.Errorf("smolvm environment: workspace key %q cannot name a state directory", ws.Key)
	}
	if !filepath.IsAbs(ws.HostPath) {
		return hostPaths{}, fmt.Errorf("smolvm environment: workspace path %q is not absolute", ws.HostPath)
	}
	stateDir, err := filepath.Abs(e.stateDir)
	if err != nil {
		return hostPaths{}, fmt.Errorf("resolve state directory: %w", err)
	}
	envDir := filepath.Join(stateDir, stateEnvDir)
	paths := hostPaths{
		control: filepath.Join(ws.HostPath, workspacekit.SortieDir),
		socket:  filepath.Join(envDir, ws.Key, toolSocketName),
	}
	if len(paths.socket) > maxUnixSocketLen {
		return hostPaths{}, fmt.Errorf("smolvm environment: tool socket path %q is longer than a Unix socket path may be (%d bytes)",
			paths.socket, maxUnixSocketLen)
	}

	dir, err := workspacekit.OpenSortieDir(ws.HostPath, true)
	if err != nil {
		return hostPaths{}, fmt.Errorf("create %s: %w", paths.control, err)
	}
	_ = dir.Close()
	if err := os.MkdirAll(filepath.Dir(paths.socket), 0o700); err != nil {
		return hostPaths{}, fmt.Errorf("create %s: %w", filepath.Dir(paths.socket), err)
	}
	for _, dir := range e.KeptDirs(ws) {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return hostPaths{}, fmt.Errorf("create the workspace's mount directory %s: %w", dir, err)
		}
	}
	paths.mounts = e.workflowMounts(ws.Key)
	return paths, nil
}

// recordNames reports whether the environment record of the workspace at
// hostPath names the smolvm VM name. A missing, unreadable, or malformed
// record names none.
func recordNames(hostPath, name string) bool {
	data, err := workspacekit.ReadSortieFile(hostPath, workspace.EnvironmentRecordFile, recordMaxBytes)
	if err != nil {
		return false
	}
	var rec workspace.EnvironmentRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return false
	}
	return rec.Kind == config.EnvironmentKindSmolvm && rec.Name == name
}

// createSpec describes the VM for ws: the configured pack and sizing,
// the labels, the user, nested virtualization and the network backend
// when the workflow asks for them, the egress allowlist and credential bindings,
// Sortie's two host resources, the control directory and the tool
// channel socket, and the workflow's mounts after them.
func (e *Environment) createSpec(name string, ws environment.Workspace, paths hostPaths) (CreateSpec, error) {
	packSHA, err := e.packSHA256(e.cfg.Pack)
	if err != nil {
		return CreateSpec{}, err
	}
	spec := CreateSpec{
		Name:       name,
		Pack:       e.cfg.Pack,
		CPUs:       e.cfg.CPUs,
		MemoryMiB:  e.cfg.MemoryMiB,
		StorageGiB: e.cfg.StorageGiB,
		Labels: []Label{
			{LabelManaged, "1"},
			{LabelWorkspace, ws.Key},
			{LabelPack, packSHA},
			{LabelWorkflow, e.workflowHash},
		},
		User:       e.cfg.User,
		Nested:     e.cfg.Nested,
		NetBackend: e.cfg.NetBackend,
		SSHAgent:   e.cfg.SSHAgent,
		Volumes: append([]Volume{
			{Host: paths.control, Guest: GuestControlDir},
		}, paths.mounts...),
		Sockets: []SocketMount{{Host: paths.socket, Guest: GuestToolSocket}},
	}
	// A credential binding alone gives the VM unrestricted egress, so
	// its hosts always join the allowlist.
	seen := map[string]bool{}
	addHost := func(h string) {
		if !seen[h] {
			seen[h] = true
			spec.AllowHosts = append(spec.AllowHosts, h)
		}
	}
	for _, h := range e.cfg.AllowHosts {
		addHost(h)
	}
	for _, cred := range e.cfg.Credentials {
		for _, h := range cred.Hosts {
			addHost(h)
		}
		spec.Credentials = append(spec.Credentials, CredentialBinding{Name: cred.Name, Env: cred.Env, Hosts: cred.Hosts, Header: cred.Header})
	}
	// A backend is given only to a VM whose egress smolvm filters.
	if spec.NetBackend != "" && (!slices.Contains(config.SmolvmNetBackendsAccepted, spec.NetBackend) || len(spec.AllowHosts) == 0) {
		return CreateSpec{}, &config.ConfigError{
			Field:   "environment.smolvm.net_backend",
			Message: fmt.Sprintf("backend %q needs %s and allow_hosts or credentials, so that smolvm filters the VM's egress", spec.NetBackend, strings.Join(config.SmolvmNetBackendsAccepted, ", ")),
		}
	}
	return spec, nil
}

// packSHA256 returns the hex SHA-256 of the pack file at path, reusing
// the last result while the file's size and modification time are
// unchanged.
func (e *Environment) packSHA256(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", &config.ConfigError{Field: "environment.smolvm.pack", Message: fmt.Sprintf("pack %q is not readable: %v", path, err)}
	}
	if !info.Mode().IsRegular() {
		return "", &config.ConfigError{Field: "environment.smolvm.pack", Message: fmt.Sprintf("pack %q is not a regular file", path)}
	}
	e.packMu.Lock()
	defer e.packMu.Unlock()
	if cached, ok := e.packSums[path]; ok && cached.size == info.Size() && cached.modTime.Equal(info.ModTime()) {
		return cached.sum, nil
	}
	f, err := os.Open(path) //nolint:gosec // G304: the operator names the pack file.
	if err != nil {
		return "", &config.ConfigError{Field: "environment.smolvm.pack", Message: fmt.Sprintf("pack %q is not readable: %v", path, err)}
	}
	defer f.Close() //nolint:errcheck // read-only file
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("hash pack %q: %w", path, err)
	}
	sum := hex.EncodeToString(h.Sum(nil))
	e.packSums[path] = packSum{size: info.Size(), modTime: info.ModTime(), sum: sum}
	return sum, nil
}

// toolListenerArgv is the guest command that serves the tool channel: a
// socat listener on the guest loopback at [GuestToolPort] that joins each
// connection to [GuestToolSocket].
var toolListenerArgv = []string{
	"socat",
	"TCP-LISTEN:" + strconv.Itoa(GuestToolPort) + ",bind=127.0.0.1,reuseaddr,fork",
	"UNIX-CONNECT:" + GuestToolSocket,
}

// noSocatExit is the exit status of [toolListenerCheck] for a guest
// without socat.
const noSocatExit = 42

// toolListenerCheck exits [noSocatExit] when the guest has no socat.
var toolListenerCheck = []string{"sh", "-c", "command -v socat >/dev/null 2>&1 || exit " + strconv.Itoa(noSocatExit)}

// Attempts and spacing of the check. Right after a start, smolvm can run
// a command before the pack's root filesystem is in place, where socat is
// missing even from a pack that has it, so every failure is retried.
const (
	toolListenerAttempts = 10
	toolListenerBackoff  = 300 * time.Millisecond
)

// startToolListener starts the guest side of the tool channel in the
// running VM name, detached, so it lives as long as the VM runs. A VM
// whose listener already runs keeps it: the second listener cannot bind
// and exits. A pack without socat fails preparation with an error that
// names it.
func (e *Environment) startToolListener(ctx context.Context, name string) error {
	var err error
	for attempt := 1; attempt <= toolListenerAttempts; attempt++ {
		_, err = e.cli.Exec(ctx, name, ExecOptions{}, toolListenerCheck)
		if err == nil || ctx.Err() != nil || attempt == toolListenerAttempts {
			break
		}
		e.logger.DebugContext(ctx, "tool listener check failed, retrying", slog.String("vm", name), slog.Int("attempt", attempt), slog.Any("error", err))
		select {
		case <-ctx.Done():
		case <-time.After(toolListenerBackoff):
		}
	}
	var cliErr *CLIError
	if errors.As(err, &cliErr) && cliErr.ExitCode == noSocatExit {
		return fmt.Errorf("smolvm environment: the pack of VM %q has no socat, which carries Sortie's tool server into the VM; add it to the pack", name)
	}
	if err != nil {
		return fmt.Errorf("smolvm environment: check the tool listener's socat in VM %q: %w", name, err)
	}
	if _, err := e.cli.Exec(ctx, name, ExecOptions{Detach: true}, toolListenerArgv); err != nil {
		return fmt.Errorf("smolvm environment: start the tool listener in VM %q: %w", name, err)
	}
	return nil
}
