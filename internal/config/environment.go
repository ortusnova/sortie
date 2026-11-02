package config

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/sortie-ai/sortie/internal/typeutil"
)

// Environment kinds accepted by environment.kind.
const (
	// EnvironmentKindLocal runs workspace code on the host, in the host
	// workspace directory. It is the kind when environment.kind is
	// absent.
	EnvironmentKindLocal = "local"

	// EnvironmentKindSmolvm runs workspace code in one smolvm microVM
	// per issue workspace.
	EnvironmentKindSmolvm = "smolvm"

	// EnvironmentKindRemote hands each run to the worker matcher, to run
	// on a worker that dials Sortie. Nothing of the issue runs on
	// Sortie's host.
	EnvironmentKindRemote = "remote"
)

// credentialHeadersRefused lists, lowercased, the headers smolvm refuses
// to set from a credential because they route or frame the request.
var credentialHeadersRefused = []string{
	"connection", "content-length", "cookie", "host", "keep-alive",
	"proxy-authenticate", "proxy-authorization", "proxy-connection",
	"te", "trailer", "transfer-encoding", "upgrade",
}

// Defaults applied to the environment.smolvm block.
const (
	defaultSmolvmBinary     = "smolvm"
	defaultRepoBranchPrefix = "sortie/"
	defaultRepoMaxBundleMiB = 512
)

// Field paths of the environment block, and the host name length limit.
const (
	environmentField          = "environment"
	environmentHostPrefixPath = environmentField + ".host_prefix"
	smolvmField               = environmentField + ".smolvm"
	smolvmRepositoryField     = smolvmField + ".repository"
	smolvmRepositoriesField   = smolvmField + ".repositories"
	smolvmDefaultRepoField    = smolvmField + ".default_repository"
	smolvmCredentialsField    = smolvmField + ".credentials"
	maxHostNameLength         = 253
)

var (
	envVarNamePattern        = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	repositoryNamePattern    = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	credentialNamePattern    = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	headerNamePattern        = regexp.MustCompile("^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
	hostLabelPattern         = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$`)
	environmentKindsAccepted = []string{EnvironmentKindLocal, EnvironmentKindSmolvm, EnvironmentKindRemote}
)

// EnvironmentConfig holds the parsed environment block: where the
// workspace code of every issue runs.
type EnvironmentConfig struct {
	// Kind is the environment kind, [EnvironmentKindLocal] when the
	// block or its kind key is absent.
	Kind string

	// HostPrefix is the argv that stays on the host in front of the
	// environment's launch command for an agent session and its
	// credential check. Nil when absent; never an empty list.
	HostPrefix []string

	// Smolvm holds the smolvm settings. It is parsed and validated only
	// when Kind is [EnvironmentKindSmolvm]; under any other kind the
	// smolvm block is accepted and ignored, and this field is the zero
	// value.
	Smolvm SmolvmConfig

	// Remote holds the remote settings. It is parsed and validated only
	// when Kind is [EnvironmentKindRemote]; under any other kind it is
	// the zero value.
	Remote RemoteConfig
}

// SmolvmConfig holds the environment.smolvm settings.
type SmolvmConfig struct {
	// Binary is the smolvm program, "smolvm" by default, with ~ and
	// $VAR expanded.
	Binary string

	// Pack is the path of the pack file every VM is created from, with
	// ~ and $VAR expanded. Validation guarantees it named a readable
	// regular file when the configuration was built.
	Pack string

	// CPUs, MemoryMiB and StorageGiB size each VM. 0 selects smolvm's
	// own default.
	CPUs       int
	MemoryMiB  int
	StorageGiB int

	// AllowHosts lists the hosts a VM may reach, each an exact host name
	// or a "*.domain" pattern. Empty means no egress except the hosts of
	// the credential bindings.
	AllowHosts []string

	// Credentials lists the credential bindings, in written order.
	Credentials []SmolvmCredential

	// Repository configures the host-owned repository the guest
	// workspace is seeded from and exported to by default: the single
	// repository block, or the default_repository entry of the
	// repositories list. Nil when neither is set, and for a
	// repositories list that names no default.
	Repository *SmolvmRepository

	// Repositories lists every repository Sortie may open, in written
	// order: the repositories list, or the single repository block as
	// its one entry. Nil when neither is set.
	Repositories []SmolvmRepository

	// DefaultRepository names the entry of Repositories an issue that
	// names no repository uses. Empty for the single repository block,
	// and for a repositories list that names no default, under which an
	// issue that names no repository is not dispatched.
	DefaultRepository string

	// PassEnv lists host variable names copied into the guest in plain
	// text.
	PassEnv []string

	// SSHAgent forwards the host SSH agent into the guest.
	SSHAgent bool

	// Nested exposes the host's virtualization extensions in each VM,
	// so the guest can run KVM guests of its own.
	Nested bool

	// NetBackend names the smolvm network backend of each VM, one of
	// [SmolvmNetBackendsAccepted]. Empty keeps smolvm's own choice.
	NetBackend string

	// Mounts lists the host directories mounted into each VM, in
	// written order.
	Mounts []SmolvmMount

	// User is the user every command in each VM runs as: a user name
	// the pack's image defines, or a numeric uid[:gid]. Empty keeps
	// smolvm's own choice.
	User string
}

// SmolvmMount is a host directory mounted into each VM.
type SmolvmMount struct {
	// Host is the absolute host directory, with ~ and $VAR expanded. It
	// may hold [SmolvmWorkspacePlaceholder], which stands for the
	// workspace key.
	Host string

	// Guest is the absolute directory in the VM it is mounted at.
	Guest string

	// ReadOnly mounts the directory read-only.
	ReadOnly bool
}

// PerWorkspace reports whether the mount's host directory is one per
// workspace: whether Host holds [SmolvmWorkspacePlaceholder].
func (m SmolvmMount) PerWorkspace() bool {
	return strings.Contains(m.Host, SmolvmWorkspacePlaceholder)
}

// HostFor returns the mount's host directory for the workspace key.
func (m SmolvmMount) HostFor(key string) string {
	return strings.ReplaceAll(m.Host, SmolvmWorkspacePlaceholder, key)
}

// SmolvmWorkspacePlaceholder in a mount's host path stands for the
// workspace key.
const SmolvmWorkspacePlaceholder = "{workspace}"

// Guest paths the smolvm kind mounts itself. No workflow mount may
// cover them.
const (
	SmolvmGuestControlDir = "/run/sortie/control"
	SmolvmGuestToolSocket = "/run/sortie/tools.sock"
)

// SmolvmGuestUserTransferDir is where the smolvm kind stages the files it
// passes into and out of a VM whose user is set, since such a user
// cannot create a directory under /run/sortie. No workflow mount of such
// a VM may cover it.
const SmolvmGuestUserTransferDir = "/tmp/sortie-transfer"

// The network backends of environment.smolvm.net_backend. smolvm
// enforces an egress policy only with virtio-net; with tsi it refuses
// one, so tsi could never carry the allowlist and is refused.
const (
	SmolvmNetBackendVirtioNet = "virtio-net"
	SmolvmNetBackendTSI       = "tsi"
)

// SmolvmNetBackendsAccepted lists the values net_backend accepts.
var SmolvmNetBackendsAccepted = []string{SmolvmNetBackendVirtioNet}

// SmolvmCredential binds a host environment variable to the guest
// variable of the same name for a set of hosts. The guest sees only a
// placeholder; the real value is substituted on HTTPS requests to the
// listed hosts.
type SmolvmCredential struct {
	// Name is the smolvm credential name, matching [A-Za-z0-9_-]+.
	Name string

	// Env is the host variable name, which is also the guest variable
	// name.
	Env string

	// Hosts lists at least one host, each an exact host name or a
	// "*.domain" pattern.
	Hosts []string

	// Header optionally names a request header that is set to the real
	// value on every HTTPS request to Hosts, replacing any value the
	// guest sent in it.
	Header string

	// File optionally names a host file, with ~ and $VAR expanded, whose
	// content is the value. It is read each time a VM starts. Excludes
	// Command.
	File string

	// Command optionally holds a shell command run on the host whose
	// standard output is the value. It runs each time a VM starts.
	// Excludes File.
	Command string
}

// Sourced reports whether the binding's value comes from File or
// Command rather than from Sortie's own environment variable Env.
func (c SmolvmCredential) Sourced() bool { return c.File != "" || c.Command != "" }

// SmolvmRepository configures the host-owned repository mirror.
type SmolvmRepository struct {
	// Name identifies the repository to plan stories and issues.
	// Required in the repositories list, matching
	// [a-z0-9]+(-[a-z0-9]+)*; optional in the single repository block.
	Name string

	// URL is the upstream the host fetches and pushes with its own
	// credentials. Required.
	URL string

	// Base is the upstream branch a new workspace starts from.
	// Required.
	Base string

	// BranchPrefix precedes the workspace key in the issue branch name.
	// "sortie/" by default.
	BranchPrefix string

	// Provider optionally names the SCM adapter that opens pull
	// requests.
	Provider string

	// Project names the repository for Provider. Required when Provider
	// is set.
	Project string

	// MaxBundleMiB caps the size of a bundle exported from the guest.
	// Positive; 512 by default.
	MaxBundleMiB int

	// ClosedToDispatch keeps every issue that runs in the repository
	// from being dispatched: Sortie still lists it, for plan sync, and
	// leaves its issues to another Sortie. Set by dispatch: false on a
	// repositories entry; never set for the single repository block.
	ClosedToDispatch bool
}

// buildEnvironmentConfig parses the top-level environment block. The
// worker extension section is consulted for the rule that the smolvm
// kind excludes SSH workers. Returns the first [*ConfigError] found.
func buildEnvironmentConfig(raw map[string]any, worker map[string]any) (EnvironmentConfig, error) {
	cfg := EnvironmentConfig{Kind: EnvironmentKindLocal}

	value, ok := raw[environmentField]
	if !ok || value == nil {
		return cfg, nil
	}
	m, ok := value.(map[string]any)
	if !ok {
		return EnvironmentConfig{}, typeError(environmentField, "mapping", value)
	}

	if rawKind, exists := m["kind"]; exists && rawKind != nil {
		kind, isString := rawKind.(string)
		if !isString {
			return EnvironmentConfig{}, typeError(environmentField+".kind", "string", rawKind)
		}
		if !slices.Contains(environmentKindsAccepted, kind) {
			return EnvironmentConfig{}, &ConfigError{
				Field:   environmentField + ".kind",
				Message: fmt.Sprintf("unknown environment kind %q; valid: %s", kind, strings.Join(environmentKindsAccepted, ", ")),
			}
		}
		cfg.Kind = kind
	}

	prefix, err := parseHostPrefix(m)
	if err != nil {
		return EnvironmentConfig{}, err
	}
	cfg.HostPrefix = prefix

	if cfg.Kind == EnvironmentKindRemote {
		if hosts := WorkerSSHHosts(worker); len(hosts) > 0 {
			return EnvironmentConfig{}, &ConfigError{
				Field:   environmentField + ".kind",
				Message: fmt.Sprintf("environment.kind remote cannot be combined with worker.ssh_hosts (%d host(s) listed); remove one of the two settings", len(hosts)),
			}
		}
		remote, err := buildRemoteConfig(m["remote"], worker)
		if err != nil {
			return EnvironmentConfig{}, err
		}
		cfg.Remote = remote
		return cfg, nil
	}

	if cfg.Kind != EnvironmentKindSmolvm {
		return cfg, nil
	}

	if hosts := WorkerSSHHosts(worker); len(hosts) > 0 {
		return EnvironmentConfig{}, &ConfigError{
			Field:   environmentField + ".kind",
			Message: fmt.Sprintf("environment.kind smolvm cannot be combined with worker.ssh_hosts (%d host(s) listed); remove one of the two settings", len(hosts)),
		}
	}

	smolvm, err := buildSmolvmConfig(m["smolvm"])
	if err != nil {
		return EnvironmentConfig{}, err
	}
	cfg.Smolvm = smolvm
	return cfg, nil
}

// parseHostPrefix reads environment.host_prefix: absent or null yields
// nil; otherwise a non-empty list of non-empty strings.
func parseHostPrefix(m map[string]any) ([]string, error) {
	raw, exists := m["host_prefix"]
	if !exists || raw == nil {
		return nil, nil
	}
	list, ok := raw.([]any)
	if !ok {
		return nil, typeError(environmentHostPrefixPath, "list", raw)
	}
	if len(list) == 0 {
		return nil, &ConfigError{
			Field:   environmentHostPrefixPath,
			Message: "must not be an empty list; remove the key to launch without a host prefix",
		}
	}
	prefix := make([]string, 0, len(list))
	for i, elem := range list {
		field := fmt.Sprintf("%s[%d]", environmentHostPrefixPath, i)
		s, isString := elem.(string)
		if !isString {
			return nil, typeError(field, "string", elem)
		}
		if s == "" {
			return nil, &ConfigError{Field: field, Message: "must not be an empty string"}
		}
		prefix = append(prefix, s)
	}
	return prefix, nil
}

// buildSmolvmConfig parses environment.smolvm for the smolvm kind.
func buildSmolvmConfig(value any) (SmolvmConfig, error) {
	var m map[string]any
	if value != nil {
		var ok bool
		m, ok = value.(map[string]any)
		if !ok {
			return SmolvmConfig{}, typeError(smolvmField, "mapping", value)
		}
	}

	binary, err := smolvmPathField(m, "binary")
	if err != nil {
		return SmolvmConfig{}, err
	}
	if binary == "" {
		binary = defaultSmolvmBinary
	}

	pack, err := smolvmPathField(m, "pack")
	if err != nil {
		return SmolvmConfig{}, err
	}
	if err := checkPackFile(pack); err != nil {
		return SmolvmConfig{}, err
	}

	cfg := SmolvmConfig{Binary: binary, Pack: pack}

	for _, sizing := range []struct {
		key string
		dst *int
	}{
		{"cpus", &cfg.CPUs},
		{"memory_mib", &cfg.MemoryMiB},
		{"storage_gib", &cfg.StorageGiB},
	} {
		n, err := coerceIntField(m, sizing.key, smolvmField+"."+sizing.key)
		if err != nil {
			return SmolvmConfig{}, err
		}
		if n < 0 {
			return SmolvmConfig{}, &ConfigError{
				Field:   smolvmField + "." + sizing.key,
				Message: fmt.Sprintf("must not be negative, got %d", n),
			}
		}
		*sizing.dst = n
	}

	cfg.AllowHosts, err = parseHostList(m, "allow_hosts", smolvmField+".allow_hosts", false)
	if err != nil {
		return SmolvmConfig{}, err
	}

	cfg.Credentials, err = parseSmolvmCredentials(m)
	if err != nil {
		return SmolvmConfig{}, err
	}

	if err := parseSmolvmRepositories(m, &cfg); err != nil {
		return SmolvmConfig{}, err
	}

	cfg.PassEnv, err = parsePassEnv(m)
	if err != nil {
		return SmolvmConfig{}, err
	}

	if rawAgent, exists := m["ssh_agent"]; exists && rawAgent != nil {
		b, isBool := rawAgent.(bool)
		if !isBool {
			return SmolvmConfig{}, typeError(smolvmField+".ssh_agent", "boolean", rawAgent)
		}
		cfg.SSHAgent = b
	}

	if rawNested, exists := m["nested"]; exists && rawNested != nil {
		b, isBool := rawNested.(bool)
		if !isBool {
			return SmolvmConfig{}, typeError(smolvmField+".nested", "boolean", rawNested)
		}
		cfg.Nested = b
	}

	cfg.NetBackend, err = parseNetBackend(m, cfg)
	if err != nil {
		return SmolvmConfig{}, err
	}

	cfg.User, err = parseSmolvmUser(m)
	if err != nil {
		return SmolvmConfig{}, err
	}

	cfg.Mounts, err = parseSmolvmMounts(m)
	if err != nil {
		return SmolvmConfig{}, err
	}
	if cfg.User != "" {
		for i, mount := range cfg.Mounts {
			if pathsOverlap(mount.Guest, SmolvmGuestUserTransferDir) {
				return SmolvmConfig{}, &ConfigError{
					Field:   fmt.Sprintf("%s[%d].guest", smolvmMountsField, i),
					Message: fmt.Sprintf("%q overlaps %s, where Sortie stages repository bundles in a VM whose user is set", mount.Guest, SmolvmGuestUserTransferDir),
				}
			}
		}
	}

	return cfg, nil
}

// parseNetBackend reads environment.smolvm.net_backend. A backend
// smolvm cannot enforce the allowlist with is refused, and so is a
// backend without allow_hosts or credentials: smolvm gives such a VM no
// network to put the backend on, and opening one would leave its egress
// unfiltered.
func parseNetBackend(m map[string]any, cfg SmolvmConfig) (string, error) {
	field := smolvmField + ".net_backend"
	backend, present, err := requireStringField(m, "net_backend", field)
	if err != nil || !present {
		return "", err
	}
	switch {
	case backend == "":
		return "", &ConfigError{Field: field, Message: "must not be an empty string; remove the key to keep smolvm's own backend"}
	case backend == SmolvmNetBackendTSI:
		return "", &ConfigError{
			Field:   field,
			Message: fmt.Sprintf("smolvm does not enforce allow_hosts or the credential hosts with %q, so its egress would be unfiltered; use %s", backend, SmolvmNetBackendVirtioNet),
		}
	case !slices.Contains(SmolvmNetBackendsAccepted, backend):
		return "", &ConfigError{
			Field:   field,
			Message: fmt.Sprintf("unknown network backend %q; valid: %s", backend, strings.Join(SmolvmNetBackendsAccepted, ", ")),
		}
	case len(cfg.AllowHosts) == 0 && len(cfg.Credentials) == 0:
		return "", &ConfigError{
			Field:   field,
			Message: "needs allow_hosts or credentials: without them the VM has no network for the backend, and Sortie never opens unfiltered egress",
		}
	}
	return backend, nil
}

// smolvmUserPattern matches the values environment.smolvm.user accepts:
// a user name or uid of letters, digits, '.', '_' and '-', or a numeric
// uid:gid.
var smolvmUserPattern = regexp.MustCompile(`^(?:[A-Za-z0-9._-]+|[0-9]+:[0-9]+)$`)

// parseSmolvmUser reads environment.smolvm.user.
func parseSmolvmUser(m map[string]any) (string, error) {
	field := smolvmField + ".user"
	user, present, err := requireStringField(m, "user", field)
	if err != nil || !present {
		return "", err
	}
	switch {
	case user == "":
		return "", &ConfigError{Field: field, Message: "must not be an empty string; remove the key to run as root"}
	case !smolvmUserPattern.MatchString(user):
		return "", &ConfigError{
			Field:   field,
			Message: fmt.Sprintf("%q is not a user name or a numeric uid[:gid]: use letters, digits, '.', '_' and '-', or one ':' between two numbers", user),
		}
	}
	return user, nil
}

// smolvmMountsField is the field path of environment.smolvm.mounts.
const smolvmMountsField = smolvmField + ".mounts"

// parseSmolvmMounts reads environment.smolvm.mounts.
func parseSmolvmMounts(m map[string]any) ([]SmolvmMount, error) {
	raw, exists := m["mounts"]
	if !exists || raw == nil {
		return nil, nil
	}
	list, ok := raw.([]any)
	if !ok {
		return nil, typeError(smolvmMountsField, "list", raw)
	}
	mounts := make([]SmolvmMount, 0, len(list))
	guests := make(map[string]int, len(list))
	for i, elem := range list {
		field := fmt.Sprintf("%s[%d]", smolvmMountsField, i)
		entry, isMap := elem.(map[string]any)
		if !isMap {
			return nil, typeError(field, "mapping", elem)
		}
		mount, err := parseSmolvmMount(entry, field)
		if err != nil {
			return nil, err
		}
		if first, dup := guests[mount.Guest]; dup {
			return nil, &ConfigError{
				Field:   field + ".guest",
				Message: fmt.Sprintf("%q is already the guest path of %s[%d]", mount.Guest, smolvmMountsField, first),
			}
		}
		guests[mount.Guest] = i
		mounts = append(mounts, mount)
	}
	if len(mounts) == 0 {
		return nil, nil
	}
	return mounts, nil
}

// parseSmolvmMount reads one mounts entry at field.
func parseSmolvmMount(entry map[string]any, field string) (SmolvmMount, error) {
	var mount SmolvmMount
	for _, p := range []struct {
		key string
		dst *string
	}{
		{"host", &mount.Host},
		{"guest", &mount.Guest},
	} {
		pathField := field + "." + p.key
		s, err := requiredString(entry, p.key, pathField)
		if err != nil {
			return SmolvmMount{}, err
		}
		expanded, err := ExpandPath(s, true)
		if err != nil {
			return SmolvmMount{}, &ConfigError{Field: pathField, Message: err.Error()}
		}
		if strings.Contains(expanded, ":") {
			return SmolvmMount{}, &ConfigError{Field: pathField, Message: fmt.Sprintf("%q contains ':', which separates smolvm's mount fields", expanded)}
		}
		if !strings.HasPrefix(expanded, "/") {
			return SmolvmMount{}, &ConfigError{Field: pathField, Message: fmt.Sprintf("%q is not an absolute path after ~ and $VAR expansion", expanded)}
		}
		*p.dst = path.Clean(expanded)
	}

	if rawRO, exists := entry["read_only"]; exists && rawRO != nil {
		b, isBool := rawRO.(bool)
		if !isBool {
			return SmolvmMount{}, typeError(field+".read_only", "boolean", rawRO)
		}
		mount.ReadOnly = b
	}

	for _, own := range []string{SmolvmGuestControlDir, SmolvmGuestToolSocket} {
		if pathsOverlap(mount.Guest, own) {
			return SmolvmMount{}, &ConfigError{
				Field:   field + ".guest",
				Message: fmt.Sprintf("%q overlaps %s, which Sortie mounts itself", mount.Guest, own),
			}
		}
	}

	if !mount.PerWorkspace() {
		info, err := os.Stat(mount.Host)
		if err != nil {
			return SmolvmMount{}, &ConfigError{Field: field + ".host", Message: fmt.Sprintf("%q does not exist or is not readable: %v", mount.Host, err)}
		}
		if !info.IsDir() {
			return SmolvmMount{}, &ConfigError{Field: field + ".host", Message: fmt.Sprintf("%q is not a directory", mount.Host)}
		}
	}
	return mount, nil
}

// pathsOverlap reports whether the clean absolute slash paths a and b
// are the same path or one lies under the other.
func pathsOverlap(a, b string) bool {
	under := func(p, dir string) bool {
		return dir == "/" || strings.HasPrefix(p, dir+"/")
	}
	return a == b || under(a, b) || under(b, a)
}

// checkSmolvmMountsOutsideRoot refuses a mount whose host directory is
// or lies under the workspace root, where a per-workspace directory
// would go with its workspace and a shared one would reach into every
// workspace's control files.
func checkSmolvmMountsOutsideRoot(env EnvironmentConfig, root string) error {
	if env.Kind != EnvironmentKindSmolvm || root == "" {
		return nil
	}
	if abs, err := filepath.Abs(root); err == nil {
		root = abs
	}
	for i, mount := range env.Smolvm.Mounts {
		host := filepath.Clean(mount.HostFor("x"))
		if host == root || strings.HasPrefix(host, root+string(filepath.Separator)) {
			return &ConfigError{
				Field:   fmt.Sprintf("%s[%d].host", smolvmMountsField, i),
				Message: fmt.Sprintf("%q lies under workspace.root %q, whose directories Sortie removes with their workspaces", mount.Host, root),
			}
		}
	}
	return nil
}

// smolvmPathField reads a string path key of environment.smolvm and
// expands ~ and $VAR in it, as workspace.root is expanded.
func smolvmPathField(m map[string]any, key string) (string, error) {
	field := smolvmField + "." + key
	s, present, err := requireStringField(m, key, field)
	if err != nil || !present {
		return "", err
	}
	expanded, err := ExpandPath(s, true)
	if err != nil {
		return "", &ConfigError{Field: field, Message: err.Error()}
	}
	return expanded, nil
}

// checkPackFile requires the pack path to be set and to name a
// readable regular file.
func checkPackFile(pack string) error {
	field := smolvmField + ".pack"
	if pack == "" {
		return &ConfigError{Field: field, Message: "is required with environment.kind smolvm"}
	}
	info, err := os.Stat(pack)
	if err != nil {
		return &ConfigError{Field: field, Message: fmt.Sprintf("pack %q is not readable: %v", pack, err)}
	}
	if !info.Mode().IsRegular() {
		return &ConfigError{Field: field, Message: fmt.Sprintf("pack %q is not a regular file", pack)}
	}
	f, err := os.Open(pack) //nolint:gosec // G304: the operator names the pack; it is opened only to prove it is readable.
	if err != nil {
		return &ConfigError{Field: field, Message: fmt.Sprintf("pack %q is not readable: %v", pack, err)}
	}
	_ = f.Close()
	return nil
}

// parseHostList reads a list of host patterns at key. When required
// holds, an absent or empty list is an error.
func parseHostList(m map[string]any, key, field string, required bool) ([]string, error) {
	raw, exists := m[key]
	if !exists || raw == nil {
		if required {
			return nil, &ConfigError{Field: field, Message: "is required and must list at least one host"}
		}
		return nil, nil
	}
	list, ok := raw.([]any)
	if !ok {
		return nil, typeError(field, "list", raw)
	}
	if required && len(list) == 0 {
		return nil, &ConfigError{Field: field, Message: "must list at least one host"}
	}
	hosts := make([]string, 0, len(list))
	for i, elem := range list {
		elemField := fmt.Sprintf("%s[%d]", field, i)
		s, isString := elem.(string)
		if !isString {
			return nil, typeError(elemField, "string", elem)
		}
		if err := validateHostPattern(s); err != nil {
			return nil, &ConfigError{Field: elemField, Message: fmt.Sprintf("invalid host %q: %v", s, err)}
		}
		hosts = append(hosts, s)
	}
	if len(hosts) == 0 {
		return nil, nil
	}
	return hosts, nil
}

// validateHostPattern accepts an exact host name or "*.domain", where
// domain is itself a host name. Schemes, ports, paths and wildcards in
// any other position are rejected.
func validateHostPattern(s string) error {
	if s == "" {
		return fmt.Errorf("must not be empty")
	}
	name := s
	if rest, found := strings.CutPrefix(s, "*."); found {
		name = rest
	}
	if strings.Contains(name, "*") {
		return fmt.Errorf("a wildcard is allowed only as a leading \"*.\"")
	}
	if len(name) > maxHostNameLength {
		return fmt.Errorf("longer than %d characters", maxHostNameLength)
	}
	for label := range strings.SplitSeq(name, ".") {
		if !hostLabelPattern.MatchString(label) {
			return fmt.Errorf("want an exact host name or \"*.domain\"")
		}
	}
	return nil
}

// parseSmolvmCredentials reads environment.smolvm.credentials.
func parseSmolvmCredentials(m map[string]any) ([]SmolvmCredential, error) {
	raw, exists := m["credentials"]
	if !exists || raw == nil {
		return nil, nil
	}
	list, ok := raw.([]any)
	if !ok {
		return nil, typeError(smolvmCredentialsField, "list", raw)
	}
	creds := make([]SmolvmCredential, 0, len(list))
	seen := make(map[string]int, len(list))
	for i, elem := range list {
		field := fmt.Sprintf("%s[%d]", smolvmCredentialsField, i)
		entry, isMap := elem.(map[string]any)
		if !isMap {
			return nil, typeError(field, "mapping", elem)
		}

		name, err := requiredString(entry, "name", field+".name")
		if err != nil {
			return nil, err
		}
		if !credentialNamePattern.MatchString(name) {
			return nil, &ConfigError{
				Field:   field + ".name",
				Message: fmt.Sprintf("invalid credential name %q; use letters, digits, '_' and '-'", name),
			}
		}
		if first, dup := seen[name]; dup {
			return nil, &ConfigError{
				Field:   field + ".name",
				Message: fmt.Sprintf("credential name %q is already used by %s[%d]", name, smolvmCredentialsField, first),
			}
		}
		seen[name] = i

		env, err := requiredString(entry, "env", field+".env")
		if err != nil {
			return nil, err
		}
		if !envVarNamePattern.MatchString(env) {
			return nil, &ConfigError{
				Field:   field + ".env",
				Message: fmt.Sprintf("invalid environment variable name %q", env),
			}
		}

		hosts, err := parseHostList(entry, "hosts", field+".hosts", true)
		if err != nil {
			return nil, err
		}

		header, present, err := requireStringField(entry, "header", field+".header")
		if err != nil {
			return nil, err
		}
		if present {
			if err := checkCredentialHeader(header, field+".header"); err != nil {
				return nil, err
			}
		}

		file, command, err := parseCredentialSource(entry, field)
		if err != nil {
			return nil, err
		}

		creds = append(creds, SmolvmCredential{Name: name, Env: env, Hosts: hosts, Header: header, File: file, Command: command})
	}
	if len(creds) == 0 {
		return nil, nil
	}
	return creds, nil
}

// parseCredentialSource reads the optional file and command keys of the
// credential binding entry at field. At most one may be set; the file is
// expanded and must be absolute. Neither is read here: the value is read
// each time a VM starts.
func parseCredentialSource(entry map[string]any, field string) (file, command string, err error) {
	rawFile, filePresent, err := requireStringField(entry, "file", field+".file")
	if err != nil {
		return "", "", err
	}
	command, commandPresent, err := requireStringField(entry, "command", field+".command")
	if err != nil {
		return "", "", err
	}
	if filePresent && commandPresent {
		return "", "", &ConfigError{Field: field, Message: "sets both file and command; a credential has one source"}
	}
	if commandPresent && strings.TrimSpace(command) == "" {
		return "", "", &ConfigError{Field: field + ".command", Message: "must not be empty; remove the key to read the value from env"}
	}
	if filePresent {
		if rawFile == "" {
			return "", "", &ConfigError{Field: field + ".file", Message: "must not be empty; remove the key to read the value from env"}
		}
		file, err = ExpandPath(rawFile, true)
		if err != nil {
			return "", "", &ConfigError{Field: field + ".file", Message: err.Error()}
		}
		if !filepath.IsAbs(file) {
			return "", "", &ConfigError{Field: field + ".file", Message: fmt.Sprintf("%q is not an absolute path after ~ and $VAR expansion", file)}
		}
		file = filepath.Clean(file)
	}
	return file, command, nil
}

// checkCredentialHeader refuses a header name that is not an HTTP field
// name token, which also keeps the separators of smolvm's credential
// syntax out of it, and a header smolvm refuses to set.
func checkCredentialHeader(header, field string) error {
	if header == "" {
		return &ConfigError{Field: field, Message: "must not be an empty string; remove the key to set no header"}
	}
	if !headerNamePattern.MatchString(header) {
		return &ConfigError{
			Field:   field,
			Message: fmt.Sprintf("invalid header name %q; use letters, digits, and !#$%%&'*+-.^_`|~", header),
		}
	}
	if slices.Contains(credentialHeadersRefused, strings.ToLower(header)) {
		return &ConfigError{
			Field:   field,
			Message: fmt.Sprintf("header %q routes or frames the request and cannot carry a credential", header),
		}
	}
	return nil
}

// parsePassEnv reads environment.smolvm.pass_env, a list of host
// variable names.
func parsePassEnv(m map[string]any) ([]string, error) {
	field := smolvmField + ".pass_env"
	raw, exists := m["pass_env"]
	if !exists || raw == nil {
		return nil, nil
	}
	list, ok := raw.([]any)
	if !ok {
		return nil, typeError(field, "list", raw)
	}
	names := make([]string, 0, len(list))
	for i, elem := range list {
		elemField := fmt.Sprintf("%s[%d]", field, i)
		s, isString := elem.(string)
		if !isString {
			return nil, typeError(elemField, "string", elem)
		}
		if !envVarNamePattern.MatchString(s) {
			return nil, &ConfigError{Field: elemField, Message: fmt.Sprintf("invalid environment variable name %q", s)}
		}
		names = append(names, s)
	}
	if len(names) == 0 {
		return nil, nil
	}
	return names, nil
}

// parseSmolvmRepository reads environment.smolvm.repository, applying
// its defaults.
func parseSmolvmRepository(m map[string]any) (*SmolvmRepository, error) {
	raw, exists := m["repository"]
	if !exists || raw == nil {
		return nil, nil
	}
	entry, ok := raw.(map[string]any)
	if !ok {
		return nil, typeError(smolvmRepositoryField, "mapping", raw)
	}
	if _, set := entry["dispatch"]; set {
		return nil, &ConfigError{Field: smolvmRepositoryField + ".dispatch", Message: "applies to repositories entries, not to the single repository block"}
	}
	return parseRepositoryEntry(entry, smolvmRepositoryField)
}

// RepositoryNamed returns the configured repository called name, and
// whether there is one. The single repository block answers its own
// name, which may be empty.
func (c SmolvmConfig) RepositoryNamed(name string) (*SmolvmRepository, bool) {
	for _, r := range c.AllRepositories() {
		if r.Name == name {
			return &r, true
		}
	}
	return nil, false
}

// AllRepositories returns Repositories, or the one Repository when only
// that is set.
func (c SmolvmConfig) AllRepositories() []SmolvmRepository {
	if len(c.Repositories) == 0 && c.Repository != nil {
		return []SmolvmRepository{*c.Repository}
	}
	return c.Repositories
}

// ListsRepositories reports whether the repositories list, rather than
// the single repository block, configures the repositories.
func (c SmolvmConfig) ListsRepositories() bool {
	return len(c.Repositories) > 0 && (c.DefaultRepository != "" || c.Repository == nil)
}

// ErrNotDispatched reports an issue that runs in no repository the
// workflow dispatches, which Sortie leaves to another Sortie.
var ErrNotDispatched = errors.New("not dispatched by this workflow")

// DispatchRepository returns the repository an issue with labels runs
// in when no seeded workspace names one: the repository its
// [RepositoryLabelPrefix] label names, or the default for an issue
// without one. An error wrapping [ErrNotDispatched] reports an issue
// that names an unlisted repository or one closed to dispatch, or that
// names none under a repositories list without a default.
func (c SmolvmConfig) DispatchRepository(labels []string) (*SmolvmRepository, error) {
	name := IssueRepositoryName(labels)
	if name == "" {
		if c.Repository == nil {
			return nil, fmt.Errorf("the issue names no repository and environment.smolvm sets no default_repository: %w", ErrNotDispatched)
		}
		repo := *c.Repository
		return &repo, nil
	}
	repo, found := c.RepositoryNamed(name)
	if !found {
		return nil, fmt.Errorf("the issue names repository %q, which environment.smolvm.repositories does not list: %w", name, ErrNotDispatched)
	}
	if repo.ClosedToDispatch {
		return nil, fmt.Errorf("the issue names repository %q, which is closed to dispatch: %w", name, ErrNotDispatched)
	}
	return repo, nil
}

// RepositoryNames lists the names of the configured repositories in
// written order, leaving out an unnamed single repository block.
func (c SmolvmConfig) RepositoryNames() []string {
	var names []string
	for _, r := range c.AllRepositories() {
		if r.Name != "" {
			names = append(names, r.Name)
		}
	}
	return names
}

// parseSmolvmRepositories reads environment.smolvm.repository, or
// environment.smolvm.repositories with default_repository, into cfg.
func parseSmolvmRepositories(m map[string]any, cfg *SmolvmConfig) error {
	single, err := parseSmolvmRepository(m)
	if err != nil {
		return err
	}
	rawList, listSet := m["repositories"]
	listSet = listSet && rawList != nil
	def, defSet, err := requireStringField(m, "default_repository", smolvmDefaultRepoField)
	if err != nil {
		return err
	}
	if single != nil {
		if listSet {
			return &ConfigError{Field: smolvmRepositoriesField, Message: "cannot be set together with repository; list the one repository under repositories instead"}
		}
		if defSet {
			return &ConfigError{Field: smolvmDefaultRepoField, Message: "applies to repositories, not to the single repository block"}
		}
		cfg.Repository = single
		cfg.Repositories = []SmolvmRepository{*single}
		return nil
	}
	if !listSet {
		if defSet {
			return &ConfigError{Field: smolvmDefaultRepoField, Message: "is set but repositories lists none"}
		}
		return nil
	}
	list, ok := rawList.([]any)
	if !ok {
		return typeError(smolvmRepositoriesField, "sequence", rawList)
	}
	if len(list) == 0 {
		return &ConfigError{Field: smolvmRepositoriesField, Message: "must list at least one repository"}
	}
	seen := map[string]bool{}
	repos := make([]SmolvmRepository, 0, len(list))
	for i, elem := range list {
		field := fmt.Sprintf("%s[%d]", smolvmRepositoriesField, i)
		entry, ok := elem.(map[string]any)
		if !ok {
			return typeError(field, "mapping", elem)
		}
		repo, err := parseRepositoryEntry(entry, field)
		if err != nil {
			return err
		}
		if rawDispatch, set := entry["dispatch"]; set && rawDispatch != nil {
			open, isBool := rawDispatch.(bool)
			if !isBool {
				return typeError(field+".dispatch", "boolean", rawDispatch)
			}
			repo.ClosedToDispatch = !open
		}
		if repo.Name == "" {
			return &ConfigError{Field: field + ".name", Message: "is required and must be a non-empty string"}
		}
		if seen[repo.Name] {
			return &ConfigError{Field: field + ".name", Message: fmt.Sprintf("%q is listed more than once", repo.Name)}
		}
		seen[repo.Name] = true
		repos = append(repos, *repo)
	}
	cfg.Repositories = repos
	if def == "" {
		return nil
	}
	if !seen[def] {
		return &ConfigError{Field: smolvmDefaultRepoField, Message: fmt.Sprintf("names %q, which repositories does not list", def)}
	}
	cfg.DefaultRepository = def
	cfg.Repository, _ = cfg.RepositoryNamed(def)
	if cfg.Repository.ClosedToDispatch {
		return &ConfigError{Field: smolvmDefaultRepoField, Message: fmt.Sprintf("names %q, which is closed to dispatch; leave default_repository out to dispatch no issue that names no repository", def)}
	}
	return nil
}

// parseRepositoryEntry reads one repository mapping at field, applying
// its defaults.
func parseRepositoryEntry(entry map[string]any, field string) (*SmolvmRepository, error) {
	name, _, err := requireStringField(entry, "name", field+".name")
	if err != nil {
		return nil, err
	}
	if name != "" && !repositoryNamePattern.MatchString(name) {
		return nil, &ConfigError{Field: field + ".name", Message: fmt.Sprintf("%q must match [a-z0-9]+(-[a-z0-9]+)*", name)}
	}
	smolvmRepositoryField := field

	url, err := requiredString(entry, "url", smolvmRepositoryField+".url")
	if err != nil {
		return nil, err
	}
	base, err := requiredString(entry, "base", smolvmRepositoryField+".base")
	if err != nil {
		return nil, err
	}

	prefix, present, err := requireStringField(entry, "branch_prefix", smolvmRepositoryField+".branch_prefix")
	if err != nil {
		return nil, err
	}
	if !present {
		prefix = defaultRepoBranchPrefix
	}

	provider, _, err := requireStringField(entry, "provider", smolvmRepositoryField+".provider")
	if err != nil {
		return nil, err
	}
	project, _, err := requireStringField(entry, "project", smolvmRepositoryField+".project")
	if err != nil {
		return nil, err
	}
	if provider != "" && project == "" {
		return nil, &ConfigError{
			Field:   smolvmRepositoryField + ".project",
			Message: fmt.Sprintf("is required when repository.provider is set (provider %q)", provider),
		}
	}

	maxBundle := defaultRepoMaxBundleMiB
	if v, set := entry["max_bundle_mib"]; set && v != nil {
		n, err := coerceIntField(entry, "max_bundle_mib", smolvmRepositoryField+".max_bundle_mib")
		if err != nil {
			return nil, err
		}
		if n <= 0 {
			return nil, &ConfigError{
				Field:   smolvmRepositoryField + ".max_bundle_mib",
				Message: fmt.Sprintf("must be a positive integer, got %d", n),
			}
		}
		maxBundle = n
	}

	return &SmolvmRepository{
		Name:         name,
		URL:          url,
		Base:         base,
		BranchPrefix: prefix,
		Provider:     provider,
		Project:      project,
		MaxBundleMiB: maxBundle,
	}, nil
}

// requiredString reads a required non-empty string key.
func requiredString(m map[string]any, key, field string) (string, error) {
	s, present, err := requireStringField(m, key, field)
	if err != nil {
		return "", err
	}
	if !present || s == "" {
		return "", &ConfigError{Field: field, Message: "is required and must be a non-empty string"}
	}
	return s, nil
}

// typeError reports a value of the wrong YAML type at field.
func typeError(field, want string, got any) *ConfigError {
	return &ConfigError{
		Field:   field,
		Message: "expected " + want + ", got " + typeutil.DescribeYAMLType(got),
	}
}

// RepositoryLabelPrefix starts the label plan sync keeps on a story that
// names its repository: the prefix followed by the repository's name.
const RepositoryLabelPrefix = "sortie-repository:"

// IssueRepositoryName returns the repository name the labels of an issue
// carry under [RepositoryLabelPrefix], or "" when they carry none. Labels
// compare case-insensitively.
func IssueRepositoryName(labels []string) string {
	for _, l := range labels {
		if name, ok := strings.CutPrefix(strings.ToLower(strings.TrimSpace(l)), RepositoryLabelPrefix); ok && name != "" {
			return name
		}
	}
	return ""
}
