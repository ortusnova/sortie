package config

import (
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/sortie-ai/sortie/internal/egress"
)

const (
	// DefaultWorkerIdentityHeader names the worker of a worker API
	// request when worker.identity_header is unset.
	DefaultWorkerIdentityHeader = "X-Sortie-Worker-Identity"

	// DefaultRemoteBackend is environment.remote.backend when unset.
	DefaultRemoteBackend = "smolvm"

	defaultRemoteLeaseTTLMS = 60000

	remoteField = environmentField + ".remote"
)

// RemoteFlows names the data-plane flows of a remote job, in the order
// an offer documents them. Each needs a URL in environment.remote.flows.
var RemoteFlows = []string{"stdio", "egress", "mcp", "bundles", "usage"}

// remoteSchemaFields are the known keys of environment.remote.
var remoteSchemaFields = []FieldDef{
	{Name: "image", Type: FieldString},
	{Name: "backend", Type: FieldString},
	{Name: "requires", Type: FieldMap, Nested: []FieldDef{
		{Name: "capabilities", Type: FieldStringList},
		{Name: "cpu", Type: FieldNumber},
		{Name: "mem_gib", Type: FieldNumber},
		{Name: "disk_gib", Type: FieldNumber},
	}},
	{Name: "credentials", Type: FieldSequence},
	{Name: "flows", Type: FieldMap, Nested: func() []FieldDef {
		out := make([]FieldDef, 0, len(RemoteFlows))
		for _, f := range RemoteFlows {
			out = append(out, FieldDef{Name: f, Type: FieldMap, Nested: []FieldDef{{Name: "url", Type: FieldString}}})
		}
		return out
	}()},
	{Name: "lease_ttl_ms", Type: FieldInt},
	{Name: "repository", Type: FieldMap},
	{Name: "repositories", Type: FieldSequence},
	{Name: "default_repository", Type: FieldString},
	{Name: "egress", Type: FieldMap},
	{Name: "profiles", Type: FieldMap},
}

// remoteRepositoryKeys are the keys of a remote repository entry.
var remoteRepositoryKeys = []string{
	"name", "url", "base", "branch_prefix", "provider", "project", "max_bundle_mib",
	"dispatch", "image", "requires",
}

// RemoteConfig holds the environment.remote settings and the worker
// identity header the remote kind's worker API reads.
type RemoteConfig struct {
	// IdentityHeader is worker.identity_header: the request header that
	// names the worker of a worker API request.
	IdentityHeader string

	// Requires is what a worker must declare to take a job of a
	// repository that sets no requires of its own.
	Requires RemoteRequirements

	// Backend is the isolation backend the worker starts the machine
	// with. A job also requires it as a capability.
	Backend string

	// Image is the machine image reference the worker starts for a job
	// of a repository that sets no image of its own. Empty only when
	// every repository sets one.
	Image string

	// Credentials lists the credentials the agent needs. The machine
	// sees each only as a placeholder that the egress proxy replaces;
	// Sortie never reads their values.
	Credentials []RemoteCredential

	// Flows maps each name of [RemoteFlows] to the URL a worker reaches
	// that flow at.
	Flows map[string]string

	// LeaseTTLMS is how long a lease lasts without a heartbeat.
	LeaseTTLMS int

	// Repository is the host-owned repository a job's workspace is
	// seeded from and whose issue branch comes back from the worker as
	// a bundle: the single repository block, or the default_repository
	// entry of the repositories list, until an issue's run sets it to
	// the issue's own. Nil when neither is set, and for a repositories
	// list that names no default.
	Repository *Repository

	// Repositories lists every repository Sortie may open, in written
	// order: the repositories list, or the single repository block as
	// its one entry. Nil when neither is set: Sortie then serves no
	// seed and pushes nothing.
	Repositories []Repository

	// DefaultRepository names the entry of Repositories an issue that
	// names no repository runs in.
	DefaultRepository string

	// Egress is environment.remote.egress: the hosts every job needs for
	// its agent harness, whatever its repository. Empty, with its lists
	// not nil, when the workflow declares none.
	Egress egress.Policy

	// Profiles is environment.remote.profiles: the job profiles a
	// supervisor fix run or live-proof request may name, by name. Nil
	// when the workflow declares none.
	Profiles map[string]JobProfile
}

// JobProfile is a job profile: the image and requirements a job that
// names it runs with, in place of its repository's.
type JobProfile struct {
	// Image is the machine image of the job; empty keeps the
	// repository's.
	Image string
	// Requires is what a worker must declare to take the job. It
	// replaces the repository's whole; the backend is still required.
	Requires RemoteRequirements
}

// JobProfile returns the profile named name, and whether the workflow
// declares it.
func (c RemoteConfig) JobProfile(name string) (JobProfile, bool) {
	p, ok := c.Profiles[name]
	return p, ok
}

// JobImage returns the image a job of repo runs in: repo's own, or
// Image when repo is nil or sets none.
func (c RemoteConfig) JobImage(repo *Repository) string {
	if repo != nil && repo.Image != "" {
		return repo.Image
	}
	return c.Image
}

// JobRequires returns what a worker must declare to take a job of repo:
// repo's own requirements, or Requires when repo is nil or sets none.
func (c RemoteConfig) JobRequires(repo *Repository) RemoteRequirements {
	if repo != nil && repo.Requires != nil {
		return *repo.Requires
	}
	return c.Requires
}

// RemoteRequirements is what each remote job needs of a worker: every
// capability listed, and at least the CPUs, memory and disk free.
type RemoteRequirements struct {
	Capabilities []string
	CPU          float64
	MemGiB       float64
	DiskGiB      float64
}

// RemoteCredential names a credential the agent needs under the remote
// kind. Env is set to a placeholder in the machine; the egress proxy
// replaces it on requests to Hosts.
type RemoteCredential struct {
	Name   string
	Env    string
	Hosts  []string
	Header string
}

// buildRemoteConfig parses environment.remote and worker.identity_header
// for the remote kind.
func buildRemoteConfig(value any, worker map[string]any) (RemoteConfig, error) {
	var m map[string]any
	if value != nil {
		var ok bool
		m, ok = value.(map[string]any)
		if !ok {
			return RemoteConfig{}, typeError(remoteField, "mapping", value)
		}
	}
	if err := refuseUnknownKeys(m, remoteField, remoteSchemaFieldNames()); err != nil {
		return RemoteConfig{}, err
	}
	cfg := RemoteConfig{IdentityHeader: DefaultWorkerIdentityHeader, Backend: DefaultRemoteBackend, LeaseTTLMS: defaultRemoteLeaseTTLMS}
	header, present, err := requireStringField(worker, "identity_header", "worker.identity_header")
	if err != nil {
		return RemoteConfig{}, err
	}
	if present {
		if header == "" || strings.ContainsAny(header, " \t:") {
			return RemoteConfig{}, &ConfigError{Field: "worker.identity_header", Message: fmt.Sprintf("must be an HTTP header name, got %q", header)}
		}
		cfg.IdentityHeader = header
	}
	if cfg.Requires, err = parseRemoteRequirements(m["requires"], remoteField+".requires"); err != nil {
		return RemoteConfig{}, err
	}
	backend, present, err := requireStringField(m, "backend", remoteField+".backend")
	if err != nil {
		return RemoteConfig{}, err
	}
	if present {
		if backend == "" {
			return RemoteConfig{}, &ConfigError{Field: remoteField + ".backend", Message: "must not be an empty string; remove the key for the default"}
		}
		cfg.Backend = backend
	}
	image, present, err := requireStringField(m, "image", remoteField+".image")
	if err != nil {
		return RemoteConfig{}, err
	}
	if present && image == "" {
		return RemoteConfig{}, &ConfigError{Field: remoteField + ".image", Message: "must not be an empty string"}
	}
	cfg.Image = image
	if cfg.Credentials, err = parseRemoteCredentials(m["credentials"]); err != nil {
		return RemoteConfig{}, err
	}
	if cfg.Flows, err = parseRemoteFlows(m["flows"]); err != nil {
		return RemoteConfig{}, err
	}
	if cfg.Egress, err = parseRemoteEgress(m["egress"]); err != nil {
		return RemoteConfig{}, err
	}
	if cfg.Profiles, err = parseRemoteProfiles(m["profiles"]); err != nil {
		return RemoteConfig{}, err
	}
	if v, exists := m["lease_ttl_ms"]; exists && v != nil {
		n, err := coerceIntField(m, "lease_ttl_ms", remoteField+".lease_ttl_ms")
		if err != nil {
			return RemoteConfig{}, err
		}
		if n <= 0 {
			return RemoteConfig{}, &ConfigError{Field: remoteField + ".lease_ttl_ms", Message: fmt.Sprintf("must be positive, got %d", n)}
		}
		cfg.LeaseTTLMS = n
	}
	single, err := parseRemoteRepository(m["repository"])
	if err != nil {
		return RemoteConfig{}, err
	}
	var set RepositorySet
	if err := parseRepositoryList(m, remoteField, single, parseRemoteRepositoryEntry, &set); err != nil {
		return RemoteConfig{}, err
	}
	cfg.Repository, cfg.Repositories, cfg.DefaultRepository = set.Repository, set.Repositories, set.DefaultRepository
	if cfg.Image == "" {
		if err := requireRemoteImage(cfg); err != nil {
			return RemoteConfig{}, err
		}
	}
	return cfg, nil
}

// requireRemoteImage refuses a remote block without image unless every
// repository sets its own.
func requireRemoteImage(cfg RemoteConfig) error {
	const field = remoteField + ".image"
	if len(cfg.Repositories) == 0 {
		return &ConfigError{Field: field, Message: "is required and must be a non-empty string"}
	}
	for i, r := range cfg.Repositories {
		if r.Image == "" {
			name := r.Name
			if name == "" {
				name = "the repository block"
			}
			return &ConfigError{Field: field, Message: fmt.Sprintf("is required: %s (%s) sets no image of its own", name, cfg.repositoryField(i))}
		}
	}
	return nil
}

// repositoryField returns the setting path of the repository at index
// i of Repositories.
func (c RemoteConfig) repositoryField(i int) string {
	if c.DefaultRepository == "" && c.Repository != nil {
		return remoteField + ".repository"
	}
	return fmt.Sprintf("%s.repositories[%d]", remoteField, i)
}

// parseRemoteRepository reads environment.remote.repository with the
// fields and defaults of a remote repositories entry, except dispatch.
func parseRemoteRepository(raw any) (*Repository, error) {
	const field = remoteField + ".repository"
	if raw == nil {
		return nil, nil
	}
	entry, ok := raw.(map[string]any)
	if !ok {
		return nil, typeError(field, "mapping", raw)
	}
	if _, set := entry["dispatch"]; set {
		return nil, &ConfigError{Field: field + ".dispatch", Message: "applies to repositories entries, not to the single repository block"}
	}
	return parseRemoteRepositoryEntry(entry, field)
}

// parseRemoteRepositoryEntry reads one remote repository mapping at
// field: the fields and defaults of a repository, and the image
// and requires of its jobs.
func parseRemoteRepositoryEntry(entry map[string]any, field string) (*Repository, error) {
	if err := refuseUnknownKeys(entry, field, remoteRepositoryKeys); err != nil {
		return nil, err
	}
	repo, err := parseRepositoryEntry(entry, field)
	if err != nil {
		return nil, err
	}
	image, present, err := requireStringField(entry, "image", field+".image")
	if err != nil {
		return nil, err
	}
	if present && image == "" {
		return nil, &ConfigError{Field: field + ".image", Message: "must not be an empty string; remove the key to take environment.remote.image"}
	}
	repo.Image = image
	if raw, set := entry["requires"]; set && raw != nil {
		req, err := parseRemoteRequirements(raw, field+".requires")
		if err != nil {
			return nil, err
		}
		repo.Requires = &req
	}
	return repo, nil
}

// refuseRemoteHooks refuses a workspace hook under the remote kind,
// which runs nothing but the agent in a job's workspace.
func refuseRemoteHooks(env EnvironmentConfig, hooks HooksConfig) error {
	if env.Kind != EnvironmentKindRemote {
		return nil
	}
	for _, h := range []struct{ key, script string }{
		{"after_create", hooks.AfterCreate},
		{"before_run", hooks.BeforeRun},
		{"after_run", hooks.AfterRun},
		{"before_remove", hooks.BeforeRemove},
	} {
		if strings.TrimSpace(h.script) != "" {
			return &ConfigError{
				Field:   "hooks." + h.key,
				Message: "is not run under environment.kind remote, where only the agent runs in the job's workspace; remove the hook",
			}
		}
	}
	return nil
}

// remoteSchemaFieldNames lists the keys of environment.remote.
func remoteSchemaFieldNames() []string {
	names := make([]string, 0, len(remoteSchemaFields))
	for _, f := range remoteSchemaFields {
		names = append(names, f.Name)
	}
	return names
}

// refuseUnknownKeys returns a [*ConfigError] naming the first key of m,
// in sorted order, that known does not list.
func refuseUnknownKeys(m map[string]any, field string, known []string) error {
	keys := make([]string, 0, len(m))
	for key := range m {
		if !slices.Contains(known, key) {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	slices.Sort(keys)
	return &ConfigError{
		Field:   field + "." + keys[0],
		Message: "unknown key: the remote kind does not support it; valid: " + strings.Join(known, ", "),
	}
}

// RepositorySet returns the repository settings of the kind c selects:
// the remote block's own. Every other kind configures none.
func (c EnvironmentConfig) RepositorySet() RepositorySet {
	if c.Kind != EnvironmentKindRemote {
		return RepositorySet{}
	}
	return RepositorySet{Repository: c.Remote.Repository, Repositories: c.Remote.Repositories, DefaultRepository: c.Remote.DefaultRepository}
}

// SetRepository makes repo the repository of the kind c selects, the one
// an issue's run uses.
func (c *EnvironmentConfig) SetRepository(repo *Repository) {
	if c.Kind == EnvironmentKindRemote {
		c.Remote.Repository = repo
	}
}

// RepositoryField returns the setting path of the repository at index i
// of [EnvironmentConfig.RepositorySet], for an error about it.
func (c EnvironmentConfig) RepositoryField(i int) string {
	return c.Remote.repositoryField(i)
}

// parseRemoteRequirements reads the requires mapping at field: that of
// environment.remote or of one of its repositories.
func parseRemoteRequirements(value any, field string) (RemoteRequirements, error) {
	var req RemoteRequirements
	if value == nil {
		return req, nil
	}
	m, ok := value.(map[string]any)
	if !ok {
		return req, typeError(field, "mapping", value)
	}
	for key := range m {
		if !slices.Contains([]string{"capabilities", "cpu", "mem_gib", "disk_gib"}, key) {
			return req, &ConfigError{Field: field + "." + key, Message: "unknown key; valid: capabilities, cpu, mem_gib, disk_gib"}
		}
	}
	if raw, exists := m["capabilities"]; exists && raw != nil {
		items, ok := raw.([]any)
		if !ok {
			return req, typeError(field+".capabilities", "list", raw)
		}
		for i, item := range items {
			s, ok := item.(string)
			if !ok || s == "" {
				return req, &ConfigError{Field: fmt.Sprintf("%s.capabilities[%d]", field, i), Message: "must be a non-empty string"}
			}
			req.Capabilities = append(req.Capabilities, s)
		}
	}
	for _, size := range []struct {
		key string
		dst *float64
	}{{"cpu", &req.CPU}, {"mem_gib", &req.MemGiB}, {"disk_gib", &req.DiskGiB}} {
		raw, exists := m[size.key]
		if !exists || raw == nil {
			continue
		}
		n, ok := number(raw)
		if !ok {
			return req, typeError(field+"."+size.key, "number", raw)
		}
		if n < 0 {
			return req, &ConfigError{Field: field + "." + size.key, Message: fmt.Sprintf("must not be negative, got %v", n)}
		}
		*size.dst = n
	}
	return req, nil
}

// number reads an integer, a float or a string-encoded number, as
// [FieldNumber] admits.
func number(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float64:
		return n, true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(n), 64)
		return f, err == nil
	}
	return 0, false
}

// parseRemoteCredentials reads environment.remote.credentials.
func parseRemoteCredentials(value any) ([]RemoteCredential, error) {
	const field = remoteField + ".credentials"
	if value == nil {
		return nil, nil
	}
	list, ok := value.([]any)
	if !ok {
		return nil, typeError(field, "list", value)
	}
	var creds []RemoteCredential
	seen := map[string]int{}
	for i, elem := range list {
		entryField := fmt.Sprintf("%s[%d]", field, i)
		entry, ok := elem.(map[string]any)
		if !ok {
			return nil, typeError(entryField, "mapping", elem)
		}
		name, err := requiredString(entry, "name", entryField+".name")
		if err != nil {
			return nil, err
		}
		if !credentialNamePattern.MatchString(name) {
			return nil, &ConfigError{Field: entryField + ".name", Message: fmt.Sprintf("invalid credential name %q; use letters, digits, '_' and '-'", name)}
		}
		if first, dup := seen[name]; dup {
			return nil, &ConfigError{Field: entryField + ".name", Message: fmt.Sprintf("credential name %q is already used by %s[%d]", name, field, first)}
		}
		seen[name] = i
		env, err := requiredString(entry, "env", entryField+".env")
		if err != nil {
			return nil, err
		}
		if !envVarNamePattern.MatchString(env) {
			return nil, &ConfigError{Field: entryField + ".env", Message: fmt.Sprintf("invalid environment variable name %q", env)}
		}
		hosts, err := parseHostList(entry, "hosts", entryField+".hosts", true)
		if err != nil {
			return nil, err
		}
		header, present, err := requireStringField(entry, "header", entryField+".header")
		if err != nil {
			return nil, err
		}
		if present && header == "" {
			return nil, &ConfigError{Field: entryField + ".header", Message: "must not be an empty string; remove the key for the default"}
		}
		for _, key := range []string{"file", "command"} {
			if _, set := entry[key]; set {
				return nil, &ConfigError{Field: entryField + "." + key, Message: "is not accepted under the remote kind, where Sortie reads no credential value: the worker's egress proxy holds it"}
			}
		}
		if err := refuseUnknownKeys(entry, entryField, []string{"name", "env", "hosts", "header"}); err != nil {
			return nil, err
		}
		creds = append(creds, RemoteCredential{Name: name, Env: env, Hosts: hosts, Header: header})
	}
	return creds, nil
}

// parseRemoteProfiles reads environment.remote.profiles: a mapping of
// profile names, each to its optional image and its required requires.
func parseRemoteProfiles(value any) (map[string]JobProfile, error) {
	const field = remoteField + ".profiles"
	if value == nil {
		return nil, nil
	}
	m, ok := value.(map[string]any)
	if !ok {
		return nil, typeError(field, "mapping", value)
	}
	out := make(map[string]JobProfile, len(m))
	for _, name := range slices.Sorted(maps.Keys(m)) {
		entryField := field + "." + name
		if !repositoryNamePattern.MatchString(name) {
			return nil, &ConfigError{Field: entryField, Message: "a profile name must match [a-z0-9]+(-[a-z0-9]+)*"}
		}
		entry, ok := m[name].(map[string]any)
		if !ok {
			return nil, typeError(entryField, "mapping", m[name])
		}
		if err := refuseUnknownKeys(entry, entryField, []string{"image", "requires"}); err != nil {
			return nil, err
		}
		image, present, err := requireStringField(entry, "image", entryField+".image")
		if err != nil {
			return nil, err
		}
		if present && image == "" {
			return nil, &ConfigError{Field: entryField + ".image", Message: "must not be an empty string; remove the key to keep the repository's image"}
		}
		raw, set := entry["requires"]
		if !set || raw == nil {
			return nil, &ConfigError{Field: entryField + ".requires", Message: "is required: a profile names what a worker must declare to take its job"}
		}
		req, err := parseRemoteRequirements(raw, entryField+".requires")
		if err != nil {
			return nil, err
		}
		out[name] = JobProfile{Image: image, Requires: req}
	}
	return out, nil
}

// parseRemoteEgress reads environment.remote.egress, the harness's
// egress policy, in the format of a repository's egress file. Absent, it
// is the empty policy.
func parseRemoteEgress(value any) (egress.Policy, error) {
	const field = remoteField + ".egress"
	if value == nil {
		return egress.Empty(), nil
	}
	p, err := egress.FromValue(value, field)
	if err != nil {
		if perr, ok := errors.AsType[*egress.Error](err); ok {
			return egress.Policy{}, &ConfigError{Field: perr.Entry, Message: perr.Message}
		}
		return egress.Policy{}, &ConfigError{Field: field, Message: err.Error()}
	}
	return p, nil
}

// parseRemoteFlows reads environment.remote.flows: a URL for each of
// [RemoteFlows], and nothing else.
func parseRemoteFlows(value any) (map[string]string, error) {
	const field = remoteField + ".flows"
	var m map[string]any
	if value != nil {
		var ok bool
		if m, ok = value.(map[string]any); !ok {
			return nil, typeError(field, "mapping", value)
		}
	}
	for key := range m {
		if !slices.Contains(RemoteFlows, key) {
			return nil, &ConfigError{Field: field + "." + key, Message: "unknown flow; valid: " + strings.Join(RemoteFlows, ", ")}
		}
	}
	out := make(map[string]string, len(RemoteFlows))
	for _, flow := range RemoteFlows {
		flowField := field + "." + flow
		raw, exists := m[flow]
		if !exists || raw == nil {
			return nil, &ConfigError{Field: flowField + ".url", Message: "is required: each data-plane flow needs the URL a worker reaches it at"}
		}
		entry, ok := raw.(map[string]any)
		if !ok {
			return nil, typeError(flowField, "mapping", raw)
		}
		if err := refuseUnknownKeys(entry, flowField, []string{"url"}); err != nil {
			return nil, err
		}
		u, err := requiredString(entry, "url", flowField+".url")
		if err != nil {
			return nil, err
		}
		parsed, err := url.Parse(u)
		if err != nil || parsed.Host == "" || !slices.Contains([]string{"http", "https", "ws", "wss"}, parsed.Scheme) || parsed.User != nil {
			return nil, &ConfigError{Field: flowField + ".url", Message: fmt.Sprintf("must be an absolute http, https, ws or wss URL without credentials, got %q", u)}
		}
		out[flow] = u
	}
	return out, nil
}
