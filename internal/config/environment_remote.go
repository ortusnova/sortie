package config

import (
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
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
}

// RemoteConfig holds the environment.remote settings and the worker
// identity header the remote kind's worker API reads.
type RemoteConfig struct {
	// IdentityHeader is worker.identity_header: the request header that
	// names the worker of a worker API request.
	IdentityHeader string

	// Requires is what a worker must declare to take a job.
	Requires RemoteRequirements

	// Backend is the isolation backend the worker starts the machine
	// with. A job also requires it as a capability.
	Backend string

	// Image is the machine image reference the worker starts.
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

	// Repository is the host-owned repository each job's workspace is
	// seeded from and whose issue branch comes back from the worker as
	// a bundle. Nil when the block is absent: Sortie then serves no
	// seed and pushes nothing.
	Repository *SmolvmRepository
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
	if cfg.Requires, err = parseRemoteRequirements(m["requires"]); err != nil {
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
	if cfg.Image, err = requiredString(m, "image", remoteField+".image"); err != nil {
		return RemoteConfig{}, err
	}
	if cfg.Credentials, err = parseRemoteCredentials(m["credentials"]); err != nil {
		return RemoteConfig{}, err
	}
	if cfg.Flows, err = parseRemoteFlows(m["flows"]); err != nil {
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
	if cfg.Repository, err = parseRemoteRepository(m["repository"]); err != nil {
		return RemoteConfig{}, err
	}
	return cfg, nil
}

// parseRemoteRepository reads environment.remote.repository with the
// fields and defaults of environment.smolvm.repository.
func parseRemoteRepository(raw any) (*SmolvmRepository, error) {
	const field = remoteField + ".repository"
	if raw == nil {
		return nil, nil
	}
	entry, ok := raw.(map[string]any)
	if !ok {
		return nil, typeError(field, "mapping", raw)
	}
	if _, set := entry["dispatch"]; set {
		return nil, &ConfigError{Field: field + ".dispatch", Message: "applies to environment.smolvm.repositories entries, not to the remote repository block"}
	}
	return parseRepositoryEntry(entry, field)
}

// RepositorySet returns the repository settings of the kind c selects,
// as a [SmolvmConfig] holding only its Repository, Repositories and
// DefaultRepository: the smolvm block's own, or the remote kind's single
// repository block as the only entry and the default. Every other kind
// configures none.
func (c EnvironmentConfig) RepositorySet() SmolvmConfig {
	switch c.Kind {
	case EnvironmentKindSmolvm:
		return SmolvmConfig{Repository: c.Smolvm.Repository, Repositories: c.Smolvm.Repositories, DefaultRepository: c.Smolvm.DefaultRepository}
	case EnvironmentKindRemote:
		if r := c.Remote.Repository; r != nil {
			return SmolvmConfig{Repository: r, Repositories: []SmolvmRepository{*r}}
		}
	}
	return SmolvmConfig{}
}

// SetRepository makes repo the repository of the kind c selects, the one
// an issue's run uses.
func (c *EnvironmentConfig) SetRepository(repo *SmolvmRepository) {
	switch c.Kind {
	case EnvironmentKindSmolvm:
		c.Smolvm.Repository = repo
	case EnvironmentKindRemote:
		c.Remote.Repository = repo
	}
}

// RepositoryField returns the setting path of the repository at index i
// of [EnvironmentConfig.RepositorySet], for an error about it.
func (c EnvironmentConfig) RepositoryField(i int) string {
	switch {
	case c.Kind == EnvironmentKindRemote:
		return remoteField + ".repository"
	case c.Smolvm.ListsRepositories():
		return fmt.Sprintf("%s[%d]", smolvmRepositoriesField, i)
	}
	return smolvmRepositoryField
}

// parseRemoteRequirements reads environment.remote.requires.
func parseRemoteRequirements(value any) (RemoteRequirements, error) {
	const field = remoteField + ".requires"
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
		creds = append(creds, RemoteCredential{Name: name, Env: env, Hosts: hosts, Header: header})
	}
	return creds, nil
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
