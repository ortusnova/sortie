package config

import (
	"errors"
	"fmt"
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

	// EnvironmentKindRemote hands each run to the worker matcher, to run
	// on a worker that dials Sortie. Nothing of the issue runs on
	// Sortie's host.
	EnvironmentKindRemote = "remote"
)

// retiredEnvironmentKindSmolvm is the removed kind that ran each issue
// workspace in a microVM on Sortie's own host. Validation refuses it
// and names the remote kind, which runs the issue on a worker instead.
const retiredEnvironmentKindSmolvm = "smolvm"

// Defaults applied to a repository entry.
const (
	defaultRepoBranchPrefix = "sortie/"
	defaultRepoMaxBundleMiB = 512
)

// Field paths of the environment block, and the host name length limit.
const (
	environmentField          = "environment"
	environmentHostPrefixPath = environmentField + ".host_prefix"
	maxHostNameLength         = 253
)

var (
	envVarNamePattern        = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	repositoryNamePattern    = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	credentialNamePattern    = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	hostLabelPattern         = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$`)
	environmentKindsAccepted = []string{EnvironmentKindLocal, EnvironmentKindRemote}
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

	// Remote holds the remote settings. It is parsed and validated only
	// when Kind is [EnvironmentKindRemote]; under any other kind it is
	// the zero value.
	Remote RemoteConfig
}

// RepositorySet holds the repositories an environment block configures.
type RepositorySet struct {
	// Repository configures the host-owned repository a workspace is
	// seeded from and exported to by default: the single repository
	// block, or the default_repository entry of the repositories list.
	// Nil when neither is set, and for a repositories list that names
	// no default.
	Repository *Repository

	// Repositories lists every repository Sortie may open, in written
	// order: the repositories list, or the single repository block as
	// its one entry. Nil when neither is set.
	Repositories []Repository

	// DefaultRepository names the entry of Repositories an issue that
	// names no repository uses. Empty for the single repository block,
	// and for a repositories list that names no default, under which an
	// issue that names no repository is not dispatched.
	DefaultRepository string
}

// Repository configures the host-owned repository mirror.
type Repository struct {
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

	// MaxBundleMiB caps the size of a bundle a worker returns.
	// Positive; 512 by default.
	MaxBundleMiB int

	// ClosedToDispatch keeps every issue that runs in the repository
	// from being dispatched: Sortie still lists it, for plan sync, and
	// leaves its issues to another Sortie. Set by dispatch: false on a
	// repositories entry; never set for the single repository block.
	ClosedToDispatch bool

	// Image is the machine image a remote job of the repository runs
	// in. Empty takes environment.remote.image.
	Image string

	// Requires is what a worker must declare to take a remote job of
	// the repository, in place of environment.remote.requires. Nil takes
	// that.
	Requires *RemoteRequirements
}

// buildEnvironmentConfig parses the top-level environment block. The
// worker extension section is consulted for the rule that the remote
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
		if kind == retiredEnvironmentKindSmolvm {
			return EnvironmentConfig{}, &ConfigError{
				Field:   environmentField + ".kind",
				Message: fmt.Sprintf("environment kind %q was removed: Sortie no longer runs issues in microVMs on its own host; set environment.kind %s to run each issue on a worker", kind, EnvironmentKindRemote),
			}
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

	if cfg.Kind != EnvironmentKindRemote {
		return cfg, nil
	}
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

// RepositoryNamed returns the configured repository called name, and
// whether there is one. The single repository block answers its own
// name, which may be empty.
func (c RepositorySet) RepositoryNamed(name string) (*Repository, bool) {
	for _, r := range c.AllRepositories() {
		if r.Name == name {
			return &r, true
		}
	}
	return nil, false
}

// AllRepositories returns Repositories, or the one Repository when only
// that is set.
func (c RepositorySet) AllRepositories() []Repository {
	if len(c.Repositories) == 0 && c.Repository != nil {
		return []Repository{*c.Repository}
	}
	return c.Repositories
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
func (c RepositorySet) DispatchRepository(labels []string) (*Repository, error) {
	name := IssueRepositoryName(labels)
	if name == "" {
		if c.Repository == nil {
			return nil, fmt.Errorf("the issue names no repository and the workflow sets no default_repository: %w", ErrNotDispatched)
		}
		repo := *c.Repository
		return &repo, nil
	}
	repo, found := c.RepositoryNamed(name)
	if !found {
		return nil, fmt.Errorf("the issue names repository %q, which the workflow's repositories do not list: %w", name, ErrNotDispatched)
	}
	if repo.ClosedToDispatch {
		return nil, fmt.Errorf("the issue names repository %q, which is closed to dispatch: %w", name, ErrNotDispatched)
	}
	return repo, nil
}

// RepositoryNames lists the names of the configured repositories in
// written order, leaving out an unnamed single repository block.
func (c RepositorySet) RepositoryNames() []string {
	var names []string
	for _, r := range c.AllRepositories() {
		if r.Name != "" {
			names = append(names, r.Name)
		}
	}
	return names
}

// parseRepositoryList reads the repositories list and default_repository
// of the environment block m at section into cfg, or makes single, the
// block's own single repository when it sets one, the only entry.
// parseEntry reads one entry of the list.
func parseRepositoryList(m map[string]any, section string, single *Repository, parseEntry func(map[string]any, string) (*Repository, error), cfg *RepositorySet) error {
	listField, defField := section+".repositories", section+".default_repository"
	rawList, listSet := m["repositories"]
	listSet = listSet && rawList != nil
	def, defSet, err := requireStringField(m, "default_repository", defField)
	if err != nil {
		return err
	}
	if single != nil {
		if listSet {
			return &ConfigError{Field: listField, Message: "cannot be set together with repository; list the one repository under repositories instead"}
		}
		if defSet {
			return &ConfigError{Field: defField, Message: "applies to repositories, not to the single repository block"}
		}
		cfg.Repository = single
		cfg.Repositories = []Repository{*single}
		return nil
	}
	if !listSet {
		if defSet {
			return &ConfigError{Field: defField, Message: "is set but repositories lists none"}
		}
		return nil
	}
	list, ok := rawList.([]any)
	if !ok {
		return typeError(listField, "sequence", rawList)
	}
	if len(list) == 0 {
		return &ConfigError{Field: listField, Message: "must list at least one repository"}
	}
	seen := map[string]bool{}
	repos := make([]Repository, 0, len(list))
	for i, elem := range list {
		field := fmt.Sprintf("%s[%d]", listField, i)
		entry, ok := elem.(map[string]any)
		if !ok {
			return typeError(field, "mapping", elem)
		}
		repo, err := parseEntry(entry, field)
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
		return &ConfigError{Field: defField, Message: fmt.Sprintf("names %q, which repositories does not list", def)}
	}
	cfg.DefaultRepository = def
	cfg.Repository, _ = cfg.RepositoryNamed(def)
	if cfg.Repository.ClosedToDispatch {
		return &ConfigError{Field: defField, Message: fmt.Sprintf("names %q, which is closed to dispatch; leave default_repository out to dispatch no issue that names no repository", def)}
	}
	return nil
}

// parseRepositoryEntry reads one repository mapping at field, applying
// its defaults.
func parseRepositoryEntry(entry map[string]any, field string) (*Repository, error) {
	name, _, err := requireStringField(entry, "name", field+".name")
	if err != nil {
		return nil, err
	}
	if name != "" && !repositoryNamePattern.MatchString(name) {
		return nil, &ConfigError{Field: field + ".name", Message: fmt.Sprintf("%q must match [a-z0-9]+(-[a-z0-9]+)*", name)}
	}

	url, err := requiredString(entry, "url", field+".url")
	if err != nil {
		return nil, err
	}
	base, err := requiredString(entry, "base", field+".base")
	if err != nil {
		return nil, err
	}

	prefix, present, err := requireStringField(entry, "branch_prefix", field+".branch_prefix")
	if err != nil {
		return nil, err
	}
	if !present {
		prefix = defaultRepoBranchPrefix
	}

	provider, _, err := requireStringField(entry, "provider", field+".provider")
	if err != nil {
		return nil, err
	}
	project, _, err := requireStringField(entry, "project", field+".project")
	if err != nil {
		return nil, err
	}
	if provider != "" && project == "" {
		return nil, &ConfigError{
			Field:   field + ".project",
			Message: fmt.Sprintf("is required when repository.provider is set (provider %q)", provider),
		}
	}

	maxBundle := defaultRepoMaxBundleMiB
	if v, set := entry["max_bundle_mib"]; set && v != nil {
		n, err := coerceIntField(entry, "max_bundle_mib", field+".max_bundle_mib")
		if err != nil {
			return nil, err
		}
		if n <= 0 {
			return nil, &ConfigError{
				Field:   field + ".max_bundle_mib",
				Message: fmt.Sprintf("must be a positive integer, got %d", n),
			}
		}
		maxBundle = n
	}

	return &Repository{
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
