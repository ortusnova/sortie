// Package forge reads and checks a job's GitHub access: the repositories
// a job may reach on GitHub and the GitHub App permissions it may hold on
// each. A repository declares its jobs' access in [File] at its root; the
// workflow caps what each repository may declare with a [Ceiling]. The
// format is published as a JSON Schema in docs/forge.schema.json; [Parse]
// and [FromValue] check every rule of that schema and the rules a schema
// cannot state, such as duplicate repositories.
package forge

import (
	"bytes"
	"fmt"
	"maps"
	"math"
	"path"
	"regexp"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/sortie-ai/sortie/internal/workspacekit"
)

// File is the path of a repository's declaration, relative to the
// repository root.
var File = treePath()

// treePath is the path of the declaration in a Git tree, which always
// separates its elements with a slash.
func treePath() string {
	return path.Join(workspacekit.SortieDir, "forge.yaml")
}

// Version is the only format version.
const Version = 1

// MaxFileBytes bounds the declaration file; a larger one is invalid.
const MaxFileBytes = 64 << 10

// Read and Write are the levels of a permission. Write includes Read.
const (
	Read  = "read"
	Write = "write"
)

// permissionLevels maps each GitHub App repository permission a
// declaration may name to the levels it takes. workflows takes only
// write, as GitHub's own App permissions do.
var permissionLevels = map[string][]string{
	"actions":                      {Read, Write},
	"administration":               {Read, Write},
	"artifact_metadata":            {Read, Write},
	"attestations":                 {Read, Write},
	"checks":                       {Read, Write},
	"code_quality":                 {Read, Write},
	"codespaces":                   {Read, Write},
	"contents":                     {Read, Write},
	"dependabot_secrets":           {Read, Write},
	"deployments":                  {Read, Write},
	"discussions":                  {Read, Write},
	"environments":                 {Read, Write},
	"issues":                       {Read, Write},
	"merge_queues":                 {Read, Write},
	"metadata":                     {Read, Write},
	"packages":                     {Read, Write},
	"pages":                        {Read, Write},
	"pull_requests":                {Read, Write},
	"repository_custom_properties": {Read, Write},
	"repository_hooks":             {Read, Write},
	"repository_projects":          {Read, Write},
	"secret_scanning_alerts":       {Read, Write},
	"secrets":                      {Read, Write},
	"security_events":              {Read, Write},
	"single_file":                  {Read, Write},
	"statuses":                     {Read, Write},
	"vulnerability_alerts":         {Read, Write},
	"workflows":                    {Write},
}

// Permissions returns the names of the permissions a declaration may
// name, sorted.
func Permissions() []string {
	names := make([]string, 0, len(permissionLevels))
	for name := range permissionLevels {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// rank orders the levels: a higher rank includes a lower.
func rank(level string) int {
	switch level {
	case Read:
		return 1
	case Write:
		return 2
	}
	return 0
}

// Access is one repository a job may reach and its permissions there.
type Access struct {
	// Repository is owner/name on GitHub.
	Repository  string            `json:"repository"`
	Permissions map[string]string `json:"permissions"`
	Reason      string            `json:"reason"`
}

// Owner returns the owner part of the repository.
func (a Access) Owner() string {
	owner, _, _ := strings.Cut(a.Repository, "/")
	return owner
}

// Declaration is a repository's GitHub access for its jobs. Its list is
// never nil once read.
type Declaration struct {
	Access []Access `json:"access"`
}

// Empty returns the declaration that grants nothing.
func Empty() Declaration {
	return Declaration{Access: []Access{}}
}

// DefaultReason is the reason of the access a repository with no file
// gets.
const DefaultReason = "the repository declares no .sortie/forge.yaml: its jobs read the repository itself"

// Default is the declaration of a repository that has no file: contents
// and metadata read on the repository itself, self as owner/name, and
// nothing else. A self that is not owner/name grants nothing.
func Default(self string) Declaration {
	if checkRepository(self) != "" {
		return Empty()
	}
	return Declaration{Access: []Access{{
		Repository:  self,
		Permissions: map[string]string{"contents": Read, "metadata": Read},
		Reason:      DefaultReason,
	}}}
}

// Clone returns a copy of d that shares no list or map with it.
func (d Declaration) Clone() Declaration {
	out := Empty()
	for _, a := range d.Access {
		a.Permissions = cloneMap(a.Permissions)
		out.Access = append(out.Access, a)
	}
	return out
}

func cloneMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	maps.Copy(out, m)
	return out
}

// Error is an invalid declaration or ceiling: Entry names the key or the
// entry, such as "access[1].permissions.contents", and Message why it is
// invalid.
type Error struct {
	Entry   string
	Message string
}

func (e *Error) Error() string {
	if e.Entry == "" {
		return e.Message
	}
	return e.Entry + ": " + e.Message
}

// topKeys are the keys of a declaration file.
var topKeys = []string{"version", "access"}

// accessKeys are the keys of a declaration entry.
var accessKeys = []string{"repository", "permissions", "reason"}

// Parse reads the declaration file data. An invalid file returns an
// [*Error] that names the entry and the reason.
func Parse(data []byte) (Declaration, error) {
	if len(data) > MaxFileBytes {
		return Declaration{}, &Error{Message: fmt.Sprintf("the file is %d bytes, over the limit of %d", len(data), MaxFileBytes)}
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return Declaration{}, &Error{Message: "the file is empty; write version: 1 and an access list"}
	}
	var doc any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return Declaration{}, &Error{Message: "not valid YAML: " + strings.TrimPrefix(err.Error(), "yaml: ")}
	}
	return FromValue(doc, "")
}

// FromValue reads a declaration already decoded from YAML. field
// prefixes the entry each error names; empty names the entries from the
// root of the file.
func FromValue(value any, field string) (Declaration, error) {
	m, ok := value.(map[string]any)
	if !ok {
		return Declaration{}, &Error{Entry: field, Message: fmt.Sprintf("must be a mapping with version: 1 and an access list, got %s", kindOf(value))}
	}
	r := reader{field: field}
	if err := r.unknownKeys(m, "", topKeys); err != nil {
		return Declaration{}, err
	}
	if err := r.version(m); err != nil {
		return Declaration{}, err
	}
	raw, ok := m["access"]
	if !ok || raw == nil {
		return Declaration{}, r.errf("access", "is required: list each repository the jobs reach, or write access: []")
	}
	list, err := r.list(raw, "access")
	if err != nil {
		return Declaration{}, err
	}
	d := Empty()
	seen := map[string]int{}
	for i, raw := range list {
		entry := fmt.Sprintf("access[%d]", i)
		a, err := r.access(raw, entry)
		if err != nil {
			return Declaration{}, err
		}
		key := strings.ToLower(a.Repository)
		if first, dup := seen[key]; dup {
			return Declaration{}, r.errf(entry+".repository", "%q is already listed at access[%d]", a.Repository, first)
		}
		seen[key] = i
		d.Access = append(d.Access, a)
	}
	return d, nil
}

// reader names each entry under its field.
type reader struct{ field string }

func (r reader) path(entry string) string {
	switch {
	case r.field == "":
		return entry
	case entry == "":
		return r.field
	case strings.HasPrefix(entry, "["):
		return r.field + entry
	}
	return r.field + "." + entry
}

func (r reader) errf(entry, format string, args ...any) error {
	return &Error{Entry: r.path(entry), Message: fmt.Sprintf(format, args...)}
}

func (r reader) unknownKeys(m map[string]any, entry string, known []string) error {
	var unknown []string
	for key := range m {
		if !slices.Contains(known, key) {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	slices.Sort(unknown)
	key := unknown[0]
	if entry != "" {
		key = entry + "." + key
	}
	return r.errf(key, "unknown key; valid: %s", strings.Join(known, ", "))
}

func (r reader) version(m map[string]any) error {
	raw, ok := m["version"]
	if !ok || raw == nil {
		return r.errf("version", "is required: write version: %d", Version)
	}
	n, ok := integer(raw)
	if !ok || n != Version {
		return r.errf("version", "must be %d, got %v", Version, raw)
	}
	return nil
}

func (r reader) list(raw any, entry string) ([]any, error) {
	list, ok := raw.([]any)
	if !ok {
		return nil, r.errf(entry, "must be a list, got %s", kindOf(raw))
	}
	return list, nil
}

func (r reader) mapping(raw any, entry string, known []string) (map[string]any, error) {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, r.errf(entry, "must be a mapping with %s, got %s", strings.Join(known, ", "), kindOf(raw))
	}
	return m, r.unknownKeys(m, entry, known)
}

// repository reads the entry's owner/name.
func (r reader) repository(m map[string]any, entry string) (string, error) {
	field := entry + ".repository"
	raw, ok := m["repository"]
	if !ok || raw == nil {
		return "", r.errf(field, "is required: write owner/name")
	}
	s, ok := raw.(string)
	if !ok {
		return "", r.errf(field, "must be a string, got %s", kindOf(raw))
	}
	if msg := checkRepository(s); msg != "" {
		return "", r.errf(field, "%q %s", s, msg)
	}
	return s, nil
}

// permissions reads the entry's permissions: at least one, each a known
// repository permission at a level it takes.
func (r reader) permissions(m map[string]any, entry string) (map[string]string, error) {
	field := entry + ".permissions"
	raw, ok := m["permissions"]
	if !ok || raw == nil {
		return nil, r.errf(field, "is required: map each GitHub App repository permission to read or write")
	}
	pm, ok := raw.(map[string]any)
	if !ok {
		return nil, r.errf(field, "must be a mapping of permission to read or write, got %s", kindOf(raw))
	}
	if len(pm) == 0 {
		return nil, r.errf(field, "must name at least one permission")
	}
	names := make([]string, 0, len(pm))
	for name := range pm {
		names = append(names, name)
	}
	slices.Sort(names)
	out := make(map[string]string, len(pm))
	for _, name := range names {
		levels, known := permissionLevels[name]
		if !known {
			return nil, r.errf(field+"."+name, "unknown permission; valid: %s", strings.Join(Permissions(), ", "))
		}
		level, ok := pm[name].(string)
		if !ok || !slices.Contains(levels, level) {
			return nil, r.errf(field+"."+name, "must be %s, got %v", strings.Join(levels, " or "), pm[name])
		}
		out[name] = level
	}
	return out, nil
}

func (r reader) access(raw any, entry string) (Access, error) {
	m, err := r.mapping(raw, entry, accessKeys)
	if err != nil {
		return Access{}, err
	}
	repo, err := r.repository(m, entry)
	if err != nil {
		return Access{}, err
	}
	perms, err := r.permissions(m, entry)
	if err != nil {
		return Access{}, err
	}
	reason, err := r.reason(m, entry, repo)
	if err != nil {
		return Access{}, err
	}
	return Access{Repository: repo, Permissions: perms, Reason: reason}, nil
}

// reason reads the entry's reason, which must say something.
func (r reader) reason(m map[string]any, entry, repo string) (string, error) {
	raw, ok := m["reason"]
	if !ok || raw == nil {
		return "", r.errf(entry+".reason", "is required: say why the jobs reach %s", repo)
	}
	s, ok := raw.(string)
	if !ok {
		return "", r.errf(entry+".reason", "must be a string, got %s", kindOf(raw))
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return "", r.errf(entry+".reason", "must not be empty: say why the jobs reach %s", repo)
	}
	return s, nil
}

var (
	ownerPattern = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9-]{0,37}[A-Za-z0-9])?$`)
	namePattern  = regexp.MustCompile(`^[A-Za-z0-9._-]{1,100}$`)
)

// checkRepository returns why s is not a GitHub owner/name, or "".
func checkRepository(s string) string {
	owner, name, ok := strings.Cut(s, "/")
	switch {
	case s == "":
		return "is empty: write owner/name"
	case !ok:
		return "has no owner: write owner/name"
	case strings.Contains(name, "/"):
		return "has more than one /: write owner/name"
	case !ownerPattern.MatchString(owner):
		return fmt.Sprintf("has an invalid owner %q: letters, digits and inner hyphens, at most 39", owner)
	case !namePattern.MatchString(name) || name == "." || name == ".." || strings.HasSuffix(name, ".git"):
		return fmt.Sprintf("has an invalid name %q: letters, digits, '.', '_' and '-', without .git", name)
	}
	return ""
}

// integer reads a whole number as YAML decodes it.
func integer(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int64:
		return n, true
	case uint64:
		if n > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	case float64:
		if n == math.Trunc(n) && math.Abs(n) < 1<<53 {
			return int64(n), true
		}
	}
	return 0, false
}

func kindOf(v any) string {
	switch v.(type) {
	case nil:
		return "nothing"
	case string:
		return "a string"
	case bool:
		return "a boolean"
	case int, int64, uint64, float64:
		return "a number"
	case []any:
		return "a list"
	case map[string]any:
		return "a mapping"
	}
	return fmt.Sprintf("%T", v)
}
