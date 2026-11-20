package forge

import (
	"fmt"
	"slices"
	"strings"
)

// Limit is one repository a ceiling lets a repository's jobs be granted,
// with the highest level of each permission they may hold there.
type Limit struct {
	Repository  string            `json:"repository"`
	Permissions map[string]string `json:"permissions"`
}

// Ceiling caps what each repository may declare: by the name of each
// repository whose jobs Sortie runs, the repositories its jobs may be
// granted. A repository it does not name may declare nothing.
type Ceiling map[string][]Limit

// limitKeys are the keys of a ceiling entry.
var limitKeys = []string{"repository", "permissions"}

// CeilingFromValue reads a ceiling already decoded from YAML, such as the
// workflow's environment.remote.forge_ceiling, whose path is field. Nil
// reads as the empty ceiling.
func CeilingFromValue(value any, field string) (Ceiling, error) {
	c := Ceiling{}
	if value == nil {
		return c, nil
	}
	m, ok := value.(map[string]any)
	if !ok {
		return nil, &Error{Entry: field, Message: fmt.Sprintf("must be a mapping of repository name to a list of repository and permissions, got %s", kindOf(value))}
	}
	r := reader{field: field}
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		raw := m[name]
		if raw == nil {
			c[name] = []Limit{}
			continue
		}
		list, err := r.list(raw, name)
		if err != nil {
			return nil, err
		}
		limits := []Limit{}
		seen := map[string]int{}
		for i, raw := range list {
			entry := fmt.Sprintf("%s[%d]", name, i)
			em, err := r.mapping(raw, entry, limitKeys)
			if err != nil {
				return nil, err
			}
			repo, err := r.repository(em, entry)
			if err != nil {
				return nil, err
			}
			perms, err := r.permissions(em, entry)
			if err != nil {
				return nil, err
			}
			key := strings.ToLower(repo)
			if first, dup := seen[key]; dup {
				return nil, r.errf(entry+".repository", "%q is already listed at %s[%d]", repo, name, first)
			}
			seen[key] = i
			limits = append(limits, Limit{Repository: repo, Permissions: perms})
		}
		c[name] = limits
	}
	return c, nil
}

func (c Ceiling) limit(name, repo string) (Limit, bool) {
	for _, l := range c[name] {
		if strings.EqualFold(l.Repository, repo) {
			return l, true
		}
	}
	return Limit{}, false
}

// Check returns nil when every entry of d, the declaration of the
// repository named name, is within that repository's ceiling, and
// otherwise an [*Error] that names the first entry above it. field is
// the ceiling's path in the workflow, named in the message.
func (c Ceiling) Check(name string, d Declaration, field string) error {
	at := field + "." + name
	for i, a := range d.Access {
		entry := fmt.Sprintf("access[%d]", i)
		if _, has := c[name]; !has {
			return &Error{Entry: entry + ".repository", Message: fmt.Sprintf("%s is outside the ceiling: %s names no ceiling for %s", a.Repository, field, name)}
		}
		limit, ok := c.limit(name, a.Repository)
		if !ok {
			return &Error{Entry: entry + ".repository", Message: fmt.Sprintf("%s is outside the ceiling: %s does not list it", a.Repository, at)}
		}
		perms := make([]string, 0, len(a.Permissions))
		for p := range a.Permissions {
			perms = append(perms, p)
		}
		slices.Sort(perms)
		for _, p := range perms {
			level := a.Permissions[p]
			max, has := limit.Permissions[p]
			switch {
			case !has:
				return &Error{Entry: entry + ".permissions." + p, Message: fmt.Sprintf("%s %s on %s is outside the ceiling: %s grants no %s there", p, level, a.Repository, at, p)}
			case rank(level) > rank(max):
				return &Error{Entry: entry + ".permissions." + p, Message: fmt.Sprintf("%s %s on %s is above the ceiling: %s grants at most %s %s there", p, level, a.Repository, at, p, max)}
			}
		}
	}
	return nil
}
