package forge

import (
	"net/url"
	"slices"
	"strings"
)

// Grant is a job's GitHub access grouped by owner: GitHub scopes every
// token to one owner, so a job gets one token per owner of its grant. It
// holds no token. Its list is never nil.
type Grant struct {
	Owners []OwnerGrant `json:"owners"`
}

// OwnerGrant is the access a job is granted on the repositories of one
// owner. Permissions is the highest level of each permission over the
// owner's repositories: what the owner's one token carries.
type OwnerGrant struct {
	Owner        string            `json:"owner"`
	Repositories []RepositoryGrant `json:"repositories"`
	Permissions  map[string]string `json:"permissions"`
}

// RepositoryGrant is one repository of a grant, as owner/name, and the
// permissions its entry declares.
type RepositoryGrant struct {
	Repository  string            `json:"repository"`
	Permissions map[string]string `json:"permissions"`
}

// Grant groups d's access by owner, owners and their repositories sorted
// by name, compared without case as GitHub compares them.
func (d Declaration) Grant() Grant {
	g := Grant{Owners: []OwnerGrant{}}
	byOwner := map[string]int{}
	for _, a := range d.Access {
		key := strings.ToLower(a.Owner())
		i, ok := byOwner[key]
		if !ok {
			i = len(g.Owners)
			byOwner[key] = i
			g.Owners = append(g.Owners, OwnerGrant{Owner: a.Owner(), Repositories: []RepositoryGrant{}, Permissions: map[string]string{}})
		}
		o := &g.Owners[i]
		o.Repositories = append(o.Repositories, RepositoryGrant{Repository: a.Repository, Permissions: cloneMap(a.Permissions)})
		for p, level := range a.Permissions {
			if rank(level) > rank(o.Permissions[p]) {
				o.Permissions[p] = level
			}
		}
	}
	for i := range g.Owners {
		slices.SortFunc(g.Owners[i].Repositories, func(a, b RepositoryGrant) int {
			return strings.Compare(strings.ToLower(a.Repository), strings.ToLower(b.Repository))
		})
	}
	slices.SortFunc(g.Owners, func(a, b OwnerGrant) int {
		return strings.Compare(strings.ToLower(a.Owner), strings.ToLower(b.Owner))
	})
	return g
}

// Clone returns a copy of g that shares no list or map with it.
func (g Grant) Clone() Grant {
	out := Grant{Owners: make([]OwnerGrant, 0, len(g.Owners))}
	for _, o := range g.Owners {
		c := OwnerGrant{Owner: o.Owner, Permissions: cloneMap(o.Permissions), Repositories: make([]RepositoryGrant, 0, len(o.Repositories))}
		for _, r := range o.Repositories {
			c.Repositories = append(c.Repositories, RepositoryGrant{Repository: r.Repository, Permissions: cloneMap(r.Permissions)})
		}
		out.Owners = append(out.Owners, c)
	}
	return out
}

// GitHubRepository returns the owner/name of a repository on GitHub: its
// project when its provider is github, else the path of a github.com
// URL, and "" when neither names one.
func GitHubRepository(provider, project, rawURL string) string {
	if provider == "github" && checkRepository(project) == "" {
		return project
	}
	host, p := "", ""
	if rest, ok := strings.CutPrefix(rawURL, "git@"); ok && !strings.Contains(rawURL, "://") {
		host, p, _ = strings.Cut(rest, ":")
	} else if u, err := url.Parse(rawURL); err == nil {
		host, p = u.Hostname(), u.Path
	}
	if !strings.EqualFold(host, "github.com") {
		return ""
	}
	repo := strings.TrimSuffix(strings.Trim(p, "/"), ".git")
	if checkRepository(repo) != "" {
		return ""
	}
	return repo
}
