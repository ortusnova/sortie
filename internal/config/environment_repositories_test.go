package config

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

const (
	remoteDefaultRepoField  = remoteField + ".default_repository"
	remoteRepositoriesField = remoteField + ".repositories"
	remoteSingleRepoField   = remoteField + ".repository"
)

func repoEntry(name, url string) map[string]any {
	e := map[string]any{"url": url, "base": "main"}
	if name != "" {
		e["name"] = name
	}
	return e
}

// The workflow lists the repositories Sortie may open.
func TestEnvironmentConfig_Repositories(t *testing.T) {
	t.Parallel()

	raw := remoteRaw(map[string]any{
		"repositories": []any{
			repoEntry("sortie", "https://example.com/o/sortie.git"),
			map[string]any{"name": "ao", "url": "https://example.com/o/ao.git", "base": "trunk", "provider": "github", "project": "o/ao"},
		},
		"default_repository": "ao",
	})
	cfg, err := NewServiceConfig(raw)
	if err != nil {
		t.Fatalf("NewServiceConfig() error = %v", err)
	}
	s := cfg.Environment.RepositorySet()
	if s.DefaultRepository != "ao" || s.Repository == nil || s.Repository.Name != "ao" || s.Repository.Base != "trunk" {
		t.Errorf("default = %q, Repository = %+v, want ao", s.DefaultRepository, s.Repository)
	}
	if got := s.RepositoryNames(); !reflect.DeepEqual(got, []string{"sortie", "ao"}) {
		t.Errorf("RepositoryNames() = %v", got)
	}
	r, ok := s.RepositoryNamed("sortie")
	if !ok || r.BranchPrefix != defaultRepoBranchPrefix || r.MaxBundleMiB != defaultRepoMaxBundleMiB {
		t.Errorf("RepositoryNamed(sortie) = %+v, %v, want defaults applied", r, ok)
	}
	if _, ok := s.RepositoryNamed("other"); ok {
		t.Error("RepositoryNamed(other) found a repository that is not listed")
	}
}

// The default repository is optional: a repositories list without one,
// of any length, names no default, and an issue that names no
// repository is not dispatched.
func TestEnvironmentConfig_DefaultRepositoryIsOptional(t *testing.T) {
	t.Parallel()

	for _, list := range [][]any{
		{repoEntry("only", "https://example.com/o/only.git")},
		{repoEntry("a", "https://example.com/a.git"), repoEntry("b", "https://example.com/b.git")},
	} {
		cfg, err := NewServiceConfig(remoteRaw(map[string]any{"repositories": list}))
		if err != nil {
			t.Fatalf("NewServiceConfig(%d entries) error = %v", len(list), err)
		}
		s := cfg.Environment.RepositorySet()
		if s.DefaultRepository != "" || s.Repository != nil || len(s.Repositories) != len(list) {
			t.Errorf("%d entries: default %q, Repository %+v, Repositories %d; want no default", len(list), s.DefaultRepository, s.Repository, len(s.Repositories))
		}
		if _, err := s.DispatchRepository(nil); !errors.Is(err, ErrNotDispatched) {
			t.Errorf("%d entries: DispatchRepository(no label) error = %v, want ErrNotDispatched", len(list), err)
		}
		if r, err := s.DispatchRepository([]string{RepositoryLabelPrefix + s.Repositories[0].Name}); err != nil || r.Name != s.Repositories[0].Name {
			t.Errorf("%d entries: DispatchRepository(labelled) = %+v, %v", len(list), r, err)
		}
	}
}

// A listed repository may be closed to dispatch: it stays listed, so
// plan sync accepts its name, and its issues are not dispatched.
func TestEnvironmentConfig_RepositoryClosedToDispatch(t *testing.T) {
	t.Parallel()

	homelab := repoEntry("homelab", "https://example.com/o/homelab.git")
	homelab["dispatch"] = false
	open := repoEntry("sortie", "https://example.com/o/sortie.git")
	open["dispatch"] = true
	cfg, err := NewServiceConfig(remoteRaw(map[string]any{
		"repositories":       []any{open, homelab},
		"default_repository": "sortie",
	}))
	if err != nil {
		t.Fatalf("NewServiceConfig() error = %v", err)
	}
	s := cfg.Environment.RepositorySet()
	if got := s.RepositoryNames(); !reflect.DeepEqual(got, []string{"sortie", "homelab"}) {
		t.Errorf("RepositoryNames() = %v, want both listed", got)
	}
	r, _ := s.RepositoryNamed("homelab")
	if !r.ClosedToDispatch || s.Repository.ClosedToDispatch {
		t.Errorf("homelab closed %v, sortie closed %v; want only homelab closed", r.ClosedToDispatch, s.Repository.ClosedToDispatch)
	}
	if _, err := s.DispatchRepository([]string{RepositoryLabelPrefix + "homelab"}); !errors.Is(err, ErrNotDispatched) || !strings.Contains(err.Error(), "closed to dispatch") {
		t.Errorf("DispatchRepository(homelab) error = %v, want closed to dispatch", err)
	}
	if r, err := s.DispatchRepository(nil); err != nil || r.Name != "sortie" {
		t.Errorf("DispatchRepository(no label) = %+v, %v; want the default", r, err)
	}
	if _, err := s.DispatchRepository([]string{RepositoryLabelPrefix + "elsewhere"}); !errors.Is(err, ErrNotDispatched) {
		t.Errorf("DispatchRepository(elsewhere) error = %v, want ErrNotDispatched", err)
	}
}

func TestEnvironmentConfig_SingleRepositoryUnchanged(t *testing.T) {
	t.Parallel()

	cfg, err := NewServiceConfig(remoteRaw(map[string]any{
		"repository": repoEntry("", "https://example.com/o/app.git"),
	}))
	if err != nil {
		t.Fatalf("NewServiceConfig() error = %v", err)
	}
	s := cfg.Environment.RepositorySet()
	if s.Repository == nil || s.Repository.URL != "https://example.com/o/app.git" || s.DefaultRepository != "" {
		t.Fatalf("Repository = %+v, default %q", s.Repository, s.DefaultRepository)
	}
	if len(s.Repositories) != 1 || len(s.RepositoryNames()) != 0 {
		t.Errorf("Repositories = %+v, names %v; want the one unnamed block", s.Repositories, s.RepositoryNames())
	}
	if r, ok := s.RepositoryNamed(""); !ok || r.URL != s.Repository.URL {
		t.Errorf("RepositoryNamed(\"\") = %+v, %v", r, ok)
	}
}

func TestEnvironmentConfig_RepositoriesErrors(t *testing.T) {
	t.Parallel()

	a := repoEntry("a", "https://example.com/a.git")
	b := repoEntry("b", "https://example.com/b.git")
	closed := repoEntry("c", "https://example.com/c.git")
	closed["dispatch"] = false
	tests := []struct {
		name  string
		extra map[string]any
		field string
		msg   string
	}{
		{"both forms", map[string]any{"repository": repoEntry("", "u"), "repositories": []any{a}}, remoteRepositoriesField, "together"},
		{"default with single", map[string]any{"repository": repoEntry("", "u"), "default_repository": "a"}, remoteDefaultRepoField, "single"},
		{"default without list", map[string]any{"default_repository": "a"}, remoteDefaultRepoField, "lists none"},
		{"not a list", map[string]any{"repositories": "a"}, remoteRepositoriesField, "sequence"},
		{"empty list", map[string]any{"repositories": []any{}}, remoteRepositoriesField, "at least one"},
		{"entry not a map", map[string]any{"repositories": []any{"a"}}, remoteRepositoriesField + "[0]", "mapping"},
		{"missing name", map[string]any{"repositories": []any{repoEntry("", "u")}}, remoteRepositoriesField + "[0].name", "required"},
		{"bad name", map[string]any{"repositories": []any{repoEntry("Bad_Name", "u")}}, remoteRepositoriesField + "[0].name", "must match"},
		{"duplicate name", map[string]any{"repositories": []any{a, repoEntry("a", "v")}}, remoteRepositoriesField + "[1].name", "more than once"},
		{"missing url", map[string]any{"repositories": []any{map[string]any{"name": "a", "base": "main"}}}, remoteRepositoriesField + "[0].url", "required"},
		{"unlisted default", map[string]any{"repositories": []any{a, b}, "default_repository": "c"}, remoteDefaultRepoField, "does not list"},
		{"closed default", map[string]any{"repositories": []any{a, closed}, "default_repository": "c"}, remoteDefaultRepoField, "closed to dispatch"},
		{"dispatch not a boolean", map[string]any{"repositories": []any{map[string]any{"name": "a", "url": "u", "base": "main", "dispatch": "no"}}}, remoteRepositoriesField + "[0].dispatch", "boolean"},
		{"dispatch on the single block", map[string]any{"repository": map[string]any{"url": "u", "base": "main", "dispatch": false}}, remoteSingleRepoField + ".dispatch", "single"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewServiceConfig(remoteRaw(tt.extra))
			var ce *ConfigError
			if !errors.As(err, &ce) {
				t.Fatalf("error = %v, want a *ConfigError", err)
			}
			if ce.Field != tt.field || !strings.Contains(ce.Message, tt.msg) {
				t.Errorf("error = %s: %s, want field %s containing %q", ce.Field, ce.Message, tt.field, tt.msg)
			}
		})
	}
}

func TestValidateFrontMatter_RepositoriesKnown(t *testing.T) {
	t.Parallel()

	closed := repoEntry("b", "v")
	closed["dispatch"] = false
	raw := remoteRaw(map[string]any{
		"repositories":       []any{repoEntry("a", "u"), closed},
		"default_repository": "a",
	})
	for _, w := range ValidateFrontMatter(raw, ServiceConfig{}) {
		if strings.Contains(w.Field, "repositor") {
			t.Errorf("unexpected warning %+v", w)
		}
	}
}
