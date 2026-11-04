package main

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
)

func repositoriesConfig(providers ...string) config.ServiceConfig {
	cfg := repositoryConfig("")
	repos := make([]config.Repository, 0, len(providers))
	for i, p := range providers {
		repos = append(repos, config.Repository{
			Name: string(rune('a' + i)), URL: "https://example.com/org/" + string(rune('a'+i)) + ".git", Base: "main",
			BranchPrefix: "sortie/", Provider: p, Project: "org/x", MaxBundleMiB: 512,
		})
	}
	cfg.Environment.Remote.Repositories, cfg.Environment.Remote.DefaultRepository, cfg.Environment.Remote.Repository = repos, "a", &repos[0]
	return cfg
}

func TestCheckRepositoryProvider_Repositories(t *testing.T) {
	t.Parallel()

	if err := checkRepositoryProvider(repositoriesConfig("", "github", "github")); err != nil {
		t.Errorf("same providers: error = %v, want nil", err)
	}
	err := checkRepositoryProvider(repositoriesConfig("github", "gitea"))
	ce, ok := errors.AsType[*config.ConfigError](err)
	if !ok || ce.Field != "environment.remote.repositories[1].provider" || !strings.Contains(ce.Message, "environment.remote.repositories[0].provider") {
		t.Errorf("mixed providers: error = %v, want one naming both entries", err)
	}
	err = checkRepositoryProvider(repositoriesConfig("", "nosuchforge"))
	if ce, ok := errors.AsType[*config.ConfigError](err); !ok || ce.Field != "environment.remote.repositories[1].provider" {
		t.Errorf("unknown provider: error = %v", err)
	}
}

// A repositories list without a default still names each entry's
// provider by its index.
func TestCheckRepositoryProvider_RepositoriesWithoutDefault(t *testing.T) {
	t.Parallel()

	cfg := repositoriesConfig("github", "gitea")
	cfg.Environment.Remote.DefaultRepository, cfg.Environment.Remote.Repository = "", nil
	err := checkRepositoryProvider(cfg)
	if ce, ok := errors.AsType[*config.ConfigError](err); !ok || ce.Field != "environment.remote.repositories[1].provider" {
		t.Errorf("mixed providers: error = %v, want the second entry named", err)
	}
}

// Plan sync sees every listed repository, a closed one included, and
// requires each story to name one only when the list names no default.
func TestWorkflowRepositoriesForPlanSync(t *testing.T) {
	t.Parallel()

	withDefault := repositoriesConfig("", "")
	withDefault.Environment.Remote.Repositories[1].ClosedToDispatch = true
	noDefault := withDefault
	noDefault.Environment.Remote.DefaultRepository, noDefault.Environment.Remote.Repository = "", nil
	local := config.ServiceConfig{}

	for _, tt := range []struct {
		name     string
		cfg      config.ServiceConfig
		names    []string
		requires bool
	}{
		{"default", withDefault, []string{"a", "b"}, false},
		{"no default", noDefault, []string{"a", "b"}, true},
		{"no repositories", local, []string{}, false},
	} {
		current := func() config.ServiceConfig { return tt.cfg }
		if got := workflowRepositories(current)(); !slices.Equal(got, tt.names) {
			t.Errorf("%s: repositories = %v, want %v", tt.name, got, tt.names)
		}
		if got := workflowRequiresRepository(current)(); got != tt.requires {
			t.Errorf("%s: requires repository = %v, want %v", tt.name, got, tt.requires)
		}
	}
	if workflowRequiresRepository(nil) != nil {
		t.Error("workflowRequiresRepository(nil) is not nil")
	}
}
