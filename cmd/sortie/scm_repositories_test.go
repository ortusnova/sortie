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
	repos := make([]config.SmolvmRepository, 0, len(providers))
	for i, p := range providers {
		repos = append(repos, config.SmolvmRepository{
			Name: string(rune('a' + i)), URL: "https://example.com/org/" + string(rune('a'+i)) + ".git", Base: "main",
			BranchPrefix: "sortie/", Provider: p, Project: "org/x", MaxBundleMiB: 512,
		})
	}
	cfg.Environment.Smolvm = config.SmolvmConfig{Repositories: repos, DefaultRepository: "a", Repository: &repos[0]}
	return cfg
}

func TestCheckRepositoryProvider_Repositories(t *testing.T) {
	t.Parallel()

	if err := checkRepositoryProvider(repositoriesConfig("", "github", "github")); err != nil {
		t.Errorf("same providers: error = %v, want nil", err)
	}
	err := checkRepositoryProvider(repositoriesConfig("github", "gitea"))
	ce, ok := errors.AsType[*config.ConfigError](err)
	if !ok || ce.Field != "environment.smolvm.repositories[1].provider" || !strings.Contains(ce.Message, "environment.smolvm.repositories[0].provider") {
		t.Errorf("mixed providers: error = %v, want one naming both entries", err)
	}
	err = checkRepositoryProvider(repositoriesConfig("", "nosuchforge"))
	if ce, ok := errors.AsType[*config.ConfigError](err); !ok || ce.Field != "environment.smolvm.repositories[1].provider" {
		t.Errorf("unknown provider: error = %v", err)
	}
}

// A repositories list without a default still names each entry's
// provider by its index.
func TestCheckRepositoryProvider_RepositoriesWithoutDefault(t *testing.T) {
	t.Parallel()

	cfg := repositoriesConfig("github", "gitea")
	cfg.Environment.Smolvm.DefaultRepository, cfg.Environment.Smolvm.Repository = "", nil
	err := checkRepositoryProvider(cfg)
	if ce, ok := errors.AsType[*config.ConfigError](err); !ok || ce.Field != "environment.smolvm.repositories[1].provider" {
		t.Errorf("mixed providers: error = %v, want the second entry named", err)
	}
}

// Plan sync sees every listed repository, a closed one included, and
// requires each story to name one only when the list names no default.
func TestWorkflowRepositoriesForPlanSync(t *testing.T) {
	t.Parallel()

	withDefault := repositoriesConfig("", "")
	withDefault.Environment.Kind = config.EnvironmentKindSmolvm
	withDefault.Environment.Smolvm.Repositories[1].ClosedToDispatch = true
	noDefault := withDefault
	noDefault.Environment.Smolvm.DefaultRepository, noDefault.Environment.Smolvm.Repository = "", nil
	local := config.ServiceConfig{}
	remote := config.ServiceConfig{Environment: config.EnvironmentConfig{Kind: config.EnvironmentKindRemote, Remote: config.RemoteConfig{
		Repositories: withDefault.Environment.Smolvm.Repositories, DefaultRepository: "a", Repository: withDefault.Environment.Smolvm.Repository,
	}}}
	remoteNoDefault := remote
	remoteNoDefault.Environment.Remote.DefaultRepository, remoteNoDefault.Environment.Remote.Repository = "", nil

	for _, tt := range []struct {
		name     string
		cfg      config.ServiceConfig
		names    []string
		requires bool
	}{
		{"default", withDefault, []string{"a", "b"}, false},
		{"no default", noDefault, []string{"a", "b"}, true},
		{"no repositories", local, []string{}, false},
		{"remote default", remote, []string{"a", "b"}, false},
		{"remote no default", remoteNoDefault, []string{"a", "b"}, true},
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
