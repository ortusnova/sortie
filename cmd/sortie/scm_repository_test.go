package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/registry"
)

// repositoryProviderField is the setting naming the SCM provider of the
// single remote repository block.
const repositoryProviderField = "environment.remote.repository.provider"

// scmWithoutPullRequestsKind is an SCM adapter kind registered for the
// tests that declares no pull request capability.
const scmWithoutPullRequestsKind = "scm-without-pull-requests"

// scmWithoutPullRequests is an SCM adapter that cannot open pull
// requests. Its methods are never called.
type scmWithoutPullRequests struct{ domain.SCMAdapter }

func init() {
	registry.SCMAdapters.Register(scmWithoutPullRequestsKind, func(map[string]any) (domain.SCMAdapter, error) {
		return scmWithoutPullRequests{}, nil
	})
}

// repositoryConfig returns a configuration under the remote kind whose
// repository names provider.
func repositoryConfig(provider string) config.ServiceConfig {
	return config.ServiceConfig{
		Tracker: config.TrackerConfig{Kind: "file"},
		Environment: config.EnvironmentConfig{
			Kind: config.EnvironmentKindRemote,
			Remote: config.RemoteConfig{Repository: &config.Repository{
				URL: "https://example.com/org/app.git", Base: "main", BranchPrefix: "sortie/", Provider: provider, Project: "org/app", MaxBundleMiB: 512,
			}},
		},
	}
}

func TestCheckRepositoryProvider(t *testing.T) {
	t.Parallel()

	withReaction := func(cfg config.ServiceConfig, name, provider string) config.ServiceConfig {
		cfg.Reactions = map[string]config.ReactionConfig{name: {Provider: provider}}
		return cfg
	}
	withCI := func(cfg config.ServiceConfig, provider string) config.ServiceConfig {
		cfg.CIFeedback.Kind = provider
		return cfg
	}
	withLabels := func(cfg config.ServiceConfig, provider string) config.ServiceConfig {
		cfg.LabelCommands.Provider = provider
		cfg.LabelCommands.FixLabel = "sortie:fix"
		return cfg
	}
	local := repositoryConfig("gitea")
	local.Environment.Kind = config.EnvironmentKindLocal

	tests := []struct {
		name string
		cfg  config.ServiceConfig
		want []string // substrings of the error; nil for none
	}{
		{name: "no provider", cfg: withCI(repositoryConfig(""), "github")},
		{name: "a repository outside remote is not checked", cfg: withCI(local, "github")},
		{name: "export as the only SCM user", cfg: repositoryConfig("gitea")},
		{name: "the same provider as every reaction", cfg: withCI(withReaction(repositoryConfig("github"), scmKindAutoMerge, "github"), "github")},
		{
			name: "a ci_failure provider mismatch",
			cfg:  withCI(repositoryConfig("gitea"), "github"),
			want: []string{repositoryProviderField, `"gitea"`, "reactions.ci_failure.provider", `"github"`},
		},
		{
			name: "a reaction provider mismatch",
			cfg:  withReaction(repositoryConfig("gitea"), scmKindReviewComments, "gitlab"),
			want: []string{repositoryProviderField, "reactions.review_comments.provider", `"gitlab"`},
		},
		{
			name: "a label command provider mismatch",
			cfg:  withLabels(repositoryConfig("gitea"), "github"),
			want: []string{repositoryProviderField, "reactions.label_commands.provider"},
		},
		{
			name: "an inactive reaction is not compared",
			cfg: func() config.ServiceConfig {
				cfg := repositoryConfig("gitea")
				cfg.LabelCommands.Provider = "github" // no label: inactive
				return cfg
			}(),
		},
		{
			name: "an unregistered provider",
			cfg:  repositoryConfig("nosuchforge"),
			want: []string{repositoryProviderField, `"nosuchforge"`, "no registered SCM adapter"},
		},
		{
			name: "an adapter without the pull request capability",
			cfg:  repositoryConfig(scmWithoutPullRequestsKind),
			want: []string{repositoryProviderField, scmWithoutPullRequestsKind, "cannot open pull requests"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := checkRepositoryProvider(tt.cfg)
			if tt.want == nil {
				if err != nil {
					t.Fatalf("checkRepositoryProvider() = %v, want nil", err)
				}
				return
			}
			ce, ok := errors.AsType[*config.ConfigError](err)
			if !ok || ce.Field != repositoryProviderField {
				t.Fatalf("checkRepositoryProvider() = %v, want a config error for %s", err, repositoryProviderField)
			}
			for _, s := range tt.want {
				if !strings.Contains(err.Error(), s) {
					t.Errorf("error %q, want it to contain %q", err, s)
				}
			}
		})
	}
}

func TestPullRequestOpeners(t *testing.T) {
	t.Parallel()

	newGitHub := func(t *testing.T) domain.SCMAdapter {
		t.Helper()
		a, err := newSCMAdapter("github", map[string]any{"api_key": "unused"})
		if err != nil {
			t.Fatal(err)
		}
		return a
	}

	t.Run("the shared adapter serves its provider", func(t *testing.T) {
		t.Parallel()
		shared := newGitHub(t)
		opener, err := pullRequestOpeners(shared, "github")(repositoryConfig("github"))
		if err != nil {
			t.Fatalf("opener error = %v", err)
		}
		if opener != shared.(domain.PullRequestOpener) {
			t.Error("opener is not the shared adapter")
		}
	})

	t.Run("another provider after a reload is built from its settings", func(t *testing.T) {
		t.Parallel()
		cfg := repositoryConfig("gitea")
		cfg.SetExtensionSection("gitea", map[string]any{"api_key": "unused", "endpoint": "https://gitea.example"})
		opener, err := pullRequestOpeners(newGitHub(t), "github")(cfg)
		if err != nil {
			t.Fatalf("opener error = %v", err)
		}
		if got := fmt.Sprintf("%T", opener); got != "*gitea.GiteaSCMAdapter" {
			t.Errorf("opener = %s, want a gitea adapter", got)
		}
	})

	t.Run("a provider whose settings fail to build fails", func(t *testing.T) {
		t.Parallel()
		_, err := pullRequestOpeners(nil, "")(repositoryConfig("gitea"))
		if err == nil || !strings.Contains(err.Error(), "gitea") {
			t.Errorf("opener error = %v, want the gitea construction failure", err)
		}
	})

	t.Run("an adapter without the capability fails", func(t *testing.T) {
		t.Parallel()
		_, err := pullRequestOpeners(scmWithoutPullRequests{}, scmWithoutPullRequestsKind)(repositoryConfig(scmWithoutPullRequestsKind))
		if err == nil || !strings.Contains(err.Error(), "cannot open pull requests") {
			t.Errorf("opener error = %v, want the missing capability", err)
		}
	})
}
