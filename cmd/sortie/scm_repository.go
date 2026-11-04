package main

import (
	"fmt"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/orchestrator"
	"github.com/sortie-ai/sortie/internal/registry"
)

// scmReactionProviderSetting returns the setting that names the provider
// of the active SCM reaction kind name. Every kind, label_commands
// included, is a block under reactions.
func scmReactionProviderSetting(name string) string {
	return "reactions." + name + ".provider"
}

// checkRepositoryProvider fails when a configured repository names a
// provider that differs from the provider of an active SCM reaction or
// of another repository, since one SCM adapter serves every repository
// and every such reaction, or that names no registered SCM adapter able
// to open pull requests. A configuration without a repository provider
// passes. The failure is a [*config.ConfigError] for the repository
// provider whose message names the other setting. It constructs no
// adapter.
func checkRepositoryProvider(cfg config.ServiceConfig) error {
	if orchestrator.RepositoriesProvider(cfg.Environment) == "" {
		return nil
	}
	first, firstField := "", ""
	for i, repo := range cfg.Environment.RepositorySet().AllRepositories() {
		if repo.Provider == "" {
			continue
		}
		field := cfg.Environment.RepositoryField(i) + ".provider"
		if first == "" {
			first, firstField = repo.Provider, field
		} else if repo.Provider != first {
			return &config.ConfigError{
				Field: field,
				Message: fmt.Sprintf("is %q but %s is %q: every repository shares one SCM adapter, so they must name the same provider",
					repo.Provider, firstField, first),
			}
		}
		if err := checkOneRepositoryProvider(cfg, field, repo.Provider); err != nil {
			return err
		}
	}
	return nil
}

// checkOneRepositoryProvider checks the provider one repository names
// at field, as [checkRepositoryProvider] describes.
func checkOneRepositoryProvider(cfg config.ServiceConfig, field, provider string) error {
	for _, kind := range activeSCMReactionKinds(cfg) {
		if kind.active && kind.provider != provider {
			return &config.ConfigError{
				Field: field,
				Message: fmt.Sprintf("is %q but %s is %q: the repository and every active SCM reaction share one SCM adapter, so they must name the same provider",
					provider, scmReactionProviderSetting(kind.name), kind.provider),
			}
		}
	}
	meta, ok := registry.SCMAdapters.Meta(provider)
	if !ok {
		return &config.ConfigError{
			Field:   field,
			Message: fmt.Sprintf("names %q, which is no registered SCM adapter (registered: %v)", provider, registry.SCMAdapters.Kinds()),
		}
	}
	if !meta.OpensPullRequests {
		return &config.ConfigError{
			Field:   field,
			Message: fmt.Sprintf("names %q, whose SCM adapter cannot open pull requests", provider),
		}
	}
	return nil
}

// scmAdapterSettings returns the adapter configuration of the SCM
// provider under cfg: the provider's top-level extension section, plus
// the tracker credentials when the tracker is the same forge.
func scmAdapterSettings(cfg config.ServiceConfig, provider string) map[string]any {
	settings := make(map[string]any)
	config.MergeAdapterExtensions(settings, cfg, provider)
	if provider == cfg.Tracker.Kind {
		mergeTrackerCredentials(settings, cfg.Tracker)
	}
	return settings
}

// newSCMAdapter constructs the SCM adapter of provider from settings.
func newSCMAdapter(provider string, settings map[string]any) (domain.SCMAdapter, error) {
	ctor, err := registry.SCMAdapters.Get(provider)
	if err != nil {
		return nil, err
	}
	return ctor(settings)
}

// pullRequestOpeners returns the [orchestrator.PullRequestOpenerFunc]
// of a program whose startup built shared, the SCM adapter of
// sharedProvider, nil when it built none. The configuration a run starts
// with may since have been reloaded to name another provider; that
// provider's adapter is then built from the configuration in the same
// way, without the reaction settings that only startup reads.
func pullRequestOpeners(shared domain.SCMAdapter, sharedProvider string) orchestrator.PullRequestOpenerFunc {
	return func(cfg config.ServiceConfig) (domain.PullRequestOpener, error) {
		provider := orchestrator.RepositoryProvider(cfg.Environment)
		adapter := shared
		if adapter == nil || provider != sharedProvider {
			built, err := newSCMAdapter(provider, scmAdapterSettings(cfg, provider))
			if err != nil {
				return nil, fmt.Errorf("construct the %q SCM adapter: %w", provider, err)
			}
			adapter = built
		}
		opener, ok := adapter.(domain.PullRequestOpener)
		if !ok {
			return nil, fmt.Errorf("the %q SCM adapter cannot open pull requests", provider)
		}
		return opener, nil
	}
}

// tokenInspectorOf returns the token report of the shared SCM adapter,
// or nil when there is none or it reports nothing of its token.
func tokenInspectorOf(shared domain.SCMAdapter) domain.SCMTokenInspector {
	inspector, _ := shared.(domain.SCMTokenInspector)
	return inspector
}
