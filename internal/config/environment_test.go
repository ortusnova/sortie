package config

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestEnvironmentConfig_AbsentIsLocal(t *testing.T) {
	t.Parallel()

	cfg, err := NewServiceConfig(map[string]any{})
	if err != nil {
		t.Fatalf("NewServiceConfig(empty) error = %v, want nil", err)
	}
	want := EnvironmentConfig{Kind: EnvironmentKindLocal}
	if !reflect.DeepEqual(cfg.Environment, want) {
		t.Errorf("Environment = %+v, want %+v", cfg.Environment, want)
	}

	cfg, err = NewServiceConfig(map[string]any{"environment": map[string]any{"host_prefix": []any{"broker", "--"}}})
	if err != nil {
		t.Fatalf("NewServiceConfig(host_prefix only) error = %v, want nil", err)
	}
	if cfg.Environment.Kind != EnvironmentKindLocal {
		t.Errorf("Environment.Kind = %q, want %q when kind is absent", cfg.Environment.Kind, EnvironmentKindLocal)
	}
	if !reflect.DeepEqual(cfg.Environment.HostPrefix, []string{"broker", "--"}) {
		t.Errorf("Environment.HostPrefix = %q, want [broker --]", cfg.Environment.HostPrefix)
	}
}

// A workflow that still selects the removed smolvm kind fails
// validation, and the error names the setting, the kind and the remote
// kind that replaces it, whatever else the block holds.
func TestEnvironmentConfig_SmolvmKindRemoved(t *testing.T) {
	t.Parallel()

	for _, raw := range []map[string]any{
		{"environment": map[string]any{"kind": "smolvm"}},
		{"environment": map[string]any{"kind": "smolvm", "smolvm": map[string]any{
			"pack":       "/srv/sortie/agent.smolmachine",
			"repository": map[string]any{"url": "https://example.com/o/app.git", "base": "main"},
		}}},
		{"environment": map[string]any{"kind": "smolvm"}, "worker": map[string]any{"ssh_hosts": []any{"build01.internal"}}},
	} {
		_, err := NewServiceConfig(raw)
		var ce *ConfigError
		if !errors.As(err, &ce) {
			t.Fatalf("NewServiceConfig(%v) error = %v, want a *ConfigError", raw, err)
		}
		if ce.Field != "environment.kind" {
			t.Errorf("ConfigError.Field = %q, want environment.kind", ce.Field)
		}
		for _, want := range []string{"environment.kind", "smolvm", "remote"} {
			if !strings.Contains(ce.Error(), want) {
				t.Errorf("error %q does not name %q", ce.Error(), want)
			}
		}
	}
}

func TestEnvironmentConfig_LocalIgnoresSmolvmBlock(t *testing.T) {
	t.Parallel()

	raw := map[string]any{
		"environment": map[string]any{
			"kind":   "local",
			"smolvm": map[string]any{"allow_hosts": []any{"https://bad"}, "cpus": "many"},
		},
		"worker": map[string]any{"ssh_hosts": []any{"host-1"}},
	}
	cfg, err := NewServiceConfig(raw)
	if err != nil {
		t.Fatalf("NewServiceConfig() error = %v, want nil: a leftover smolvm block is ignored", err)
	}
	if !reflect.DeepEqual(cfg.Environment, EnvironmentConfig{Kind: EnvironmentKindLocal}) {
		t.Errorf("Environment = %+v, want the local kind alone", cfg.Environment)
	}
	if got := cfg.Environment.RepositorySet(); !reflect.DeepEqual(got, RepositorySet{}) {
		t.Errorf("RepositorySet() = %+v, want none under kind local", got)
	}
}

func TestEnvironmentConfig_Errors(t *testing.T) {
	t.Parallel()

	withEnv := func(env map[string]any) map[string]any {
		return map[string]any{"environment": env}
	}
	withRepo := func(repo any) map[string]any {
		return remoteRaw(map[string]any{"repository": repo})
	}

	tests := []struct {
		name      string
		raw       map[string]any
		wantField string
		wantMsg   []string
	}{
		{
			name:      "environment is not a mapping",
			raw:       map[string]any{"environment": "remote"},
			wantField: "environment",
			wantMsg:   []string{"expected mapping"},
		},
		{
			name:      "unknown kind",
			raw:       withEnv(map[string]any{"kind": "firecracker"}),
			wantField: "environment.kind",
			wantMsg:   []string{"firecracker", "local, remote"},
		},
		{
			name:      "empty kind",
			raw:       withEnv(map[string]any{"kind": ""}),
			wantField: "environment.kind",
			wantMsg:   []string{`""`},
		},
		{
			name:      "kind of the wrong type",
			raw:       withEnv(map[string]any{"kind": 3}),
			wantField: "environment.kind",
			wantMsg:   []string{"expected string, got integer"},
		},
		{
			name:      "empty host_prefix list",
			raw:       withEnv(map[string]any{"host_prefix": []any{}}),
			wantField: "environment.host_prefix",
			wantMsg:   []string{"empty list"},
		},
		{
			name:      "empty host_prefix element",
			raw:       withEnv(map[string]any{"host_prefix": []any{"broker", ""}}),
			wantField: "environment.host_prefix[1]",
			wantMsg:   []string{"empty string"},
		},
		{
			name:      "host_prefix element of the wrong type",
			raw:       withEnv(map[string]any{"host_prefix": []any{"broker", 7}}),
			wantField: "environment.host_prefix[1]",
			wantMsg:   []string{"expected string, got integer"},
		},
		{
			name:      "host_prefix of the wrong type",
			raw:       withEnv(map[string]any{"host_prefix": "broker --"}),
			wantField: "environment.host_prefix",
			wantMsg:   []string{"expected list, got string"},
		},
		{
			name:      "repository of the wrong type",
			raw:       withRepo("git@github.com:org/app.git"),
			wantField: "environment.remote.repository",
			wantMsg:   []string{"expected mapping, got string"},
		},
		{
			name:      "repository without url",
			raw:       withRepo(map[string]any{"base": "main"}),
			wantField: "environment.remote.repository.url",
			wantMsg:   []string{"required"},
		},
		{
			name:      "repository without base",
			raw:       withRepo(map[string]any{"url": "u"}),
			wantField: "environment.remote.repository.base",
			wantMsg:   []string{"required"},
		},
		{
			name:      "repository provider without project",
			raw:       withRepo(map[string]any{"url": "u", "base": "main", "provider": "github"}),
			wantField: "environment.remote.repository.project",
			wantMsg:   []string{"github"},
		},
		{
			name:      "repository max_bundle_mib not positive",
			raw:       withRepo(map[string]any{"url": "u", "base": "main", "max_bundle_mib": 0}),
			wantField: "environment.remote.repository.max_bundle_mib",
			wantMsg:   []string{"positive"},
		},
		{
			name:      "repository branch_prefix of the wrong type",
			raw:       withRepo(map[string]any{"url": "u", "base": "main", "branch_prefix": 1}),
			wantField: "environment.remote.repository.branch_prefix",
			wantMsg:   []string{"expected string, got integer"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewServiceConfig(tt.raw)
			var ce *ConfigError
			if !errors.As(err, &ce) {
				t.Fatalf("NewServiceConfig() error = %v, want a *ConfigError", err)
			}
			if ce.Field != tt.wantField {
				t.Errorf("ConfigError.Field = %q, want %q (message %q)", ce.Field, tt.wantField, ce.Message)
			}
			for _, want := range tt.wantMsg {
				if !strings.Contains(ce.Message, want) {
					t.Errorf("ConfigError.Message = %q, want it to contain %q", ce.Message, want)
				}
			}
		})
	}
}

func TestEnvironmentConfig_RepositoryDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := NewServiceConfig(remoteRaw(map[string]any{
		"repository": map[string]any{"url": "https://example.com/o/app.git", "base": "main"},
	}))
	if err != nil {
		t.Fatalf("NewServiceConfig() error = %v", err)
	}
	r := cfg.Environment.RepositorySet().Repository
	if r == nil || r.BranchPrefix != "sortie/" || r.MaxBundleMiB != 512 {
		t.Errorf("Repository = %+v, want branch_prefix %q and max_bundle_mib 512", r, "sortie/")
	}
}

func TestValidateFrontMatter_Environment(t *testing.T) {
	t.Parallel()

	raw := map[string]any{
		"environment": map[string]any{
			"kind":    "local",
			"flavour": "x",
			"smolvm":  map[string]any{"pack": "/p"},
		},
	}
	cfg, err := NewServiceConfig(raw)
	if err != nil {
		t.Fatalf("NewServiceConfig() error = %v, want nil", err)
	}

	got := map[string]string{}
	for _, w := range ValidateFrontMatter(raw, cfg) {
		got[w.Field] = w.Check
	}
	want := map[string]string{
		"environment.flavour": "unknown_sub_key",
		"environment.smolvm":  "unknown_sub_key",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ValidateFrontMatter() warnings = %v, want %v", got, want)
	}
}
