package config

import (
	"errors"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// writePack creates a readable pack file and returns its path.
func writePack(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.smolmachine")
	if err := os.WriteFile(path, []byte("pack"), 0o644); err != nil {
		t.Fatalf("write pack: %v", err)
	}
	return path
}

// smolvmRaw returns front matter selecting the smolvm kind with the
// given pack and extra smolvm keys.
func smolvmRaw(pack string, extra map[string]any) map[string]any {
	smolvm := map[string]any{"pack": pack}
	maps.Copy(smolvm, extra)
	return map[string]any{
		"environment": map[string]any{"kind": "smolvm", "smolvm": smolvm},
	}
}

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

func TestEnvironmentConfig_LocalIgnoresSmolvmBlock(t *testing.T) {
	t.Parallel()

	// A smolvm block that would fail validation under the smolvm kind:
	// no pack, a malformed host, and a wrong-typed key.
	raw := map[string]any{
		"environment": map[string]any{
			"kind": "local",
			"smolvm": map[string]any{
				"allow_hosts": []any{"https://bad"},
				"cpus":        "many",
			},
		},
		"worker": map[string]any{"ssh_hosts": []any{"host-1"}},
	}
	cfg, err := NewServiceConfig(raw)
	if err != nil {
		t.Fatalf("NewServiceConfig() error = %v, want nil: smolvm keys are ignored under kind local", err)
	}
	if !reflect.DeepEqual(cfg.Environment.Smolvm, SmolvmConfig{}) {
		t.Errorf("Environment.Smolvm = %+v, want the zero value under kind local", cfg.Environment.Smolvm)
	}
}

func TestEnvironmentConfig_SmolvmFull(t *testing.T) {
	t.Parallel()

	pack := writePack(t)
	raw := smolvmRaw(pack, map[string]any{
		"binary":      "/opt/smolvm/bin/smolvm",
		"cpus":        4,
		"memory_mib":  "8192",
		"storage_gib": 20,
		"allow_hosts": []any{"openrouter.ai", "*.npmjs.org"},
		"credentials": []any{
			map[string]any{"name": "openrouter", "env": "OPENROUTER_API_KEY", "hosts": []any{"openrouter.ai"}},
			map[string]any{"name": "gh_api-1", "env": "GH_TOKEN", "hosts": []any{"api.github.com", "*.github.com"}, "header": "Authorization"},
		},
		"repository": map[string]any{
			"url":            "git@github.com:org/app.git",
			"base":           "main",
			"branch_prefix":  "agent/",
			"provider":       "github",
			"project":        "org/app",
			"max_bundle_mib": 64,
		},
		"pass_env":  []any{"GOFLAGS"},
		"ssh_agent": true,
	})
	raw["environment"].(map[string]any)["host_prefix"] = []any{"acp-broker", "--"}

	cfg, err := NewServiceConfig(raw)
	if err != nil {
		t.Fatalf("NewServiceConfig() error = %v, want nil", err)
	}
	want := EnvironmentConfig{
		Kind:       EnvironmentKindSmolvm,
		HostPrefix: []string{"acp-broker", "--"},
		Smolvm: SmolvmConfig{
			Binary:     "/opt/smolvm/bin/smolvm",
			Pack:       pack,
			CPUs:       4,
			MemoryMiB:  8192,
			StorageGiB: 20,
			AllowHosts: []string{"openrouter.ai", "*.npmjs.org"},
			Credentials: []SmolvmCredential{
				{Name: "openrouter", Env: "OPENROUTER_API_KEY", Hosts: []string{"openrouter.ai"}},
				{Name: "gh_api-1", Env: "GH_TOKEN", Hosts: []string{"api.github.com", "*.github.com"}, Header: "Authorization"},
			},
			Repository: &SmolvmRepository{
				URL:          "git@github.com:org/app.git",
				Base:         "main",
				BranchPrefix: "agent/",
				Provider:     "github",
				Project:      "org/app",
				MaxBundleMiB: 64,
			},
			Repositories: []SmolvmRepository{{
				URL: "git@github.com:org/app.git", Base: "main", BranchPrefix: "agent/",
				Provider: "github", Project: "org/app", MaxBundleMiB: 64,
			}},
			PassEnv:  []string{"GOFLAGS"},
			SSHAgent: true,
		},
	}
	if !reflect.DeepEqual(cfg.Environment, want) {
		t.Errorf("Environment =\n%+v\nwant\n%+v", cfg.Environment, want)
	}
}

func TestEnvironmentConfig_SmolvmDefaults(t *testing.T) {
	t.Parallel()

	pack := writePack(t)
	cfg, err := NewServiceConfig(smolvmRaw(pack, map[string]any{
		"repository": map[string]any{"url": "https://example.com/app.git", "base": "main"},
	}))
	if err != nil {
		t.Fatalf("NewServiceConfig() error = %v, want nil", err)
	}
	s := cfg.Environment.Smolvm
	if s.Binary != "smolvm" {
		t.Errorf("Smolvm.Binary = %q, want the default %q", s.Binary, "smolvm")
	}
	if s.CPUs != 0 || s.MemoryMiB != 0 || s.StorageGiB != 0 {
		t.Errorf("Smolvm sizing = %d/%d/%d, want 0/0/0 for smolvm's defaults", s.CPUs, s.MemoryMiB, s.StorageGiB)
	}
	if s.AllowHosts != nil || s.Credentials != nil || s.PassEnv != nil || s.SSHAgent {
		t.Errorf("Smolvm = %+v, want no hosts, credentials, pass_env or ssh_agent", s)
	}
	if s.Repository == nil || s.Repository.BranchPrefix != "sortie/" || s.Repository.MaxBundleMiB != 512 {
		t.Errorf("Smolvm.Repository = %+v, want branch_prefix %q and max_bundle_mib 512", s.Repository, "sortie/")
	}
}

func TestEnvironmentConfig_SmolvmPathExpansion(t *testing.T) {
	pack := writePack(t)
	t.Setenv("SORTIE_TEST_ENV_PACK_DIR", filepath.Dir(pack))
	t.Setenv("SORTIE_TEST_ENV_BIN", "/usr/local/bin/smolvm")

	cfg, err := NewServiceConfig(smolvmRaw("$SORTIE_TEST_ENV_PACK_DIR/"+filepath.Base(pack), map[string]any{
		"binary": "${SORTIE_TEST_ENV_BIN}",
	}))
	if err != nil {
		t.Fatalf("NewServiceConfig() error = %v, want nil", err)
	}
	if cfg.Environment.Smolvm.Pack != pack {
		t.Errorf("Smolvm.Pack = %q, want %q", cfg.Environment.Smolvm.Pack, pack)
	}
	if cfg.Environment.Smolvm.Binary != "/usr/local/bin/smolvm" {
		t.Errorf("Smolvm.Binary = %q, want %q", cfg.Environment.Smolvm.Binary, "/usr/local/bin/smolvm")
	}
}

func TestEnvironmentConfig_Errors(t *testing.T) {
	t.Parallel()

	pack := writePack(t)
	missing := filepath.Join(t.TempDir(), "missing.smolmachine")
	dirPack := t.TempDir()

	cred := func(fields map[string]any) map[string]any {
		entry := map[string]any{"name": "openrouter", "env": "OPENROUTER_API_KEY", "hosts": []any{"openrouter.ai"}}
		for k, v := range fields {
			if v == nil {
				delete(entry, k)
				continue
			}
			entry[k] = v
		}
		return entry
	}
	withCreds := func(entries ...any) map[string]any {
		return smolvmRaw(pack, map[string]any{"credentials": entries})
	}
	withEnv := func(env map[string]any) map[string]any {
		return map[string]any{"environment": env}
	}

	tests := []struct {
		name      string
		raw       map[string]any
		wantField string
		wantMsg   []string
	}{
		{
			name:      "environment is not a mapping",
			raw:       map[string]any{"environment": "smolvm"},
			wantField: "environment",
			wantMsg:   []string{"expected mapping"},
		},
		{
			name:      "unknown kind",
			raw:       withEnv(map[string]any{"kind": "firecracker"}),
			wantField: "environment.kind",
			wantMsg:   []string{"firecracker"},
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
			name: "smolvm with worker.ssh_hosts",
			raw: func() map[string]any {
				raw := smolvmRaw(pack, nil)
				raw["worker"] = map[string]any{"ssh_hosts": []any{"build01.internal"}}
				return raw
			}(),
			wantField: "environment.kind",
			wantMsg:   []string{"smolvm", "worker.ssh_hosts"},
		},
		{
			name:      "smolvm block of the wrong type",
			raw:       withEnv(map[string]any{"kind": "smolvm", "smolvm": []any{}}),
			wantField: "environment.smolvm",
			wantMsg:   []string{"expected mapping, got list"},
		},
		{
			name:      "smolvm without pack",
			raw:       withEnv(map[string]any{"kind": "smolvm"}),
			wantField: "environment.smolvm.pack",
			wantMsg:   []string{"required"},
		},
		{
			name:      "missing pack",
			raw:       smolvmRaw(missing, nil),
			wantField: "environment.smolvm.pack",
			wantMsg:   []string{missing},
		},
		{
			name:      "pack is a directory",
			raw:       smolvmRaw(dirPack, nil),
			wantField: "environment.smolvm.pack",
			wantMsg:   []string{dirPack, "not a regular file"},
		},
		{
			name:      "pack of the wrong type",
			raw:       smolvmRaw("", map[string]any{"pack": 12}),
			wantField: "environment.smolvm.pack",
			wantMsg:   []string{"expected string, got integer"},
		},
		{
			name:      "binary of the wrong type",
			raw:       smolvmRaw(pack, map[string]any{"binary": true}),
			wantField: "environment.smolvm.binary",
			wantMsg:   []string{"expected string, got boolean"},
		},
		{
			name:      "cpus not an integer",
			raw:       smolvmRaw(pack, map[string]any{"cpus": "many"}),
			wantField: "environment.smolvm.cpus",
			wantMsg:   []string{"many"},
		},
		{
			name:      "negative memory_mib",
			raw:       smolvmRaw(pack, map[string]any{"memory_mib": -1}),
			wantField: "environment.smolvm.memory_mib",
			wantMsg:   []string{"negative", "-1"},
		},
		{
			name:      "fractional storage_gib",
			raw:       smolvmRaw(pack, map[string]any{"storage_gib": 1.5}),
			wantField: "environment.smolvm.storage_gib",
			wantMsg:   []string{"1.5"},
		},
		{
			name:      "allow_hosts of the wrong type",
			raw:       smolvmRaw(pack, map[string]any{"allow_hosts": "openrouter.ai"}),
			wantField: "environment.smolvm.allow_hosts",
			wantMsg:   []string{"expected list, got string"},
		},
		{
			name:      "allow_hosts entry with a scheme",
			raw:       smolvmRaw(pack, map[string]any{"allow_hosts": []any{"openrouter.ai", "https://github.com"}}),
			wantField: "environment.smolvm.allow_hosts[1]",
			wantMsg:   []string{"https://github.com"},
		},
		{
			name:      "allow_hosts entry with a port",
			raw:       smolvmRaw(pack, map[string]any{"allow_hosts": []any{"github.com:443"}}),
			wantField: "environment.smolvm.allow_hosts[0]",
			wantMsg:   []string{"github.com:443"},
		},
		{
			name:      "allow_hosts entry with an inner wildcard",
			raw:       smolvmRaw(pack, map[string]any{"allow_hosts": []any{"api.*.com"}}),
			wantField: "environment.smolvm.allow_hosts[0]",
			wantMsg:   []string{"api.*.com", "wildcard"},
		},
		{
			name:      "allow_hosts entry that is a bare wildcard",
			raw:       smolvmRaw(pack, map[string]any{"allow_hosts": []any{"*"}}),
			wantField: "environment.smolvm.allow_hosts[0]",
			wantMsg:   []string{`"*"`},
		},
		{
			name:      "empty allow_hosts entry",
			raw:       smolvmRaw(pack, map[string]any{"allow_hosts": []any{""}}),
			wantField: "environment.smolvm.allow_hosts[0]",
			wantMsg:   []string{"empty"},
		},
		{
			name:      "allow_hosts entry of the wrong type",
			raw:       smolvmRaw(pack, map[string]any{"allow_hosts": []any{42}}),
			wantField: "environment.smolvm.allow_hosts[0]",
			wantMsg:   []string{"expected string, got integer"},
		},
		{
			name:      "credentials of the wrong type",
			raw:       smolvmRaw(pack, map[string]any{"credentials": map[string]any{"name": "x"}}),
			wantField: "environment.smolvm.credentials",
			wantMsg:   []string{"expected list, got mapping"},
		},
		{
			name:      "credential entry of the wrong type",
			raw:       withCreds("openrouter=OPENROUTER_API_KEY@openrouter.ai"),
			wantField: "environment.smolvm.credentials[0]",
			wantMsg:   []string{"expected mapping, got string"},
		},
		{
			name:      "credential without name",
			raw:       withCreds(cred(map[string]any{"name": nil})),
			wantField: "environment.smolvm.credentials[0].name",
			wantMsg:   []string{"required"},
		},
		{
			name:      "credential with an invalid name",
			raw:       withCreds(cred(map[string]any{"name": "open router"})),
			wantField: "environment.smolvm.credentials[0].name",
			wantMsg:   []string{"open router"},
		},
		{
			name:      "duplicate credential names",
			raw:       withCreds(cred(nil), cred(map[string]any{"env": "OTHER_KEY"})),
			wantField: "environment.smolvm.credentials[1].name",
			wantMsg:   []string{"openrouter", "credentials[0]"},
		},
		{
			name:      "credential without env",
			raw:       withCreds(cred(map[string]any{"env": nil})),
			wantField: "environment.smolvm.credentials[0].env",
			wantMsg:   []string{"required"},
		},
		{
			name:      "credential env that is not an identifier",
			raw:       withCreds(cred(map[string]any{"env": "1BAD-NAME"})),
			wantField: "environment.smolvm.credentials[0].env",
			wantMsg:   []string{"1BAD-NAME"},
		},
		{
			name:      "credential env of the wrong type",
			raw:       withCreds(cred(map[string]any{"env": 5})),
			wantField: "environment.smolvm.credentials[0].env",
			wantMsg:   []string{"expected string, got integer"},
		},
		{
			name:      "credential without hosts",
			raw:       withCreds(cred(map[string]any{"hosts": nil})),
			wantField: "environment.smolvm.credentials[0].hosts",
			wantMsg:   []string{"at least one host"},
		},
		{
			name:      "credential with an empty hosts list",
			raw:       withCreds(cred(map[string]any{"hosts": []any{}})),
			wantField: "environment.smolvm.credentials[0].hosts",
			wantMsg:   []string{"at least one host"},
		},
		{
			name:      "credential with a bad host pattern",
			raw:       withCreds(cred(map[string]any{"hosts": []any{"openrouter.ai", "*bad.com"}})),
			wantField: "environment.smolvm.credentials[0].hosts[1]",
			wantMsg:   []string{"*bad.com"},
		},
		{
			name:      "credential with an empty header",
			raw:       withCreds(cred(map[string]any{"header": ""})),
			wantField: "environment.smolvm.credentials[0].header",
			wantMsg:   []string{"empty"},
		},
		{
			name:      "credential header with a colon",
			raw:       withCreds(cred(map[string]any{"header": "X-Api:Key"})),
			wantField: "environment.smolvm.credentials[0].header",
			wantMsg:   []string{"invalid header name"},
		},
		{
			name:      "credential header with a comma",
			raw:       withCreds(cred(map[string]any{"header": "X-A,X-B"})),
			wantField: "environment.smolvm.credentials[0].header",
			wantMsg:   []string{"invalid header name"},
		},
		{
			name:      "credential header with an equals sign",
			raw:       withCreds(cred(map[string]any{"header": "X=Y"})),
			wantField: "environment.smolvm.credentials[0].header",
			wantMsg:   []string{"invalid header name"},
		},
		{
			name:      "credential header with a space",
			raw:       withCreds(cred(map[string]any{"header": "X Api"})),
			wantField: "environment.smolvm.credentials[0].header",
			wantMsg:   []string{"invalid header name"},
		},
		{
			name:      "credential header with a tab",
			raw:       withCreds(cred(map[string]any{"header": "X\tApi"})),
			wantField: "environment.smolvm.credentials[0].header",
			wantMsg:   []string{"invalid header name"},
		},
		{
			name:      "credential header with a slash",
			raw:       withCreds(cred(map[string]any{"header": "X/Y"})),
			wantField: "environment.smolvm.credentials[0].header",
			wantMsg:   []string{"invalid header name"},
		},
		{
			name:      "credential header with an at sign",
			raw:       withCreds(cred(map[string]any{"header": "X@Y"})),
			wantField: "environment.smolvm.credentials[0].header",
			wantMsg:   []string{"invalid header name"},
		},
		{
			name:      "credential header with a quote",
			raw:       withCreds(cred(map[string]any{"header": "X\"Y"})),
			wantField: "environment.smolvm.credentials[0].header",
			wantMsg:   []string{"invalid header name"},
		},
		{
			name:      "credential header with a non-ASCII letter",
			raw:       withCreds(cred(map[string]any{"header": "X-Ápi"})),
			wantField: "environment.smolvm.credentials[0].header",
			wantMsg:   []string{"invalid header name"},
		},
		{
			name:      "credential header with a modifier",
			raw:       withCreds(cred(map[string]any{"header": "X:set-header=Y"})),
			wantField: "environment.smolvm.credentials[0].header",
			wantMsg:   []string{"invalid header name"},
		},
		{
			name:      "credential with both file and command",
			raw:       withCreds(cred(map[string]any{"file": "/run/token", "command": "cat /run/token"})),
			wantField: "environment.smolvm.credentials[0]",
			wantMsg:   []string{"both file and command"},
		},
		{
			name:      "credential with an empty file",
			raw:       withCreds(cred(map[string]any{"file": ""})),
			wantField: "environment.smolvm.credentials[0].file",
			wantMsg:   []string{"empty"},
		},
		{
			name:      "credential with a relative file",
			raw:       withCreds(cred(map[string]any{"file": "token.txt"})),
			wantField: "environment.smolvm.credentials[0].file",
			wantMsg:   []string{"token.txt", "absolute"},
		},
		{
			name:      "credential file of the wrong type",
			raw:       withCreds(cred(map[string]any{"file": 5})),
			wantField: "environment.smolvm.credentials[0].file",
			wantMsg:   []string{"expected string, got integer"},
		},
		{
			name:      "credential with a blank command",
			raw:       withCreds(cred(map[string]any{"command": "  "})),
			wantField: "environment.smolvm.credentials[0].command",
			wantMsg:   []string{"empty"},
		},
		{
			name:      "credential header Host",
			raw:       withCreds(cred(map[string]any{"header": "Host"})),
			wantField: "environment.smolvm.credentials[0].header",
			wantMsg:   []string{"Host", "routes or frames"},
		},
		{
			name:      "credential header content-length",
			raw:       withCreds(cred(map[string]any{"header": "content-length"})),
			wantField: "environment.smolvm.credentials[0].header",
			wantMsg:   []string{"content-length", "routes or frames"},
		},
		{
			name:      "credential header Cookie",
			raw:       withCreds(cred(map[string]any{"header": "Cookie"})),
			wantField: "environment.smolvm.credentials[0].header",
			wantMsg:   []string{"Cookie", "routes or frames"},
		},
		{
			name:      "credential header Proxy-Authorization",
			raw:       withCreds(cred(map[string]any{"header": "Proxy-Authorization"})),
			wantField: "environment.smolvm.credentials[0].header",
			wantMsg:   []string{"Proxy-Authorization", "routes or frames"},
		},
		{
			name:      "credential header Transfer-Encoding",
			raw:       withCreds(cred(map[string]any{"header": "Transfer-Encoding"})),
			wantField: "environment.smolvm.credentials[0].header",
			wantMsg:   []string{"Transfer-Encoding", "routes or frames"},
		},
		{
			name:      "credential header of the wrong type",
			raw:       withCreds(cred(map[string]any{"header": []any{"a"}})),
			wantField: "environment.smolvm.credentials[0].header",
			wantMsg:   []string{"expected string, got list"},
		},
		{
			name:      "pass_env of the wrong type",
			raw:       smolvmRaw(pack, map[string]any{"pass_env": "GOFLAGS"}),
			wantField: "environment.smolvm.pass_env",
			wantMsg:   []string{"expected list, got string"},
		},
		{
			name:      "pass_env name that is not an identifier",
			raw:       smolvmRaw(pack, map[string]any{"pass_env": []any{"GOFLAGS", "A=B"}}),
			wantField: "environment.smolvm.pass_env[1]",
			wantMsg:   []string{"A=B"},
		},
		{
			name:      "ssh_agent of the wrong type",
			raw:       smolvmRaw(pack, map[string]any{"ssh_agent": "yes"}),
			wantField: "environment.smolvm.ssh_agent",
			wantMsg:   []string{"expected boolean, got string"},
		},
		{
			name:      "repository of the wrong type",
			raw:       smolvmRaw(pack, map[string]any{"repository": "git@github.com:org/app.git"}),
			wantField: "environment.smolvm.repository",
			wantMsg:   []string{"expected mapping, got string"},
		},
		{
			name:      "repository without url",
			raw:       smolvmRaw(pack, map[string]any{"repository": map[string]any{"base": "main"}}),
			wantField: "environment.smolvm.repository.url",
			wantMsg:   []string{"required"},
		},
		{
			name:      "repository without base",
			raw:       smolvmRaw(pack, map[string]any{"repository": map[string]any{"url": "u"}}),
			wantField: "environment.smolvm.repository.base",
			wantMsg:   []string{"required"},
		},
		{
			name:      "repository provider without project",
			raw:       smolvmRaw(pack, map[string]any{"repository": map[string]any{"url": "u", "base": "main", "provider": "github"}}),
			wantField: "environment.smolvm.repository.project",
			wantMsg:   []string{"github"},
		},
		{
			name:      "repository max_bundle_mib not positive",
			raw:       smolvmRaw(pack, map[string]any{"repository": map[string]any{"url": "u", "base": "main", "max_bundle_mib": 0}}),
			wantField: "environment.smolvm.repository.max_bundle_mib",
			wantMsg:   []string{"positive"},
		},
		{
			name:      "repository branch_prefix of the wrong type",
			raw:       smolvmRaw(pack, map[string]any{"repository": map[string]any{"url": "u", "base": "main", "branch_prefix": 1}}),
			wantField: "environment.smolvm.repository.branch_prefix",
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

func TestEnvironmentConfig_UnreadablePack(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("file modes do not deny reads on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root reads a file whatever its mode")
	}

	pack := writePack(t)
	if err := os.Chmod(pack, 0o000); err != nil {
		t.Fatalf("chmod pack: %v", err)
	}

	_, err := NewServiceConfig(smolvmRaw(pack, nil))
	var ce *ConfigError
	if !errors.As(err, &ce) || ce.Field != "environment.smolvm.pack" || !strings.Contains(ce.Message, pack) {
		t.Errorf("NewServiceConfig() error = %v, want a *ConfigError for environment.smolvm.pack naming %q", err, pack)
	}
}

func TestValidateFrontMatter_Environment(t *testing.T) {
	t.Parallel()

	raw := map[string]any{
		"environment": map[string]any{
			"kind":    "local",
			"flavour": "x",
			"smolvm": map[string]any{
				"pack": "/p",
				"cpu":  2,
				"cpus": "four",
				"credentials": []any{
					map[string]any{"name": "n"},
				},
				"repository": map[string]any{
					"url":    "u",
					"branch": "main",
				},
			},
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
		"environment.flavour":                  "unknown_sub_key",
		"environment.smolvm.cpu":               "unknown_sub_key",
		"environment.smolvm.cpus":              "type_mismatch",
		"environment.smolvm.repository.branch": "unknown_sub_key",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ValidateFrontMatter() warnings = %v, want %v", got, want)
	}
}

func TestSmolvmCredentialSource(t *testing.T) {
	t.Setenv("SORTIE_TEST_TOKEN_DIR", "/run/tokens")
	pack := writePack(t)

	cfg, err := NewServiceConfig(smolvmRaw(pack, map[string]any{"credentials": []any{
		map[string]any{"name": "plain", "env": "PLAIN_API_KEY", "hosts": []any{"a.example"}},
		map[string]any{"name": "fromfile", "env": "FILE_API_KEY", "hosts": []any{"b.example"}, "file": "$SORTIE_TEST_TOKEN_DIR/./b"},
		map[string]any{"name": "fromcmd", "env": "CMD_API_KEY", "hosts": []any{"c.example"}, "command": "anthropic-token.py --print"},
	}}))
	if err != nil {
		t.Fatalf("NewServiceConfig() error = %v", err)
	}
	want := []SmolvmCredential{
		{Name: "plain", Env: "PLAIN_API_KEY", Hosts: []string{"a.example"}},
		{Name: "fromfile", Env: "FILE_API_KEY", Hosts: []string{"b.example"}, File: "/run/tokens/b"},
		{Name: "fromcmd", Env: "CMD_API_KEY", Hosts: []string{"c.example"}, Command: "anthropic-token.py --print"},
	}
	if got := cfg.Environment.Smolvm.Credentials; !reflect.DeepEqual(got, want) {
		t.Errorf("Credentials = %+v, want %+v", got, want)
	}
	for i, c := range want {
		if got, wantSourced := c.Sourced(), i > 0; got != wantSourced {
			t.Errorf("%s.Sourced() = %v, want %v", c.Name, got, wantSourced)
		}
	}
}

func TestSmolvmCredentialHeader_AcceptsFieldNameTokens(t *testing.T) {
	t.Parallel()

	for _, header := range []string{"X-Api-Key", "authorization", "x_api.key~1!#$%&'*+^`|", "Expect"} {
		err := checkCredentialHeader(header, "environment.smolvm.credentials[0].header")
		if err != nil {
			t.Errorf("checkCredentialHeader(%q) = %v, want nil", header, err)
		}
	}
}
