package config

import (
	"errors"
	"maps"
	"reflect"
	"strings"
	"testing"
)

func remoteFlowsRaw() map[string]any {
	out := map[string]any{}
	for _, f := range RemoteFlows {
		out[f] = map[string]any{"url": "https://edge.example/" + f}
	}
	return out
}

func remoteFlowsWant() map[string]string {
	out := map[string]string{}
	for _, f := range RemoteFlows {
		out[f] = "https://edge.example/" + f
	}
	return out
}

func remoteRaw(extra map[string]any) map[string]any {
	remote := map[string]any{"image": "ghcr.io/ortusnova/agent:1", "flows": remoteFlowsRaw()}
	maps.Copy(remote, extra)
	return map[string]any{"environment": map[string]any{"kind": "remote", "remote": remote}}
}

func TestEnvironmentConfig_Remote(t *testing.T) {
	t.Parallel()

	raw := remoteRaw(map[string]any{
		"requires": map[string]any{"capabilities": []any{"nested", "x86_64"}, "cpu": 2, "mem_gib": 7.5, "disk_gib": 40},
		"backend":  "firecracker",
		"credentials": []any{map[string]any{
			"name": "anthropic", "env": "ANTHROPIC_API_KEY", "hosts": []any{"api.anthropic.com"}, "header": "x-api-key",
		}},
		"lease_ttl_ms": 30000,
	})
	raw["worker"] = map[string]any{"identity_header": "X-Forwarded-User"}
	cfg, err := NewServiceConfig(raw)
	if err != nil {
		t.Fatalf("NewServiceConfig() error = %v", err)
	}
	want := RemoteConfig{
		IdentityHeader: "X-Forwarded-User",
		Requires:       RemoteRequirements{Capabilities: []string{"nested", "x86_64"}, CPU: 2, MemGiB: 7.5, DiskGiB: 40},
		Backend:        "firecracker",
		Image:          "ghcr.io/ortusnova/agent:1",
		Credentials:    []RemoteCredential{{Name: "anthropic", Env: "ANTHROPIC_API_KEY", Hosts: []string{"api.anthropic.com"}, Header: "x-api-key"}},
		Flows:          remoteFlowsWant(),
		LeaseTTLMS:     30000,
	}
	if cfg.Environment.Kind != EnvironmentKindRemote || !reflect.DeepEqual(cfg.Environment.Remote, want) {
		t.Fatalf("Environment = %+v, want remote %+v", cfg.Environment, want)
	}
}

func TestEnvironmentConfig_RemoteDefaults(t *testing.T) {
	t.Parallel()

	cfg, err := NewServiceConfig(remoteRaw(nil))
	if err != nil {
		t.Fatalf("NewServiceConfig() error = %v", err)
	}
	want := RemoteConfig{IdentityHeader: "X-Sortie-Worker-Identity", Backend: "smolvm", Image: "ghcr.io/ortusnova/agent:1",
		Flows: remoteFlowsWant(), LeaseTTLMS: 60000}
	if !reflect.DeepEqual(cfg.Environment.Remote, want) {
		t.Fatalf("Remote = %+v, want %+v", cfg.Environment.Remote, want)
	}
}

func TestEnvironmentConfig_RemoteRejects(t *testing.T) {
	t.Parallel()

	withoutFlow := remoteFlowsRaw()
	delete(withoutFlow, "usage")
	extraFlow := remoteFlowsRaw()
	extraFlow["git"] = map[string]any{"url": "https://edge.example/git"}
	badURL := remoteFlowsRaw()
	badURL["egress"] = map[string]any{"url": "edge.example:3128"}
	withUser := remoteFlowsRaw()
	withUser["bundles"] = map[string]any{"url": "https://u:p@edge.example/b"}
	for name, tc := range map[string]struct {
		raw   map[string]any
		field string
	}{
		"ssh hosts": {map[string]any{
			"environment": remoteRaw(nil)["environment"],
			"worker":      map[string]any{"ssh_hosts": []any{"h"}},
		}, "environment.kind"},
		"bad header": {map[string]any{
			"environment": remoteRaw(nil)["environment"],
			"worker":      map[string]any{"identity_header": "X: y"},
		}, "worker.identity_header"},
		"no image":            {map[string]any{"environment": map[string]any{"kind": "remote", "remote": map[string]any{"flows": remoteFlowsRaw()}}}, "environment.remote.image"},
		"no flows":            {map[string]any{"environment": map[string]any{"kind": "remote", "remote": map[string]any{"image": "i"}}}, "environment.remote.flows.stdio.url"},
		"missing flow":        {remoteRaw(map[string]any{"flows": withoutFlow}), "environment.remote.flows.usage.url"},
		"unknown flow":        {remoteRaw(map[string]any{"flows": extraFlow}), "environment.remote.flows.git"},
		"relative flow url":   {remoteRaw(map[string]any{"flows": badURL}), "environment.remote.flows.egress.url"},
		"flow url with user":  {remoteRaw(map[string]any{"flows": withUser}), "environment.remote.flows.bundles.url"},
		"negative cpu":        {remoteRaw(map[string]any{"requires": map[string]any{"cpu": -1}}), "environment.remote.requires.cpu"},
		"mem not a number":    {remoteRaw(map[string]any{"requires": map[string]any{"mem_gib": "8G"}}), "environment.remote.requires.mem_gib"},
		"old cpus key":        {remoteRaw(map[string]any{"requires": map[string]any{"cpus": 2}}), "environment.remote.requires.cpus"},
		"requires not a map":  {remoteRaw(map[string]any{"requires": []any{"linux"}}), "environment.remote.requires"},
		"capability not text": {remoteRaw(map[string]any{"requires": map[string]any{"capabilities": []any{1}}}), "environment.remote.requires.capabilities[0]"},
		"empty backend":       {remoteRaw(map[string]any{"backend": ""}), "environment.remote.backend"},
		"zero lease":          {remoteRaw(map[string]any{"lease_ttl_ms": 0}), "environment.remote.lease_ttl_ms"},
		"credential no hosts": {remoteRaw(map[string]any{"credentials": []any{map[string]any{"name": "a", "env": "A"}}}), "environment.remote.credentials[0].hosts"},
		"credential file":     {remoteRaw(map[string]any{"credentials": []any{map[string]any{"name": "a", "env": "A", "hosts": []any{"h.example"}, "file": "/t"}}}), "environment.remote.credentials[0].file"},
		"credential command":  {remoteRaw(map[string]any{"credentials": []any{map[string]any{"name": "a", "env": "A", "hosts": []any{"h.example"}, "command": "t"}}}), "environment.remote.credentials[0].command"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := NewServiceConfig(tc.raw)
			var ce *ConfigError
			if !errors.As(err, &ce) || ce.Field != tc.field {
				t.Fatalf("error = %v, want a ConfigError on %s", err, tc.field)
			}
		})
	}
}

// remoteRepositoriesRaw is the remote block of a homelab story and a
// Sortie story at once: homelab runs in its own image on a worker that
// declares nested, sortie is the default, and a third repository is
// closed to dispatch.
func remoteRepositoriesRaw() map[string]any {
	return remoteRaw(map[string]any{
		"repositories": []any{
			map[string]any{
				"name": "homelab", "url": "https://example.com/o/homelab.git", "base": "trunk",
				"provider": "github", "project": "o/homelab", "image": "ghcr.io/ortusnova/homelab:1",
				"requires": map[string]any{"capabilities": []any{"nested"}, "cpu": 4},
			},
			map[string]any{
				"name": "sortie", "url": "https://example.com/o/sortie.git", "base": "main",
				"provider": "github", "project": "o/sortie", "image": "ghcr.io/ortusnova/sortie:1",
			},
			map[string]any{"name": "attic", "url": "https://example.com/o/attic.git", "base": "main", "dispatch": false},
		},
		"default_repository": "sortie",
	})
}

// remote-runs: each repository decides its job's seed, image and
// requirements, and the default takes an issue that names none.
func TestEnvironmentConfig_RemoteRepositories(t *testing.T) {
	t.Parallel()

	cfg, err := NewServiceConfig(remoteRepositoriesRaw())
	if err != nil {
		t.Fatalf("NewServiceConfig() error = %v", err)
	}
	rc := cfg.Environment.Remote
	if rc.DefaultRepository != "sortie" || rc.Repository == nil || rc.Repository.Name != "sortie" {
		t.Fatalf("default = %q, Repository = %+v, want sortie", rc.DefaultRepository, rc.Repository)
	}
	set := cfg.Environment.RepositorySet()
	if got := set.RepositoryNames(); !reflect.DeepEqual(got, []string{"homelab", "sortie", "attic"}) {
		t.Fatalf("RepositoryNames() = %v", got)
	}

	homelab, err := set.DispatchRepository([]string{"sortie-repository:homelab"})
	if err != nil {
		t.Fatalf("DispatchRepository(homelab) error = %v", err)
	}
	want := SmolvmRepository{
		Name: "homelab", URL: "https://example.com/o/homelab.git", Base: "trunk", BranchPrefix: defaultRepoBranchPrefix,
		Provider: "github", Project: "o/homelab", MaxBundleMiB: defaultRepoMaxBundleMiB,
		Image: "ghcr.io/ortusnova/homelab:1", Requires: &RemoteRequirements{Capabilities: []string{"nested"}, CPU: 4},
	}
	if !reflect.DeepEqual(*homelab, want) {
		t.Errorf("homelab = %+v, want %+v", *homelab, want)
	}
	if rc.JobImage(homelab) != "ghcr.io/ortusnova/homelab:1" || !reflect.DeepEqual(rc.JobRequires(homelab).Capabilities, []string{"nested"}) {
		t.Errorf("homelab job: image %q requires %+v", rc.JobImage(homelab), rc.JobRequires(homelab))
	}

	sortie, err := set.DispatchRepository(nil)
	if err != nil || sortie.Name != "sortie" {
		t.Fatalf("DispatchRepository(no label) = %+v, %v, want sortie", sortie, err)
	}
	if rc.JobImage(sortie) != "ghcr.io/ortusnova/sortie:1" || !reflect.DeepEqual(rc.JobRequires(sortie), RemoteRequirements{}) {
		t.Errorf("sortie job: image %q requires %+v, want its own image and the block's requirements", rc.JobImage(sortie), rc.JobRequires(sortie))
	}

	if _, err := set.DispatchRepository([]string{"sortie-repository:attic"}); !errors.Is(err, ErrNotDispatched) {
		t.Errorf("DispatchRepository(attic) error = %v, want ErrNotDispatched", err)
	}
	if got := cfg.Environment.RepositoryField(0); got != "environment.remote.repositories[0]" {
		t.Errorf("RepositoryField(0) = %q", got)
	}
	for _, w := range ValidateFrontMatter(remoteRepositoriesRaw(), ServiceConfig{}) {
		if strings.Contains(w.Field, "repositor") {
			t.Errorf("unexpected warning %+v", w)
		}
	}
}

// The block's image is required only for a repository without its own.
func TestEnvironmentConfig_RemoteImageFallsBack(t *testing.T) {
	t.Parallel()

	own := map[string]any{"name": "a", "url": "u", "base": "main", "image": "ghcr.io/o/a:1"}
	cfg, err := NewServiceConfig(map[string]any{"environment": map[string]any{"kind": "remote", "remote": map[string]any{
		"flows": remoteFlowsRaw(), "repositories": []any{own}, "default_repository": "a",
	}}})
	if err != nil {
		t.Fatalf("NewServiceConfig() error = %v", err)
	}
	if rc := cfg.Environment.Remote; rc.Image != "" || rc.JobImage(rc.Repository) != "ghcr.io/o/a:1" {
		t.Errorf("image %q, job image %q", rc.Image, rc.JobImage(rc.Repository))
	}

	cfg, err = NewServiceConfig(remoteRaw(map[string]any{"repository": map[string]any{"url": "u", "base": "main"}}))
	if err != nil {
		t.Fatalf("NewServiceConfig() error = %v", err)
	}
	if rc := cfg.Environment.Remote; rc.JobImage(rc.Repository) != "ghcr.io/ortusnova/agent:1" || len(rc.Repositories) != 1 || rc.DefaultRepository != "" {
		t.Errorf("single repository block: %+v", rc)
	}
}

// remote-runs: A key the remote kind does not run. A key environment.remote
// does not know, at any depth, fails validation naming the key.
func TestEnvironmentConfig_RemoteRefusesUnknownKeys(t *testing.T) {
	t.Parallel()

	repo := func(extra map[string]any) map[string]any {
		r := map[string]any{"name": "a", "url": "u", "base": "main"}
		maps.Copy(r, extra)
		return r
	}
	extraFlow := remoteFlowsRaw()
	extraFlow["stdio"] = map[string]any{"url": "https://edge.example/stdio", "token": "t"}
	noImage := map[string]any{"environment": map[string]any{"kind": "remote", "remote": map[string]any{
		"flows": remoteFlowsRaw(), "repositories": []any{repo(map[string]any{"image": "i"}), repo(map[string]any{"name": "b"})},
	}}}
	closed := repo(map[string]any{"dispatch": false})
	for name, tc := range map[string]struct {
		raw   map[string]any
		field string
	}{
		"pass_env":              {remoteRaw(map[string]any{"pass_env": []any{"HOME"}}), "environment.remote.pass_env"},
		"allow_hosts":           {remoteRaw(map[string]any{"allow_hosts": []any{"example.com"}}), "environment.remote.allow_hosts"},
		"smolvm key":            {remoteRaw(map[string]any{"pack": "/p"}), "environment.remote.pack"},
		"repository entry key":  {remoteRaw(map[string]any{"repositories": []any{repo(map[string]any{"pass_env": []any{"A"}})}}), "environment.remote.repositories[0].pass_env"},
		"single block key":      {remoteRaw(map[string]any{"repository": repo(map[string]any{"mounts": []any{}})}), "environment.remote.repository.mounts"},
		"repository requires":   {remoteRaw(map[string]any{"repositories": []any{repo(map[string]any{"requires": map[string]any{"gpu": 1}})}}), "environment.remote.repositories[0].requires.gpu"},
		"credential key":        {remoteRaw(map[string]any{"credentials": []any{map[string]any{"name": "a", "env": "A", "hosts": []any{"h.example"}, "value": "x"}}}), "environment.remote.credentials[0].value"},
		"flow key":              {remoteRaw(map[string]any{"flows": extraFlow}), "environment.remote.flows.stdio.token"},
		"repository image":      {noImage, "environment.remote.image"},
		"empty repo image":      {remoteRaw(map[string]any{"repositories": []any{repo(map[string]any{"image": ""})}}), "environment.remote.repositories[0].image"},
		"single block dispatch": {remoteRaw(map[string]any{"repository": repo(map[string]any{"dispatch": false})}), "environment.remote.repository.dispatch"},
		"closed default":        {remoteRaw(map[string]any{"repositories": []any{closed}, "default_repository": "a"}), "environment.remote.default_repository"},
		"unlisted default":      {remoteRaw(map[string]any{"repositories": []any{repo(nil)}, "default_repository": "b"}), "environment.remote.default_repository"},
		"both forms":            {remoteRaw(map[string]any{"repositories": []any{repo(nil)}, "repository": repo(nil)}), "environment.remote.repositories"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := NewServiceConfig(tc.raw)
			var ce *ConfigError
			if !errors.As(err, &ce) || ce.Field != tc.field {
				t.Fatalf("error = %v, want a ConfigError on %s", err, tc.field)
			}
		})
	}
}

// remote-runs: A key the remote kind does not run. A workspace hook under
// the remote kind fails validation naming the hook; the other kinds keep
// their hooks.
func TestEnvironmentConfig_RemoteRefusesHooks(t *testing.T) {
	t.Parallel()

	for _, hook := range []string{"after_create", "before_run", "after_run", "before_remove"} {
		raw := remoteRaw(nil)
		raw["hooks"] = map[string]any{hook: "make setup", "timeout_ms": 1000}
		_, err := NewServiceConfig(raw)
		var ce *ConfigError
		if !errors.As(err, &ce) || ce.Field != "hooks."+hook {
			t.Errorf("%s: error = %v, want a ConfigError on hooks.%s", hook, err, hook)
		}
	}

	raw := remoteRaw(nil)
	raw["hooks"] = map[string]any{"timeout_ms": 1000}
	if _, err := NewServiceConfig(raw); err != nil {
		t.Errorf("hooks.timeout_ms alone: error = %v, want none", err)
	}
	if _, err := NewServiceConfig(map[string]any{"hooks": map[string]any{"after_create": "make setup"}}); err != nil {
		t.Errorf("hook under the local kind: error = %v, want none", err)
	}
}
