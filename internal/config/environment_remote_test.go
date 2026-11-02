package config

import (
	"errors"
	"maps"
	"reflect"
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
