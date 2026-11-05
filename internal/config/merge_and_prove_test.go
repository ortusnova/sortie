package config

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestMergeAndProveConfig(t *testing.T) {
	t.Setenv("SORTIE_TEST_KIT_DIR", "/var/lib/stage0")

	tests := []struct {
		name      string
		section   any
		want      MergeAndProveConfig
		wantField string
	}{
		{name: "absent", want: MergeAndProveConfig{Deadline: 30 * time.Minute, RevertScanInterval: 3 * time.Minute}},
		{name: "empty", section: map[string]any{}, want: MergeAndProveConfig{Deadline: 30 * time.Minute, RevertScanInterval: 3 * time.Minute}},
		{name: "deadline", section: map[string]any{"deadline": "45m"}, want: MergeAndProveConfig{Deadline: 45 * time.Minute, RevertScanInterval: 3 * time.Minute}},
		{name: "revert scan interval", section: map[string]any{"revert_scan_interval": "5m"}, want: MergeAndProveConfig{Deadline: 30 * time.Minute, RevertScanInterval: 5 * time.Minute}},
		{
			name:    "hosts",
			section: map[string]any{"hosts": map[string]any{"taichi": "$SORTIE_TEST_KIT_DIR/taichi.json", "p1": "/var/lib/stage0/./deploy.json"}},
			want: MergeAndProveConfig{Deadline: 30 * time.Minute, RevertScanInterval: 3 * time.Minute, Hosts: []DeployHost{
				{Name: "p1", DeployState: "/var/lib/stage0/deploy.json"},
				{Name: "taichi", DeployState: "/var/lib/stage0/taichi.json"},
			}},
		},
		{name: "not a map", section: "on", wantField: "merge_and_prove"},
		{name: "unknown key", section: map[string]any{"timeout": "1m"}, wantField: "merge_and_prove.timeout"},
		{name: "deadline not a duration", section: map[string]any{"deadline": "soon"}, wantField: "merge_and_prove.deadline"},
		{name: "deadline zero", section: map[string]any{"deadline": "0s"}, wantField: "merge_and_prove.deadline"},
		{name: "deadline a number", section: map[string]any{"deadline": 30}, wantField: "merge_and_prove.deadline"},
		{name: "revert scan interval zero", section: map[string]any{"revert_scan_interval": "0s"}, wantField: "merge_and_prove.revert_scan_interval"},
		{name: "revert scan interval below a minute", section: map[string]any{"revert_scan_interval": "15s"}, wantField: "merge_and_prove.revert_scan_interval"},
		{name: "hosts a list", section: map[string]any{"hosts": []any{"/a"}}, wantField: "merge_and_prove.hosts"},
		{name: "host path relative", section: map[string]any{"hosts": map[string]any{"p1": "deploy.json"}}, wantField: "merge_and_prove.hosts.p1"},
		{name: "host path blank", section: map[string]any{"hosts": map[string]any{"p1": " "}}, wantField: "merge_and_prove.hosts.p1"},
		{name: "host name invalid", section: map[string]any{"hosts": map[string]any{"p 1": "/a.json"}}, wantField: "merge_and_prove.hosts.p 1"},
	}
	for _, tt := range tests {
		raw := map[string]any{"tracker": map[string]any{"kind": "file"}}
		if tt.section != nil {
			raw["merge_and_prove"] = tt.section
		}
		cfg, err := NewServiceConfig(raw)
		if tt.wantField != "" {
			var ce *ConfigError
			if !errors.As(err, &ce) || ce.Field != tt.wantField {
				t.Errorf("%s: NewServiceConfig err = %v, want a ConfigError on %s", tt.name, err, tt.wantField)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: NewServiceConfig: %v", tt.name, err)
		}
		if !reflect.DeepEqual(cfg.MergeAndProve, tt.want) {
			t.Errorf("%s: MergeAndProve = %+v, want %+v", tt.name, cfg.MergeAndProve, tt.want)
		}
		if cfg.ExtensionSection("merge_and_prove") != nil {
			t.Errorf("%s: merge_and_prove is collected as an extension, want it a core section", tt.name)
		}
	}
}

func TestDeployHosts(t *testing.T) {
	t.Parallel()
	var cfg ServiceConfig
	if got := cfg.DeployHosts("p1"); got != nil {
		t.Errorf("DeployHosts with neither = %+v, want none", got)
	}
	cfg.Health.DeployState = "/var/lib/deploy.json"
	if got := cfg.DeployHosts("p1"); !reflect.DeepEqual(got, []DeployHost{{Name: "p1", DeployState: "/var/lib/deploy.json"}}) {
		t.Errorf("DeployHosts of health.deploy_state = %+v, want the one local host", got)
	}
	cfg.MergeAndProve.Hosts = []DeployHost{{Name: "taichi", DeployState: "/t.json"}}
	if got := cfg.DeployHosts("p1"); !reflect.DeepEqual(got, cfg.MergeAndProve.Hosts) {
		t.Errorf("DeployHosts with hosts = %+v, want the listed hosts", got)
	}
}
