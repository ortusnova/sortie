package config

import (
	"errors"
	"reflect"
	"testing"
)

func TestHealthConfig(t *testing.T) {
	t.Setenv("SORTIE_TEST_KIT_DIR", "/var/lib/stage0")

	tests := []struct {
		name      string
		section   any
		want      HealthConfig
		wantField string
	}{
		{name: "absent", section: nil},
		{name: "empty", section: map[string]any{}},
		{name: "deploy state", section: map[string]any{"deploy_state": "$SORTIE_TEST_KIT_DIR/./deploy.json"}, want: HealthConfig{DeployState: "/var/lib/stage0/deploy.json"}},
		{name: "not a map", section: "on", wantField: "health"},
		{name: "blank deploy state", section: map[string]any{"deploy_state": " "}, wantField: "health.deploy_state"},
		{name: "relative deploy state", section: map[string]any{"deploy_state": "deploy.json"}, wantField: "health.deploy_state"},
		{name: "deploy state not a string", section: map[string]any{"deploy_state": 3}, wantField: "health.deploy_state"},
	}
	for _, tt := range tests {
		raw := map[string]any{"tracker": map[string]any{"kind": "file"}}
		if tt.section != nil {
			raw["health"] = tt.section
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
		if cfg.Health != tt.want {
			t.Errorf("%s: Health = %+v, want %+v", tt.name, cfg.Health, tt.want)
		}
		if cfg.ExtensionSection("health") != nil {
			t.Errorf("%s: health is collected as an extension, want it a core section", tt.name)
		}
	}
}

func TestSmolvmCredentialHealthFields(t *testing.T) {
	pack := writePack(t)

	cfg, err := NewServiceConfig(smolvmRaw(pack, map[string]any{"credentials": []any{
		map[string]any{"name": "claude", "env": "CLAUDE_CODE_OAUTH_TOKEN", "hosts": []any{"api.anthropic.com"},
			"command":         "jq -r .claudeAiOauth.accessToken ~/.claude/.credentials.json",
			"expires_command": "jq -r .claudeAiOauth.expiresAt ~/.claude/.credentials.json",
			"login":           " Claude Code on taichi, as sortie "},
	}}))
	if err != nil {
		t.Fatalf("NewServiceConfig() error = %v", err)
	}
	want := []SmolvmCredential{{
		Name: "claude", Env: "CLAUDE_CODE_OAUTH_TOKEN", Hosts: []string{"api.anthropic.com"},
		Command:        "jq -r .claudeAiOauth.accessToken ~/.claude/.credentials.json",
		ExpiresCommand: "jq -r .claudeAiOauth.expiresAt ~/.claude/.credentials.json",
		Login:          "Claude Code on taichi, as sortie",
	}}
	if got := cfg.Environment.Smolvm.Credentials; !reflect.DeepEqual(got, want) {
		t.Errorf("Credentials = %+v, want %+v", got, want)
	}

	for _, bad := range []struct {
		key   string
		value any
	}{{"expires_command", " "}, {"expires_command", 1}, {"login", ""}} {
		key, value := bad.key, bad.value
		entry := map[string]any{"name": "claude", "env": "CLAUDE_TOKEN", "hosts": []any{"api.anthropic.com"}, key: value}
		field := "environment.smolvm.credentials[0]." + key
		_, err := NewServiceConfig(smolvmRaw(pack, map[string]any{"credentials": []any{entry}}))
		var ce *ConfigError
		if !errors.As(err, &ce) || ce.Field != field {
			t.Errorf("%s = %#v: NewServiceConfig err = %v, want a ConfigError on %s", key, value, err, field)
		}
	}
}
