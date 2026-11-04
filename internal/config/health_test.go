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
		if !reflect.DeepEqual(cfg.Health, tt.want) {
			t.Errorf("%s: Health = %+v, want %+v", tt.name, cfg.Health, tt.want)
		}
		if cfg.ExtensionSection("health") != nil {
			t.Errorf("%s: health is collected as an extension, want it a core section", tt.name)
		}
	}
}

func TestHealthCredentials(t *testing.T) {
	raw := func(creds any) map[string]any {
		return map[string]any{"tracker": map[string]any{"kind": "file"}, "health": map[string]any{"credentials": creds}}
	}
	cfg, err := NewServiceConfig(raw([]any{
		map[string]any{"name": "claude", "env": "CLAUDE_CODE_OAUTH_TOKEN",
			"expires_command": "jq -r .claudeAiOauth.expiresAt ~/.claude/.credentials.json",
			"login":           " Claude Code on taichi, as sortie "},
		map[string]any{"name": "openrouter", "env": "OPENROUTER_API_KEY"},
	}))
	if err != nil {
		t.Fatalf("NewServiceConfig() error = %v", err)
	}
	want := []HealthCredential{{
		Name: "claude", Env: "CLAUDE_CODE_OAUTH_TOKEN",
		ExpiresCommand: "jq -r .claudeAiOauth.expiresAt ~/.claude/.credentials.json",
		Login:          "Claude Code on taichi, as sortie",
	}, {Name: "openrouter", Env: "OPENROUTER_API_KEY"}}
	if got := cfg.Health.Credentials; !reflect.DeepEqual(got, want) {
		t.Errorf("Credentials = %+v, want %+v", got, want)
	}

	for _, bad := range []struct {
		name  string
		creds any
		field string
	}{
		{"not a list", "claude", "health.credentials"},
		{"not a map", []any{"claude"}, "health.credentials[0]"},
		{"no name", []any{map[string]any{"env": "CLAUDE_TOKEN"}}, "health.credentials[0].name"},
		{"bad name", []any{map[string]any{"name": "a b", "env": "CLAUDE_TOKEN"}}, "health.credentials[0].name"},
		{"duplicate name", []any{map[string]any{"name": "claude", "env": "A"}, map[string]any{"name": "claude", "env": "B"}}, "health.credentials[1].name"},
		{"bad env", []any{map[string]any{"name": "claude", "env": "1A"}}, "health.credentials[0].env"},
		{"blank expires_command", []any{map[string]any{"name": "claude", "expires_command": " "}}, "health.credentials[0].expires_command"},
		{"expires_command not a string", []any{map[string]any{"name": "claude", "expires_command": 1}}, "health.credentials[0].expires_command"},
		{"blank login", []any{map[string]any{"name": "claude", "env": "A", "login": ""}}, "health.credentials[0].login"},
		{"nothing to watch", []any{map[string]any{"name": "claude", "login": "Claude Code"}}, "health.credentials[0]"},
		{"unknown key", []any{map[string]any{"name": "claude", "env": "A", "command": "cat token"}}, "health.credentials[0].command"},
	} {
		_, err := NewServiceConfig(raw(bad.creds))
		var ce *ConfigError
		if !errors.As(err, &ce) || ce.Field != bad.field {
			t.Errorf("%s: NewServiceConfig err = %v, want a ConfigError on %s", bad.name, err, bad.field)
		}
	}
}
