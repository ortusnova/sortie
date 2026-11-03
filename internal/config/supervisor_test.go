package config

import (
	"errors"
	"testing"
)

func TestSupervisorConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		section   any
		want      SupervisorConfig
		wantField string
	}{
		{name: "absent", section: nil},
		{
			name:    "defaults",
			section: map[string]any{"agent": "claude-code", "prompt": "supervisor.md"},
			want: SupervisorConfig{
				Agent: "claude-code", Prompt: "supervisor.md",
				SweepIntervalMS:   DefaultSupervisorSweepIntervalMS,
				MaxSessionsPerDay: DefaultSupervisorMaxSessionsPerDay,
				MaxTokensPerDay:   DefaultSupervisorMaxTokensPerDay,
			},
		},
		{
			name: "every field",
			section: map[string]any{
				"agent": "codex", "prompt": "/etc/sortie/supervisor.md",
				"sweep_interval_ms": 600000, "max_sessions_per_day": "12", "max_tokens_per_day": 500000,
			},
			want: SupervisorConfig{Agent: "codex", Prompt: "/etc/sortie/supervisor.md", SweepIntervalMS: 600000, MaxSessionsPerDay: 12, MaxTokensPerDay: 500000},
		},
		{name: "not a map", section: "on", wantField: "supervisor"},
		{name: "no agent", section: map[string]any{"prompt": "supervisor.md"}, wantField: "supervisor.agent"},
		{name: "no prompt", section: map[string]any{"agent": "claude-code"}, wantField: "supervisor.prompt"},
		{name: "a zero sweep", section: map[string]any{"agent": "claude-code", "prompt": "p.md", "sweep_interval_ms": 0}, wantField: "supervisor.sweep_interval_ms"},
		{name: "a negative session budget", section: map[string]any{"agent": "claude-code", "prompt": "p.md", "max_sessions_per_day": -1}, wantField: "supervisor.max_sessions_per_day"},
		{name: "a text token budget", section: map[string]any{"agent": "claude-code", "prompt": "p.md", "max_tokens_per_day": "lots"}, wantField: "supervisor.max_tokens_per_day"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			raw := map[string]any{"tracker": map[string]any{"kind": "file"}}
			if tt.section != nil {
				raw["supervisor"] = tt.section
			}
			cfg, err := NewServiceConfig(raw)
			if tt.wantField != "" {
				var ce *ConfigError
				if !errors.As(err, &ce) || ce.Field != tt.wantField {
					t.Fatalf("NewServiceConfig err = %v, want a ConfigError on %s", err, tt.wantField)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewServiceConfig: %v", err)
			}
			if cfg.Supervisor != tt.want {
				t.Errorf("Supervisor = %+v, want %+v", cfg.Supervisor, tt.want)
			}
			if cfg.Supervisor.Enabled() != (tt.want.Agent != "") {
				t.Errorf("Enabled() = %v", cfg.Supervisor.Enabled())
			}
			if _, ok := cfg.ExtensionValue("supervisor"); ok {
				t.Error("supervisor collected as an extension, want a core section")
			}
		})
	}
}
