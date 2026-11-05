package config

import (
	"errors"
	"testing"
)

func TestOffLoopConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		section   any
		want      int
		wantField string
	}{
		{name: "absent", want: DefaultOffLoopWorkers},
		{name: "empty", section: map[string]any{}, want: DefaultOffLoopWorkers},
		{name: "workers", section: map[string]any{"workers": 3}, want: 3},
		{name: "workers as a string", section: map[string]any{"workers": "16"}, want: 16},
		{name: "not a map", section: "on", wantField: "off_loop"},
		{name: "unknown key", section: map[string]any{"timeout": "1m"}, wantField: "off_loop.timeout"},
		{name: "workers zero", section: map[string]any{"workers": 0}, wantField: "off_loop.workers"},
		{name: "workers too many", section: map[string]any{"workers": 257}, wantField: "off_loop.workers"},
		{name: "workers not a number", section: map[string]any{"workers": "many"}, wantField: "off_loop.workers"},
	}
	for _, tt := range tests {
		raw := map[string]any{"tracker": map[string]any{"kind": "file"}}
		if tt.section != nil {
			raw["off_loop"] = tt.section
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
		if cfg.OffLoop.Workers != tt.want {
			t.Errorf("%s: OffLoop.Workers = %d, want %d", tt.name, cfg.OffLoop.Workers, tt.want)
		}
	}
}
