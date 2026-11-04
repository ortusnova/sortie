package main

import (
	"slices"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/environment"
)

// TestEnvironmentKindsRegistered proves main's blank imports register
// every environment kind the configuration accepts, and that each
// resolves to an environment reporting its own kind.
func TestEnvironmentKindsRegistered(t *testing.T) {
	t.Parallel()

	want := []string{config.EnvironmentKindLocal, config.EnvironmentKindRemote}
	if got := environment.Registry.Kinds(); !slices.Equal(got, want) {
		t.Errorf("environment.Registry.Kinds() = %q, want %q", got, want)
	}
	for _, kind := range want {
		ctor, err := environment.Registry.Get(kind)
		if err != nil {
			t.Errorf("environment.Registry.Get(%q) error = %v", kind, err)
			continue
		}
		env, err := ctor(environment.Params{Config: config.EnvironmentConfig{Kind: kind}})
		if err != nil {
			t.Errorf("constructor for %q error = %v, want nil", kind, err)
			continue
		}
		if env.Kind() != kind {
			t.Errorf("constructor for %q built kind %q", kind, env.Kind())
		}
	}
	if _, err := environment.Registry.Get("firecracker"); err == nil {
		t.Error(`environment.Registry.Get("firecracker") error = nil, want an unknown-kind error`)
	}
}
