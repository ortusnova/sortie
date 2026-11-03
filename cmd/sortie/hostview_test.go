package main

import (
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/hostview"
)

func TestHostMode(t *testing.T) {
	for kind, want := range map[string]hostview.Mode{
		config.EnvironmentKindRemote: hostview.ModeElsewhere,
		config.EnvironmentKindLocal:  hostview.ModeHost,
		"":                           hostview.ModeHost,
	} {
		if got := hostMode(kind); got != want {
			t.Errorf("hostMode(%q) = %v, want %v", kind, got, want)
		}
	}
}
