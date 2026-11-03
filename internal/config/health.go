package config

import (
	"fmt"
	"path/filepath"
	"strings"
)

// HealthConfig is the parsed health section: what Sortie's own health
// check reads beyond the rest of the configuration. The zero value reads
// nothing more.
type HealthConfig struct {
	// DeployState is the absolute path of the deploy state file a deploy
	// kit writes, with ~ and $VAR expanded; empty when no kit deploys
	// this Sortie.
	DeployState string
}

// buildHealthConfig parses the health section of raw. An absent section
// is the zero value.
func buildHealthConfig(raw map[string]any) (HealthConfig, error) {
	v, present := raw["health"]
	if !present || v == nil {
		return HealthConfig{}, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return HealthConfig{}, &ConfigError{Field: "health", Message: fmt.Sprintf("expected map, got %T", v)}
	}
	path, present, err := requireStringField(m, "deploy_state", "health.deploy_state")
	if err != nil || !present {
		return HealthConfig{}, err
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return HealthConfig{}, &ConfigError{Field: "health.deploy_state", Message: "must not be empty; remove the key when no kit deploys this Sortie"}
	}
	expanded, err := ExpandPath(path, true)
	if err != nil {
		return HealthConfig{}, &ConfigError{Field: "health.deploy_state", Message: err.Error()}
	}
	if !filepath.IsAbs(expanded) {
		return HealthConfig{}, &ConfigError{Field: "health.deploy_state", Message: fmt.Sprintf("%q is not an absolute path after ~ and $VAR expansion", expanded)}
	}
	return HealthConfig{DeployState: filepath.Clean(expanded)}, nil
}
