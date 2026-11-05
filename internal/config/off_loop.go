package config

import (
	"maps"
	"slices"
	"strings"
)

// DefaultOffLoopWorkers is how many calls Sortie runs off its event loop
// at once when off_loop.workers is not set.
const DefaultOffLoopWorkers = 8

// maxOffLoopWorkers is the largest off_loop.workers.
const maxOffLoopWorkers = 256

// offLoopKeys are the keys of the off_loop section.
var offLoopKeys = []string{"workers"}

// OffLoopConfig is the parsed off_loop section: how the forge, tracker,
// git and command calls Sortie makes off its event loop are run.
type OffLoopConfig struct {
	// Workers bounds the calls running at once; a call started while
	// every worker is busy waits in the queue.
	Workers int
}

// buildOffLoopConfig parses the off_loop section of raw. An absent
// section has the default workers.
func buildOffLoopConfig(raw map[string]any) (OffLoopConfig, error) {
	cfg := OffLoopConfig{Workers: DefaultOffLoopWorkers}
	v, present := raw["off_loop"]
	if !present || v == nil {
		return cfg, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return OffLoopConfig{}, typeError("off_loop", "mapping", v)
	}
	for _, key := range slices.Sorted(maps.Keys(m)) {
		if !slices.Contains(offLoopKeys, key) {
			return OffLoopConfig{}, &ConfigError{Field: "off_loop." + key, Message: "unknown key; valid: " + strings.Join(offLoopKeys, ", ")}
		}
	}
	if w, exists := m["workers"]; exists && w != nil {
		n, err := coerceIntField(m, "workers", "off_loop.workers")
		if err != nil {
			return OffLoopConfig{}, err
		}
		if n < 1 || n > maxOffLoopWorkers {
			return OffLoopConfig{}, &ConfigError{Field: "off_loop.workers", Message: "must be an integer from 1 to 256"}
		}
		cfg.Workers = n
	}
	return cfg, nil
}
