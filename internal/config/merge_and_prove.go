package config

import (
	"fmt"
	"maps"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
)

// DefaultMergeAndProveDeadline is how long a story merged through
// merge-and-prove waits for every host to deploy it, and then for its
// live proof, when merge_and_prove.deadline is not set.
const DefaultMergeAndProveDeadline = 30 * time.Minute

// DefaultRevertScanInterval is how often Sortie reads the forge for merged
// reverts of the stories it holds a merge record of, when
// merge_and_prove.revert_scan_interval is not set.
const DefaultRevertScanInterval = 3 * time.Minute

// minRevertScanInterval is the shortest merge_and_prove.revert_scan_interval.
const minRevertScanInterval = time.Minute

// mergeAndProveKeys are the keys of the merge_and_prove section.
var mergeAndProveKeys = []string{"deadline", "hosts", "revert_scan_interval"}

// hostNamePattern is what a merge_and_prove.hosts key may be.
var hostNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// MergeAndProveConfig is the parsed merge_and_prove section: how Sortie
// merges a story that only a deploy can prove, and how long it waits.
type MergeAndProveConfig struct {
	// Deadline bounds each wait after the merge: for every host to
	// deploy the merge commit, then for the live proof's record.
	Deadline time.Duration

	// RevertScanInterval is how often the forge is read for merged
	// reverts of recorded merges.
	RevertScanInterval time.Duration

	// Hosts lists each host's deploy state file, sorted by host name.
	// Empty means the one host of health.deploy_state.
	Hosts []DeployHost
}

// DeployHost is one host whose deploy state merge-and-prove reads.
type DeployHost struct {
	// Name names the host as a person knows it.
	Name string

	// DeployState is the absolute path of the host's deploy state file,
	// with ~ and $VAR expanded.
	DeployState string
}

// DeployHosts returns the hosts merge-and-prove waits on: the listed
// ones, or else the one of health.deploy_state named local; nil when
// neither is set.
func (c ServiceConfig) DeployHosts(local string) []DeployHost {
	if len(c.MergeAndProve.Hosts) > 0 {
		return slices.Clone(c.MergeAndProve.Hosts)
	}
	if c.Health.DeployState == "" {
		return nil
	}
	return []DeployHost{{Name: local, DeployState: c.Health.DeployState}}
}

// buildMergeAndProveConfig parses the merge_and_prove section of raw. An
// absent section has the default deadline and no hosts of its own.
func buildMergeAndProveConfig(raw map[string]any) (MergeAndProveConfig, error) {
	cfg := MergeAndProveConfig{Deadline: DefaultMergeAndProveDeadline, RevertScanInterval: DefaultRevertScanInterval}
	v, present := raw["merge_and_prove"]
	if !present || v == nil {
		return cfg, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return MergeAndProveConfig{}, typeError("merge_and_prove", "mapping", v)
	}
	for _, key := range slices.Sorted(maps.Keys(m)) {
		if !slices.Contains(mergeAndProveKeys, key) {
			return MergeAndProveConfig{}, &ConfigError{Field: "merge_and_prove." + key, Message: "unknown key; valid: " + strings.Join(mergeAndProveKeys, ", ")}
		}
	}
	deadline, present, err := requireStringField(m, "deadline", "merge_and_prove.deadline")
	if err != nil {
		return MergeAndProveConfig{}, err
	}
	if present {
		d, err := time.ParseDuration(strings.TrimSpace(deadline))
		if err != nil || d <= 0 {
			return MergeAndProveConfig{}, &ConfigError{Field: "merge_and_prove.deadline", Message: fmt.Sprintf("%q is not a positive duration such as 30m or 1h", deadline)}
		}
		cfg.Deadline = d
	}
	interval, present, err := requireStringField(m, "revert_scan_interval", "merge_and_prove.revert_scan_interval")
	if err != nil {
		return MergeAndProveConfig{}, err
	}
	if present {
		d, err := time.ParseDuration(strings.TrimSpace(interval))
		if err != nil || d < minRevertScanInterval {
			return MergeAndProveConfig{}, &ConfigError{Field: "merge_and_prove.revert_scan_interval", Message: fmt.Sprintf("%q is not a duration of at least 1m, such as 3m or 10m", interval)}
		}
		cfg.RevertScanInterval = d
	}
	if cfg.Hosts, err = parseDeployHosts(m["hosts"]); err != nil {
		return MergeAndProveConfig{}, err
	}
	return cfg, nil
}

// parseDeployHosts reads merge_and_prove.hosts, a mapping of host name to
// deploy state file.
func parseDeployHosts(value any) ([]DeployHost, error) {
	if value == nil {
		return nil, nil
	}
	m, ok := value.(map[string]any)
	if !ok {
		return nil, typeError("merge_and_prove.hosts", "mapping", value)
	}
	hosts := make([]DeployHost, 0, len(m))
	for _, name := range slices.Sorted(maps.Keys(m)) {
		field := "merge_and_prove.hosts." + name
		if !hostNamePattern.MatchString(name) {
			return nil, &ConfigError{Field: field, Message: fmt.Sprintf("invalid host name %q; use letters, digits, '.', '_' and '-'", name)}
		}
		path, ok := m[name].(string)
		if !ok {
			return nil, typeError(field, "string", m[name])
		}
		path = strings.TrimSpace(path)
		if path == "" {
			return nil, &ConfigError{Field: field, Message: "must name the host's deploy state file"}
		}
		expanded, err := ExpandPath(path, true)
		if err != nil {
			return nil, &ConfigError{Field: field, Message: err.Error()}
		}
		if !filepath.IsAbs(expanded) {
			return nil, &ConfigError{Field: field, Message: fmt.Sprintf("%q is not an absolute path after ~ and $VAR expansion", expanded)}
		}
		hosts = append(hosts, DeployHost{Name: name, DeployState: filepath.Clean(expanded)})
	}
	return hosts, nil
}
