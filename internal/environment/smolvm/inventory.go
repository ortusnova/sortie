package smolvm

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Listed is one machine of the host's listing with what it was given:
// every machine smolvm knows, whoever made it.
type Listed struct {
	Name   string
	State  string
	Labels map[string]string
	CPUs   int
	// MemoryMiB is the machine's memory; 0 when smolvm lists none.
	MemoryMiB int
	// StorageGiB is the machine's disk and overlay; 0 when smolvm lists
	// neither.
	StorageGiB float64
	// Created is when smolvm created it; zero when it does not say.
	Created time.Time
}

// Inventory lists every machine smolvm knows with its sizes and
// creation time.
func (c *CLI) Inventory(ctx context.Context) ([]Listed, error) {
	out, err := c.run(ctx, []string{"machine", "ls"}, "", machineArgs("ls", "--json"))
	if err != nil {
		return nil, err
	}
	return ParseInventory(out)
}

// ParseInventory parses the machine listing for [CLI.Inventory]. An
// entry without a name is skipped.
func ParseInventory(data []byte) ([]Listed, error) {
	var raw []struct {
		Name      string            `json:"name"`
		State     string            `json:"state"`
		Labels    map[string]string `json:"labels"`
		CPUs      int               `json:"cpus"`
		MemoryMiB int               `json:"memory_mib"`
		StorageGB *float64          `json:"storage_gb"`
		OverlayGB *float64          `json:"overlay_gb"`
		CreatedAt json.RawMessage   `json:"created_at"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse smolvm machine listing: %w", err)
	}
	listed := make([]Listed, 0, len(raw))
	for _, m := range raw {
		if m.Name == "" {
			continue
		}
		l := Listed{Name: m.Name, State: m.State, Labels: m.Labels, CPUs: m.CPUs, MemoryMiB: m.MemoryMiB,
			Created: createdAt(m.CreatedAt)}
		for _, gib := range []*float64{m.StorageGB, m.OverlayGB} {
			if gib != nil && *gib > 0 {
				l.StorageGiB += *gib
			}
		}
		if l.Labels == nil {
			l.Labels = map[string]string{}
		}
		listed = append(listed, l)
	}
	return listed, nil
}

// createdAt reads smolvm's creation time: Unix seconds, or an RFC 3339
// string.
func createdAt(v json.RawMessage) time.Time {
	var unix int64
	if json.Unmarshal(v, &unix) == nil && unix > 0 {
		return time.Unix(unix, 0).UTC()
	}
	var s string
	if json.Unmarshal(v, &s) == nil {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}
