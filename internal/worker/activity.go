package worker

import (
	"bytes"
	"encoding/json"
	"io"
	"slices"
	"strings"
	"time"
	"unicode"
)

// What a worker tells Sortie for the operator's view of it, beyond what
// matching needs: what its jobs were promised, the machines on its host
// no Sortie started, and the step each job reports. Every member is
// optional, and a malformed one is dropped rather than refused, so a
// worker that sends it wrong still runs jobs.
const (
	// MaxStep bounds a reported step or phase, in characters.
	MaxStep = 200
	// MaxUnmanaged bounds the machines kept from one declaration.
	MaxUnmanaged = 64
	// MaxCapabilities bounds the capabilities kept from one declaration.
	MaxCapabilities = 64
	// maxBeatBytes bounds the heartbeat body read.
	maxBeatBytes = 16 << 10
)

// Unmanaged is a machine on a worker's host that no Sortie started.
type Unmanaged struct {
	Name string `json:"name"`
	// Since is when the machine was created, or when the worker first
	// saw it.
	Since time.Time `json:"since"`
	// State is the backend's word for it, such as "running"; empty when
	// unknown.
	State string `json:"state,omitempty"`
}

// Step is what a job last reported of its progress, as its worker's
// heartbeat carries it.
type Step struct {
	Step   string     `json:"step,omitempty"`
	Phase  string     `json:"phase,omitempty"`
	StepAt *time.Time `json:"step_at,omitempty"`
}

func (s Step) empty() bool { return s.Step == "" && s.Phase == "" }

// decodeStep reads a heartbeat body. An empty, malformed or stepless
// body reports no step.
func decodeStep(r io.Reader) Step {
	b, err := io.ReadAll(io.LimitReader(r, maxBeatBytes))
	if err != nil || len(bytes.TrimSpace(b)) == 0 {
		return Step{}
	}
	var s Step
	if json.Unmarshal(b, &s) != nil {
		return Step{}
	}
	s.Step, s.Phase = cleanText(s.Step), cleanText(s.Phase)
	if s.empty() {
		return Step{}
	}
	if s.StepAt != nil {
		at := s.StepAt.UTC()
		s.StepAt = &at
	}
	return s
}

// decodeActivity reads the optional used, unmanaged and capabilities
// members of a check-in into d.
func decodeActivity(raw map[string]json.RawMessage, d *Declaration) {
	if v, ok := raw["capabilities"]; ok && !isNull(v) {
		var listed []string
		if json.Unmarshal(v, &listed) == nil {
			for _, c := range listed {
				if c = cleanText(c); c != "" && !slices.Contains(d.Capabilities, c) && len(d.Capabilities) < MaxCapabilities {
					d.Capabilities = append(d.Capabilities, c)
				}
			}
		}
	}
	if v, ok := raw["used"]; ok && !isNull(v) {
		var used Free
		if json.Unmarshal(v, &used) == nil && used.CPU >= 0 && used.MemGiB >= 0 && used.DiskGiB >= 0 {
			d.Used = &used
		}
	}
	if v, ok := raw["unmanaged"]; ok && !isNull(v) {
		var listed []Unmanaged
		if json.Unmarshal(v, &listed) != nil {
			return
		}
		d.Unmanaged = make([]Unmanaged, 0, min(len(listed), MaxUnmanaged))
		for _, m := range listed {
			m.Name, m.State = cleanText(m.Name), cleanText(m.State)
			if m.Name == "" || len(d.Unmanaged) == MaxUnmanaged {
				continue
			}
			m.Since = m.Since.UTC()
			d.Unmanaged = append(d.Unmanaged, m)
		}
	}
}

func isNull(v json.RawMessage) bool { return bytes.Equal(bytes.TrimSpace(v), []byte("null")) }

// cleanText keeps a reported string printable and short.
func cleanText(s string) string {
	s = strings.TrimSpace(strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s))
	if r := []rune(s); len(r) > MaxStep {
		s = string(r[:MaxStep])
	}
	return s
}
