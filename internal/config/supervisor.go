package config

import (
	"fmt"
	"strings"
)

// Defaults of the supervisor section.
const (
	// DefaultSupervisorSweepIntervalMS is how often the supervisor looks
	// at the whole state when nothing else started it: 30 minutes.
	DefaultSupervisorSweepIntervalMS = 30 * 60 * 1000

	// DefaultSupervisorMaxSessionsPerDay is the supervisor sessions one
	// UTC day may start.
	DefaultSupervisorMaxSessionsPerDay = 48

	// DefaultSupervisorMaxTokensPerDay is the tokens, less cache reads,
	// the supervisor sessions of one UTC day may spend.
	DefaultSupervisorMaxTokensPerDay = 2_000_000

	// DefaultSupervisorQuietSweepMS is how often a sweep starts a session
	// while the state digest has not changed: two hours.
	DefaultSupervisorQuietSweepMS = 2 * 60 * 60 * 1000

	// DefaultSupervisorNoopLimit is the consecutive sessions of one trigger
	// kind that may take no action on the same state digest before that
	// kind is suspended.
	DefaultSupervisorNoopLimit = 3

	// DefaultSupervisorSessionTokens is the tokens, less cache reads, one
	// supervisor session may spend before it is stopped.
	DefaultSupervisorSessionTokens = 150_000

	// DefaultSupervisorVelocityFactor is how many times the even hourly
	// pace of the daily token budget the last hour may spend before only
	// sweeps start a session.
	DefaultSupervisorVelocityFactor = 3
)

// SupervisorConfig is the parsed supervisor section: the agent that
// settles what Sortie's own rules cannot, the prompt file it is given,
// how often it sweeps the state, and its daily budget. The zero value
// (Agent == "") means no supervisor runs.
type SupervisorConfig struct {
	// Agent is the agent kind the supervisor session runs.
	Agent string

	// Prompt is the path of the supervisor's prompt template as written,
	// resolved against the directory of WORKFLOW.md when relative.
	Prompt string

	// SweepIntervalMS is the time between two sweeps.
	SweepIntervalMS int

	// MaxSessionsPerDay bounds the supervisor sessions started in one
	// UTC day.
	MaxSessionsPerDay int

	// MaxTokensPerDay bounds the tokens, less cache reads, the
	// supervisor sessions of one UTC day spend.
	MaxTokensPerDay int

	// QuietSweepMS is how often a sweep starts a session while the state
	// digest is the one the last session started on.
	QuietSweepMS int

	// NoopLimit is the consecutive sessions of one trigger kind that may
	// take no action on the same state digest before that kind starts no
	// session until the digest changes.
	NoopLimit int

	// SessionTokens bounds the tokens, less cache reads, one supervisor
	// session spends; a session that reaches it is stopped.
	SessionTokens int

	// VelocityFactor is how many times the even hourly pace of
	// MaxTokensPerDay the tokens of the last hour may reach before only
	// sweeps start a session.
	VelocityFactor int
}

// Enabled reports whether the workflow configures a supervisor.
func (c SupervisorConfig) Enabled() bool { return c.Agent != "" }

// buildSupervisorConfig parses the supervisor section of raw. An absent
// section is the zero value; a present one must be a mapping naming agent
// and prompt.
func buildSupervisorConfig(raw map[string]any) (SupervisorConfig, error) {
	v, present := raw["supervisor"]
	if !present || v == nil {
		return SupervisorConfig{}, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		return SupervisorConfig{}, &ConfigError{Field: "supervisor", Message: fmt.Sprintf("expected map, got %T", v)}
	}
	agent, _, err := requireStringField(m, "agent", "supervisor.agent")
	if err != nil {
		return SupervisorConfig{}, err
	}
	agent = strings.TrimSpace(agent)
	if agent == "" {
		return SupervisorConfig{}, &ConfigError{Field: "supervisor.agent", Message: "is required: the agent kind the supervisor session runs"}
	}
	promptPath, _, err := requireStringField(m, "prompt", "supervisor.prompt")
	if err != nil {
		return SupervisorConfig{}, err
	}
	promptPath = strings.TrimSpace(promptPath)
	if promptPath == "" {
		return SupervisorConfig{}, &ConfigError{Field: "supervisor.prompt", Message: "is required: the path of the supervisor's prompt file"}
	}
	expanded, err := ExpandPath(promptPath, true)
	if err != nil {
		return SupervisorConfig{}, &ConfigError{Field: "supervisor.prompt", Message: err.Error()}
	}

	cfg := SupervisorConfig{
		Agent:             agent,
		Prompt:            expanded,
		SweepIntervalMS:   DefaultSupervisorSweepIntervalMS,
		MaxSessionsPerDay: DefaultSupervisorMaxSessionsPerDay,
		MaxTokensPerDay:   DefaultSupervisorMaxTokensPerDay,
		QuietSweepMS:      DefaultSupervisorQuietSweepMS,
		NoopLimit:         DefaultSupervisorNoopLimit,
		SessionTokens:     DefaultSupervisorSessionTokens,
		VelocityFactor:    DefaultSupervisorVelocityFactor,
	}
	for _, f := range []struct {
		key  string
		dest *int
	}{
		{"sweep_interval_ms", &cfg.SweepIntervalMS},
		{"max_sessions_per_day", &cfg.MaxSessionsPerDay},
		{"max_tokens_per_day", &cfg.MaxTokensPerDay},
		{"quiet_sweep_ms", &cfg.QuietSweepMS},
		{"noop_limit", &cfg.NoopLimit},
		{"session_tokens", &cfg.SessionTokens},
		{"velocity_factor", &cfg.VelocityFactor},
	} {
		if v, exists := m[f.key]; !exists || v == nil {
			continue
		}
		n, err := coerceIntField(m, f.key, "supervisor."+f.key)
		if err != nil {
			return SupervisorConfig{}, err
		}
		if n < 1 {
			return SupervisorConfig{}, &ConfigError{Field: "supervisor." + f.key, Message: "must be a positive integer"}
		}
		*f.dest = n
	}
	return cfg, nil
}
