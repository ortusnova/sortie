package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
)

// dotenvPathOverride holds the .env file path set by the CLI --env-file flag.
// Read by applyEnvOverrides on each call (supporting dynamic reload).
// Access is synchronized via dotenvPathMu.
var (
	dotenvPathOverride string
	dotenvPathMu       sync.RWMutex
)

// SetDotEnvPath sets the .env file path for config overrides.
// Safe for concurrent use. Call this from cmd/sortie/main.go before any
// [NewServiceConfig] calls. If not set, applyEnvOverrides falls back to
// os.Getenv("SORTIE_ENV_FILE").
func SetDotEnvPath(path string) {
	dotenvPathMu.Lock()
	dotenvPathOverride = path
	dotenvPathMu.Unlock()
}

// getDotEnvPath returns the current .env file path override. Safe for
// concurrent use.
func getDotEnvPath() string {
	dotenvPathMu.RLock()
	defer dotenvPathMu.RUnlock()
	return dotenvPathOverride
}

// envOverride maps a SORTIE_* environment variable to a config field path.
type envOverride struct {
	EnvVar  string                    // e.g. "SORTIE_TRACKER_KIND"
	Section string                    // empty string = top-level field
	Field   string                    // dotted for nested (e.g. "comments.on_dispatch")
	Coerce  func(string) (any, error) // string->typed coercion; error fails startup
}

// envOverrides is the curated registry of environment variable overrides.
// Each entry maps exactly one SORTIE_* variable to one config field.
var envOverrides = []envOverride{
	// Tracker
	{"SORTIE_TRACKER_KIND", "tracker", "kind", coerceString},
	{"SORTIE_TRACKER_ENDPOINT", "tracker", "endpoint", coerceString},
	{"SORTIE_TRACKER_API_KEY", "tracker", "api_key", coerceString},
	{"SORTIE_TRACKER_PROJECT", "tracker", "project", coerceString},
	{"SORTIE_TRACKER_ACTIVE_STATES", "tracker", "active_states", coerceCSVList},
	{"SORTIE_TRACKER_TERMINAL_STATES", "tracker", "terminal_states", coerceCSVList},
	{"SORTIE_TRACKER_QUERY_FILTER", "tracker", "query_filter", coerceString},
	{"SORTIE_TRACKER_HANDOFF_STATE", "tracker", "handoff_state", coerceString},
	{"SORTIE_TRACKER_NO_CHANGE_STATE", "tracker", "no_change_state", coerceString},
	{"SORTIE_TRACKER_IN_PROGRESS_STATE", "tracker", "in_progress_state", coerceString},
	{"SORTIE_TRACKER_RELEASE_STATE", "tracker", "release_state", coerceString},
	{"SORTIE_TRACKER_BACKLOG_STATE", "tracker", "backlog_state", coerceString},
	{"SORTIE_TRACKER_CANCEL_STATE", "tracker", "cancel_state", coerceString},
	{"SORTIE_TRACKER_DONE_STATE", "tracker", "done_state", coerceString},

	// Tracker comments
	{"SORTIE_TRACKER_COMMENTS_ON_DISPATCH", "tracker", "comments.on_dispatch", coerceEnvBool},
	{"SORTIE_TRACKER_COMMENTS_ON_COMPLETION", "tracker", "comments.on_completion", coerceEnvBool},
	{"SORTIE_TRACKER_COMMENTS_ON_FAILURE", "tracker", "comments.on_failure", coerceEnvBool},

	// Polling
	{"SORTIE_POLLING_INTERVAL_MS", "polling", "interval_ms", coerceEnvInt},

	// Workspace
	{"SORTIE_WORKSPACE_ROOT", "workspace", "root", coerceString},
	{"SORTIE_WORKSPACE_RETENTION_DAYS", "workspace", "retention_days", coerceEnvInt},

	// Agent
	{"SORTIE_AGENT_KIND", "agent", "kind", coerceString},
	{"SORTIE_AGENT_COMMAND", "agent", "command", coerceString},
	{"SORTIE_AGENT_TURN_TIMEOUT_MS", "agent", "turn_timeout_ms", coerceEnvInt},
	{"SORTIE_AGENT_READ_TIMEOUT_MS", "agent", "read_timeout_ms", coerceEnvInt},
	{"SORTIE_AGENT_STALL_TIMEOUT_MS", "agent", "stall_timeout_ms", coerceEnvInt},
	{"SORTIE_AGENT_TOOL_CALL_TIMEOUT_MS", "agent", "tool_call_timeout_ms", coerceEnvInt},
	{"SORTIE_AGENT_MAX_CONCURRENT_AGENTS", "agent", "max_concurrent_agents", coerceEnvInt},
	{"SORTIE_AGENT_MAX_TURNS", "agent", "max_turns", coerceEnvInt},
	{"SORTIE_AGENT_MAX_RETRY_BACKOFF_MS", "agent", "max_retry_backoff_ms", coerceEnvInt},
	{"SORTIE_AGENT_MAX_SESSIONS", "agent", "max_sessions", coerceEnvInt},
	{"SORTIE_AGENT_MAX_TOKENS", "agent", "max_tokens", coerceEnvInt},
	{"SORTIE_AGENT_MAX_COST_USD", "agent", "max_cost_usd", coerceEnvFloat},
	{"SORTIE_AGENT_MAX_DAILY_COST_USD", "agent", "max_daily_cost_usd", coerceEnvFloat},
	{"SORTIE_AGENT_MAX_WEEKLY_COST_USD", "agent", "max_weekly_cost_usd", coerceEnvFloat},
	{"SORTIE_AGENT_MAX_MONTHLY_COST_USD", "agent", "max_monthly_cost_usd", coerceEnvFloat},
	{"SORTIE_AGENT_TOKEN_WARNING_PERCENT", "agent", "token_warning_percent", coerceEnvInt},
	{"SORTIE_AGENT_MAX_CONSECUTIVE_ABSENCES", "agent", "max_consecutive_absences", coerceEnvInt},
	{"SORTIE_AGENT_STOP_GRACE_MS", "agent", "stop_grace_ms", coerceEnvInt},

	// Price table
	{"SORTIE_PRICE_TABLE_FETCH", "price_table", "fetch", coerceEnvBool},

	// Top-level
	{"SORTIE_DB_PATH", "", "db_path", coerceString},
}

// applyEnvOverrides merges SORTIE_* environment variables and .env file
// values into the raw config map. Returns a set of field paths that were
// set from environment sources (used by section builders to skip $VAR
// expansion) and the advisories recording a missing .env file or a
// non-map section an override replaced. Returns an error on .env parse
// failures or type coercion failures.
func applyEnvOverrides(raw map[string]any) (map[string]bool, []Advisory, error) {
	// Resolve dotenv path: CLI flag -> env var -> empty (no loading).
	dotenvPath := getDotEnvPath()
	if dotenvPath == "" {
		dotenvPath = os.Getenv("SORTIE_ENV_FILE")
	}

	var advisories []Advisory

	var dotenv map[string]string
	if dotenvPath != "" {
		var err error
		dotenv, err = parseDotEnv(dotenvPath)
		if err != nil {
			return nil, nil, fmt.Errorf("config: %w", err)
		}
		if dotenv == nil {
			// Path was set but file does not exist.
			advisories = append(advisories, Advisory{
				Check:   "env_file.missing",
				Text:    fmt.Sprintf("env file %s does not exist; no values are read from it", strconv.Quote(dotenvPath)),
				Message: "env file not found, skipping",
				Attrs:   []slog.Attr{slog.String("path", dotenvPath)},
			})
		}
	}

	envKeys := make(map[string]bool)

	for _, ov := range envOverrides {
		envStr := os.Getenv(ov.EnvVar)
		if envStr == "" && dotenv != nil {
			envStr = dotenv[ov.EnvVar]
		}
		if envStr == "" {
			continue
		}

		coerced, err := ov.Coerce(envStr)
		if err != nil {
			fieldPath := ov.Field
			if ov.Section != "" {
				fieldPath = ov.Section + "." + ov.Field
			}
			return nil, nil, &ConfigError{
				Field:   fieldPath,
				Message: fmt.Sprintf("%s (from %s)", err.Error(), ov.EnvVar),
			}
		}

		// Debug-level logging of applied overrides. Values are omitted
		// to prevent accidental leakage of secrets embedded in URLs,
		// commands, or other string-typed env vars.
		slog.Debug("env override applied",
			slog.String("var", ov.EnvVar))

		if ov.Section == "" {
			// Top-level field.
			raw[ov.Field] = coerced
			envKeys[ov.Field] = true
			continue
		}

		if strings.Contains(ov.Field, ".") {
			// Nested field (e.g. "comments.on_dispatch" under "tracker").
			parent, child, _ := strings.Cut(ov.Field, ".")
			secMap, secAdvisory := ensureSubMap(raw, ov.Section)
			if secAdvisory != nil {
				advisories = append(advisories, *secAdvisory)
			}
			subMap, subAdvisory := ensureSubMap(secMap, parent)
			if subAdvisory != nil {
				advisories = append(advisories, *subAdvisory)
			}
			subMap[child] = coerced
			envKeys[ov.Section+"."+ov.Field] = true
			continue
		}

		// Section-level field.
		secMap, secAdvisory := ensureSubMap(raw, ov.Section)
		if secAdvisory != nil {
			advisories = append(advisories, *secAdvisory)
		}
		secMap[ov.Field] = coerced
		envKeys[ov.Section+"."+ov.Field] = true
	}

	return envKeys, advisories, nil
}

// DotEnvEntries returns the "NAME=value" entries of the .env file at
// the path [applyEnvOverrides] resolves (the path [SetDotEnvPath] set,
// falling back to SORTIE_ENV_FILE), read fresh on every call. It
// returns nil when no path is set, or the file is missing or does not
// parse.
func DotEnvEntries() []string {
	path := getDotEnvPath()
	if path == "" {
		path = os.Getenv("SORTIE_ENV_FILE")
	}
	if path == "" {
		return nil
	}

	pairs, err := parseDotEnv(path)
	if err != nil || len(pairs) == 0 {
		return nil
	}

	entries := make([]string, 0, len(pairs))
	for k, v := range pairs {
		entries = append(entries, k+"="+v)
	}
	return entries
}

// ensureSubMap ensures m[key] is a map[string]any and returns it. If
// the existing value is nil or absent, a fresh empty map is created and
// assigned. If the existing value is a non-map type, it is replaced and
// an advisory records the silent data loss for the operator.
func ensureSubMap(m map[string]any, key string) (map[string]any, *Advisory) {
	existing, ok := m[key]
	var advisory *Advisory
	if ok {
		if v, isMap := existing.(map[string]any); isMap {
			return v, nil
		}
		// Present but wrong type, so record the silent data loss.
		if existing != nil {
			advisory = &Advisory{
				Check:   "env_override.section_replaced",
				Text:    fmt.Sprintf("the %s section is not a mapping; an environment override replaced it with one that holds only the overridden settings", strconv.Quote(key)),
				Message: "env override replaced non-map YAML section",
				Attrs:   []slog.Attr{slog.String("section", key), slog.String("yaml_type", fmt.Sprintf("%T", existing))},
			}
		}
	}
	v := make(map[string]any)
	m[key] = v
	return v, advisory
}

func coerceString(s string) (any, error) {
	return s, nil
}

func coerceEnvInt(s string) (any, error) {
	trimmed := strings.TrimSpace(s)
	n, err := strconv.Atoi(trimmed)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return nil, ErrIntegerOutOfRange
		}
		return nil, fmt.Errorf("invalid integer value: %s", s)
	}
	return n, nil
}

func coerceEnvFloat(s string) (any, error) {
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return nil, fmt.Errorf("invalid number value: %s", s)
	}
	return f, nil
}

func coerceCSVList(s string) (any, error) {
	if s == "" {
		return []any{}, nil
	}
	parts := strings.Split(s, ",")
	elems := make([]any, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			elems = append(elems, trimmed)
		}
	}
	return elems, nil
}

func coerceEnvBool(s string) (any, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "true", "1":
		return true, nil
	case "false", "0":
		return false, nil
	default:
		return nil, fmt.Errorf("invalid boolean value: %s (expected true/false/1/0)", s)
	}
}
