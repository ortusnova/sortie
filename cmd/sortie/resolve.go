package main

import (
	"fmt"
	"log/slog"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/logging"
)

func resolveWorkflowPath(args []string) (string, error) {
	if len(args) > 1 {
		return "", fmt.Errorf("too many arguments")
	}
	raw := "./WORKFLOW.md"
	if len(args) == 1 {
		raw = args[0]
	}
	return filepath.Abs(raw)
}

// trackerConfigMap converts typed [config.TrackerConfig] fields into
// the raw map expected by [registry.TrackerConstructor]. Adapter
// packages extract their required fields from this map.
func trackerConfigMap(tc config.TrackerConfig) map[string]any {
	return map[string]any{
		"kind":              tc.Kind,
		"endpoint":          tc.Endpoint,
		"api_key":           tc.APIKey,
		"project":           tc.Project,
		"active_states":     tc.ActiveStates,
		"terminal_states":   tc.TerminalStates,
		"query_filter":      tc.QueryFilter,
		"handoff_state":     tc.HandoffState,
		"handoff_evidence":  string(tc.HandoffEvidence.Effective()),
		"no_change_state":   tc.NoChangeState,
		"in_progress_state": tc.InProgressState,
		"release_state":     tc.ReleaseState,
		"backlog_state":     tc.BacklogState,
		"cancel_state":      tc.CancelState,
		"done_state":        tc.DoneState,
		"api_version":       tc.APIVersion,
		"comments": map[string]any{
			"on_dispatch":   tc.Comments.OnDispatch,
			"on_completion": tc.Comments.OnCompletion,
			"on_failure":    tc.Comments.OnFailure,
		},
	}
}

// mergeTrackerCredentials copies api_key, project, and endpoint from
// the tracker config into dst when the corresponding key is absent.
// Called only when the CI provider kind matches the tracker kind so
// that shared-platform credentials flow to the CI adapter without
// operator duplication.
func mergeTrackerCredentials(dst map[string]any, tc config.TrackerConfig) {
	if _, ok := dst["api_key"]; !ok && tc.APIKey != "" {
		dst["api_key"] = tc.APIKey
	}
	if _, ok := dst["project"]; !ok && tc.Project != "" {
		dst["project"] = tc.Project
	}
	if _, ok := dst["endpoint"]; !ok && tc.Endpoint != "" {
		dst["endpoint"] = tc.Endpoint
	}
}

// resolveDBPath returns the effective database file path. An absolute
// cfgPath is used as-is. A relative cfgPath is joined with workflowDir
// so database location is deterministic regardless of the process CWD.
// An empty cfgPath falls back to .sortie.db inside workflowDir.
func resolveDBPath(cfgPath, workflowDir string) string {
	if cfgPath == "" {
		return filepath.Join(workflowDir, ".sortie.db")
	}
	if filepath.IsAbs(cfgPath) {
		return cfgPath
	}
	return filepath.Join(workflowDir, cfgPath)
}

// resolveServerPort determines the effective HTTP server port from the
// CLI flag and the server extension section. Returns the port,
// whether the server should be started, and an error if an explicitly
// configured port is invalid. When no port is configured, the default
// port is returned with the server enabled. Port 0 disables the
// server.
func resolveServerPort(portFlag int, portFlagSet bool, serverSection map[string]any) (int, bool, error) {
	if portFlagSet {
		if portFlag < 0 || portFlag > 65535 {
			return 0, false, fmt.Errorf("invalid --port value %d: must be between 0 and 65535", portFlag)
		}
		return portFlag, portFlag != 0, nil
	}

	if serverSection == nil {
		return defaultServerPort, true, nil
	}

	portVal, exists := serverSection["port"]
	if !exists {
		return defaultServerPort, true, nil
	}

	var port int
	switch v := portVal.(type) {
	case int:
		port = v
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) || v != math.Trunc(v) {
			return 0, false, fmt.Errorf("invalid server.port value %v: must be an integer", v)
		}
		parsed, ok := config.IntFromNumber(v)
		if !ok {
			return 0, false, fmt.Errorf("invalid server.port value: must be between 0 and 65535")
		}
		port = parsed
	case int64, uint64:
		parsed, ok := config.IntFromNumber(v)
		if !ok {
			return 0, false, fmt.Errorf("invalid server.port value: must be between 0 and 65535")
		}
		port = parsed
	default:
		return 0, false, fmt.Errorf("invalid server.port value: unsupported type %T, must be an integer", portVal)
	}

	if port < 0 || port > 65535 {
		return 0, false, fmt.Errorf("invalid server.port value %d: must be between 0 and 65535", port)
	}
	return port, port != 0, nil
}

// resolveServerHost determines the effective HTTP server bind address
// from the CLI flag and the server extension section. Returns an
// error if the resolved value is not a parseable IP address. When no
// host is configured, the loopback address is returned.
func resolveServerHost(hostFlag string, hostFlagSet bool, serverSection map[string]any) (string, error) {
	if hostFlagSet {
		if net.ParseIP(hostFlag) == nil {
			return "", fmt.Errorf("invalid --host value %q: not a valid IP address", hostFlag)
		}
		return hostFlag, nil
	}

	if serverSection == nil {
		return defaultServerHost, nil
	}

	hostVal, exists := serverSection["host"]
	if !exists {
		return defaultServerHost, nil
	}

	hostStr, ok := hostVal.(string)
	if !ok {
		return "", fmt.Errorf("invalid server.host value: unsupported type %T, must be a string", hostVal)
	}

	if net.ParseIP(hostStr) == nil {
		return "", fmt.Errorf("invalid server.host value %q: not a valid IP address", hostStr)
	}
	return hostStr, nil
}

// controlTokenSetting names the server setting that holds the token
// every state-changing HTTP endpoint requires.
const controlTokenSetting = "server.control_token"

// resolveControlToken returns server.control_token from the server
// extension section, whose $VAR references the configuration load has
// already resolved and whose value it has already registered for
// masking. An absent or blank value returns the empty string.
func resolveControlToken(serverSection map[string]any) (string, error) {
	raw, exists := serverSection["control_token"]
	if !exists || raw == nil {
		return "", nil
	}
	token, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("invalid %s value: unsupported type %T, must be a string", controlTokenSetting, raw)
	}
	return strings.TrimSpace(token), nil
}

// Server settings of the supervisor token.
const (
	supervisorTokenSetting = "server.supervisor_token"
	supervisorCapSetting   = "server.supervisor_max_sessions"

	// defaultSupervisorSessionCap is the sessions granted to one issue in
	// total that a supervisor grant may reach when
	// server.supervisor_max_sessions is unset.
	defaultSupervisorSessionCap = 4
)

// resolveSupervisor returns server.supervisor_token and
// server.supervisor_max_sessions from the server extension section. An
// absent or blank token returns the empty string. The token needs a
// control token, because without one every control route admits a request
// that carries no token, and must differ from it; the cap must be a whole
// number from 0.
func resolveSupervisor(serverSection map[string]any, controlToken string) (token string, sessionCap int, err error) {
	if raw, exists := serverSection["supervisor_token"]; exists && raw != nil {
		s, ok := raw.(string)
		if !ok {
			return "", 0, fmt.Errorf("invalid %s value: unsupported type %T, must be a string", supervisorTokenSetting, raw)
		}
		token = strings.TrimSpace(s)
	}
	if token != "" && controlToken == "" {
		return "", 0, fmt.Errorf("%s needs %s: without it every control route admits a request with no token", supervisorTokenSetting, controlTokenSetting)
	}
	if token != "" && token == controlToken {
		return "", 0, fmt.Errorf("%s must differ from %s", supervisorTokenSetting, controlTokenSetting)
	}
	sessionCap = defaultSupervisorSessionCap
	if raw, exists := serverSection["supervisor_max_sessions"]; exists && raw != nil {
		n, ok := config.IntFromNumber(raw)
		sessionCap = n
		if !ok || sessionCap < 0 {
			return "", 0, fmt.Errorf("invalid %s value %v: must be a whole number from 0", supervisorCapSetting, raw)
		}
	}
	return token, sessionCap, nil
}

// checkSupervisorSection refuses a supervisor section without a
// supervisor token, which the supervisor session acts with.
func checkSupervisorSection(sc config.SupervisorConfig, supervisorToken string) error {
	if sc.Enabled() && supervisorToken == "" {
		return fmt.Errorf("the supervisor section needs %s: without it the supervisor session could not act", supervisorTokenSetting)
	}
	return nil
}

// checkControlBinding refuses a non-loopback bind address without a
// control token, because every state-changing endpoint would then be
// open to the network.
func checkControlBinding(host, controlToken string) error {
	if controlToken != "" {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf("server.host %q is not a loopback address and %s is not set; set %s or bind to 127.0.0.1", host, controlTokenSetting, controlTokenSetting)
}

// unsetControlTokenEnv removes from the process environment every
// variable whose value is the control token, so no agent, hook, or tool
// server Sortie starts inherits it, and returns the names it removed.
// The token itself was read when the configuration loaded.
func unsetControlTokenEnv(controlToken string) []string {
	if controlToken == "" {
		return nil
	}
	var removed []string
	for _, entry := range os.Environ() {
		name, value, ok := strings.Cut(entry, "=")
		if !ok || name == "" || strings.TrimSpace(value) != controlToken {
			continue
		}
		if err := os.Unsetenv(name); err == nil {
			removed = append(removed, name)
		}
	}
	return removed
}

// serverBaseURL returns the base URL a local client uses to reach the
// HTTP server listening on addr. An unspecified bind address, which
// accepts connections on every interface, is reached through the
// loopback address of its family.
func serverBaseURL(addr net.Addr) string {
	tcp, ok := addr.(*net.TCPAddr)
	if !ok {
		return "http://" + addr.String()
	}
	ip := tcp.IP
	switch {
	case ip == nil || (ip.IsUnspecified() && ip.To4() != nil):
		ip = net.IPv4(127, 0, 0, 1)
	case ip.IsUnspecified():
		ip = net.IPv6loopback
	}
	return "http://" + net.JoinHostPort(ip.String(), strconv.Itoa(tcp.Port))
}

// hasServerPortExtension reports whether the server extension section
// contains a port key. The check is presence-based: even server.port
// matching the default counts as explicit configuration.
func hasServerPortExtension(serverSection map[string]any) bool {
	if serverSection == nil {
		return false
	}
	_, exists := serverSection["port"]
	return exists
}

// resolveLogLevel determines the effective log level from the CLI flag
// and the logging extension section. Precedence: CLI flag >
// logging.level extension > default (info).
func resolveLogLevel(flagValue string, flagSet bool, loggingSection map[string]any) (slog.Level, error) {
	if flagSet {
		return logging.ParseLevel(flagValue)
	}

	if loggingSection == nil {
		return slog.LevelInfo, nil
	}

	rawLevel, ok := loggingSection["level"]
	if !ok || rawLevel == nil {
		return slog.LevelInfo, nil
	}

	levelStr, ok := rawLevel.(string)
	if !ok {
		return 0, fmt.Errorf("invalid logging.level: expected string, got %T", rawLevel)
	}

	return logging.ParseLevel(levelStr)
}

// resolveLogFormat determines the effective log output format from the
// CLI flag and the logging extension section. Precedence: CLI flag >
// logging.format extension > default (text). All map and type
// accesses use the comma-ok idiom to avoid panics on unexpected
// extension shapes.
func resolveLogFormat(flagValue string, flagSet bool, loggingSection map[string]any) (logging.Format, error) {
	if flagSet {
		return logging.ParseFormat(flagValue)
	}

	if loggingSection == nil {
		return logging.FormatText, nil
	}

	formatRaw, ok := loggingSection["format"]
	if !ok {
		return logging.FormatText, nil
	}

	formatStr, ok := formatRaw.(string)
	if !ok {
		return "", fmt.Errorf("invalid logging.format: expected string, got %T", formatRaw)
	}

	return logging.ParseFormat(formatStr)
}
