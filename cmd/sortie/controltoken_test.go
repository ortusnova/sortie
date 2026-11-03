package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/redact"
	"github.com/sortie-ai/sortie/internal/waittest"
)

func TestResolveControlToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		section map[string]any
		want    string
		wantErr bool
	}{
		{name: "no server section", section: nil, want: ""},
		{name: "unset", section: map[string]any{"port": 8080}, want: ""},
		{name: "set", section: map[string]any{"control_token": " operator-token-123 "}, want: "operator-token-123"},
		{name: "blank", section: map[string]any{"control_token": "   "}, want: ""},
		{name: "not a string", section: map[string]any{"control_token": 42}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := resolveControlToken(tt.section)
			if (err != nil) != tt.wantErr {
				t.Fatalf("resolveControlToken() err = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), "server.control_token") {
				t.Errorf("resolveControlToken() err = %v, want it to name server.control_token", err)
			}
			if got != tt.want {
				t.Errorf("resolveControlToken() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCheckControlBinding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		host, token string
		wantErr     bool
	}{
		{host: "127.0.0.1", token: "", wantErr: false},
		{host: "::1", token: "", wantErr: false},
		{host: "127.0.0.2", token: "", wantErr: false},
		{host: "0.0.0.0", token: "", wantErr: true},
		{host: "::", token: "", wantErr: true},
		{host: "192.168.1.10", token: "", wantErr: true},
		{host: "0.0.0.0", token: "operator-token-123", wantErr: false},
	}
	for _, tt := range tests {
		err := checkControlBinding(tt.host, tt.token)
		if (err != nil) != tt.wantErr {
			t.Errorf("checkControlBinding(%q, token set %v) err = %v, wantErr %v", tt.host, tt.token != "", err, tt.wantErr)
		}
		if err != nil && !strings.Contains(err.Error(), "server.control_token") {
			t.Errorf("checkControlBinding(%q) err = %v, want it to name server.control_token", tt.host, err)
		}
	}
}

// TestControlToken_ResolvesEnvAndIsMasked loads a configuration whose
// control token is a $VAR reference and asserts that the resolved value
// is what the server reads and that the value is masked.
func TestControlToken_ResolvesEnvAndIsMasked(t *testing.T) {
	// Not parallel: t.Setenv.
	const secret = "control-token-from-env-6b1f0e"
	t.Setenv("SORTIE_TEST_CONTROL_TOKEN", secret)

	cfg, err := config.NewServiceConfig(map[string]any{
		"tracker": map[string]any{"kind": "file", "api_key": "unused"},
		"server":  map[string]any{"control_token": "$SORTIE_TEST_CONTROL_TOKEN"},
	})
	if err != nil {
		t.Fatalf("NewServiceConfig() error = %v", err)
	}
	got, err := resolveControlToken(cfg.ExtensionSection("server"))
	if err != nil || got != secret {
		t.Fatalf("resolveControlToken() = %q, %v, want %q", got, err, secret)
	}
	if masked := redact.Mask("token=" + secret); strings.Contains(masked, secret) {
		t.Errorf("redact.Mask() = %q, want the control token masked", masked)
	}
}

func writeWorkflowWithServer(t *testing.T, dir, serverBlock string) string {
	t.Helper()
	content := strings.Replace(string(minimalWorkflow()), "---\n", "---\nserver:\n"+serverBlock, 1)
	p := filepath.Join(dir, "WORKFLOW.md")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestRunNonLoopbackHostWithoutControlTokenRefused(t *testing.T) {
	// No t.Parallel: setupRunDir calls t.Chdir.
	wfPath := setupRunDir(t)

	var stdout, stderr bytes.Buffer
	code := run(waittest.Context(t), []string{"--host", "0.0.0.0", "--port", strconv.Itoa(freePort(t)), wfPath}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr: %s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "server.control_token") {
		t.Errorf("stderr = %q, want it to name server.control_token", stderr.String())
	}
}

func TestRunNonLoopbackHostWithControlTokenStarts(t *testing.T) {
	// No t.Parallel: setupRunDir calls t.Chdir.
	setupRunDir(t)
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	wfPath := writeWorkflowWithServer(t, dir, "  control_token: operator-token-123\n")
	port := freePort(t)

	var stdout, stderr bytes.Buffer
	code := runUntilStarted(t, []string{"--host", "0.0.0.0", "--port", strconv.Itoa(port), wfPath}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr.String())
	}
	if strings.Contains(stderr.String(), "operator-token-123") {
		t.Errorf("stderr carries the control token unmasked: %s", stderr.String())
	}
}

func TestUnsetControlTokenEnv(t *testing.T) {
	// Not parallel: t.Setenv.
	const token = "control-token-in-env-0a9c"
	t.Setenv("SORTIE_TEST_CONTROL_TOKEN_A", token)
	t.Setenv("SORTIE_TEST_UNRELATED", "unrelated-value")

	removed := unsetControlTokenEnv(token)

	if _, ok := os.LookupEnv("SORTIE_TEST_CONTROL_TOKEN_A"); ok {
		t.Error("variable holding the control token is still set")
	}
	if os.Getenv("SORTIE_TEST_UNRELATED") != "unrelated-value" {
		t.Error("unrelated variable was changed")
	}
	if len(removed) != 1 || removed[0] != "SORTIE_TEST_CONTROL_TOKEN_A" {
		t.Errorf("removed = %v, want [SORTIE_TEST_CONTROL_TOKEN_A]", removed)
	}
	if got := unsetControlTokenEnv(""); got != nil {
		t.Errorf("unsetControlTokenEnv(\"\") = %v, want nil", got)
	}
}

func TestResolveSupervisor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		section   map[string]any
		wantToken string
		wantCap   int
		wantErr   string
	}{
		{name: "unset", section: nil, wantCap: defaultSupervisorSessionCap},
		{name: "set", section: map[string]any{"supervisor_token": " sup-1 ", "supervisor_max_sessions": 6}, wantToken: "sup-1", wantCap: 6},
		{name: "a zero cap", section: map[string]any{"supervisor_token": "sup-1", "supervisor_max_sessions": float64(0)}, wantToken: "sup-1"},
		{name: "the control token", section: map[string]any{"supervisor_token": "operator-token-123"}, wantErr: "server.supervisor_token"},
		{name: "not a string", section: map[string]any{"supervisor_token": 7}, wantErr: "server.supervisor_token"},
		{name: "a negative cap", section: map[string]any{"supervisor_max_sessions": -1}, wantErr: "server.supervisor_max_sessions"},
		{name: "a fractional cap", section: map[string]any{"supervisor_max_sessions": 1.5}, wantErr: "server.supervisor_max_sessions"},
		{name: "a text cap", section: map[string]any{"supervisor_max_sessions": "4"}, wantErr: "server.supervisor_max_sessions"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			token, sessionCap, err := resolveSupervisor(tt.section, "operator-token-123")
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("resolveSupervisor() err = %v, want one naming %s", err, tt.wantErr)
				}
				return
			}
			if err != nil || token != tt.wantToken || sessionCap != tt.wantCap {
				t.Errorf("resolveSupervisor() = %q, %d, %v, want %q, %d", token, sessionCap, err, tt.wantToken, tt.wantCap)
			}
		})
	}
}

// A supervisor token without a control token would scope nothing: every
// control route admits a request that carries no token.
func TestResolveSupervisor_NeedsAControlToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		section map[string]any
		wantErr string
	}{
		{name: "a supervisor token", section: map[string]any{"supervisor_token": "sup-1"}, wantErr: "server.control_token"},
		{name: "no supervisor token", section: map[string]any{"supervisor_max_sessions": 2}},
		{name: "a blank supervisor token", section: map[string]any{"supervisor_token": "  "}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			token, _, err := resolveSupervisor(tt.section, "")
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("resolveSupervisor() err = %v, want one naming %s", err, tt.wantErr)
				}
				return
			}
			if err != nil || token != "" {
				t.Errorf("resolveSupervisor() = %q, %v, want no token and no error", token, err)
			}
		})
	}
}
