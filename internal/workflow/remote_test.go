package workflow

import (
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
)

// remoteWorkflow is a remote-kind workflow polling every intervalMS,
// with extraRemote appended under environment.remote and extraTop
// appended at the top level of the front matter.
func remoteWorkflow(intervalMS int, extraRemote, extraTop string) []byte {
	var b strings.Builder
	b.WriteString("---\npolling:\n  interval_ms: ")
	b.WriteString(strconv.Itoa(intervalMS))
	b.WriteString("\nenvironment:\n  kind: remote\n  remote:\n    image: ghcr.io/ortusnova/agent:1\n    flows:\n")
	for _, f := range config.RemoteFlows {
		b.WriteString("      " + f + ": { url: https://edge.example/" + f + " }\n")
	}
	b.WriteString("    repositories:\n")
	b.WriteString("      - { name: homelab, url: https://example.com/o/homelab.git, base: main, image: ghcr.io/ortusnova/homelab:1, requires: { capabilities: [nested] } }\n")
	b.WriteString("      - { name: sortie, url: https://example.com/o/sortie.git, base: main }\n")
	b.WriteString("    default_repository: sortie\n")
	b.WriteString(extraRemote)
	b.WriteString(extraTop)
	b.WriteString("---\nDo the task for {{ .issue.title }}.\n")
	return []byte(b.String())
}

// remote-runs: A key the remote kind does not run. A workflow that sets
// environment.remote.pass_env, or a hook, is refused at start and at
// reload, naming the key, and a reload keeps the previous workflow.
func TestManager_RemoteRefusesUnsupportedKeys(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		extraRemote, extraTop, key string
	}{
		"pass_env":     {"    pass_env: [HOME]\n", "", "environment.remote.pass_env"},
		"after_create": {"", "hooks:\n  after_create: make setup\n", "hooks.after_create"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "WORKFLOW.md")
			mustWriteFile(t, path, remoteWorkflow(5000, tc.extraRemote, tc.extraTop))
			if _, err := NewManager(path, testLogger()); err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("NewManager() error = %v, want one naming %s", err, tc.key)
			}

			mustWriteFile(t, path, remoteWorkflow(5000, "", ""))
			mgr, err := NewManager(path, testLogger())
			if err != nil {
				t.Fatalf("NewManager(valid remote workflow) error = %v", err)
			}
			if names := mgr.Config().Environment.RepositorySet().RepositoryNames(); len(names) != 2 {
				t.Fatalf("repositories = %v, want homelab and sortie", names)
			}

			mustWriteFile(t, path, remoteWorkflow(9000, tc.extraRemote, tc.extraTop))
			if err := mgr.Reload(); err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("Reload() error = %v, want one naming %s", err, tc.key)
			}
			if err := mgr.LastLoadError(); err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Errorf("LastLoadError() = %v, want one naming %s", err, tc.key)
			}
			cfg := mgr.Config()
			if cfg.Polling.IntervalMS != 5000 || cfg.Hooks.AfterCreate != "" {
				t.Errorf("after the refused reload: interval %d, after_create %q, want the previous workflow", cfg.Polling.IntervalMS, cfg.Hooks.AfterCreate)
			}
		})
	}
}
