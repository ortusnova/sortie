package orchestrator

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	"github.com/sortie-ai/sortie/internal/agent/mcpconfig"
	"github.com/sortie-ai/sortie/internal/dataplane"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/tool/mcpserver"
)

// fakeAgentCommand makes the test binary run [fakeAgent] as a process:
// `<test binary> fake-agent <workspace> <prompts file>`.
const fakeAgentCommand = "fake-agent"

// TestMain runs the test binary as the stdio bridge, as a fake agent, or
// as a session's tool server when a test launches it as one, and runs
// the tests otherwise.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case dataplane.BridgeCommand:
			os.Exit(dataplane.RunBridge(os.Args[2], os.Stdin, os.Stdout, os.Stderr))
		case fakeAgentCommand:
			os.Exit(runFakeAgentProcess(os.Args[2], os.Args[3]))
		case toolServerCommand:
			os.Exit(runStandInToolServer(os.Args[2:]))
		}
	}
	os.Exit(m.Run())
}

// fakeAgentLine is one line of the fake agent's protocol: the prompt of
// a turn in, and a message, a usage report, or the turn's end out.
type fakeAgentLine struct {
	Prompt  string          `json:"prompt,omitempty"`
	Message string          `json:"message,omitempty"`
	Usage   *fakeAgentUsage `json:"usage,omitempty"`
	Done    bool            `json:"done,omitempty"`
}

type fakeAgentUsage struct {
	Input  int64 `json:"input"`
	Output int64 `json:"output"`
}

// runFakeAgentProcess is the fake agent run on this host: it reports
// usage on its standard output and appends every prompt to prompts.
func runFakeAgentProcess(dir, prompts string) int {
	out := json.NewEncoder(os.Stdout)
	err := fakeAgent(os.Stdin, os.Stdout, dir, func(u fakeAgentUsage) error {
		return out.Encode(fakeAgentLine{Usage: &u})
	}, func(p string) error {
		f, err := os.OpenFile(prompts, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600) //nolint:gosec // G304: the test's own file
		if err != nil {
			return err
		}
		defer f.Close() //nolint:errcheck // checked by the encode
		return json.NewEncoder(f).Encode(p)
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "fake agent:", err) //nolint:errcheck // test diagnostics
		return 1
	}
	return 0
}

// fakeAgent answers each prompt read from in as a turn. On the first
// turn it commits two changes to the clone at dir, suggests the pull
// request in .sortie/pr.json, and reports 12000 input and 3000 output
// tokens through usage; every turn then ends. It returns when in ends.
func fakeAgent(in io.Reader, out io.Writer, dir string, usage func(fakeAgentUsage) error, record func(string) error) error {
	enc := json.NewEncoder(out)
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 0, 64<<10), 4<<20)
	for turn := 1; scanner.Scan(); turn++ {
		var line fakeAgentLine
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			return err
		}
		if err := record(line.Prompt); err != nil {
			return err
		}
		if turn == 1 {
			for _, name := range []string{"one.txt", "two.txt"} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(name+"\n"), 0o600); err != nil {
					return err
				}
				if err := agentGit(dir, "add", "--", name); err != nil {
					return err
				}
				if err := agentGit(dir, "commit", "--quiet", "-m", "add "+name); err != nil {
					return err
				}
			}
			if err := os.MkdirAll(filepath.Join(dir, ".sortie"), 0o750); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(dir, ".sortie", "pr.json"), []byte(`{"title":"Add one and two","body":"Two changes."}`), 0o600); err != nil {
				return err
			}
			if err := enc.Encode(fakeAgentLine{Message: "committed two changes"}); err != nil {
				return err
			}
			if err := usage(fakeAgentUsage{Input: 12000, Output: 3000}); err != nil {
				return err
			}
		}
		if err := enc.Encode(fakeAgentLine{Done: true}); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// agentGit runs git in dir as the agent, with a fixed identity and
// clock, so the same work makes the same commits wherever it runs.
func agentGit(dir string, args ...string) error {
	cmd := exec.Command("git", args...) //nolint:gosec // G204: the test agent's own git
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_CONFIG_NOSYSTEM=1",
		"GIT_AUTHOR_NAME=Agent", "GIT_AUTHOR_EMAIL=agent@example.com", "GIT_AUTHOR_DATE=2026-11-01T12:00:00Z",
		"GIT_COMMITTER_NAME=Agent", "GIT_COMMITTER_EMAIL=agent@example.com", "GIT_COMMITTER_DATE=2026-11-01T12:00:00Z",
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("git %v: %w: %s", args, err, out)
	}
	return nil
}

// toolServerCommand makes the test binary stand in for `sortie
// mcp-server`, the command a generated MCP configuration names.
const toolServerCommand = "mcp-server"

// sessionIdentityTool is the stand-in tool server's one tool. It answers
// with the issue of the session whose tool server runs it, read from the
// server's environment as Sortie's own tools read it.
type sessionIdentityTool struct{}

const sessionIdentityToolName = "session_identity"

func (sessionIdentityTool) Name() string                 { return sessionIdentityToolName }
func (sessionIdentityTool) Description() string          { return "Returns the session's issue." }
func (sessionIdentityTool) InputSchema() json.RawMessage { return json.RawMessage(`{"type":"object"}`) }

func (sessionIdentityTool) Execute(context.Context, json.RawMessage) (json.RawMessage, error) {
	return json.Marshal(map[string]string{
		"issue_id": os.Getenv("SORTIE_ISSUE_ID"), "issue_identifier": os.Getenv("SORTIE_ISSUE_IDENTIFIER"),
	})
}

// runStandInToolServer serves [sessionIdentityTool] as `sortie mcp-server
// --http` serves Sortie's tools: on a loopback port it announces, behind
// the bearer the relay hands it, until its standard input closes.
func runStandInToolServer(args []string) int {
	if !slices.Contains(args, "--http") {
		fmt.Fprintln(os.Stderr, "stand-in tool server: --http is required") //nolint:errcheck // test diagnostics
		return 2
	}
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, "stand-in tool server:", err) //nolint:errcheck // test diagnostics
		return 1
	}
	if err := json.NewEncoder(os.Stdout).Encode(mcpserver.HTTPEvent{Listening: ln.Addr().String()}); err != nil {
		return 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		cancel()
	}()
	registry := domain.NewToolRegistry()
	registry.Register(sessionIdentityTool{})
	err = mcpserver.NewServer(registry, nil, "test").ServeStreamableHTTP(ctx, ln,
		mcpserver.HTTPOptions{Bearer: os.Getenv(mcpconfig.BearerEnv), Events: os.Stdout})
	if err != nil {
		fmt.Fprintln(os.Stderr, "stand-in tool server:", err) //nolint:errcheck // test diagnostics
		return 1
	}
	return 0
}
