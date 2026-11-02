//go:build unix

package agentcore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sortie-ai/sortie/internal/agent/agenttest"
	"github.com/sortie-ai/sortie/internal/agent/mcpconfig"
	"github.com/sortie-ai/sortie/internal/agent/sshutil"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/registry"
)

const channelGuestPort = 47800

func carrierChannel(t *testing.T) (*agenttest.FakeCarrier, domain.ToolChannel) {
	t.Helper()
	channel := agenttest.NewToolChannel(t)
	return &agenttest.FakeCarrier{GuestWorkspace: "/workspace", Channel: channel}, *channel
}

// getThroughSocket sends one HTTP request over the Unix socket at path, as
// the guest's listener does for the agent, and returns the response.
func getThroughSocket(t *testing.T, path, bearer string) (int, string) {
	t.Helper()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial tool channel: %v", err)
	}
	defer conn.Close() //nolint:errcheck // test cleanup
	_, _ = fmt.Fprintf(conn, "GET /mcp HTTP/1.1\r\nHost: 127.0.0.1:%d\r\nAuthorization: Bearer %s\r\nConnection: close\r\n\r\n", channelGuestPort, bearer)
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response through the tool channel: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck // test cleanup
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func TestToolSession_CarrierServesTheRelayOverItsChannel(t *testing.T) {
	t.Parallel()

	carrier, channel := carrierChannel(t)
	tools := fmt.Sprintf(`"sortie-tools":{"command":%q,"args":["mcp-server"],"env":{"TRACKER_PAT":%q}}`,
		agenttest.FakeRuntime(t, t.TempDir(), "sortie", agenttest.HTTPToolServerScenario, struct{}{}), envBlockSecret)
	config := writeConfig(t, tools, operatorHTTP, operatorStdio)
	target := LaunchTarget{Command: "agent", WorkspacePath: t.TempDir(), Carrier: carrier}
	session := startSession(t, domain.StartSessionParams{MCPConfigPath: config}, target, testDialect)

	servers := declaredServers(t, session.Delivery(sshutil.ForwardOutcome{}).Document)
	if got, want := servers[mcpconfig.ToolServerName].URL, fmt.Sprintf("http://127.0.0.1:%d/mcp", channelGuestPort); got != want {
		t.Errorf("sortie-tools url = %q, want %q: the agent reaches the relay at the channel's guest port", got, want)
	}
	if _, ok := servers["operator-stdio"]; ok {
		t.Error("operator stdio server delivered into the environment, want it withheld")
	}
	if _, ok := servers["operator-http"]; !ok {
		t.Error("operator HTTP server withheld, want it delivered")
	}

	launch := session.Launch(placeDocument)
	if len(launch.Args) != 2 || launch.Args[0] != "--mcp" || !strings.Contains(launch.Args[1], fmt.Sprint(channelGuestPort)) {
		t.Errorf("Launch().Args = %q, want the placed document naming the guest port", launch.Args)
	}
	var bearer string
	for _, secret := range launch.Secrets {
		if secret.Name == mcpconfig.BearerEnv {
			bearer = secret.Value
		}
	}
	if bearer == "" {
		t.Fatalf("Launch().Secrets = %v, want the bearer", launch.Secrets)
	}
	if launch.Forward != nil {
		t.Error("Launch().Forward set on a carrier launch, want none")
	}
	if strings.Contains(strings.Join(launch.Args, " "), bearer) {
		t.Error("the bearer value is in the launch arguments")
	}

	if status, body := getThroughSocket(t, channel.HostSocket, bearer); status != http.StatusOK || body != agenttest.HTTPToolServerReply {
		t.Errorf("request through the channel = %d %q, want 200 %q", status, body, agenttest.HTTPToolServerReply)
	}
	if status, _ := getThroughSocket(t, channel.HostSocket, "wrong"); status != http.StatusUnauthorized {
		t.Errorf("request with another bearer = %d, want 401: the channel carries bytes, the relay authorizes", status)
	}

	if err := session.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	if _, err := os.Lstat(channel.HostSocket); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Lstat(socket) after Close() = %v, want the socket removed", err)
	}
}

// A channel whose environment authorizes the agent's requests itself is
// handed the relay's bearer once it opens, and that bearer is the one the
// relay accepts through the channel.
func TestToolSession_CarrierChannelAuthorizeReceivesTheBearer(t *testing.T) {
	t.Parallel()

	channel := agenttest.NewToolChannel(t)
	var handed []string
	channel.Authorize = func(bearer string) { handed = append(handed, bearer) }
	carrier := &agenttest.FakeCarrier{GuestWorkspace: "/workspace", Channel: channel}
	tools := fmt.Sprintf(`"sortie-tools":{"command":%q,"args":["mcp-server"]}`,
		agenttest.FakeRuntime(t, t.TempDir(), "sortie", agenttest.HTTPToolServerScenario, struct{}{}))
	target := LaunchTarget{Command: "agent", WorkspacePath: t.TempDir(), Carrier: carrier}
	session := startSession(t, domain.StartSessionParams{MCPConfigPath: writeConfig(t, tools)}, target, testDialect)

	if len(handed) != 1 || handed[0] == "" {
		t.Fatalf("Authorize received %d bearers, want the relay's one", len(handed))
	}
	var launched string
	for _, secret := range session.Launch(placeDocument).Secrets {
		if secret.Name == mcpconfig.BearerEnv {
			launched = secret.Value
		}
	}
	if launched != handed[0] {
		t.Error("Authorize received another bearer than the session delivers")
	}
	if status, body := getThroughSocket(t, channel.HostSocket, handed[0]); status != http.StatusOK || body != agenttest.HTTPToolServerReply {
		t.Errorf("request with the handed bearer = %d %q, want 200 %q", status, body, agenttest.HTTPToolServerReply)
	}
}

func TestToolSession_CarrierWithoutChannelDeclaresTheGap(t *testing.T) {
	t.Parallel()

	carrier := &agenttest.FakeCarrier{GuestWorkspace: "/workspace"}
	tools := fmt.Sprintf(`"sortie-tools":{"command":%q,"args":["mcp-server"]}`,
		agenttest.FakeRuntime(t, t.TempDir(), "sortie", agenttest.HTTPToolServerScenario, struct{}{}))
	target := LaunchTarget{Command: "agent", WorkspacePath: t.TempDir(), Carrier: carrier}
	session := startSession(t, domain.StartSessionParams{MCPConfigPath: writeConfig(t, tools)}, target, registry.MCPDialectMCPServersJSONAlwaysLoad)

	if _, ok := declaredServers(t, session.Delivery(sshutil.ForwardOutcome{}).Document)[mcpconfig.ToolServerName]; ok {
		t.Error("sortie-tools delivered without a tool channel, want it left out")
	}
	if !session.NoToolChannel() {
		t.Error("NoToolChannel() = false without a tool channel, want the gap declared")
	}
	if session.PlacesLate() || len(session.Launch(placeDocument).Secrets) != 0 {
		t.Error("a launch without a tool channel carries tool secrets, want none")
	}
}

// A carrier session runs its agent only through the carrier: settling the
// tool server must not start the agent's command on Sortie's host.
func TestToolSession_CarrierRunsNothingOnTheHost(t *testing.T) {
	t.Parallel()

	carrier, _ := carrierChannel(t)
	tools := fmt.Sprintf(`"sortie-tools":{"command":%q,"args":["mcp-server"]}`,
		agenttest.FakeRuntime(t, t.TempDir(), "sortie", agenttest.HTTPToolServerScenario, struct{}{}))
	dir := t.TempDir()
	ran := filepath.Join(dir, "ran")
	agent := filepath.Join(dir, "agent")
	if err := os.WriteFile(agent, []byte("#!/bin/sh\necho \"$@\" >> "+ran+"\n"), 0o700); err != nil { //nolint:gosec // G306: the test's own script
		t.Fatalf("write agent: %v", err)
	}
	target := LaunchTarget{Command: agent, WorkspacePath: t.TempDir(), Carrier: carrier}
	startSession(t, domain.StartSessionParams{MCPConfigPath: writeConfig(t, tools)}, target, registry.MCPDialectMCPServersJSONAlwaysLoad)

	if got, err := os.ReadFile(ran); !errors.Is(err, fs.ErrNotExist) { //nolint:gosec // G304: the test's own temp file
		t.Errorf("the agent's command ran on the host with %q, want it never started there", got)
	}
}

// A socket an earlier session left behind is replaced; anything else at
// the channel's path is never removed.
func TestListenToolChannel_ReplacesOnlyASocket(t *testing.T) {
	t.Parallel()

	_, channel := carrierChannel(t)
	var lc net.ListenConfig
	stale, err := lc.Listen(t.Context(), "unix", channel.HostSocket)
	if err != nil {
		t.Fatal(err)
	}
	stale.(*net.UnixListener).SetUnlinkOnClose(false)
	_ = stale.Close()
	c, err := listenToolChannel(t.Context(), channel.HostSocket, channelGuestPort, 1, discardLogger)
	if err != nil {
		t.Fatalf("listenToolChannel() over a stale socket = %v, want it replaced", err)
	}
	c.close()

	if err := os.WriteFile(channel.HostSocket, []byte("not a socket"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := listenToolChannel(t.Context(), channel.HostSocket, channelGuestPort, 1, discardLogger); err == nil {
		t.Error("listenToolChannel() over a regular file = nil, want a refusal")
	}
	if data, err := os.ReadFile(channel.HostSocket); err != nil || string(data) != "not a socket" {
		t.Errorf("file after the refusal = %q, %v, want it untouched", data, err)
	}
}
