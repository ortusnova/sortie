//go:build unix

package orchestrator

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/agent/agentcore"
	"github.com/sortie-ai/sortie/internal/agent/sshutil"
	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/dataplane"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/environment"
	"github.com/sortie-ai/sortie/internal/jobtoken"
	"github.com/sortie-ai/sortie/internal/prompt"
	"github.com/sortie-ai/sortie/internal/registry"
	"github.com/sortie-ai/sortie/internal/waittest"
	"github.com/sortie-ai/sortie/internal/worker"
)

// lineAdapter is an agent adapter that drives a real agent process over
// its standard streams with the fake agent's line protocol. It launches
// the session through the carrier, behind the host prefix, as the
// process-based adapters do, and records what it launched and sent.
type lineAdapter struct {
	mu        sync.Mutex
	launches  [][]string
	prompts   []string
	delivered []string
}

type lineSession struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	lines  chan []byte
	stderr *bytes.Buffer
	exited chan struct{}
	tools  *agentcore.ToolSession
	// readErr is why reading the agent's output stopped, set before lines closes.
	readErr error
}

var _ domain.AgentAdapter = (*lineAdapter)(nil)

func (a *lineAdapter) StartSession(ctx context.Context, params domain.StartSessionParams) (domain.Session, error) {
	if params.CredentialVerification {
		return domain.Session{ID: "line-verify"}, nil
	}
	if params.Carrier == nil {
		return domain.Session{}, errors.New("lineAdapter needs a carrier")
	}
	// The session's tool server is delivered as a process-based adapter
	// delivers it through a carrier.
	var tools *agentcore.ToolSession
	if params.MCPConfigPath != "" {
		resolved, agentErr := agentcore.ResolveToolDelivery(ctx, params,
			agentcore.LaunchTarget{Command: "fake-agent", WorkspacePath: params.WorkspacePath, Carrier: params.Carrier},
			registry.MCPDialectMCPServersJSON, testRunLogger())
		if agentErr != nil {
			return domain.Session{}, agentErr
		}
		tools = resolved
		a.mu.Lock()
		a.delivered = append(a.delivered, string(tools.Delivery(sshutil.ForwardOutcome{}).Document))
		a.mu.Unlock()
	}
	closeTools := func() {
		if tools != nil {
			_ = tools.Close()
		}
	}
	carried, err := params.Carrier.LaunchArgv(domain.CarrierLaunch{Argv: []string{"fake-agent"}})
	if err != nil {
		closeTools()
		return domain.Session{}, err
	}
	argv := append(slices.Clone(params.HostPrefix), carried...)
	a.mu.Lock()
	a.launches = append(a.launches, argv)
	a.mu.Unlock()
	cmd := exec.Command(argv[0], argv[1:]...) //nolint:gosec // G204: the test's own launch
	stdin, err := cmd.StdinPipe()
	if err != nil {
		closeTools()
		return domain.Session{}, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		closeTools()
		return domain.Session{}, err
	}
	s := &lineSession{cmd: cmd, stdin: stdin, lines: make(chan []byte, 16), stderr: &bytes.Buffer{}, exited: make(chan struct{}), tools: tools}
	cmd.Stderr = s.stderr
	if err := cmd.Start(); err != nil {
		closeTools()
		return domain.Session{}, err
	}
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			s.lines <- slices.Clone(sc.Bytes())
		}
		s.readErr = sc.Err()
		close(s.lines)
	}()
	go func() {
		_ = cmd.Wait()
		close(s.exited)
	}()
	return domain.Session{ID: "line-session", Internal: s}, nil
}

func (a *lineAdapter) RunTurn(ctx context.Context, session domain.Session, params domain.RunTurnParams) (domain.TurnResult, error) {
	s, ok := session.Internal.(*lineSession)
	if !ok {
		return domain.TurnResult{SessionID: session.ID, ExitReason: domain.EventTurnCompleted}, nil
	}
	a.mu.Lock()
	a.prompts = append(a.prompts, params.Prompt)
	a.mu.Unlock()
	if err := json.NewEncoder(s.stdin).Encode(fakeAgentLine{Prompt: params.Prompt}); err != nil {
		return domain.TurnResult{}, err
	}
	for {
		select {
		case <-ctx.Done():
			_ = s.cmd.Process.Kill()
			return domain.TurnResult{SessionID: session.ID}, context.Cause(ctx)
		case raw, open := <-s.lines:
			if !open {
				if s.readErr != nil {
					return domain.TurnResult{SessionID: session.ID}, fmt.Errorf("reading the agent's output: %w", s.readErr)
				}
				return domain.TurnResult{SessionID: session.ID}, fmt.Errorf("agent exited mid-turn: %s", s.stderr)
			}
			var line fakeAgentLine
			if err := json.Unmarshal(raw, &line); err != nil {
				return domain.TurnResult{}, err
			}
			switch {
			case line.Done:
				return domain.TurnResult{SessionID: session.ID, ExitReason: domain.EventTurnCompleted}, nil
			case line.Usage != nil:
				params.OnEvent(domain.AgentEvent{Type: domain.EventTokenUsage, Timestamp: time.Now().UTC(),
					Usage: domain.TokenUsage{InputTokens: line.Usage.Input, OutputTokens: line.Usage.Output, TotalTokens: line.Usage.Input + line.Usage.Output}})
			default:
				params.OnEvent(domain.AgentEvent{Type: domain.EventNotification, Timestamp: time.Now().UTC(), Message: line.Message})
			}
		}
	}
}

func (a *lineAdapter) StopSession(ctx context.Context, session domain.Session) error {
	s, ok := session.Internal.(*lineSession)
	if !ok {
		return nil
	}
	_ = s.stdin.Close()
	select {
	case <-s.exited:
	case <-ctx.Done():
		_ = s.cmd.Process.Kill()
		<-s.exited
	}
	if s.tools != nil {
		return s.tools.Close()
	}
	return nil
}

func (a *lineAdapter) sent() ([][]string, []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.launches), slices.Clone(a.prompts)
}

// deliveries returns the tool-server declaration each session received.
func (a *lineAdapter) deliveries() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.delivered)
}

// gitFixture holds the repository both runs start from.
type gitFixture struct {
	t      *testing.T
	env    []string
	origin string
}

func newGitFixture(t *testing.T) *gitFixture {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	empty := filepath.Join(t.TempDir(), "gitconfig")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GIT_CONFIG_GLOBAL", empty)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	f := &gitFixture{t: t, env: append(os.Environ(),
		"GIT_AUTHOR_NAME=Fixture", "GIT_AUTHOR_EMAIL=fixture@example.com", "GIT_AUTHOR_DATE=2026-10-01T00:00:00Z",
		"GIT_COMMITTER_NAME=Fixture", "GIT_COMMITTER_EMAIL=fixture@example.com", "GIT_COMMITTER_DATE=2026-10-01T00:00:00Z")}
	work := filepath.Join(t.TempDir(), "work")
	f.git(filepath.Dir(work), "init", "--quiet", "--template=", "-b", "main", work)
	if err := os.WriteFile(filepath.Join(work, "README"), []byte("hello\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f.git(work, "add", "README")
	f.git(work, "commit", "--quiet", "-m", "initial")
	f.origin = filepath.Join(t.TempDir(), "origin.git")
	f.git(filepath.Dir(f.origin), "clone", "--quiet", "--bare", work, f.origin)
	return f
}

func (f *gitFixture) git(dir string, args ...string) string {
	f.t.Helper()
	cmd := exec.Command("git", args...) //nolint:gosec // G204: the test's own fixture
	cmd.Dir = dir
	cmd.Env = f.env
	out, err := cmd.CombinedOutput()
	if err != nil {
		f.t.Fatalf("git %v in %s: %v: %s", args, dir, err, out)
	}
	return strings.TrimSpace(string(out))
}

// upstream returns a new bare copy of the origin for one run.
func (f *gitFixture) upstream() string {
	f.t.Helper()
	dir := filepath.Join(f.t.TempDir(), "upstream.git")
	f.git(filepath.Dir(dir), "clone", "--quiet", "--bare", f.origin, dir)
	return dir
}

// branch returns the upstream's commit of branch, or "".
func (f *gitFixture) branch(upstream, branch string) string {
	f.t.Helper()
	cmd := exec.Command("git", "--git-dir="+upstream, "rev-parse", "--verify", "--quiet", "refs/heads/"+branch) //nolint:gosec // G204: the test's own check
	cmd.Env = f.env
	out, _ := cmd.Output()
	return strings.TrimSpace(string(out))
}

// runCapture records what a run produced.
type runCapture struct {
	result   WorkerResult
	launches [][]string
	prompts  []string
	received []string
	turn     []domain.AgentEvent
	opener   *fakeOpener
}

// turnRecord is an event of a turn without its clock.
type turnRecord struct {
	Type    domain.AgentEventType
	Message string
	Usage   domain.TokenUsage
}

func (c runCapture) turnRecords() []turnRecord {
	out := make([]turnRecord, 0, len(c.turn))
	for _, e := range c.turn {
		out = append(out, turnRecord{Type: e.Type, Message: e.Message, Usage: e.Usage})
	}
	return out
}

const remoteIssueID = "issue-30"

func remoteRunIssue() domain.Issue {
	return domain.Issue{ID: remoteIssueID, Identifier: "ONS-30", Title: "Add one and two", State: "To Do"}
}

// runDeps returns the dependencies of one run under cfg, recording into
// c.
func runDeps(t *testing.T, cfg config.ServiceConfig, adapter *lineAdapter, c *runCapture, done chan<- WorkerResult) WorkerDeps {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	turning := false
	c.opener = &fakeOpener{pr: domain.PullRequest{Number: 12, Created: true}}
	return WorkerDeps{
		TrackerAdapter: &mockTrackerAdapter{},
		AgentAdapter:   adapter,
		AgentKind:      cfg.Agent.Kind,
		UsageArrival:   registry.UsageArrivalIncremental,
		ConfigFunc:     func() config.ServiceConfig { return cfg },
		PromptTemplateByIDFunc: func(_ string) *prompt.Template {
			return mustParseTemplate(t, "Work on {{ .issue.identifier }}: {{ .issue.title }}")
		},
		OnTurnStarted: func(string, int) {
			mu.Lock()
			turning = true
			mu.Unlock()
		},
		OnEvent: func(_ string, e domain.AgentEvent) {
			mu.Lock()
			defer mu.Unlock()
			if turning {
				c.turn = append(c.turn, e)
			}
		},
		OnExit:          func(_ string, r WorkerResult) { done <- r },
		Logger:          testRunLogger(),
		DBPath:          filepath.Join(t.TempDir(), "sortie.db"),
		MCPServerBinary: self,
		PullRequestOpenerFunc: func(config.ServiceConfig) (domain.PullRequestOpener, error) {
			return c.opener, nil
		},
	}
}

func runRepository(upstream string) *config.SmolvmRepository {
	return &config.SmolvmRepository{URL: upstream, Base: "main", BranchPrefix: "sortie/", Provider: "github", Project: "org/app", MaxBundleMiB: 16}
}

func baseRunConfig(t *testing.T) config.ServiceConfig {
	t.Helper()
	cfg := defaultWorkerConfig(t.TempDir())
	cfg.Agent.Kind = "agent-client-protocol"
	cfg.Agent.MaxTurns = 2
	cfg.Agent.StallTimeoutMS = 300000
	return cfg
}

func envPath(t *testing.T) string {
	t.Helper()
	p, err := exec.LookPath("env")
	if err != nil {
		t.Skip("env is not installed")
	}
	return p
}

// colocatedEnv runs the workspace on this host, as an environment that
// carries the agent: commands run in the host workspace, and the session
// launch is the fake agent there.
type colocatedEnv struct {
	environment.Environment
	guestRoot string
	self      string
	transfer  string
	prompts   string
}

func (e colocatedEnv) GuestTransferDir() string { return e.transfer }

// GuestPath puts the workspace apart from the host workspace, as a
// machine does.
func (e colocatedEnv) GuestPath(hostPath string) string {
	return filepath.Join(e.guestRoot, filepath.Base(hostPath))
}

func (e colocatedEnv) Prepare(ctx context.Context, ws environment.Workspace) (environment.PrepareResult, error) {
	if err := os.MkdirAll(ws.GuestPath, 0o750); err != nil {
		return environment.PrepareResult{}, err
	}
	return e.Environment.Prepare(ctx, ws)
}

func (e colocatedEnv) Command(ctx context.Context, ws environment.Workspace, spec environment.CommandSpec) (*exec.Cmd, error) {
	ws.HostPath = ws.GuestPath
	return e.Environment.Command(ctx, ws, spec)
}

// AfterCreate links the workspace's .sortie to the host's control
// directory, as smolvm does.
func (e colocatedEnv) AfterCreate(_ context.Context, ws environment.Workspace) error {
	control := filepath.Join(ws.HostPath, ".sortie")
	if err := os.MkdirAll(control, 0o750); err != nil {
		return err
	}
	return os.Symlink(control, filepath.Join(ws.GuestPath, ".sortie"))
}

func (e colocatedEnv) LaunchArgv(ws environment.Workspace, _ domain.CarrierLaunch) ([]string, error) {
	return []string{e.self, fakeAgentCommand, ws.GuestPath, e.prompts}, nil
}

func (colocatedEnv) ConnectionFailed(environment.Workspace, int, []string) bool { return false }

// runColocated runs ONS-30 with its agent on this host, against a copy
// of the fixture's origin, and returns what it produced and the
// upstream.
func runColocated(t *testing.T, f *gitFixture) (runCapture, string) {
	t.Helper()
	upstream := f.upstream()
	cfg := baseRunConfig(t)
	cfg.Environment = config.EnvironmentConfig{Kind: config.EnvironmentKindSmolvm, HostPrefix: []string{envPath(t)},
		Smolvm: config.SmolvmConfig{Repository: runRepository(upstream)}}
	self, _ := os.Executable()
	prompts := filepath.Join(t.TempDir(), "prompts.jsonl")
	inner := recordingEnvironment(t)
	env := colocatedEnv{Environment: inner, guestRoot: t.TempDir(), self: self, transfer: t.TempDir(), prompts: prompts}

	adapter := &lineAdapter{}
	var c runCapture
	done := make(chan WorkerResult, 1)
	deps := runDeps(t, cfg, adapter, &c, done)
	deps.EnvironmentFunc = func(config.EnvironmentConfig) (environment.Environment, error) { return env, nil }
	go RunWorkerAttempt(context.Background(), remoteRunIssue(), nil, deps)
	c.result = waittest.Receive(t, done, "co-located run result")
	c.launches, c.prompts = adapter.sent()
	raw, _ := os.ReadFile(prompts) //nolint:gosec // G304: the test's own file
	for line := range strings.Lines(string(raw)) {
		var p string
		if err := json.Unmarshal([]byte(line), &p); err != nil {
			t.Fatal(err)
		}
		c.received = append(c.received, p)
	}
	return c, upstream
}

// fakeWorker is a worker that speaks worker API protocol 1 and carries
// the agent's standard streams and the workspace bundles over the data
// plane. Its agent is [fakeAgent], run in a clone of the seed.
type fakeWorker struct {
	t        *testing.T
	git      *gitFixture
	base     string
	id       string
	header   string
	received []string
	offer    worker.Offer
	clone    string

	// returnRefs names what the returned bundle holds; nil returns the
	// issue branch's new commits on top of the seed.
	returnRefs []string
	// breakStdio closes the stdio stream after the first output, without
	// the agent's exit.
	breakStdio bool
	// unknownUsage reports spend whose amount is unknown as well.
	unknownUsage bool
	// ended reports that the run already ended.
	ended func() bool
	// callTools makes the agent call a Sortie tool on its first turn, over
	// the mcp flow, with its own job's token and with another job's.
	callTools bool
	// issuer mints the other job's token.
	issuer *jobtoken.Issuer
	// toolCalls holds the answers to those calls, in that order.
	toolCalls []toolCall
}

// toolCall is the answer to one tool call on the mcp flow.
type toolCall struct {
	status int
	body   []byte
}

// callTool calls the stand-in tool server's tool on the mcp flow with
// token, as the worker forwards an agent's request to its machine's tool
// port: the agent's request goes on with the job's mcp token as its only
// credential.
func (w *fakeWorker) callTool(token string) (toolCall, error) {
	target, _ := w.flow("mcp")
	body := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"` + sessionIdentityToolName + `","arguments":{}}}`
	req, _ := http.NewRequest(http.MethodPost, target, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return toolCall{}, err
	}
	defer resp.Body.Close() //nolint:errcheck // test
	raw, err := io.ReadAll(resp.Body)
	return toolCall{status: resp.StatusCode, body: raw}, err
}

// makeToolCalls makes the first turn's tool calls.
func (w *fakeWorker) makeToolCalls() error {
	foreign, err := w.issuer.Mint("ONS-31", 1, []jobtoken.Flow{dataplane.FlowMCP})
	if err != nil {
		return err
	}
	for _, token := range []string{w.offer.Flows["mcp"].Token, foreign[dataplane.FlowMCP].Reveal()} {
		c, err := w.callTool(token)
		if err != nil {
			return err
		}
		w.toolCalls = append(w.toolCalls, c)
	}
	return nil
}

func (w *fakeWorker) api(method, path string, body any) *http.Response {
	w.t.Helper()
	var r io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		r = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, w.base+path, r)
	req.Header.Set(worker.ProtocolHeader, "1")
	req.Header.Set(w.header, w.id)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		w.t.Fatal(err)
	}
	return resp
}

func (w *fakeWorker) flow(name string) (string, string) {
	f := w.offer.Flows[name]
	return f.URL + "/jobs/" + url.PathEscape(w.offer.Job.ID) + "/attempts/" + fmt.Sprint(w.offer.Job.Attempt), f.Token
}

// take checks in and polls until it holds a job.
func (w *fakeWorker) take() {
	w.t.Helper()
	resp := w.api(http.MethodPost, "/api/v1/workers/checkin", map[string]any{
		"os": "linux", "arch": "x86_64", "kvm": true, "nested": true, "backends": []string{"smolvm"},
		"run_slots": 2, "running": 0, "free": map[string]any{"cpu": 8, "mem_gib": 32, "disk_gib": 100},
	})
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		w.t.Fatalf("checkin: %d", resp.StatusCode)
	}
	waittest.Until(w.t, func() error {
		if w.ended != nil && w.ended() {
			w.t.Fatal("the run ended before its job was offered")
		}
		resp := w.api(http.MethodPost, "/api/v1/workers/poll", nil)
		defer resp.Body.Close() //nolint:errcheck // test
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("poll: %d", resp.StatusCode)
		}
		return json.NewDecoder(resp.Body).Decode(&w.offer)
	})
}

// seed fetches the seed with the bundles token and clones it.
func (w *fakeWorker) seed() {
	w.t.Helper()
	f := w.offer.Flows["bundles"]
	req, _ := http.NewRequest(http.MethodGet, f.URL+w.offer.Run.SeedPath, nil)
	req.Header.Set("Authorization", "Bearer "+f.Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		w.t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // test
	if resp.StatusCode != http.StatusOK {
		w.t.Fatalf("seed: %d", resp.StatusCode)
	}
	bundle := filepath.Join(w.t.TempDir(), "seed.bundle")
	out, _ := os.Create(bundle)
	_, _ = io.Copy(out, resp.Body)
	_ = out.Close()
	w.clone = filepath.Join(w.t.TempDir(), "workspace")
	w.git.git(filepath.Dir(w.clone), "clone", "--quiet", bundle, w.clone)
}

// session opens the stdio stream and runs the agent over it until its
// input closes.
func (w *fakeWorker) session() {
	w.t.Helper()
	target, token := w.flow("stdio")
	u, _ := url.Parse(target)
	conn, err := net.Dial("tcp", u.Host)
	if err != nil {
		w.t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck // test
	_, _ = fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", u.Path, u.Host, token, dataplane.StdioProtocol)
	in := bufio.NewReader(conn)
	resp, err := http.ReadResponse(in, nil)
	if err != nil {
		w.t.Fatalf("stdio upgrade: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		w.t.Fatalf("stdio upgrade: %d", resp.StatusCode)
	}
	stdinR, stdinW := io.Pipe()
	go func() {
		for {
			typ, payload, err := dataplane.ReadFrame(in)
			if err != nil || typ == dataplane.FrameStdinClosed {
				_ = stdinW.Close()
				return
			}
			_, _ = stdinW.Write(payload)
		}
	}()
	out := frameWriter{conn: conn, broken: w.breakStdio}
	err = fakeAgent(stdinR, &out, w.clone, w.reportUsage, func(p string) error {
		w.received = append(w.received, p)
		if w.callTools && len(w.received) == 1 {
			return w.makeToolCalls()
		}
		return nil
	})
	if errors.Is(err, errStreamBroken) {
		return
	}
	if err != nil {
		w.t.Errorf("fake agent: %v", err)
	}
	if err := dataplane.WriteExit(conn, 0); err != nil {
		w.t.Errorf("exit frame: %v", err)
	}
}

var errStreamBroken = errors.New("stdio stream broken by the test")

// frameWriter writes the agent's output as stdout frames; with broken
// set it closes the stream after the first write instead.
type frameWriter struct {
	conn   net.Conn
	broken bool
}

func (f *frameWriter) Write(p []byte) (int, error) {
	if err := dataplane.WriteFrame(f.conn, dataplane.FrameStdout, p); err != nil {
		return 0, err
	}
	if f.broken {
		_ = f.conn.Close()
		return 0, errStreamBroken
	}
	return len(p), nil
}

func (w *fakeWorker) reportUsage(u fakeAgentUsage) error {
	target, token := w.flow("usage")
	bodies := []string{fmt.Sprintf(`{"input_tokens":%d,"output_tokens":%d}`, u.Input, u.Output)}
	if w.unknownUsage {
		bodies = append(bodies, `{"measured":false}`)
	}
	for _, body := range bodies {
		req, _ := http.NewRequest(http.MethodPost, target, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			return fmt.Errorf("usage: %d", resp.StatusCode)
		}
	}
	return nil
}

// giveBack returns the issue branch and the pull request suggestion,
// then reports the attempt.
func (w *fakeWorker) giveBack() {
	w.t.Helper()
	refs := w.returnRefs
	branch := "sortie/" + w.offer.Job.ID
	if refs == nil {
		refs = []string{"refs/heads/" + branch, "^origin/" + branch}
	}
	bundle := filepath.Join(w.t.TempDir(), "out.bundle")
	w.git.git(w.clone, append([]string{"bundle", "create", "--quiet", bundle}, refs...)...)
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	part, _ := mw.CreateFormFile("bundle", "out.bundle")
	raw, _ := os.ReadFile(bundle) //nolint:gosec // G304: the test's own file
	_, _ = part.Write(raw)
	if pr, err := os.ReadFile(filepath.Join(w.clone, ".sortie", "pr.json")); err == nil {
		part, _ := mw.CreateFormFile("pr", "pr.json")
		_, _ = part.Write(pr)
	}
	_ = mw.Close()
	f := w.offer.Flows["bundles"]
	target, _ := w.flow("bundles")
	req, _ := http.NewRequest(http.MethodPost, target+"/result", body)
	req.Header.Set("Authorization", "Bearer "+f.Token)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		w.t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		w.t.Fatalf("result: %d", resp.StatusCode)
	}
	resp = w.api(http.MethodPost, fmt.Sprintf("/api/v1/workers/jobs/%s/attempts/%d/report", w.offer.Job.ID, w.offer.Job.Attempt),
		map[string]string{"outcome": "succeeded", "machine": "stopped"})
	_ = resp.Body.Close()
}

// remoteServer serves the worker API and the data plane of rw, as
// Sortie's HTTP server does under the remote kind.
func remoteServer(t *testing.T, rw *remoteWorkers, header string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(worker.Prefix, worker.Handler(rw.matcher, header))
	mux.Handle(dataplane.Prefix, rw.handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

type remoteRun struct {
	runCapture
	upstream string
	adapter  *lineAdapter
	worker   *fakeWorker
	broken   []string
	offerRaw []byte
}

// runRemoteONS30 runs ONS-30 under the remote kind, on taichi's fake
// worker, against a copy of the fixture's origin. prepare adjusts the
// worker; drive runs it, the whole job by default.
func runRemoteONS30(t *testing.T, f *gitFixture, prepare func(*fakeWorker), drive func(*fakeWorker, context.CancelCauseFunc)) remoteRun {
	t.Helper()
	upstream := f.upstream()
	rw := newRemoteWorkers(time.Now)
	const header = "X-Sortie-Worker-Identity"
	srv := remoteServer(t, rw, header)
	cfg := baseRunConfig(t)
	flows := map[string]string{}
	for _, fl := range config.RemoteFlows {
		flows[fl] = srv.URL + dataplane.Prefix + fl
	}
	cfg.Environment = config.EnvironmentConfig{Kind: config.EnvironmentKindRemote, HostPrefix: []string{envPath(t)},
		Remote: config.RemoteConfig{IdentityHeader: header, Backend: "smolvm", Image: "ghcr.io/ortusnova/agent:1",
			Flows: flows, LeaseTTLMS: 60000, Repository: runRepository(upstream)}}

	adapter := &lineAdapter{}
	run := remoteRun{upstream: upstream, adapter: adapter}
	var brokenMu sync.Mutex
	done := make(chan WorkerResult, 1)
	w := &fakeWorker{t: t, git: f, base: srv.URL, id: "taichi", header: header, issuer: rw.tokens, ended: func() bool { return len(done) > 0 }}
	if prepare != nil {
		prepare(w)
	}
	deps := runDeps(t, cfg, adapter, &run.runCapture, done)
	if w.callTools {
		deps.WorkflowPath = filepath.Join(t.TempDir(), "WORKFLOW.md")
		if err := os.WriteFile(deps.WorkflowPath, []byte("---\n---\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	go RunRemoteAttempt(ctx, rw, remoteRunIssue(), nil, deps, RemoteAttemptParams{
		DefaultCommand: "opencode acp",
		OnStreamBroken: func(id string) {
			brokenMu.Lock()
			run.broken = append(run.broken, id)
			brokenMu.Unlock()
			cancel(errCancelStall)
		},
	})

	if drive == nil {
		drive = func(w *fakeWorker, _ context.CancelCauseFunc) {
			w.take()
			w.seed()
			w.session()
			w.giveBack()
		}
	}
	drive(w, cancel)
	run.offerRaw, _ = json.Marshal(w.offer)
	run.result = waittest.Receive(t, done, "remote run result")
	run.launches, run.prompts = adapter.sent()
	run.received = w.received
	run.worker = w
	brokenMu.Lock()
	defer brokenMu.Unlock()
	return run
}

// remote-runs: A turn on taichi, Pull request, and the parity of a
// remote run with the same run co-located: the same prompt, the same
// turn records, and the same pull request.
func TestRemoteRunMatchesTheColocatedRun(t *testing.T) {
	f := newGitFixture(t)
	local, localUpstream := runColocated(t, f)
	remote := runRemoteONS30(t, f, nil, nil)

	for name, c := range map[string]runCapture{"co-located": local, "remote": remote.runCapture} {
		if c.result.ExitKind != WorkerExitNormal || c.result.Error != nil {
			t.Fatalf("%s run: %v %v", name, c.result.ExitKind, c.result.Error)
		}
	}
	if len(local.prompts) == 0 || !slices.Equal(remote.prompts, local.prompts) {
		t.Errorf("prompts sent:\nremote %q\nlocal  %q", remote.prompts, local.prompts)
	}
	if !slices.Equal(remote.received, local.received) || !slices.Equal(remote.received, remote.prompts) {
		t.Errorf("prompts the agent received:\nremote %q\nlocal  %q", remote.received, local.received)
	}
	if got, want := remote.turnRecords(), local.turnRecords(); !slices.Equal(got, want) || len(want) == 0 {
		t.Errorf("turn records:\nremote %+v\nlocal  %+v", got, want)
	}
	if remote.result.TurnsCompleted != local.result.TurnsCompleted || remote.result.Usage != local.result.Usage {
		t.Errorf("turns %d usage %+v; co-located %d %+v", remote.result.TurnsCompleted, remote.result.Usage, local.result.TurnsCompleted, local.result.Usage)
	}

	// The same two commits are pushed, and the same pull request opened.
	remoteTip, localTip := f.branch(remote.upstream, "sortie/ONS-30"), f.branch(localUpstream, "sortie/ONS-30")
	if remoteTip == "" || remoteTip != localTip || remote.result.ExportedCommit != remoteTip {
		t.Errorf("pushed sortie/ONS-30: remote %q (exported %q), co-located %q", remoteTip, remote.result.ExportedCommit, localTip)
	}
	if n := f.git(remote.upstream, "rev-list", "--count", "main..sortie/ONS-30"); n != "2" {
		t.Errorf("remote push holds %s new commits, want 2", n)
	}
	want := ensureCall{Owner: "org", Repo: "app", Head: "sortie/ONS-30", Base: "main", Title: "Add one and two", Body: "Two changes."}
	if got, lc := remote.opener.ensureCalls(), local.opener.ensureCalls(); !slices.Equal(got, []ensureCall{want}) || !slices.Equal(lc, got) {
		t.Errorf("pull requests: remote %+v, co-located %+v, want %+v", got, lc, want)
	}

	// The session ran behind the host prefix, so acp-broker serves its
	// watch view, and it is the stdio bridge, not the agent.
	if len(remote.launches) != 1 || remote.launches[0][0] != envPath(t) || remote.launches[0][2] != dataplane.BridgeCommand {
		t.Errorf("remote session launches = %q, want one behind the host prefix through the stdio bridge", remote.launches)
	}

	// No forge credential reaches the worker: the offer names no
	// repository, and the clone knows no upstream.
	if bytes.Contains(remote.offerRaw, []byte(remote.upstream)) {
		t.Errorf("offer names the upstream: %s", remote.offerRaw)
	}
	if origin := f.git(remote.worker.clone, "remote", "get-url", "origin"); strings.Contains(origin, remote.upstream) {
		t.Errorf("the worker's clone has origin %q", origin)
	}

	// The issue moves to its handoff state.
	var sequence []string
	_, store, tracker := handOff(t, remote.result, &sequence)
	if len(tracker.transitionCalls) != 1 || tracker.transitionCalls[0].TargetState != "Human Review" || store.runHistories[0].Status != "succeeded" {
		t.Errorf("handoff transitions %+v, history %+v", tracker.transitionCalls, store.runHistories)
	}
}

// remote-runs: Not the branch. A returned bundle that carries another
// ref, or commits not on top of the seed, is refused naming the ref;
// nothing is pushed and the attempt fails.
func TestRemoteRunRefusesNotTheBranch(t *testing.T) {
	f := newGitFixture(t)
	for name, tc := range map[string]struct {
		refs   func(w *fakeWorker) []string
		naming string
	}{
		"another ref": {func(w *fakeWorker) []string {
			w.git.git(w.clone, "branch", "other")
			return []string{"refs/heads/other", "^origin/sortie/ONS-30"}
		}, "refs/heads/other"},
		"not on top of the seed": {func(w *fakeWorker) []string {
			w.git.git(w.clone, "checkout", "--quiet", "--orphan", "fresh")
			w.git.git(w.clone, "commit", "--quiet", "--allow-empty", "-m", "unrelated")
			w.git.git(w.clone, "branch", "--force", "sortie/ONS-30", "fresh")
			return []string{"refs/heads/sortie/ONS-30"}
		}, "refs/heads/sortie/ONS-30"},
	} {
		t.Run(name, func(t *testing.T) {
			run := runRemoteONS30(t, f, nil, func(w *fakeWorker, _ context.CancelCauseFunc) {
				w.take()
				w.seed()
				w.session()
				w.returnRefs = tc.refs(w)
				w.giveBack()
			})
			if run.result.ExitKind != WorkerExitError || run.result.Error == nil || !strings.Contains(run.result.Error.Error(), tc.naming) {
				t.Fatalf("result %v %v, want a failure naming %s", run.result.ExitKind, run.result.Error, tc.naming)
			}
			if tip := f.branch(run.upstream, "sortie/ONS-30"); tip != "" {
				t.Errorf("upstream sortie/ONS-30 = %s, want nothing pushed", tip)
			}
			if calls := run.opener.ensureCalls(); len(calls) != 0 {
				t.Errorf("pull requests %+v, want none", calls)
			}
		})
	}
}

// remote-runs: Usage. taichi's 12000 input and 3000 output tokens show
// in GET /api/v1/state and in the run history; a report of an unknown
// amount counts as unaccounted spend.
func TestRemoteRunUsage(t *testing.T) {
	f := newGitFixture(t)
	run := runRemoteONS30(t, f, func(w *fakeWorker) { w.unknownUsage = true }, nil)
	if run.result.ExitKind != WorkerExitNormal {
		t.Fatalf("result %v %v", run.result.ExitKind, run.result.Error)
	}
	if u := run.result.Usage; u.InputTokens != 12000 || u.OutputTokens != 3000 || !run.result.UsageMeasured {
		t.Errorf("run usage %+v measured %v, want 12000/3000", u, run.result.UsageMeasured)
	}
	if run.result.UnaccountedTurns != 1 {
		t.Errorf("unaccounted turns = %d, want 1 for the unknown amount", run.result.UnaccountedTurns)
	}

	o, entry := tokenWarningApplyEventOrchestrator(t, 0, 0)
	entry.Issue.State = "To Do"
	for _, e := range run.turn {
		o.applyAgentEvent(context.Background(), agentEventMsg{IssueID: "id-apply", Event: e}, true)
	}
	snap := RuntimeSnapshot(o.state, time.Now())
	if len(snap.Running) != 1 || snap.Running[0].AgentInputTokens != 12000 || snap.Running[0].AgentOutputTokens != 3000 {
		t.Fatalf("state running = %+v, want 12000 input and 3000 output tokens", snap.Running)
	}
	store := &mockExitStore{}
	result := run.result
	result.IssueID = "id-apply"
	HandleWorkerExit(o.state, result, defaultExitParams(t, store))
	if len(store.runHistories) != 1 || store.runHistories[0].InputTokens != 12000 || store.runHistories[0].OutputTokens != 3000 {
		t.Errorf("run history %+v, want 12000 input and 3000 output tokens", store.runHistories)
	}
}

// remote-runs: A tool call from taichi. The agent of ONS-30 calls a
// Sortie tool through the mcp flow, and the call acts on ONS-30's
// session: it reaches the tool server Sortie started for that session.
// The same call with another job's token answers 401.
func TestRemoteRunToolCallReachesTheJobsOwnSession(t *testing.T) {
	f := newGitFixture(t)
	run := runRemoteONS30(t, f, func(w *fakeWorker) { w.callTools = true }, nil)
	if run.result.ExitKind != WorkerExitNormal || run.result.Error != nil {
		t.Fatalf("result %v %v", run.result.ExitKind, run.result.Error)
	}

	// The session declares Sortie's tool server at the machine's tool
	// port, where the worker serves the mcp flow.
	delivered := run.adapter.deliveries()
	if len(delivered) != 1 || !strings.Contains(delivered[0], fmt.Sprintf(`"http://127.0.0.1:%d/mcp"`, remoteToolPort)) {
		t.Errorf("tool servers delivered %q, want sortie-tools at the machine's port %d", delivered, remoteToolPort)
	}

	calls := run.worker.toolCalls
	if len(calls) != 2 {
		t.Fatalf("tool calls made %d, want 2", len(calls))
	}
	own := calls[0]
	if own.status != http.StatusOK {
		t.Fatalf("tool call with ONS-30's mcp token: %d %s", own.status, own.body)
	}
	var answer struct {
		Result struct {
			IsError bool `json:"isError"`
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
	}
	if err := json.Unmarshal(own.body, &answer); err != nil || len(answer.Result.Content) != 1 || answer.Result.IsError {
		t.Fatalf("tool call answer %s: %v", own.body, err)
	}
	var identity map[string]string
	if err := json.Unmarshal([]byte(answer.Result.Content[0].Text), &identity); err != nil {
		t.Fatalf("tool result %q: %v", answer.Result.Content[0].Text, err)
	}
	if identity["issue_identifier"] != "ONS-30" || identity["issue_id"] != remoteIssueID {
		t.Errorf("the tool call acted on %v, want ONS-30's session", identity)
	}

	if foreign := calls[1]; foreign.status != http.StatusUnauthorized || bytes.Contains(foreign.body, []byte("ONS-30")) {
		t.Errorf("tool call with another job's token: %d %s, want 401 reaching nothing", foreign.status, foreign.body)
	}
}

// remote-runs: a session whose stdio stream breaks ends the turn as a
// stalled agent ends it.
func TestRemoteRunBrokenStdioEndsTheTurnAsAStall(t *testing.T) {
	f := newGitFixture(t)
	run := runRemoteONS30(t, f, func(w *fakeWorker) { w.breakStdio = true }, func(w *fakeWorker, _ context.CancelCauseFunc) {
		w.take()
		w.seed()
		w.session()
	})
	if !slices.Equal(run.broken, []string{remoteIssueID}) {
		t.Fatalf("broken streams reported %q, want %s", run.broken, remoteIssueID)
	}
	if run.result.ExitKind != WorkerExitCancelled || !errors.Is(run.result.CancelCause, errCancelStall) {
		t.Fatalf("result %v cause %v, want the stall's cancellation", run.result.ExitKind, run.result.CancelCause)
	}
}

// The orchestrator ends a run whose stdio stream broke as stall
// detection ends a stalled one: its run is cancelled for a stall and
// retried with the stall's error.
func TestStallBrokenStream(t *testing.T) {
	store := &mockReconcileStore{}
	state := NewState(5000, 4, 0, nil, AgentTotals{})
	var cause error
	state.Running["ISSUE-1"] = &RunningEntry{Identifier: "ONS-30", StartedAt: time.Now(),
		CancelCauseFunc: func(c error) { cause = c }}
	state.Claimed["ISSUE-1"] = struct{}{}
	stallBrokenStream(state, "ISSUE-1", ReconcileParams{Store: store, MaxRetryBackoffMS: 300000, OnRetryFire: noopRetryFire, Logger: discardLogger()})
	if !errors.Is(cause, errCancelStall) {
		t.Fatalf("cancel cause %v, want the stall's", cause)
	}
	if retry, ok := state.RetryAttempts["ISSUE-1"]; !ok || retry.Error != StallCancelError || len(store.savedEntries) != 1 {
		t.Fatalf("retry %+v ok=%v saved %d, want the stall retry", retry, ok, len(store.savedEntries))
	}
}

func testRunLogger() *slog.Logger {
	if os.Getenv("SORTIE_TEST_RUN_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return discardLogger()
}
