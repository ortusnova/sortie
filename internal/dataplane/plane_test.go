package dataplane

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sortie-ai/sortie/internal/jobtoken"
	"github.com/sortie-ai/sortie/internal/waittest"
)

var flows = []jobtoken.Flow{"stdio", "egress", "mcp", "bundles", "usage"}

type fixture struct {
	t      *testing.T
	issuer *jobtoken.Issuer
	plane  *Plane
	srv    *httptest.Server
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	issuer := jobtoken.NewIssuer()
	plane := New(issuer)
	srv := httptest.NewServer(plane.Handler())
	t.Cleanup(srv.Close)
	return &fixture{t: t, issuer: issuer, plane: plane, srv: srv}
}

// mint returns the tokens of job's attempt, one per flow.
func (f *fixture) mint(job string, attempt uint64) map[jobtoken.Flow]string {
	f.t.Helper()
	toks, err := f.issuer.Mint(job, attempt, flows)
	if err != nil {
		f.t.Fatal(err)
	}
	out := map[jobtoken.Flow]string{}
	for fl, tok := range toks {
		out[fl] = tok.Reveal()
	}
	return out
}

func (f *fixture) open(attempt int, opts Options) *Attempt {
	f.t.Helper()
	if opts.Dir == "" {
		opts.Dir = f.t.TempDir()
	}
	if opts.MaxBundleBytes == 0 {
		opts.MaxBundleBytes = 1 << 20
	}
	a, err := f.plane.Open("ONS-30", attempt, opts)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(a.Close)
	return a
}

// reply is a response with its body read.
type reply struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

func (f *fixture) do(method, path, token, contentType string, body io.Reader) reply {
	f.t.Helper()
	req, err := http.NewRequest(method, f.srv.URL+path, body)
	if err != nil {
		f.t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // test
	raw, _ := io.ReadAll(resp.Body)
	return reply{StatusCode: resp.StatusCode, Header: resp.Header, Body: raw}
}

const seedPath = Prefix + "bundles/jobs/ONS-30/attempts/1/seed"

// remote-runs: Seed. The seed is served to the job's live bundles token
// only; another job's token, another flow's, an ended one, or none
// answers 401 and serves nothing.
func TestSeedOnlyForTheJobsBundlesToken(t *testing.T) {
	f := newFixture(t)
	seed := filepath.Join(t.TempDir(), "seed.bundle")
	if err := os.WriteFile(seed, []byte("bundle bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	f.open(1, Options{Seed: seed})
	own := f.mint("ONS-30", 1)
	other := f.mint("ONS-31", 1)

	resp := f.do(http.MethodGet, seedPath, own["bundles"], "", nil)
	body := resp.Body
	if resp.StatusCode != http.StatusOK || string(body) != "bundle bytes" || resp.Header.Get("Content-Type") != "application/x-git-bundle" {
		t.Fatalf("own token: %d %q %q", resp.StatusCode, body, resp.Header.Get("Content-Type"))
	}
	for name, tok := range map[string]string{
		"another job's token": other["bundles"], "another flow's token": own["stdio"], "no token": "",
	} {
		resp := f.do(http.MethodGet, seedPath, tok, "", nil)
		body := resp.Body
		if resp.StatusCode != http.StatusUnauthorized || bytes.Contains(body, []byte("bundle bytes")) {
			t.Errorf("%s: %d %q, want 401 serving nothing", name, resp.StatusCode, body)
		}
	}
	f.issuer.OnLeaseEnd("ONS-30")
	if resp := f.do(http.MethodGet, seedPath, own["bundles"], "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("ended token: %d, want 401", resp.StatusCode)
	}
}

func TestSeedOfAnotherAttemptRefused(t *testing.T) {
	f := newFixture(t)
	f.open(2, Options{})
	toks := f.mint("ONS-30", 2)
	if resp := f.do(http.MethodGet, seedPath, toks["bundles"], "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("attempt 2 token on attempt 1: %d, want 401", resp.StatusCode)
	}
}

// dialStdio opens the stdio stream of ONS-30 attempt 1 as a worker does.
func (f *fixture) dialStdio(token string) (net.Conn, *bufio.Reader) {
	f.t.Helper()
	u, _ := url.Parse(f.srv.URL)
	conn, err := net.Dial("tcp", u.Host)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { _ = conn.Close() })
	req := "GET " + Prefix + "stdio/jobs/ONS-30/attempts/1 HTTP/1.1\r\nHost: " + u.Host +
		"\r\nAuthorization: Bearer " + token + "\r\nConnection: Upgrade\r\nUpgrade: " + StdioProtocol + "\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		f.t.Fatal(err)
	}
	in := bufio.NewReader(conn)
	resp, err := http.ReadResponse(in, nil)
	if err != nil {
		f.t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		f.t.Fatalf("stdio upgrade: %d", resp.StatusCode)
	}
	return conn, in
}

type bridgeRun struct {
	stdin  *io.PipeWriter
	stdout *io.PipeReader
	stderr *bytes.Buffer
	done   chan int
}

func startBridge(a *Attempt) *bridgeRun {
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	b := &bridgeRun{stdin: inW, stdout: outR, stderr: &bytes.Buffer{}, done: make(chan int, 1)}
	go func() {
		code := RunBridge(a.Socket(), inR, outW, b.stderr)
		_ = outW.Close()
		b.done <- code
	}()
	return b
}

// remote-runs: A turn on taichi. The agent's input and output cross the
// stdio flow frame by frame, and the bridge exits with the agent's
// status.
func TestStdioCarriesTheAgentsStreams(t *testing.T) {
	f := newFixture(t)
	a := f.open(1, Options{})
	toks := f.mint("ONS-30", 1)
	conn, in := f.dialStdio(toks["stdio"])
	if err := a.WaitStdio(context.Background()); err != nil {
		t.Fatal(err)
	}
	b := startBridge(a)

	if _, err := b.stdin.Write([]byte("prompt\n")); err != nil {
		t.Fatal(err)
	}
	typ, payload, err := ReadFrame(in)
	if err != nil || typ != FrameStdin || string(payload) != "prompt\n" {
		t.Fatalf("worker got %q %q %v", typ, payload, err)
	}
	if err := WriteFrame(conn, FrameStdout, []byte("reply\n")); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(b.stdout).ReadString('\n')
	if err != nil || line != "reply\n" {
		t.Fatalf("bridge stdout %q %v", line, err)
	}
	_ = b.stdin.Close()
	if typ, _, err := ReadFrame(in); err != nil || typ != FrameStdinClosed {
		t.Fatalf("worker got %q %v, want stdin closed", typ, err)
	}
	if err := WriteExit(conn, 3); err != nil {
		t.Fatal(err)
	}
	if code := waittest.Receive(t, b.done, "bridge exit"); code != 3 {
		t.Fatalf("bridge exit %d, want 3", code)
	}

	if resp := f.do(http.MethodGet, Prefix+"stdio/jobs/ONS-30/attempts/1", toks["stdio"], "", nil); resp.StatusCode == http.StatusSwitchingProtocols {
		t.Fatal("a second stdio stream was accepted")
	}
}

// A stream that breaks before the agent's exit leaves the session
// silent, as a stalled agent leaves it, and reports the break.
func TestBrokenStdioLeavesTheSessionSilent(t *testing.T) {
	f := newFixture(t)
	broke := make(chan error, 1)
	a := f.open(1, Options{OnStdioBroken: func(err error) { broke <- err }})
	conn, _ := f.dialStdio(f.mint("ONS-30", 1)["stdio"])
	b := startBridge(a)
	if err := WriteFrame(conn, FrameStdout, []byte("partial")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len("partial"))
	if _, err := io.ReadFull(b.stdout, buf); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if err := waittest.Receive(t, broke, "broken stream report"); err == nil {
		t.Fatal("broken with a nil error")
	}
	select {
	case code := <-b.done:
		t.Fatalf("bridge exited %d on a broken stream, want it silent", code)
	default:
	}
	a.Close()
	if code := waittest.Receive(t, b.done, "bridge exit after close"); code == 0 {
		t.Fatal("bridge exit 0 after the attempt closed")
	}
}

func multipartBody(t *testing.T, parts map[string][]byte) (string, *bytes.Buffer) {
	t.Helper()
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	for name, data := range parts {
		w, err := mw.CreateFormFile(name, name)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write(data)
	}
	_ = mw.Close()
	return mw.FormDataContentType(), body
}

const resultPath = Prefix + "bundles/jobs/ONS-30/attempts/1/result"

func TestResultCarriesTheBundleAndPRFile(t *testing.T) {
	f := newFixture(t)
	a := f.open(1, Options{MaxBundleBytes: 64})
	tok := f.mint("ONS-30", 1)["bundles"]

	ct, body := multipartBody(t, map[string][]byte{"bundle": bytes.Repeat([]byte("b"), 65)})
	if resp := f.do(http.MethodPost, resultPath, tok, ct, body); resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized bundle: %d, want 413", resp.StatusCode)
	}
	ct, body = multipartBody(t, map[string][]byte{"bundle": []byte("the bundle"), "pr": []byte(`{"title":"t","body":"b"}`)})
	if resp := f.do(http.MethodPost, resultPath, tok, ct, body); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("result: %d, want 204", resp.StatusCode)
	}
	res, err := a.Result(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(res.Bundle); string(got) != "the bundle" || string(res.PR) != `{"title":"t","body":"b"}` {
		t.Fatalf("result %q, pr %q", got, res.PR)
	}
	ct, body = multipartBody(t, map[string][]byte{"bundle": []byte("again")})
	if resp := f.do(http.MethodPost, resultPath, tok, ct, body); resp.StatusCode != http.StatusConflict {
		t.Fatalf("second result: %d, want 409", resp.StatusCode)
	}
}

func TestResultWaitEndsWithTheAttempt(t *testing.T) {
	f := newFixture(t)
	a := f.open(1, Options{})
	a.Close()
	if _, err := a.Result(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Result after close: %v, want ErrClosed", err)
	}
	if resp := f.do(http.MethodGet, seedPath, f.mint("ONS-30", 1)["bundles"], "", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("seed of a closed attempt: %d, want 404", resp.StatusCode)
	}
}

// remote-runs: Usage. A report is passed on as it arrived, an unknown
// amount included.
func TestUsageReports(t *testing.T) {
	f := newFixture(t)
	got := make(chan Usage, 2)
	f.open(1, Options{OnUsage: func(u Usage) { got <- u }})
	toks := f.mint("ONS-30", 1)
	path := Prefix + "usage/jobs/ONS-30/attempts/1"
	if resp := f.do(http.MethodPost, path, toks["usage"], "application/json", strings.NewReader(`{"input_tokens":12000,"output_tokens":3000,"model":"m"}`)); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("usage: %d", resp.StatusCode)
	}
	if u := waittest.Receive(t, got, "usage"); u != (Usage{InputTokens: 12000, OutputTokens: 3000, Model: "m", Measured: true}) {
		t.Fatalf("usage %+v", u)
	}
	if resp := f.do(http.MethodPost, path, toks["usage"], "application/json", strings.NewReader(`{"measured":false}`)); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("unknown usage: %d", resp.StatusCode)
	}
	if u := waittest.Receive(t, got, "unknown usage"); u.Measured {
		t.Fatalf("usage %+v, want unmeasured", u)
	}
	for name, tc := range map[string]struct {
		tok, body string
		code      int
	}{
		"bundles token": {toks["bundles"], `{}`, http.StatusUnauthorized},
		"negative":      {toks["usage"], `{"input_tokens":-1}`, http.StatusBadRequest},
	} {
		if resp := f.do(http.MethodPost, path, tc.tok, "application/json", strings.NewReader(tc.body)); resp.StatusCode != tc.code {
			t.Errorf("%s: %d, want %d", name, resp.StatusCode, tc.code)
		}
	}
}

// toolServer stands in for a session's tool server behind a's tool
// socket: it answers every request with what it received.
func toolServer(t *testing.T, a *Attempt) {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", a.ToolSocket())
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { //nolint:gosec // G112: a test stand-in
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"method": r.Method, "path": r.URL.Path, "query": r.URL.RawQuery, "host": r.Host,
			"authorization": r.Header.Get("Authorization"), "body": string(body),
		})
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
}

const mcpPath = Prefix + "mcp/jobs/ONS-30/attempts/1"

// remote-runs: A tool call from taichi. A request with the attempt's
// live mcp token reaches the tool server of the attempt's own session at
// /mcp, carrying that server's bearer and not the job token; a token of
// another job, attempt or flow, an ended one, or none answers 401 and
// reaches nothing.
func TestMCPReachesTheAttemptsOwnToolServer(t *testing.T) {
	f := newFixture(t)
	a := f.open(1, Options{})
	own := f.mint("ONS-30", 1)
	other := f.mint("ONS-31", 1)

	if resp := f.do(http.MethodPost, mcpPath, own["mcp"], "application/json", strings.NewReader(`{}`)); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("before the session's tool server: %d, want 503", resp.StatusCode)
	}

	toolServer(t, a)
	a.AuthorizeTools("session-bearer")
	call := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"sortie_status"}}`
	resp := f.do(http.MethodPost, mcpPath+"?x=1", own["mcp"], "application/json", strings.NewReader(call))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("own mcp token: %d %s", resp.StatusCode, resp.Body)
	}
	var got map[string]string
	if err := json.Unmarshal(resp.Body, &got); err != nil {
		t.Fatalf("tool server answer %s: %v", resp.Body, err)
	}
	want := map[string]string{"method": http.MethodPost, "path": "/mcp", "query": "x=1", "host": "127.0.0.1",
		"authorization": "Bearer session-bearer", "body": call}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("tool server received %s = %q, want %q", k, got[k], v)
		}
	}

	refused := map[string]string{"another job's token": other["mcp"], "another flow's token": own["stdio"], "no token": ""}
	attempt2 := f.mint("ONS-30", 2)
	refused["another attempt's token"], refused["an ended token"] = attempt2["mcp"], own["mcp"]
	for name, tok := range refused {
		resp := f.do(http.MethodPost, mcpPath, tok, "application/json", strings.NewReader(call))
		if resp.StatusCode != http.StatusUnauthorized || bytes.Contains(resp.Body, []byte("session-bearer")) {
			t.Errorf("%s: %d %s, want 401 reaching nothing", name, resp.StatusCode, resp.Body)
		}
	}

	a.Close()
	if resp := f.do(http.MethodPost, mcpPath, f.mint("ONS-30", 1)["mcp"], "application/json", strings.NewReader(call)); resp.StatusCode != http.StatusNotFound {
		t.Errorf("closed attempt: %d, want 404", resp.StatusCode)
	}
}

// A tool server that is gone answers 502.
func TestMCPWithoutAToolServerAnswersBadGateway(t *testing.T) {
	f := newFixture(t)
	a := f.open(1, Options{})
	a.AuthorizeTools("session-bearer")
	if resp := f.do(http.MethodPost, mcpPath, f.mint("ONS-30", 1)["mcp"], "application/json", strings.NewReader(`{}`)); resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("no tool server: %d, want 502", resp.StatusCode)
	}
}
