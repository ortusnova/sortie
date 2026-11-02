// Package dataplane serves the data-plane flows Sortie itself answers for
// a remote job: the workspace seed and the returned issue branch on the
// bundles flow, the agent's standard streams on the stdio flow, the run's
// token usage on the usage flow, and the agent's requests to its own
// session's tool server on the mcp flow. Every request carries the job
// token of its flow, for that job and attempt; any other request is
// answered 401 and served nothing.
package dataplane

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sortie-ai/sortie/internal/jobtoken"
)

// The flows this package serves.
const (
	FlowStdio   jobtoken.Flow = "stdio"
	FlowBundles jobtoken.Flow = "bundles"
	FlowUsage   jobtoken.Flow = "usage"
	FlowMCP     jobtoken.Flow = "mcp"
)

// Prefix is the path every data-plane route sits under, followed by the
// flow's name: a flow URL that points at Sortie itself ends in
// Prefix+<flow>.
const Prefix = "/api/v1/flows/"

// Routes lists the path of every data-plane route, for a server that
// mounts [Plane.Handler] beside routes of its own.
var Routes = []string{
	Prefix + "bundles/jobs/{job}/attempts/{attempt}/seed",
	Prefix + "bundles/jobs/{job}/attempts/{attempt}/result",
	Prefix + "stdio/jobs/{job}/attempts/{attempt}",
	Prefix + "usage/jobs/{job}/attempts/{attempt}",
	Prefix + "mcp/jobs/{job}/attempts/{attempt}",
}

// StdioProtocol is the Upgrade token of the stdio flow.
const StdioProtocol = "sortie-stdio/1"

// MaxPRFile bounds the .sortie/pr.json a returned result carries.
const MaxPRFile = 64 << 10

// ErrClosed reports an attempt that ended before what was waited for
// arrived.
var ErrClosed = errors.New("dataplane: the attempt ended")

// Usage is one usage report: the attempt's token counts so far. Measured
// false reports spend whose amount the worker does not know.
type Usage struct {
	InputTokens      int64  `json:"input_tokens"`
	OutputTokens     int64  `json:"output_tokens"`
	CacheReadTokens  int64  `json:"cache_read_tokens"`
	CacheWriteTokens int64  `json:"cache_write_tokens"`
	Model            string `json:"model"`
	Measured         bool   `json:"measured"`
}

// Result is what the worker returned when the agent's session ended.
type Result struct {
	// Bundle is the path of the returned bundle of the issue branch, or
	// "" when the worker returned none because the machine has no issue
	// branch.
	Bundle string

	// PR is the agent's .sortie/pr.json, nil when the worker returned
	// none.
	PR []byte
}

// Options configures one attempt.
type Options struct {
	// Seed is the path of the workspace seed served on the bundles
	// flow; "" serves none.
	Seed string

	// Dir is where returned files are written. It must exist.
	Dir string

	// MaxBundleBytes caps the returned bundle.
	MaxBundleBytes int64

	// OnUsage receives each usage report, in arrival order. It must not
	// block.
	OnUsage func(Usage)

	// OnStdioBroken is called once when the worker's stdio stream ends
	// before the agent's exit was reported.
	OnStdioBroken func(error)

	// Logger receives diagnostics. Nil is [slog.Default].
	Logger *slog.Logger
}

type attemptKey struct {
	job     string
	attempt uint64
}

// Plane holds the open attempts and serves their flows. It is safe for
// concurrent use.
type Plane struct {
	tokens *jobtoken.Issuer

	mu       sync.Mutex
	attempts map[attemptKey]*Attempt
}

// New returns a plane that admits the job tokens of tokens.
func New(tokens *jobtoken.Issuer) *Plane {
	return &Plane{tokens: tokens, attempts: map[attemptKey]*Attempt{}}
}

// Attempt is one open attempt of a remote job.
type Attempt struct {
	plane  *Plane
	key    attemptKey
	opts   Options
	logger *slog.Logger

	listener net.Listener
	sockDir  string
	tools    *http.Transport

	mu         sync.Mutex
	result     *Result
	resultSet  chan struct{}
	worker     net.Conn
	workerIn   *bufio.Reader
	workerSet  chan struct{}
	session    net.Conn
	relaying   bool
	toolBearer string
	brokenOnce sync.Once
	closed     chan struct{}
	closeOnce  sync.Once
}

// Open opens attempt of job, so its flows are served, and listens on the
// session socket [RunBridge] joins. Opening an attempt of a job closes
// any earlier open attempt of it.
func (p *Plane) Open(job string, attempt int, opts Options) (*Attempt, error) {
	if attempt < 1 {
		return nil, fmt.Errorf("dataplane: attempt %d of %s, want 1 or more", attempt, job)
	}
	dir, err := os.MkdirTemp("", "sortie-stdio-")
	if err != nil {
		return nil, fmt.Errorf("dataplane: create the session socket directory: %w", err)
	}
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", filepath.Join(dir, "session.sock"))
	if err != nil {
		_ = os.RemoveAll(dir) //nolint:errcheck // best effort after a failure
		return nil, fmt.Errorf("dataplane: listen on the session socket: %w", err)
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	a := &Attempt{
		plane: p, key: attemptKey{job: job, attempt: uint64(attempt)}, opts: opts, logger: logger,
		listener: ln, sockDir: dir,
		resultSet: make(chan struct{}), workerSet: make(chan struct{}), closed: make(chan struct{}),
	}
	a.tools = &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", a.ToolSocket())
		},
	}
	p.mu.Lock()
	var earlier []*Attempt
	for k, old := range p.attempts {
		if k.job == job {
			earlier = append(earlier, old)
		}
	}
	p.attempts[a.key] = a
	p.mu.Unlock()
	for _, old := range earlier {
		old.Close()
	}
	go a.acceptSession()
	return a, nil
}

// Socket returns the path of the session socket.
func (a *Attempt) Socket() string { return a.listener.Addr().String() }

// ToolSocketName is the file name of [Attempt.ToolSocket].
const ToolSocketName = "tools.sock"

// ToolSocket returns the path of the tool socket: the session's tool
// server is reached there, and the mcp flow forwards each request it
// admits to it. Its directory belongs to the attempt alone.
func (a *Attempt) ToolSocket() string { return filepath.Join(a.sockDir, ToolSocketName) }

// AuthorizeTools records the bearer the tool server behind
// [Attempt.ToolSocket] accepts. The mcp flow presents it in place of the
// job token of each request it admits; until it is recorded the flow
// answers 503.
func (a *Attempt) AuthorizeTools(bearer string) {
	a.mu.Lock()
	a.toolBearer = bearer
	a.mu.Unlock()
}

// Done is closed when the attempt is closed.
func (a *Attempt) Done() <-chan struct{} { return a.closed }

// Close stops serving the attempt: its flows answer 404, its stdio
// stream and session socket close, and every waiter returns [ErrClosed].
// Returned files stay in Options.Dir for the caller to remove. Later
// calls do nothing.
func (a *Attempt) Close() {
	a.closeOnce.Do(func() {
		a.plane.mu.Lock()
		if a.plane.attempts[a.key] == a {
			delete(a.plane.attempts, a.key)
		}
		a.plane.mu.Unlock()
		close(a.closed)
		_ = a.listener.Close() //nolint:errcheck // closing
		a.mu.Lock()
		for _, c := range []net.Conn{a.worker, a.session} {
			if c != nil {
				_ = c.Close() //nolint:errcheck // closing
			}
		}
		a.toolBearer = ""
		a.mu.Unlock()
		a.tools.CloseIdleConnections()
		_ = os.RemoveAll(a.sockDir) //nolint:errcheck // a leftover socket directory only costs an inode
	})
}

// WaitStdio waits until the worker opened the stdio stream.
func (a *Attempt) WaitStdio(ctx context.Context) error {
	select {
	case <-a.workerSet:
		return nil
	case <-a.closed:
		return ErrClosed
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Returned reports what the worker returned, without waiting.
func (a *Attempt) Returned() (Result, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.result == nil {
		return Result{}, false
	}
	return *a.result, true
}

// Result waits for what the worker returns when the session ended. A
// result that arrived is returned even after the attempt closed.
func (a *Attempt) Result(ctx context.Context) (Result, error) {
	if res, ok := a.Returned(); ok {
		return res, nil
	}
	select {
	case <-a.resultSet:
		a.mu.Lock()
		defer a.mu.Unlock()
		return *a.result, nil
	case <-a.closed:
		if res, ok := a.Returned(); ok {
			return res, nil
		}
		return Result{}, ErrClosed
	case <-ctx.Done():
		return Result{}, context.Cause(ctx)
	}
}

// acceptSession takes the first connection on the session socket as the
// agent session and refuses every later one.
func (a *Attempt) acceptSession() {
	for {
		conn, err := a.listener.Accept()
		if err != nil {
			return
		}
		a.mu.Lock()
		if a.session != nil {
			a.mu.Unlock()
			_ = conn.Close() //nolint:errcheck // a second session is refused
			continue
		}
		a.session = conn
		a.mu.Unlock()
		a.startRelay()
	}
}

// attachWorker takes the worker's stdio stream, reporting false when the
// attempt already has one or is closed.
func (a *Attempt) attachWorker(conn net.Conn, in *bufio.Reader) bool {
	a.mu.Lock()
	select {
	case <-a.closed:
		a.mu.Unlock()
		return false
	default:
	}
	if a.worker != nil {
		a.mu.Unlock()
		return false
	}
	a.worker, a.workerIn = conn, in
	close(a.workerSet)
	a.mu.Unlock()
	a.startRelay()
	return true
}

// startRelay joins the session and the worker's stream once both are
// there.
func (a *Attempt) startRelay() {
	a.mu.Lock()
	if a.relaying || a.session == nil || a.worker == nil {
		a.mu.Unlock()
		return
	}
	a.relaying = true
	session, worker, in := a.session, a.worker, a.workerIn
	a.mu.Unlock()

	// The session's frames, the agent's input, go to the worker as
	// they are. When the session goes, as when Sortie stops it, the
	// worker learns that the agent's input closed.
	go func() {
		_, _ = io.Copy(worker, session)               //nolint:errcheck // either side closing ends the copy
		_ = WriteFrame(worker, FrameStdinClosed, nil) //nolint:errcheck // the worker may be gone
	}()
	go a.relayWorker(worker, in, session)
}

// relayWorker forwards the worker's frames to the session until the
// agent's exit. A stream that ends before it is broken: the session is
// left open and silent, as a stalled agent leaves it, and OnStdioBroken
// is told.
func (a *Attempt) relayWorker(worker net.Conn, in *bufio.Reader, session net.Conn) {
	defer worker.Close() //nolint:errcheck // the stream is done
	for {
		typ, payload, err := ReadFrame(in)
		if err == nil && typ != FrameStdout && typ != FrameStderr && typ != FrameExit {
			err = fmt.Errorf("dataplane: unexpected stdio frame type %q from the worker", typ)
		}
		if err != nil {
			a.broken(err)
			return
		}
		if werr := WriteFrame(session, typ, payload); werr != nil {
			return
		}
		if typ == FrameExit {
			return
		}
	}
}

func (a *Attempt) broken(err error) {
	select {
	case <-a.closed:
		return
	default:
	}
	a.brokenOnce.Do(func() {
		a.logger.Warn("remote job stdio stream broke before the agent's exit",
			slog.String("job", a.key.job), slog.Uint64("attempt", a.key.attempt), slog.Any("error", err))
		if a.opts.OnStdioBroken != nil {
			a.opts.OnStdioBroken(err)
		}
	})
}

// Handler serves the data-plane routes:
//
//	GET  /api/v1/flows/bundles/jobs/{job}/attempts/{attempt}/seed    the workspace seed
//	POST /api/v1/flows/bundles/jobs/{job}/attempts/{attempt}/result  multipart: bundle, pr; 204
//	GET  /api/v1/flows/stdio/jobs/{job}/attempts/{attempt}           Upgrade: sortie-stdio/1; 101
//	POST /api/v1/flows/usage/jobs/{job}/attempts/{attempt}           body Usage; 204
//	*    /api/v1/flows/mcp/jobs/{job}/attempts/{attempt}             the session's tool server, as at /mcp
func (p *Plane) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+Routes[0], p.flow(FlowBundles, serveSeed))
	mux.HandleFunc("POST "+Routes[1], p.flow(FlowBundles, receiveResult))
	mux.HandleFunc("GET "+Routes[2], p.flow(FlowStdio, serveStdio))
	mux.HandleFunc("POST "+Routes[3], p.flow(FlowUsage, receiveUsage))
	mux.HandleFunc(Routes[4], p.flow(FlowMCP, serveTools))
	return mux
}

// flow admits a request only with a live token of flow for the job and
// attempt its path names, and then only for an open attempt.
func (p *Plane) flow(flow jobtoken.Flow, h func(a *Attempt, w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		job := r.PathValue("job")
		attempt, err := strconv.ParseUint(r.PathValue("attempt"), 10, 64)
		g, lookupErr := p.tokens.Lookup(bearer(r))
		if err != nil || lookupErr != nil || g.JobID != job || g.Attempt != attempt || g.Flow != flow {
			w.Header().Set("WWW-Authenticate", `Bearer realm="sortie-job"`)
			problem(w, http.StatusUnauthorized, "invalid_token", "the request carries no live "+string(flow)+" token of this job's attempt")
			return
		}
		p.mu.Lock()
		a := p.attempts[attemptKey{job: job, attempt: attempt}]
		p.mu.Unlock()
		if a == nil {
			problem(w, http.StatusNotFound, "not_found", "the attempt is not open")
			return
		}
		h(a, w, r)
	}
}

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const p = "Bearer "
	if len(h) > len(p) && strings.EqualFold(h[:len(p)], p) {
		return strings.TrimSpace(h[len(p):])
	}
	return ""
}

func problem(w http.ResponseWriter, code int, kind, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": kind, "message": msg}}) //nolint:errcheck // the client may be gone
}

// unbounded lifts the server's read and write deadlines from a request
// that moves a bundle or a stream.
func unbounded(w http.ResponseWriter) {
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Time{})  //nolint:errcheck // unsupported by a test recorder only
	_ = rc.SetWriteDeadline(time.Time{}) //nolint:errcheck // unsupported by a test recorder only
}

func serveSeed(a *Attempt, w http.ResponseWriter, r *http.Request) {
	if a.opts.Seed == "" {
		problem(w, http.StatusNotFound, "no_seed", "the job has no workspace seed; its workflow configures no repository")
		return
	}
	f, err := os.Open(a.opts.Seed)
	if err != nil {
		problem(w, http.StatusInternalServerError, "internal", "the workspace seed cannot be read")
		return
	}
	defer f.Close() //nolint:errcheck // read only
	unbounded(w)
	w.Header().Set("Content-Type", "application/x-git-bundle")
	if st, err := f.Stat(); err == nil {
		w.Header().Set("Content-Length", strconv.FormatInt(st.Size(), 10))
	}
	_, _ = io.Copy(w, f) //nolint:errcheck // the worker may be gone
}

func receiveResult(a *Attempt, w http.ResponseWriter, r *http.Request) {
	select {
	case <-a.resultSet:
		problem(w, http.StatusConflict, "already_returned", "the attempt's result was already returned")
		return
	default:
	}
	unbounded(w)
	r.Body = http.MaxBytesReader(w, r.Body, a.opts.MaxBundleBytes+MaxPRFile+(64<<10))
	parts, err := r.MultipartReader()
	if err != nil {
		problem(w, http.StatusBadRequest, "invalid_result", "the result must be multipart/form-data with the parts bundle and pr")
		return
	}
	var res Result
	cleanup := func() {
		if res.Bundle != "" {
			_ = os.Remove(res.Bundle) //nolint:errcheck // a refused upload
		}
	}
	for {
		part, err := parts.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			cleanup()
			problem(w, http.StatusBadRequest, "invalid_result", "the result body cannot be read: "+err.Error())
			return
		}
		switch part.FormName() {
		case "bundle":
			if res.Bundle != "" {
				cleanup()
				problem(w, http.StatusBadRequest, "invalid_result", "the result carries two bundles")
				return
			}
			path, code, msg := a.saveBundle(part)
			if code != 0 {
				cleanup()
				problem(w, code, "invalid_result", msg)
				return
			}
			res.Bundle = path
		case "pr":
			body, err := io.ReadAll(io.LimitReader(part, MaxPRFile+1))
			if err != nil || len(body) > MaxPRFile {
				cleanup()
				problem(w, http.StatusRequestEntityTooLarge, "invalid_result", "the pr part exceeds 64 KiB")
				return
			}
			res.PR = body
		default:
			cleanup()
			problem(w, http.StatusBadRequest, "invalid_result", fmt.Sprintf("unknown result part %q; valid: bundle, pr", part.FormName()))
			return
		}
	}
	a.mu.Lock()
	if a.result != nil {
		a.mu.Unlock()
		cleanup()
		problem(w, http.StatusConflict, "already_returned", "the attempt's result was already returned")
		return
	}
	a.result = &res
	close(a.resultSet)
	a.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// saveBundle writes a returned bundle under Options.Dir, refusing one
// larger than MaxBundleBytes. A non-zero code is the refusal's status.
func (a *Attempt) saveBundle(r io.Reader) (string, int, string) {
	f, err := os.CreateTemp(a.opts.Dir, "returned-*.bundle")
	if err != nil {
		return "", http.StatusInternalServerError, "the bundle cannot be stored"
	}
	n, err := io.Copy(f, io.LimitReader(r, a.opts.MaxBundleBytes+1))
	closeErr := f.Close()
	switch {
	case err != nil || closeErr != nil:
		_ = os.Remove(f.Name()) //nolint:errcheck // a failed upload
		return "", http.StatusBadRequest, "the bundle cannot be read"
	case n > a.opts.MaxBundleBytes:
		_ = os.Remove(f.Name()) //nolint:errcheck // a refused upload
		return "", http.StatusRequestEntityTooLarge, fmt.Sprintf("the bundle exceeds %d bytes", a.opts.MaxBundleBytes)
	}
	return f.Name(), 0, ""
}

func serveStdio(a *Attempt, w http.ResponseWriter, r *http.Request) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), StdioProtocol) {
		w.Header().Set("Upgrade", StdioProtocol)
		problem(w, http.StatusUpgradeRequired, "upgrade_required", "the stdio flow needs Upgrade: "+StdioProtocol)
		return
	}
	select {
	case <-a.workerSet:
		problem(w, http.StatusConflict, "stdio_open", "the attempt's stdio stream is already open")
		return
	default:
	}
	conn, rw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		problem(w, http.StatusInternalServerError, "internal", "the stream cannot be taken over")
		return
	}
	_ = conn.SetDeadline(time.Time{}) //nolint:errcheck // a stream has no deadline
	if _, err := rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: " + StdioProtocol + "\r\n\r\n"); err != nil || rw.Flush() != nil {
		_ = conn.Close() //nolint:errcheck // the worker is gone
		return
	}
	if !a.attachWorker(conn, rw.Reader) {
		_ = conn.Close() //nolint:errcheck // a second stream is refused
	}
}

func receiveUsage(a *Attempt, w http.ResponseWriter, r *http.Request) {
	var body struct {
		InputTokens      *int64  `json:"input_tokens"`
		OutputTokens     *int64  `json:"output_tokens"`
		CacheReadTokens  *int64  `json:"cache_read_tokens"`
		CacheWriteTokens *int64  `json:"cache_write_tokens"`
		Model            *string `json:"model"`
		Measured         *bool   `json:"measured"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&body); err != nil {
		problem(w, http.StatusBadRequest, "invalid_usage", "the usage body must be a JSON object")
		return
	}
	u := Usage{Measured: body.Measured == nil || *body.Measured}
	if body.Model != nil {
		u.Model = *body.Model
	}
	for _, f := range []struct {
		name string
		src  *int64
		dst  *int64
	}{
		{"input_tokens", body.InputTokens, &u.InputTokens},
		{"output_tokens", body.OutputTokens, &u.OutputTokens},
		{"cache_read_tokens", body.CacheReadTokens, &u.CacheReadTokens},
		{"cache_write_tokens", body.CacheWriteTokens, &u.CacheWriteTokens},
	} {
		if f.src == nil {
			continue
		}
		if *f.src < 0 {
			problem(w, http.StatusBadRequest, "invalid_usage", f.name+" must not be negative")
			return
		}
		*f.dst = *f.src
	}
	if a.opts.OnUsage != nil {
		a.opts.OnUsage(u)
	}
	w.WriteHeader(http.StatusNoContent)
}

// toolHost is the loopback host the tool server sees in every forwarded
// request, as in a request from an agent on Sortie's host.
const toolHost = "127.0.0.1"

// serveTools forwards an admitted request of the mcp flow to the tool
// server of the attempt's session as a request to its /mcp: the job
// token is replaced with the tool server's bearer, and the answer streams
// back as it arrives.
func serveTools(a *Attempt, w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	bearer := a.toolBearer
	a.mu.Unlock()
	if bearer == "" {
		problem(w, http.StatusServiceUnavailable, "no_tool_server", "the attempt's session has no tool server")
		return
	}
	unbounded(w)
	proxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = "http"
			pr.Out.URL.Host = toolHost
			pr.Out.URL.Path, pr.Out.URL.RawPath = "/mcp", ""
			pr.Out.Host = toolHost
			pr.Out.Header.Set("Authorization", "Bearer "+bearer)
		},
		Transport:     a.tools,
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			a.logger.Warn("remote job tool request did not reach the session's tool server",
				slog.String("job", a.key.job), slog.Uint64("attempt", a.key.attempt), slog.Any("error", err))
			problem(w, http.StatusBadGateway, "tool_server_unreachable", "the session's tool server cannot be reached")
		},
	}
	proxy.ServeHTTP(w, r)
}
