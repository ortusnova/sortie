package agentcore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/sortie-ai/sortie/internal/agent/sshutil"
)

// toolChannelDialTimeout bounds the connection from the channel's host
// socket to the relay's loopback port.
const toolChannelDialTimeout = 5 * time.Second

// toolChannel serves a carrier session's tool channel: it listens on the
// channel's host socket and joins every connection to the relay's loopback
// port, so the agent reaches the relay at the channel's guest port. It
// carries bytes only; the relay's bearer authorizes each request.
type toolChannel struct {
	ctx       context.Context
	cancel    context.CancelFunc
	logger    *slog.Logger
	listener  net.Listener
	path      string
	guestPort int
	target    string

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup
}

// openChannel opens the carrier's tool channel to the relay. A carrier
// without a channel, a relay that never announced its listener, or a socket
// that cannot be opened leaves the session without Sortie's tool server, with
// the gap declared.
func (s *ToolSession) openChannel(ctx context.Context) {
	relay := s.currentRelay()
	if relay == nil || relay.HostPort() == 0 {
		return
	}
	channel, ok := s.carrier.ToolChannel()
	if !ok || channel.GuestPort < 1 || channel.GuestPort > 65535 || channel.HostSocket == "" {
		s.logger.Warn("the execution environment offers no tool channel")
		relay.Observe(sshutil.ForwardOutcome{Status: sshutil.ForwardRefused})
		return
	}
	opened, err := listenToolChannel(ctx, channel.HostSocket, channel.GuestPort, relay.HostPort(), s.logger)
	if err != nil {
		s.logger.Warn("could not open the tool channel", slog.Any("error", err))
		relay.Observe(sshutil.ForwardOutcome{Status: sshutil.ForwardUnconfirmed})
		return
	}
	s.mu.Lock()
	s.channel = opened
	s.mu.Unlock()
	if channel.Authorize != nil {
		if ep, ok := relay.LocalEndpoint(); ok {
			channel.Authorize(ep.Bearer)
		}
	}
}

// channelOutcome returns the outcome a carrier session's endpoint renders
// from: allocated at the guest port while the channel is open, and
// unconfirmed otherwise.
func (s *ToolSession) channelOutcome() sshutil.ForwardOutcome {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.channel == nil {
		return sshutil.ForwardOutcome{Status: sshutil.ForwardUnconfirmed}
	}
	return sshutil.ForwardOutcome{Status: sshutil.ForwardAllocated, RemotePort: s.channel.guestPort}
}

// listenToolChannel listens on path, replacing a socket an earlier session
// left there, and starts joining its connections to the loopback hostPort.
// It refuses to replace anything at path that is not a socket.
// The channel outlives ctx, which bounds only the listen itself; close ends
// it.
func listenToolChannel(ctx context.Context, path string, guestPort, hostPort int, logger *slog.Logger) (*toolChannel, error) {
	if info, err := os.Lstat(path); err == nil {
		if info.Mode().Type() != fs.ModeSocket {
			return nil, fmt.Errorf("tool channel %s exists and is not a socket", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("remove the tool channel an earlier session left: %w", err)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("inspect tool channel: %w", err)
	}
	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "unix", path)
	if err != nil {
		return nil, fmt.Errorf("listen on tool channel: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		listener.Close() //nolint:errcheck,gosec // best-effort; the chmod error is what the caller needs
		return nil, fmt.Errorf("restrict tool channel: %w", err)
	}
	channelCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c := &toolChannel{
		ctx:       channelCtx,
		cancel:    cancel,
		logger:    logger,
		listener:  listener,
		path:      path,
		guestPort: guestPort,
		target:    net.JoinHostPort("127.0.0.1", strconv.Itoa(hostPort)),
		conns:     map[net.Conn]struct{}{},
	}
	c.wg.Add(1)
	go c.serve()
	return c, nil
}

func (c *toolChannel) serve() {
	defer c.wg.Done()
	for {
		conn, err := c.listener.Accept()
		if err != nil {
			return
		}
		if !c.track(conn) {
			conn.Close() //nolint:errcheck,gosec // the channel is closing
			return
		}
		c.wg.Add(1)
		go c.join(conn)
	}
}

// join copies bytes both ways between conn and a new connection to the
// relay until either side ends.
func (c *toolChannel) join(conn net.Conn) {
	defer c.wg.Done()
	defer c.untrack(conn)
	dialer := net.Dialer{Timeout: toolChannelDialTimeout}
	upstream, err := dialer.DialContext(c.ctx, "tcp", c.target)
	if err != nil {
		c.logger.Warn("tool channel could not reach the tool server", slog.Any("error", err))
		conn.Close() //nolint:errcheck,gosec // best-effort; the dial error is logged
		return
	}
	if !c.track(upstream) {
		upstream.Close() //nolint:errcheck,gosec // the channel is closing
		conn.Close()     //nolint:errcheck,gosec // the channel is closing
		return
	}
	defer c.untrack(upstream)

	done := make(chan struct{}, 2)
	pipe := func(dst, src net.Conn) {
		io.Copy(dst, src) //nolint:errcheck,gosec // either side ending ends the connection
		done <- struct{}{}
	}
	go pipe(upstream, conn)
	go pipe(conn, upstream)
	<-done
	conn.Close()     //nolint:errcheck,gosec // ends the other copy
	upstream.Close() //nolint:errcheck,gosec // ends the other copy
	<-done
}

func (c *toolChannel) track(conn net.Conn) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	c.conns[conn] = struct{}{}
	return true
}

func (c *toolChannel) untrack(conn net.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.conns, conn)
}

// close stops listening, ends every joined connection, waits for the
// channel's goroutines, and removes the socket.
func (c *toolChannel) close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.cancel()
	for conn := range c.conns {
		conn.Close() //nolint:errcheck,gosec // ends the copy that reads it
	}
	c.mu.Unlock()
	c.listener.Close() //nolint:errcheck,gosec // ends the accept loop
	c.wg.Wait()
	if err := os.Remove(c.path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		c.logger.Debug("could not remove the tool channel socket", slog.Any("error", err))
	}
}
