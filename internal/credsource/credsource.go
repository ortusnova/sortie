// Package credsource reads the value of a credential from its source: a
// host file, or a host command's standard output. A source is read
// anew on every call, so a credential refreshed on the host reaches the
// next read without a restart.
package credsource

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/agent/procutil"
	"github.com/sortie-ai/sortie/internal/redact"
)

// Bound is how long a command source may run before its read fails.
const Bound = 30 * time.Second

// maxValueBytes caps what a source may yield; a longer value is refused.
const maxValueBytes = 64 << 10

// stderrBytes caps the tail of a failed command's standard error an
// error carries.
const stderrBytes = 512

// Source names where a credential's value comes from. Exactly one of
// File and Command is set.
type Source struct {
	// Name names the credential in errors.
	Name string

	// File is an absolute host path whose content is the value.
	File string

	// Command is a shell command run with sh -c on the host, in
	// Sortie's environment; its standard output is the value.
	Command string
}

// Error reports a source that yielded no value. It never holds the
// value.
type Error struct {
	// Credential is the credential's name.
	Credential string

	// Source describes the source: "file <path>" or "command".
	Source string

	// Reason says why the source yielded no value.
	Reason string

	Err error
}

func (e *Error) Error() string {
	return fmt.Sprintf("credential %q: its source (%s) yielded no value: %s", e.Credential, e.Source, e.Reason)
}

func (e *Error) Unwrap() error { return e.Err }

// Read reads s once and returns its value with surrounding white space
// removed, registered with [redact] so that no log shows it. A source
// that cannot be read, a command that fails or outlives [Bound], an
// empty value, a value of more than one line, and a value longer than
// 64 KiB are each an [*Error].
func Read(ctx context.Context, s Source) (string, error) {
	var (
		raw  []byte
		desc string
		err  error
	)
	switch {
	case s.File != "" && s.Command != "":
		return "", &Error{Credential: s.Name, Source: "file and command", Reason: "a credential has one source"}
	case s.File != "":
		desc = "file " + s.File
		raw, err = readFile(s.File)
	case s.Command != "":
		desc = "command"
		raw, err = runCommand(ctx, s.Command)
	default:
		return "", &Error{Credential: s.Name, Source: "none", Reason: "no file or command is set"}
	}
	if err != nil {
		return "", &Error{Credential: s.Name, Source: desc, Reason: err.Error(), Err: err}
	}
	value := strings.TrimSpace(string(raw))
	switch {
	case value == "":
		return "", &Error{Credential: s.Name, Source: desc, Reason: "the value is empty"}
	case strings.ContainsAny(value, "\r\n\x00"):
		return "", &Error{Credential: s.Name, Source: desc, Reason: "the value holds more than one line"}
	}
	redact.Add("credential "+s.Name, value)
	return value, nil
}

func readFile(path string) ([]byte, error) {
	f, err := os.Open(path) //nolint:gosec // G304: the operator names the credential file.
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck // read-only file
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("not a regular file")
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxValueBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > maxValueBytes {
		return nil, fmt.Errorf("longer than %d bytes", maxValueBytes)
	}
	return raw, nil
}

func runCommand(parent context.Context, command string) ([]byte, error) {
	if err := parent.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, Bound)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", command) //nolint:gosec // G204: the operator configures the credential command, run on the host by design.
	// The value is read unmasked: a registered value, such as the one
	// the source gave last time, must not come back as a mask.
	stdout := &cappedWriter{max: maxValueBytes}
	stderr := procutil.NewTailBuffer(stderrBytes)
	result, err := procutil.RunCapture(ctx, cmd, procutil.DefaultStopGrace, procutil.CaptureParams{Stdout: stdout, Stderr: stderr})
	if err == nil {
		err = result.WaitErr
	}
	if err == nil && !result.OutputComplete {
		err = errors.New("the command's output was not fully read")
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	if ctx.Err() != nil {
		return nil, fmt.Errorf("the command did not finish within %s: %w", Bound, ctx.Err())
	}
	if err != nil {
		if msg := strings.TrimSpace(string(stderr.Bytes())); msg != "" {
			return nil, fmt.Errorf("%w: %s", err, msg)
		}
		return nil, err
	}
	if stdout.overflow {
		return nil, fmt.Errorf("the command printed more than %d bytes", maxValueBytes)
	}
	return stdout.buf.Bytes(), nil
}

// cappedWriter keeps up to max bytes and records whether more arrived.
type cappedWriter struct {
	buf      bytes.Buffer
	max      int
	overflow bool
}

func (c *cappedWriter) Write(p []byte) (int, error) {
	if room := c.max - c.buf.Len(); len(p) > room {
		c.overflow = true
		if room > 0 {
			c.buf.Write(p[:room]) //nolint:errcheck // bytes.Buffer.Write never returns an error
		}
		return len(p), nil
	}
	c.buf.Write(p) //nolint:errcheck // bytes.Buffer.Write never returns an error
	return len(p), nil
}
