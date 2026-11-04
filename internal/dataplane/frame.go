package dataplane

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Frame types of the stdio flow. A frame is one type byte, a four-byte
// big-endian payload length, and the payload.
const (
	// FrameStdin carries bytes for the agent's standard input, from
	// Sortie to the worker.
	FrameStdin byte = 'I'
	// FrameStdinClosed closes the agent's standard input. It has no
	// payload.
	FrameStdinClosed byte = 'C'
	// FrameStdout carries bytes the agent wrote to its standard output,
	// from the worker to Sortie.
	FrameStdout byte = 'O'
	// FrameStderr carries bytes the agent wrote to its standard error.
	FrameStderr byte = 'E'
	// FrameExit reports that the agent exited. Its payload is the exit
	// status as a four-byte big-endian signed integer. It is the last
	// frame the worker sends.
	FrameExit byte = 'X'
	// FrameSortieFilesRequest asks the worker for the agent's .sortie
	// files, from Sortie to a worker on [StdioProtocolFiles]. It has no
	// payload.
	FrameSortieFilesRequest byte = 'S'
	// FrameSortieFiles answers it: a JSON object of each file the agent
	// wrote in .sortie, by its name, its content base64-encoded.
	FrameSortieFiles byte = 'F'
)

// MaxFramePayload bounds the payload of one frame.
const MaxFramePayload = 1 << 20

// ErrFrameTooLarge reports a frame whose payload exceeds
// [MaxFramePayload].
var ErrFrameTooLarge = errors.New("dataplane: stdio frame exceeds 1 MiB")

// WriteFrame writes one frame of type typ with payload to w.
func WriteFrame(w io.Writer, typ byte, payload []byte) error {
	if len(payload) > MaxFramePayload {
		return ErrFrameTooLarge
	}
	var head [5]byte
	head[0] = typ
	binary.BigEndian.PutUint32(head[1:], uint32(len(payload))) //nolint:gosec // G115: bounded by MaxFramePayload
	if _, err := w.Write(append(head[:], payload...)); err != nil {
		return err
	}
	return nil
}

// WriteExit writes the exit frame of status to w.
func WriteExit(w io.Writer, status int) error {
	var p [4]byte
	binary.BigEndian.PutUint32(p[:], uint32(int32(status))) //nolint:gosec // G115: an exit status fits in 32 bits
	return WriteFrame(w, FrameExit, p[:])
}

// ReadFrame reads one frame from r.
func ReadFrame(r io.Reader) (byte, []byte, error) {
	var head [5]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(head[1:])
	if n > MaxFramePayload {
		return 0, nil, ErrFrameTooLarge
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return 0, nil, err
	}
	return head[0], payload, nil
}

// ExitStatus decodes the payload of an exit frame.
func ExitStatus(payload []byte) (int, error) {
	if len(payload) != 4 {
		return 0, fmt.Errorf("dataplane: exit frame payload of %d bytes, want 4", len(payload))
	}
	return int(int32(binary.BigEndian.Uint32(payload))), nil //nolint:gosec // G115: decoding what WriteExit encoded
}
