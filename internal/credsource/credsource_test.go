package credsource

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestRead_FileIsReadAnewEachTime(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "token")
	src := Source{Name: "anthropic", File: path}

	writeFile(t, path, "first-token\n")
	got, err := Read(context.Background(), src)
	if err != nil || got != "first-token" {
		t.Fatalf("Read() = %q, %v, want first-token, nil", got, err)
	}

	writeFile(t, path, "  second-token  \n")
	got, err = Read(context.Background(), src)
	if err != nil || got != "second-token" {
		t.Fatalf("Read() after the change = %q, %v, want second-token, nil", got, err)
	}
}

func TestRead_CommandIsRunAnewEachTime(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "auth.json")
	src := Source{Name: "anthropic", Command: "cat '" + path + "'"}

	writeFile(t, path, "first-token")
	got, err := Read(context.Background(), src)
	if err != nil || got != "first-token" {
		t.Fatalf("Read() = %q, %v, want first-token, nil", got, err)
	}

	writeFile(t, path, "second-token\n")
	got, err = Read(context.Background(), src)
	if err != nil || got != "second-token" {
		t.Fatalf("Read() after the change = %q, %v, want second-token, nil", got, err)
	}
}

func TestRead_SourceThatYieldsNothingIsAnError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	empty := filepath.Join(dir, "empty")
	writeFile(t, empty, " \n")
	multi := filepath.Join(dir, "multi")
	writeFile(t, multi, "a\nb\n")
	big := filepath.Join(dir, "big")
	writeFile(t, big, strings.Repeat("x", maxValueBytes+1))

	tests := []struct {
		name       string
		src        Source
		wantSource string
		wantReason string
	}{
		{"missing file", Source{Name: "c", File: filepath.Join(dir, "missing")}, "file " + filepath.Join(dir, "missing"), "no such file"},
		{"directory", Source{Name: "c", File: dir}, "file " + dir, "not a regular file"},
		{"empty file", Source{Name: "c", File: empty}, "file " + empty, "empty"},
		{"multi-line file", Source{Name: "c", File: multi}, "file " + multi, "more than one line"},
		{"oversized file", Source{Name: "c", File: big}, "file " + big, "longer than"},
		{"silent command", Source{Name: "c", Command: "true"}, "command", "empty"},
		{"failing command", Source{Name: "c", Command: "echo login expired >&2; exit 3"}, "command", "login expired"},
		{"both", Source{Name: "c", File: empty, Command: "true"}, "file and command", "one source"},
		{"neither", Source{Name: "c"}, "none", "no file or command"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Read(context.Background(), tt.src)
			if got != "" {
				t.Errorf("Read() value = %q, want empty", got)
			}
			var srcErr *Error
			if !errors.As(err, &srcErr) {
				t.Fatalf("Read() error = %v, want *Error", err)
			}
			if srcErr.Credential != "c" || srcErr.Source != tt.wantSource || !strings.Contains(srcErr.Reason, tt.wantReason) {
				t.Errorf("Read() error = %+v, want credential c, source %q, reason containing %q", srcErr, tt.wantSource, tt.wantReason)
			}
			if !strings.Contains(err.Error(), "yielded no value") {
				t.Errorf("Error() = %q, want it to say the source yielded no value", err)
			}
		})
	}
}

func TestRead_CancelledContextFailsTheCommand(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Read(ctx, Source{Name: "c", Command: "echo token"})
	var srcErr *Error
	if !errors.As(err, &srcErr) || !errors.Is(err, context.Canceled) {
		t.Fatalf("Read() error = %v, want an *Error wrapping context.Canceled", err)
	}
}
