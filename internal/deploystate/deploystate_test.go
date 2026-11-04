package deploystate

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

var now = time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)

func TestRead_TheKitsFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "deploy.json")
	content := `{"status":"succeeded","commit":"0123456789abcdef","main":"0123456789abcdef","behind_since":null,"error":"","updated_at":"2026-11-03T11:58:00Z"}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := Read(path)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if s.Status != StatusSucceeded || s.Commit != "0123456789abcdef" || s.BehindSince != nil || !s.UpdatedAt.Equal(now.Add(-2*time.Minute)) {
		t.Errorf("Read() = %+v", s)
	}
	if problem := s.Problem(now); problem != "" {
		t.Errorf("Problem() = %q, want none", problem)
	}
}

func TestRead_RefusesWhatIsNotADeployState(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if _, err := Read(filepath.Join(dir, "missing.json")); err == nil {
		t.Error("Read(missing file) error = nil")
	}
	for name, content := range map[string]string{
		"not-json":       "deployed",
		"no-status":      `{"updated_at":"2026-11-03T11:58:00Z"}`,
		"unknown-status": `{"status":"ok","updated_at":"2026-11-03T11:58:00Z"}`,
		"no-updated-at":  `{"status":"succeeded"}`,
	} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if s, err := Read(path); err == nil {
			t.Errorf("Read(%s) = %+v, want an error", name, s)
		}
	}
}

func TestProblem(t *testing.T) {
	t.Parallel()

	at := func(d time.Duration) *time.Time { since := now.Add(-d); return &since }
	fresh := now.Add(-time.Minute)
	for _, tc := range []struct {
		name  string
		state State
		want  string
	}{
		{"deploying", State{Status: StatusDeploying, UpdatedAt: fresh}, ""},
		{"behind within the grace", State{Status: StatusSucceeded, Commit: "aaa", Main: "bbb", BehindSince: at(10 * time.Minute), UpdatedAt: fresh}, ""},
		{"behind for long", State{Status: StatusSucceeded, Commit: "aaaaaaaaaaaaaaaa", Main: "bbb", BehindSince: at(2 * time.Hour), UpdatedAt: fresh},
			"the deployed commit aaaaaaaaaaaa has been behind main bbb since 2026-11-03T10:00:00Z"},
		{"failed", State{Status: StatusFailed, Error: " make build failed\n", UpdatedAt: fresh}, "the last deploy failed: make build failed"},
		{"failed without a reason", State{Status: StatusFailed, UpdatedAt: fresh}, "the last deploy failed"},
		{"stale", State{Status: StatusSucceeded, UpdatedAt: now.Add(-2 * time.Hour)}, "the deploy kit has not written its state since 2026-11-03T10:00:00Z"},
	} {
		if got := tc.state.Problem(now); got != tc.want {
			t.Errorf("%s: Problem() = %q, want %q", tc.name, got, tc.want)
		}
	}
}
