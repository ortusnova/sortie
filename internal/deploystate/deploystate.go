// Package deploystate reads the deploy state file a deploy kit writes
// for the Sortie it deploys, and says whether that deploy is healthy: the
// last deploy did not fail, the deployed commit has not stayed behind
// main, and the kit still writes the file.
package deploystate

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// Statuses of a deploy.
const (
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
	StatusDeploying = "deploying"
)

// BehindGrace is how long the deployed commit may stay behind main
// before the deploy counts as behind: a merge to main is deployed within
// it.
const BehindGrace = 30 * time.Minute

// StaleAfter is how long the kit may go without writing the file before
// the state counts as stale: the kit writes it at least once each check.
const StaleAfter = time.Hour

// maxFileBytes caps the file; a longer one is refused.
const maxFileBytes = 64 << 10

// State is the content of the deploy state file.
type State struct {
	// Status is the last deploy's: "succeeded", "failed" or "deploying".
	Status string `json:"status"`

	// Commit is the commit of main deployed.
	Commit string `json:"commit"`

	// Main is the newest commit of main the kit saw.
	Main string `json:"main"`

	// BehindSince is when Commit first fell behind Main; nil while the
	// deployed commit is main.
	BehindSince *time.Time `json:"behind_since"`

	// Error says why the last deploy failed; empty otherwise.
	Error string `json:"error"`

	// UpdatedAt is when the kit last wrote the file.
	UpdatedAt time.Time `json:"updated_at"`
}

// Read reads and checks the file at path.
func Read(path string) (State, error) {
	f, err := os.Open(path) //nolint:gosec // G304: the operator names the deploy state file.
	if err != nil {
		return State{}, err
	}
	defer f.Close() //nolint:errcheck // read-only file
	raw, err := io.ReadAll(io.LimitReader(f, maxFileBytes+1))
	if err != nil {
		return State{}, err
	}
	if len(raw) > maxFileBytes {
		return State{}, fmt.Errorf("longer than %d bytes", maxFileBytes)
	}
	return Parse(raw)
}

// Parse reads raw as a deploy state file and checks its fields.
func Parse(raw []byte) (State, error) {
	var s State
	if err := json.Unmarshal(raw, &s); err != nil {
		return State{}, fmt.Errorf("not a deploy state object: %w", err)
	}
	switch s.Status {
	case StatusSucceeded, StatusFailed, StatusDeploying:
	case "":
		return State{}, errors.New("status is missing")
	default:
		return State{}, fmt.Errorf("status %q is not succeeded, failed or deploying", s.Status)
	}
	if s.UpdatedAt.IsZero() {
		return State{}, errors.New("updated_at is missing")
	}
	return s, nil
}

// Problem returns what is wrong with the deploy s describes at now, or ""
// when nothing is: the kit stopped writing the file, the last deploy
// failed, or the deployed commit has been behind main for longer than
// [BehindGrace].
func (s State) Problem(now time.Time) string {
	switch {
	case now.Sub(s.UpdatedAt) > StaleAfter:
		return fmt.Sprintf("the deploy kit has not written its state since %s", s.UpdatedAt.UTC().Format(time.RFC3339))
	case s.Status == StatusFailed:
		if reason := strings.TrimSpace(s.Error); reason != "" {
			return "the last deploy failed: " + reason
		}
		return "the last deploy failed"
	case s.BehindSince != nil && now.Sub(*s.BehindSince) > BehindGrace:
		return fmt.Sprintf("the deployed commit %s has been behind main %s since %s",
			short(s.Commit), short(s.Main), s.BehindSince.UTC().Format(time.RFC3339))
	}
	return ""
}

func short(commit string) string {
	if commit == "" {
		return "(unknown)"
	}
	if len(commit) > 12 {
		return commit[:12]
	}
	return commit
}
