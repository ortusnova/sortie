//go:build unix

package orchestrator

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/sortie-ai/sortie/internal/failcause"
	"github.com/sortie-ai/sortie/internal/redact"
)

// workflowScopeRefusal is the line GitHub gives for a push its token's
// scopes do not allow.
const workflowScopeRefusal = "refusing to allow a Personal Access Token to create or update workflow `.github/workflows/talos-installer.yml` without `workflow` scope"

// refuseEveryPush gives the bare repository upstream a pre-receive hook
// that writes a line of filler bytes, workflowScopeRefusal, and a line
// naming token, and refuses the push.
func refuseEveryPush(t *testing.T, upstream, token string, filler int) {
	t.Helper()
	hook := "#!/bin/sh\n" +
		"head -c " + strconv.Itoa(filler) + " /dev/zero | tr '\\0' 'x'; echo\n" +
		"echo '" + workflowScopeRefusal + "'\n" +
		"echo 'the token " + token + " lacks the workflow scope'\n" +
		"exit 1\n"
	// The hook writes to standard output, which git relays to the
	// pusher's standard error as remote lines.
	if err := os.MkdirAll(filepath.Join(upstream, "hooks"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(upstream, "hooks", "pre-receive"), []byte(hook), 0o700); err != nil { //nolint:gosec // G306: a hook must be executable
		t.Fatal(err)
	}
}

func TestRefusedPushKeepsItsWholeCauseInTheRunRecord(t *testing.T) {
	f := newGitFixture(t)
	upstream := f.upstream()
	const token = "ghp_orchestratorRefusedPushToken42"
	redact.Add("test.orchestrator refused push token", token)
	refuseEveryPush(t, upstream, token, 3*failcause.MaxBytes)

	c := runColocatedOn(t, upstream)
	if c.result.ExitKind != WorkerExitError {
		t.Fatalf("ExitKind = %v, error %v; want the refused push to fail the run", c.result.ExitKind, c.result.Error)
	}
	if refs := f.git(upstream, "for-each-ref", "--format=%(refname)", "refs/heads/sortie/"); refs != "" {
		t.Errorf("upstream issue branches = %q, want the push refused", refs)
	}

	store := &mockExitStore{}
	state := exitStateWithIssue(t, c.result.IssueID, "To Do")
	HandleWorkerExit(state, c.result, defaultExitParams(t, store))
	t.Cleanup(func() { CancelRetry(state, c.result.IssueID) })

	if len(store.runHistories) != 1 {
		t.Fatalf("run history rows = %d, want 1", len(store.runHistories))
	}
	row := store.runHistories[0]
	if row.Status != "failed" || row.Error == nil || !strings.Contains(*row.Error, "[remote rejected] (pre-receive hook declined)") {
		t.Errorf("run history = status %q, error %v; want failed naming the remote's reason", row.Status, row.Error)
	}
	for _, want := range []string{
		"git push: exit status 1",
		"stdout:\nTo " + upstream + "\n!\trefs/sortie/incoming/ONS-30:refs/heads/sortie/ONS-30\t[remote rejected] (pre-receive hook declined)",
		"stderr:\n",
		"remote: " + workflowScopeRefusal,
		"remote: the token " + redact.Marker + " lacks the workflow scope",
		"failed to push some refs",
	} {
		if !strings.Contains(row.ErrorDetail, want) {
			t.Errorf("ErrorDetail = %q, want it to hold %q", row.ErrorDetail, want)
		}
	}
	if strings.Contains(row.ErrorDetail, token) || (row.Error != nil && strings.Contains(*row.Error, token)) {
		t.Error("the run record holds the token, want it masked")
	}
	if len(row.ErrorDetail) > failcause.MaxBytes {
		t.Errorf("len(ErrorDetail) = %d, want at most %d", len(row.ErrorDetail), failcause.MaxBytes)
	}
}
