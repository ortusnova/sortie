package orchestrator

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
)

// healthClockAt makes the health checks of h read base advanced by the
// returned function's argument.
func healthClockAt(h *runHarness, base time.Time) func(time.Duration) {
	var mu sync.Mutex
	offset := time.Duration(0)
	h.o.healthNow = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return base.Add(offset)
	}
	return func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		offset = d
	}
}

// healthHarness is a run harness whose workflow runs under env with one
// agent kind, kind-a.
func healthHarness(t *testing.T, env config.EnvironmentConfig, candidates ...domain.Issue) *runHarness {
	t.Helper()
	cfg := runConfig(t, map[string]any{"agent": map[string]any{"kind": "kind-a", "command": "a-cmd"}})
	cfg.Agent.MaxConcurrentAgents = 4
	cfg.Environment = env
	h := newRunHarness(t, cfg, "kind-a")
	h.state.MaxConcurrentAgents = 4
	h.tracker.candidates = candidates
	h.build()
	h.o.hostName = "taichi"
	t.Cleanup(h.state.WorkerWg.Wait)
	return h
}

// healthFaults returns the open faults a health check recorded, as the
// state API serves them.
func healthFaults(h *runHarness) []SnapshotPlatformFault {
	var out []SnapshotPlatformFault
	for _, f := range snapshotPlatformFaults(h.state) {
		if f.Step == healthStep {
			out = append(out, f)
		}
	}
	return out
}

func heldStories(f SnapshotPlatformFault) []string {
	var out []string
	for _, h := range f.Holds {
		out = append(out, h.Identifier)
	}
	return out
}

func requireNotRunning(t *testing.T, h *runHarness, id string) {
	t.Helper()
	if _, running := h.state.Running[id]; running {
		t.Fatalf("%s is running, want it held before any session", id)
	}
	if started := h.started(); len(started) != 0 {
		t.Fatalf("sessions started = %v, want none", started)
	}
}

func writeHealthFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// Scenario: A credential near expiry. The Claude token Sortie hands to
// sessions expires within 30 minutes and no fresher one exists: the
// workbench shows a question that names the login to renew, before any
// session fails. Once the login is renewed the story runs.
func TestHealth_CredentialNearExpiryAsksForTheLoginBeforeAnySession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	now := time.Now().UTC()
	dir := t.TempDir()
	expiresFile := filepath.Join(dir, "expires")
	writeHealthFile(t, expiresFile, now.Add(20*time.Minute).Format(time.RFC3339))
	h := healthHarness(t, config.EnvironmentConfig{}, candidate("issue-70", "ONS-70"))
	cfg := h.wm.Config()
	cfg.Health.Credentials = []config.HealthCredential{{
		Name:           "claude",
		ExpiresCommand: "cat '" + expiresFile + "'",
		Login:          "the Claude Code login of the sortie user (claude setup-token)",
	}}
	h.wm.config = cfg
	advance := healthClockAt(h, now)

	h.o.handleTick(ctx)

	faults := healthFaults(h)
	if len(faults) != 1 {
		t.Fatalf("health faults = %+v, want one", faults)
	}
	f := faults[0]
	if f.Class != healthClassCredentials || f.Action == nil {
		t.Fatalf("fault = %+v, want a credentials fault with a person action", f)
	}
	if want := "Renew the Claude Code login of the sortie user (claude setup-token) on taichi"; f.Action.Action != want {
		t.Errorf("person action = %q, want %q", f.Action.Action, want)
	}
	if !strings.Contains(f.Cause, `credential "claude" expires at`) || !strings.Contains(f.Cause, "within 30m0s") {
		t.Errorf("cause = %q, want it to say the credential expires within 30 minutes", f.Cause)
	}
	if got := heldStories(f); !slices.Equal(got, []string{"ONS-70"}) {
		t.Errorf("held stories = %v, want [ONS-70]: the question names the story it holds", got)
	}
	requireNotRunning(t, h, "issue-70")

	// The login is renewed: the next check reads a fresh expiry, the
	// fault resolves, and the story it held runs.
	writeHealthFile(t, expiresFile, now.Add(8*time.Hour).Format(time.RFC3339))
	advance(HealthCheckInterval)
	h.o.handleTick(ctx)

	if faults := healthFaults(h); len(faults) != 0 {
		t.Fatalf("health faults after the renewal = %+v, want none", faults)
	}
	if _, running := h.state.Running["issue-70"]; !running {
		t.Errorf("ONS-70 not running after the renewal; log: %s", h.logs.String())
	}
}

// A credential whose expiry is hours away, or that names no expiry,
// holds nothing; one that already expired, expires soon by its token's
// own exp claim, or whose variable is not set, is a fault that holds
// dispatch.
func TestCredentialFinding(t *testing.T) {
	now := time.Now().UTC()
	jwt := func(exp time.Time) string {
		payload := base64.RawURLEncoding.EncodeToString(fmt.Appendf(nil, `{"exp":%d}`, exp.Unix()))
		return "eyJhbGciOiJub25lIn0." + payload + ".sig"
	}
	t.Setenv("SORTIE_TEST_HEALTH_PLAIN", "sk-plain-token")
	t.Setenv("SORTIE_TEST_HEALTH_SOON", jwt(now.Add(20*time.Minute)))
	t.Setenv("SORTIE_TEST_HEALTH_LATER", jwt(now.Add(5*time.Hour)))
	at := func(d time.Duration) string { return "echo " + now.Add(d).Format(time.RFC3339) }
	cred := func(env, expires string) config.HealthCredential {
		return config.HealthCredential{Name: "claude", Env: env, ExpiresCommand: expires}
	}
	for _, tc := range []struct {
		name  string
		cred  config.HealthCredential
		cause string
	}{
		{"hours away", cred("", at(3*time.Hour)), ""},
		{"no known expiry", cred("SORTIE_TEST_HEALTH_PLAIN", ""), ""},
		{"token hours away", cred("SORTIE_TEST_HEALTH_LATER", ""), ""},
		{"expired", cred("", at(-time.Minute)), `credential "claude" expired at`},
		{"token expires soon", cred("SORTIE_TEST_HEALTH_SOON", ""), `credential "claude" expires at`},
		{"not set", cred("SORTIE_TEST_HEALTH_UNSET", at(3*time.Hour)), `credential "claude": the variable SORTIE_TEST_HEALTH_UNSET is not set`},
		{"unreadable expiry", cred("SORTIE_TEST_HEALTH_PLAIN", "echo soon"), `credential "claude": its source (expires_command) yielded no value`},
	} {
		f, failed := credentialFinding(context.Background(), tc.cred, "taichi", now)
		switch {
		case tc.cause == "" && failed:
			t.Errorf("%s: finding = %+v, want none", tc.name, f)
		case tc.cause != "" && (!failed || !strings.Contains(f.Cause, tc.cause) || !f.Holds):
			t.Errorf("%s: finding = %+v, %v, want a holding finding whose cause holds %q", tc.name, f, failed, tc.cause)
		}
	}
}

// fakeTokenInspector reports scopes for the token, and says a change to
// a workflow file needs the workflow scope.
type fakeTokenInspector struct {
	mu        sync.Mutex
	scopes    []string
	expiresAt time.Time
	err       error
}

func (f *fakeTokenInspector) InspectToken(context.Context) (domain.SCMTokenReport, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return domain.SCMTokenReport{Scopes: slices.Clone(f.scopes), ExpiresAt: f.expiresAt}, f.err
}

func (f *fakeTokenInspector) ChangeScopes(paths []string) []string {
	for _, p := range paths {
		if strings.HasPrefix(p, ".github/workflows/") {
			return []string{"workflow"}
		}
	}
	return nil
}

func (f *fakeTokenInspector) grant(scopes ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scopes = scopes
}

// The forge-permission check refuses to dispatch a story whose change
// needs a scope the token lacks: the story is held under a platform
// fault naming the scope before any session, while a story whose change
// needs nothing more runs. Once the token has the scope, the fault's
// check passes and releases the story.
func TestHealth_ForgeTokenWithoutTheScopeAChangeNeedsHoldsTheStory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	h := healthHarness(t, config.EnvironmentConfig{}, candidate("issue-71", "ONS-71"), candidate("issue-72", "ONS-72"))
	inspector := &fakeTokenInspector{scopes: []string{"repo", "read:org"}}
	h.o.tokenInspector = inspector
	h.o.scmProvider = "github"
	h.o.storyChanges = func(_ context.Context, _ config.ServiceConfig, issue domain.Issue) ([]string, error) {
		if issue.Identifier == "ONS-71" {
			return []string{".github/workflows/ci.yml", "Makefile"}, nil
		}
		return []string{"README.md"}, nil
	}

	h.o.handleTick(ctx)

	faults := healthFaults(h)
	if len(faults) != 1 {
		t.Fatalf("health faults = %+v, want one", faults)
	}
	f := faults[0]
	if f.Class != healthClassTokenScope || f.Action == nil || f.Action.Action != "Give taichi's GitHub token the `workflow` scope" {
		t.Fatalf("fault = %+v, want a token_scope fault asking for the workflow scope", f)
	}
	if !strings.Contains(f.Cause, "ONS-71") || !strings.Contains(f.Detail, ".github/workflows/ci.yml") || strings.Contains(f.Detail, "Makefile") {
		t.Errorf("cause = %q, detail = %q; want them to name the story and the workflow file only", f.Cause, f.Detail)
	}
	if got := heldStories(f); !slices.Equal(got, []string{"ONS-71"}) {
		t.Errorf("held stories = %v, want [ONS-71]", got)
	}
	if _, running := h.state.Running["issue-71"]; running {
		t.Error("ONS-71 is running, want it held before any session")
	}
	if _, running := h.state.Running["issue-72"]; !running {
		t.Errorf("ONS-72 not running; its change needs no more scope; log: %s", h.logs.String())
	}

	check := checkOf(h.state.PlatformFaults[f.ID])
	if err := h.o.runFaultCheck(ctx, check); err == nil {
		t.Fatal("fault check passed while the token still lacks the scope")
	}

	inspector.grant("repo", "workflow")
	if _, resolved := h.o.applyFaultCheckOnLoop(ctx, f.ID, h.o.runFaultCheck(ctx, check)); !resolved {
		t.Fatal("fault check did not pass once the token has the workflow scope")
	}
	if _, held := h.state.FaultHolds["issue-71"]; held {
		t.Error("ONS-71 still held after the check passed")
	}
}

// A token whose forge lists no scopes is not refused: Sortie cannot tell
// what it lacks, and the push tells.
func TestHealth_TokenWithoutListedScopesHoldsNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	h := healthHarness(t, config.EnvironmentConfig{}, candidate("issue-73", "ONS-73"))
	h.o.tokenInspector = &fakeTokenInspector{}
	h.o.storyChanges = func(context.Context, config.ServiceConfig, domain.Issue) ([]string, error) {
		return []string{".github/workflows/ci.yml"}, nil
	}

	h.o.handleTick(ctx)

	if faults := healthFaults(h); len(faults) != 0 {
		t.Fatalf("health faults = %+v, want none", faults)
	}
	if _, running := h.state.Running["issue-73"]; !running {
		t.Error("ONS-73 not running")
	}
}

// A forge token the forge refuses, or that expires within 30 minutes,
// holds every dispatch with the action that replaces it.
func TestHealth_ForgeTokenRefusedOrExpiringHoldsDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	h := healthHarness(t, config.EnvironmentConfig{}, candidate("issue-74", "ONS-74"))
	now := time.Now().UTC()
	advance := healthClockAt(h, now)
	inspector := &fakeTokenInspector{err: &domain.SCMError{Kind: domain.ErrSCMAuth, Message: "Bad credentials"}}
	h.o.tokenInspector = inspector
	h.o.scmProvider = "github"

	h.o.handleTick(ctx)
	faults := healthFaults(h)
	if len(faults) != 1 || faults[0].Class != healthClassCredentials || !strings.Contains(faults[0].Cause, "Bad credentials") ||
		faults[0].Action == nil || faults[0].Action.Action != "Replace taichi's GitHub token with a valid one" {
		t.Fatalf("health faults = %+v, want one credentials fault replacing the refused token", faults)
	}
	requireNotRunning(t, h, "issue-74")

	inspector.mu.Lock()
	inspector.err, inspector.scopes, inspector.expiresAt = nil, []string{"repo"}, now.Add(10*time.Minute)
	inspector.mu.Unlock()
	advance(HealthCheckInterval)
	h.o.handleTick(ctx)
	faults = healthFaults(h)
	if len(faults) != 1 || !strings.Contains(faults[0].Cause, "expires at") || faults[0].Action.Action != "Renew taichi's GitHub token, or replace it with one that expires later" {
		t.Fatalf("health faults = %+v, want the same fault saying the token expires", faults)
	}
	requireNotRunning(t, h, "issue-74")
}

// Under the deploy kit, a deploy that failed holds every dispatch; once
// the kit writes a healthy state the story runs. A deploy behind main
// within the grace holds nothing.
func TestHealth_FailedDeployHoldsDispatchUntilTheKitDeploys(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	now := time.Now().UTC()
	path := filepath.Join(t.TempDir(), "deploy.json")
	writeHealthFile(t, path, `{"status":"failed","commit":"aaa","main":"bbb","error":"make build: exit 2","updated_at":"`+now.Format(time.RFC3339)+`"}`)
	h := healthHarness(t, config.EnvironmentConfig{}, candidate("issue-75", "ONS-75"))
	cfg := h.wm.Config()
	cfg.Health.DeployState = path
	h.wm.config = cfg
	advance := healthClockAt(h, now)

	h.o.handleTick(ctx)
	faults := healthFaults(h)
	if len(faults) != 1 || faults[0].Class != healthClassDeploy || faults[0].Cause != "the last deploy failed: make build: exit 2" {
		t.Fatalf("health faults = %+v, want one deploy fault", faults)
	}
	requireNotRunning(t, h, "issue-75")

	writeHealthFile(t, path, `{"status":"succeeded","commit":"bbb","main":"ccc","behind_since":"`+now.Format(time.RFC3339)+`","updated_at":"`+now.Format(time.RFC3339)+`"}`)
	advance(HealthCheckInterval)
	h.o.handleTick(ctx)
	if faults := healthFaults(h); len(faults) != 0 {
		t.Fatalf("health faults after the deploy = %+v, want none", faults)
	}
	if _, running := h.state.Running["issue-75"]; !running {
		t.Error("ONS-75 not running after the deploy")
	}
}

// A worker that stays unreachable is a platform fault that holds no
// dispatch; a person marking it done dismisses it until the worker
// checks in again.
func TestHealth_WorkerThatStaysUnreachable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	h := remoteHoldHarness(t, 1)
	h.o.hostName = "taichi"
	h.o.remote.matcher.CheckIn("p1", p1Worker())
	advance := healthClockAt(h, time.Now())

	h.o.handleTick(ctx)
	if faults := healthFaults(h); len(faults) != 0 {
		t.Fatalf("health faults with p1 checked in = %+v, want none", faults)
	}

	advance(MachineDownAfter + time.Minute)
	h.o.handleTick(ctx)
	faults := healthFaults(h)
	if len(faults) != 1 || faults[0].Class != healthClassMachine || !strings.Contains(faults[0].Cause, `worker "p1" has not checked in since`) {
		t.Fatalf("health faults = %+v, want one machine fault for p1", faults)
	}
	if h.o.health.holding[faults[0].Signature] {
		t.Error("the machine fault holds dispatch, want it to hold none")
	}

	done := h.o.PlatformFaultDoneFunc()
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		h.o.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-stopped
	})
	result := done(ctx, faults[0].ID, "serghei")
	if result.Status != PlatformFaultDoneChecked || !result.Resolved {
		t.Fatalf("action done = %+v, want the fault resolved", result)
	}
	h.o.runOnLoop(ctx, func(loopCtx context.Context) {
		advance(MachineDownAfter + HealthCheckInterval + time.Minute)
		h.o.runDueHealthChecks(loopCtx, h.wm.Config())
	})
	var after []SnapshotPlatformFault
	h.o.runOnLoop(ctx, func(context.Context) { after = healthFaults(h) })
	if len(after) != 0 {
		t.Errorf("health faults after the dismissal = %+v, want none while p1 stays away", after)
	}
}

// A retry a failing health check holds is not dispatched: its retry
// entry is deleted, as the fault holds the issue; a reaction
// continuation is re-armed instead.
func TestHandleRetryTimer_HealthCheckHoldsTheRetry(t *testing.T) {
	for _, continuation := range []bool{false, true} {
		store := &mockRetryStore{}
		tracker := &mockRetryTracker{fetchedIssue: candidateIssue("ISS-77", "ONS-77", "To Do")}
		state := retryState(t, "ISS-77", "ONS-77", 1)
		if continuation {
			state.RetryAttempts["ISS-77"].ReactionKind = ReactionKindCI
		}
		params := defaultRetryParams(t, store, tracker)
		var asked []bool
		params.HealthHold = func(_ domain.Issue, hold bool) bool {
			asked = append(asked, hold)
			return true
		}
		dispatched := 0
		params.MakeWorkerFn = func(_, _, _, _, _ string, _ domain.AgentAdapter, _ AttemptSettings) WorkerFunc {
			dispatched++
			return func(context.Context, domain.Issue, *int) {}
		}

		HandleRetryTimer(state, "ISS-77", params)

		if dispatched != 0 || !slices.Equal(asked, []bool{!continuation}) {
			t.Fatalf("continuation %v: dispatches = %d, holds asked = %v; want none dispatched and hold %v", continuation, dispatched, asked, !continuation)
		}
		entry := state.RetryAttempts["ISS-77"]
		if continuation {
			if entry == nil {
				t.Fatal("continuation: retry entry dropped, want it re-armed")
			}
			if entry.TimerHandle != nil {
				entry.TimerHandle.Stop()
			}
			continue
		}
		if !slices.Equal(store.deletedIssueID, []string{"ISS-77"}) {
			t.Errorf("deleted retry entries = %v, want [ISS-77]", store.deletedIssueID)
		}
	}
}

// A credential the agent runtime refused at a session's start is a
// platform fault at once: every later dispatch of that agent kind is
// held before its session, the health run does not clear it, and it
// passes once a person signs the runtime in again and marks it done.
func TestHealth_RefusedAgentCredentialHoldsTheKindUntilSignedIn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	h := healthHarness(t, config.EnvironmentConfig{}, candidate("issue-79", "ONS-79"))
	advance := healthClockAt(h, time.Now())

	h.o.noteAgentCredentialRefused(ctx, WorkerResult{AgentAdapter: "kind-a", Identifier: "ONS-78",
		StartupError: &domain.AgentError{Kind: domain.ErrCredentialUnverified, Message: "401 invalid bearer token"}})
	h.o.noteAgentCredentialRefused(ctx, WorkerResult{AgentAdapter: "kind-a", Identifier: "ONS-77",
		StartupError: &domain.AgentError{Kind: domain.ErrTurnFailed, Message: "the turn failed"}})

	h.o.handleTick(ctx)
	faults := healthFaults(h)
	if len(faults) != 1 || faults[0].Class != healthClassCredentials || faults[0].Action == nil ||
		faults[0].Action.Action != "Sign the kind-a agent runtime in again on taichi, then mark this done" {
		t.Fatalf("health faults = %+v, want one credentials fault asking to sign kind-a in", faults)
	}
	if got := heldStories(faults[0]); !slices.Equal(got, []string{"ONS-79"}) {
		t.Errorf("held stories = %v, want [ONS-79]", got)
	}
	requireNotRunning(t, h, "issue-79")

	advance(HealthCheckInterval)
	h.o.handleTick(ctx)
	if len(healthFaults(h)) != 1 {
		t.Fatal("the health run cleared the refused credential, want it kept for a person")
	}
	id := faults[0].ID
	if err := h.o.runFaultCheck(ctx, checkOf(h.state.PlatformFaults[id])); err == nil {
		t.Fatal("fault check passed before a person marked it done")
	}
	h.state.PlatformFaults[id].ActionDoneAt = time.Now()
	if _, resolved := h.o.applyFaultCheckOnLoop(ctx, id, h.o.runFaultCheck(ctx, checkOf(h.state.PlatformFaults[id]))); !resolved {
		t.Fatal("fault check did not pass once marked done")
	}
	h.o.handleTick(ctx)
	if _, running := h.state.Running["issue-79"]; !running {
		t.Error("ONS-79 not running after the sign-in")
	}
}
