package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/dataplane"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/egress"
	"github.com/sortie-ai/sortie/internal/jobtoken"
	"github.com/sortie-ai/sortie/internal/prompt"
	"github.com/sortie-ai/sortie/internal/waittest"
	"github.com/sortie-ai/sortie/internal/worker"
)

type remoteClock struct{ t time.Time }

func (c *remoteClock) now() time.Time { return c.t }

var dataPlaneFlows = []string{"stdio", "egress", "mcp", "bundles", "usage"}

func remoteFlowURLs() map[string]string {
	out := map[string]string{}
	for _, f := range dataPlaneFlows {
		out[f] = "https://edge.example/" + f
	}
	return out
}

func remoteCfg() config.ServiceConfig {
	cfg := config.ServiceConfig{Environment: config.EnvironmentConfig{
		Kind: config.EnvironmentKindRemote,
		Remote: config.RemoteConfig{
			IdentityHeader: "X-W",
			Requires:       config.RemoteRequirements{Capabilities: []string{"linux"}, CPU: 2, MemGiB: 4, DiskGiB: 10},
			Backend:        "smolvm",
			Image:          "ghcr.io/ortusnova/agent:1",
			Credentials:    []config.RemoteCredential{{Name: "anthropic", Env: "ANTHROPIC_API_KEY", Hosts: []string{"api.anthropic.com"}}},
			Flows:          remoteFlowURLs(),
			LeaseTTLMS:     60000,
		},
	}}
	cfg.Agent.Kind = "claude-code"
	cfg.Agent.CommandArgv = []string{"claude", "--model", "opus"}
	cfg.Agent.StallTimeoutMS = 300000
	return cfg
}

// worker is a declaration with room for every remote test job.
func remoteHost() worker.Declaration {
	return worker.Declaration{OS: "linux", Arch: "x86_64", KVM: true, Nested: true, Backends: []string{"smolvm"},
		RunSlots: 4, Free: worker.Free{CPU: 16, MemGiB: 52, DiskGiB: 210}}
}

// remoteWorkersOf binds m to a data plane of its own tokens, as
// newRemoteWorkers does for the orchestrator's matcher.
func remoteWorkersOf(m *worker.Matcher) *remoteWorkers {
	tokens := jobtoken.NewIssuer()
	plane := dataplane.New(tokens)
	return &remoteWorkers{matcher: m, tokens: tokens, plane: plane, handler: plane.Handler()}
}

// remoteDeps returns the worker dependencies of a remote attempt under
// cfg whose result goes to done.
func remoteDeps(t *testing.T, cfg config.ServiceConfig, agent domain.AgentAdapter, done chan<- WorkerResult) WorkerDeps {
	t.Helper()
	if cfg.Workspace.Root == "" {
		cfg.Workspace.Root = t.TempDir()
	}
	if agent == nil {
		agent = &mockAgentAdapter{}
	}
	return WorkerDeps{
		TrackerAdapter:         &mockTrackerAdapter{},
		AgentAdapter:           agent,
		AgentKind:              cfg.Agent.Kind,
		ConfigFunc:             func() config.ServiceConfig { return cfg },
		PromptTemplateByIDFunc: func(_ string) *prompt.Template { return mustParseTemplate(t, "{{ .issue.title }}") },
		OnEvent:                func(_ string, _ domain.AgentEvent) {},
		OnExit:                 func(_ string, r WorkerResult) { done <- r },
		Logger:                 discardLogger(),
	}
}

// runRemoteCfg starts RunRemoteAttempt with cfg and returns the channel
// its result arrives on, once the job is queued.
func runRemoteCfg(t *testing.T, ctx context.Context, rw *remoteWorkers, cfg config.ServiceConfig, issue domain.Issue, attempt *int) <-chan WorkerResult {
	t.Helper()
	done := make(chan WorkerResult, 1)
	go RunRemoteAttempt(withDispatchID(ctx, "D1"), rw, issue, attempt, remoteDeps(t, cfg, nil, done), RemoteAttemptParams{DefaultCommand: "claude"})
	waittest.Until(t, func() error {
		if len(rw.matcher.Snapshot().Queue) == 0 {
			return errors.New("job not queued")
		}
		return nil
	})
	return done
}

func runRemote(t *testing.T, ctx context.Context, m *worker.Matcher, issue domain.Issue, attempt *int) <-chan WorkerResult {
	t.Helper()
	return runRemoteCfg(t, ctx, remoteWorkersOf(m), remoteCfg(), issue, attempt)
}

// worker-dispatch: A run under the remote kind. The job carries the
// issue, its run attempt (the run history's, counting from 1), its
// requirements and its priority, and the run descriptor names what to
// start.
func TestARunUnderTheRemoteKind(t *testing.T) {
	c := &remoteClock{time.Unix(0, 0)}
	m := worker.NewMatcher(worker.NewTTLLeaser(time.Minute), c.now, nil)
	prio := 2
	issue := domain.Issue{ID: "1", Identifier: "ONS-7", Title: "t", Priority: &prio}
	done := runRemote(t, context.Background(), m, issue, new(3))
	q := m.Snapshot().Queue[0].Job
	want := worker.Job{ID: "ONS-7", Attempt: 4, Priority: &prio, Issue: worker.Issue{ID: "1", Identifier: "ONS-7", Title: "t"},
		Requires: worker.Requires{Capabilities: []string{"linux", "smolvm"}, CPU: 2, MemGiB: 4, DiskGiB: 10},
		Flows:    remoteFlowURLs()}
	q.QueuedAt, q.Run = time.Time{}, worker.Run{}
	if !reflect.DeepEqual(q, want) {
		t.Fatalf("job %+v\nwant %+v", q, want)
	}
	m.CheckIn("w", remoteHost())
	offer, ok, _ := m.Poll("w")
	wantRun := worker.Run{
		Backend: "smolvm", Image: "ghcr.io/ortusnova/agent:1", Command: "claude", Args: []string{"--model", "opus"},
		Env: map[string]string{
			"SORTIE_ISSUE_ID": "1", "SORTIE_ISSUE_IDENTIFIER": "ONS-7", "SORTIE_DISPATCH_ID": "D1", "SORTIE_ATTEMPT": "3",
			"SORTIE_RULE": "", "SORTIE_CHAIN_ID": "", "SORTIE_WORKSPACE": "/workspace", "SORTIE_SESSION_PURPOSE": "work",
			"ANTHROPIC_API_KEY": "sortie-credential-anthropic",
		},
		StallTimeoutMS: 300000,
		SeedPath:       "/jobs/ONS-7/attempts/4/seed",
		Credentials: []worker.RunCredential{{Name: "anthropic", Env: "ANTHROPIC_API_KEY",
			Placeholder: "sortie-credential-anthropic", Hosts: []string{"api.anthropic.com"}}},
		Egress: worker.Egress{Harness: egress.Empty(), Repository: egress.Empty(), CredentialHosts: []string{"api.anthropic.com"}},
	}
	if !ok || !reflect.DeepEqual(offer.Run, wantRun) {
		t.Fatalf("run %+v\nwant %+v", offer.Run, wantRun)
	}
	if err := m.Report("w", "ONS-7", 4, worker.Outcome{Succeeded: true, Machine: worker.MachineStopped}); err != nil {
		t.Fatal(err)
	}
	// Sortie drives the session, so a report that arrives before the
	// stdio stream opened ends an attempt that never ran.
	r := waittest.Receive(t, done, "remote attempt result")
	if r.ExitKind != WorkerExitError || r.Identifier != "ONS-7" || *r.Attempt != 3 || r.Error == nil ||
		!strings.Contains(r.Error.Error(), "before its agent session started") {
		t.Fatalf("%+v", r)
	}
}

// The agent command falls back to the kind's default command when the
// workflow sets none, and a string command is split into arguments.
func TestRemoteRunAgentCommand(t *testing.T) {
	for name, tc := range map[string]struct {
		line    string
		argv    []string
		command string
		args    []string
	}{
		"default": {command: "claude", args: []string{}},
		"line":    {line: "claude --verbose", command: "claude", args: []string{"--verbose"}},
		"argv":    {argv: []string{"/opt/my agent/bin", "-x"}, command: "/opt/my agent/bin", args: []string{"-x"}},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := remoteCfg()
			cfg.Agent.Command, cfg.Agent.CommandArgv = tc.line, tc.argv
			j := remoteJob(context.Background(), cfg, domain.Issue{ID: "1", Identifier: "ONS-7"}, nil, "claude-code", "claude")
			if j.Run.Command != tc.command || !reflect.DeepEqual(j.Run.Args, tc.args) || j.Attempt != 1 {
				t.Fatalf("%+v", j)
			}
		})
	}
}

// worker-api: No secret in the offer.
func TestNoSecretInTheOffer(t *testing.T) {
	const modelKey, forgeToken = "sk-ant-model-credential-7f3a", "ghp_forge_token_9b21"
	t.Setenv("ANTHROPIC_API_KEY", modelKey)
	t.Setenv("GITHUB_TOKEN", forgeToken)
	cfg := remoteCfg()
	cfg.Tracker.APIKey = forgeToken
	cfg.Environment.Remote.Credentials = append(cfg.Environment.Remote.Credentials,
		config.RemoteCredential{Name: "forge", Env: "GITHUB_TOKEN", Hosts: []string{"api.github.com"}})
	c := &remoteClock{time.Unix(0, 0)}
	r := newRemoteWorkers(c.now)
	ctx := withSessionIdentity(context.Background(), sessionIdentity{Token: "session-token-5e1d", RuleName: "build"})
	runRemoteCfg(t, ctx, r, cfg, domain.Issue{ID: "1", Identifier: "ONS-7"}, nil)
	r.matcher.CheckIn("w", remoteHost())
	offer, ok, err := r.matcher.Poll("w")
	if !ok || err != nil {
		t.Fatal(ok, err)
	}
	body, err := json.Marshal(offer)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{modelKey, forgeToken, "session-token-5e1d"} {
		if strings.Contains(string(body), secret) {
			t.Fatalf("offer holds %q: %s", secret, body)
		}
	}
	if offer.Run.Env["GITHUB_TOKEN"] != "sortie-credential-forge" || offer.Run.Env["SORTIE_RULE"] != "build" {
		t.Fatalf("env %v", offer.Run.Env)
	}
}

// worker-dispatch: A worker goes quiet. The expired lease ends attempt 1
// as failed, and the exit handler returns the issue to the queue as
// attempt 2 after the normal retry backoff.
func TestAWorkerGoesQuiet(t *testing.T) {
	c := &remoteClock{time.Unix(0, 0)}
	m := worker.NewMatcher(worker.NewTTLLeaser(time.Minute), c.now, nil)
	done := runRemote(t, context.Background(), m, domain.Issue{ID: "ISSUE-7", Identifier: "ONS-7"}, nil)
	m.CheckIn("w", remoteHost())
	if offer, ok, _ := m.Poll("w"); !ok || offer.Job.Attempt != 1 {
		t.Fatalf("offer %+v", offer.Job)
	}
	c.t = c.t.Add(2 * time.Minute)
	m.ExpireLeases()
	r := waittest.Receive(t, done, "remote attempt result")
	if r.ExitKind != WorkerExitError || r.Error == nil {
		t.Fatalf("%+v", r)
	}
	if _, err := m.Heartbeat("w", "ONS-7", 1); err == nil {
		t.Fatal("late heartbeat accepted")
	}

	store := &mockExitStore{}
	state := exitState(t, "ISSUE-7", nil)
	params := defaultExitParams(t, store)
	r.IssueID = "ISSUE-7"
	HandleWorkerExit(state, r, params)
	retry, ok := state.RetryAttempts["ISSUE-7"]
	if !ok || retry.Attempt != 1 || store.runHistories[0].Status != "failed" {
		t.Fatalf("retry %+v ok=%v history %+v", retry, ok, store.runHistories)
	}
	if now := params.NowFunc().UnixMilli(); retry.DueAtMS <= now {
		t.Fatalf("retry due %d, not after now %d", retry.DueAtMS, now)
	}
	next := remoteJob(context.Background(), remoteCfg(), domain.Issue{ID: "ISSUE-7", Identifier: "ONS-7"}, &retry.Attempt, "claude-code", "claude")
	if next.Attempt != 2 {
		t.Fatalf("next job attempt %d, want 2", next.Attempt)
	}
}

func TestRemoteAttemptCancelled(t *testing.T) {
	c := &remoteClock{time.Unix(0, 0)}
	m := worker.NewMatcher(worker.NewTTLLeaser(time.Minute), c.now, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := runRemote(t, ctx, m, domain.Issue{ID: "1", Identifier: "ONS-7"}, nil)
	cancel()
	if r := waittest.Receive(t, done, "remote attempt result"); r.ExitKind != WorkerExitCancelled {
		t.Fatalf("%+v", r)
	}
	if q := m.Snapshot().Queue; len(q) != 0 {
		t.Fatalf("withdrawn job still queued: %+v", q)
	}
}

func offerTokens(t *testing.T, offer worker.Offer) map[string]string {
	t.Helper()
	out := map[string]string{}
	for name, f := range offer.Flows {
		out[name] = f.Token
	}
	return out
}

// Taking a job mints one live token per flow, bound to the job, the
// attempt and the flow; the attempt's end voids them, whether by
// report, lease expiry or cancel, and a new attempt gets new ones.
func TestRemoteJobTokensLiveForTheAttempt(t *testing.T) {
	c := &remoteClock{time.Unix(0, 0)}
	r := newRemoteWorkers(c.now)
	m, issuer := r.matcher, r.tokens
	m.CheckIn("w", remoteHost())
	take := func(id string, attempt int) map[string]string {
		t.Helper()
		m.Submit(worker.Job{ID: id, Attempt: attempt, Flows: remoteFlowURLs()})
		offer, ok, err := m.Poll("w")
		if !ok || err != nil {
			t.Fatal(ok, err)
		}
		return offerTokens(t, offer)
	}
	live := func(toks map[string]string) bool {
		for _, tok := range toks {
			if _, err := issuer.Lookup(tok); err == nil {
				return true
			}
		}
		return false
	}

	first := take("ONS-7", 1)
	seen := map[string]bool{}
	for _, f := range dataPlaneFlows {
		tok := first[f]
		if tok == "" || seen[tok] {
			t.Fatalf("flow %s token empty or shared: %v", f, first)
		}
		seen[tok] = true
		g, err := issuer.Lookup(tok)
		if err != nil || g.JobID != "ONS-7" || g.Attempt != 1 || string(g.Flow) != f {
			t.Fatalf("flow %s grant %+v, %v", f, g, err)
		}
	}
	if err := m.Report("w", "ONS-7", 1, worker.Outcome{Succeeded: true, Machine: worker.MachineDeleted}); err != nil {
		t.Fatal(err)
	}
	if live(first) {
		t.Fatal("a token outlived the report")
	}

	second := take("ONS-7", 2)
	for f, tok := range second {
		if seen[tok] {
			t.Fatalf("attempt 2 reuses an attempt 1 token on %s", f)
		}
		if g, _ := issuer.Lookup(tok); g.Attempt != 2 {
			t.Fatalf("attempt 2 grant %+v", g)
		}
	}
	cancelled := take("ONS-8", 1)
	m.Cancel("ONS-8")
	if live(cancelled) {
		t.Fatal("a token outlived the cancel")
	}
	c.t = c.t.Add(2 * time.Minute)
	m.ExpireLeases()
	if live(second) {
		t.Fatal("a token outlived the lease")
	}
}

// Stopping the run, as shutdown does to every running attempt, voids
// its tokens.
func TestRemoteJobTokensVoidOnStop(t *testing.T) {
	c := &remoteClock{time.Unix(0, 0)}
	r := newRemoteWorkers(c.now)
	ctx, cancel := context.WithCancel(context.Background())
	done := runRemoteCfg(t, ctx, r, remoteCfg(), domain.Issue{ID: "1", Identifier: "ONS-7"}, nil)
	r.matcher.CheckIn("w", remoteHost())
	offer, ok, _ := r.matcher.Poll("w")
	if !ok || len(offer.Flows) != 5 || offer.Flows["stdio"].Token == "" {
		t.Fatalf("%+v", offer)
	}
	cancel()
	waittest.Receive(t, done, "remote attempt result")
	if _, err := r.tokens.Lookup(offer.Flows["stdio"].Token); err == nil {
		t.Fatal("a token outlived the stopped run")
	}
}
