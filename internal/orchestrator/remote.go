package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/dataplane"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/environment"
	"github.com/sortie-ai/sortie/internal/gitsync"
	"github.com/sortie-ai/sortie/internal/jobtoken"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/worker"
	"github.com/sortie-ai/sortie/internal/workspace"
	"github.com/sortie-ai/sortie/internal/workspacekit"
)

// leaseSweepInterval is how often the orchestrator ends attempts whose
// worker lease expired.
const leaseSweepInterval = time.Second

// remoteWorkers holds the matcher the remote environment kind hands
// runs to, the lease rule it uses, the issuer of its job tokens, and the
// data plane that serves the flows Sortie answers itself.
type remoteWorkers struct {
	leaser  *worker.TTLLeaser
	matcher *worker.Matcher
	tokens  *jobtoken.Issuer
	plane   *dataplane.Plane
	handler http.Handler

	// runs records each running attempt whose job a worker took, so a
	// restart re-adopts it; nil records nothing.
	runs remoteRunStore

	mu sync.Mutex
	// process is the context of the process's event loop; once it ends,
	// every attempt a worker holds is left with it for the next start.
	process context.Context
	// attempts holds the running attempts by issue ID.
	attempts map[string]*remoteAttempt
	// kept holds the issues whose attempt was left with its worker;
	// their exits record nothing.
	kept map[string]struct{}
	// adoptions holds the runs restored at start, until adopted.
	adoptions []*remoteAdoption
}

func newRemoteWorkers(now func() time.Time) *remoteWorkers {
	leaser := worker.NewTTLLeaser(time.Duration(60000) * time.Millisecond)
	tokens := jobtoken.NewIssuer()
	plane := dataplane.New(tokens)
	matcher := worker.NewMatcher(leaser, now, jobTokens{tokens})
	// A job this process does not know may be one a previous process
	// left with its worker, until the runs restored at start are adopted.
	matcher.Recovering()
	return &remoteWorkers{leaser: leaser, tokens: tokens, plane: plane, handler: plane.Handler(), matcher: matcher}
}

// started records the context of the process's event loop.
func (r *remoteWorkers) started(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.process = ctx
}

// stopping reports whether the process is shutting down.
func (r *remoteWorkers) stopping() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.process != nil && r.process.Err() != nil
}

func (r *remoteWorkers) keep(issueID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.kept == nil {
		r.kept = map[string]struct{}{}
	}
	r.kept[issueID] = struct{}{}
}

// wasKept reports whether the attempt of issueID was left with its
// worker for the next start.
func (r *remoteWorkers) wasKept(issueID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.kept[issueID]
	return ok
}

func (r *remoteWorkers) track(a *remoteAttempt) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.attempts == nil {
		r.attempts = map[string]*remoteAttempt{}
	}
	r.attempts[a.issueID] = a
}

func (r *remoteWorkers) untrack(a *remoteAttempt) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.attempts[a.issueID] == a {
		delete(r.attempts, a.issueID)
	}
}

func (r *remoteWorkers) attempt(issueID string) *remoteAttempt {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.attempts[issueID]
}

// submitted reports whether the running attempt of issueID handed its
// job to the matcher. An attempt still preparing its workspace, or not
// yet started, has not: its job is charged ahead of a new dispatch.
func (r *remoteWorkers) submitted(issueID string) bool {
	a := r.attempt(issueID)
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.submitted
}

// leased reports whether the running attempt of issueID is recorded as
// held by a worker.
func (r *remoteWorkers) leased(issueID string) bool {
	a := r.attempt(issueID)
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.recorded && (!a.finished || a.detached)
}

// leaseCurrent reports whether a worker holds the job of the running
// attempt of issueID under a lease that has not expired at now: the
// worker's heartbeats renew it while the worker shows the job alive.
func (r *remoteWorkers) leaseCurrent(issueID string, now time.Time) bool {
	a := r.attempt(issueID)
	if a == nil {
		return false
	}
	for _, ls := range r.leaser.Leases() {
		if ls.JobID == a.job.ID {
			return now.Before(ls.Expires)
		}
	}
	return false
}

// workerLive returns the State.WorkerLive of cfg: under the remote kind,
// whether the run's job lease is current, and nil, every worker live,
// under any other kind, whose worker is this process.
func (o *Orchestrator) workerLive(cfg config.ServiceConfig) func(issueID string, now time.Time) bool {
	if o.remote == nil || !isRemoteKind(cfg.Environment) {
		return nil
	}
	return o.remote.leaseCurrent
}

// detach leaves the running attempt of issueID with its worker for the
// next start; see [remoteAttempt.detach].
func (r *remoteWorkers) detach(issueID string) bool {
	a := r.attempt(issueID)
	return a != nil && a.detach()
}

// jobTokens gives the matcher the issuer's tokens: one per flow of the
// job, bound to its attempt, minted at the lease grant and revoked when
// the attempt ends.
type jobTokens struct{ issuer *jobtoken.Issuer }

func (t jobTokens) Mint(j worker.Job) (map[string]string, error) {
	flows := make([]jobtoken.Flow, 0, len(j.Flows))
	for f := range j.Flows {
		flows = append(flows, jobtoken.Flow(f))
	}
	toks, err := t.issuer.Mint(j.ID, uint64(max(j.Attempt, 0)), flows)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(toks))
	for f, tok := range toks {
		out[string(f)] = tok.Reveal()
	}
	return out, nil
}

func (t jobTokens) Void(jobID string) { t.issuer.OnLeaseEnd(jobID) }

// applyConfig takes the lease duration of cfg's remote block.
func (r *remoteWorkers) applyConfig(cfg config.EnvironmentConfig) {
	if r != nil && isRemoteKind(cfg) && cfg.Remote.LeaseTTLMS > 0 {
		r.leaser.SetTTL(time.Duration(cfg.Remote.LeaseTTLMS) * time.Millisecond)
	}
}

func isRemoteKind(cfg config.EnvironmentConfig) bool {
	return cfg.Kind == config.EnvironmentKindRemote
}

// Workers returns the worker matcher while the current configuration
// selects the remote environment kind, and nil otherwise, so the worker
// API is served under the remote kind only.
func (o *Orchestrator) Workers() *worker.Matcher {
	if o.remote == nil || !isRemoteKind(o.workflowManager.Config().Environment) {
		return nil
	}
	return o.remote.matcher
}

// JobTokens returns the issuer of job tokens while the current
// configuration selects the remote environment kind, and nil otherwise.
func (o *Orchestrator) JobTokens() *jobtoken.Issuer {
	if o.Workers() == nil {
		return nil
	}
	return o.remote.tokens
}

// DataPlane returns the handler of the data-plane flows Sortie serves
// itself while the current configuration selects the remote environment
// kind, and nil otherwise.
func (o *Orchestrator) DataPlane() http.Handler {
	if o.Workers() == nil {
		return nil
	}
	return o.remote.handler
}

// WorkerIdentityHeader returns worker.identity_header of the current
// configuration.
func (o *Orchestrator) WorkerIdentityHeader() string {
	if h := o.workflowManager.Config().Environment.Remote.IdentityHeader; h != "" {
		return h
	}
	return config.DefaultWorkerIdentityHeader
}

// expireLeases ends every attempt whose lease expired. The attempt's
// worker goroutine receives the outcome and reports the exit, which
// takes the normal retry path.
func (o *Orchestrator) expireLeases() {
	if o.remote != nil {
		o.remote.matcher.ExpireLeases()
		o.refreshWorkerQueue(time.Now())
	}
}

// refreshWorkerQueue marks each running entry whose remote job waits in
// the matcher's queue, and stamps the moment a worker took one that
// waited. Runs on the event loop, with the lease sweep.
func (o *Orchestrator) refreshWorkerQueue(now time.Time) {
	for _, e := range o.state.Running {
		queued := o.remote.matcher.Queued(e.Identifier)
		if e.WorkerQueued && !queued {
			e.WorkerGrantedAt = now
		}
		e.WorkerQueued = queued
	}
}

// credentialPlaceholder is what the machine holds in place of the
// credential name; the egress proxy replaces it on the credential's
// hosts.
func credentialPlaceholder(name string) string { return "sortie-credential-" + name }

// remoteJob builds the matcher job of one dispatched attempt of issue.
// Its ID is the issue identifier and its attempt the run history's,
// counting from 1. Its image and requirements are those of the
// repository cfg's environment is set to, the issue's own. Its run
// descriptor holds no credential: Sortie reads no credential value
// under the remote kind, and the machine gets each credential the
// workflow binds only as a placeholder.
func remoteJob(ctx context.Context, cfg config.ServiceConfig, issue domain.Issue, attempt *int, agentKind, defaultCommand string) worker.Job {
	rc := cfg.Environment.Remote
	n := normalizeAttempt(attempt) + 1
	identity := sessionIdentityFrom(ctx)
	identity.Token = ""
	env := map[string]string{}
	for _, v := range buildSessionEnv(sessionEnvParams{
		Issue: issue, DispatchID: dispatchIDFromContext(ctx), Attempt: attempt, Identity: identity,
		WorkspacePath: remoteWorkspace, Purpose: sessionPurposeWork,
	}) {
		if v.Name != envSessionAPIURL && v.Name != envSessionToken {
			env[v.Name] = v.Value
		}
	}
	creds := make([]worker.RunCredential, 0, len(rc.Credentials))
	for _, c := range rc.Credentials {
		env[c.Env] = credentialPlaceholder(c.Name)
		creds = append(creds, worker.RunCredential{Name: c.Name, Env: c.Env, Placeholder: credentialPlaceholder(c.Name),
			Hosts: slices.Clone(c.Hosts), Header: c.Header})
	}
	command, args := remoteAgentCommand(cfg.AgentCommand(agentKind, false), defaultCommand)
	return worker.Job{
		ID:       issue.Identifier,
		Attempt:  n,
		Priority: issue.Priority,
		Issue:    worker.Issue{ID: issue.ID, Identifier: issue.Identifier, Title: issue.Title},
		Requires: remoteRequires(rc),
		Flows:    maps.Clone(rc.Flows),
		Run: worker.Run{
			Backend:        rc.Backend,
			Image:          rc.JobImage(rc.Repository),
			Command:        command,
			Args:           args,
			Env:            env,
			StallTimeoutMS: cfg.Agent.StallTimeoutMS,
			SeedPath:       worker.SeedPath(issue.Identifier, n),
			Credentials:    creds,
		},
	}
}

// remoteRequires returns what a worker must declare to take a job of
// rc's repository: its requirements, with the backend as a capability.
func remoteRequires(rc config.RemoteConfig) worker.Requires {
	requires := rc.JobRequires(rc.Repository)
	caps := slices.Clone(requires.Capabilities)
	if rc.Backend != "" && !slices.Contains(caps, rc.Backend) {
		caps = append(caps, rc.Backend)
	}
	if caps == nil {
		caps = []string{}
	}
	return worker.Requires{Capabilities: caps, CPU: requires.CPU, MemGiB: requires.MemGiB, DiskGiB: requires.DiskGiB}
}

// workerWait returns why no checked-in worker can take issue's remote
// job now, after the queued jobs, and then the running attempts that
// have not queued theirs yet, took what they fit; and nil when one can
// or cfg selects another kind. A job that waits so is not
// dispatched: it would hold an agent slot, and count as running, until
// a worker took it.
func (o *Orchestrator) workerWait(cfg config.ServiceConfig, issue domain.Issue) *WorkerHold {
	if o.remote == nil || !isRemoteKind(cfg.Environment) {
		return nil
	}
	requires := issueRequires(cfg, issue)
	var ahead []worker.Requires
	for id, e := range o.state.Running {
		// submitted first: an attempt marks itself submitted and queues
		// its job under one lock, so once it reads true the matcher holds
		// the job, or held it, and it is not counted twice.
		if !o.remote.submitted(id) && !o.remote.matcher.Holds(e.Identifier) {
			ahead = append(ahead, issueRequires(cfg, e.Issue))
		}
	}
	waiting, ok := o.remote.matcher.Placeable(requires, ahead...)
	if ok {
		return nil
	}
	return &WorkerHold{Capabilities: requires.Capabilities, Waiting: waiting}
}

// issueRequires returns what a worker must declare to take issue's
// remote job: the requirements of the issue's repository.
func issueRequires(cfg config.ServiceConfig, issue domain.Issue) worker.Requires {
	env := cfg.Environment
	if issueEnv, _, err := issueRepository(env, remoteWorkspacePath(cfg, issue), issue.Labels); err == nil {
		env = issueEnv
	}
	return remoteRequires(env.Remote)
}

// remoteWorkspace is the workspace path inside a remote job's machine.
const remoteWorkspace = "/workspace"

// remoteAgentCommand splits the agent command into the program and its
// arguments: a list as written, a string on whitespace, and the kind's
// default command when the workflow sets none.
func remoteAgentCommand(c domain.AgentCommand, defaultCommand string) (string, []string) {
	argv := slices.Clone(c.Argv)
	if len(argv) == 0 {
		line := c.Line
		if strings.TrimSpace(line) == "" {
			line = defaultCommand
		}
		argv = strings.Fields(line)
	}
	if len(argv) == 0 {
		return "", []string{}
	}
	return argv[0], append([]string{}, argv[1:]...)
}

// errRunsOnTheWorker refuses a command Sortie would run in a remote
// job's workspace: only the agent runs there, over the stdio flow.
var errRunsOnTheWorker = errors.New("under the remote environment kind only the agent runs in the job's workspace, over the stdio flow; hooks, verification commands and workspace git inspection do not run")

// remoteEnded is the cause a remote attempt's run is cancelled with when
// the worker ended it first: by a failure report or an expired lease.
type remoteEnded struct{ reason string }

func (e *remoteEnded) Error() string { return e.reason }

// RemoteAttemptParams binds one remote attempt to the orchestrator.
type RemoteAttemptParams struct {
	// DefaultCommand is the agent kind's own command, used when the
	// workflow sets none.
	DefaultCommand string

	// OnStreamBroken is called with the issue ID when the job's stdio
	// stream breaks before the agent's exit, so the orchestrator ends
	// the turn as it ends a stalled agent's. Nil ignores the break, and
	// stall detection ends the silent session in its own time.
	OnStreamBroken func(issueID string)

	// Adopt, when set, is the run a previous process left with the
	// worker that holds its job: the attempt is not queued again, and
	// its session resumes where the run was.
	Adopt *remoteAdoption

	// OnRecord is called with the issue ID when the run's record should
	// take the running entry: at the lease grant and after each turn.
	OnRecord func(issueID string)
}

// RunRemoteAttempt runs one attempt of issue under the remote kind. The
// attempt is a job in the matcher, and it is a normal worker run: Sortie
// renders the prompt, drives the agent session and its turns, and
// applies its timeouts and stall detection as for a local run, but the
// session's agent runs on the worker that takes the job, its standard
// streams carried by the stdio flow and its tool calls reaching the
// session's tool server on this host over the mcp flow. The workspace seed is served on the
// bundles flow, the returned issue branch comes back on it and is pushed
// with Sortie's own credentials, and usage reports on the usage flow are
// recorded as the run's own. deps.OnExit is called exactly once.
//
// A failure report or an expired lease ends the run as failed with the
// worker's reason, and the next attempt is a new job attempt with new
// tokens.
func RunRemoteAttempt(ctx context.Context, rw *remoteWorkers, issue domain.Issue, attempt *int, deps WorkerDeps, p RemoteAttemptParams) {
	cfg := deps.ConfigFunc()
	agentKind := deps.AgentKind
	if agentKind == "" {
		agentKind = cfg.Agent.Kind
	}
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	runCtx, cancelRun := context.WithCancelCause(ctx)
	defer cancelRun(nil)

	// The job runs in the image and needs the requirements of the
	// issue's repository. One that cannot be resolved fails the
	// attempt's preparation before the job is queued.
	if issueEnv, _, err := issueRepository(cfg.Environment, remoteWorkspacePath(cfg, issue), issue.Labels); err == nil {
		cfg.Environment = issueEnv
	}
	job := remoteJob(ctx, cfg, issue, attempt, agentKind, p.DefaultCommand)
	if p.Adopt != nil {
		job = p.Adopt.job()
		deps.Adopted = p.Adopt.adoptedRun()
	}
	stateDir := ""
	if deps.DBPath != "" {
		stateDir = filepath.Join(filepath.Dir(deps.DBPath), "remote")
	}
	ra := &remoteAttempt{
		workers:  rw,
		job:      job,
		issueID:  issue.ID,
		logger:   logger,
		cancel:   cancelRun,
		outcome:  make(chan struct{}),
		onBroken: p.OnStreamBroken,
		onRecord: p.OnRecord,
		adopt:    p.Adopt,
		stateDir: stateDir,
		maxBundle: func() int64 {
			if r := cfg.Environment.Remote.Repository; r != nil && r.MaxBundleMiB > 0 {
				return int64(r.MaxBundleMiB) << 20
			}
			return 512 << 20
		}(),
	}
	ra.bridge, ra.bridgeErr = resolveToolServerBinary(deps.MCPServerBinary)
	if a := p.Adopt; a != nil {
		ra.dir, ra.seed, ra.seedPath = a.record.Dir, a.record.Seed, a.record.SeedPath
		ra.recorded, ra.sessionID, ra.turns = true, a.run.SessionID, a.run.TurnsCompleted
	}
	rw.track(ra)
	defer rw.untrack(ra)

	deps.EnvironmentFunc = func(config.EnvironmentConfig) (environment.Environment, error) { return ra, nil }
	deps.RepositoryFunc = func(envCfg config.EnvironmentConfig, stateDir string, logger *slog.Logger) (Repository, error) {
		repo := envCfg.RepositorySet().Repository
		if repo == nil {
			return nil, nil
		}
		syncer, err := gitsync.New(gitsync.Params{Repository: *repo, StateDir: stateDir, Logger: logger})
		if err != nil {
			return nil, err
		}
		return &remoteRepository{syncer: syncer, attempt: ra}, nil
	}
	deps.AgentAdapter = &remoteUsageAdapter{AgentAdapter: deps.AgentAdapter, attempt: ra}
	onExit := deps.OnExit
	deps.OnExit = func(issueID string, result WorkerResult) {
		ra.finish()
		onExit(issueID, ra.adjust(result))
	}
	RunWorkerAttempt(runCtx, issue, attempt, deps)
}

// remoteWorkspacePath returns the host workspace path of issue under
// cfg, or "" when the identifier yields none.
func remoteWorkspacePath(cfg config.ServiceConfig, issue domain.Issue) string {
	path, err := workspace.ComputePath(cfg.Workspace.Root, issue.Identifier)
	if err != nil {
		return ""
	}
	return path.Path
}

// remoteAttempt is the execution environment of one remote attempt: its
// workspace is the job's machine on a worker. It implements
// [environment.Carrier], so the session launch is the stdio bridge, and
// [environment.ToolChanneler], so the session's tool server is served on
// the mcp flow.
type remoteAttempt struct {
	workers   *remoteWorkers
	job       worker.Job
	issueID   string
	logger    *slog.Logger
	cancel    context.CancelCauseFunc
	onBroken  func(issueID string)
	onRecord  func(issueID string)
	adopt     *remoteAdoption
	stateDir  string
	maxBundle int64
	bridge    string
	bridgeErr error

	mu        sync.Mutex
	dir       string
	seed      *gitsync.Seed
	seedPath  string
	dp        *dataplane.Attempt
	submitted bool
	ended     *worker.Outcome
	outcome   chan struct{}
	finished  bool
	// worker is the ID of the worker that took the job, once one did.
	worker string
	// recorded reports that the run's record holds the job a worker
	// took; detached that the attempt was left with it for the next
	// start.
	recorded  bool
	detached  bool
	sessionID string
	turns     int

	usageMu sync.Mutex
	pending []dataplane.Usage
}

var (
	_ environment.Environment         = (*remoteAttempt)(nil)
	_ environment.Carrier             = (*remoteAttempt)(nil)
	_ environment.AfterCreateFinisher = (*remoteAttempt)(nil)
	_ environment.ToolChanneler       = (*remoteAttempt)(nil)
)

func (*remoteAttempt) Kind() string { return config.EnvironmentKindRemote }

func (*remoteAttempt) GuestPath(string) string { return remoteWorkspace }

// Prepare readies the host side of the attempt. Every attempt is a new
// machine, so the workspace is seeded each time.
func (a *remoteAttempt) Prepare(context.Context, environment.Workspace) (environment.PrepareResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.dir != "" {
		// An adopted attempt keeps its directory, which may hold the seed
		// the worker has not fetched yet.
		if err := os.MkdirAll(a.dir, 0o700); err != nil {
			return environment.PrepareResult{}, fmt.Errorf("create the remote attempt's directory: %w", err)
		}
		return environment.PrepareResult{CreatedNow: true}, nil
	}
	if a.stateDir != "" {
		if err := os.MkdirAll(a.stateDir, 0o700); err != nil {
			return environment.PrepareResult{}, fmt.Errorf("create the remote attempts' directory: %w", err)
		}
	}
	dir, err := os.MkdirTemp(a.stateDir, "sortie-remote-")
	if err != nil {
		return environment.PrepareResult{}, fmt.Errorf("create the remote attempt's directory: %w", err)
	}
	a.dir = dir
	return environment.PrepareResult{CreatedNow: true}, nil
}

func (*remoteAttempt) Command(context.Context, environment.Workspace, environment.CommandSpec) (*exec.Cmd, error) {
	return nil, errRunsOnTheWorker
}

// AfterCreate hands the job to the matcher, once its seed is built, and
// waits until the worker that took it opened the stdio stream.
func (a *remoteAttempt) AfterCreate(ctx context.Context, _ environment.Workspace) error {
	a.mu.Lock()
	if a.submitted {
		a.mu.Unlock()
		return nil
	}
	dp, err := a.workers.plane.Open(a.job.ID, a.job.Attempt, dataplane.Options{
		Seed: a.seedPath, Dir: a.dir, MaxBundleBytes: a.maxBundle, Logger: a.logger,
		OnUsage: a.receiveUsage,
		Keep:    a.leaving,
		OnStdioBroken: func(error) {
			if a.onBroken != nil {
				a.onBroken(a.issueID)
			}
		},
	})
	if err != nil {
		a.mu.Unlock()
		return err
	}
	a.dp = dp
	a.submitted = true
	if a.adopt != nil {
		ended := a.adopt.ended
		a.mu.Unlock()
		go a.watch(ended, dp)
		a.logger.Info("remote job adopted", slog.String("job", a.job.ID), slog.Int("attempt", a.job.Attempt),
			slog.String("worker", a.adopt.run.WorkerID))
		if a.adopt.run.SessionEnded {
			return nil
		}
		return a.waitStdio(ctx, dp)
	}
	ended := a.workers.matcher.Submit(a.job)
	granted := a.workers.matcher.Granted(a.job.ID)
	a.mu.Unlock()
	go a.watch(ended, dp)
	go a.record(context.WithoutCancel(ctx), granted, dp)

	a.logger.Info("remote job queued", slog.String("job", a.job.ID), slog.Int("attempt", a.job.Attempt))
	return a.waitStdio(ctx, dp)
}

// waitStdio waits until the worker that holds the job opened the stdio
// stream.
func (a *remoteAttempt) waitStdio(ctx context.Context, dp *dataplane.Attempt) error {
	err := dp.WaitStdio(ctx)
	if err == nil {
		a.logger.Info("remote job stdio stream open", slog.String("job", a.job.ID), slog.Int("attempt", a.job.Attempt))
		return nil
	}
	if out, ok := a.endedOutcome(); ok {
		return fmt.Errorf("remote job %s attempt %d ended before its agent session started: %s", a.job.ID, a.job.Attempt, outcomeReason(out))
	}
	return err
}

// watch records how the matcher ended the job and stops serving its
// flows; a result already returned stays readable. A failure or an
// expired lease ends the run; after a success the run finishes what
// remains on the host.
func (a *remoteAttempt) watch(ended <-chan worker.Outcome, dp *dataplane.Attempt) {
	select {
	case out := <-ended:
		a.mu.Lock()
		a.ended = &out
		close(a.outcome)
		a.mu.Unlock()
		if !out.Succeeded && !out.Cancelled {
			a.cancel(&remoteEnded{reason: outcomeReason(out)})
		}
		dp.Close()
	case <-dp.Done():
	}
}

// record writes the run's record when a worker takes the job, so a
// restart from then on re-adopts the run.
func (a *remoteAttempt) record(ctx context.Context, granted <-chan string, dp *dataplane.Attempt) {
	var workerID string
	select {
	case workerID = <-granted:
	case <-a.outcome:
		return
	case <-dp.Done():
		return
	}
	a.mu.Lock()
	a.worker = workerID
	a.mu.Unlock()
	if a.workers.runs == nil {
		return
	}
	attempt, digests := a.workers.tokens.Digests(a.job.ID)
	a.mu.Lock()
	rec := remoteRunRecord{Job: a.job, Flows: a.job.Flows, Run: a.job.Run, Tokens: digests,
		Seed: a.seed, SeedPath: a.seedPath, Dir: a.dir}
	a.mu.Unlock()
	raw, err := json.Marshal(rec)
	if err == nil {
		err = a.workers.runs.SaveRemoteRun(ctx, persistence.RemoteRun{
			IssueID: a.issueID, JobID: a.job.ID, Attempt: int(attempt), WorkerID: workerID, Record: string(raw), //nolint:gosec // G115: an attempt counts from 1
		})
	}
	if err != nil {
		a.logger.Warn("remote run not recorded; a restart would not re-adopt it", slog.Any("error", err))
		return
	}
	a.mu.Lock()
	finished := a.finished
	a.recorded = !finished
	a.mu.Unlock()
	if finished {
		// The attempt ended while its record was written.
		_ = a.workers.runs.DeleteRemoteRun(ctx, a.issueID) //nolint:errcheck // a leftover row is dropped at the next start
		return
	}
	if a.onRecord != nil {
		a.onRecord(a.issueID)
	}
}

// recordSession stores the progress of the run's session.
func (a *remoteAttempt) recordSession(sessionID string, turnDone, ended bool) {
	a.mu.Lock()
	if sessionID != "" {
		a.sessionID = sessionID
	}
	if turnDone {
		a.turns++
	}
	recorded, id, turns := a.recorded, a.sessionID, a.turns
	a.mu.Unlock()
	if !recorded || a.leaving() || a.workers.runs == nil {
		return
	}
	if err := a.workers.runs.UpdateRemoteRunSession(context.Background(), a.issueID, id, turns, ended); err != nil {
		a.logger.Warn("remote run session not recorded", slog.Any("error", err))
	}
	if a.onRecord != nil {
		a.onRecord(a.issueID)
	}
}

// detach leaves the attempt with its worker for the next start: the data
// plane drops the worker's stream without telling it that the agent's
// input closed, the job stays leased, and the record stays. It reports
// false for an attempt no worker holds.
func (a *remoteAttempt) detach() bool {
	a.mu.Lock()
	if !a.recorded || (a.finished && !a.detached) {
		a.mu.Unlock()
		return false
	}
	a.detached = true
	dp := a.dp
	a.mu.Unlock()
	if dp != nil {
		dp.Detach()
	}
	return true
}

// leaving reports whether the attempt is left with its worker for the
// next start: detached, or recorded while the process stops. The process
// context ends before any worker sees its run end, so every step that
// would end the job, its record or its stream asks this first.
func (a *remoteAttempt) leaving() bool {
	a.mu.Lock()
	recorded, detached := a.recorded, a.detached
	a.mu.Unlock()
	return detached || (recorded && a.workers.stopping())
}

func (a *remoteAttempt) endedOutcome() (worker.Outcome, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ended == nil {
		return worker.Outcome{}, false
	}
	return *a.ended, true
}

func outcomeReason(out worker.Outcome) string {
	switch {
	case out.Reason != "":
		return out.Reason
	case out.Succeeded:
		return "the worker reported the attempt succeeded"
	}
	return "worker reported failure"
}

// LaunchArgv runs the session as the stdio bridge: Sortie's own binary,
// joined to the job's stdio stream. The worker starts the agent command
// of the run descriptor, so launch.Argv does not cross.
func (a *remoteAttempt) LaunchArgv(_ environment.Workspace, _ domain.CarrierLaunch) ([]string, error) {
	if a.bridgeErr != nil {
		return nil, fmt.Errorf("stdio bridge: %w", a.bridgeErr)
	}
	a.mu.Lock()
	dp := a.dp
	a.mu.Unlock()
	if dp == nil {
		return nil, errors.New("the remote job has no stdio stream")
	}
	return []string{a.bridge, dataplane.BridgeCommand, dp.Socket()}, nil
}

func (*remoteAttempt) ConnectionFailed(environment.Workspace, int, []string) bool { return false }

// remoteToolPort is the loopback port in a remote job's machine at
// which the agent reaches Sortie's tool server; the worker forwards each
// request there to the mcp flow.
const remoteToolPort = 47800

// ToolChannel names the attempt's tool channel: the session's tool
// server is reached at the data plane's tool socket of the attempt, and
// the mcp flow forwards the agent's requests to it with the bearer the
// channel hands it.
func (a *remoteAttempt) ToolChannel(environment.Workspace) (domain.ToolChannel, error) {
	a.mu.Lock()
	dp := a.dp
	a.mu.Unlock()
	if dp == nil {
		return domain.ToolChannel{}, errors.New("the remote job has no data plane attempt")
	}
	return domain.ToolChannel{HostSocket: dp.ToolSocket(), GuestPort: remoteToolPort, Authorize: dp.AuthorizeTools}, nil
}

// Release ends the attempt on the host: the data plane stops serving it,
// and a job the worker has not reported is withdrawn.
func (a *remoteAttempt) Release(context.Context, environment.Workspace) error {
	a.finish()
	return nil
}

func (*remoteAttempt) Remove(context.Context, environment.Workspace) error { return nil }

func (*remoteAttempt) Sweep(context.Context, environment.SweepParams) error { return nil }

func (a *remoteAttempt) finish() {
	leaving := a.leaving()
	a.mu.Lock()
	if a.finished {
		a.mu.Unlock()
		return
	}
	a.finished = true
	dp, submitted, dir, recorded := a.dp, a.submitted, a.dir, a.recorded
	if leaving {
		// The run goes on with its worker; the next start adopts it.
		a.detached = true
		a.mu.Unlock()
		if dp != nil {
			dp.Detach()
		}
		a.workers.keep(a.issueID)
		return
	}
	a.mu.Unlock()
	if recorded && a.workers.runs != nil {
		if err := a.workers.runs.DeleteRemoteRun(context.Background(), a.issueID); err != nil {
			a.logger.Warn("remote run record not deleted; the next start drops it", slog.Any("error", err))
		}
	}
	// An adopted job is the matcher's from the restore on, whether or
	// not the run got as far as handing it to the data plane.
	if submitted || a.adopt != nil {
		a.workers.matcher.Cancel(a.job.ID)
	}
	if dp != nil {
		dp.Close()
	}
	if dir != "" {
		_ = os.RemoveAll(dir) //nolint:errcheck // a leftover directory only costs disk
	}
}

// adjust reports a run the worker ended first as failed with the
// worker's reason, counts usage reported after the last turn, and names
// the worker that took the job.
func (a *remoteAttempt) adjust(r WorkerResult) WorkerResult {
	r.Worker = a.workerID()
	if out, ok := a.endedOutcome(); ok && !out.Succeeded && !out.Cancelled && r.ExitKind != WorkerExitNormal {
		r.ExitKind = WorkerExitError
		r.Error = errors.New(outcomeReason(out))
		r.CancelCause = nil
		r.StoppedByCeiling = false
	}
	a.usageMu.Lock()
	defer a.usageMu.Unlock()
	if len(a.pending) > 0 {
		for _, u := range a.pending {
			if !u.Measured {
				r.UnaccountedTurns++
				continue
			}
			r.Usage = maxUsage(r.Usage, toTokenUsage(u))
			r.UsageMeasured = true
		}
		a.pending = nil
	}
	return r
}

// workerID returns the ID of the worker that took the job: the one an
// adopted attempt was recorded with, else the one the matcher granted it
// to, or "" while none did.
func (a *remoteAttempt) workerID() string {
	if a.adopt != nil {
		return a.adopt.run.WorkerID
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.worker
}

func (a *remoteAttempt) receiveUsage(u dataplane.Usage) {
	a.usageMu.Lock()
	defer a.usageMu.Unlock()
	a.pending = append(a.pending, u)
}

// takeUsage returns the usage reports not yet delivered.
func (a *remoteAttempt) takeUsage() []dataplane.Usage {
	a.usageMu.Lock()
	defer a.usageMu.Unlock()
	out := a.pending
	a.pending = nil
	return out
}

func toTokenUsage(u dataplane.Usage) domain.TokenUsage {
	return domain.TokenUsage{
		InputTokens: u.InputTokens, OutputTokens: u.OutputTokens, TotalTokens: u.InputTokens + u.OutputTokens,
		CacheReadTokens: u.CacheReadTokens, CacheWriteTokens: u.CacheWriteTokens,
	}
}

func maxUsage(a, b domain.TokenUsage) domain.TokenUsage {
	return domain.TokenUsage{
		InputTokens: max(a.InputTokens, b.InputTokens), OutputTokens: max(a.OutputTokens, b.OutputTokens),
		TotalTokens:     max(a.TotalTokens, b.TotalTokens),
		CacheReadTokens: max(a.CacheReadTokens, b.CacheReadTokens), CacheWriteTokens: max(a.CacheWriteTokens, b.CacheWriteTokens),
	}
}

// remoteUsageAdapter delivers the usage the worker reported on the
// usage flow during a turn as that turn's own: each measured report as
// a token usage event, and a report of spend whose amount is unknown as
// the turn's unaccounted spend.
type remoteUsageAdapter struct {
	domain.AgentAdapter
	attempt *remoteAttempt
}

// endedSession is the session of an adopted run whose session had
// already ended: nothing is launched, and what is left of the run is the
// worker's returned result.
type endedSession struct{}

// StartSession starts the run's session and records it. An adopted run
// resumes the recorded session, through the deps' ResumeSessionID, on the
// agent the worker starts again on the job's machine.
func (a *remoteUsageAdapter) StartSession(ctx context.Context, params domain.StartSessionParams) (domain.Session, error) {
	if ad := a.attempt.adopt; ad != nil && ad.run.SessionEnded {
		return domain.Session{ID: ad.run.SessionID, Internal: endedSession{}}, nil
	}
	session, err := a.AgentAdapter.StartSession(ctx, params)
	if err == nil {
		a.attempt.recordSession(session.ID, false, false)
	}
	return session, err
}

// StopSession records that the session ended, unless the attempt was
// left for the next start, and stops it.
func (a *remoteUsageAdapter) StopSession(ctx context.Context, session domain.Session) error {
	if _, ok := session.Internal.(endedSession); ok {
		return nil
	}
	if a.attempt.leaving() {
		a.attempt.detach()
	} else {
		a.attempt.recordSession(session.ID, false, true)
	}
	return a.AgentAdapter.StopSession(ctx, session)
}

func (a *remoteUsageAdapter) RunTurn(ctx context.Context, session domain.Session, params domain.RunTurnParams) (domain.TurnResult, error) {
	res, err := a.AgentAdapter.RunTurn(ctx, session, params)
	if err == nil && !a.attempt.leaving() {
		a.attempt.recordSession(coalesce(res.SessionID, session.ID), true, false)
	}
	reports := a.attempt.takeUsage()
	measured, unknown := false, false
	for _, u := range reports {
		if !u.Measured {
			unknown = true
			continue
		}
		measured = true
		if params.OnEvent != nil {
			params.OnEvent(domain.AgentEvent{
				Type: domain.EventTokenUsage, Timestamp: time.Now().UTC(),
				Usage: toTokenUsage(u), Model: u.Model,
			})
		}
	}
	// The worker measures the attempt's model traffic, so its reports
	// decide whether the turn's spend is accounted for.
	switch {
	case unknown:
		res.SpendUnaccounted, res.SpendCoverable = true, false
	case measured:
		res.SpendUnaccounted, res.SpendCoverable = false, false
	}
	return res, err
}

// remoteRepository seeds a remote job's workspace through the bundles
// flow and takes its issue branch back from it.
type remoteRepository struct {
	syncer  *gitsync.Syncer
	attempt *remoteAttempt
}

var _ Repository = (*remoteRepository)(nil)

// statusFileName is the agent's status file in .sortie.
const statusFileName = "status"

// returnedSortieFiles are the files of the agent's .sortie directory a
// worker returns with the issue branch: the pull request suggestion, the
// status and a review's criteria verdict.
var returnedSortieFiles = []string{pullRequestSuggestionFile, statusFileName, CriteriaFile}

// Seed builds the job's workspace seed, served on the bundles flow once
// the job is queued.
func (r *remoteRepository) Seed(ctx context.Context, _ environment.Environment, ws environment.Workspace) error {
	a := r.attempt
	if a.adopt != nil {
		// The worker's machine was seeded by the run that left it; its
		// seed is the record's.
		return nil
	}
	a.mu.Lock()
	dir := a.dir
	a.mu.Unlock()
	path := dir + string(os.PathSeparator) + "seed.bundle"
	_ = os.Remove(path) //nolint:errcheck // a seed of this attempt rebuilt
	seed, err := r.syncer.BuildSeed(ctx, ws.Key, path)
	if err != nil {
		return err
	}
	for _, name := range returnedSortieFiles {
		if err := workspacekit.RemoveSortieFile(ws.HostPath, name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("clear the previous .sortie/%s: %w", name, err)
		}
	}
	a.mu.Lock()
	a.seed, a.seedPath = &seed, path
	a.mu.Unlock()
	return nil
}

// Refresh seeds again: every attempt runs on a new machine.
func (r *remoteRepository) Refresh(ctx context.Context, env environment.Environment, ws environment.Workspace) error {
	return r.Seed(ctx, env, ws)
}

// Export waits for the bundle the worker returns when the session ended,
// checks that it holds only the issue branch on top of the seed, and
// pushes it. The .sortie/pr.json the worker returned with it becomes the
// workspace's pull request suggestion.
func (r *remoteRepository) Export(ctx context.Context, _ environment.Environment, ws environment.Workspace) (gitsync.ExportResult, error) {
	a := r.attempt
	branch := r.syncer.IssueBranch(ws.Key)
	none := gitsync.ExportResult{Branch: branch}
	a.mu.Lock()
	dp, seed, outcome := a.dp, a.seed, a.outcome
	a.mu.Unlock()
	if dp == nil || seed == nil {
		return none, errors.New("the remote job was never seeded")
	}
	res, err := a.awaitResult(ctx, dp, outcome)
	if err != nil {
		return none, err
	}
	for name, data := range map[string][]byte{pullRequestSuggestionFile: res.PR, statusFileName: res.Status, CriteriaFile: res.Criteria} {
		if data == nil {
			continue
		}
		if err := workspacekit.WriteSortieFile(ws.HostPath, name, data); err != nil {
			return none, fmt.Errorf("store the returned .sortie/%s: %w", name, err)
		}
	}
	if res.Bundle == "" {
		none.NothingToExport = true
		return none, nil
	}
	return r.syncer.ImportReturned(ctx, ws.Key, res.Bundle, *seed)
}

// awaitResult waits for the worker's returned result. A worker that
// ended the attempt without returning one fails the export.
func (a *remoteAttempt) awaitResult(ctx context.Context, dp *dataplane.Attempt, outcome <-chan struct{}) (dataplane.Result, error) {
	waitCtx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		select {
		case <-outcome:
			stop()
		case <-waitCtx.Done():
		}
	}()
	res, err := dp.Result(waitCtx)
	if err == nil {
		return res, nil
	}
	if out, ok := a.endedOutcome(); ok {
		if res, ok := dp.Returned(); ok {
			return res, nil
		}
		return dataplane.Result{}, fmt.Errorf("the worker ended the attempt without returning the issue branch: %s", outcomeReason(out))
	}
	if ctxErr := context.Cause(ctx); ctxErr != nil {
		return dataplane.Result{}, ctxErr
	}
	return dataplane.Result{}, err
}

// reportStreamBroken passes the break of a remote run's stdio stream to
// the event loop. It never blocks the data plane: with the channel full,
// stall detection ends the silent session in its own time.
func (o *Orchestrator) reportStreamBroken(issueID string) {
	select {
	case o.streamBrokenCh <- issueID:
	default:
		o.logger.Warn("remote stdio break channel full; stall detection ends the session",
			slog.String("issue_id", issueID))
	}
}

// stallBrokenStream ends, on the event loop, the run whose remote stdio
// stream broke, as stall detection ends a stalled agent's.
func (o *Orchestrator) stallBrokenStream(ctx context.Context, issueID string) {
	cfg := o.workflowManager.Config()
	stallBrokenStream(o.state, issueID, ReconcileParams{
		MaxRetryBackoffMS: cfg.Agent.MaxRetryBackoffMS,
		Store:             o.store,
		OnRetryFire:       o.onRetryFire,
		Ctx:               ctx,
		Logger:            o.logger,
		Metrics:           o.metrics,
	})
}

// stallBrokenStream ends the run of issueID as stall detection ends a
// stalled agent's, because its remote stdio stream broke.
func stallBrokenStream(state *State, issueID string, params ReconcileParams) {
	entry, ok := state.Running[issueID]
	if !ok {
		return
	}
	log := params.Logger
	if log == nil {
		log = slog.Default()
	}
	ctx, metrics := params.Ctx, params.Metrics
	if ctx == nil {
		ctx = context.Background()
	}
	if metrics == nil {
		metrics = &domain.NoopMetrics{}
	}
	stallRunningEntry(ctx, state, issueID, entry, params, log, metrics, "remote stdio stream broke")
}
