package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"os/exec"
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
	"github.com/sortie-ai/sortie/internal/worker"
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
}

func newRemoteWorkers(now func() time.Time) *remoteWorkers {
	leaser := worker.NewTTLLeaser(time.Duration(60000) * time.Millisecond)
	tokens := jobtoken.NewIssuer()
	plane := dataplane.New(tokens)
	return &remoteWorkers{leaser: leaser, tokens: tokens, plane: plane, handler: plane.Handler(),
		matcher: worker.NewMatcher(leaser, now, jobTokens{tokens})}
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
	}
}

// credentialPlaceholder is what the machine holds in place of the
// credential name; the egress proxy replaces it on the credential's
// hosts.
func credentialPlaceholder(name string) string { return "sortie-credential-" + name }

// remoteJob builds the matcher job of one dispatched attempt of issue.
// Its ID is the issue identifier and its attempt the run history's,
// counting from 1. Its run descriptor holds no credential: Sortie reads
// no credential value under the remote kind, and the machine gets each
// credential the workflow binds only as a placeholder.
func remoteJob(ctx context.Context, cfg config.ServiceConfig, issue domain.Issue, attempt *int, agentKind, defaultCommand string) worker.Job {
	rc := cfg.Environment.Remote
	n := normalizeAttempt(attempt) + 1
	caps := slices.Clone(rc.Requires.Capabilities)
	if rc.Backend != "" && !slices.Contains(caps, rc.Backend) {
		caps = append(caps, rc.Backend)
	}
	if caps == nil {
		caps = []string{}
	}
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
		Requires: worker.Requires{Capabilities: caps, CPU: rc.Requires.CPU, MemGiB: rc.Requires.MemGiB, DiskGiB: rc.Requires.DiskGiB},
		Flows:    maps.Clone(rc.Flows),
		Run: worker.Run{
			Backend:        rc.Backend,
			Image:          rc.Image,
			Command:        command,
			Args:           args,
			Env:            env,
			StallTimeoutMS: cfg.Agent.StallTimeoutMS,
			SeedPath:       worker.SeedPath(issue.Identifier, n),
			Credentials:    creds,
		},
	}
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

	ra := &remoteAttempt{
		workers:  rw,
		job:      remoteJob(ctx, cfg, issue, attempt, agentKind, p.DefaultCommand),
		issueID:  issue.ID,
		logger:   logger,
		cancel:   cancelRun,
		outcome:  make(chan struct{}),
		onBroken: p.OnStreamBroken,
		maxBundle: func() int64 {
			if r := cfg.Environment.Remote.Repository; r != nil && r.MaxBundleMiB > 0 {
				return int64(r.MaxBundleMiB) << 20
			}
			return 512 << 20
		}(),
	}
	ra.bridge, ra.bridgeErr = resolveToolServerBinary(deps.MCPServerBinary)

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
	if a.dir == "" {
		dir, err := os.MkdirTemp("", "sortie-remote-")
		if err != nil {
			return environment.PrepareResult{}, fmt.Errorf("create the remote attempt's directory: %w", err)
		}
		a.dir = dir
	}
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
	ended := a.workers.matcher.Submit(a.job)
	a.mu.Unlock()
	go a.watch(ended, dp)

	a.logger.Info("remote job queued", slog.String("job", a.job.ID), slog.Int("attempt", a.job.Attempt))
	err = dp.WaitStdio(ctx)
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
	a.mu.Lock()
	if a.finished {
		a.mu.Unlock()
		return
	}
	a.finished = true
	dp, submitted, dir := a.dp, a.submitted, a.dir
	a.mu.Unlock()
	if submitted {
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
// worker's reason, and counts usage reported after the last turn.
func (a *remoteAttempt) adjust(r WorkerResult) WorkerResult {
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

func (a *remoteUsageAdapter) RunTurn(ctx context.Context, session domain.Session, params domain.RunTurnParams) (domain.TurnResult, error) {
	res, err := a.AgentAdapter.RunTurn(ctx, session, params)
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

// Seed builds the job's workspace seed, served on the bundles flow once
// the job is queued.
func (r *remoteRepository) Seed(ctx context.Context, _ environment.Environment, ws environment.Workspace) error {
	a := r.attempt
	a.mu.Lock()
	dir := a.dir
	a.mu.Unlock()
	path := dir + string(os.PathSeparator) + "seed.bundle"
	_ = os.Remove(path) //nolint:errcheck // a seed of this attempt rebuilt
	seed, err := r.syncer.BuildSeed(ctx, ws.Key, path)
	if err != nil {
		return err
	}
	if err := workspacekit.RemoveSortieFile(ws.HostPath, pullRequestSuggestionFile); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("clear the previous pull request suggestion: %w", err)
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
	if res.PR != nil {
		if err := workspacekit.WriteSortieFile(ws.HostPath, pullRequestSuggestionFile, res.PR); err != nil {
			return none, fmt.Errorf("store the returned pull request suggestion: %w", err)
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
