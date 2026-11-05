package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/credsource"
	"github.com/sortie-ai/sortie/internal/deploystate"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/failcause"
	"github.com/sortie-ai/sortie/internal/gitsync"
	"github.com/sortie-ai/sortie/internal/logging"
	"github.com/sortie-ai/sortie/internal/redact"
	"github.com/sortie-ai/sortie/internal/worker"
	"github.com/sortie-ai/sortie/internal/workspace"
)

// HealthCheckInterval is how often Sortie runs its own health checks.
const HealthCheckInterval = 5 * time.Minute

// healthDispatchFreshness is the oldest health run a dispatch relies on;
// a dispatch after an older one runs the checks again first.
const healthDispatchFreshness = time.Minute

// CredentialExpiryWarning is how long before a credential expires its
// health check fails, so that a person renews it before a run hits it.
const CredentialExpiryWarning = 30 * time.Minute

// MachineDownAfter is how long a worker may go without checking in
// before its check fails.
const MachineDownAfter = 10 * time.Minute

// healthCheckTimeout bounds one run of the health checks.
const healthCheckTimeout = time.Minute

// storyChangesTimeout bounds the read of the paths a story's branch
// changes.
const storyChangesTimeout = 30 * time.Second

// healthStep is the step of every platform fault a health check records.
const healthStep = "health"

// Classes of the platform faults a health check records.
const (
	healthClassCredentials = "credentials"
	healthClassTokenScope  = "token_scope"
	healthClassDeploy      = "deploy"
	healthClassMachine     = "machine"
)

// Keys of the health checks, the cause part of each fault's signature.
const (
	healthKeyCredential = "credential " //nolint:gosec // G101: a check key, not a credential
	healthKeyForgeToken = "forge token"
	healthKeyDeploy     = "deploy"
	healthKeyWorker     = "worker "
)

// errHealthRun fails the check of a health fault that only the health
// run, on the event loop, can check again.
var errHealthRun = errors.New("the health check clears it once the condition passes")

// healthFinding is one failing health check.
type healthFinding struct {
	// Key names the check and what it checked, such as
	// `credential claude`; it is the cause part of the fault's signature.
	Key    string
	Class  string
	Cause  string
	Detail string
	Action *failcause.PersonAction

	// Holds reports that the failing check holds every dispatch.
	Holds bool
}

func (f healthFinding) signature() string {
	return healthSignature(f.Class, f.Key)
}

func healthSignature(class, key string) string {
	return failcause.Signature{Step: healthStep, Class: failcause.Class(class), Cause: key}.String()
}

// healthKeyOf returns the key part of a health fault's signature.
func healthKeyOf(signature string) string {
	parts := strings.SplitN(signature, " | ", 3)
	if len(parts) < 3 {
		return ""
	}
	return parts[2]
}

// healthResult is one run of the health checks.
type healthResult struct {
	findings []healthFinding

	// unknown lists the key prefixes of the checks that could not run;
	// their open faults stay as they are.
	unknown []string

	// token is what the forge said of its token; nil when it was not
	// asked or did not answer.
	token *domain.SCMTokenReport
}

func (r *healthResult) covers(key string) bool {
	return slices.ContainsFunc(r.unknown, func(prefix string) bool { return strings.HasPrefix(key, prefix) })
}

// healthState is the state of the health checks. Only the event loop
// touches it.
type healthState struct {
	lastRun time.Time
	nextRun time.Time

	// holding maps the signature of each failing check to whether it
	// holds every dispatch.
	holding map[string]bool

	// dismissed holds the signatures of the faults a person marked done
	// while their check still failed; it is dropped once the check passes.
	dismissed map[string]bool

	// token is the latest report of the forge token; nil before one.
	token *domain.SCMTokenReport
}

func newHealthState() healthState {
	return healthState{holding: map[string]bool{}, dismissed: map[string]bool{}}
}

// healthClock returns the time the health checks read.
func (o *Orchestrator) healthClock() time.Time {
	if o.healthNow != nil {
		return o.healthNow()
	}
	return time.Now()
}

// runDueHealthChecks runs the health checks when their scheduled run is
// due. Must run on the event loop.
func (o *Orchestrator) runDueHealthChecks(ctx context.Context, cfg config.ServiceConfig) {
	now := o.healthClock()
	if !o.health.lastRun.IsZero() && now.Before(o.health.nextRun) {
		return
	}
	o.runHealthChecks(ctx, cfg, now)
}

// runHealthChecks runs every health check and applies the result: a
// failing check is recorded as a platform fault, and the fault of a
// check that passes again is resolved. Must run on the event loop.
func (o *Orchestrator) runHealthChecks(ctx context.Context, cfg config.ServiceConfig, now time.Time) {
	checkCtx, cancel := context.WithTimeout(ctx, healthCheckTimeout)
	result := o.probeHost(checkCtx, cfg, now)
	o.probeMachines(cfg, now, &result)
	cancel()
	if ctx.Err() != nil {
		return
	}
	o.health.lastRun, o.health.nextRun = now, now.Add(HealthCheckInterval)
	o.applyHealth(ctx, result, now)
}

// probeHost runs the checks that read no runtime state: the credentials,
// the forge token and the deploy. Safe off the event loop.
func (o *Orchestrator) probeHost(ctx context.Context, cfg config.ServiceConfig, now time.Time) healthResult {
	var r healthResult
	o.probeCredentials(ctx, cfg, now, &r)
	o.probeForgeToken(ctx, cfg, now, &r)
	probeDeploy(cfg, o.hostName, now, &r)
	return r
}

// probeCredentials checks every credential health.credentials lists: its
// variable is set in Sortie's environment when it names one and, when
// its expiry is known, it does not expire within
// [CredentialExpiryWarning].
func (o *Orchestrator) probeCredentials(ctx context.Context, cfg config.ServiceConfig, now time.Time, r *healthResult) {
	for _, cred := range cfg.Health.Credentials {
		finding, failed := credentialFinding(ctx, cred, o.hostName, now)
		switch {
		case ctx.Err() != nil:
			r.unknown = append(r.unknown, healthKeyCredential)
			return
		case failed:
			r.findings = append(r.findings, finding)
		}
	}
}

// credentialFinding checks one credential. It reads the variable and the
// expiry anew, so a credential renewed at its source passes at once.
func credentialFinding(ctx context.Context, cred config.HealthCredential, host string, now time.Time) (healthFinding, bool) {
	finding := healthFinding{
		Key:    healthKeyCredential + cred.Name,
		Class:  healthClassCredentials,
		Action: credentialAction(cred, host),
		Holds:  true,
	}
	var value string
	if cred.Env != "" {
		if value = os.Getenv(cred.Env); value == "" {
			finding.Cause = fmt.Sprintf("credential %q: the variable %s is not set in Sortie's environment", cred.Name, cred.Env)
			return finding, true
		}
	}

	var expires time.Time
	known := false
	if cred.ExpiresCommand != "" {
		at, err := credsource.ReadExpiry(ctx, cred.Name, cred.ExpiresCommand)
		if err != nil {
			finding.Cause = err.Error()
			return finding, true
		}
		expires, known = at, true
	} else {
		expires, known = credsource.TokenExpiry(value)
	}
	if !known {
		return healthFinding{}, false
	}
	at := expires.UTC().Format(time.RFC3339)
	switch {
	case !now.Before(expires):
		finding.Cause = fmt.Sprintf("credential %q expired at %s", cred.Name, at)
	case expires.Sub(now) <= CredentialExpiryWarning:
		finding.Cause = fmt.Sprintf("credential %q expires at %s, within %s", cred.Name, at, CredentialExpiryWarning)
	default:
		return healthFinding{}, false
	}
	finding.Detail = fmt.Sprintf("Sortie read credential %q anew and found no fresher value: it expires at %s. A session started now could outlive it, so no story is dispatched until it is renewed.", cred.Name, at)
	return finding, true
}

// credentialAction is the person action that renews cred: it names the
// credential's login, or the credential when it names none.
func credentialAction(cred config.HealthCredential, host string) *failcause.PersonAction {
	login := cred.Login
	if login == "" {
		login = fmt.Sprintf("the login behind credential %q", cred.Name)
	}
	where := fmt.Sprintf("%s, where Sortie reads the expiry of credential %q with its expires_command", host, cred.Name)
	if cred.Env != "" {
		where = fmt.Sprintf("The variable %s in the environment of Sortie on %s", cred.Env, host)
	}
	return &failcause.PersonAction{Action: fmt.Sprintf("Renew %s on %s", login, host), Where: where}
}

// probeForgeToken asks the forge about the SCM adapter's token: the forge
// accepts it, and it does not expire within [CredentialExpiryWarning].
func (o *Orchestrator) probeForgeToken(ctx context.Context, cfg config.ServiceConfig, now time.Time, r *healthResult) {
	if o.tokenInspector == nil {
		return
	}
	forge := o.healthForge(cfg)
	finding := healthFinding{
		Key:   healthKeyForgeToken,
		Class: healthClassCredentials,
		Action: &failcause.PersonAction{
			Action: fmt.Sprintf("Replace %s's %s token with a valid one", o.hostName, forge),
			Where:  fmt.Sprintf("The credential Sortie on %s reads for %s", o.hostName, forge),
		},
		Holds: true,
	}
	report, err := o.tokenInspector.InspectToken(ctx)
	if err != nil {
		var scmErr *domain.SCMError
		if errors.As(err, &scmErr) && scmErr.Kind == domain.ErrSCMAuth {
			finding.Cause = fmt.Sprintf("the %s token was refused: %s", forge, scmErr.Message)
			r.findings = append(r.findings, finding)
			return
		}
		r.unknown = append(r.unknown, healthKeyForgeToken)
		return
	}
	r.token = &report
	if report.ExpiresAt.IsZero() {
		return
	}
	at := report.ExpiresAt.UTC().Format(time.RFC3339)
	switch {
	case !now.Before(report.ExpiresAt):
		finding.Cause = fmt.Sprintf("the %s token expired at %s", forge, at)
	case report.ExpiresAt.Sub(now) <= CredentialExpiryWarning:
		finding.Cause = fmt.Sprintf("the %s token expires at %s, within %s", forge, at, CredentialExpiryWarning)
	default:
		return
	}
	finding.Action = &failcause.PersonAction{
		Action: fmt.Sprintf("Renew %s's %s token, or replace it with one that expires later", o.hostName, forge),
		Where:  fmt.Sprintf("%s, the settings of the token Sortie on %s calls %s with", forge, o.hostName, forge),
	}
	r.findings = append(r.findings, finding)
}

// healthForge names the forge of the SCM adapter.
func (o *Orchestrator) healthForge(cfg config.ServiceConfig) string {
	if o.scmProvider != "" {
		return faultForge(o.scmProvider)
	}
	return faultForge(RepositoryProvider(cfg.Environment))
}

// probeDeploy reads the deploy state file the deploy kit writes, when
// the workflow names one.
func probeDeploy(cfg config.ServiceConfig, host string, now time.Time, r *healthResult) {
	path := cfg.Health.DeployState
	if path == "" {
		return
	}
	finding := healthFinding{
		Key:   healthKeyDeploy,
		Class: healthClassDeploy,
		Action: &failcause.PersonAction{
			Action: fmt.Sprintf("Fix the deploy of Sortie on %s, or mark this done to run on the deployed commit", host),
			Where:  fmt.Sprintf("The deploy kit on %s, which writes %s", host, path),
		},
		Holds: true,
	}
	state, err := deploystate.Read(path)
	if err != nil {
		finding.Cause = fmt.Sprintf("the deploy state file %s cannot be read: %v", path, err)
		r.findings = append(r.findings, finding)
		return
	}
	if problem := state.Problem(now); problem != "" {
		finding.Cause = problem
		finding.Detail = fmt.Sprintf("status %s, deployed commit %q, main %q, written at %s", state.Status, state.Commit, state.Main,
			state.UpdatedAt.UTC().Format(time.RFC3339))
		r.findings = append(r.findings, finding)
	}
}

// probeMachines checks the machines runs go to: under remote, each
// worker checked in within [MachineDownAfter]. Must run on the event
// loop.
func (o *Orchestrator) probeMachines(cfg config.ServiceConfig, now time.Time, r *healthResult) {
	if cfg.Environment.Kind != config.EnvironmentKindRemote || o.remote == nil || o.remote.matcher == nil {
		return
	}
	heartbeat, _ := o.remote.matcher.Timings()
	bound := max(MachineDownAfter, time.Duration(worker.GoneHeartbeats)*heartbeat)
	for _, w := range o.remote.matcher.Snapshot().Workers {
		if now.Sub(w.LastCheckIn) < bound {
			continue
		}
		r.findings = append(r.findings, healthFinding{
			Key:   healthKeyWorker + w.ID,
			Class: healthClassMachine,
			Cause: fmt.Sprintf("worker %q has not checked in since %s", w.ID, w.LastCheckIn.UTC().Format(time.RFC3339)),
			Action: &failcause.PersonAction{
				Action: fmt.Sprintf("Start worker %q again, or mark this done when it is out of service", w.ID),
				Where:  fmt.Sprintf("The host worker %q runs on", w.ID),
			},
		})
	}
}

// applyHealth records each failing check as an open platform fault, or
// updates the one it has, and resolves the fault of each check that
// passes again, which releases the stories it holds. Must run on the
// event loop.
func (o *Orchestrator) applyHealth(ctx context.Context, result healthResult, now time.Time) {
	if result.token != nil {
		o.health.token = result.token
	}
	failing := make(map[string]healthFinding, len(result.findings))
	for _, f := range result.findings {
		failing[f.signature()] = f
	}
	for sig := range o.health.dismissed {
		if _, still := failing[sig]; !still && !result.covers(healthKeyOf(sig)) {
			delete(o.health.dismissed, sig)
		}
	}

	holding := make(map[string]bool, len(failing))
	changed := false
	for sig, finding := range failing {
		if o.health.dismissed[sig] {
			continue
		}
		holding[sig] = finding.Holds
		if o.recordHealthFault(ctx, sig, finding, now) {
			changed = true
		}
	}

	var passed []string
	for id, f := range o.state.PlatformFaults {
		if f.Step != healthStep || f.Class == healthClassTokenScope || isAgentCredentialFault(f.Signature) {
			continue
		}
		if _, still := failing[f.Signature]; still && !o.health.dismissed[f.Signature] {
			continue
		}
		if result.covers(healthKeyOf(f.Signature)) {
			if h, ok := o.health.holding[f.Signature]; ok {
				holding[f.Signature] = h
			}
			continue
		}
		passed = append(passed, id)
	}
	o.health.holding = holding

	released := false
	for _, id := range passed {
		if f, resolved := applyFaultCheck(ctx, o.state, o.store, id, nil, now, o.logger); resolved {
			changed = true
			released = released || len(f.Holds) > 0
		}
	}
	if released {
		o.RefreshFunc()()
	}
	if changed {
		o.notifyObservers()
	}
}

// recordHealthFault records the finding under sig as an open platform
// fault, or brings the open one up to date. It reports whether it
// changed anything.
func (o *Orchestrator) recordHealthFault(ctx context.Context, sig string, finding healthFinding, now time.Time) bool {
	cause, detail := redact.Mask(finding.Cause), redact.Mask(finding.Detail)
	if f := platformFaultWithSignature(o.state, sig); f != nil {
		if f.Cause == cause && f.Detail == detail && actionEqual(f.Action, finding.Action) {
			return false
		}
		f.Cause, f.Detail, f.Action = cause, detail, finding.Action
		savePlatformFault(ctx, o.store, f, time.Time{}, o.logger)
		return true
	}
	f := &PlatformFault{
		ID:          platformFaultID(sig, now),
		Signature:   sig,
		Class:       finding.Class,
		Step:        healthStep,
		Cause:       cause,
		Detail:      detail,
		Action:      finding.Action,
		RecordedAt:  now.UTC(),
		NextCheckAt: now.Add(PlatformFaultCheckInterval),
	}
	o.state.PlatformFaults[f.ID] = f
	savePlatformFault(ctx, o.store, f, time.Time{}, o.logger)
	o.logger.Warn("health check failed, recorded a platform fault",
		slog.String("platform_fault_id", f.ID),
		slog.String("fault_class", f.Class),
		slog.String("cause", f.Cause),
		slog.Bool("holds_dispatch", finding.Holds),
	)
	return true
}

func actionEqual(a, b *failcause.PersonAction) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// platformFaultWithSignature returns the open fault recorded under
// signature, or nil.
func platformFaultWithSignature(state *State, signature string) *PlatformFault {
	for _, f := range state.PlatformFaults {
		if f.Signature == signature {
			return f
		}
	}
	return nil
}

// healthHold runs the health checks again when their last run is older
// than [healthDispatchFreshness], then holds the issue under the oldest
// open fault of a check that holds every dispatch, under the fault of a
// refused credential of the agent kind the issue would run on, or under
// the fault of a forge token that lacks a scope the issue's change needs. It returns
// the fault that holds the issue, or nil when the issue may dispatch;
// without hold the issue is not added to the fault's holds. Must run on
// the event loop, before the dispatch starts any session.
func (o *Orchestrator) healthHold(ctx context.Context, cfg config.ServiceConfig, issue domain.Issue, hold bool) *PlatformFault {
	now := o.healthClock()
	if o.health.lastRun.IsZero() || now.Sub(o.health.lastRun) >= healthDispatchFreshness {
		o.runHealthChecks(ctx, cfg, now)
	}
	var fault *PlatformFault
	for _, f := range o.state.PlatformFaults {
		if f.Step == healthStep && o.health.holding[f.Signature] && (fault == nil || f.RecordedAt.Before(fault.RecordedAt)) {
			fault = f
		}
	}
	if fault == nil {
		fault = o.agentCredentialFaultFor(cfg, issue)
	}
	if fault == nil {
		fault = o.changeScopeFault(ctx, cfg, issue, now)
	}
	if fault == nil {
		return nil
	}
	log := logging.WithIssue(o.logger, issue.ID, issue.Identifier)
	if !hold {
		log.Info("dispatch waits: a health check fails",
			slog.String("platform_fault_id", fault.ID),
			slog.String("fault_class", fault.Class),
		)
		return fault
	}
	holdUnderPlatformFault(ctx, o.state, o.store, fault, PlatformFaultHold{
		IssueID: issue.ID, Identifier: issue.Identifier, DisplayID: issue.DisplayID, HeldAt: now,
	}, log)
	log.Warn("dispatch held before any session: a health check fails",
		slog.String("platform_fault_id", fault.ID),
		slog.String("fault_class", fault.Class),
		slog.String("cause", fault.Cause),
	)
	o.notifyObservers()
	return fault
}

// scopeInCause reads the scope a token_scope health fault names.
var scopeInCause = regexp.MustCompile("`([^`]+)` scope")

// changeScopeFault returns the open fault, recorded now when there is
// none, of a forge token that lacks a scope a push of the issue's change
// needs, or nil when the token lacks none, the forge does not list its
// scopes, or the change cannot be read.
func (o *Orchestrator) changeScopeFault(ctx context.Context, cfg config.ServiceConfig, issue domain.Issue, now time.Time) *PlatformFault {
	if o.tokenInspector == nil || o.health.token == nil || o.health.token.Scopes == nil {
		return nil
	}
	read := o.storyChanges
	if read == nil {
		read = o.storyChangedPaths
	}
	readCtx, cancel := context.WithTimeout(ctx, storyChangesTimeout)
	paths, err := read(readCtx, cfg, issue)
	cancel()
	if err != nil {
		logging.WithIssue(o.logger, issue.ID, issue.Identifier).Warn("could not read the paths the issue's branch changes, dispatching without the scope check",
			slog.Any("error", err))
		return nil
	}
	var missing string
	for _, scope := range o.tokenInspector.ChangeScopes(paths) {
		if !slices.Contains(o.health.token.Scopes, scope) {
			missing = scope
			break
		}
	}
	if missing == "" {
		return nil
	}
	sig := healthSignature(healthClassTokenScope, fmt.Sprintf("the token is missing the `%s` scope", missing))
	if f := platformFaultWithSignature(o.state, sig); f != nil {
		return f
	}
	var needing []string
	for _, p := range paths {
		if slices.Contains(o.tokenInspector.ChangeScopes([]string{p}), missing) {
			needing = append(needing, p)
		}
	}
	forge := o.healthForge(cfg)
	f := &PlatformFault{
		ID:        platformFaultID(sig, now),
		Signature: sig,
		Class:     healthClassTokenScope,
		Step:      healthStep,
		Cause:     redact.Mask(fmt.Sprintf("the %s token lacks the `%s` scope a push of %s's change needs", forge, missing, issue.Identifier)),
		Detail:    redact.Mask(fmt.Sprintf("The change touches %s. The token's scopes are %s.", strings.Join(needing, ", "), strings.Join(o.health.token.Scopes, ", "))),
		Action: &failcause.PersonAction{
			Action: fmt.Sprintf("Give %s's %s token the `%s` scope", o.hostName, forge, missing),
			Where:  fmt.Sprintf("%s, the settings of the token Sortie on %s pushes and calls %s with", forge, o.hostName, forge),
		},
		RecordedAt:  now.UTC(),
		NextCheckAt: now.Add(PlatformFaultCheckInterval),
	}
	o.state.PlatformFaults[f.ID] = f
	savePlatformFault(ctx, o.store, f, time.Time{}, o.logger)
	o.logger.Warn("health check failed, recorded a platform fault",
		slog.String("platform_fault_id", f.ID),
		slog.String("fault_class", f.Class),
		slog.String("cause", f.Cause),
	)
	return f
}

// storyChangedPaths returns the paths the issue's branch changes against
// the base branch, as the host mirror of its repository holds them; none
// when the workflow configures no repository or the repository opens no
// pull request on a forge.
func (o *Orchestrator) storyChangedPaths(ctx context.Context, cfg config.ServiceConfig, issue domain.Issue) ([]string, error) {
	if !configuresRepository(cfg.Environment) || o.dbPath == "" {
		return nil, nil
	}
	path, err := workspace.ComputePath(cfg.Workspace.Root, issue.Identifier)
	if err != nil {
		return nil, err
	}
	envCfg, _, err := issueRepository(cfg.Environment, path.Path, issue.Labels)
	if err != nil {
		return nil, err
	}
	repo := envCfg.RepositorySet().Repository
	if repo == nil || repo.Provider == "" {
		return nil, nil
	}
	syncer, err := gitsync.New(gitsync.Params{Repository: *repo, StateDir: filepath.Dir(o.dbPath), Logger: o.logger})
	if err != nil {
		return nil, err
	}
	return syncer.ChangedPaths(ctx, path.Key)
}

// checkHealthFault checks a fault a health check recorded again, off the
// event loop. A token_scope fault passes once the forge lists the scope
// it names; a deploy or machine fault passes once a person marked it
// done, as a person may decide to run on as things are; any other fault
// passes once its check does.
func (o *Orchestrator) checkHealthFault(ctx context.Context, check PlatformFaultCheck) error {
	if isAgentCredentialFault(check.Signature) {
		if check.ActionDone {
			return nil
		}
		return errAwaitingPersonAction
	}
	switch check.Class {
	case healthClassTokenScope:
		if o.tokenInspector == nil {
			return nil
		}
		report, err := o.tokenInspector.InspectToken(ctx)
		if err != nil {
			return err
		}
		m := scopeInCause.FindStringSubmatch(check.Signature)
		switch {
		case m == nil || slices.Contains(report.Scopes, m[1]):
			return nil
		case report.Scopes == nil && check.ActionDone:
			return nil
		case report.Scopes == nil:
			return errors.New("the forge does not list the token's scopes")
		}
		return fmt.Errorf("the token still lacks the `%s` scope", m[1])
	case healthClassDeploy, healthClassMachine:
		if check.ActionDone {
			return nil
		}
		if check.Class == healthClassMachine {
			return errHealthRun
		}
	}
	if o.workflowManager == nil {
		return errHealthRun
	}
	result := o.probeHost(ctx, o.workflowManager.Config(), o.healthClock())
	for _, f := range result.findings {
		if f.signature() == check.Signature {
			return errors.New(f.Cause)
		}
	}
	if result.covers(healthKeyOf(check.Signature)) {
		return errHealthRun
	}
	return nil
}

// noteHealthFaultChecked records, on the event loop, what a check of a
// health fault means for the health run: a deploy or machine fault a
// person resolved while its check still fails is dismissed until the
// check passes.
func (o *Orchestrator) noteHealthFaultChecked(f *PlatformFault, resolved bool) {
	if f == nil || f.Step != healthStep || !resolved {
		return
	}
	delete(o.health.holding, f.Signature)
	if !f.ActionDoneAt.IsZero() && (f.Class == healthClassDeploy || f.Class == healthClassMachine) {
		o.health.dismissed[f.Signature] = true
	}
}

// healthKeyAgentCredential prefixes the key of the fault of an agent
// kind whose credential its runtime refused.
const healthKeyAgentCredential = "agent credential " //nolint:gosec // G101: a check key, not a credential

// isAgentCredentialFault reports whether signature is the fault of an
// agent kind whose credential its runtime refused. Only a person clears
// it, so no health run resolves it and it holds dispatch whatever the
// health run found.
func isAgentCredentialFault(signature string) bool {
	return strings.HasPrefix(healthKeyOf(signature), healthKeyAgentCredential)
}

// noteAgentCredentialRefused records, when the run of result ended
// because its agent runtime refused or could not confirm its credential
// at session start, a platform fault that holds every later dispatch of
// that agent kind before its session, so no other run hits the refused
// credential. Must run on the event loop.
func (o *Orchestrator) noteAgentCredentialRefused(ctx context.Context, result WorkerResult) {
	var agentErr *domain.AgentError
	if result.AgentAdapter == "" || !errors.As(result.StartupError, &agentErr) || agentErr.Kind != domain.ErrCredentialUnverified {
		return
	}
	o.recordAgentCredentialRefused(ctx, result.AgentAdapter, result.Identifier+"'s session", agentErr.Message)
}

// recordAgentCredentialRefused records that the runtime of the agent
// kind refused or could not confirm its credential in session, with the
// runtime's message: a platform fault that holds every later dispatch of
// that kind, and every supervisor session of it, until a person signs it
// in again and marks it done. Must run on the event loop.
func (o *Orchestrator) recordAgentCredentialRefused(ctx context.Context, kind, session, message string) {
	finding := healthFinding{
		Key:   healthKeyAgentCredential + kind,
		Class: healthClassCredentials,
		Cause: fmt.Sprintf("the %s agent runtime refused or could not confirm its credential: %s", kind, message),
		Detail: fmt.Sprintf("The credential verification of %s failed. No session of the %s agent kind is dispatched until a person signs it in again and marks this done.",
			session, kind),
		Action: &failcause.PersonAction{
			Action: fmt.Sprintf("Sign the %s agent runtime in again on %s, then mark this done", kind, o.hostName),
			Where:  fmt.Sprintf("%s, as the user Sortie runs as", o.hostName),
		},
		Holds: true,
	}
	if o.recordHealthFault(ctx, finding.signature(), finding, o.healthClock()) {
		o.notifyObservers()
	}
}

// agentCredentialFaultFor returns the oldest open fault of a refused
// credential of the agent kind the issue would dispatch on, or nil.
func (o *Orchestrator) agentCredentialFaultFor(cfg config.ServiceConfig, issue domain.Issue) *PlatformFault {
	var fault *PlatformFault
	kind := ""
	for _, f := range o.state.PlatformFaults {
		if f.Step != healthStep || !isAgentCredentialFault(f.Signature) {
			continue
		}
		if kind == "" {
			kind = ResolveRule(issue, cfg.Dispatch, hopRouteOf(o.state, issue.ID), cfg.Agent.Kind, "").AgentKind
		}
		if healthKeyOf(f.Signature) == healthKeyAgentCredential+kind && (fault == nil || f.RecordedAt.Before(fault.RecordedAt)) {
			fault = f
		}
	}
	return fault
}
