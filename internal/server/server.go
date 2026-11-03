// Package server implements Sortie's embedded HTTP server for
// observability and operational control. Start with [New] to construct
// a server and [Server.ListenAndServe] to begin accepting connections.
package server

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sortie-ai/sortie/internal/dataplane"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/jobtoken"
	"github.com/sortie-ai/sortie/internal/orchestrator"
	"github.com/sortie-ai/sortie/internal/plan"
	"github.com/sortie-ai/sortie/internal/pricing"
	"github.com/sortie-ai/sortie/internal/server/ui"
	"github.com/sortie-ai/sortie/internal/worker"
)

// SnapshotFunc returns a point-in-time capture of the orchestrator's
// runtime state. The server calls this on each API request; it must
// be safe to call from any goroutine.
type SnapshotFunc func() (orchestrator.RuntimeSnapshotResult, error)

// RefreshFunc signals the orchestrator to perform an immediate
// poll+reconciliation cycle. Returns true if the signal was accepted,
// false if it was coalesced (channel already has a pending signal).
type RefreshFunc func() bool

// RetryNowFunc makes the scheduled retry of the issue with the given
// identifier due now; ctx is the request's context.
type RetryNowFunc func(ctx context.Context, identifier string) orchestrator.RetryNowResult

// AcknowledgeFunc records that a person has seen the attention item with the
// given ID; ctx is the request's context.
type AcknowledgeFunc func(ctx context.Context, itemID string) orchestrator.AcknowledgeResult

// CheckNowFunc makes the next check of an issue's pending pull request
// reaction of the given kind due now; ctx is the request's context.
type CheckNowFunc func(ctx context.Context, identifier, kind string) orchestrator.CheckNowResult

// RunEventsFunc returns a copy of a running run's retained events, oldest
// first, and false when no running run carries dispatchID.
type RunEventsFunc func(dispatchID string) ([]orchestrator.RunEvent, bool)

// RunEventsAfterFunc returns the events of a running run after cursor, and
// false when no running run carries dispatchID.
type RunEventsAfterFunc func(dispatchID string, cursor int64) (orchestrator.RunEventPage, bool)

// RunHistoryEntry is a server-layer view of a completed run attempt.
// Decoupled from persistence types to avoid leaking internal packages.
type RunHistoryEntry struct {
	// Identifier is the tracker-assigned issue identifier (e.g., "PROJ-123").
	Identifier string
	// DisplayID is the short display label that the JSON API reports.
	DisplayID string
	// Attempt is the one-based retry attempt number.
	Attempt int
	// Status is the terminal run outcome: "succeeded", "failed",
	// "cancelled", "ci_failed", "needs_person", "budget_stopped", or "usage_limited".
	Status string
	// WorkflowFile is the path to the workflow definition used.
	WorkflowFile string
	// StartedAt is the formatted start timestamp.
	StartedAt string
	// CompletedAt is the formatted completion timestamp.
	CompletedAt string
	// Error holds the error message when Status indicates failure.
	// Nil when no error occurred.
	Error *string
	// ErrorDetail is the full output of the step that failed the run, as
	// recorded; empty when none was recorded.
	ErrorDetail string
	// TurnsCompleted is the number of agent turns completed before exit.
	TurnsCompleted int
	// RuleName is the dispatch rule that routed the run; empty when none did.
	RuleName string
	// ChainID is the stage chain the run belongs to; empty for a run
	// recorded before chains existed.
	ChainID string
	// StagePrevious is the rule whose hop led to the run; empty when none
	// did.
	StagePrevious string
	// StageTarget is the rule the run's hop decision targeted; empty when
	// the run reached none.
	StageTarget string
	// StageResult is the result of the run's hop decision; empty when
	// StageTarget is.
	StageResult string
	// ID is the run's identifier in run history; it grows with every
	// recorded run and is the paging key.
	ID int64
	// IssueID is the tracker-assigned issue ID.
	IssueID string
	// IssueTitle is the title as recorded, unmasked.
	IssueTitle string
	// IssueURL is the tracker link as recorded, unmasked.
	IssueURL string
	// AgentKind is the agent adapter kind that ran the session.
	AgentKind string
	// ConfiguredModel is the model the dispatch rule configured.
	ConfiguredModel string
	// ConfiguredEffort is the reasoning effort the dispatch rule configured.
	ConfiguredEffort string
	// ReportedModel is the model the agent reported using.
	ReportedModel string
	// DispatchedAt is the dispatch timestamp as stored; empty when unrecorded.
	DispatchedAt string
	// Tokens holds the run's token figures; meaningful only when
	// TokensMeasured is true.
	Tokens domain.TokenUsage
	// TokensMeasured is false when the agent reported no usage.
	TokensMeasured bool
	// UnaccountedTurns counts turns whose usage the agent never reported.
	UnaccountedTurns int
	// EstimatedCostUSD and CostStatus are the cost recorded when the run
	// finished. CostStatus is empty when the run was recorded without one.
	EstimatedCostUSD *float64
	CostStatus       string
	// StagePreviousOutcome is the outcome of the run that led to this one.
	StagePreviousOutcome string
	// ReviewMetadata is the self-review JSON as recorded; nil when
	// self-review did not run.
	ReviewMetadata *string
	// Skills is the JSON array of the run's skill calls as recorded; nil when
	// the run recorded none.
	Skills *string
	// OriginRecorded is false for a run recorded before the origin fields
	// existed; Trigger, ReactionKind, SessionID, and SSHHost are then unknown.
	OriginRecorded bool
	// Trigger is what started the retry that dispatched the run; empty for
	// none, nil when the run's retry never stored it.
	Trigger *string
	// ReactionKind is the dispatching retry's reaction kind; empty for none.
	ReactionKind string
	// SessionID is the agent session the run used; empty when it started none.
	SessionID string
	// SSHHost is the SSH worker the run executed on; empty for a local run.
	SSHHost string
	// DispatchID keys the run's retained events: empty on the recorded CI
	// result, nil for a run recorded before the dispatch ID existed.
	DispatchID *string
	// DeclaredOutcome and AgentText are as stored, empty when none.
	// AgentText is masked when recorded and again by the server at serve time.
	DeclaredOutcome string
	AgentText       string
	// ModelUsage is the run's per-model spend ordered by model; empty when
	// the run recorded none.
	ModelUsage []RunModelUsage
}

// RunModelUsage is one model's part of a finished run's spend.
type RunModelUsage struct {
	// Model is the model name as recorded; empty when the agent reported none.
	Model string
	// Tokens.TotalTokens is InputTokens plus OutputTokens, as for a run.
	Tokens domain.TokenUsage
	// EstimatedCostUSD and CostStatus are the cost recorded for the part.
	// CostStatus is empty when the part was recorded without a price.
	EstimatedCostUSD *float64
	CostStatus       string
	// UnratedClasses names the token classes that had no rate; empty when
	// none did or when UnratedRecorded is false.
	UnratedClasses []string
	// UnratedRecorded is false for a part recorded without a price or priced
	// before the classes were recorded.
	UnratedRecorded bool
}

// RunHistoryQuery selects completed runs; a zero field matches every run.
type RunHistoryQuery struct {
	IssueID, Identifier, Status, StageTarget string
	// RuleName nil matches every run; a pointer to "" selects the runs that
	// record no rule name.
	RuleName *string
	// Since keeps runs completed at or after it.
	Since *time.Time
	// Until keeps runs completed before it.
	Until *time.Time
	// EndedSince keeps runs of issues whose outcome was observed at or after it.
	EndedSince *time.Time
	// EndedUntil keeps runs of issues whose outcome was observed before it.
	EndedUntil *time.Time
	// HasDispatchID keeps runs whose dispatch ID is a non-empty string.
	HasDispatchID bool
	// BeforeID keeps runs recorded before this run ID; 0 starts at the newest.
	BeforeID int64
	// Limit is the most runs returned.
	Limit int
}

// IssueGroupQuery selects the issues of run history by issue; a zero field
// matches every run.
type IssueGroupQuery struct {
	// IssueKey keeps the issues with a run whose identifier or display
	// identifier equals it.
	IssueKey, Status, StageTarget string
	// Search keeps the issues with a run whose identifier, display
	// identifier, or title contains it, ignoring ASCII case.
	Search string
	// RuleName nil matches every run; a pointer to "" selects the runs that
	// record no rule name.
	RuleName *string
	// BeforeID continues after the group whose newest matching run has this
	// ID; 0 starts at the newest group.
	BeforeID int64
	// Limit is the most groups returned.
	Limit int
}

// IssueGroupKey is one issue with a matching run and the ID of its newest
// matching run.
type IssueGroupKey struct {
	IssueID     string
	NewestRunID int64
}

// IssueOutcome is how an issue's latest work ended, as the orchestrator
// observed it. State is empty for a merged outcome; MergeReactionKind,
// PRNumber, and PRRepository are set only for a merged outcome.
type IssueOutcome struct {
	// Kind is "handed_off", "merged", or "terminal".
	Kind string
	// ObservedAt is when the outcome was observed, as stored.
	ObservedAt string
	// State is the tracker state a handoff wrote or a terminal read found,
	// unmasked.
	State string
	// MergeReactionKind is the reaction kind that observed the merge.
	MergeReactionKind string
	// PRNumber is the merged pull request number.
	PRNumber int
	// PRRepository is the merged pull request's owner/repo, unmasked.
	PRRepository string
}

// PassRef names one pass of one issue and when its first run was dispatched.
type PassRef struct {
	IssueID      string
	ChainID      string
	DispatchedAt time.Time // zero for an undated pass
}

// PassKey keys a pass in a PassActivities result.
type PassKey struct {
	IssueID string
	ChainID string
}

// KindActivity sums what one reaction kind recorded in one pass.
type KindActivity struct {
	Watched       bool
	Continuations int
	Acted         bool // an action row exists
	Escalated     bool
}

// PassActivity is what the reactions recorded in one pass. Recorded is false
// for a pass without a chain identifier or a dispatch time, or dispatched
// before reaction activity was first recorded; Kinds is then empty.
type PassActivity struct {
	Recorded bool
	Kinds    map[string]KindActivity // keyed by reaction kind
}

// IssueRunTotals is one issue's spend over all of its runs.
type IssueRunTotals struct {
	// Usage counts runs and prices spend per agent kind.
	Usage orchestrator.SnapshotIssueUsage
	// Tokens sums each token class over the measured runs.
	Tokens domain.TokenUsage
}

// RunHistoryReader reads persisted run history. Runs come newest first, and
// an implementation is safe for concurrent use.
type RunHistoryReader interface {
	QueryRuns(ctx context.Context, q RunHistoryQuery) ([]RunHistoryEntry, error)
	// QueryIssueGroups returns the issues whose runs match q, ordered by their
	// newest matching run, newest first.
	QueryIssueGroups(ctx context.Context, q IssueGroupQuery) ([]IssueGroupKey, error)
	IssueTotals(ctx context.Context, issueID string) (IssueRunTotals, error)
	// IssueOutcomes returns the outcome of each listed issue that has one,
	// keyed by issue ID.
	IssueOutcomes(ctx context.Context, issueIDs []string) (map[string]IssueOutcome, error)
	// PassActivities returns what the reactions recorded in each listed
	// pass, keyed by issue and chain. A listed pass without records maps to
	// a PassActivity with no Kinds.
	PassActivities(ctx context.Context, passes []PassRef) (map[PassKey]PassActivity, error)
	// RunEvents returns one run's stored events ascending by seq: all when
	// newest is 0, else the newest up to newest. It returns an empty non-nil
	// slice when the run has none, and an error for a row that does not
	// decode.
	RunEvents(ctx context.Context, dispatchID string, newest int) ([]orchestrator.RunEvent, error)
}

// DBPingFunc checks whether the SQLite database is accessible.
// Returns nil on success, an error on failure. Called on each
// GET /readyz request.
type DBPingFunc func(ctx context.Context) error

// Params holds construction-time dependencies for [New].
type Params struct {
	// SnapshotFn returns the current runtime state. Called on each
	// GET /api/v1/state and GET /api/v1/{identifier}.
	SnapshotFn SnapshotFunc

	// WorkflowShapeFn returns the effective workflow shape. Called on
	// each GET /api/v1/state; nil omits config.
	WorkflowShapeFn func() orchestrator.WorkflowShape

	// HealthFn returns the health of Sortie's own checks. Called on each
	// GET /api/v1/state; nil omits health.
	HealthFn func() orchestrator.SnapshotHealth

	// RefreshFn signals an immediate poll+reconciliation cycle.
	// Called on POST /api/v1/refresh.
	RefreshFn RefreshFunc

	// RetryNowFn makes an issue's scheduled retry due now. Called on
	// POST /api/v1/retries/{identifier}/run; nil answers 503.
	RetryNowFn RetryNowFunc

	// AcknowledgeFn records that a person has seen an attention item. Called
	// on POST /api/v1/attention/{id}/acknowledge; nil answers 503.
	AcknowledgeFn AcknowledgeFunc

	// CheckNowFn makes the next check of a pending pull request reaction due
	// now. Called on POST /api/v1/reactions/{identifier}/{kind}/check; nil
	// answers 503.
	CheckNowFn CheckNowFunc

	// Logger is the structured logger for request and error logging.
	Logger *slog.Logger

	// Addr is the TCP address to listen on (e.g. "127.0.0.1:8080").
	Addr string

	// Version is the build version string that the JSON API reports.
	Version string

	// StartedAt is the time the process started, used to compute
	// uptime in the JSON API.
	StartedAt time.Time

	// MetricsRegistry is an optional Prometheus registry for the
	// /metrics endpoint. When non-nil, New registers a handler at
	// GET /metrics that serves the Prometheus text exposition format.
	// Obtain via [PromMetrics.Registry]. When nil, /metrics is
	// not registered.
	MetricsRegistry *prometheus.Registry

	// DBPingFn checks database accessibility for /readyz.
	// When nil, the database check always reports "pass".
	DBPingFn DBPingFunc

	// PreflightFn returns the result of the most recent dispatch
	// preflight validation. Called on each GET /readyz request.
	// When nil, the preflight check always reports "pass".
	PreflightFn func() bool

	// WorkflowLoadedFn returns whether the workflow file has been
	// successfully loaded at least once. Called on each GET /readyz.
	// When nil, the workflow check always reports "pass".
	WorkflowLoadedFn func() bool

	// RunHistory reads persisted run history. When nil, GET /api/v1/runs
	// answers 503, GET /api/v1/{identifier} serves live detail with a null
	// history.
	RunHistory RunHistoryReader

	// RunEventsFn returns a running run's retained events. When nil, the
	// run-events route and recent_events serve running runs from the store
	// only, which holds none.
	RunEventsFn RunEventsFunc

	// RunEventsAfterFn reads running runs for the change stream. When nil,
	// the stream reads every run from the store only.
	RunEventsAfterFn RunEventsAfterFunc

	// Stats computes the run statistics report. Called on each
	// GET /api/v1/stats; nil answers 503.
	Stats StatsFunc

	// ModelUse lists the agent kind and model pairs with a recorded run.
	// Called on each GET /api/v1/prices; nil answers 503.
	ModelUse ModelUseFunc

	// ConfiguredModelsFn lists the agent kind and model pairs the workflow
	// configures. When nil, GET /api/v1/prices lists only models with a
	// recorded run.
	ConfiguredModelsFn func() []orchestrator.ConfiguredModel

	// RatesFn returns the rates the next recorded run gets. When nil,
	// GET /api/v1/prices serves every model unpriced and the fetched and
	// embedded tables off.
	RatesFn func() *pricing.Rates

	// PriceDownloads reports the state of the price table downloads. When
	// nil, GET /api/v1/prices reads an enabled download as not loaded.
	PriceDownloads *pricing.Downloader

	// WorkflowLoadFn returns when the latest workflow load ended and its
	// error, nil when it succeeded. When nil, GET /api/v1/prices reports no
	// load failure.
	WorkflowLoadFn func() (endedAt time.Time, err error)

	// MaskErrorFn masks an error text before GET /api/v1/prices serves it.
	// When nil, the route serves no error text.
	MaskErrorFn func(string) string

	// WorkersFn returns the worker matcher while the remote environment
	// kind is selected, and nil otherwise. The worker API answers 404
	// while it returns nil, and GET /api/v1/state lists workers and
	// leases while it does not. Nil serves no worker API.
	WorkersFn func() *worker.Matcher

	// WorkerIdentityHeaderFn returns worker.identity_header, the header
	// that names the worker of a worker API request.
	WorkerIdentityHeaderFn func() string

	// JobTokensFn returns the issuer of job tokens while the remote
	// environment kind is selected, and nil otherwise.
	// POST /api/v1/tokens/introspect answers 404 for every token while it
	// returns nil. Nil issues no job tokens.
	JobTokensFn func() *jobtoken.Issuer

	// DataPlaneFn returns the handler of the data-plane flows Sortie
	// serves itself, under /api/v1/flows/, while the remote environment
	// kind is selected, and nil otherwise. Its routes answer 404 while it
	// returns nil. Nil serves no data plane.
	DataPlaneFn func() http.Handler

	// ControlToken is the bearer token every state-changing endpoint
	// requires. When empty, those endpoints accept a request that
	// presents no token; startup permits that only on a loopback bind.
	ControlToken string

	// SessionTokenFn resolves a session token to the issue its running
	// session serves. When nil, no session token is accepted.
	SessionTokenFn SessionTokenFunc

	// SupervisorToken is the bearer token of the supervisor, which only
	// the supervisor action routes accept; every other control route
	// answers it 403. Empty accepts no supervisor token.
	SupervisorToken string

	// SupervisorSessionCap bounds the sessions granted to one issue in
	// total that a supervisor grant may reach; 0 refuses every
	// supervisor grant.
	SupervisorSessionCap int

	// Supervisor carries out the supervisor action routes. When nil,
	// those routes answer 503.
	Supervisor SupervisorActions

	// SupervisorLog stores each request the supervisor token makes, and
	// GET /api/v1/state serves the newest. When nil, nothing is stored.
	SupervisorLog SupervisorLog

	// HoldFn and ReleaseFn hold and release a running issue. Called on
	// POST /api/v1/{identifier}/hold and /release. When nil, those
	// routes answer 503.
	HoldFn    HoldFunc
	ReleaseFn ReleaseFunc

	// EscalateFn escalates a running issue to people. Called on
	// POST /api/v1/{identifier}/escalate. When nil, that route answers
	// 503.
	EscalateFn EscalateFunc

	// AnswerFn posts a person's answer on a parked issue and releases
	// the park. Called on POST /api/v1/{identifier}/answer. When nil,
	// that route answers 503.
	AnswerFn AnswerFunc

	// DispatchPauseFn pauses and resumes dispatch. Called on POST
	// /api/v1/dispatch/pause and /resume. When nil, those routes answer
	// 503.
	DispatchPauseFn DispatchPauseFunc

	// SessionGrantFn grants an issue more sessions. Called on POST
	// /api/v1/{identifier}/sessions. When nil, that route answers 503.
	SessionGrantFn SessionGrantFunc

	// HostFn returns the host Sortie runs its agents on. Called on GET
	// /api/v1/state, which omits host while it is nil or returns nil.
	HostFn HostFunc

	// PullBackFn and PlanCancelFn pull back and cancel a plan story by
	// plan key. Called on POST /api/v1/plan/pull-back and
	// /api/v1/plan/cancel. When nil, those routes answer 503.
	PullBackFn   PlanControlFunc
	PlanCancelFn PlanControlFunc

	// BrokerReportFn and BrokerReadFn keep and read the address of a
	// running issue's broker. Called on POST and GET
	// /api/v1/{identifier}/broker. When nil, that route answers 503.
	BrokerReportFn BrokerReportFunc
	BrokerReadFn   BrokerReadFunc

	// PlanProjects serves POST /api/v1/plan/sync, GET /api/v1/plan/graph,
	// GET /api/v1/plan/capabilities, and GET and PUT
	// /api/v1/plan/projects/{key}, for the configured project and, when
	// the tracker supports it, other projects of its workspace. When nil,
	// those routes answer 503.
	PlanProjects *plan.Projects

	// PlanStatesFn returns the workflow's states; plan control is on when
	// it reports a release state. PlanSatisfaction reads what satisfies
	// each plan story. With either nil, plan graph nodes carry no plan
	// control fields.
	PlanStatesFn     func() domain.TrackerStates
	PlanSatisfaction orchestrator.PlanSatisfactionLoader

	// PlanRunsElsewhereFn returns the report of which blockers another
	// Sortie runs, which the plan graph judges by their state; see
	// [orchestrator.PlanJudge.RunsElsewhere]. Nil treats every blocker as
	// this Sortie's own.
	PlanRunsElsewhereFn func() func(domain.BlockerRef) bool

	// TerminalStatesFn returns the workflow's current terminal states,
	// which mark plan graph nodes terminal. When nil, no node is.
	TerminalStatesFn func() []string
}

// Server is the embedded HTTP server for the JSON API, the dashboard
// application's assets, and metrics endpoints. Construct via [New]. Safe for concurrent use
// after construction.
type Server struct {
	httpServer      *http.Server
	mux             *http.ServeMux
	logger          *slog.Logger
	snapshotFn      SnapshotFunc
	refreshFn       RefreshFunc
	retryNowFn      RetryNowFunc
	acknowledgeFn   AcknowledgeFunc
	checkNowFn      CheckNowFunc
	workflowShapeFn func() orchestrator.WorkflowShape
	healthFn        func() orchestrator.SnapshotHealth
	version         string
	startedAt       time.Time

	drainingFlag     atomic.Bool
	dbPingFn         DBPingFunc
	preflightFn      func() bool
	workflowLoadedFn func() bool
	runHistory       RunHistoryReader
	runEventsFn      RunEventsFunc
	runEventsAfterFn RunEventsAfterFunc
	stats            StatsFunc

	modelUse           ModelUseFunc
	configuredModelsFn func() []orchestrator.ConfiguredModel
	ratesFn            func() *pricing.Rates
	priceDownloads     *pricing.Downloader
	workflowLoadFn     func() (time.Time, error)
	maskErrorFn        func(string) string
	workersFn          func() *worker.Matcher
	workerHeaderFn     func() string
	jobTokensFn        func() *jobtoken.Issuer
	dataPlaneFn        func() http.Handler

	changes *changeFeed
	timing  streamTiming

	controlToken     string
	sessionTokenFn   SessionTokenFunc
	holdFn           HoldFunc
	releaseFn        ReleaseFunc
	escalateFn       EscalateFunc
	answerFn         AnswerFunc
	dispatchPauseFn  DispatchPauseFunc
	sessionGrantFn   SessionGrantFunc
	hostFn           HostFunc
	workerView       workerViewState
	pullBackFn       PlanControlFunc
	brokerReportFn   BrokerReportFunc
	planStatesFn     func() domain.TrackerStates
	planSatisfaction orchestrator.PlanSatisfactionLoader
	planElsewhereFn  func() func(domain.BlockerRef) bool
	brokerReadFn     BrokerReadFunc
	planCancelFn     PlanControlFunc
	planProjects     *plan.Projects
	terminalStatesFn func() []string

	supervisorToken      string
	supervisorSessionCap int
	supervisor           SupervisorActions
	supervisorLog        SupervisorLog
}

// Compile-time assertion: Server satisfies orchestrator.Observer.
var _ orchestrator.Observer = (*Server)(nil)

// New creates a [Server] with all API routes registered on an internal
// [http.ServeMux]. The server refuses a state-changing request that a page on
// another origin sent. Does not start listening; call
// [Server.ListenAndServe].
func New(params Params) *Server {
	if params.SnapshotFn == nil {
		panic("server.New: SnapshotFn must not be nil")
	}
	if params.RefreshFn == nil {
		panic("server.New: RefreshFn must not be nil")
	}

	logger := params.Logger
	if logger == nil {
		logger = slog.Default()
	}

	mux := http.NewServeMux()

	s := &Server{
		mux:              mux,
		logger:           logger,
		snapshotFn:       params.SnapshotFn,
		refreshFn:        params.RefreshFn,
		retryNowFn:       params.RetryNowFn,
		acknowledgeFn:    params.AcknowledgeFn,
		checkNowFn:       params.CheckNowFn,
		workflowShapeFn:  params.WorkflowShapeFn,
		healthFn:         params.HealthFn,
		version:          params.Version,
		startedAt:        params.StartedAt,
		dbPingFn:         params.DBPingFn,
		preflightFn:      params.PreflightFn,
		workflowLoadedFn: params.WorkflowLoadedFn,
		controlToken:     params.ControlToken,
		sessionTokenFn:   params.SessionTokenFn,
		holdFn:           params.HoldFn,
		pullBackFn:       params.PullBackFn,
		brokerReportFn:   params.BrokerReportFn,
		planStatesFn:     params.PlanStatesFn,
		planSatisfaction: params.PlanSatisfaction,
		planElsewhereFn:  params.PlanRunsElsewhereFn,
		brokerReadFn:     params.BrokerReadFn,
		planCancelFn:     params.PlanCancelFn,
		releaseFn:        params.ReleaseFn,
		escalateFn:       params.EscalateFn,
		answerFn:         params.AnswerFn,
		dispatchPauseFn:  params.DispatchPauseFn,
		sessionGrantFn:   params.SessionGrantFn,
		hostFn:           params.HostFn,
		planProjects:     params.PlanProjects,
		terminalStatesFn: params.TerminalStatesFn,
		runHistory:       params.RunHistory,
		runEventsFn:      params.RunEventsFn,
		runEventsAfterFn: params.RunEventsAfterFn,
		stats:            params.Stats,

		supervisorToken:      params.SupervisorToken,
		supervisorSessionCap: params.SupervisorSessionCap,
		supervisor:           params.Supervisor,
		supervisorLog:        params.SupervisorLog,

		modelUse:           params.ModelUse,
		configuredModelsFn: params.ConfiguredModelsFn,
		ratesFn:            params.RatesFn,
		priceDownloads:     params.PriceDownloads,
		workflowLoadFn:     params.WorkflowLoadFn,
		maskErrorFn:        params.MaskErrorFn,
		workersFn:          params.WorkersFn,
		workerHeaderFn:     params.WorkerIdentityHeaderFn,
		jobTokensFn:        params.JobTokensFn,
		dataPlaneFn:        params.DataPlaneFn,
		changes:            newChangeFeed(),
		timing: streamTiming{
			heartbeat:      15 * time.Second,
			signalInterval: time.Second,
			frameDeadline:  2 * time.Second,
		},
	}

	// Method-specific pattern so non-GET methods receive the default
	// 405 response from Go's ServeMux.
	mux.HandleFunc("GET /{$}", handleRoot)
	mux.HandleFunc("GET /favicon.ico", handleFavicon)
	mux.Handle("GET /ui/", ui.Handler())

	// Method-specific patterns ensure non-GET methods receive the
	// default 405 response from Go's ServeMux.
	mux.HandleFunc("GET /livez", s.handleLivez)
	mux.HandleFunc("GET /readyz", s.handleReadyz)

	// Method-agnostic patterns with internal method checking ensure
	// 405 responses use JSON error envelopes instead of the default
	// plain-text body from Go's ServeMux.
	mux.HandleFunc("/api/v1/state", s.routeState)
	mux.HandleFunc("/api/v1/plan/sync", s.controlRoute(scopeOperator, s.handlePlanSync))
	mux.HandleFunc("/api/v1/plan/graph", s.controlGet(s.handlePlanGraph))
	mux.HandleFunc("/api/v1/plan/capabilities", s.controlGet(s.handlePlanCapabilities))
	mux.HandleFunc("/api/v1/plan/projects/{key}", s.routePlanProject)
	mux.HandleFunc("/api/v1/plan/pull-back", s.controlRoute(scopeOperator, s.planControlHandler(func() PlanControlFunc { return s.pullBackFn })))
	mux.HandleFunc("/api/v1/plan/cancel", s.controlRoute(scopeOperator, s.planControlHandler(func() PlanControlFunc { return s.planCancelFn })))
	mux.HandleFunc("/api/v1/runs", s.routeRuns)
	mux.HandleFunc("/api/v1/runs/issues", s.routeIssueGroups)
	mux.HandleFunc("/api/v1/runs/{dispatch_id}/events", s.routeRunEvents)
	mux.HandleFunc("/api/v1/stats", s.routeStats)
	mux.HandleFunc("/api/v1/prices", s.routePrices)
	mux.HandleFunc("/api/v1/refresh", s.controlRoute(scopeOperator, s.handleRefresh))
	mux.HandleFunc("/api/v1/dispatch/pause", s.controlRoute(scopeOperator, s.handleDispatchPause))
	mux.HandleFunc("/api/v1/dispatch/resume", s.controlRoute(scopeOperator, s.handleDispatchResume))
	mux.HandleFunc("/api/v1/retries/{identifier}/run", s.controlRoute(scopeOperator, s.handleRetryNow))
	mux.HandleFunc("/api/v1/attention/{id}/acknowledge", s.controlRoute(scopeOperator, s.handleAcknowledge))
	mux.HandleFunc("/api/v1/reactions/{identifier}/{kind}/check", s.controlRoute(scopeOperator, s.handleCheckNow))
	for _, route := range workerRoutes {
		mux.HandleFunc(route, s.routeWorker)
	}
	for _, route := range dataplane.Routes {
		mux.HandleFunc(route, s.routeDataPlane)
	}
	mux.HandleFunc("/api/v1/tokens/introspect", s.controlRoute(scopeOperator, s.handleIntrospect))
	mux.HandleFunc("/api/v1/events", s.routeEvents)
	mux.HandleFunc("/api/v1/{identifier}", s.routeIssueDetail)
	mux.HandleFunc("/api/v1/{identifier}/hold", s.controlRoute(scopeSessionSupervisor, s.recordSupervised(supervisorActionHold, s.handleHold)))
	mux.HandleFunc("/api/v1/{identifier}/release", s.controlRoute(scopeSessionSupervisor, s.recordSupervised(supervisorActionReleaseHold, s.handleRelease)))
	mux.HandleFunc("/api/v1/{identifier}/escalate", s.controlRoute(scopeSession, s.handleEscalate))
	mux.HandleFunc("/api/v1/{identifier}/answer", s.supervisorRoute(supervisorActionAnswer, s.handleAnswer))
	mux.HandleFunc("/api/v1/{identifier}/sessions", s.supervisorRoute(supervisorActionGrant, s.handleSessionGrant))
	s.registerSupervisorRoutes(mux)
	mux.HandleFunc("/api/v1/{identifier}/broker", s.routeBroker)

	if params.MetricsRegistry != nil {
		mux.Handle("GET /metrics", promhttp.InstrumentMetricHandler(
			params.MetricsRegistry,
			promhttp.HandlerFor(params.MetricsRegistry, promhttp.HandlerOpts{}),
		))
	}

	protection := http.NewCrossOriginProtection()
	protection.SetDenyHandler(http.HandlerFunc(s.crossOriginForbidden))

	s.httpServer = &http.Server{
		Addr:              params.Addr,
		Handler:           protection.Handler(mux),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	s.httpServer.RegisterOnShutdown(s.changes.close)

	return s
}

// Mux returns the underlying [*http.ServeMux] so callers can register
// additional routes beyond those registered by [New]. When
// [Params.MetricsRegistry] is non-nil, New registers GET /metrics
// internally; Mux remains available for any further external routes. A
// handler built from the returned mux alone skips the cross-origin check.
func (s *Server) Mux() *http.ServeMux {
	return s.mux
}

// ListenAndServe starts the HTTP listener. Blocks until the server is
// shut down or an unrecoverable error occurs. Returns
// [http.ErrServerClosed] on graceful shutdown.
func (s *Server) ListenAndServe() error {
	return s.httpServer.ListenAndServe()
}

// Serve accepts connections on the provided [net.Listener]. Use this
// instead of [Server.ListenAndServe] when the listener is pre-bound
// (e.g. to discover the actual port for ephemeral port 0).
func (s *Server) Serve(ln net.Listener) error {
	return s.httpServer.Serve(ln)
}

// Shutdown gracefully shuts down the server without interrupting
// in-flight requests. Sets the draining flag as defense-in-depth
// before delegating to [http.Server.Shutdown]. Every open event stream
// ends as soon as shutdown begins, so its connection stops counting as
// active.
func (s *Server) Shutdown(ctx context.Context) error {
	s.drainingFlag.Store(true)
	return s.httpServer.Shutdown(ctx)
}

// SetDraining marks the server as draining. After this call, /livez
// and /readyz return 503. The listener remains open so K8s probes
// receive HTTP responses rather than connection refused. Safe to call
// from any goroutine.
func (s *Server) SetDraining() {
	s.drainingFlag.Store(true)
}

// OnStateChange satisfies [orchestrator.Observer] and tells every open event
// stream that something the API serves has changed. It performs no I/O,
// never blocks, and waits on nothing, so it is safe for concurrent use from
// any goroutine, before [Server.Serve] and after [Server.Shutdown].
func (s *Server) OnStateChange() {
	s.changes.publish()
}

// handleRoot redirects to the dashboard application. The header is written
// directly because [http.Redirect] adds a body to GET responses, and the
// status is 302 so that browsers do not cache a redirect that may change.
func handleRoot(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Location", "/ui/")
	w.WriteHeader(http.StatusFound)
}
