package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// AgentEventType enumerates the normalized event types that agent
// adapters emit.
type AgentEventType string

const (
	EventSessionStarted AgentEventType = "session_started"
	EventStartupFailed  AgentEventType = "startup_failed"
	EventTurnCompleted  AgentEventType = "turn_completed"
	EventTurnFailed     AgentEventType = "turn_failed"
	EventTurnCancelled  AgentEventType = "turn_cancelled"

	EventTurnEndedWithError AgentEventType = "turn_ended_with_error"

	// EventTurnInputRequired is a declared, non-retryable ending: the
	// agent asked for a decision only a person could give.
	EventTurnInputRequired AgentEventType = "turn_input_required"

	EventTokenUsage   AgentEventType = "token_usage"
	EventNotification AgentEventType = "notification"
	EventOtherMessage AgentEventType = "other_message"
	EventMalformed    AgentEventType = "malformed"

	// EventToolResult indicates a tool call completed, with ToolName,
	// ToolKind, ToolArgument, and ToolDurationMS populated when the adapter
	// can observe them.
	EventToolResult AgentEventType = "tool_result"
)

// TokenUsage holds normalized token counts emitted by agent adapters.
// Every field is cumulative over the agent session an adapter opened
// with StartSession and excludes usage a resumed session accumulated
// before StartSession, so re-reporting a figure is safe: the orchestrator
// folds deltas against what it already recorded. Adapters compute
// TotalTokens as InputTokens plus OutputTokens rather than passing a
// vendor total through.
type TokenUsage struct {
	// InputTokens is cumulative, including prompt-cache reads and writes.
	InputTokens int64

	// OutputTokens is cumulative, including reasoning tokens.
	OutputTokens int64

	// TotalTokens equals InputTokens plus OutputTokens.
	TotalTokens int64

	// CacheReadTokens is the cache-read subset of InputTokens, never
	// added to any other counter. Zero when the adapter reports no cache
	// data.
	CacheReadTokens int64

	// CacheWriteTokens counts input tokens written to the prompt cache.
	// It is disjoint from CacheReadTokens, never added to any other
	// counter, and zero when the runtime reports no cache-write count.
	CacheWriteTokens int64
}

// Uncached returns TotalTokens less CacheReadTokens, floored at zero.
// Cache writes stay in the result.
func (u TokenUsage) Uncached() int64 {
	return max(u.TotalTokens-u.CacheReadTokens, 0)
}

// ToolKind is the kind of work a tool call does. It picks the call's icon and
// fold phrase and whether the call has a main argument; empty for any other
// tool.
type ToolKind string

const (
	ToolKindShell ToolKind = "shell"
	ToolKindRead  ToolKind = "read"
	ToolKindEdit  ToolKind = "edit"
	ToolKindSkill ToolKind = "skill"
)

// SkillCall is one skill call of a run.
type SkillCall struct {
	Skill     string `json:"skill"`
	ToolError bool   `json:"tool_error,omitempty"`
}

// AgentEvent is a normalized event emitted by an agent adapter over the
// lifetime of an agent session, including startup and individual turns.
type AgentEvent struct {
	Type AgentEventType

	// Timestamp is the UTC time the event was observed.
	Timestamp time.Time

	// AgentPID is the agent process ID, or empty when not applicable.
	AgentPID string

	// Usage carries the session's run-cumulative token counts as of this
	// event. The zero value means the event carries no usage information,
	// not that the session has consumed no tokens.
	Usage TokenUsage

	// UsageByModel is the per-model split of Usage, in the same scope and
	// covering at most its totals. Nil when the event names the model only
	// through Model.
	UsageByModel map[string]TokenUsage

	// Message is an adapter-normalized summary of the event payload. May
	// be empty.
	Message string

	// SessionID is the adapter-assigned session identifier, populated on
	// EventSessionStarted and empty otherwise. The orchestrator copies a
	// non-empty value into RunningEntry.SessionID.
	SessionID string

	// Model is the LLM model identifier reported on token_usage events,
	// or empty when unknown.
	Model string

	// UsageWindows carries the provider usage windows this event reports;
	// nil when it reports none. A window the event does not carry keeps its
	// earlier report.
	UsageWindows []UsageWindow

	// APIDurationMS is the LLM API wait time in milliseconds for this
	// event; a value > 0 contributes to the session's cumulative API
	// time. Zero when unavailable.
	APIDurationMS int64

	// ToolName is the name the runtime gives the tool that completed, or
	// "{server}.{tool}" for an MCP or dynamic tool, for tool_result events.
	// Empty when the runtime names none; never argument text or a protocol
	// item type.
	ToolName string

	// ToolKind is the kind of work the completed tool call did, for
	// tool_result events. Empty for any other tool.
	ToolKind ToolKind

	// ToolArgument is the call's one main argument as the runtime sent it: a
	// shell call's command, a read or edit call's path (several joined by
	// ", "), a skill call's skill. Empty for any other tool and when the
	// runtime gives none.
	ToolArgument string

	// ToolDurationMS is the tool call's wall-clock time in milliseconds,
	// for tool_result events. Zero when unavailable.
	ToolDurationMS int64

	// ToolError reports whether a tool_result call returned an error.
	ToolError bool
}

// UsageWindowStatus is how near a usage window is to its limit, as the
// runtime reported it.
type UsageWindowStatus string

const (
	// UsageWindowAllowed means the window is under its limit.
	UsageWindowAllowed UsageWindowStatus = "allowed"
	// UsageWindowWarning means the window is near its limit.
	UsageWindowWarning UsageWindowStatus = "warning"
	// UsageWindowExhausted means the window is used up until it resets.
	UsageWindowExhausted UsageWindowStatus = "exhausted"
)

// UsageWindow is one provider usage window in runtime-neutral form, as
// the usage component normalized it. Immutable once emitted.
type UsageWindow struct {
	// Key is the window's identifier as its source reported it, its
	// identity within one agent kind. Empty when the report names none.
	Key string

	// Label is the display name, for example "5-hour limit".
	Label string

	// Minutes is the window length; 0 when not reported.
	Minutes int64

	// UsedPercent is the share used in percent and may exceed 100. Nil
	// when not reported.
	UsedPercent *float64

	// Status is empty when not reported.
	Status UsageWindowStatus

	// ResetsAt is in UTC; zero when not reported.
	ResetsAt time.Time

	// ReportedAt is the UTC instant the adapter observed the report.
	ReportedAt time.Time

	// Amount is nil when the window reports no amount.
	Amount *UsageWindowAmount
}

// UsageWindowAmount is a window's use in its vendor's own unit, never
// converted.
type UsageWindowAmount struct {
	// Used is in Unit and not negative.
	Used float64

	// Limit is in Unit and above 0.
	Limit float64

	// Unit is a plural display name, for example "credits".
	Unit string
}

// AgentCommand is an agent launch command in one of two forms: the
// string form in Line, or the list form in a non-empty Argv, never
// both. The zero value holds no command.
type AgentCommand struct {
	// Line is the string form.
	Line string

	// Argv is the list form: element zero names the executable and
	// each later element is one argument.
	Argv []string
}

// IsZero reports whether c holds neither form.
func (c AgentCommand) IsZero() bool {
	return c.Line == "" && len(c.Argv) == 0
}

// NamesExecutable reports whether c can launch a process: it is a list,
// or a string holding a non-whitespace character.
func (c AgentCommand) NamesExecutable() bool {
	return len(c.Argv) > 0 || strings.TrimSpace(c.Line) != ""
}

// AgentConfig is the subset of configuration relevant to agent
// adapters, passed into [StartSessionParams] so adapters do not depend
// on the full config package.
type AgentConfig struct {
	// Kind identifies the agent adapter (e.g. "claude-code", "mock").
	Kind string

	// Command is the string form of the launch command. Locally it is
	// split on whitespace into an argv the adapter execs directly with
	// no shell; in SSH mode it is passed through unsplit to the remote
	// shell. Empty when CommandArgv holds the command or when the kind
	// launches its own default command.
	Command string

	// CommandArgv is the list form of the launch command: element zero
	// names the executable and each later element is one argument,
	// launched exactly as written locally and single-quoted per element
	// remotely. Set only when Command is empty.
	CommandArgv []string

	// TurnTimeoutMS bounds a single agent turn. Always positive: the
	// config layer rejects a non-positive value rather than treating it
	// as a disable sentinel.
	TurnTimeoutMS int

	// ReadTimeoutMS is the request/response timeout during startup and
	// synchronous requests.
	ReadTimeoutMS int

	// StallTimeoutMS bounds event inactivity before the session is
	// considered stalled. Non-positive disables stall detection.
	StallTimeoutMS int

	// StopGraceMS is how long an adapter waits after a catchable
	// termination signal before force-terminating the process group. A
	// value from workflow config is always positive; a value assembled
	// in code may be zero, so adapters resolve it through the shared
	// helper that maps non-positive to the built-in grace rather than
	// reading it as no limit.
	StopGraceMS int
}

// Session is an opaque handle returned by [AgentAdapter.StartSession].
// The orchestrator passes the handle back to RunTurn and StopSession and
// may copy ID and AgentPID for observability, but must treat Internal as
// adapter-owned and opaque.
type Session struct {
	// ID is the adapter-assigned session identifier.
	ID string

	// AgentPID is the agent subprocess PID, or empty for HTTP-based
	// adapters.
	AgentPID string

	// NoToolChannel reports that the runtime cannot reach Sortie's tool
	// server, so the worker withholds the tool advertisement.
	NoToolChannel bool

	// Internal holds adapter-specific state the orchestrator must not
	// read or modify.
	Internal any
}

// StartSessionParams contains the inputs for
// [AgentAdapter.StartSession].
type StartSessionParams struct {
	// WorkspacePath is the absolute per-issue workspace directory the
	// adapter must launch the agent with as cwd.
	WorkspacePath string

	// AgentConfig is the typed agent configuration from WORKFLOW.md.
	AgentConfig AgentConfig

	// ResumeSessionID, when non-empty, resumes the existing conversation
	// from a previous worker attempt instead of starting fresh. Adapters
	// without session continuity ignore it.
	ResumeSessionID string

	// ResumeUsage is the figure the newest run on ResumeSessionID ended
	// with; nil when ResumeSessionID is empty or that run recorded none.
	// Adapters whose runtime keeps no such figure ignore it.
	ResumeUsage *TokenUsage

	// ResumeUsageByModel is the per-model split of ResumeUsage; nil when none.
	ResumeUsageByModel map[string]TokenUsage

	// SSHHost, when non-empty, launches the agent on that host via SSH
	// instead of as a local subprocess.
	SSHHost string

	// SSHStrictHostKeyChecking is the OpenSSH StrictHostKeyChecking value
	// for remote sessions; adapters default to "accept-new" when empty.
	// Only meaningful when SSHHost is non-empty.
	SSHStrictHostKeyChecking string

	// SSHEnvNames lists environment variable names to carry from the
	// orchestrator's environment into a remote session. It carries names
	// only; adapters resolve each value at launch. Only meaningful when
	// SSHHost is non-empty.
	SSHEnvNames []string

	// MCPConfigPath, when non-empty, names a file directly inside the
	// workspace's .sortie directory: the absolute merged MCP config path
	// adapters inject via their MCP config CLI flag. When empty, adapters
	// use the operator-configured mcp_config passthrough.
	MCPConfigPath string

	// Settings is the resolved settings block of the session's agent
	// kind. It is read-only, and nil reads as an empty block.
	Settings map[string]any

	// CredentialVerification marks a session that carries only the
	// request proving the runtime's credential works. The session runs
	// under the contract its kind declares in
	// `registry.AgentMeta.VerificationContext`.
	CredentialVerification bool

	// SessionEnv lists the variables that tell the agent process which
	// session it serves. Every launch of the session sets them last, so
	// each overrides a variable of the same name the launch would
	// otherwise inherit, and a remote launch carries them the way it
	// carries its other variables, never as arguments. An entry with an
	// empty Value is still set, to the empty string.
	SessionEnv []EnvVar

	// Carrier, when non-nil, launches the agent inside the execution
	// environment it stands for rather than on this host. It never
	// combines with SSHHost. Nil launches the agent on this host or,
	// with SSHHost set, over SSH.
	Carrier LaunchCarrier

	// HostPrefix, when non-empty, is the argv that runs on this host in
	// front of every launch of the session's agent: the host process
	// becomes HostPrefix followed by the launch it would otherwise be,
	// whether local, over SSH, or through Carrier. HostPrefix[0] is
	// resolved on this host. The prefix process inherits the session's
	// full host environment, SessionEnv included. Auxiliary runtime
	// commands, such as a version query, never run behind it.
	HostPrefix []string

	// VerificationFullContext is set only together with
	// CredentialVerification. It asks for a verification session
	// without the switches that drop agent context.
	VerificationFullContext bool
}

// LaunchCarrier carries an agent launch into an execution environment
// that runs the workspace's code somewhere other than this host. The
// agent binary is resolved inside that environment. Implementations must
// be safe for concurrent use.
type LaunchCarrier interface {
	// LaunchArgv returns the host argv, program first, whose process
	// runs launch.Argv in the workspace inside the environment, with its
	// standard input and output connected to that host process. The
	// carrier passes every variable it carries through the argv, never
	// through standard input, which belongs to the agent alone.
	LaunchArgv(launch CarrierLaunch) ([]string, error)

	// Workspace returns the workspace's path inside the environment,
	// where the agent runs.
	Workspace() string

	// ToolChannel returns how the agent reaches a tool server on this
	// host, and false when the environment offers no such channel.
	ToolChannel() (ToolChannel, bool)

	// ConnectionFailed reports whether a carrier process that exited
	// with exitCode failed before the agent command started, such as an
	// environment that is not running. stderrTail holds the last lines
	// the carrier process wrote to standard error, or nil where the
	// caller holds none.
	ConnectionFailed(exitCode int, stderrTail []string) bool
}

// ToolChannel is the path from an execution environment to a tool server
// on this host: a connection the agent opens to GuestPort on the
// environment's loopback arrives at the Unix socket HostSocket on this
// host. Whoever serves tools listens on HostSocket; the environment keeps
// the guest side open.
type ToolChannel struct {
	HostSocket string
	GuestPort  int

	// Authorize, when set, receives the bearer the tool server behind
	// HostSocket authorizes requests with, once the channel opens. It
	// serves an environment that authorizes the agent's requests itself
	// and presents that bearer on the agent's behalf. Nil hands the
	// bearer to no one.
	Authorize func(bearer string)
}

// CarrierLaunch describes one agent launch for
// [LaunchCarrier.LaunchArgv].
type CarrierLaunch struct {
	// Argv is the agent command and its arguments. Argv[0] is resolved
	// inside the environment.
	Argv []string

	// Env holds the session's KEY=VALUE identity variables as a launch on
	// this host would set them. The carrier decides which of them cross
	// into the environment and how each is rewritten there.
	Env []string

	// AgentEnv holds KEY=VALUE variables the agent adapter computed for
	// this launch, such as a runtime setting. The carrier passes each to
	// the agent unchanged. None of them is a credential.
	AgentEnv []string

	// SecretEnv names variables the agent receives with the value the
	// carrier process inherits on this host. Their values never appear in
	// the argv; the caller sets them in the carrier process's
	// environment.
	SecretEnv []string

	// Unset names variables the agent must run without, even when the
	// environment itself defines them.
	Unset []string
}

// EnvVar is one environment variable a launch sets. Every rendering of
// an EnvVar - through [fmt], through a [log/slog] handler, or through
// [encoding/json.Marshal] - carries Name and never Value, so a value
// that is a credential never reaches a log record.
type EnvVar struct {
	Name  string
	Value string
}

// Format implements [fmt.Formatter], rendering only v.Name.
func (v EnvVar) Format(f fmt.State, _ rune) {
	_, _ = fmt.Fprintf(f, "domain.EnvVar{Name:%q}", v.Name) //nolint:errcheck // best-effort formatting
}

// LogValue implements [slog.LogValuer], rendering only v.Name.
func (v EnvVar) LogValue() slog.Value {
	return slog.StringValue(v.Name)
}

// MarshalJSON implements [json.Marshaler], rendering only v.Name.
func (v EnvVar) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Name string `json:"name"`
	}{Name: v.Name})
}

// ReducedVerificationContext reports whether the session is a
// credential-verification session that drops agent context.
func (p StartSessionParams) ReducedVerificationContext() bool {
	return p.CredentialVerification && !p.VerificationFullContext
}

// RunTurnParams contains the inputs for [AgentAdapter.RunTurn].
type RunTurnParams struct {
	// Prompt is the fully rendered prompt for this turn.
	Prompt string

	// Issue is the normalized issue being worked on.
	Issue Issue

	// OnEvent delivers events during the turn. Must be non-nil.
	// Implementations must not retain or call it after RunTurn returns.
	OnEvent func(AgentEvent)
}

// TurnResult is the outcome of a single agent turn returned by
// [AgentAdapter.RunTurn].
type TurnResult struct {
	// SessionID is the opaque session identifier, which may change
	// between turns for adapters that rotate identifiers.
	SessionID string

	// ExitReason summarizes why the turn ended, mapping to the
	// normalized event types.
	ExitReason AgentEventType

	// Usage carries the session's run-cumulative token counts as of this
	// turn's completion, not just this turn's contribution.
	Usage TokenUsage

	// UsageMeasured reports whether the adapter observed at least one
	// usage figure for the session. A false value with zero Usage means
	// spend is unknown, not zero. Once true for a session it must not
	// become false on a later turn.
	UsageMeasured bool

	// SpendUnaccounted reports that this turn issued a model request no
	// figure was proven to cover in full, making the run's summed total a
	// lower bound. Unlike UsageMeasured it is per turn and not monotone,
	// and any partial figure is still reported in Usage.
	SpendUnaccounted bool

	// SpendCoverable reports that this turn completed with unsettled spend
	// that a later session total of the same session would cover: the
	// turn served model output, no figure was proven to cover it in full,
	// and its figure rests on no estimated baseline.
	SpendCoverable bool

	// CoversSession reports that this turn's figure is the runtime's
	// running session total, holding what every earlier completed turn of
	// the session spent, read or not.
	CoversSession bool

	// ResumeUsage is the usage the agent runtime itself holds for the
	// session at this turn's end, earlier runs on the session included,
	// which a later run resuming the session receives as
	// StartSessionParams.ResumeUsage. Nil when the runtime keeps no such
	// figure or the adapter cannot state it exactly.
	ResumeUsage *TokenUsage

	// ResumeUsageByModel is the per-model split of ResumeUsage, handed to a
	// later run as StartSessionParams.ResumeUsageByModel; nil when none.
	ResumeUsageByModel map[string]TokenUsage
}

// AgentAdapter is the contract every coding-agent integration must
// satisfy, normalizing native protocol events into domain types.
// Implementations must be safe for concurrent use across workers.
type AgentAdapter interface {
	// StartSession launches or connects to an agent in the given
	// workspace and returns an opaque handle. The caller must eventually
	// call StopSession.
	StartSession(ctx context.Context, params StartSessionParams) (Session, error)

	// RunTurn executes one turn, delivering events via params.OnEvent,
	// and returns when the turn completes. Continuation turns reuse the
	// same [Session].
	RunTurn(ctx context.Context, session Session, params RunTurnParams) (TurnResult, error)

	// StopSession terminates the agent cleanly. Must be called exactly
	// once per session, and is safe to call after a failed RunTurn.
	StopSession(ctx context.Context, session Session) error
}
