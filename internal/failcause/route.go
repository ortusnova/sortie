package failcause

import (
	"regexp"
	"strings"
)

// Route names who can act on a stopped failure: the story's own agent, or
// the platform the story runs on.
type Route string

const (
	// RouteStory is a failure the story's agent can fix in its own work,
	// such as a failing test.
	RouteStory Route = "story"

	// RoutePlatform is a failure of what the story runs on, such as a
	// token without a scope. No session of the story fixes it.
	RoutePlatform Route = "platform"
)

// ClassUnclassified is the fault class of a signature no class lists. It
// is a platform fault with a person action.
const ClassUnclassified = "unclassified"

// ActionTemplate is the action a person takes to clear a platform fault,
// and where to take it. Both texts may hold the placeholders {host}, the
// name of the host Sortie runs on, and {forge}, the forge the repository
// is on.
type ActionTemplate struct {
	Action string
	Where  string
}

// PersonAction is an [ActionTemplate] with its placeholders filled.
type PersonAction struct {
	Action string
	Where  string
}

// Render fills the placeholders of t.
func (t ActionTemplate) Render(host, forge string) PersonAction {
	r := strings.NewReplacer("{host}", host, "{forge}", forge)
	return PersonAction{Action: r.Replace(t.Action), Where: r.Replace(t.Where)}
}

// FaultSignature is one entry of a fault class's list: a pattern matched
// against the [Signature.String] form of a stopped failure, and the
// action that clears a platform fault only a person can clear. Action is
// nil for a story fault and for a platform fault Sortie's own check
// clears.
type FaultSignature struct {
	Pattern *regexp.Regexp
	Action  *ActionTemplate
}

// FaultClass is a class of stopped failure: its name, who it goes to, and
// the fixed list of signatures in it.
type FaultClass struct {
	Name       string
	Route      Route
	Signatures []FaultSignature
}

// Fault is where a stopped failure goes: its class, its route, and for a
// platform fault only a person can clear, the action that clears it.
type Fault struct {
	Class  string
	Route  Route
	Action *ActionTemplate
}

// PersonOnly reports whether only a person can clear the fault.
func (f Fault) PersonOnly() bool { return f.Route == RoutePlatform && f.Action != nil }

func sig(pattern string, action *ActionTemplate) FaultSignature {
	return FaultSignature{Pattern: regexp.MustCompile(pattern), Action: action}
}

var (
	workflowScopeAction = &ActionTemplate{
		Action: `Give {host}'s GitHub token "Workflows: Read and write"`,
		Where:  "GitHub, Settings > Developer settings > Personal access tokens: the token Sortie on {host} pushes with",
	}
	tokenScopeAction = &ActionTemplate{
		Action: "Give {host}'s {forge} token the scope the cause names",
		Where:  "{forge}, the settings of the token Sortie on {host} pushes and calls {forge} with",
	}
	tokenAction = &ActionTemplate{
		Action: "Replace {host}'s {forge} token with a valid one",
		Where:  "The credential Sortie on {host} reads for {forge}",
	}
	agentSignInAction = &ActionTemplate{
		Action: "Sign the agent runtime in again on {host}",
		Where:  "{host}, as the user Sortie runs as",
	}
	repositoryAccessAction = &ActionTemplate{
		Action: "Give {host}'s {forge} account write access to the repository",
		Where:  "{forge}, the repository's collaborator settings",
	}
	agentRuntimeAction = &ActionTemplate{
		Action: "Install the agent runtime on {host}, or correct the agent command in WORKFLOW.md",
		Where:  "{host}",
	}
	workflowAction = &ActionTemplate{
		Action: "Correct WORKFLOW.md on {host}: the cause names the setting",
		Where:  "WORKFLOW.md on {host}",
	}
	unclassifiedAction = &ActionTemplate{
		Action: "Read the cause, clear it on {host}, then mark this action done",
		Where:  "The failed run's error detail on {host}'s Sortie dashboard",
	}
)

// faultClasses is the fixed list of fault classes, in the order a
// signature is matched against them: the first class with a matching
// signature wins.
var faultClasses = []FaultClass{
	{Name: "token_scope", Route: RoutePlatform, Signatures: []FaultSignature{
		sig("(?i)without `?workflow`? scope", workflowScopeAction),
		sig(`(?i)insufficient[_ ]scope|token scope=|missing (?:the )?\S+ scope|resource not accessible by (?:personal access )?token`, tokenScopeAction),
	}},
	{Name: "credentials", Route: RoutePlatform, Signatures: []FaultSignature{
		sig(`(?i)authentication failed|bad credentials|invalid (?:username or password|credentials)|could not read username|\| (?:scm|tracker|ci)_auth_error \|`, tokenAction),
		sig(`\| credential_unverified \|`, agentSignInAction),
	}},
	{Name: "repository_access", Route: RoutePlatform, Signatures: []FaultSignature{
		sig(`(?i)permission to \S+ denied|repository not found|\| scm_not_found \|`, repositoryAccessAction),
	}},
	{Name: "agent_runtime", Route: RoutePlatform, Signatures: []FaultSignature{
		sig(`(?i)\| agent_not_found \||executable file not found|command not found`, agentRuntimeAction),
	}},
	{Name: "workflow_config", Route: RoutePlatform, Signatures: []FaultSignature{
		sig(`(?i)\bno_seed\b|configures no repository|only the agent runs in the job's workspace|closed to dispatch|no longer lists|^prompt_render`, workflowAction),
	}},
	{Name: "worker_lost", Route: RoutePlatform, Signatures: []FaultSignature{
		sig(`(?i)lease (?:held by worker \S+ )?expired|ended the attempt without returning|ended before its agent session started|stdio stream broke|worker daemon shut down`, nil),
	}},
	{Name: "environment", Route: RoutePlatform, Signatures: []FaultSignature{
		sig(`(?i)^workspace_preparation \| [^|]* \| (?:workspace preparation: )?execution environment`, nil),
	}},
	{Name: "story_tests", Route: RouteStory, Signatures: []FaultSignature{
		sig(`(?i)\bmake (?:test|lint|check|vet)\b`, nil),
		sig(`(?i)\btests? (?:failed|failing|fail)\b`, nil),
		sig(`--- FAIL\b|\bFAIL\b`, nil),
	}},
	{Name: "story_turn", Route: RouteStory, Signatures: []FaultSignature{
		sig(`\| (?:turn_failed|turn_timeout|turn_incomplete|turn_token_limit|turn_request_limit) \|`, nil),
	}},
	{Name: "story_branch", Route: RouteStory, Signatures: []FaultSignature{
		sig(`(?i)branch moved upstream|non-fast-forward|\(fetch first\)|merge conflict|\bCONFLICT \(|exported bundle (?:exceeds|names)`, nil),
	}},
}

// FaultClasses returns the fixed list of fault classes, in match order.
// The slice and its classes are copies; the patterns are shared and must
// not be changed.
func FaultClasses() []FaultClass {
	out := make([]FaultClass, len(faultClasses))
	for i, c := range faultClasses {
		c.Signatures = append([]FaultSignature(nil), c.Signatures...)
		out[i] = c
	}
	return out
}

// RouteOf returns where a stopped failure of signature s goes: the first
// class whose list has a signature matching s. A signature in no class is
// a platform fault of [ClassUnclassified] with a person action.
func RouteOf(s Signature) Fault {
	line := s.String()
	for _, c := range faultClasses {
		for _, fs := range c.Signatures {
			if fs.Pattern.MatchString(line) {
				return Fault{Class: c.Name, Route: c.Route, Action: fs.Action}
			}
		}
	}
	return Fault{Class: ClassUnclassified, Route: RoutePlatform, Action: unclassifiedAction}
}

// ForgeName returns the name a person knows the forge of an SCM provider
// kind by, such as "GitHub" for "github"; "the forge" when the kind is
// empty, and the kind itself when it is unknown.
func ForgeName(provider string) string {
	switch strings.ToLower(provider) {
	case "github":
		return "GitHub"
	case "gitlab":
		return "GitLab"
	case "gitea":
		return "Gitea"
	case "":
		return "the forge"
	}
	return provider
}
