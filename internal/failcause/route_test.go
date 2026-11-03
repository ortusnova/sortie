package failcause

import (
	"errors"
	"fmt"
	"testing"

	"github.com/sortie-ai/sortie/internal/domain"
)

// The fixed list of fault classes: a change to a class, its route, or a
// signature in it must change this table too.
func TestFaultClasses_AreAFixedList(t *testing.T) {
	t.Parallel()

	type entry struct {
		pattern string
		person  bool
	}
	want := []struct {
		name    string
		route   Route
		entries []entry
	}{
		{"token_scope", RoutePlatform, []entry{
			{"(?i)without `?workflow`? scope", true},
			{`(?i)insufficient[_ ]scope|token scope=|missing (?:the )?\S+ scope|resource not accessible by (?:personal access )?token`, true},
		}},
		{"credentials", RoutePlatform, []entry{
			{`(?i)authentication failed|bad credentials|invalid (?:username or password|credentials)|could not read username|\| (?:scm|tracker|ci)_auth_error \|`, true},
			{`\| credential_unverified \|`, true},
		}},
		{"repository_access", RoutePlatform, []entry{
			{`(?i)permission to \S+ denied|repository not found|\| scm_not_found \|`, true},
		}},
		{"agent_runtime", RoutePlatform, []entry{
			{`(?i)\| agent_not_found \||executable file not found|command not found`, true},
		}},
		{"workflow_config", RoutePlatform, []entry{
			{`(?i)\bno_seed\b|configures no repository|only the agent runs in the job's workspace|closed to dispatch|no longer lists|^prompt_render`, true},
		}},
		{"worker_lost", RoutePlatform, []entry{
			{`(?i)lease expired|ended the attempt without returning|ended before its agent session started|stdio stream broke`, false},
		}},
		{"environment", RoutePlatform, []entry{
			{`(?i)^workspace_preparation \| [^|]* \| (?:workspace preparation: )?execution environment`, false},
		}},
		{"story_tests", RouteStory, []entry{
			{`(?i)\bmake (?:test|lint|check|vet)\b`, false},
			{`(?i)\btests? (?:failed|failing|fail)\b`, false},
			{`--- FAIL\b|\bFAIL\b`, false},
		}},
		{"story_turn", RouteStory, []entry{
			{`\| (?:turn_failed|turn_timeout|turn_incomplete|turn_token_limit|turn_request_limit) \|`, false},
		}},
		{"story_branch", RouteStory, []entry{
			{`(?i)branch moved upstream|non-fast-forward|\(fetch first\)|merge conflict|\bCONFLICT \(|exported bundle (?:exceeds|names)`, false},
		}},
	}

	got := FaultClasses()
	if len(got) != len(want) {
		t.Fatalf("FaultClasses() has %d classes, want %d", len(got), len(want))
	}
	for i, w := range want {
		g := got[i]
		if g.Name != w.name || g.Route != w.route || len(g.Signatures) != len(w.entries) {
			t.Errorf("class %d = %s %s with %d signatures, want %s %s with %d", i, g.Name, g.Route, len(g.Signatures), w.name, w.route, len(w.entries))
			continue
		}
		for j, e := range w.entries {
			s := g.Signatures[j]
			if s.Pattern.String() != e.pattern || (s.Action != nil) != e.person {
				t.Errorf("%s signature %d = %q, person action %t; want %q, %t", w.name, j, s.Pattern, s.Action != nil, e.pattern, e.person)
			}
			if s.Action != nil && w.route != RoutePlatform {
				t.Errorf("%s signature %d: a story fault has a person action", w.name, j)
			}
		}
	}
}

func TestRouteOf(t *testing.T) {
	t.Parallel()

	refusal := errors.New("push sortie/ONS-49 to https://github.com/ortusnova/sortie.git: " +
		"[remote rejected] (refusing to allow a Personal Access Token to create or update workflow `.github/workflows/ci.yml` without `workflow` scope): git push: exit status 1")
	tests := []struct {
		name  string
		step  string
		err   error
		class string
		route Route
	}{
		{"workflow scope", "export", refusal, "token_scope", RoutePlatform},
		{"gitlab scope", "export", errors.New("ensure pull request: insufficient_scope"), "token_scope", RoutePlatform},
		{"gitea scope", "export", errors.New("token scope=[read:repository]"), "token_scope", RoutePlatform},
		{"push credentials", "export", errors.New("push: fatal: Authentication failed for 'https://github.com/o/r.git/'"), "credentials", RoutePlatform},
		{"scm auth", "export", &domain.SCMError{Kind: domain.ErrSCMAuth, Message: "401"}, "credentials", RoutePlatform},
		{"agent credential", "agent session start", &domain.AgentError{Kind: domain.ErrCredentialUnverified, Message: "sign in"}, "credentials", RoutePlatform},
		{"repository access", "export", errors.New("remote: Permission to ortusnova/sortie.git denied to taichi."), "repository_access", RoutePlatform},
		{"agent missing", "agent session start", &domain.AgentError{Kind: domain.ErrAgentNotFound, Message: "no such command"}, "agent_runtime", RoutePlatform},
		{"no seed", "workspace seed", errors.New(`flows: bundles: HTTP 404: {"error":{"code":"no_seed"}}`), "workflow_config", RoutePlatform},
		{"remote hooks", "workspace preparation", errors.New("hook start: under the remote environment kind only the agent runs in the job's workspace"), "workflow_config", RoutePlatform},
		{"prompt render", "prompt render (turn 1)", errors.New(`template: prompt:3: map has no entry for key "x"`), "workflow_config", RoutePlatform},
		{"lease expired", "run", errors.New("the worker's lease expired"), "worker_lost", RoutePlatform},
		{"no bundle back", "export", errors.New("the worker ended the attempt without returning the issue branch: gone"), "worker_lost", RoutePlatform},
		{"environment start", "workspace preparation", errors.New("execution environment: machine did not start"), "environment", RoutePlatform},
		{"make test", "agent turn 2", &domain.AgentError{Kind: domain.ErrTurnFailed, Message: "make test: exit status 2"}, "story_tests", RouteStory},
		{"tests failed", "run", errors.New("tests failed"), "story_tests", RouteStory},
		{"go test FAIL", "run", errors.New("FAIL github.com/x/y 0.1s"), "story_tests", RouteStory},
		{"turn failed", "agent turn 1", fmt.Errorf("x: %w", &domain.AgentError{Kind: domain.ErrTurnFailed, Message: "the model gave up"}), "story_turn", RouteStory},
		{"turn timeout", "agent turn 4", &domain.AgentError{Kind: domain.ErrTurnTimeout, Message: "deadline"}, "story_turn", RouteStory},
		{"branch moved", "export", errors.New("push: branch moved upstream"), "story_branch", RouteStory},
		{"bundle too big", "export", errors.New("the exported bundle exceeds max_bundle_mib (100 MiB)"), "story_branch", RouteStory},
		{"unknown", "run", errors.New("something nobody has seen"), ClassUnclassified, RoutePlatform},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := Sign(tt.step, tt.err)
			got := RouteOf(s)
			if got.Class != tt.class || got.Route != tt.route {
				t.Errorf("RouteOf(%q) = %s %s, want %s %s", s, got.Class, got.Route, tt.class, tt.route)
			}
			if got.Route == RouteStory && got.PersonOnly() {
				t.Errorf("RouteOf(%q): a story fault only a person can clear", s)
			}
		})
	}
}

func TestRouteOf_ASignatureInNoClassIsAPersonAction(t *testing.T) {
	t.Parallel()

	got := RouteOf(Sign("run", errors.New("the disk sang")))
	if got.Class != ClassUnclassified || got.Route != RoutePlatform || !got.PersonOnly() {
		t.Fatalf("RouteOf(unknown) = %+v, want an unclassified platform fault with a person action", got)
	}
}

func TestActionTemplate_Render(t *testing.T) {
	t.Parallel()

	got := RouteOf(Sign("export", errors.New("[remote rejected] (refusing to allow a Personal Access Token to create or update workflow without `workflow` scope)")))
	if !got.PersonOnly() {
		t.Fatalf("RouteOf(workflow scope) = %+v, want a person action", got)
	}
	action := got.Action.Render("taichi", ForgeName("github"))
	if action.Action != `Give taichi's GitHub token "Workflows: Read and write"` {
		t.Errorf("action = %q", action.Action)
	}
	if action.Where != "GitHub, Settings > Developer settings > Personal access tokens: the token Sortie on taichi pushes with" {
		t.Errorf("where = %q", action.Where)
	}
}

func TestForgeName(t *testing.T) {
	t.Parallel()

	for provider, want := range map[string]string{"github": "GitHub", "GitLab": "GitLab", "gitea": "Gitea", "": "the forge", "forgejo": "forgejo"} {
		if got := ForgeName(provider); got != want {
			t.Errorf("ForgeName(%q) = %q, want %q", provider, got, want)
		}
	}
}
