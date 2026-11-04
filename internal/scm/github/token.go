package github

import (
	"context"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/scm/scmcore"
)

// WorkflowScope is the classic token scope GitHub requires of a push
// that creates or updates a file under .github/workflows.
const WorkflowScope = "workflow"

// workflowsDir holds the GitHub Actions workflow files.
const workflowsDir = ".github/workflows/"

// tokenExpirationHeader carries the expiry of a personal access token
// that has one.
const tokenExpirationHeader = "GitHub-Authentication-Token-Expiration"

var _ domain.SCMTokenInspector = (*GitHubSCMAdapter)(nil)

// InspectToken calls GET /rate_limit, which every valid token may read,
// and reports the scopes X-OAuth-Scopes grants a classic token and the
// expiry GitHub-Authentication-Token-Expiration gives a token that has
// one. A fine-grained token or an installation token lists no scopes, so
// Scopes is nil for it.
func (a *GitHubSCMAdapter) InspectToken(ctx context.Context) (domain.SCMTokenReport, error) {
	_, header, err := a.client.Get(ctx, "/rate_limit", nil)
	if err != nil {
		return domain.SCMTokenReport{}, scmcore.AsSCMError(err)
	}
	report := domain.SCMTokenReport{Scopes: splitScopes(header.Get("X-OAuth-Scopes"))}
	if len(report.Scopes) == 0 {
		report.Scopes = nil
	}
	report.ExpiresAt = parseTokenExpiration(header.Get(tokenExpirationHeader))
	return report, nil
}

// ChangeScopes returns [WorkflowScope] when a path is under
// .github/workflows, and nil otherwise: GitHub refuses a push that
// creates or updates a workflow file without it.
func (a *GitHubSCMAdapter) ChangeScopes(paths []string) []string {
	for _, p := range paths {
		if strings.HasPrefix(p, workflowsDir) {
			return []string{WorkflowScope}
		}
	}
	return nil
}

// parseTokenExpiration reads the value of [tokenExpirationHeader], such
// as "2026-11-05 10:00:00 UTC"; zero when it is absent or unreadable.
func parseTokenExpiration(value string) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	for _, layout := range []string{"2006-01-02 15:04:05 MST", "2006-01-02 15:04:05 -0700", time.RFC3339} {
		if at, err := time.Parse(layout, value); err == nil {
			return at.UTC()
		}
	}
	return time.Time{}
}
