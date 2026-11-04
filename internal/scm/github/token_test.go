package github

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/adaptertest"
	"github.com/sortie-ai/sortie/internal/domain"
)

func TestInspectToken_ClassicTokenScopesAndExpiry(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rate_limit" {
			t.Errorf("path = %q, want /rate_limit", r.URL.Path)
		}
		w.Header().Set("X-OAuth-Scopes", "repo, read:org")
		w.Header().Set("GitHub-Authentication-Token-Expiration", "2026-11-05 10:00:00 UTC")
		_, _ = w.Write([]byte(`{"rate":{"limit":5000}}`))
	}))
	defer srv.Close()

	report, err := newTestSCMAdapter(t, srv.URL).InspectToken(t.Context())
	if err != nil {
		t.Fatalf("InspectToken: %v", err)
	}
	if !slices.Equal(report.Scopes, []string{"repo", "read:org"}) {
		t.Errorf("Scopes = %v, want [repo read:org]", report.Scopes)
	}
	if want := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC); !report.ExpiresAt.Equal(want) {
		t.Errorf("ExpiresAt = %v, want %v", report.ExpiresAt, want)
	}
}

func TestInspectToken_FineGrainedTokenListsNoScopes(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"rate":{"limit":5000}}`))
	}))
	defer srv.Close()

	report, err := newTestSCMAdapter(t, srv.URL).InspectToken(t.Context())
	if err != nil {
		t.Fatalf("InspectToken: %v", err)
	}
	if report.Scopes != nil || !report.ExpiresAt.IsZero() {
		t.Errorf("report = %+v, want no scopes and no expiry", report)
	}
}

func TestInspectToken_RefusedTokenIsAnAuthError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"message":"Bad credentials"}`))
	}))
	defer srv.Close()

	_, err := newTestSCMAdapter(t, srv.URL).InspectToken(t.Context())
	adaptertest.AssertSCMErrorKind(t, err, domain.ErrSCMAuth)
}

func TestChangeScopes_WorkflowFilesNeedTheWorkflowScope(t *testing.T) {
	t.Parallel()

	a := &GitHubSCMAdapter{}
	if got := a.ChangeScopes([]string{"README.md", ".github/workflows/ci.yml"}); !slices.Equal(got, []string{WorkflowScope}) {
		t.Errorf("ChangeScopes(workflow file) = %v, want [workflow]", got)
	}
	if got := a.ChangeScopes([]string{"README.md", ".github/CODEOWNERS", "docs/.github/workflows/x.yml"}); got != nil {
		t.Errorf("ChangeScopes(no workflow file) = %v, want nil", got)
	}
}
