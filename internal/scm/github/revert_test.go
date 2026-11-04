package github

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
)

func TestRevertPullRequest(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body graphqlRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(body.Query, "mutation") {
			if body.Variables["id"] != "PR_node7" || body.Variables["title"] != "Revert ONS-1" || body.Variables["body"] != "why" {
				t.Errorf("mutation variables = %v", body.Variables)
			}
			_, _ = w.Write([]byte(`{"data":{"revertPullRequest":{"revertPullRequest":{"number":9,"url":"https://github.com/acme/app/pull/9"}}}}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":{"repository":{"pullRequest":{"id":"PR_node7","isDraft":false}}}}`))
	}))
	defer srv.Close()

	got, err := newTestSCMAdapter(t, srv.URL).RevertPullRequest(context.Background(), 7, "acme", "app", "Revert ONS-1", "why")
	if err != nil {
		t.Fatalf("RevertPullRequest() error = %v", err)
	}
	if got.Number != 9 || got.URL != "https://github.com/acme/app/pull/9" || !got.Created {
		t.Errorf("RevertPullRequest() = %+v, want #9 created", got)
	}
}

func TestRevertPullRequest_NoPullRequest(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body graphqlRequestBody
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(body.Query, "mutation") {
			_, _ = w.Write([]byte(`{"data":{"revertPullRequest":{"revertPullRequest":null}}}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":{"repository":{"pullRequest":{"id":"PR_node7","isDraft":false}}}}`))
	}))
	defer srv.Close()

	_, err := newTestSCMAdapter(t, srv.URL).RevertPullRequest(context.Background(), 7, "acme", "app", "t", "b")
	var scmErr *domain.SCMError
	if !errors.As(err, &scmErr) || scmErr.Kind != domain.ErrSCMPayload {
		t.Errorf("RevertPullRequest() error = %v, want a payload error", err)
	}
}

func TestCommitContains(t *testing.T) {
	t.Parallel()

	statuses := map[string]string{"deployed": "ahead", "same": "identical", "old": "behind", "fork": "diverged"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "/repos/acme/app/compare/merge1..."
		if !strings.HasPrefix(r.URL.Path, prefix) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		status, ok := statuses[strings.TrimPrefix(r.URL.Path, prefix)]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"Not Found"}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"` + status + `"}`))
	}))
	defer srv.Close()
	a := newTestSCMAdapter(t, srv.URL)

	for commit, want := range map[string]bool{"deployed": true, "same": true, "old": false, "fork": false} {
		got, err := a.CommitContains(context.Background(), "acme", "app", commit, "merge1")
		if err != nil || got != want {
			t.Errorf("CommitContains(%s) = %v, %v, want %v", commit, got, err, want)
		}
	}
	_, err := a.CommitContains(context.Background(), "acme", "app", "unknown", "merge1")
	var scmErr *domain.SCMError
	if !errors.As(err, &scmErr) || scmErr.Kind != domain.ErrSCMNotFound {
		t.Errorf("CommitContains(unknown) error = %v, want not found", err)
	}
}

// A revert merged by a person through GitHub's revert button and one made
// with git revert are both found; a pull request that reverts nothing, one
// still open, and one older than since are not.
func TestFindMergedReverts(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/acme/app/pulls":
			q := r.URL.Query()
			if q.Get("state") != "closed" || q.Get("sort") != "updated" || q.Get("direction") != "desc" {
				t.Errorf("list query = %v", q)
			}
			_, _ = w.Write([]byte(`[
				{"number":144,"title":"Revert \"feat: ONS-121\"","body":"Reverts acme/app#142","merged_at":"2026-11-03T10:00:00Z","updated_at":"2026-11-03T10:00:00Z","merge_commit_sha":"AAA1111"},
				{"number":143,"title":"Revert the flag","body":null,"merged_at":"2026-11-03T09:00:00Z","updated_at":"2026-11-03T09:00:00Z","merge_commit_sha":"bbb2222"},
				{"number":141,"title":"feat: something","body":"adds it","merged_at":"2026-11-03T08:00:00Z","updated_at":"2026-11-03T08:00:00Z"},
				{"number":140,"title":"Revert x","body":"Reverts acme/app#100","merged_at":null,"updated_at":"2026-11-03T07:00:00Z"},
				{"number":139,"title":"Revert y","body":"Reverts acme/app#99","merged_at":"2026-10-01T07:00:00Z","updated_at":"2026-10-01T07:00:00Z"}
			]`))
		case "/repos/acme/app/pulls/144/commits":
			_, _ = w.Write([]byte(`[{"commit":{"message":"Revert \"feat: ONS-121\"\n\nThis reverts commit 0123456789abcdef0123456789abcdef01234567."}}]`))
		case "/repos/acme/app/pulls/143/commits":
			_, _ = w.Write([]byte(`[{"commit":{"message":"Revert \"flag\"\n\nThis reverts commit DEADBEEF1."}}]`))
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	since := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	got, err := newTestSCMAdapter(t, srv.URL).FindMergedReverts(context.Background(), "acme", "app", since)
	if err != nil {
		t.Fatalf("FindMergedReverts() error = %v", err)
	}
	want := []domain.MergedRevert{
		{Number: 144, MergedAt: time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC), MergeCommitSHA: "aaa1111",
			Commits: []string{"0123456789abcdef0123456789abcdef01234567"}, PullRequests: []int{142}},
		{Number: 143, MergedAt: time.Date(2026, 11, 3, 9, 0, 0, 0, time.UTC), MergeCommitSHA: "bbb2222",
			Commits: []string{"deadbeef1"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("FindMergedReverts() = %+v, want %+v", got, want)
	}
}
