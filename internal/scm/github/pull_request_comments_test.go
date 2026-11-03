package github

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
)

func TestFetchPullRequestComments(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/acme/app/issues/7/comments" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"not found"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
			{"id": 11, "body": "The table is missing the worker column.", "user": {"login": "ana", "type": "User"}, "created_at": "2026-11-02T10:00:00Z"},
			{"id": 12, "body": "Preview deployed.", "user": {"login": "deploy[bot]", "type": "Bot"}, "created_at": "2026-11-02T10:01:00Z"},
			{"id": 13, "body": "   ", "user": {"login": "ana", "type": "User"}, "created_at": "2026-11-02T10:02:00Z"}
		]`))
	}))
	defer srv.Close()

	got, err := newTestSCMAdapter(t, srv.URL).FetchPullRequestComments(context.Background(), 7, "acme", "app")
	if err != nil {
		t.Fatalf("FetchPullRequestComments() error = %v", err)
	}
	want := domain.ReviewComment{
		ID: "issuecomment-11", Reviewer: "ana", Body: "The table is missing the worker column.",
		SubmittedAt: time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC),
	}
	if len(got) != 1 || got[0] != want {
		t.Errorf("comments = %+v, want only %+v", got, want)
	}
}

func TestFetchPullRequestComments_Error(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"not found"}`))
	}))
	defer srv.Close()

	_, err := newTestSCMAdapter(t, srv.URL).FetchPullRequestComments(context.Background(), 7, "acme", "app")
	assertSCMErrorKind(t, err, domain.ErrSCMNotFound)
}
