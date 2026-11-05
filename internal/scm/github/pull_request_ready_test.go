package github

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/sortie-ai/sortie/internal/domain"
)

func TestMarkPullRequestReadyForReview(t *testing.T) {
	t.Parallel()

	var mutations atomic.Int64
	srv := draftServer(t, true, `{"data":{"markPullRequestReadyForReview":{"pullRequest":{"isDraft":false}}}}`, &mutations)
	defer srv.Close()
	if err := newTestSCMAdapter(t, srv.URL).MarkPullRequestReadyForReview(context.Background(), 7, "acme", "app"); err != nil {
		t.Fatalf("MarkPullRequestReadyForReview() error = %v", err)
	}
	if mutations.Load() != 1 {
		t.Errorf("mutations = %d, want 1", mutations.Load())
	}
}

func TestMarkPullRequestReadyForReview_NotDraft(t *testing.T) {
	t.Parallel()

	var mutations atomic.Int64
	srv := draftServer(t, false, `{}`, &mutations)
	defer srv.Close()
	if err := newTestSCMAdapter(t, srv.URL).MarkPullRequestReadyForReview(context.Background(), 7, "acme", "app"); err != nil {
		t.Fatalf("MarkPullRequestReadyForReview() error = %v", err)
	}
	if mutations.Load() != 0 {
		t.Errorf("mutations = %d, want none for a pull request that is not a draft", mutations.Load())
	}
}

func TestMarkPullRequestReadyForReview_StillDraft(t *testing.T) {
	t.Parallel()

	var mutations atomic.Int64
	srv := draftServer(t, true, `{"data":{"markPullRequestReadyForReview":{"pullRequest":{"isDraft":true}}}}`, &mutations)
	defer srv.Close()
	err := newTestSCMAdapter(t, srv.URL).MarkPullRequestReadyForReview(context.Background(), 7, "acme", "app")
	assertSCMErrorKind(t, err, domain.ErrSCMPayload)
}

func TestMarkPullRequestReadyForReview_Refused(t *testing.T) {
	t.Parallel()

	var mutations atomic.Int64
	srv := draftServer(t, true, `{"errors":[{"message":"Resource not accessible by integration"}]}`, &mutations)
	defer srv.Close()
	err := newTestSCMAdapter(t, srv.URL).MarkPullRequestReadyForReview(context.Background(), 7, "acme", "app")
	assertSCMErrorKind(t, err, domain.ErrSCMAPI)
}
