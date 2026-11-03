package github

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/sortie-ai/sortie/internal/domain"
)

// draftServer answers the node query with isDraft and the mutation with
// mutated, and counts the mutations.
func draftServer(t *testing.T, isDraft bool, mutated string, mutations *atomic.Int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/graphql" || r.Method != http.MethodPost {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var body graphqlRequestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(body.Query, "mutation") {
			mutations.Add(1)
			if body.Variables["id"] != "PR_node7" {
				t.Errorf("mutation id = %v, want PR_node7", body.Variables["id"])
			}
			_, _ = w.Write([]byte(mutated))
			return
		}
		if body.Variables["number"] != float64(7) || body.Variables["owner"] != "acme" {
			t.Errorf("query variables = %v, want acme/app#7", body.Variables)
		}
		draft := "false"
		if isDraft {
			draft = "true"
		}
		_, _ = w.Write([]byte(`{"data":{"repository":{"pullRequest":{"id":"PR_node7","isDraft":` + draft + `}}}}`))
	}))
}

func TestMarkPullRequestDraft(t *testing.T) {
	t.Parallel()

	var mutations atomic.Int64
	srv := draftServer(t, false, `{"data":{"convertPullRequestToDraft":{"pullRequest":{"isDraft":true}}}}`, &mutations)
	defer srv.Close()
	if err := newTestSCMAdapter(t, srv.URL).MarkPullRequestDraft(context.Background(), 7, "acme", "app"); err != nil {
		t.Fatalf("MarkPullRequestDraft() error = %v", err)
	}
	if mutations.Load() != 1 {
		t.Errorf("mutations = %d, want 1", mutations.Load())
	}
}

func TestMarkPullRequestDraft_AlreadyDraft(t *testing.T) {
	t.Parallel()

	var mutations atomic.Int64
	srv := draftServer(t, true, `{}`, &mutations)
	defer srv.Close()
	if err := newTestSCMAdapter(t, srv.URL).MarkPullRequestDraft(context.Background(), 7, "acme", "app"); err != nil {
		t.Fatalf("MarkPullRequestDraft() error = %v", err)
	}
	if mutations.Load() != 0 {
		t.Errorf("mutations = %d, want none for a draft", mutations.Load())
	}
}

func TestMarkPullRequestDraft_Refused(t *testing.T) {
	t.Parallel()

	var mutations atomic.Int64
	srv := draftServer(t, false, `{"errors":[{"message":"Resource not accessible by integration"}]}`, &mutations)
	defer srv.Close()
	err := newTestSCMAdapter(t, srv.URL).MarkPullRequestDraft(context.Background(), 7, "acme", "app")
	assertSCMErrorKind(t, err, domain.ErrSCMAPI)
}
