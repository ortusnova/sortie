package orchestrator

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/redact"
)

type observedCall func(ctx context.Context, scm domain.SCMAdapter, ci domain.CIStatusProvider) error

type scriptedProvider struct {
	stubSCMForRecovery

	err   error
	calls atomic.Int32
}

func (p *scriptedProvider) FetchPendingReviews(context.Context, int, string, string) ([]domain.ReviewComment, error) {
	p.calls.Add(1)
	return nil, p.err
}

func (p *scriptedProvider) GetMergeability(context.Context, int, string, string) (domain.PRMergeStatus, error) {
	p.calls.Add(1)
	return domain.PRMergeStatus{}, p.err
}

func (p *scriptedProvider) MergePR(context.Context, int, string, string, domain.MergeStrategy, string, string, string) (domain.MergeResult, error) {
	p.calls.Add(1)
	return domain.MergeResult{}, p.err
}

func (p *scriptedProvider) DeleteBranch(context.Context, string, string, string) error {
	p.calls.Add(1)
	return p.err
}

func (p *scriptedProvider) FetchCIStatus(context.Context, string) (domain.CIResult, error) {
	p.calls.Add(1)
	return domain.CIResult{}, p.err
}

type verifyingProvider struct {
	*scriptedProvider
}

func (p verifyingProvider) VerifyAutoMergeScopes(context.Context, bool) ([]string, []string, error) {
	return nil, nil, p.err
}

func tickWithin(o *Orchestrator) (from, to time.Time) {
	from = time.Now()
	o.handleTick(context.Background())
	return from, time.Now()
}

func requireWithin(t *testing.T, name string, got, from, to time.Time) {
	t.Helper()
	if got.Before(from) || got.After(to) {
		t.Errorf("%s = %v, want within [%v, %v]", name, got, from, to)
	}
}

func outcomeOf(check SnapshotHealthCheck) string {
	switch {
	case check.CheckedAt.IsZero():
		return "untouched"
	case check.ConsecutiveFailures > 0:
		return "fail"
	default:
		return "pass"
	}
}

func TestHealthMasksTrackerFailureAcrossConsecutiveFailures(t *testing.T) {
	t.Parallel()

	const secret = "tkn-health-poll-0123456789"
	redact.Add("test.health tracker failure", secret)

	var reads atomic.Int32
	tracker := &candidateTrackerAdapter{
		mockTrackerAdapter: &mockTrackerAdapter{},
		fetchCandidatesFn: func(context.Context) ([]domain.Issue, error) {
			if reads.Add(1) == 1 {
				return nil, nil
			}
			return nil, &domain.TrackerError{Kind: domain.ErrTrackerTransport, Message: "request failed with token " + secret}
		},
	}
	wm := &stubWorkflowManager{config: lifecycleConfig(t.TempDir())}
	regs := passingPreflightRegistries()
	o := NewOrchestrator(OrchestratorParams{
		State:           NewState(60000, 10, 0, nil, AgentTotals{}),
		Logger:          discardLogger(),
		TrackerAdapter:  tracker,
		AgentAdapter:    &mockAgentAdapter{},
		WorkflowManager: wm,
		Store:           &stubStore{},
		PreflightParams: PreflightParams{
			ReloadWorkflow:  func() error { return nil },
			ConfigFunc:      wm.Config,
			TrackerRegistry: regs.TrackerRegistry,
			AgentRegistry:   regs.AgentRegistry,
		},
	})

	okFrom, okTo := tickWithin(o)
	failFrom, failTo := tickWithin(o)
	tickWithin(o)

	poll := o.Health().Poll
	if poll.ConsecutiveFailures != 2 {
		t.Errorf("Health().Poll.ConsecutiveFailures = %d, want 2", poll.ConsecutiveFailures)
	}
	requireWithin(t, "Health().Poll.LastSuccessAt", poll.LastSuccessAt, okFrom, okTo)
	requireWithin(t, "Health().Poll.FailingSince", poll.FailingSince, failFrom, failTo)
	if poll.Outcome != "tracker_failed" {
		t.Errorf("Health().Poll.Outcome = %q, want %q", poll.Outcome, "tracker_failed")
	}
	if !strings.Contains(poll.Error, redact.Marker) || strings.Contains(poll.Error, secret) {
		t.Errorf("Health().Poll.Error = %q, want the tracker error with the registered value masked", poll.Error)
	}
}

func TestHealthReportsWorkflowLoadFailedFromLatestPreflightRun(t *testing.T) {
	t.Parallel()

	failing := func(checks ...string) PreflightResult {
		var result PreflightResult
		for _, check := range checks {
			result.Errors = append(result.Errors, PreflightError{Check: check, Message: check + " failed"})
		}
		return result
	}

	tests := []struct {
		name     string
		runs     []PreflightResult
		wantLoad bool
		wantFail int
	}{
		{name: "failing on the workflow load", runs: []PreflightResult{failing("workflow_load")}, wantLoad: true, wantFail: 1},
		{name: "failing on another check", runs: []PreflightResult{failing("tracker.kind")}, wantFail: 1},
		{name: "failing on the workflow load and another check", runs: []PreflightResult{failing("tracker.kind", "workflow_load")}, wantLoad: true, wantFail: 1},
		{name: "passing run after a load failure", runs: []PreflightResult{failing("workflow_load"), {}}},
		{name: "other failure after a load failure", runs: []PreflightResult{failing("workflow_load"), failing("tracker.kind")}, wantFail: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			wm := &stubWorkflowManager{config: lifecycleConfig(t.TempDir())}
			o := NewOrchestrator(OrchestratorParams{
				State:           NewState(60000, 5, 0, nil, AgentTotals{}),
				Logger:          discardLogger(),
				TrackerAdapter:  &mockTrackerAdapter{},
				AgentAdapter:    &mockAgentAdapter{},
				WorkflowManager: wm,
				Store:           &stubStore{},
			})

			for _, run := range tt.runs {
				o.recordPreflight(run)
			}

			got := o.Health().Preflight
			if got.WorkflowLoadFailed != tt.wantLoad || got.ConsecutiveFailures != tt.wantFail {
				t.Errorf("Health().Preflight = {WorkflowLoadFailed: %t, ConsecutiveFailures: %d}, want {%t, %d}",
					got.WorkflowLoadFailed, got.ConsecutiveFailures, tt.wantLoad, tt.wantFail)
			}
		})
	}
}

func TestObserveReadClassification(t *testing.T) {
	t.Parallel()

	fetchReviews := func(ctx context.Context, scm domain.SCMAdapter, _ domain.CIStatusProvider) error {
		_, err := scm.FetchPendingReviews(ctx, 1, "owner", "repo")
		return err
	}
	getMergeability := func(ctx context.Context, scm domain.SCMAdapter, _ domain.CIStatusProvider) error {
		_, err := scm.GetMergeability(ctx, 1, "owner", "repo")
		return err
	}
	fetchCIStatus := func(ctx context.Context, _ domain.SCMAdapter, ci domain.CIStatusProvider) error {
		_, err := ci.FetchCIStatus(ctx, "main")
		return err
	}
	fetchRepositoryCIStatus := func(ctx context.Context, _ domain.SCMAdapter, ci domain.CIStatusProvider) error {
		_, err := domain.FetchPullRequestCIStatus(ctx, ci, "owner", "repo", "main")
		return err
	}
	deleteBranch := func(ctx context.Context, scm domain.SCMAdapter, _ domain.CIStatusProvider) error {
		return scm.DeleteBranch(ctx, "owner", "repo", "feature")
	}
	mergePR := func(ctx context.Context, scm domain.SCMAdapter, _ domain.CIStatusProvider) error {
		_, err := scm.MergePR(ctx, 1, "owner", "repo", "", "", "", "")
		return err
	}

	tests := []struct {
		name        string
		call        observedCall
		err         error
		canceledCtx bool
		wantOutcome string
	}{
		{name: "read returning nil passes", call: fetchReviews, wantOutcome: "pass"},
		{name: "not found from a read fails", call: getMergeability, err: &domain.SCMError{Kind: domain.ErrSCMNotFound, Message: "no such pull request"}, wantOutcome: "fail"},
		{name: "transport error from a read fails", call: fetchReviews, err: &domain.SCMError{Kind: domain.ErrSCMTransport, Message: "connection refused"}, wantOutcome: "fail"},
		{name: "CI not found fails", call: fetchCIStatus, err: &domain.CIError{Kind: domain.ErrCINotFound, Message: "no such ref"}, wantOutcome: "fail"},
		{name: "CI not found in another repository fails", call: fetchRepositoryCIStatus, err: &domain.CIError{Kind: domain.ErrCINotFound, Message: "no such ref"}, wantOutcome: "fail"},
		{name: "cancellation with a live context fails", call: fetchReviews, err: context.Canceled, wantOutcome: "fail"},
		{name: "read canceled by shutdown is not recorded", call: getMergeability, err: context.Canceled, canceledCtx: true, wantOutcome: "untouched"},
		{name: "refused branch delete is not recorded", call: deleteBranch, err: &domain.SCMError{Kind: domain.ErrSCMPayload, Message: "reference does not exist"}, wantOutcome: "untouched"},
		{name: "refused merge is not recorded", call: mergePR, err: &domain.SCMError{Kind: domain.ErrSCMPayload, Message: "merge failed"}, wantOutcome: "untouched"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tt.canceledCtx {
				cancel()
			}
			rec := &healthRecord{}
			provider := &scriptedProvider{err: tt.err}

			gotErr := tt.call(ctx, observeSCM(provider, rec), observeCI(provider, rec))

			check := rec.snapshot()
			if got := outcomeOf(check); got != tt.wantOutcome {
				t.Errorf("record after %q = %q, want %q", tt.name, got, tt.wantOutcome)
			}
			if tt.wantOutcome == "fail" && check.Error != tt.err.Error() {
				t.Errorf("record Error = %q, want %q", check.Error, tt.err.Error())
			}
			if !errors.Is(gotErr, tt.err) {
				t.Errorf("wrapped call error = %v, want %v", gotErr, tt.err)
			}
			if got := provider.calls.Load(); got != 1 {
				t.Errorf("inner calls = %d, want 1", got)
			}
		})
	}
}

func TestObserveSCMForwardsScopeVerifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		inner domain.SCMAdapter
		want  bool
	}{
		{name: "inner adapter that verifies scopes", inner: verifyingProvider{&scriptedProvider{}}, want: true},
		{name: "inner adapter that does not", inner: &scriptedProvider{}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, got := observeSCM(tt.inner, &healthRecord{}).(AutoMergeScopeVerifier)

			if got != tt.want {
				t.Errorf("observeSCM(%s) satisfies AutoMergeScopeVerifier = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}
