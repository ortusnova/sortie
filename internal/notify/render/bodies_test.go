package render

import (
	"strings"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
)

func TestStartedBody_IsTheFixedHeadline(t *testing.T) {
	t.Parallel()

	const want = "Sortie session started."
	if startedBody != want {
		t.Errorf("startedBody = %q, want %q", startedBody, want)
	}
	for _, forbidden := range []string{"Session:", "Workspace:", "Attempt:", "Agent:"} {
		if strings.Contains(startedBody, forbidden) {
			t.Errorf("startedBody = %q, must not contain %q", startedBody, forbidden)
		}
	}
}

func TestCompletionBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		facts domain.SessionCompletedFacts
		want  string
	}{
		{
			name:  "completed no retry",
			facts: domain.SessionCompletedFacts{Elapsed: 90 * time.Second, Turns: 5},
			want:  "Sortie session completed.\nDuration: 1m30s\nTurns: 5",
		},
		{
			name:  "completed with re-queuing",
			facts: domain.SessionCompletedFacts{Elapsed: 90 * time.Second, Turns: 3, RetryScheduled: true},
			want:  "Sortie session completed (re-queuing).\nDuration: 1m30s\nTurns: 3",
		},
		{
			name:  "sub-second elapsed truncated to zero",
			facts: domain.SessionCompletedFacts{Elapsed: 500 * time.Millisecond},
			want:  "Sortie session completed.\nDuration: 0s\nTurns: 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := completionBody(tt.facts)

			if got != tt.want {
				t.Errorf("completionBody(%+v) = %q, want %q", tt.facts, got, tt.want)
			}
		})
	}
}

func TestCompletionBody_NeverCarriesASessionIdentifier(t *testing.T) {
	t.Parallel()

	got := completionBody(domain.SessionCompletedFacts{Elapsed: 90 * time.Second, Turns: 5})

	if strings.Contains(got, "Session") {
		t.Errorf("completionBody() = %q, want no session-identifier line", got)
	}
}

func TestFailureBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		facts domain.SessionFailedFacts
		want  string
	}{
		{
			name:  "failure with retry scheduled",
			facts: domain.SessionFailedFacts{Elapsed: 45 * time.Second, RetryScheduled: true, NextAttempt: 2},
			want:  "Sortie session failed.\nDuration: 45s\nRetry: yes (attempt 2)",
		},
		{
			name:  "failure no retry",
			facts: domain.SessionFailedFacts{Elapsed: 30 * time.Second},
			want:  "Sortie session failed.\nDuration: 30s\nRetry: no (not retryable)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := failureBody(tt.facts)

			if got != tt.want {
				t.Errorf("failureBody(%+v) = %q, want %q", tt.facts, got, tt.want)
			}
		})
	}
}

func TestFailureBody_NeverCarriesErrorTextOrSessionIdentifier(t *testing.T) {
	t.Parallel()

	got := failureBody(domain.SessionFailedFacts{Elapsed: 45 * time.Second, RetryScheduled: true, NextAttempt: 2})

	for _, forbidden := range []string{"Error:", "Session", "process killed", "unknown error"} {
		if strings.Contains(got, forbidden) {
			t.Errorf("failureBody() = %q, must not contain %q", got, forbidden)
		}
	}
}

func TestSoftStopBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		facts domain.SessionStoppedFacts
		want  string
	}{
		{
			name:  "blocked reason",
			facts: domain.SessionStoppedFacts{Elapsed: 60 * time.Second, Turns: 3, Reason: "blocked"},
			want:  "Sortie session completed (agent signaled: blocked).\nDuration: 1m0s\nTurns: 3",
		},
		{
			name:  "needs-human-review reason",
			facts: domain.SessionStoppedFacts{Elapsed: 90 * time.Second, Turns: 5, Reason: "needs-human-review"},
			want:  "Sortie session completed (agent signaled: needs-human-review).\nDuration: 1m30s\nTurns: 5",
		},
		{
			name:  "sub-second elapsed truncated",
			facts: domain.SessionStoppedFacts{Elapsed: 500 * time.Millisecond, Reason: "blocked"},
			want:  "Sortie session completed (agent signaled: blocked).\nDuration: 0s\nTurns: 0",
		},
		{
			name:  "not re-queuing",
			facts: domain.SessionStoppedFacts{Elapsed: 30 * time.Second, Turns: 2, Reason: "blocked"},
			want:  "Sortie session completed (agent signaled: blocked).\nDuration: 30s\nTurns: 2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := softStopBody(tt.facts)

			if got != tt.want {
				t.Errorf("softStopBody(%+v) = %q, want %q", tt.facts, got, tt.want)
			}
		})
	}
}

func TestSoftStopBody_NeverCarriesASessionIdentifier(t *testing.T) {
	t.Parallel()

	got := softStopBody(domain.SessionStoppedFacts{Elapsed: 60 * time.Second, Turns: 3, Reason: "blocked"})

	if strings.Contains(got, "Session") {
		t.Errorf("softStopBody() = %q, want no session-identifier line", got)
	}
}

func TestCIEscalationBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		facts          domain.CIFailureFacts
		wantContains   []string
		wantNotContain []string
	}{
		{
			name: "basic format with failing count and details URL",
			facts: domain.CIFailureFacts{
				Ref:          "abc1234",
				Attempts:     3,
				FailingCount: 2,
				Checks: []domain.CICheckFacts{
					{Name: "test", Conclusion: string(domain.CheckConclusionFailure), DetailsURL: "https://ci.example.com/runs/42", Failing: true},
				},
			},
			wantContains: []string{
				"CI fix retries exhausted",
				"abc1234",
				"3 CI-fix continuation(s)",
				"Failing checks:",
				"2",
				"test",
				"https://ci.example.com/runs/42",
				"Manual intervention required",
			},
		},
		{
			name:  "zero failing count omits count line",
			facts: domain.CIFailureFacts{Ref: "main", Attempts: 1},
			wantContains: []string{
				"CI fix retries exhausted",
				"main",
				"1 CI-fix continuation(s)",
				"Manual intervention required",
			},
			wantNotContain: []string{"Failing checks:"},
		},
		{
			name: "only failing check runs included",
			facts: domain.CIFailureFacts{
				Ref:          "feature/x",
				Attempts:     2,
				FailingCount: 2,
				Checks: []domain.CICheckFacts{
					{Name: "lint", Conclusion: string(domain.CheckConclusionSuccess)},
					{Name: "test", Conclusion: string(domain.CheckConclusionFailure), Failing: true},
					{Name: "deploy", Conclusion: string(domain.CheckConclusionTimedOut), Failing: true},
					{Name: "e2e", Conclusion: string(domain.CheckConclusionCancelled)},
				},
			},
			wantContains:   []string{"test", "deploy"},
			wantNotContain: []string{"lint", "e2e"},
		},
		{
			name: "check run without details URL omits link",
			facts: domain.CIFailureFacts{
				Ref:          "sha9999",
				Attempts:     1,
				FailingCount: 1,
				Checks:       []domain.CICheckFacts{{Name: "build", Conclusion: string(domain.CheckConclusionFailure), Failing: true}},
			},
			wantContains:   []string{"build"},
			wantNotContain: []string{"details"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := ciEscalationBody(tt.facts)

			for _, want := range tt.wantContains {
				if !strings.Contains(got, want) {
					t.Errorf("ciEscalationBody(%+v) missing %q\ngot: %s", tt.facts, want, got)
				}
			}
			for _, notWant := range tt.wantNotContain {
				if strings.Contains(got, notWant) {
					t.Errorf("ciEscalationBody(%+v) unexpectedly contains %q\ngot: %s", tt.facts, notWant, got)
				}
			}
		})
	}
}

func TestAutoMergedBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sha  string
		want string
	}{
		{
			name: "merge commit line follows the strategy",
			sha:  "abc123def",
			want: "Sortie auto-merged PR #99 (myorg/myrepo).\nStrategy: squash.\nMerge commit: abc123def.",
		},
		{
			name: "empty merge commit omits the line",
			want: "Sortie auto-merged PR #99 (myorg/myrepo).\nStrategy: squash.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			facts := domain.AutoMergedFacts{Owner: "myorg", Repo: "myrepo", PRNumber: 99, Strategy: domain.StrategySquash, SHA: tt.sha}

			got := autoMergedBody(facts)

			if got != tt.want {
				t.Errorf("autoMergedBody(sha %q) = %q, want %q", tt.sha, got, tt.want)
			}
		})
	}
}

func TestMergeConflictEscalationBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		facts domain.MergeConflictFacts
		want  string
	}{
		{
			name:  "one dispatched turn at max_retries 1",
			facts: domain.MergeConflictFacts{PRNumber: 42, Attempts: 2},
			want:  "Sortie attempted 1 merge-conflict resolution turn(s) and could not clear the conflicts for PR #42. Manual rebase required.",
		},
		{
			name:  "zero dispatched turns at max_retries 0 (escalates without a rebase)",
			facts: domain.MergeConflictFacts{PRNumber: 7, Attempts: 1},
			want:  "Sortie attempted 0 merge-conflict resolution turn(s) and could not clear the conflicts for PR #7. Manual rebase required.",
		},
		{
			name:  "three dispatched turns at max_retries 3",
			facts: domain.MergeConflictFacts{PRNumber: 99, Attempts: 4},
			want:  "Sortie attempted 3 merge-conflict resolution turn(s) and could not clear the conflicts for PR #99. Manual rebase required.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := mergeConflictEscalationBody(tt.facts)

			if got != tt.want {
				t.Errorf("mergeConflictEscalationBody(%+v) =\n  %q\nwant:\n  %q", tt.facts, got, tt.want)
			}
		})
	}
}

func budgetLines(lines ...string) string {
	return strings.Join(lines, "\n")
}

func TestBudgetHeldBody(t *testing.T) {
	t.Parallel()

	recordedAt := time.Date(2026, 8, 25, 9, 14, 3, 0, time.UTC)
	usedTokens := int64(128400)
	zeroTokens := int64(0)
	zeroStopped := 0
	oneStopped := 1
	twoStopped := 2
	usedUSD, noSpendUSD := 5.12, 0.0
	complete, incomplete := true, false

	const header = "Sortie stopped dispatching this issue.\nRecorded: 2026-08-25T09:14:03Z"

	tests := []struct {
		name     string
		facts    domain.BudgetHeldFacts
		want     string
		wantLine string
	}{
		{
			name: "session-only hold",
			facts: domain.BudgetHeldFacts{
				Reason: domain.BudgetHoldReasonSession, CeilingSetting: "agent.max_sessions",
				UsedSessions: 4, BudgetSessions: 3, TokensComplete: true, ExhaustedAt: recordedAt,
			},
			want: budgetLines(header,
				"Ceiling: session budget (agent.max_sessions)",
				"Sessions: 4 of 3",
				"Raising agent.max_sessions raises this ceiling."),
		},
		{
			name: "hold exhausted on both axes places Tokens before Sessions",
			facts: domain.BudgetHeldFacts{
				Reason: domain.BudgetHoldReasonToken, CeilingSetting: "agent.max_tokens",
				UsedSessions: 5, BudgetSessions: 3, UsedTokens: &usedTokens, BudgetTokens: 100000,
				TokensComplete: true, ExhaustedAt: recordedAt,
			},
			want: budgetLines(header,
				"Ceiling: token budget (agent.max_tokens)",
				"Tokens: 128400 of 100000",
				"Sessions: 5 of 3",
				"Raising agent.max_tokens raises this ceiling."),
		},
		{
			name: "no usage reported prints the Tokens line as not measured",
			facts: domain.BudgetHeldFacts{
				Reason: domain.BudgetHoldReasonSession, UsedTokens: &zeroTokens, BudgetTokens: 500000, ExhaustedAt: recordedAt,
			},
			wantLine: "Tokens: not measured of 500000",
		},
		{
			name: "measured zero prints the Tokens line as 0",
			facts: domain.BudgetHeldFacts{
				Reason: domain.BudgetHoldReasonSession, UsedTokens: &zeroTokens, BudgetTokens: 500000,
				TokensComplete: true, ExhaustedAt: recordedAt,
			},
			wantLine: "Tokens: 0 of 500000",
		},
		{
			name: "reason without ceiling words omits the Ceiling and closing lines",
			facts: domain.BudgetHeldFacts{
				Reason: "some_future_reason", UsedSessions: 4, BudgetSessions: 3, ExhaustedAt: recordedAt,
			},
			want: budgetLines(header, "Sessions: 4 of 3"),
		},
		{
			name: "one stopped-in-flight session renders the line in singular form",
			facts: domain.BudgetHeldFacts{
				Reason: domain.BudgetHoldReasonToken, CeilingSetting: "agent.max_tokens",
				UsedSessions: 5, BudgetSessions: 3, UsedTokens: &usedTokens, BudgetTokens: 100000,
				TokensComplete: true, StoppedInFlight: &oneStopped, ExhaustedAt: recordedAt,
			},
			want: budgetLines(header,
				"Ceiling: token budget (agent.max_tokens)",
				"Tokens: 128400 of 100000",
				"Sessions: 5 of 3",
				"Stopped in flight: 1 session",
				"Raising agent.max_tokens raises this ceiling."),
		},
		{
			name: "two stopped-in-flight sessions render the line in plural form",
			facts: domain.BudgetHeldFacts{
				Reason: domain.BudgetHoldReasonToken, CeilingSetting: "agent.max_tokens",
				UsedSessions: 5, BudgetSessions: 3, UsedTokens: &usedTokens, BudgetTokens: 100000,
				TokensComplete: true, StoppedInFlight: &twoStopped, ExhaustedAt: recordedAt,
			},
			want: budgetLines(header,
				"Ceiling: token budget (agent.max_tokens)",
				"Tokens: 128400 of 100000",
				"Sessions: 5 of 3",
				"Stopped in flight: 2 sessions",
				"Raising agent.max_tokens raises this ceiling."),
		},
		{
			name: "zero stopped-in-flight renders a body byte-identical to the no-stops golden",
			facts: domain.BudgetHeldFacts{
				Reason: domain.BudgetHoldReasonToken, CeilingSetting: "agent.max_tokens",
				UsedSessions: 5, BudgetSessions: 3, UsedTokens: &usedTokens, BudgetTokens: 100000,
				TokensComplete: true, StoppedInFlight: &zeroStopped, ExhaustedAt: recordedAt,
			},
			want: budgetLines(header,
				"Ceiling: token budget (agent.max_tokens)",
				"Tokens: 128400 of 100000",
				"Sessions: 5 of 3",
				"Raising agent.max_tokens raises this ceiling."),
		},
		{
			name: "unset session ceiling omits the Sessions line and nil StoppedInFlight omits the Stopped in flight line",
			facts: domain.BudgetHeldFacts{
				Reason: domain.BudgetHoldReasonToken, CeilingSetting: "agent.max_tokens",
				UsedSessions: 5, UsedTokens: &usedTokens, BudgetTokens: 100000,
				TokensComplete: true, ExhaustedAt: recordedAt,
			},
			want: budgetLines(header,
				"Ceiling: token budget (agent.max_tokens)",
				"Tokens: 128400 of 100000",
				"Raising agent.max_tokens raises this ceiling."),
		},
		{
			name: "session-reason hold with stopped-in-flight sessions renders the line too: the gate is data availability, not the firing reason",
			facts: domain.BudgetHeldFacts{
				Reason: domain.BudgetHoldReasonSession, CeilingSetting: "agent.max_sessions",
				UsedSessions: 4, BudgetSessions: 3, TokensComplete: true, StoppedInFlight: &oneStopped, ExhaustedAt: recordedAt,
			},
			want: budgetLines(header,
				"Ceiling: session budget (agent.max_sessions)",
				"Sessions: 4 of 3",
				"Stopped in flight: 1 session",
				"Raising agent.max_sessions raises this ceiling."),
		},
		{
			name: "money hold with a complete figure places Estimated cost between Ceiling and Tokens",
			facts: domain.BudgetHeldFacts{
				Reason: domain.BudgetHoldReasonMoney, CeilingSetting: "agent.max_cost_usd",
				UsedSessions: 5, BudgetSessions: 3, UsedTokens: &usedTokens, BudgetTokens: 100000,
				TokensComplete: true, UsedUSD: &usedUSD, UsedUSDComplete: &complete, BudgetUSD: 5, ExhaustedAt: recordedAt,
			},
			want: budgetLines(header,
				"Ceiling: money budget (agent.max_cost_usd)",
				"Estimated cost: $5.12 of $5.00",
				"Tokens: 128400 of 100000",
				"Sessions: 5 of 3",
				"Raising agent.max_cost_usd raises this ceiling."),
		},
		{
			name: "an incomplete figure at zero prints the Estimated cost line as unknown",
			facts: domain.BudgetHeldFacts{
				Reason: domain.BudgetHoldReasonSession, UsedUSD: &noSpendUSD, UsedUSDComplete: &incomplete, BudgetUSD: 5, ExhaustedAt: recordedAt,
			},
			wantLine: "Estimated cost: unknown of $5.00",
		},
		{
			name: "an incomplete figure above zero prints the Estimated cost line as a lower bound",
			facts: domain.BudgetHeldFacts{
				Reason: domain.BudgetHoldReasonSession, UsedUSD: &usedUSD, UsedUSDComplete: &incomplete, BudgetUSD: 5, ExhaustedAt: recordedAt,
			},
			wantLine: "Estimated cost: at least $5.12 of $5.00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := budgetHeldBody(tt.facts)

			if tt.wantLine != "" {
				if !strings.Contains(got, tt.wantLine) {
					t.Errorf("budgetHeldBody(%+v) =\n%q\nwant a line %q", tt.facts, got, tt.wantLine)
				}
				return
			}
			if got != tt.want {
				t.Errorf("budgetHeldBody(%+v) =\n%q\nwant\n%q", tt.facts, got, tt.want)
			}
		})
	}
}

func TestSessionEscalatedBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		facts domain.SessionEscalatedFacts
		want  string
	}{
		{name: "question on a rule with a viewer", facts: domain.SessionEscalatedFacts{Kind: "question", Rule: "implement", URL: "https://viewer.example/s/1"}, want: "The running session on rule implement asks for a person (question).\nSession: https://viewer.example/s/1"},
		{name: "no rule", facts: domain.SessionEscalatedFacts{Kind: "other"}, want: "The running session asks for a person (other)."},
		{name: "failed criteria check", facts: domain.SessionEscalatedFacts{Kind: domain.SessionEscalationCriteriaFailed, Rule: "criteria"}, want: "The acceptance-criteria check of rule criteria did not pass."},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := sessionEscalatedBody(tt.facts); got != tt.want {
				t.Errorf("sessionEscalatedBody(%+v) = %q, want %q", tt.facts, got, tt.want)
			}
		})
	}
}

func TestHoldExpiredBody(t *testing.T) {
	t.Parallel()

	until := time.Date(2026, 11, 3, 14, 0, 0, 0, time.UTC)
	got := holdExpiredBody(domain.HoldExpiredFacts{
		By:        "supervisor",
		Reason:    "full vm chain proof running",
		Evidence:  "sandbox hl-ons-54-proof-1 on taichi",
		RenewedAt: until.Add(-time.Hour),
		Until:     until,
	})
	want := "The hold by supervisor expired at 2026-11-03T14:00:00Z without a renewal. The issue waits for a person.\n" +
		"Reason: full vm chain proof running\n" +
		"Last evidence: sandbox hl-ons-54-proof-1 on taichi\n" +
		"Last renewed: 2026-11-03T13:00:00Z"
	if got != want {
		t.Errorf("holdExpiredBody() = %q, want %q", got, want)
	}
	if got := holdExpiredBody(domain.HoldExpiredFacts{Until: until}); got != "The hold by its holder expired at 2026-11-03T14:00:00Z without a renewal. The issue waits for a person." {
		t.Errorf("holdExpiredBody(no by) = %q", got)
	}
}

func TestLoopSuspectedBody(t *testing.T) {
	t.Parallel()

	since := time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)
	got := loopSuspectedBody(domain.SupervisorLoopSuspectedFacts{TriggerKind: "idle_slot", Digest: "4f2a9c01d3e5b778", Sessions: 3, Since: since})
	want := "3 consecutive idle_slot supervisor sessions took no action on state digest 4f2a9c01d3e5b778. " +
		"The idle_slot trigger starts no session until the state digest changes.\n" +
		"First of them: 2026-11-04T09:00:00Z"
	if got != want {
		t.Errorf("loopSuspectedBody() = %q, want %q", got, want)
	}
}

func TestBuiltin_TitleWithoutAnIssue(t *testing.T) {
	t.Parallel()

	n := domain.Notification{
		Envelope: domain.NotificationEnvelope{EventType: domain.EventSupervisorLoopSuspected},
		Message:  domain.NotificationMessage{Severity: "warning", Facts: domain.SupervisorLoopSuspectedFacts{TriggerKind: "sweep", Digest: "d", Sessions: 3}},
	}
	if got := builtin(n).Title; got != "supervisor.loop_suspected" {
		t.Errorf("title = %q, want the event type alone", got)
	}
}

func TestLoopSuspectedBody_Velocity(t *testing.T) {
	t.Parallel()

	got := loopSuspectedBody(domain.SupervisorLoopSuspectedFacts{HourlyTokens: 300000, HourlyLimit: 250000, Pace: 83333})
	want := "The supervisor spent 300000 tokens in the last hour, over its limit of 250000. " +
		"Only sweeps start a supervisor session until the hourly spend is back under the even pace of 83333 tokens an hour."
	if got != want {
		t.Errorf("loopSuspectedBody(velocity) = %q, want %q", got, want)
	}
}

func TestSupervisorAvailabilityBodies(t *testing.T) {
	t.Parallel()

	since := time.Date(2026, 11, 4, 6, 8, 0, 0, time.UTC)
	got := supervisorUnavailableBody(domain.SupervisorUnavailableFacts{
		Reason: domain.SupervisorUnavailableBudget, Detail: "the supervisor spent 3000000 of 3000000 tokens today",
		Since: since, ResetsAt: time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC),
	})
	want := "The supervisor cannot start the sessions it would start: its daily budget is spent.\n" +
		"Reason: budget_exhausted\n" +
		"Detail: the supervisor spent 3000000 of 3000000 tokens today\n" +
		"Since: 2026-11-04T06:08:00Z\n" +
		"Resets at: 2026-11-05T00:00:00Z"
	if got != want {
		t.Errorf("supervisorUnavailableBody() = %q, want %q", got, want)
	}
	if got := supervisorUnavailableBody(domain.SupervisorUnavailableFacts{Reason: domain.SupervisorUnavailablePaused}); got !=
		"The supervisor cannot start the sessions it would start: an operator paused it.\nReason: paused" {
		t.Errorf("supervisorUnavailableBody(paused) = %q", got)
	}
	if got := supervisorAvailableBody(domain.SupervisorAvailableFacts{Reason: domain.SupervisorUnavailablePaused, Since: since}); got !=
		"The supervisor can start sessions again. It could not (paused) since 2026-11-04T06:08:00Z." {
		t.Errorf("supervisorAvailableBody() = %q", got)
	}
	n := domain.Notification{
		Envelope: domain.NotificationEnvelope{EventType: domain.EventSupervisorAvailable},
		Message:  domain.NotificationMessage{Severity: "info", Facts: domain.SupervisorAvailableFacts{}},
	}
	if got := builtin(n); got.Title != "supervisor.available" || got.Body != "The supervisor can start sessions again." {
		t.Errorf("builtin(supervisor.available) = %+v", got)
	}
}
