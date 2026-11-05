package orchestrator

import (
	"context"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/dataplane"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/pricing"
)

// turnAdapter returns res from every turn, as an agent whose runtime
// reports no usage of its own.
type turnAdapter struct {
	domain.AgentAdapter
	res domain.TurnResult
}

func (a turnAdapter) RunTurn(context.Context, domain.Session, domain.RunTurnParams) (domain.TurnResult, error) {
	return a.res, nil
}

// remoteTurns runs n turns of a remote attempt whose agent reports no
// usage, and returns the attempt and the worker's fold of the turns.
func remoteTurns(t *testing.T, n int) (*remoteAttempt, WorkerResult) {
	t.Helper()
	a := &remoteAttempt{}
	ad := &remoteUsageAdapter{
		AgentAdapter: turnAdapter{res: domain.TurnResult{SessionID: "ses", SpendUnaccounted: true}},
		attempt:      a,
	}
	r := WorkerResult{IssueID: "ISS-1", Identifier: "ISS-1-ident", ExitKind: WorkerExitNormal, AgentAdapter: "agent-client-protocol"}
	for range n {
		res, err := ad.RunTurn(context.Background(), domain.Session{ID: "ses"}, domain.RunTurnParams{})
		if err != nil {
			t.Fatalf("RunTurn: %v", err)
		}
		if res.SpendUnaccounted {
			r.UnaccountedTurns++
		}
		r.TurnsCompleted++
	}
	return a, r
}

// The worker daemon reports an attempt's usage once, after its agent
// exited and so after the last turn. The model that report names is the
// run's reported model, and its measured totals cover the turns that
// ended waiting for it, so the run prices from the price table.
func TestRemoteUsageAfterTheLastTurnNamesTheModelAndCoversTheTurns(t *testing.T) {
	t.Parallel()

	a, r := remoteTurns(t, 2)
	a.receiveUsage(dataplane.Usage{
		InputTokens: 102, OutputTokens: 28_568, CacheReadTokens: 3_349_243, CacheWriteTokens: 86_547,
		Model: "claude-opus-5-5", Measured: true,
	})
	r = a.adjust(r)
	if r.ModelName != "claude-opus-5-5" {
		t.Errorf("ModelName = %q, want the model the usage report named", r.ModelName)
	}
	if !r.UsageMeasured || r.Usage.CacheReadTokens != 3_349_243 {
		t.Errorf("usage %+v measured %v, want the report's", r.Usage, r.UsageMeasured)
	}
	if r.UnaccountedTurns != 0 {
		t.Errorf("UnaccountedTurns = %d, want 0: the measured report covers the turns that waited for it", r.UnaccountedTurns)
	}

	cfg, err := config.NewServiceConfig(map[string]any{})
	if err != nil {
		t.Fatalf("NewServiceConfig: %v", err)
	}
	rates, _, err := pricing.LoadRates(cfg, pricing.LoadInput{IsKind: func(string) bool { return false }})
	if err != nil {
		t.Fatalf("LoadRates: %v", err)
	}
	state := exitState(t, "ISS-1", nil)
	state.Rates = rates
	store := &mockExitStore{}
	HandleWorkerExit(state, r, defaultExitParams(t, store))
	if len(store.runHistories) != 1 {
		t.Fatalf("run histories %d, want 1", len(store.runHistories))
	}
	run := store.runHistories[0]
	if run.ReportedModel != "claude-opus-5-5" {
		t.Errorf("reported_model = %q, want claude-opus-5-5", run.ReportedModel)
	}
	if run.CostStatus != string(pricing.CostEstimated) || run.EstimatedCostUSD == nil || *run.EstimatedCostUSD <= 0 {
		t.Errorf("cost (%v, %q), want an estimated cost", run.EstimatedCostUSD, run.CostStatus)
	}
}

// A report of spend whose amount the worker does not know leaves the
// turns that waited for the worker's figure unaccounted.
func TestRemoteUnknownUsageAfterTheLastTurnLeavesTheTurnsUnaccounted(t *testing.T) {
	t.Parallel()

	a, r := remoteTurns(t, 1)
	a.receiveUsage(dataplane.Usage{Measured: false})
	if r = a.adjust(r); r.UnaccountedTurns != 2 || r.UsageMeasured {
		t.Errorf("unaccounted %d measured %v, want 2 and false", r.UnaccountedTurns, r.UsageMeasured)
	}
}

// A measured report during a turn covers the earlier turns that ended
// waiting for the worker's figure: the worker's totals are the attempt's.
func TestRemoteUsageDuringATurnCoversTheTurnsBefore(t *testing.T) {
	t.Parallel()

	a := &remoteAttempt{}
	ad := &remoteUsageAdapter{
		AgentAdapter: turnAdapter{res: domain.TurnResult{SessionID: "ses", SpendUnaccounted: true}},
		attempt:      a,
	}
	first, _ := ad.RunTurn(context.Background(), domain.Session{ID: "ses"}, domain.RunTurnParams{})
	if !first.SpendUnaccounted || !first.SpendCoverable {
		t.Errorf("turn without a report: unaccounted %v coverable %v, want both", first.SpendUnaccounted, first.SpendCoverable)
	}
	a.receiveUsage(dataplane.Usage{InputTokens: 10, OutputTokens: 5, Model: "claude-opus-5-5", Measured: true})
	var models []string
	second, _ := ad.RunTurn(context.Background(), domain.Session{ID: "ses"}, domain.RunTurnParams{
		OnEvent: func(e domain.AgentEvent) { models = append(models, e.Model) },
	})
	if second.SpendUnaccounted || !second.CoversSession {
		t.Errorf("turn with a measured report: unaccounted %v covers %v, want false and true", second.SpendUnaccounted, second.CoversSession)
	}
	if len(models) != 1 || models[0] != "claude-opus-5-5" {
		t.Errorf("token usage events' models %q, want the report's", models)
	}
	if r := a.adjust(WorkerResult{}); r.UnaccountedTurns != 0 {
		t.Errorf("UnaccountedTurns after adjust = %d, want 0 with no late report", r.UnaccountedTurns)
	}
}
