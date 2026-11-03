package config

import (
	"strings"
	"testing"
)

// criteriaRules is a valid chain: implement criteria stories, then check
// them. The check hops back to the implement stage on a failure.
func criteriaRules() []any {
	return []any{
		map[string]any{"name": "implement", "match": map[string]any{"labels": []any{"sortie-review:criteria"}}, "agent": "mock", "next": "check"},
		map[string]any{"name": "check", "stage": "stage-check", "agent": "mock", "gate": "criteria", "on_fail": "rework"},
		map[string]any{"name": "rework", "stage": "stage-rework", "agent": "mock"},
	}
}

func buildRules(t *testing.T, rules []any) (DispatchConfig, error) {
	t.Helper()
	return BuildDispatchConfig(map[string]any{"dispatch": map[string]any{"rules": rules}}, mkDispatchDir(t), alwaysRegistered, "")
}

func TestReviewGate_ValidChain(t *testing.T) {
	t.Parallel()
	d, err := buildRules(t, criteriaRules())
	if err != nil {
		t.Fatalf("BuildDispatchConfig() error = %v", err)
	}
	check, _ := d.RuleByName("check")
	if check.Gate != GateCriteria || check.OnFail != "rework" {
		t.Errorf("check = gate %q on_fail %q, want criteria and rework", check.Gate, check.OnFail)
	}
	if err := ValidatePlanReviewGates(d, true); err != nil {
		t.Errorf("ValidatePlanReviewGates() = %v, want nil", err)
	}
}

func TestReviewGate_OnFailDefaultsToEscalate(t *testing.T) {
	t.Parallel()
	rules := criteriaRules()
	delete(rules[1].(map[string]any), "on_fail")
	d, err := buildRules(t, rules)
	if err != nil {
		t.Fatal(err)
	}
	if check, _ := d.RuleByName("check"); check.OnFail != OnFailEscalate {
		t.Errorf("on_fail = %q, want escalate", check.OnFail)
	}
}

func TestReviewGate_Rejections(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		edit      func(rules []any)
		wantField string
		wantMsg   string
	}{
		{"unknown gate", func(r []any) { r[1].(map[string]any)["gate"] = "vibes" }, "dispatch.rules[1].gate", "not criteria"},
		{"on_fail without gate", func(r []any) { r[2].(map[string]any)["on_fail"] = "check" }, "dispatch.rules[2].on_fail", "needs gate"},
		{"on_fail names no rule", func(r []any) { r[1].(map[string]any)["on_fail"] = "nowhere" }, "dispatch.rules[1].on_fail", "names no dispatch rule"},
		{"on_fail names itself", func(r []any) { r[1].(map[string]any)["on_fail"] = "check" }, "dispatch.rules[1].on_fail", "rule that carries it"},
		{"on_fail without stage", func(r []any) { r[1].(map[string]any)["on_fail"] = "implement" }, "dispatch.rules[1].on_fail", "without a stage label"},
		{"gate with next", func(r []any) { r[1].(map[string]any)["next"] = "rework" }, "dispatch.rules[1].next", "last stage"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rules := criteriaRules()
			tt.edit(rules)
			_, err := buildRules(t, rules)
			ce := requireConfigError(t, err)
			if ce.Field != tt.wantField || !strings.Contains(ce.Message, tt.wantMsg) {
				t.Errorf("error = %s: %s, want %s containing %q", ce.Field, ce.Message, tt.wantField, tt.wantMsg)
			}
		})
	}
}

// Under plan control, a rule that takes criteria stories must lead to a
// criteria check. Without plan control the same rules are fine.
func TestReviewGate_ChainWithoutTheGate(t *testing.T) {
	t.Parallel()
	rules := criteriaRules()
	delete(rules[1].(map[string]any), "gate")
	delete(rules[1].(map[string]any), "on_fail")
	d, err := buildRules(t, rules)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidatePlanReviewGates(d, false); err != nil {
		t.Errorf("plan control off: %v, want nil", err)
	}
	err = ValidatePlanReviewGates(d, true)
	ce := requireConfigError(t, err)
	if ce.Field != "dispatch.rules[0]" || !strings.Contains(ce.Message, "ends in no gate") {
		t.Errorf("error = %s: %s", ce.Field, ce.Message)
	}
}

// A rule that takes ui stories must lead to a criteria check too: a ui
// story's criteria review passes before the operator looks at it.
func TestReviewGate_UIStoriesNeedTheCheck(t *testing.T) {
	t.Parallel()
	rules := criteriaRules()
	rules[0].(map[string]any)["match"] = map[string]any{"labels": []any{"sortie-review:ui"}}
	d, err := buildRules(t, rules)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidatePlanReviewGates(d, true); err != nil {
		t.Errorf("ui chain ending in the check: %v, want nil", err)
	}
	delete(rules[0].(map[string]any), "next")
	d, err = buildRules(t, rules)
	if err != nil {
		t.Fatal(err)
	}
	ce := requireConfigError(t, ValidatePlanReviewGates(d, true))
	if ce.Field != "dispatch.rules[0]" || !strings.Contains(ce.Message, "sortie-review:ui") || !strings.Contains(ce.Message, "ends in no gate") {
		t.Errorf("error = %s: %s", ce.Field, ce.Message)
	}
}
