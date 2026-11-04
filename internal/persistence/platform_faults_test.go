package persistence

import (
	"context"
	"reflect"
	"testing"
)

func TestPlatformFaults_SaveLoadAndResolve(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t)
	migrateOrFatal(t, s)

	if open, err := s.LoadOpenPlatformFaults(ctx); err != nil || open == nil || len(open) != 0 {
		t.Fatalf("LoadOpenPlatformFaults() on a new store = %v, %v, want an empty slice", open, err)
	}

	fault := PlatformFault{
		ID:          "pf-1",
		Signature:   "export | git_push | push sortie/<id>: [remote rejected]",
		Class:       "token_scope",
		Step:        "export",
		Cause:       "push sortie/ONS-49: [remote rejected]",
		Detail:      "remote: refusing to allow a Personal Access Token",
		Action:      `Give taichi's GitHub token "Workflows: Read and write"`,
		ActionWhere: "GitHub",
		RecordedAt:  "2026-11-02T10:00:00Z",
		Holds: []PlatformFaultHold{
			{IssueID: "id-49", Identifier: "ONS-49", HeldAt: "2026-11-02T10:00:00Z"},
			{IssueID: "id-50", Identifier: "ONS-50", DisplayID: "ortusnova#50", HeldAt: "2026-11-02T10:01:00Z"},
		},
	}
	other := PlatformFault{ID: "pf-2", Signature: "run | error | x", Class: "unclassified", RecordedAt: "2026-11-02T11:00:00Z", Holds: []PlatformFaultHold{}}
	for _, f := range []PlatformFault{fault, other} {
		if err := s.SavePlatformFault(ctx, f); err != nil {
			t.Fatalf("SavePlatformFault(%s): %v", f.ID, err)
		}
	}

	open, err := s.LoadOpenPlatformFaults(ctx)
	if err != nil || len(open) != 2 {
		t.Fatalf("LoadOpenPlatformFaults() = %d faults, %v, want 2", len(open), err)
	}
	if !reflect.DeepEqual(open[0], fault) || !reflect.DeepEqual(open[1], other) {
		t.Errorf("LoadOpenPlatformFaults() = %+v, want %+v then %+v", open, fault, other)
	}

	fault.Holds = append(fault.Holds, PlatformFaultHold{IssueID: "id-51", Identifier: "ONS-51", HeldAt: "2026-11-02T10:02:00Z"})
	fault.ActionDoneAt, fault.ActionDoneBy = "2026-11-02T12:00:00Z", "taichi"
	fault.LastCheckAt, fault.LastCheckError = "2026-11-02T12:00:00Z", "still refused"
	if err := s.SavePlatformFault(ctx, fault); err != nil {
		t.Fatalf("SavePlatformFault(update): %v", err)
	}
	open, err = s.LoadOpenPlatformFaults(ctx)
	if err != nil || len(open) != 2 || !reflect.DeepEqual(open[0], fault) {
		t.Fatalf("LoadOpenPlatformFaults() after the update = %+v, %v, want %+v first", open, err, fault)
	}

	fault.ResolvedAt = "2026-11-02T12:05:00Z"
	if err := s.SavePlatformFault(ctx, fault); err != nil {
		t.Fatalf("SavePlatformFault(resolved): %v", err)
	}
	open, err = s.LoadOpenPlatformFaults(ctx)
	if err != nil || len(open) != 1 || open[0].ID != "pf-2" {
		t.Fatalf("LoadOpenPlatformFaults() after the resolution = %+v, %v, want only pf-2", open, err)
	}
	var holds int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM platform_fault_holds WHERE fault_id = 'pf-1'`).Scan(&holds); err != nil || holds != 0 {
		t.Errorf("holds of the resolved fault = %d, %v, want none", holds, err)
	}
}

func TestPlatformFaults_QuestionKindRaiserAndWithdrawal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t)
	migrateOrFatal(t, s)

	q := PlatformFault{
		ID: "pf-q", Signature: "supervisor_question: prove o/r#12 @ the workbench", Class: "supervisor_question", Step: "supervisor",
		Action: "Prove o/r#12", ActionWhere: "the workbench", RecordedAt: "2026-11-02T10:00:00Z",
		QuestionKind: "live_proof", RaisedBy: "supervisor",
		Holds: []PlatformFaultHold{{IssueID: "id-77", Identifier: "ONS-77", HeldAt: "2026-11-02T10:00:00Z"}},
	}
	if err := s.SavePlatformFault(ctx, q); err != nil {
		t.Fatalf("SavePlatformFault: %v", err)
	}
	open, err := s.LoadOpenPlatformFaults(ctx)
	if err != nil || len(open) != 1 || !reflect.DeepEqual(open[0], q) {
		t.Fatalf("LoadOpenPlatformFaults() = %+v, %v, want %+v", open, err, q)
	}

	q.Withdrawn, q.ResolvedAt = "the forge shows o/r#12 merged and reverted", "2026-11-02T11:00:00Z"
	if err := s.SavePlatformFault(ctx, q); err != nil {
		t.Fatalf("SavePlatformFault(withdrawn): %v", err)
	}
	var withdrawn string
	if err := s.db.QueryRowContext(ctx, `SELECT withdrawn FROM platform_faults WHERE id = 'pf-q'`).Scan(&withdrawn); err != nil || withdrawn != q.Withdrawn {
		t.Errorf("withdrawn = %q, %v, want the evidence", withdrawn, err)
	}
	if open, err := s.LoadOpenPlatformFaults(ctx); err != nil || len(open) != 0 {
		t.Errorf("LoadOpenPlatformFaults() after the withdrawal = %+v, %v, want none", open, err)
	}
}
