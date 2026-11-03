package persistence

import (
	"context"
	"testing"
)

func TestStandingHolds_RoundTripReplaceAndDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := mustOpenStore(t)

	first := StandingHold{
		IssueID:    "id-54",
		Identifier: "ONS-54",
		Holder:     "supervisor",
		Reason:     "live proof in a taichi sandbox",
		Evidence:   "sandbox hl-ons-54-proof-1",
		HeldAt:     "2026-11-03T12:00:00Z",
		RenewedAt:  "2026-11-03T12:00:00Z",
		Until:      "2026-11-03T13:00:00Z",
	}
	if err := s.UpsertStandingHold(ctx, first); err != nil {
		t.Fatalf("UpsertStandingHold: %v", err)
	}
	second := first
	second.Evidence = "step k8s-up running"
	second.RenewedAt = "2026-11-03T12:50:00Z"
	second.Until = "2026-11-03T13:50:00Z"
	second.ExpiredAt = "2026-11-03T13:50:00Z"
	if err := s.UpsertStandingHold(ctx, second); err != nil {
		t.Fatalf("UpsertStandingHold (replace): %v", err)
	}

	rows, err := s.ListStandingHolds(ctx)
	if err != nil {
		t.Fatalf("ListStandingHolds: %v", err)
	}
	if len(rows) != 1 || rows[0] != second {
		t.Fatalf("ListStandingHolds() = %+v, want [%+v]", rows, second)
	}

	if err := s.DeleteStandingHold(ctx, "id-54"); err != nil {
		t.Fatalf("DeleteStandingHold: %v", err)
	}
	if err := s.DeleteStandingHold(ctx, "id-54"); err != nil {
		t.Fatalf("DeleteStandingHold of a missing row: %v", err)
	}
	rows, err = s.ListStandingHolds(ctx)
	if err != nil {
		t.Fatalf("ListStandingHolds after delete: %v", err)
	}
	if rows == nil || len(rows) != 0 {
		t.Errorf("ListStandingHolds() after delete = %#v, want an empty non-nil slice", rows)
	}
}
