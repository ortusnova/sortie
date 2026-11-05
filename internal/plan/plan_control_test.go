package plan

import (
	"context"
	"slices"
	"testing"

	"github.com/sortie-ai/sortie/internal/domain"
)

func v2(g Graph) Graph {
	g.SchemaVersion = 2
	return g
}

func story(g *Graph, key string) *Node {
	for i := range g.Nodes {
		if g.Nodes[i].PlanKey == key {
			return &g.Nodes[i]
		}
	}
	panic("no node " + key)
}

func released(g *Graph, keys ...string) {
	yes := true
	for _, k := range keys {
		story(g, k).Release = &yes
	}
}

func apply(t *testing.T, tr Tracker, g Graph) Result {
	t.Helper()
	s := NewSyncer(tr)
	_, id, err := s.Preview(context.Background(), g)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Apply(context.Background(), g, id)
	if err != nil {
		t.Fatalf("Apply() error = %v (result %+v)", err, r)
	}
	return r
}

// New drafts beside approved stories: the approved ones sync.
func TestPlanSync_DraftsPerStory(t *testing.T) {
	t.Parallel()
	g := newGraph().epic("billing", ApprovalApproved, "a", "b", "c", "d", "e", "f").block("billing/e", "billing/a").build()
	story(&g, "billing/e").Approval = ApprovalDraft
	story(&g, "billing/f").Approval = ApprovalDraft
	p := preview(t, g, newFakeTracker().tracker())

	var created []string
	for _, c := range p.Creates {
		created = append(created, c.PlanKey)
	}
	if !slices.Equal(created, []string{"billing", "billing/a", "billing/b", "billing/c", "billing/d"}) {
		t.Errorf("creates = %v, want the epic and the 4 approved stories", created)
	}
	if !slices.Equal(p.SkippedDrafts, []string{"billing/e", "billing/f"}) {
		t.Errorf("skipped = %v, want the 2 drafts", p.SkippedDrafts)
	}
	if len(p.RelationsAdded) != 0 {
		t.Errorf("relations = %+v, want none: an edge to a draft is not synced", p.RelationsAdded)
	}
}

// An epic whose own approval is draft still syncs for an approved story.
func TestPlanSync_EpicFollowsItsStories(t *testing.T) {
	t.Parallel()
	g := newGraph().epic("billing", ApprovalDraft, "a").build()
	story(&g, "billing/a").Approval = ApprovalApproved
	p := preview(t, g, newFakeTracker().tracker())
	if len(p.Creates) != 2 || p.Creates[0].PlanKey != "billing" || p.Creates[1].ParentKey != "billing" {
		t.Errorf("creates = %+v, want the epic, then the story under it", p.Creates)
	}
}

func TestPlanSync_ReleaseNewAndWaitingStories(t *testing.T) {
	t.Parallel()
	f := newFakeTracker()
	g := v2(newGraph().epic("billing", ApprovalApproved, "invoice", "refund", "tax").block("billing/invoice", "billing/tax").build())
	released(&g, "billing/invoice", "billing/tax")

	r := apply(t, f.planControlled(), g)

	for _, i := range f.issues {
		want := "Backlog"
		if i.PlanKey == "billing/invoice" || i.PlanKey == "billing/tax" {
			want = "Todo"
		}
		if i.State != want {
			t.Errorf("%s state = %s, want %s", i.PlanKey, i.State, want)
		}
	}
	if len(r.Released) != 2 {
		t.Errorf("released = %+v, want 2", r.Released)
	}
	// The relation of tax is written before tax is released.
	link := slices.Index(f.log, "link billing/invoice>billing/tax")
	rel := slices.Index(f.log, "transition billing/tax>Todo")
	if link < 0 || rel < 0 || link > rel {
		t.Errorf("log = %v, want the relation before the release", f.log)
	}
	// A second sync releases nothing more.
	if p := preview(t, g, f.planControlled()); len(p.Releases) != 0 || p.Writes() != 0 {
		t.Errorf("second preview = %+v, want no writes", p.Counts)
	}
}

// A story a person moved on is never released again.
func TestPlanSync_ReleaseOnlyFromBacklog(t *testing.T) {
	t.Parallel()
	f := newFakeTracker()
	g := v2(newGraph().epic("billing", ApprovalApproved, "invoice").build())
	apply(t, f.planControlled(), g)
	f.issues[1].State = "In Progress"
	released(&g, "billing/invoice")
	if p := preview(t, g, f.planControlled()); len(p.Releases) != 0 {
		t.Errorf("releases = %+v, want none for an issue in progress", p.Releases)
	}
}

// A cancelled story, such as one a person cancelled after a revert of its
// merge, stays cancelled across a full sync that still marks it released.
func TestPlanSync_CancelledStoryStaysCancelled(t *testing.T) {
	t.Parallel()
	f := newFakeTracker()
	g := v2(newGraph().epic("platform-upkeep", ApprovalApproved, "home-released-stories").build())
	released(&g, "platform-upkeep/home-released-stories")
	apply(t, f.planControlled(), g)
	f.issues[1].State = "Canceled"

	r := apply(t, f.planControlled(), g)
	if f.issues[1].State != "Canceled" || len(r.Released) != 0 {
		t.Errorf("state = %s, released = %+v, want the cancelled story left cancelled", f.issues[1].State, r.Released)
	}
}

// An issue that leaves the backlog between preview and apply is skipped.
func TestPlanSync_ReleaseRecheckedAtApply(t *testing.T) {
	t.Parallel()
	f := newFakeTracker()
	g := v2(newGraph().epic("billing", ApprovalApproved, "invoice").build())
	apply(t, f.planControlled(), g)
	released(&g, "billing/invoice")

	s := NewSyncer(f.planControlled())
	p, id, err := s.Preview(context.Background(), g)
	if err != nil || len(p.Releases) != 1 {
		t.Fatalf("preview = %+v, %v, want one release", p.Releases, err)
	}
	f.issues[1].State = "Todo" // a person released it by hand meanwhile
	r, err := s.Apply(context.Background(), g, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Released) != 0 || len(r.ReleaseSkipped) != 1 {
		t.Errorf("released %+v skipped %+v, want one skip", r.Released, r.ReleaseSkipped)
	}
}

// Without plan control, or with a version 1 graph, nothing is released
// and new issues start where the tracker puts them.
func TestPlanSync_NoReleaseWithoutPlanControl(t *testing.T) {
	t.Parallel()
	g := v2(newGraph().epic("billing", ApprovalApproved, "invoice").build())
	released(&g, "billing/invoice")
	if p := preview(t, g, newFakeTracker().tracker()); len(p.Releases) != 0 || p.Creates[0].State != "" {
		t.Errorf("plan control off: releases %+v, create state %q, want none", p.Releases, p.Creates[0].State)
	}
	g1 := newGraph().epic("billing", ApprovalApproved, "invoice").build()
	if p := preview(t, g1, newFakeTracker().planControlled()); len(p.Releases) != 0 || p.Creates[0].State != "" {
		t.Errorf("version 1: releases %+v, create state %q, want none", p.Releases, p.Creates[0].State)
	}
}

func TestPlanSync_ReviewLabel(t *testing.T) {
	t.Parallel()
	f := newFakeTracker()
	g := v2(newGraph().epic("billing", ApprovalApproved, "invoice").build())
	story(&g, "billing/invoice").Review = ReviewCriteria
	apply(t, f.planControlled(), g)
	inv := f.issues[1]
	if !slices.Equal(inv.Labels, []string{"sortie-review:criteria"}) {
		t.Fatalf("labels = %v, want the criteria label", inv.Labels)
	}

	inv.Labels = append(inv.Labels, "customer-visible")
	story(&g, "billing/invoice").Review = ReviewHuman
	apply(t, f.planControlled(), g)
	slices.Sort(inv.Labels)
	if !slices.Equal(inv.Labels, []string{"customer-visible", "sortie-review:human"}) {
		t.Errorf("labels = %v, want the human label and the person's label", inv.Labels)
	}
	if p := preview(t, g, f.planControlled()); len(p.Labels) != 0 {
		t.Errorf("third preview labels = %+v, want none", p.Labels)
	}
}

func TestPlanSync_ApprovalFields(t *testing.T) {
	t.Parallel()
	f := newFakeTracker()
	g := v2(newGraph().epic("billing", ApprovalApproved, "invoice").build())
	story(&g, "billing/invoice").ApprovedBy = &Approver{Name: "Ana Ruiz", ID: "iss|ana"}
	story(&g, "billing/invoice").ApprovedAt = "2026-10-16T14:02:00Z"
	apply(t, f.planControlled(), g)
	if a := f.issues[1].Approval; a == nil || a.By != "Ana Ruiz" || a.ByID != "iss|ana" {
		t.Fatalf("approval = %+v, want Ana", a)
	}
	if p := preview(t, g, f.planControlled()); len(p.Approvals) != 0 {
		t.Errorf("second preview approvals = %+v, want none", p.Approvals)
	}

	// A changed approver between preview and apply makes the preview stale.
	s := NewSyncer(f.planControlled())
	story(&g, "billing/invoice").ApprovedBy = &Approver{Name: "Bo", ID: "iss|bo"}
	_, id, err := s.Preview(context.Background(), g)
	if err != nil {
		t.Fatal(err)
	}
	f.issues[1].Approval.By = "Someone else"
	if _, err := s.Apply(context.Background(), g, id); err == nil {
		t.Error("Apply() after a tracker approval change = nil, want a stale preview")
	}
}

// An adapter without approval fields still syncs the rest, and says so.
func TestPlanSync_ApprovalFieldsUnsupported(t *testing.T) {
	t.Parallel()
	f := newFakeTracker()
	g := v2(newGraph().epic("billing", ApprovalApproved, "invoice").build())
	story(&g, "billing/invoice").ApprovedBy = &Approver{Name: "Ana Ruiz", ID: "iss|ana"}
	p := preview(t, g, f.noApprovals())
	if len(p.Approvals) != 0 || !slices.Equal(p.Unsupported, []string{UnsupportedApprovalFields}) || len(p.MissingCapabilities) != 0 {
		t.Errorf("preview approvals %+v unsupported %v missing %v", p.Approvals, p.Unsupported, p.MissingCapabilities)
	}
	if len(p.Creates) != 2 {
		t.Errorf("creates = %d, want the rest of the sync", len(p.Creates))
	}
}

// Scenario "A UI story from spec to merge", plan sync part: a draft ui
// story that covers a requirement is created, labelled sortie-review:ui,
// and released, with no approver written. One that covers none waits.
func TestPlanSync_UIStoryReleasedWithoutApproval(t *testing.T) {
	t.Parallel()
	f := newFakeTracker()
	g := v2(newGraph().epic("workbench", ApprovalDraft, "worker-activity-view", "sketch").build())
	view := story(&g, "workbench/worker-activity-view")
	view.Review = ReviewUI
	view.Covers = []string{"agent-platform/run-supervision#A UI story is built before it is approved, and its merge is the approval"}
	story(&g, "workbench/sketch").Review = ReviewUI

	p := preview(t, g, f.planControlled())
	if !slices.Equal(p.SkippedDrafts, []string{"workbench/sketch"}) {
		t.Errorf("skipped = %v, want only the ui story that covers no requirement", p.SkippedDrafts)
	}
	if len(p.Approvals) != 0 || len(p.Unsupported) != 0 {
		t.Errorf("approvals %+v unsupported %v, want none for a ui story", p.Approvals, p.Unsupported)
	}

	r := apply(t, f.planControlled(), g)
	if len(r.Released) != 1 || r.Released[0].PlanKey != "workbench/worker-activity-view" {
		t.Fatalf("released = %+v, want the ui story", r.Released)
	}
	var got *domain.PlanIssue
	for _, i := range f.issues {
		if i.PlanKey == "workbench/worker-activity-view" {
			got = i
		}
	}
	if got == nil {
		t.Fatal("the ui story was not created")
	}
	if got.State != "Todo" || !slices.Equal(got.Labels, []string{"sortie-review:ui"}) || got.Approval != nil {
		t.Errorf("ui story state %q labels %v approval %+v, want Todo, sortie-review:ui, and no approver", got.State, got.Labels, got.Approval)
	}
	if p := preview(t, g, f.planControlled()); p.Writes() != 0 {
		t.Errorf("second preview = %+v, want no writes", p.Counts)
	}
}
