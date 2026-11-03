package plan

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/registry"
)

// maxStoredPreviews bounds how many preview ids a [Syncer] remembers.
const maxStoredPreviews = 32

// ErrApplyInProgress reports that another apply holds the syncer.
var ErrApplyInProgress = errors.New("plan sync: another apply is in progress")

// ConflictError reports an apply refused because the graph or the tracker
// changed since its preview, or because the preview is unknown.
type ConflictError struct {
	Reason string
}

func (e *ConflictError) Error() string { return "plan sync: " + e.Reason }

// Failure names the write an apply stopped at.
type Failure struct {
	Op      string `json:"op"`
	PlanKey string `json:"plan_key"`
	Error   string `json:"error"`
}

// Result reports what an apply wrote, in order, and where it stopped.
// Every slice is non-nil.
type Result struct {
	Created          []IssueRef   `json:"created"`
	Updated          []IssueRef   `json:"updated"`
	ParentsSet       []IssueRef   `json:"parents_set"`
	RelationsAdded   []RelationOp `json:"relations_added"`
	RelationsRemoved []RelationOp `json:"relations_removed"`
	Labeled          []IssueRef   `json:"labeled"`
	Approved         []IssueRef   `json:"approved"`
	Released         []IssueRef   `json:"released"`
	ReleaseSkipped   []IssueRef   `json:"release_skipped"`
	Orphans          []IssueRef   `json:"orphans"`
	SkippedDrafts    []string     `json:"skipped_drafts"`
	Unsupported      []string     `json:"unsupported"`
	Failed           *Failure     `json:"failed"`
}

type storedPreview struct {
	id            string
	sourceDigest  string
	trackerDigest string
	releases      []string
}

// Syncer previews and applies plan graphs against one tracker. Only one
// apply runs at a time; a second gets [ErrApplyInProgress]. Safe for
// concurrent use.
type Syncer struct {
	tracker Tracker

	apply sync.Mutex

	mu       sync.Mutex
	previews []storedPreview
}

// NewSyncer returns a syncer for t.
func NewSyncer(t Tracker) *Syncer {
	return &Syncer{tracker: t}
}

// Tracker returns the tracker the syncer writes to.
func (s *Syncer) Tracker() Tracker { return s.tracker }

// Preview computes a preview of g and remembers it under the returned id,
// so a later [Syncer.Apply] can check that nothing changed in between.
func (s *Syncer) Preview(ctx context.Context, g Graph) (Preview, string, error) {
	p, err := ComputePreview(ctx, g, s.tracker)
	if err != nil {
		return Preview{}, "", err
	}
	id := newPreviewID()
	s.mu.Lock()
	var releases []string
	for _, r := range p.Releases {
		releases = append(releases, r.PlanKey)
	}
	s.previews = append(s.previews, storedPreview{id: id, sourceDigest: p.SourceDigest, trackerDigest: p.TrackerDigest, releases: releases})
	if len(s.previews) > maxStoredPreviews {
		s.previews = s.previews[len(s.previews)-maxStoredPreviews:]
	}
	s.mu.Unlock()
	return p, id, nil
}

func newPreviewID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (s *Syncer) stored(id string) (storedPreview, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.previews {
		if p.id == id {
			return p, true
		}
	}
	return storedPreview{}, false
}

// Apply applies the preview with previewID to g. It refuses, before any
// write, when the preview is unknown, when g's source digest or the
// tracker's plan-keyed state differs from the preview's, or when the
// tracker lacks a capability the writes need. Otherwise it writes in
// dependency order and stops at the first failure, reporting progress in
// the result alongside the error.
func (s *Syncer) Apply(ctx context.Context, g Graph, previewID string) (Result, error) {
	if !s.apply.TryLock() {
		return Result{}, ErrApplyInProgress
	}
	defer s.apply.Unlock()

	stored, ok := s.stored(previewID)
	if !ok {
		return Result{}, &ConflictError{Reason: fmt.Sprintf("unknown preview %q; request a new preview", previewID)}
	}
	if g.SourceDigest != stored.sourceDigest {
		return Result{}, &ConflictError{Reason: "the plan graph changed since the preview; request a new preview"}
	}
	p, err := ComputePreview(ctx, g, s.tracker)
	if err != nil {
		return Result{}, err
	}
	if p.TrackerDigest != stored.trackerDigest {
		return Result{}, &ConflictError{Reason: "the tracker changed since the preview; request a new preview"}
	}
	r, err := s.run(ctx, p)
	// The tracker digest leaves out state, so a story that left the
	// backlog after the preview is dropped from the releases here, never
	// forced: report it.
	now := map[string]bool{}
	for _, op := range p.Releases {
		now[op.PlanKey] = true
	}
	for _, key := range stored.releases {
		if !now[key] {
			ref := IssueRef{PlanKey: key}
			if issue, ok := p.existing[key]; ok {
				ref.IssueID, ref.Identifier = issue.ID, issue.Identifier
			}
			r.ReleaseSkipped = append(r.ReleaseSkipped, ref)
		}
	}
	return r, err
}

// ApplyNow previews g and applies that preview at once, under the same
// lock and checks as [Syncer.Apply].
func (s *Syncer) ApplyNow(ctx context.Context, g Graph) (Preview, Result, error) {
	if !s.apply.TryLock() {
		return Preview{}, Result{}, ErrApplyInProgress
	}
	defer s.apply.Unlock()

	p, err := ComputePreview(ctx, g, s.tracker)
	if err != nil {
		return Preview{}, Result{}, err
	}
	r, err := s.run(ctx, p)
	return p, r, err
}

// UnsupportedError builds the error for a preview that needs capabilities
// the tracker lacks.
func UnsupportedError(missing []registry.WriteCapability) error {
	names := make([]string, 0, len(missing))
	for _, c := range missing {
		names = append(names, string(c))
	}
	return &domain.TrackerError{
		Kind:    domain.ErrTrackerUnsupportedOperation,
		Message: "the configured tracker does not declare " + strings.Join(names, ", "),
	}
}

func (s *Syncer) run(ctx context.Context, p Preview) (Result, error) {
	r := Result{
		Created: []IssueRef{}, Updated: []IssueRef{}, ParentsSet: []IssueRef{},
		RelationsAdded: []RelationOp{}, RelationsRemoved: []RelationOp{},
		Labeled: []IssueRef{}, Approved: []IssueRef{}, Released: []IssueRef{}, ReleaseSkipped: []IssueRef{},
		Orphans: p.Orphans, SkippedDrafts: p.SkippedDrafts, Unsupported: p.Unsupported,
	}
	if len(p.MissingCapabilities) > 0 {
		return r, UnsupportedError(p.MissingCapabilities)
	}
	if len(p.UnlistedRepositories) > 0 {
		return r, UnlistedRepositoriesError(p.UnlistedRepositories)
	}
	if len(p.MissingRepositories) > 0 {
		return r, MissingRepositoriesError(p.MissingRepositories)
	}

	nodes := map[string]Node{}
	for _, n := range p.graph.Nodes {
		nodes[n.PlanKey] = n
	}
	ids := map[string]string{}
	idents := map[string]string{}
	for key, issue := range p.existing {
		ids[key] = issue.ID
		idents[key] = issue.Identifier
	}
	createdNow := map[string]bool{}
	fail := func(op, key string, err error) (Result, error) {
		r.Failed = &Failure{Op: op, PlanKey: key, Error: err.Error()}
		return r, err
	}

	if len(p.Creates) > 0 {
		creator := s.tracker.Adapter.(domain.IssueCreator)
		for _, c := range p.Creates {
			n := nodes[c.PlanKey]
			in := domain.NewIssue{Title: c.Title, Body: n.Body, Kind: domain.IssueKind(c.Kind), PlanKey: c.PlanKey, State: c.State}
			if c.ParentKey != "" {
				in.ParentID = ids[c.ParentKey]
			}
			created, err := creator.CreateIssue(ctx, in)
			if err != nil {
				return fail("create", c.PlanKey, err)
			}
			ids[c.PlanKey] = created.ID
			idents[c.PlanKey] = created.Identifier
			createdNow[c.PlanKey] = true
			r.Created = append(r.Created, IssueRef{PlanKey: c.PlanKey, IssueID: created.ID, Identifier: created.Identifier})
		}
	}
	if len(p.Parents) > 0 {
		parenter := s.tracker.Adapter.(domain.IssueParenter)
		for _, op := range p.Parents {
			if err := parenter.SetParent(ctx, op.IssueID, ids[op.ParentKey]); err != nil {
				return fail("set_parent", op.PlanKey, err)
			}
			r.ParentsSet = append(r.ParentsSet, op.IssueRef)
		}
	}
	if len(p.Updates) > 0 {
		updater := s.tracker.Adapter.(domain.IssueUpdater)
		for _, op := range p.Updates {
			n := nodes[op.PlanKey]
			var fields domain.IssueFields
			for _, f := range op.Fields {
				switch f {
				case "title":
					title := strings.TrimSpace(n.Title)
					fields.Title = &title
				case "body":
					body := n.Body
					fields.Body = &body
				case "kind":
					kind := domain.IssueKind(n.Kind)
					fields.Kind = &kind
				}
			}
			if err := updater.UpdateIssue(ctx, op.IssueID, fields); err != nil {
				return fail("update", op.PlanKey, err)
			}
			r.Updated = append(r.Updated, op.IssueRef)
		}
	}
	if len(p.RelationsAdded)+len(p.RelationsRemoved) > 0 {
		linker := s.tracker.Adapter.(domain.DependencyLinker)
		for _, rel := range p.RelationsAdded {
			if err := linker.AddDependency(ctx, ids[rel.Blocker], ids[rel.Blocked]); err != nil {
				return fail("add_relation", rel.Blocked, err)
			}
			r.RelationsAdded = append(r.RelationsAdded, rel)
		}
		for _, rel := range p.RelationsRemoved {
			if err := linker.RemoveDependency(ctx, ids[rel.Blocker], ids[rel.Blocked]); err != nil {
				return fail("remove_relation", rel.Blocked, err)
			}
			r.RelationsRemoved = append(r.RelationsRemoved, rel)
		}
	}
	for _, op := range p.Labels {
		ref := IssueRef{PlanKey: op.PlanKey, IssueID: ids[op.PlanKey], Identifier: idents[op.PlanKey]}
		if op.Add != "" {
			if err := s.tracker.Adapter.AddLabel(ctx, ref.IssueID, op.Add); err != nil {
				return fail("add_label", op.PlanKey, err)
			}
		}
		for _, l := range op.Remove {
			if err := s.tracker.Adapter.RemoveLabel(ctx, ref.IssueID, l); err != nil {
				return fail("remove_label", op.PlanKey, err)
			}
		}
		r.Labeled = append(r.Labeled, ref)
	}
	if len(p.Approvals) > 0 {
		writer := s.tracker.Adapter.(domain.ApprovalWriter)
		for _, op := range p.Approvals {
			ref := IssueRef{PlanKey: op.PlanKey, IssueID: ids[op.PlanKey], Identifier: idents[op.PlanKey]}
			if err := writer.SetApproval(ctx, ref.IssueID, domain.PlanApproval{By: op.By, ByID: op.ByID, At: op.At}); err != nil {
				return fail("set_approval", op.PlanKey, err)
			}
			r.Approved = append(r.Approved, ref)
		}
	}
	// Releases come last, after the story's blocking relations exist. A
	// story this apply did not create is read again, and released only
	// while it still waits in the backlog state.
	if len(p.Releases) > 0 {
		_, backlog, _ := s.tracker.planStates()
		finder, _ := s.tracker.finder()
		for _, op := range p.Releases {
			ref := IssueRef{PlanKey: op.PlanKey, IssueID: ids[op.PlanKey], Identifier: idents[op.PlanKey]}
			if !createdNow[op.PlanKey] {
				if finder == nil {
					r.ReleaseSkipped = append(r.ReleaseSkipped, ref)
					continue
				}
				now, err := finder.FindIssueByPlanKey(ctx, op.PlanKey)
				if err != nil {
					return fail("release", op.PlanKey, err)
				}
				if !strings.EqualFold(now.State, backlog) {
					r.ReleaseSkipped = append(r.ReleaseSkipped, ref)
					continue
				}
			}
			if err := s.tracker.Adapter.TransitionIssue(ctx, ref.IssueID, op.To); err != nil {
				return fail("release", op.PlanKey, err)
			}
			r.Released = append(r.Released, ref)
		}
	}
	return r, nil
}

// UnlistedRepositoriesError is the payload error an apply returns when
// stories name repositories the workflow does not list. It writes
// nothing.
func UnlistedRepositoriesError(unlisted []UnlistedRepository) error {
	names := make([]string, 0, len(unlisted))
	for _, u := range unlisted {
		names = append(names, fmt.Sprintf("%s (%s)", u.PlanKey, u.Repository))
	}
	return &domain.TrackerError{
		Kind:    domain.ErrTrackerPayload,
		Message: "stories name repositories the workflow does not list in its repositories: " + strings.Join(names, ", "),
	}
}

// MissingRepositoriesError is the payload error an apply returns when
// stories, named by plan key, name no repository and the workflow names
// no default_repository. It writes nothing.
func MissingRepositoriesError(planKeys []string) error {
	return &domain.TrackerError{
		Kind:    domain.ErrTrackerPayload,
		Message: "stories name no repository and the workflow sets no default_repository: " + strings.Join(planKeys, ", "),
	}
}
