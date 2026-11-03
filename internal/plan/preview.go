package plan

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/registry"
)

// Tracker is the configured tracker as plan sync sees it: the adapter and
// the metadata its kind registered.
type Tracker struct {
	Adapter domain.TrackerAdapter
	Meta    registry.TrackerMeta

	// Repositories returns the names of the repositories the workflow
	// lets Sortie open. Nil means the workflow is not consulted, and a
	// preview refuses no repository.
	Repositories func() []string

	// RequiresRepository reports whether every story must name its
	// repository: the workflow lists repositories and names no default.
	// Nil requires none.
	RequiresRepository func() bool

	// States returns the workflow's states. Nil means plan control is
	// off. Plan control is on when it reports a release state.
	States func() domain.TrackerStates
}

// planStates returns the release and backlog states when plan control is
// on, and ok false otherwise.
func (t Tracker) planStates() (release, backlog string, ok bool) {
	if t.States == nil {
		return "", "", false
	}
	st := t.States()
	if st.Release == "" || st.Backlog == "" {
		return "", "", false
	}
	return st.Release, st.Backlog, true
}

// Has reports whether the adapter declares c and implements it.
func (t Tracker) Has(c registry.WriteCapability) bool {
	if !t.Meta.HasWriteCapability(c) {
		return false
	}
	var ok bool
	switch c {
	case registry.CapCreateIssue:
		_, ok = t.Adapter.(domain.IssueCreator)
	case registry.CapUpdateIssue:
		_, ok = t.Adapter.(domain.IssueUpdater)
	case registry.CapSetParent:
		_, ok = t.Adapter.(domain.IssueParenter)
	case registry.CapLinkDependency:
		_, ok = t.Adapter.(domain.DependencyLinker)
	case registry.CapApprovalFields:
		_, ok = t.Adapter.(domain.ApprovalWriter)
	}
	return ok
}

// Capabilities lists the write capabilities the adapter declares and
// implements, in a stable order. Never nil.
func (t Tracker) Capabilities() []registry.WriteCapability {
	out := []registry.WriteCapability{}
	for _, c := range registry.AllWriteCapabilities() {
		if t.Has(c) {
			out = append(out, c)
		}
	}
	return out
}

func (t Tracker) finder() (domain.PlanKeyFinder, bool) {
	f, ok := t.Adapter.(domain.PlanKeyFinder)
	return f, ok && len(t.Meta.WriteCapabilities) > 0
}

// IssueRef names a tracker issue that carries a plan key.
type IssueRef struct {
	PlanKey    string `json:"plan_key"`
	IssueID    string `json:"issue_id"`
	Identifier string `json:"identifier"`
}

// CreateOp creates the issue of one plan node. A story's parent is the
// issue of ParentKey, created first when it does not exist yet.
type CreateOp struct {
	PlanKey   string `json:"plan_key"`
	Kind      string `json:"kind"`
	Title     string `json:"title"`
	ParentKey string `json:"parent_key,omitempty"`
	State     string `json:"state,omitempty"`
}

// ReleaseOp moves a story from the backlog state to the release state.
// IssueID is empty for a story the same apply creates.
type ReleaseOp struct {
	PlanKey    string `json:"plan_key"`
	IssueID    string `json:"issue_id,omitempty"`
	Identifier string `json:"identifier,omitempty"`
	To         string `json:"to"`
}

// LabelOp keeps a story's one review label: it adds Add and removes each
// label in Remove. IssueID is empty for a story the same apply creates.
type LabelOp struct {
	PlanKey string   `json:"plan_key"`
	IssueID string   `json:"issue_id,omitempty"`
	Add     string   `json:"add,omitempty"`
	Remove  []string `json:"remove"`
}

// ApprovalOp records a story's approver. IssueID is empty for a story
// the same apply creates.
type ApprovalOp struct {
	PlanKey string `json:"plan_key"`
	IssueID string `json:"issue_id,omitempty"`
	By      string `json:"by"`
	ByID    string `json:"by_id"`
	At      string `json:"at"`
}

// RepositoryLabelPrefix starts the label a sync keeps on a story that
// names its repository.
const RepositoryLabelPrefix = config.RepositoryLabelPrefix

// UnlistedRepository is a story naming a repository the workflow does
// not list. A preview holding one cannot be applied.
type UnlistedRepository struct {
	PlanKey    string `json:"plan_key"`
	Repository string `json:"repository"`
}

// ReviewLabelPrefix starts every review label a sync keeps on a story.
const ReviewLabelPrefix = "sortie-review:"

// UnsupportedApprovalFields is listed in [Preview.Unsupported] when the
// graph carries approvers that the adapter cannot store. Labels and
// releases need no capability: every adapter labels and transitions.
const UnsupportedApprovalFields = "approval_fields_unsupported"

// UpdateOp changes the listed fields ("title", "body", "kind") of an
// existing issue.
type UpdateOp struct {
	IssueRef
	Fields []string `json:"fields"`
}

// ParentOp sets an existing story's parent to the issue of ParentKey.
type ParentOp struct {
	IssueRef
	ParentKey string `json:"parent_key"`
}

// RelationOp is a blocking relation between two plan nodes.
type RelationOp struct {
	Blocker string `json:"blocker"`
	Blocked string `json:"blocked"`
}

// Counts summarizes a preview's writes.
type Counts struct {
	Creates          int `json:"creates"`
	Updates          int `json:"updates"`
	Parents          int `json:"parents"`
	RelationsAdded   int `json:"relations_added"`
	RelationsRemoved int `json:"relations_removed"`
	Orphans          int `json:"orphans"`
	Releases         int `json:"releases"`
	Labels           int `json:"labels"`
	Approvals        int `json:"approvals"`
}

// Preview lists the writes that make the tracker match a graph's approved
// nodes. It writes nothing. Every slice is non-nil.
type Preview struct {
	SourceDigest         string                     `json:"source_digest"`
	TrackerDigest        string                     `json:"tracker_digest"`
	Counts               Counts                     `json:"counts"`
	Creates              []CreateOp                 `json:"creates"`
	Updates              []UpdateOp                 `json:"updates"`
	Parents              []ParentOp                 `json:"parents"`
	RelationsAdded       []RelationOp               `json:"relations_added"`
	RelationsRemoved     []RelationOp               `json:"relations_removed"`
	Orphans              []IssueRef                 `json:"orphans"`
	Releases             []ReleaseOp                `json:"releases"`
	Labels               []LabelOp                  `json:"labels"`
	Approvals            []ApprovalOp               `json:"approvals"`
	SkippedDrafts        []string                   `json:"skipped_drafts"`
	UnplannedChanges     []string                   `json:"unplanned_changes"`
	MissingCapabilities  []registry.WriteCapability `json:"missing_capabilities"`
	Unsupported          []string                   `json:"unsupported"`
	UnlistedRepositories []UnlistedRepository       `json:"unlisted_repositories"`
	MissingRepositories  []string                   `json:"missing_repositories"`

	graph    Graph
	existing map[string]domain.PlanIssue
}

// Writes is the number of tracker writes an apply of p makes.
func (p Preview) Writes() int {
	return len(p.Creates) + len(p.Updates) + len(p.Parents) + len(p.RelationsAdded) + len(p.RelationsRemoved) +
		len(p.Releases) + len(p.Labels) + len(p.Approvals)
}

// ComputePreview reads the tracker's plan-keyed issues and compares them
// with the graph's approved nodes. It fails only when the tracker read
// fails; a capability the tracker lacks is listed, not raised.
func ComputePreview(ctx context.Context, g Graph, t Tracker) (Preview, error) {
	var issues []domain.PlanIssue
	if f, ok := t.finder(); ok {
		var err error
		issues, err = f.ListPlanIssues(ctx, false)
		if err != nil {
			return Preview{}, err
		}
	}
	return diff(g, t, issues), nil
}

func diff(g Graph, t Tracker, issues []domain.PlanIssue) Preview {
	p := Preview{
		SourceDigest:         g.SourceDigest,
		TrackerDigest:        TrackerDigest(issues),
		Creates:              []CreateOp{},
		Updates:              []UpdateOp{},
		Parents:              []ParentOp{},
		RelationsAdded:       []RelationOp{},
		RelationsRemoved:     []RelationOp{},
		Orphans:              []IssueRef{},
		Releases:             []ReleaseOp{},
		Labels:               []LabelOp{},
		Approvals:            []ApprovalOp{},
		Unsupported:          []string{},
		UnlistedRepositories: []UnlistedRepository{},
		MissingRepositories:  []string{},
		SkippedDrafts:        []string{},
		UnplannedChanges:     append([]string{}, g.UnplannedChanges...),
		MissingCapabilities:  []registry.WriteCapability{},
		graph:                g,
		existing:             map[string]domain.PlanIssue{},
	}

	byID := map[string]domain.PlanIssue{}
	for _, i := range issues {
		if _, dup := p.existing[i.PlanKey]; !dup {
			p.existing[i.PlanKey] = i
		}
		byID[i.ID] = i
	}

	parentOf := map[string]string{}
	for _, e := range g.Parents {
		parentOf[e.Child] = e.Parent
	}

	// Approval is decided node by node: an approved story syncs beside
	// drafts of its epic, and an epic syncs when it, or any of its
	// stories, is approved. A ui story that covers a requirement syncs as
	// an approved one does, without an approval.
	inGraph := map[string]bool{}
	approved := map[string]Node{}
	for _, n := range g.Nodes {
		inGraph[n.PlanKey] = true
		if n.synced() {
			approved[n.PlanKey] = n
		}
	}
	for _, n := range g.Nodes {
		if n.Kind != KindStory || !n.synced() {
			continue
		}
		if pk, ok := parentOf[n.PlanKey]; ok {
			for _, e := range g.Nodes {
				if e.PlanKey == pk {
					approved[pk] = e
				}
			}
		}
	}
	for _, n := range g.Nodes {
		if _, ok := approved[n.PlanKey]; !ok && n.Approval == ApprovalDraft {
			p.SkippedDrafts = append(p.SkippedDrafts, n.PlanKey)
		}
	}
	slices.Sort(p.SkippedDrafts)

	release, backlog, planControl := t.planStates()
	planControl = planControl && g.SchemaVersion >= 2
	approvals := t.Has(registry.CapApprovalFields)
	requiresRepository := t.RequiresRepository != nil && t.RequiresRepository()
	approvalsSkipped := false

	// Epics first, then stories, each in graph order: the order an apply
	// creates them in.
	ordered := make([]Node, 0, len(approved))
	for _, kind := range []string{KindEpic, KindStory} {
		for _, n := range g.Nodes {
			if _, ok := approved[n.PlanKey]; ok && n.Kind == kind {
				ordered = append(ordered, n)
			}
		}
	}

	for _, n := range ordered {
		parentKey := ""
		if n.Kind == KindStory {
			if pk, ok := parentOf[n.PlanKey]; ok {
				if _, ok := approved[pk]; ok {
					parentKey = pk
				}
			}
		}
		issue, ok := p.existing[n.PlanKey]
		if planControl && n.Kind == KindStory {
			if !approvals && n.ApprovedBy != nil {
				approvalsSkipped = true
			}
			p.planOps(n, issue, ok, release, backlog, approvals)
		}
		if n.Kind == KindStory && n.Repository == "" && requiresRepository {
			p.MissingRepositories = append(p.MissingRepositories, n.PlanKey)
		} else if n.Kind == KindStory && g.SchemaVersion >= 2 {
			p.repositoryOp(t, n, issue, ok)
		}
		if !ok {
			op := CreateOp{PlanKey: n.PlanKey, Kind: n.Kind, Title: strings.TrimSpace(n.Title), ParentKey: parentKey}
			if planControl {
				op.State = backlog
			}
			p.Creates = append(p.Creates, op)
			continue
		}
		ref := IssueRef{PlanKey: n.PlanKey, IssueID: issue.ID, Identifier: issue.Identifier}
		var fields []string
		if strings.TrimSpace(n.Title) != issue.Title {
			fields = append(fields, "title")
		}
		if BodyDigest(n.Body) != issue.BodyDigest {
			fields = append(fields, "body")
		}
		if string(issue.Kind) != n.Kind {
			fields = append(fields, "kind")
		}
		if len(fields) > 0 {
			p.Updates = append(p.Updates, UpdateOp{IssueRef: ref, Fields: fields})
		}
		if parentKey != "" {
			parent, exists := p.existing[parentKey]
			if !exists || issue.ParentID != parent.ID {
				p.Parents = append(p.Parents, ParentOp{IssueRef: ref, ParentKey: parentKey})
			}
		}
	}

	// Relations: the graph's edges between approved nodes, against the
	// tracker's relations between plan-keyed issues. A relation is removed
	// only when both ends are approved nodes or orphans, so relations of
	// draft changes and relations people added to issues without a plan
	// key are left alone.
	want := map[RelationOp]bool{}
	for _, e := range g.Blocks {
		_, a := approved[e.Blocker]
		_, b := approved[e.Blocked]
		if a && b {
			want[RelationOp(e)] = true
		}
	}
	managed := func(key string) bool {
		_, ok := approved[key]
		return ok || !inGraph[key]
	}
	have := map[RelationOp]bool{}
	for _, i := range issues {
		for _, blockerID := range i.BlockedBy {
			blocker, ok := byID[blockerID]
			if !ok || blocker.PlanKey == "" {
				continue
			}
			have[RelationOp{Blocker: blocker.PlanKey, Blocked: i.PlanKey}] = true
		}
	}
	for r := range want {
		if !have[r] {
			p.RelationsAdded = append(p.RelationsAdded, r)
		}
	}
	for r := range have {
		if !want[r] && managed(r.Blocker) && managed(r.Blocked) {
			p.RelationsRemoved = append(p.RelationsRemoved, r)
		}
	}
	sortRelations(p.RelationsAdded)
	sortRelations(p.RelationsRemoved)

	for _, i := range issues {
		if !inGraph[i.PlanKey] {
			p.Orphans = append(p.Orphans, IssueRef{PlanKey: i.PlanKey, IssueID: i.ID, Identifier: i.Identifier})
		}
	}
	slices.SortFunc(p.Orphans, func(a, b IssueRef) int { return cmp.Compare(a.PlanKey, b.PlanKey) })

	need := map[registry.WriteCapability]bool{}
	if len(p.Creates) > 0 {
		need[registry.CapCreateIssue] = true
	}
	if len(p.Updates) > 0 {
		need[registry.CapUpdateIssue] = true
	}
	if len(p.Parents) > 0 {
		need[registry.CapSetParent] = true
	}
	if len(want) > 0 || len(p.RelationsRemoved) > 0 {
		need[registry.CapLinkDependency] = true
	}
	for _, c := range registry.AllWriteCapabilities() {
		if need[c] && !t.Has(c) {
			p.MissingCapabilities = append(p.MissingCapabilities, c)
		}
	}

	if approvalsSkipped {
		p.Unsupported = append(p.Unsupported, UnsupportedApprovalFields)
	}

	p.Counts = Counts{
		Creates: len(p.Creates), Updates: len(p.Updates), Parents: len(p.Parents),
		RelationsAdded: len(p.RelationsAdded), RelationsRemoved: len(p.RelationsRemoved), Orphans: len(p.Orphans),
		Releases: len(p.Releases), Labels: len(p.Labels), Approvals: len(p.Approvals),
	}
	return p
}

// planOps adds the plan-control operations of one synced story: its
// release, its review label, and its approver. A ui story has none. exists reports whether the
// story's issue exists, as issue.
func (p *Preview) planOps(n Node, issue domain.PlanIssue, exists bool, release, backlog string, approvals bool) {
	id := ""
	if exists {
		id = issue.ID
	}
	if n.releases() && (!exists || strings.EqualFold(issue.State, backlog)) {
		p.Releases = append(p.Releases, ReleaseOp{PlanKey: n.PlanKey, IssueID: id, Identifier: issue.Identifier, To: release})
	}
	if n.Review != "" {
		want := ReviewLabelPrefix + n.Review
		op := LabelOp{PlanKey: n.PlanKey, IssueID: id, Remove: []string{}}
		has := false
		for _, l := range issue.Labels {
			if !strings.HasPrefix(strings.ToLower(l), ReviewLabelPrefix) {
				continue
			}
			if strings.EqualFold(l, want) {
				has = true
			} else {
				op.Remove = append(op.Remove, l)
			}
		}
		if !has {
			op.Add = want
		}
		if op.Add != "" || len(op.Remove) > 0 {
			p.Labels = append(p.Labels, op)
		}
	}
	if approvals && n.ApprovedBy != nil {
		want := domain.PlanApproval{By: n.ApprovedBy.Name, ByID: n.ApprovedBy.ID, At: n.ApprovedAt}
		if issue.Approval == nil || *issue.Approval != want {
			p.Approvals = append(p.Approvals, ApprovalOp{PlanKey: n.PlanKey, IssueID: id, By: want.By, ByID: want.ByID, At: want.At})
		}
	}
}

func sortRelations(rs []RelationOp) {
	slices.SortFunc(rs, func(a, b RelationOp) int {
		return cmp.Or(cmp.Compare(a.Blocked, b.Blocked), cmp.Compare(a.Blocker, b.Blocker))
	})
}

// BodyDigest is the hex SHA-256 of a Markdown body, the form
// [domain.PlanIssue.BodyDigest] reports.
func BodyDigest(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

// TrackerDigest identifies the plan-keyed tracker state a preview was
// computed from: each issue's id, plan key, title, body digest, kind,
// parent, blockers, review labels, and approver. It leaves out state, so
// an apply checks each release against the issue's state again.
func TrackerDigest(issues []domain.PlanIssue) string {
	lines := make([]string, 0, len(issues))
	for _, i := range issues {
		blockers := slices.Sorted(slices.Values(i.BlockedBy))
		var review []string
		for _, l := range i.Labels {
			if strings.HasPrefix(strings.ToLower(l), ReviewLabelPrefix) || strings.HasPrefix(strings.ToLower(l), RepositoryLabelPrefix) {
				review = append(review, strings.ToLower(l))
			}
		}
		slices.Sort(review)
		approval := ""
		if i.Approval != nil {
			approval = i.Approval.By + "\x01" + i.Approval.ByID + "\x01" + i.Approval.At
		}
		lines = append(lines, fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s",
			i.ID, i.PlanKey, i.Title, i.BodyDigest, i.Kind, i.ParentID, strings.Join(blockers, ","),
			strings.Join(review, ","), approval))
	}
	slices.Sort(lines)
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// repositoryOp keeps a story's one repository label: the label of the
// repository the node names, and none when it names none. A name the
// workflow does not list is recorded in UnlistedRepositories instead.
func (p *Preview) repositoryOp(t Tracker, n Node, issue domain.PlanIssue, exists bool) {
	if n.Repository != "" && t.Repositories != nil && !slices.Contains(t.Repositories(), n.Repository) {
		p.UnlistedRepositories = append(p.UnlistedRepositories, UnlistedRepository{PlanKey: n.PlanKey, Repository: n.Repository})
		return
	}
	id := ""
	if exists {
		id = issue.ID
	}
	want := ""
	if n.Repository != "" {
		want = RepositoryLabelPrefix + n.Repository
	}
	op := LabelOp{PlanKey: n.PlanKey, IssueID: id, Remove: []string{}}
	has := false
	for _, l := range issue.Labels {
		if !strings.HasPrefix(strings.ToLower(l), RepositoryLabelPrefix) {
			continue
		}
		if want != "" && strings.EqualFold(l, want) {
			has = true
		} else {
			op.Remove = append(op.Remove, l)
		}
	}
	if want != "" && !has {
		op.Add = want
	}
	if op.Add != "" || len(op.Remove) > 0 {
		p.Labels = append(p.Labels, op)
	}
}
