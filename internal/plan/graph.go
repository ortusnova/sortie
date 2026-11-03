// Package plan syncs a validated plan graph into the configured tracker.
// The graph is a JSON document that the plan validator outputs; this
// package decodes it, checks only that it is well formed, previews the
// tracker writes that would make the tracker match it, and applies a
// preview through the tracker adapter's optional write capabilities.
// It never reads planning files and never repeats the validator's checks.
package plan

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// SupportedVersion is the newest plan graph schema version this package
// reads. Version 1 is still read. Version 2 adds the optional story
// fields release, review, approved_by, and approved_at.
const SupportedVersion = 2

// Review gates a story can carry.
const (
	ReviewHuman    = "human"
	ReviewCriteria = "criteria"
	// ReviewUI marks a story that is built before it is approved: once it
	// covers a requirement a sync releases it without an approval, and the
	// operator's merge of its pull request is its approval.
	ReviewUI = "ui"
)

// Node kinds and approval states of the graph format.
const (
	KindEpic  = "epic"
	KindStory = "story"

	ApprovalApproved  = "approved"
	ApprovalDraft     = "draft"
	ApprovalUnplanned = "unplanned"
)

var (
	planKeyPattern    = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*(/[a-z0-9]+(-[a-z0-9]+)*)?$`)
	changeNamePattern = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	digestPattern     = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

// Graph is a decoded plan graph document.
type Graph struct {
	SchemaVersion    int          `json:"schema_version"`
	SourceDigest     string       `json:"source_digest"`
	Nodes            []Node       `json:"nodes"`
	Parents          []ParentEdge `json:"parents"`
	Blocks           []BlockEdge  `json:"blocks"`
	UnplannedChanges []string     `json:"unplanned_changes"`
}

// Node is one epic or story of the graph.
type Node struct {
	PlanKey            string   `json:"plan_key"`
	Kind               string   `json:"kind"`
	Change             string   `json:"change"`
	Title              string   `json:"title"`
	Body               string   `json:"body"`
	Approval           string   `json:"approval"`
	HasSpec            bool     `json:"has_spec"`
	Depth              int      `json:"depth"`
	DeliverableKind    *string  `json:"deliverable_kind"`
	Covers             []string `json:"covers"`
	AcceptanceCriteria []string `json:"acceptance_criteria"`

	// Version 2 only, all optional, stories only.
	Release    *bool     `json:"release,omitempty"`
	Review     string    `json:"review,omitempty"`
	ApprovedBy *Approver `json:"approved_by,omitempty"`
	ApprovedAt string    `json:"approved_at,omitempty"`

	// Repository names the configured repository a story runs in.
	// Version 2, stories only, optional: absent means the default.
	Repository string `json:"repository,omitempty"`
}

// Approver names the person who approved a story.
type Approver struct {
	Name string `json:"name"`
	ID   string `json:"id"`
}

// Released reports whether the node asks to be released.
func (n Node) Released() bool { return n.Release != nil && *n.Release }

// builtUnapproved reports whether the node is a ui story that covers a
// requirement. Such a story syncs and is released without an approval.
func (n Node) builtUnapproved() bool {
	return n.Kind == KindStory && n.Review == ReviewUI && len(n.Covers) > 0
}

// synced reports whether a sync writes the node's issue: it is approved,
// or it is a ui story built before its approval.
func (n Node) synced() bool { return n.Approval == ApprovalApproved || n.builtUnapproved() }

// releases reports whether a sync under plan control releases the story.
// A ui story is released once it covers a requirement, and only then.
func (n Node) releases() bool {
	if n.Review == ReviewUI {
		return n.builtUnapproved()
	}
	return n.Released()
}

// ParentEdge parents a story to its epic.
type ParentEdge struct {
	Child  string `json:"child"`
	Parent string `json:"parent"`
}

// BlockEdge records that Blocker blocks Blocked.
type BlockEdge struct {
	Blocker string `json:"blocker"`
	Blocked string `json:"blocked"`
}

// GraphError reports a malformed graph document. It names every problem
// found.
type GraphError struct {
	Problems []string
}

func (e *GraphError) Error() string {
	return "plan graph: " + strings.Join(e.Problems, "; ")
}

var requiredTop = []string{"schema_version", "source_digest", "nodes", "parents", "blocks", "unplanned_changes"}

var requiredNode = []string{
	"plan_key", "kind", "change", "title", "body", "approval", "has_spec",
	"depth", "deliverable_kind", "covers", "acceptance_criteria",
}

// Decode parses and checks a plan graph document: a supported schema
// version first, then the shape the graph schema defines, then that every
// edge names a node in the document.
func Decode(raw []byte) (Graph, error) {
	var head map[string]json.RawMessage
	if err := json.Unmarshal(raw, &head); err != nil {
		return Graph{}, &GraphError{Problems: []string{"not a JSON object: " + err.Error()}}
	}
	var version int
	if err := json.Unmarshal(head["schema_version"], &version); err != nil || head["schema_version"] == nil {
		return Graph{}, &GraphError{Problems: []string{"schema_version must be an integer"}}
	}
	if version < 1 || version > SupportedVersion {
		return Graph{}, &GraphError{Problems: []string{fmt.Sprintf(
			"schema_version %d is not supported; this Sortie reads plan graph versions 1 to %d", version, SupportedVersion)}}
	}

	var problems []string
	problems = append(problems, missing("document", head, requiredTop)...)

	var rawNodes []map[string]json.RawMessage
	if err := json.Unmarshal(head["nodes"], &rawNodes); err == nil {
		for i, n := range rawNodes {
			problems = append(problems, missing(fmt.Sprintf("nodes[%d]", i), n, requiredNode)...)
		}
	}

	var g Graph
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&g); err != nil {
		problems = append(problems, err.Error())
		return Graph{}, &GraphError{Problems: problems}
	}
	problems = append(problems, g.check()...)
	if len(problems) > 0 {
		return Graph{}, &GraphError{Problems: problems}
	}
	return g, nil
}

func missing(where string, obj map[string]json.RawMessage, fields []string) []string {
	var out []string
	for _, f := range fields {
		if _, ok := obj[f]; !ok {
			out = append(out, fmt.Sprintf("%s: missing field %s", where, f))
		}
	}
	return out
}

// checkVersion2 checks the version 2 story fields: absent in version 1,
// stories only, and well formed.
func (g Graph) checkVersion2(where string, n Node) []string {
	set := n.Release != nil || n.Review != "" || n.ApprovedBy != nil || n.ApprovedAt != "" || n.Repository != ""
	if !set {
		return nil
	}
	if g.SchemaVersion < 2 {
		return []string{where + ": release, review, approved_by, approved_at, and repository need schema_version 2"}
	}
	var problems []string
	if n.Kind != KindStory {
		problems = append(problems, where+": release, review, approved_by, approved_at, and repository are for stories only")
	}
	if n.Review != "" && n.Review != ReviewHuman && n.Review != ReviewCriteria && n.Review != ReviewUI {
		problems = append(problems, fmt.Sprintf("%s: review %q is not human, criteria, or ui", where, n.Review))
	}
	if n.Review == ReviewUI && (n.ApprovedBy != nil || n.ApprovedAt != "") {
		problems = append(problems, where+": a ui story carries no approved_by or approved_at; the merge of its pull request is its approval")
	}
	if n.Repository != "" && !changeNamePattern.MatchString(n.Repository) {
		problems = append(problems, fmt.Sprintf("%s: repository %q must match [a-z0-9]+(-[a-z0-9]+)*", where, n.Repository))
	}
	if n.ApprovedBy != nil && strings.TrimSpace(n.ApprovedBy.Name) == "" {
		problems = append(problems, where+": approved_by needs a name")
	}
	if n.ApprovedAt != "" {
		if _, err := time.Parse(time.RFC3339, n.ApprovedAt); err != nil {
			problems = append(problems, where+": approved_at is not an RFC 3339 time")
		}
	}
	if n.Released() && n.Approval != ApprovalApproved && !n.builtUnapproved() {
		problems = append(problems, where+": only an approved story, or a ui story that covers a requirement, can be released")
	}
	return problems
}

func (g Graph) check() []string {
	var problems []string
	if !digestPattern.MatchString(g.SourceDigest) {
		problems = append(problems, "source_digest must be sha256:<64 hex digits>")
	}
	kinds := map[string]string{}
	for i, n := range g.Nodes {
		where := fmt.Sprintf("nodes[%d] (%s)", i, n.PlanKey)
		if !planKeyPattern.MatchString(n.PlanKey) {
			problems = append(problems, where+": malformed plan_key")
		}
		if _, dup := kinds[n.PlanKey]; dup {
			problems = append(problems, where+": duplicate plan_key")
		}
		kinds[n.PlanKey] = n.Kind
		switch n.Kind {
		case KindEpic, KindStory:
		default:
			problems = append(problems, fmt.Sprintf("%s: kind %q is not epic or story", where, n.Kind))
		}
		switch n.Approval {
		case ApprovalApproved, ApprovalDraft, ApprovalUnplanned:
		default:
			problems = append(problems, fmt.Sprintf("%s: approval %q is not approved, draft, or unplanned", where, n.Approval))
		}
		if !changeNamePattern.MatchString(n.Change) {
			problems = append(problems, where+": malformed change")
		}
		if strings.TrimSpace(n.Title) == "" {
			problems = append(problems, where+": empty title")
		}
		if n.Depth < 0 {
			problems = append(problems, where+": negative depth")
		}
		if n.DeliverableKind != nil && *n.DeliverableKind != "story" && *n.DeliverableKind != "enabler" {
			problems = append(problems, fmt.Sprintf("%s: deliverable_kind %q is not story or enabler", where, *n.DeliverableKind))
		}
		if n.Covers == nil || n.AcceptanceCriteria == nil {
			problems = append(problems, where+": covers and acceptance_criteria must be lists")
		}
		problems = append(problems, g.checkVersion2(where, n)...)
	}
	for i, e := range g.Parents {
		for _, key := range []string{e.Child, e.Parent} {
			if _, ok := kinds[key]; !ok {
				problems = append(problems, fmt.Sprintf("parents[%d]: %q is not a node in the document", i, key))
			}
		}
	}
	for i, e := range g.Blocks {
		for _, key := range []string{e.Blocker, e.Blocked} {
			if _, ok := kinds[key]; !ok {
				problems = append(problems, fmt.Sprintf("blocks[%d]: %q is not a node in the document", i, key))
			}
		}
	}
	for _, c := range g.UnplannedChanges {
		if !changeNamePattern.MatchString(c) {
			problems = append(problems, fmt.Sprintf("unplanned_changes: malformed change %q", c))
		}
	}
	if g.Nodes == nil || g.Parents == nil || g.Blocks == nil || g.UnplannedChanges == nil {
		problems = append(problems, "nodes, parents, blocks, and unplanned_changes must be lists")
	}
	return problems
}
