package server

import (
	"time"

	"github.com/sortie-ai/sortie/internal/orchestrator"
	"github.com/sortie-ai/sortie/internal/redact"
)

// deployedHostResponse is the commit one host deployed a merge in.
type deployedHostResponse struct {
	Host   string `json:"host"`
	Commit string `json:"commit"`
}

// mergeAndProveEntryResponse is one story in merge-and-prove as
// GET /api/v1/state serves it. Deadline is null in the queue and while
// merging; MergeCommit until the merge; RevertPullRequest until a revert
// is opened.
type mergeAndProveEntryResponse struct {
	IssueID           string                 `json:"issue_id"`
	IssueIdentifier   string                 `json:"issue_identifier"`
	Position          int                    `json:"position"`
	Phase             string                 `json:"phase"`
	PullRequest       string                 `json:"pull_request"`
	Head              string                 `json:"head"`
	MergeCommit       *string                `json:"merge_commit"`
	Deployed          []deployedHostResponse `json:"deployed"`
	QueuedAt          time.Time              `json:"queued_at"`
	PhaseAt           time.Time              `json:"phase_at"`
	Deadline          *time.Time             `json:"deadline"`
	RevertPullRequest *string                `json:"revert_pull_request"`
	Reason            *string                `json:"reason"`
	Waiting           *string                `json:"waiting"`
}

// mergeAndProveResponse is merge_and_prove on GET /api/v1/state: the story
// in flight, null when none, and the queue behind it in order.
type mergeAndProveResponse struct {
	InFlight *mergeAndProveEntryResponse  `json:"in_flight"`
	Queue    []mergeAndProveEntryResponse `json:"queue"`
}

func toMergeAndProveEntryResponse(e orchestrator.SnapshotMergeAndProveEntry) mergeAndProveEntryResponse {
	r := mergeAndProveEntryResponse{
		IssueID:           e.IssueID,
		IssueIdentifier:   e.Identifier,
		Position:          e.Position,
		Phase:             e.Phase,
		PullRequest:       redact.Mask(e.PullRequest),
		Head:              e.Head,
		MergeCommit:       nonEmpty(e.MergeCommit),
		Deployed:          make([]deployedHostResponse, 0, len(e.Deployed)),
		QueuedAt:          e.QueuedAt.UTC(),
		PhaseAt:           e.PhaseAt.UTC(),
		RevertPullRequest: nonEmpty(redact.Mask(e.RevertPR)),
		Reason:            nonEmpty(redact.Mask(e.Reason)),
		Waiting:           nonEmpty(redact.Mask(e.Waiting)),
	}
	for _, d := range e.Deployed {
		r.Deployed = append(r.Deployed, deployedHostResponse{Host: d.Host, Commit: d.Commit})
	}
	if !e.Deadline.IsZero() {
		d := e.Deadline.UTC()
		r.Deadline = &d
	}
	return r
}

func toMergeAndProveResponse(snap orchestrator.SnapshotMergeAndProve) mergeAndProveResponse {
	resp := mergeAndProveResponse{Queue: make([]mergeAndProveEntryResponse, 0, len(snap.Queue))}
	if snap.InFlight != nil {
		v := toMergeAndProveEntryResponse(*snap.InFlight)
		resp.InFlight = &v
	}
	for _, e := range snap.Queue {
		resp.Queue = append(resp.Queue, toMergeAndProveEntryResponse(e))
	}
	return resp
}
