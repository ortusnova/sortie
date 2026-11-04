package server

import (
	"slices"
	"time"

	"github.com/sortie-ai/sortie/internal/orchestrator"
)

// handoffPartResponse is an open part of a story a session could not do,
// as /api/v1/state serves it: what it needs, how to check it, and the
// work Sortie turned it into.
type handoffPartResponse struct {
	IssueID         string    `json:"issue_id"`
	IssueIdentifier string    `json:"issue_identifier"`
	ID              string    `json:"id"`
	Kind            string    `json:"kind"`
	Need            string    `json:"need"`
	Check           string    `json:"check"`
	Capabilities    []string  `json:"capabilities"`
	Repository      string    `json:"repository"`
	Status          string    `json:"status"`
	JobAttempt      int       `json:"job_attempt"`
	WorkIdentifier  string    `json:"work_identifier"`
	PlatformFaultID string    `json:"platform_fault_id"`
	Result          string    `json:"result"`
	RecordedAt      time.Time `json:"recorded_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

func toHandoffPartResponses(parts []orchestrator.SnapshotHandoffPart) []handoffPartResponse {
	out := make([]handoffPartResponse, 0, len(parts))
	for _, p := range parts {
		caps := slices.Clone(p.Capabilities)
		if caps == nil {
			caps = []string{}
		}
		out = append(out, handoffPartResponse{
			IssueID: p.IssueID, IssueIdentifier: p.Identifier, ID: p.ID, Kind: p.Kind,
			Need: p.Need, Check: p.Check, Capabilities: caps, Repository: p.Repository,
			Status: p.Status, JobAttempt: p.JobAttempt, WorkIdentifier: p.WorkIdentifier,
			PlatformFaultID: p.FaultID, Result: p.Result,
			RecordedAt: p.RecordedAt.UTC(), UpdatedAt: p.UpdatedAt.UTC(),
		})
	}
	return out
}
