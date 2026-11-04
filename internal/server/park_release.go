package server

import (
	"strconv"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/orchestrator"
	"github.com/sortie-ai/sortie/internal/redact"
)

// parkRecordResponse names how a park was released: its gesture and, for
// the gesture "record", the pull request record's head and comments.
type parkRecordResponse struct {
	Gesture  string   `json:"gesture"`
	Head     string   `json:"head,omitempty"`
	Comments []string `json:"comments,omitempty"`
}

// parkReleaseResponse is one row of park_releases on GET /api/v1/state: a
// released park its issue's next primary dispatch has not yet consumed.
// PullRequest is present for the gesture "record".
type parkReleaseResponse struct {
	IssueID           string               `json:"issue_id"`
	IssueIdentifier   string               `json:"issue_identifier"`
	DisplayIdentifier string               `json:"display_identifier,omitempty"`
	Reason            string               `json:"reason"`
	ParkedAt          time.Time            `json:"parked_at"`
	ReleasedAt        time.Time            `json:"released_at"`
	Release           parkRecordResponse   `json:"release"`
	PullRequest       *pullRequestResponse `json:"pull_request"`
}

// toParkReleaseResponses converts the snapshot's park releases to their
// wire format, masking the repository at serve time.
func toParkReleaseResponses(rows []orchestrator.SnapshotParkRelease) []parkReleaseResponse {
	out := make([]parkReleaseResponse, 0, len(rows))
	for _, r := range rows {
		row := parkReleaseResponse{
			IssueID:           r.IssueID,
			IssueIdentifier:   r.Identifier,
			DisplayIdentifier: r.DisplayID,
			Reason:            r.Reason,
			ParkedAt:          r.ParkedAt.UTC(),
			ReleasedAt:        r.ReleasedAt.UTC(),
			Release:           parkRecordResponse{Gesture: r.Gesture, Head: r.Head, Comments: r.Comments},
		}
		if repo, number, ok := splitPullRequest(r.PullRequest); ok {
			row.PullRequest = &pullRequestResponse{Number: number, Repository: redact.Mask(repo), HeadSHA: r.Head}
		}
		out = append(out, row)
	}
	return out
}

// splitPullRequest parses owner/repo#number.
func splitPullRequest(s string) (repo string, number int, ok bool) {
	repo, num, found := strings.Cut(s, "#")
	if !found || repo == "" {
		return "", 0, false
	}
	number, err := strconv.Atoi(num)
	if err != nil || number <= 0 {
		return "", 0, false
	}
	return repo, number, true
}

// splitComments splits a stored comma-separated comment list; empty yields
// nil.
func splitComments(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}
