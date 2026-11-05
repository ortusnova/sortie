package server

import (
	"time"

	"github.com/sortie-ai/sortie/internal/orchestrator"
	"github.com/sortie-ai/sortie/internal/redact"
)

// offLoopCallResponse is one call the off-loop runner runs: its subject,
// such as "story:<issue id>", what it does, and when it started.
type offLoopCallResponse struct {
	Subject   string    `json:"subject"`
	Kind      string    `json:"kind"`
	StartedAt time.Time `json:"started_at"`
}

// offLoopTimeoutResponse is one call that outlived its timeout.
type offLoopTimeoutResponse struct {
	Subject    string    `json:"subject"`
	Kind       string    `json:"kind"`
	StartedAt  time.Time `json:"started_at"`
	TimedOutAt time.Time `json:"timed_out_at"`
}

// offLoopResponse is off_loop on GET /api/v1/state: the runner of the
// forge, tracker, git and command calls Sortie makes off its event loop.
type offLoopResponse struct {
	Workers     int                      `json:"workers"`
	InFlight    []offLoopCallResponse    `json:"in_flight"`
	QueueLength int                      `json:"queue_length"`
	Timeouts    []offLoopTimeoutResponse `json:"timeouts"`
}

func toOffLoopResponse(snap orchestrator.SnapshotOffLoop) offLoopResponse {
	resp := offLoopResponse{
		Workers:     snap.Workers,
		InFlight:    make([]offLoopCallResponse, 0, len(snap.InFlight)),
		QueueLength: snap.QueueLength,
		Timeouts:    make([]offLoopTimeoutResponse, 0, len(snap.Timeouts)),
	}
	for _, c := range snap.InFlight {
		resp.InFlight = append(resp.InFlight, offLoopCallResponse{Subject: redact.Mask(c.Subject), Kind: c.Kind, StartedAt: c.StartedAt.UTC()})
	}
	for _, c := range snap.Timeouts {
		resp.Timeouts = append(resp.Timeouts, offLoopTimeoutResponse{
			Subject: redact.Mask(c.Subject), Kind: c.Kind, StartedAt: c.StartedAt.UTC(), TimedOutAt: c.TimedOutAt.UTC(),
		})
	}
	return resp
}
