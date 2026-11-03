package main

import (
	"log/slog"
	"os"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/hostview"
	"github.com/sortie-ai/sortie/internal/server"
)

// newHostView returns the host GET /api/v1/state serves for the
// environment in force: none under remote, and the host alone otherwise.
func newHostView(cfg func() config.ServiceConfig, logger *slog.Logger) server.HostFunc {
	name, err := os.Hostname()
	if err != nil {
		name = "localhost"
	}
	src := hostview.New(hostview.Options{
		Name: name,
		Mode: func() hostview.Mode { return hostMode(cfg().Environment.Kind) },
		Capacity: func() *hostview.Size {
			return hostview.ReadCapacity(cfg().Workspace.Root)
		},
		Logger: logger,
	})
	return src.Host
}

func hostMode(kind string) hostview.Mode {
	if kind == config.EnvironmentKindRemote {
		return hostview.ModeElsewhere
	}
	return hostview.ModeHost
}
