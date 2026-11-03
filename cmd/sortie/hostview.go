package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/environment/smolvm"
	"github.com/sortie-ai/sortie/internal/hostview"
	"github.com/sortie-ai/sortie/internal/server"
)

// newHostView returns the host GET /api/v1/state serves for the
// environment in force: none under remote, the machines smolvm lists
// under smolvm, and the host alone otherwise.
func newHostView(cfg func() config.ServiceConfig, logger *slog.Logger) server.HostFunc {
	name, err := os.Hostname()
	if err != nil {
		name = "localhost"
	}
	src := hostview.New(hostview.Options{
		Name: name,
		Mode: func() hostview.Mode { return hostMode(cfg().Environment.Kind) },
		Capacity: func() *hostview.Size {
			c := cfg()
			return hostview.ReadCapacity(hostDiskPath(c.Environment.Kind, c.Workspace.Root))
		},
		List: func(ctx context.Context) ([]hostview.Listed, error) {
			return listSmolvm(ctx, smolvm.NewCLI(cfg().Environment.Smolvm.Binary))
		},
		Logger: logger,
	})
	return src.Host
}

func hostMode(kind string) hostview.Mode {
	switch kind {
	case config.EnvironmentKindRemote:
		return hostview.ModeElsewhere
	case config.EnvironmentKindSmolvm:
		return hostview.ModeMachines
	}
	return hostview.ModeHost
}

// hostDiskPath is the directory whose file system the host's disk is
// read from: where smolvm keeps its machines under smolvm, when it
// exists, and the workspace root otherwise.
func hostDiskPath(kind, workspaceRoot string) string {
	if kind == config.EnvironmentKindSmolvm {
		if cache, err := os.UserCacheDir(); err == nil {
			dir := filepath.Join(cache, "smolvm")
			if info, err := os.Stat(dir); err == nil && info.IsDir() {
				return dir
			}
		}
	}
	return workspaceRoot
}

func listSmolvm(ctx context.Context, cli *smolvm.CLI) ([]hostview.Listed, error) {
	machines, err := cli.Inventory(ctx)
	if err != nil {
		return nil, err
	}
	listed := make([]hostview.Listed, 0, len(machines))
	for _, m := range machines {
		listed = append(listed, hostview.Listed{Name: m.Name, State: m.State, Labels: m.Labels, CPUs: m.CPUs,
			MemoryMiB: m.MemoryMiB, StorageGiB: m.StorageGiB, Created: m.Created})
	}
	return listed, nil
}
