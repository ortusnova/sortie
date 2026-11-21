package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/forge"
	"github.com/sortie-ai/sortie/internal/forgetoken"
	"github.com/sortie-ai/sortie/internal/gitsync"
	"github.com/sortie-ai/sortie/internal/worker"
)

// sandboxIDPrefix is the prefix of a sandbox set's ID the API hides.
const sandboxIDPrefix = forgetoken.KindSandbox + ":"

// ErrForgeTokensOff is a sandbox request to a Sortie that mints no
// GitHub token: the workflow names no GitHub App.
var ErrForgeTokensOff = errors.New("this Sortie mints no GitHub token: the workflow names no environment.remote.forge_app")

// ErrUnknownRepository is a sandbox request naming a repository the
// workflow does not list.
var ErrUnknownRepository = errors.New("the workflow lists no such repository")

// ForgeRefusal is a sandbox request the repository's declaration
// refuses: invalid, or above the forge ceiling.
type ForgeRefusal struct{ Hold *ForgeHold }

func (r *ForgeRefusal) Error() string { return r.Hold.Text() }

// ForgeSandbox is a sandbox's token set: what the declaration grants,
// where it was read, and the tokens, none when it grants nothing.
type ForgeSandbox struct {
	ID     string
	Source worker.ForgeSource
	Set    forgetoken.Set
}

// MintForgeSandboxFunc returns the function that mints a sandbox's
// GitHub tokens for the repository the workflow lists under name: by its
// .sortie/forge.yaml at the tip of its base branch, fetched now, and
// within its forge ceiling, as a job of that repository would be. It is
// safe for concurrent use and runs off the event loop.
func (o *Orchestrator) MintForgeSandboxFunc() func(ctx context.Context, name string) (ForgeSandbox, error) {
	return func(ctx context.Context, name string) (ForgeSandbox, error) {
		if o.remote == nil || o.remote.forgeTokens == nil {
			return ForgeSandbox{}, ErrForgeTokensOff
		}
		cfg := o.workflowManager.Config()
		repo, ok := remoteRepositoryNamed(cfg.Environment.Remote, name)
		if !ok {
			return ForgeSandbox{}, fmt.Errorf("%w: %q", ErrUnknownRepository, name)
		}
		stateDir := ""
		if o.dbPath != "" {
			stateDir = filepath.Dir(o.dbPath)
		}
		syncer, err := gitsync.New(gitsync.Params{Repository: repo, StateDir: stateDir, Logger: o.logger})
		if err != nil {
			return ForgeSandbox{}, err
		}
		rctx, cancel := context.WithTimeout(ctx, egressReadBound)
		defer cancel()
		f, readErr := syncer.ReadBaseFile(rctx, forge.File, forge.MaxFileBytes)
		read, err := forgeFromFile(repo, f, readErr)
		if err != nil {
			return ForgeSandbox{}, fmt.Errorf("read the repository's %s: %w", forge.File, err)
		}
		if hold := read.hold(repo, cfg.Environment.Remote.ForgeCeiling); hold != nil {
			return ForgeSandbox{}, &ForgeRefusal{Hold: hold}
		}
		grant := read.declaration.Grant()
		out := ForgeSandbox{Source: read.source}
		if len(grant.Owners) == 0 {
			return out, nil
		}
		set, err := o.remote.forgeTokens.MintSandbox(ctx, grant)
		if err != nil {
			return ForgeSandbox{}, err
		}
		out.ID, out.Set = strings.TrimPrefix(set.ID, sandboxIDPrefix), set
		return out, nil
	}
}

// EndForgeSandboxFunc returns the function that revokes the sandbox set
// id, as [MintForgeSandboxFunc] named it. It returns
// [forgetoken.ErrNotFound] for a set that is not recorded or already
// ended.
func (o *Orchestrator) EndForgeSandboxFunc() func(ctx context.Context, id string) error {
	return func(ctx context.Context, id string) error {
		if o.remote == nil || o.remote.forgeTokens == nil {
			return ErrForgeTokensOff
		}
		if id == "" || strings.ContainsAny(id, ":/") {
			return forgetoken.ErrNotFound
		}
		return o.remote.forgeTokens.EndSandbox(ctx, sandboxIDPrefix+id)
	}
}

// remoteRepositoryNamed returns the repository of rc named name, by its
// label.
func remoteRepositoryNamed(rc config.RemoteConfig, name string) (config.Repository, bool) {
	for _, r := range rc.Repositories {
		if r.Label() == name {
			return r, true
		}
	}
	return config.Repository{}, false
}
