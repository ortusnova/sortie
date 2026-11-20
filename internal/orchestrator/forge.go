package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/forge"
	"github.com/sortie-ai/sortie/internal/gitsync"
	"github.com/sortie-ai/sortie/internal/worker"
)

// forgeCommentBound bounds the tracker comment of a forge hold.
const forgeCommentBound = 30 * time.Second

// ForgeHold is why a remote job is not offered: its repository's GitHub
// access declaration, at the named commit of its base branch, is invalid
// or above the workflow's forge ceiling.
type ForgeHold struct {
	Repository string `json:"repository"`
	Branch     string `json:"branch"`
	Commit     string `json:"commit"`
	File       string `json:"file"`
	// Entry names the invalid or refused key or entry, such as
	// access[1].repository; empty when the file as a whole is invalid.
	Entry string `json:"entry,omitempty"`
	// Error says why the entry or the file is refused.
	Error string `json:"error"`
}

// Text is the hold's reason in one line: the repository, the commit, the
// file, the entry and the error.
func (h *ForgeHold) Text() string {
	where := h.File
	if h.Entry != "" {
		where += " " + h.Entry
	}
	return fmt.Sprintf("GitHub access refused: repository %s at %s commit %s, %s: %s", h.Repository, h.Branch, h.Commit, where, h.Error)
}

func cloneForgeHold(h *ForgeHold) *ForgeHold {
	if h == nil {
		return nil
	}
	c := *h
	return &c
}

// forgeRead is a repository's GitHub access declaration as read at a
// commit of its base branch: the file's, or the default a repository
// with no file gets. invalid is set, and the declaration empty, when the
// file there is invalid.
type forgeRead struct {
	source      worker.ForgeSource
	declaration forge.Declaration
	invalid     *ForgeHold
}

// hold returns the hold the read gives a job of repo under ceiling: the
// read's own when the file is invalid, else one naming the first entry
// above the ceiling, else nil. The default access of a repository with
// no file is never above it.
func (r forgeRead) hold(repo config.Repository, ceiling forge.Ceiling) *ForgeHold {
	if r.invalid != nil || !r.source.Declared {
		return r.invalid
	}
	err := ceiling.Check(repo.Label(), r.declaration, config.ForgeCeilingField)
	if err == nil {
		return nil
	}
	h := &ForgeHold{Repository: r.source.Repository, Branch: r.source.Branch, Commit: r.source.Commit, File: r.source.File, Error: err.Error()}
	if perr, ok := errors.AsType[*forge.Error](err); ok {
		h.Entry, h.Error = perr.Entry, perr.Message
	}
	return h
}

// grant is the job's GitHub access, grouped by owner, as the offer
// carries it.
func (r forgeRead) grant() worker.Forge {
	return worker.Forge{Source: r.source, Owners: r.declaration.Grant().Owners}
}

// ForgeReader reads repo's GitHub access declaration at commit, a commit
// of its base branch the repository's mirror already holds. An error
// means the declaration could not be read; an invalid file is a read
// with its hold set.
type ForgeReader func(ctx context.Context, repo config.Repository, commit string) (forgeRead, error)

// forgeFromFile checks the declaration file f read from repo. A file too
// large, or one that fails the format, is an invalid read; no file is
// the default access; any other readErr is returned.
func forgeFromFile(repo config.Repository, f gitsync.BaseFile, readErr error) (forgeRead, error) {
	read := forgeRead{
		source:      worker.ForgeSource{Repository: repositoryLabel(repo), Branch: coalesce(f.Branch, repo.Base), Commit: f.Commit, File: forge.File},
		declaration: forge.Empty(),
	}
	hold := func(entry, message string) (forgeRead, error) {
		read.source.Declared = true
		read.invalid = &ForgeHold{Repository: read.source.Repository, Branch: read.source.Branch, Commit: f.Commit,
			File: forge.File, Entry: entry, Error: message}
		return read, nil
	}
	switch {
	case errors.Is(readErr, gitsync.ErrFileTooLarge):
		return hold("", fmt.Sprintf("the file is over the limit of %d bytes", forge.MaxFileBytes))
	case readErr != nil && f.Found && f.Commit != "":
		return hold("", readErr.Error())
	case readErr != nil:
		return forgeRead{}, readErr
	case !f.Found:
		read.declaration = forge.Default(forge.GitHubRepository(repo.Provider, repo.Project, repo.URL))
		return read, nil
	}
	d, err := forge.Parse(f.Data)
	if err != nil {
		if perr, ok := errors.AsType[*forge.Error](err); ok {
			return hold(perr.Entry, perr.Message)
		}
		return hold("", err.Error())
	}
	read.source.Declared, read.declaration = true, d
	return read, nil
}

// readBaseForge is the default [ForgeReader]: the repository's mirror
// under the state directory, read at the commit without a fetch.
func readBaseForge(stateDir string, logger *slog.Logger) ForgeReader {
	return func(ctx context.Context, repo config.Repository, commit string) (forgeRead, error) {
		syncer, err := gitsync.New(gitsync.Params{Repository: repo, StateDir: stateDir, Logger: logger})
		if err != nil {
			return forgeRead{}, err
		}
		rctx, cancel := context.WithTimeout(ctx, egressReadBound)
		defer cancel()
		f, err := syncer.FileAt(rctx, commit, forge.File, forge.MaxFileBytes)
		return forgeFromFile(repo, f, err)
	}
}

// forgePass reads each repository's declaration once per dispatch pass
// and commit, however many candidates run in it.
type forgePass struct {
	read  ForgeReader
	reads map[string]forgePassRead
}

type forgePassRead struct {
	read forgeRead
	err  error
}

func newForgePass(read ForgeReader) *forgePass {
	return &forgePass{read: read, reads: map[string]forgePassRead{}}
}

func (p *forgePass) get(ctx context.Context, repo config.Repository, commit string) (forgeRead, error) {
	key := repo.URL + "\x00" + repo.Base + "\x00" + commit
	if r, ok := p.reads[key]; ok {
		return r.read, r.err
	}
	r, err := p.read(ctx, repo, commit)
	p.reads[key] = forgePassRead{read: r, err: err}
	return r, err
}

// forgeReader returns the reader of repositories' declarations.
func (o *Orchestrator) forgeReader() ForgeReader {
	if o.readForge != nil {
		return o.readForge
	}
	stateDir := ""
	if o.dbPath != "" {
		stateDir = filepath.Dir(o.dbPath)
	}
	return readBaseForge(stateDir, o.logger)
}

// forgeHold returns the hold the GitHub access declaration of issue's
// repository gives its remote job, and whether the declaration was read.
// It reads the declaration at the base branch commit the pass's egress
// read fetched, so both files come from one commit and one fetch. A
// valid read within the ceiling releases an earlier hold; a failed read,
// of either file, neither holds nor releases, the job's seed failing on
// the same read. Nil when cfg selects another kind or the issue runs in
// no repository.
func (o *Orchestrator) forgeHold(ctx context.Context, cfg config.ServiceConfig, issue domain.Issue, egressReads *egressPass, forgeReads *forgePass) (*ForgeHold, bool) {
	if o.remote == nil || !isRemoteKind(cfg.Environment) {
		return nil, false
	}
	env, _, err := issueRepository(cfg.Environment, remoteWorkspacePath(cfg, issue), issue.Labels)
	if err != nil {
		return nil, false
	}
	repo := env.RepositorySet().Repository
	if repo == nil {
		return nil, false
	}
	base, err := egressReads.get(ctx, *repo)
	if err != nil || base.invalid != nil || base.source.Commit == "" {
		return nil, false
	}
	read, err := forgeReads.get(ctx, *repo, base.source.Commit)
	if err != nil {
		o.logger.Warn("GitHub access declaration not read; the job's seed reads it again",
			slog.String("issue_identifier", issue.Identifier), slog.String("repository", repositoryLabel(*repo)), slog.Any("error", err))
		return nil, false
	}
	return read.hold(*repo, cfg.Environment.Remote.ForgeCeiling), true
}

// holdForForge applies the result of [Orchestrator.forgeHold] to the
// issue's notice: a hold is commented on the issue once, until a valid
// read within the ceiling releases it.
func (o *Orchestrator) holdForForge(ctx context.Context, issue domain.Issue, hold *ForgeHold, read bool) {
	if !read {
		return
	}
	if hold == nil {
		if _, noticed := o.state.ForgeHoldNoticed[issue.ID]; noticed {
			delete(o.state.ForgeHoldNoticed, issue.ID)
			o.logger.Info("GitHub access declaration within the ceiling again; the job is offered",
				slog.String("issue_identifier", issue.Identifier))
		}
		return
	}
	if _, noticed := o.state.ForgeHoldNoticed[issue.ID]; noticed {
		return
	}
	if o.state.ForgeHoldNoticed == nil {
		o.state.ForgeHoldNoticed = map[string]string{}
	}
	o.state.ForgeHoldNoticed[issue.ID] = hold.Commit
	o.logger.Warn("remote job held by its repository's GitHub access declaration",
		slog.String("issue_identifier", issue.Identifier), slog.String("reason", hold.Text()))
	if o.trackerAdapter == nil {
		return
	}
	tracker, issueID, body := o.trackerAdapter, issue.ID, forgeHoldComment(hold)
	log := o.logger
	o.state.TrackerOpsWg.Go(func() {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), forgeCommentBound)
		defer cancel()
		if err := tracker.CommentIssue(cctx, issueID, body); err != nil {
			log.Warn("forge hold comment failed", slog.String("issue_id", issueID), slog.Any("error", err))
		}
	})
}

// forgeHoldComment is the tracker comment of a forge hold.
func forgeHoldComment(h *ForgeHold) string {
	return fmt.Sprintf("Sortie holds this issue: %s.\n\nNo session starts, no attempt is used and no GitHub token is minted. "+
		"Sortie reads %s again at each poll and offers the job once the %s branch holds a valid file within the workflow's %s.",
		h.Text(), h.File, h.Branch, config.ForgeCeilingField)
}
