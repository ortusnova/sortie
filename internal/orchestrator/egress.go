package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/egress"
	"github.com/sortie-ai/sortie/internal/gitsync"
	"github.com/sortie-ai/sortie/internal/worker"
)

// egressReadBound bounds the fetch and read of one repository's egress
// policy on the event loop.
const egressReadBound = 60 * time.Second

// egressCommentBound bounds the tracker comment of an egress hold.
const egressCommentBound = 30 * time.Second

// EgressHold is why a remote job is not offered: its repository's egress
// policy, at the named commit of its base branch, is invalid.
type EgressHold struct {
	Repository string `json:"repository"`
	Branch     string `json:"branch"`
	Commit     string `json:"commit"`
	File       string `json:"file"`
	// Entry names the invalid key or entry, such as hosts[0].host; empty
	// when the file as a whole is invalid.
	Entry string `json:"entry,omitempty"`
	// Error says why the entry or the file is invalid.
	Error string `json:"error"`
}

// Text is the hold's reason in one line: the repository, the commit, the
// file and the error.
func (h *EgressHold) Text() string {
	where := h.File
	if h.Entry != "" {
		where += " " + h.Entry
	}
	return fmt.Sprintf("egress policy invalid: repository %s at %s commit %s, %s: %s", h.Repository, h.Branch, h.Commit, where, h.Error)
}

func cloneEgressHold(h *EgressHold) *EgressHold {
	if h == nil {
		return nil
	}
	c := *h
	return &c
}

// egressRead is a repository's egress policy as read at a commit of its
// base branch. invalid is set, and policy empty, when the file there is
// invalid.
type egressRead struct {
	source  worker.EgressSource
	policy  egress.Policy
	invalid *EgressHold
}

// EgressReader fetches repo's upstream and reads its egress policy at
// the tip of its base branch. An error means the policy could not be
// read; an invalid file is a read with its hold set.
type EgressReader func(ctx context.Context, repo config.Repository) (egressRead, error)

// repositoryLabel names repo in an offer and a hold: its name, else its
// project. It is never the URL, which may hold a credential and never
// reaches a worker.
func repositoryLabel(repo config.Repository) string {
	return coalesce(repo.Name, repo.Project)
}

// egressFromFile checks the egress file f read from repo. A file too
// large, or one that fails the format, is an invalid read; any other
// readErr is returned.
func egressFromFile(repo config.Repository, f gitsync.BaseFile, readErr error) (egressRead, error) {
	read := egressRead{
		source: worker.EgressSource{Repository: repositoryLabel(repo), Branch: coalesce(f.Branch, repo.Base), Commit: f.Commit, File: egress.File},
		policy: egress.Empty(),
	}
	hold := func(entry, message string) (egressRead, error) {
		read.invalid = &EgressHold{Repository: read.source.Repository, Branch: read.source.Branch, Commit: f.Commit,
			File: egress.File, Entry: entry, Error: message}
		return read, nil
	}
	switch {
	case errors.Is(readErr, gitsync.ErrFileTooLarge):
		return hold("", fmt.Sprintf("the file is over the limit of %d bytes", egress.MaxFileBytes))
	case readErr != nil && f.Found && f.Commit != "":
		return hold("", readErr.Error())
	case readErr != nil:
		return egressRead{}, readErr
	case !f.Found:
		return read, nil
	}
	policy, err := egress.Parse(f.Data)
	if err != nil {
		if perr, ok := errors.AsType[*egress.Error](err); ok {
			return hold(perr.Entry, perr.Message)
		}
		return hold("", err.Error())
	}
	read.policy = policy
	return read, nil
}

// readBaseEgress is the default [EgressReader]: the repository's mirror
// under the state directory, fetched and read at the tip of its base
// branch.
func readBaseEgress(stateDir string, logger *slog.Logger) EgressReader {
	return func(ctx context.Context, repo config.Repository) (egressRead, error) {
		syncer, err := gitsync.New(gitsync.Params{Repository: repo, StateDir: stateDir, Logger: logger})
		if err != nil {
			return egressRead{}, err
		}
		rctx, cancel := context.WithTimeout(ctx, egressReadBound)
		defer cancel()
		f, err := syncer.ReadBaseFile(rctx, egress.File, egress.MaxFileBytes)
		return egressFromFile(repo, f, err)
	}
}

// jobEgress is the egress of a job under rc before its repository's
// policy is read: the harness's policy, the hosts of the credential
// bindings, and an empty repository policy.
func jobEgress(rc config.RemoteConfig) worker.Egress {
	hosts := []string{}
	for _, c := range rc.Credentials {
		for _, h := range c.Hosts {
			if !slices.Contains(hosts, h) {
				hosts = append(hosts, h)
			}
		}
	}
	return worker.Egress{Harness: rc.Egress.Clone(), Repository: egress.Empty(), CredentialHosts: hosts}
}

// egressPass reads each repository's egress policy once per dispatch
// pass, however many candidates run in it.
type egressPass struct {
	read  EgressReader
	reads map[string]egressPassRead
}

type egressPassRead struct {
	read egressRead
	err  error
}

func newEgressPass(read EgressReader) *egressPass {
	return &egressPass{read: read, reads: map[string]egressPassRead{}}
}

func (p *egressPass) get(ctx context.Context, repo config.Repository) (egressRead, error) {
	key := repo.URL + "\x00" + repo.Base
	if r, ok := p.reads[key]; ok {
		return r.read, r.err
	}
	r, err := p.read(ctx, repo)
	p.reads[key] = egressPassRead{read: r, err: err}
	return r, err
}

// egressReader returns the reader of repositories' egress policies.
func (o *Orchestrator) egressReader() EgressReader {
	if o.readEgress != nil {
		return o.readEgress
	}
	stateDir := ""
	if o.dbPath != "" {
		stateDir = filepath.Dir(o.dbPath)
	}
	return readBaseEgress(stateDir, o.logger)
}

// egressHold returns the hold an invalid egress policy of issue's
// repository gives its remote job, and whether the policy was read: a
// valid read releases an earlier hold, and a failed read neither holds
// nor releases, the job's seed failing on the same fetch. Nil when cfg
// selects another kind or the issue runs in no repository.
func (o *Orchestrator) egressHold(ctx context.Context, cfg config.ServiceConfig, issue domain.Issue, pass *egressPass) (*EgressHold, bool) {
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
	read, err := pass.get(ctx, *repo)
	if err != nil {
		o.logger.Warn("egress policy not read; the job's seed fetches the repository again",
			slog.String("issue_identifier", issue.Identifier), slog.String("repository", repositoryLabel(*repo)), slog.Any("error", err))
		return nil, false
	}
	return read.invalid, true
}

// holdForEgress applies the result of [Orchestrator.egressHold] to the
// issue's notice: a hold is commented on the issue once, until a valid
// read releases it.
func (o *Orchestrator) holdForEgress(ctx context.Context, issue domain.Issue, hold *EgressHold, read bool) {
	if !read {
		return
	}
	if hold == nil {
		if _, noticed := o.state.EgressHoldNoticed[issue.ID]; noticed {
			delete(o.state.EgressHoldNoticed, issue.ID)
			o.logger.Info("egress policy valid again; the job is offered",
				slog.String("issue_identifier", issue.Identifier))
		}
		return
	}
	if _, noticed := o.state.EgressHoldNoticed[issue.ID]; noticed {
		return
	}
	if o.state.EgressHoldNoticed == nil {
		o.state.EgressHoldNoticed = map[string]string{}
	}
	o.state.EgressHoldNoticed[issue.ID] = hold.Commit
	o.logger.Warn("remote job held by its repository's egress policy",
		slog.String("issue_identifier", issue.Identifier), slog.String("reason", hold.Text()))
	if o.trackerAdapter == nil {
		return
	}
	tracker, issueID, body := o.trackerAdapter, issue.ID, egressHoldComment(hold)
	log := o.logger
	o.state.TrackerOpsWg.Go(func() {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), egressCommentBound)
		defer cancel()
		if err := tracker.CommentIssue(cctx, issueID, body); err != nil {
			log.Warn("egress hold comment failed", slog.String("issue_id", issueID), slog.Any("error", err))
		}
	})
}

// egressHoldComment is the tracker comment of an egress hold.
func egressHoldComment(h *EgressHold) string {
	return fmt.Sprintf("Sortie holds this issue: %s.\n\nNo session starts and no attempt is used. "+
		"Sortie reads %s again at each poll and offers the job once the %s branch holds a valid file.", h.Text(), h.File, h.Branch)
}
