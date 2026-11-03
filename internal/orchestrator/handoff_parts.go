package orchestrator

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/failcause"
	"github.com/sortie-ai/sortie/internal/logging"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/redact"
	"github.com/sortie-ai/sortie/internal/worker"
	"github.com/sortie-ai/sortie/internal/workspacekit"
)

const (
	// handoffPartsFile is the control file in which a session records
	// each part of its story it could not do.
	handoffPartsFile = "handoff.json"

	// handoffPartsMaxBytes caps how much of the hand-off file Sortie
	// reads. A larger file is ignored whole.
	handoffPartsMaxBytes = 64 << 10

	// handoffMaxParts caps the parts taken from one hand-off file.
	handoffMaxParts = 16

	// handoffMaxText caps need, check, action and where, in characters.
	handoffMaxText = 4096

	// handoffTrackerBound bounds the tracker calls of one hand-off pass.
	handoffTrackerBound = 30 * time.Second
)

// Hand-off part kinds: what Sortie turns the part into.
const (
	// HandoffKindJob is a check that needs a host or capability the
	// session's machine lacks; it runs as a check job on a worker that
	// declares the capabilities.
	HandoffKindJob = "job"
	// HandoffKindPullRequest is a change in another repository; it is
	// filed as an issue in that repository, whose run opens the pull
	// request.
	HandoffKindPullRequest = "pull_request"
	// HandoffKindPerson is a step only a person can take, such as a
	// permission; it is a platform fault with that person action.
	HandoffKindPerson = "person"
)

// Hand-off part statuses.
const (
	// HandoffPending is a part Sortie has not turned into work yet.
	HandoffPending = "pending"
	// HandoffWorking is a part a check job or a filed issue carries.
	HandoffWorking = "working"
	// HandoffFault is a part a platform fault holds for a person.
	HandoffFault = "fault"
	// HandoffDone is a closed part.
	HandoffDone = persistence.HandoffPartDone
)

// Platform fault classes of hand-off parts.
const (
	PlatformFaultClassHandoffPerson      = "handoff_person"
	PlatformFaultClassHandoffUnplaced    = "handoff_unplaced"
	PlatformFaultClassHandoffCheckFailed = "handoff_check_failed"
	PlatformFaultClassHandoffChange      = "handoff_change"
)

// handoffFaultStep is the step of a hand-off part's platform fault.
const handoffFaultStep = "handoff"

// waitHandoffParts is the wait of a merge or merge-completion reaction
// whose story still has an open hand-off part.
const waitHandoffParts = "handoff_parts"

var handoffPartIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

// HandoffPartSpec is one part of a story as the session recorded it.
type HandoffPartSpec struct {
	ID           string
	Kind         string
	Need         string
	Check        string
	Capabilities []string
	Repository   string
	Patch        string
	Action       string
	Where        string
}

// HandoffPart is one recorded part of a story and the work Sortie turned
// it into. Only the event loop mutates it.
type HandoffPart struct {
	HandoffPartSpec

	IssueID    string
	Identifier string

	Status         string
	JobAttempt     int
	WorkIssueID    string
	WorkIdentifier string
	FaultID        string
	Result         string

	RecordedAt time.Time
	UpdatedAt  time.Time
	DoneAt     time.Time
}

// handoffPartKey keys a part in [State.HandoffParts].
func handoffPartKey(issueID, partID string) string { return issueID + "\x00" + partID }

// open reports whether the part keeps its story out of Done.
func (p *HandoffPart) open() bool { return p.Status != HandoffDone }

// readHandoffParts reads the parts the session recorded in the
// workspace's .sortie/handoff.json. The file is agent-authored: it is
// read with the link refusal of every control file and at most 64 KiB,
// and a part that is malformed is dropped with a warn record while the
// others are kept. An absent file has no parts and logs nothing.
func readHandoffParts(workspacePath string, logger *slog.Logger) []HandoffPartSpec {
	if workspacePath == "" {
		return nil
	}
	data, err := workspacekit.ReadSortieFile(workspacePath, handoffPartsFile, handoffPartsMaxBytes)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			logger.Warn("ignoring the hand-off in .sortie/handoff.json",
				slog.String("workspace", workspacePath), slog.Any("error", err))
		}
		return nil
	}
	specs, problems, err := parseHandoffParts(data)
	if err != nil {
		logger.Warn("ignoring the malformed hand-off in .sortie/handoff.json",
			slog.String("workspace", workspacePath), slog.Any("error", err))
		return nil
	}
	for _, problem := range problems {
		logger.Warn("ignoring a part of the hand-off in .sortie/handoff.json",
			slog.String("workspace", workspacePath), slog.String("reason", problem))
	}
	return specs
}

// parseHandoffParts parses a hand-off file. It returns the valid parts,
// why each other part was dropped, and an error when the file is not a
// JSON object with a parts list.
func parseHandoffParts(data []byte) ([]HandoffPartSpec, []string, error) {
	var file struct {
		Parts *[]json.RawMessage `json:"parts"`
	}
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, nil, err
	}
	if file.Parts == nil {
		return nil, nil, errors.New("want a JSON object with a parts list")
	}
	var (
		specs    []HandoffPartSpec
		problems []string
	)
	for i, raw := range *file.Parts {
		if len(specs) == handoffMaxParts {
			problems = append(problems, fmt.Sprintf("parts after the first %d are ignored", handoffMaxParts))
			break
		}
		spec, err := parseHandoffPart(raw)
		if err == nil && slices.ContainsFunc(specs, func(s HandoffPartSpec) bool { return s.ID == spec.ID }) {
			err = fmt.Errorf("id %q is used twice", spec.ID)
		}
		if err != nil {
			problems = append(problems, fmt.Sprintf("part %d: %v", i, err))
			continue
		}
		specs = append(specs, spec)
	}
	return specs, problems, nil
}

func parseHandoffPart(raw json.RawMessage) (HandoffPartSpec, error) {
	var part struct {
		ID           string   `json:"id"`
		Kind         string   `json:"kind"`
		Need         string   `json:"need"`
		Check        string   `json:"check"`
		Capabilities []string `json:"capabilities"`
		Repository   string   `json:"repository"`
		Patch        string   `json:"patch"`
		Action       string   `json:"action"`
		Where        string   `json:"where"`
	}
	if err := json.Unmarshal(raw, &part); err != nil {
		return HandoffPartSpec{}, errors.New("not an object of string members")
	}
	spec := HandoffPartSpec{
		ID:         strings.ToLower(strings.TrimSpace(part.ID)),
		Kind:       strings.TrimSpace(part.Kind),
		Need:       handoffText(part.Need),
		Check:      handoffText(part.Check),
		Repository: strings.TrimSpace(stripControl(part.Repository)),
		Patch:      stripControl(part.Patch),
		Action:     handoffText(part.Action),
		Where:      handoffText(part.Where),
	}
	for _, c := range part.Capabilities {
		if c = strings.TrimSpace(stripControl(c)); c != "" && !slices.Contains(spec.Capabilities, c) {
			spec.Capabilities = append(spec.Capabilities, c)
		}
	}
	if spec.Kind == "" {
		switch {
		case spec.Repository != "":
			spec.Kind = HandoffKindPullRequest
		case len(spec.Capabilities) > 0:
			spec.Kind = HandoffKindJob
		default:
			spec.Kind = HandoffKindPerson
		}
	}
	switch {
	case spec.Kind != HandoffKindJob && spec.Kind != HandoffKindPullRequest && spec.Kind != HandoffKindPerson:
		return HandoffPartSpec{}, fmt.Errorf("kind %q is not job, pull_request or person", spec.Kind)
	case spec.Need == "":
		return HandoffPartSpec{}, errors.New("need is empty")
	case spec.Check == "":
		return HandoffPartSpec{}, errors.New("check is empty")
	case spec.Kind == HandoffKindPullRequest && spec.Repository == "":
		return HandoffPartSpec{}, errors.New("a pull_request part names no repository")
	}
	if spec.ID == "" {
		sum := sha256.Sum256([]byte(spec.Kind + "\x00" + spec.Need))
		spec.ID = "part-" + hex.EncodeToString(sum[:4])
	}
	if !handoffPartIDPattern.MatchString(spec.ID) {
		return HandoffPartSpec{}, fmt.Errorf("id %q is not [a-z0-9][a-z0-9._-]{0,63}", spec.ID)
	}
	return spec, nil
}

// handoffText cleans one text member: control characters other than
// newline and tab removed, trimmed, and cut to [handoffMaxText].
func handoffText(s string) string {
	s = strings.TrimSpace(stripControl(s))
	if utf8.RuneCountInString(s) > handoffMaxText {
		s = strings.TrimSpace(string([]rune(s)[:handoffMaxText]))
	}
	return s
}

// handoffPartStore persists hand-off parts.
type handoffPartStore interface {
	SaveHandoffPart(ctx context.Context, part persistence.HandoffPart) error
}

// saveHandoffPart persists p when the store keeps parts. A failed write is
// logged: the runtime state stays authoritative for this process.
func (o *Orchestrator) saveHandoffPart(ctx context.Context, p *HandoffPart) {
	store, ok := o.store.(handoffPartStore)
	if !ok {
		return
	}
	if err := store.SaveHandoffPart(ctx, handoffPartRow(p)); err != nil {
		logging.WithIssue(o.logger, p.IssueID, p.Identifier).Error("failed to persist hand-off part",
			slog.String("handoff_part", p.ID), slog.Any("error", err))
	}
}

func handoffPartRow(p *HandoffPart) persistence.HandoffPart {
	return persistence.HandoffPart{
		IssueID: p.IssueID, PartID: p.ID, Identifier: p.Identifier, Kind: p.Kind, Need: p.Need, Check: p.Check,
		Capabilities: slices.Clone(p.Capabilities), Repository: p.Repository, Patch: p.Patch,
		Action: p.Action, ActionWhere: p.Where, Status: p.Status, JobAttempt: p.JobAttempt,
		WorkIssueID: p.WorkIssueID, WorkIdentifier: p.WorkIdentifier, FaultID: p.FaultID, Result: p.Result,
		RecordedAt: formatFaultTime(p.RecordedAt), UpdatedAt: formatFaultTime(p.UpdatedAt), DoneAt: formatFaultTime(p.DoneAt),
	}
}

// PopulateHandoffParts restores the recorded parts into state at
// startup. A check job does not outlive the process that queued it, so a
// part a job carried is pending again and its check runs again.
func PopulateHandoffParts(state *State, rows []persistence.HandoffPart, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	for _, row := range rows {
		if row.IssueID == "" || row.PartID == "" {
			log.Warn("skipping malformed hand-off part record")
			continue
		}
		p := &HandoffPart{
			HandoffPartSpec: HandoffPartSpec{
				ID: row.PartID, Kind: row.Kind, Need: row.Need, Check: row.Check,
				Capabilities: row.Capabilities, Repository: row.Repository, Patch: row.Patch,
				Action: row.Action, Where: row.ActionWhere,
			},
			IssueID: row.IssueID, Identifier: row.Identifier, Status: row.Status, JobAttempt: row.JobAttempt,
			WorkIssueID: row.WorkIssueID, WorkIdentifier: row.WorkIdentifier, FaultID: row.FaultID, Result: row.Result,
			RecordedAt: parseFaultTime(row.RecordedAt), UpdatedAt: parseFaultTime(row.UpdatedAt), DoneAt: parseFaultTime(row.DoneAt),
		}
		if p.Kind == HandoffKindJob && p.Status == HandoffWorking {
			p.Status = HandoffPending
		}
		state.HandoffParts[handoffPartKey(p.IssueID, p.ID)] = p
	}
}

// openHandoffParts returns the open parts of the issue, oldest first.
func openHandoffParts(state *State, issueID string) []*HandoffPart {
	var out []*HandoffPart
	for _, p := range state.HandoffParts {
		if p.IssueID == issueID && p.open() {
			out = append(out, p)
		}
	}
	sortHandoffParts(out)
	return out
}

// handoffOpen reports whether the issue has a part that keeps it out of
// Done.
func handoffOpen(state *State, issueID string) bool {
	for _, p := range state.HandoffParts {
		if p.IssueID == issueID && p.open() {
			return true
		}
	}
	return false
}

func sortHandoffParts(parts []*HandoffPart) {
	slices.SortFunc(parts, func(a, b *HandoffPart) int {
		if c := a.RecordedAt.Compare(b.RecordedAt); c != 0 {
			return c
		}
		return cmp.Or(cmp.Compare(a.IssueID, b.IssueID), cmp.Compare(a.ID, b.ID))
	})
}

// takeHandoffParts records the parts a run's session recorded and starts
// turning them into work. A part whose ID the story already has, open or
// done, is the same part again and changes nothing. Must run on the event
// loop.
func (o *Orchestrator) takeHandoffParts(ctx context.Context, result WorkerResult) {
	if len(result.HandoffParts) == 0 {
		return
	}
	now := time.Now().UTC()
	log := logging.WithIssue(o.logger, result.IssueID, result.Identifier)
	added := false
	for _, spec := range result.HandoffParts {
		key := handoffPartKey(result.IssueID, spec.ID)
		if _, known := o.state.HandoffParts[key]; known {
			continue
		}
		p := &HandoffPart{
			HandoffPartSpec: spec, IssueID: result.IssueID, Identifier: result.Identifier,
			Status: HandoffPending, RecordedAt: now, UpdatedAt: now,
		}
		o.state.HandoffParts[key] = p
		o.saveHandoffPart(ctx, p)
		added = true
		log.Info("hand-off part recorded",
			slog.String("handoff_part", p.ID), slog.String("handoff_kind", p.Kind))
	}
	if added {
		o.advanceHandoffParts(ctx, now)
	}
}

// advanceHandoffParts turns every pending part into work, starts again a
// job part whose refusal a worker can now lift, and closes each part
// whose filed issue reached a terminal state. It runs on every poll tick
// and when a run records parts. Must run on the event loop.
func (o *Orchestrator) advanceHandoffParts(ctx context.Context, now time.Time) {
	if len(o.state.HandoffParts) == 0 || o.workflowManager == nil {
		return
	}
	cfg := o.workflowManager.Config()
	parts := make([]*HandoffPart, 0, len(o.state.HandoffParts))
	for _, p := range o.state.HandoffParts {
		if p.open() {
			parts = append(parts, p)
		}
	}
	sortHandoffParts(parts)
	changed := false
	for _, p := range parts {
		if p.Status == HandoffFault && p.Kind == HandoffKindJob && o.liftHandoffRefusal(ctx, cfg, p, now) {
			changed = true
		}
		if p.Status == HandoffPending {
			o.startHandoffPart(ctx, cfg, p, now)
			changed = true
		}
	}
	if o.closeFiledHandoffParts(ctx, cfg, parts, now) {
		changed = true
	}
	if changed {
		o.notifyObservers()
	}
}

// startHandoffPart turns a pending part into work: a check job on a
// worker that declares its capabilities, an issue in the repository it
// changes, or a platform fault with a person action when neither can
// carry it. A part whose work could not start is left pending for the
// next pass.
func (o *Orchestrator) startHandoffPart(ctx context.Context, cfg config.ServiceConfig, p *HandoffPart, now time.Time) {
	switch p.Kind {
	case HandoffKindJob:
		matcher := o.Workers()
		requires := o.handoffJobRequires(cfg, p)
		switch {
		case matcher == nil:
			o.raiseHandoffFault(ctx, p, PlatformFaultClassHandoffUnplaced,
				"Sortie runs no workers, so no worker can run the check "+p.ID+" of "+p.Identifier+" needs.",
				handoffRunAction(p), now)
		case !matcher.Capable(requires.Capabilities):
			o.raiseHandoffFault(ctx, p, PlatformFaultClassHandoffUnplaced,
				"No checked-in worker declares "+strings.Join(requires.Capabilities, ", ")+", which the check "+p.ID+" of "+p.Identifier+" needs.",
				handoffRunAction(p), now)
		default:
			o.submitHandoffJob(ctx, cfg, matcher, p, requires, now)
		}
	case HandoffKindPullRequest:
		o.fileHandoffChange(ctx, cfg, p, now)
	default:
		action := p.Action
		if action == "" {
			action = p.Need
		}
		where := p.Where
		if where == "" {
			where = p.Identifier
		}
		o.raiseHandoffFault(ctx, p, PlatformFaultClassHandoffPerson,
			"A session of "+p.Identifier+" could not do part "+p.ID+": it needs a person.",
			failcause.PersonAction{Action: action + ". Then check: " + p.Check, Where: where}, now)
	}
}

// handoffRunAction is the person action of a check no worker can run.
func handoffRunAction(p *HandoffPart) failcause.PersonAction {
	return failcause.PersonAction{
		Action: "Run the check of " + p.Identifier + " from a host that has " + strings.Join(p.Capabilities, ", ") +
			", and mark this done when it passes, or check in a worker that declares them: " + p.Check,
		Where: "a worker or host with " + strings.Join(p.Capabilities, ", "),
	}
}

// handoffJobRequires returns what a worker must declare to run p's check:
// what a job of the story's repository requires, plus p's capabilities.
func (o *Orchestrator) handoffJobRequires(cfg config.ServiceConfig, p *HandoffPart) worker.Requires {
	env := cfg.Environment
	if issueEnv, _, err := issueRepository(env, remoteWorkspacePath(cfg, domain.Issue{Identifier: p.Identifier}), nil); err == nil {
		env = issueEnv
	}
	requires := remoteRequires(env.Remote)
	for _, c := range p.Capabilities {
		if !slices.Contains(requires.Capabilities, c) {
			requires.Capabilities = append(requires.Capabilities, c)
		}
	}
	return requires
}

// handoffJobID is the matcher job ID of p's check.
func handoffJobID(p *HandoffPart) string { return "handoff-" + p.Identifier + "-" + p.ID }

// submitHandoffJob queues p's check as a check job and waits, off the
// event loop, for its outcome.
func (o *Orchestrator) submitHandoffJob(ctx context.Context, cfg config.ServiceConfig, matcher *worker.Matcher, p *HandoffPart, requires worker.Requires, now time.Time) {
	env := cfg.Environment
	if issueEnv, _, err := issueRepository(env, remoteWorkspacePath(cfg, domain.Issue{Identifier: p.Identifier}), nil); err == nil {
		env = issueEnv
	}
	rc := env.Remote
	p.JobAttempt++
	job := worker.Job{
		ID:       handoffJobID(p),
		Attempt:  p.JobAttempt,
		Kind:     worker.JobKindCheck,
		Issue:    worker.Issue{ID: p.IssueID, Identifier: p.Identifier, Title: p.Need},
		Requires: requires,
		Run: worker.Run{
			Backend: rc.Backend,
			Image:   rc.JobImage(rc.Repository),
			Command: "sh",
			Args:    []string{"-c", p.Check},
			Env: map[string]string{
				envSessionIssueID:         p.IssueID,
				envSessionIssueIdentifier: p.Identifier,
				"SORTIE_HANDOFF_PART":     p.ID,
			},
			StallTimeoutMS: cfg.Agent.StallTimeoutMS,
		},
	}
	ended := matcher.Submit(job)
	p.Status, p.FaultID, p.Result, p.UpdatedAt = HandoffWorking, "", "", now
	o.saveHandoffPart(ctx, p)
	logging.WithIssue(o.logger, p.IssueID, p.Identifier).Info("hand-off check queued for a worker",
		slog.String("handoff_part", p.ID), slog.String("job", job.ID), slog.Int("attempt", job.Attempt),
		slog.Any("capabilities", requires.Capabilities))
	key, attempt := handoffPartKey(p.IssueID, p.ID), p.JobAttempt
	o.state.TrackerOpsWg.Go(func() {
		var out worker.Outcome
		select {
		case out = <-ended:
		case <-ctx.Done():
			return
		case <-o.stopping:
			return
		}
		o.runOnLoop(ctx, func(loopCtx context.Context) {
			o.applyHandoffJobOutcome(loopCtx, key, attempt, out, time.Now().UTC())
		})
	})
}

// applyHandoffJobOutcome closes the part whose check passed on a worker.
// A check that failed, or whose worker was lost, is a platform fault the
// story waits on. Must run on the event loop.
func (o *Orchestrator) applyHandoffJobOutcome(ctx context.Context, key string, attempt int, out worker.Outcome, now time.Time) {
	p := o.state.HandoffParts[key]
	if p == nil || p.Status != HandoffWorking || p.JobAttempt != attempt {
		return
	}
	log := logging.WithIssue(o.logger, p.IssueID, p.Identifier)
	if out.Succeeded {
		reason := "the check passed on a worker"
		if r := strings.TrimSpace(out.Reason); r != "" {
			reason += ": " + r
		}
		o.closeHandoffPart(ctx, p, reason, now)
		log.Info("hand-off check passed on a worker", slog.String("handoff_part", p.ID))
		return
	}
	reason := outcomeReason(out)
	log.Warn("hand-off check did not pass on a worker", slog.String("handoff_part", p.ID), slog.String("reason", reason))
	o.raiseHandoffFault(ctx, p, PlatformFaultClassHandoffCheckFailed,
		"The check "+p.ID+" of "+p.Identifier+" did not pass on a worker: "+reason,
		failcause.PersonAction{
			Action: "Find why the check of " + p.Identifier + " does not pass, make it pass, and mark this done: " + p.Check,
			Where:  "a worker or host with " + strings.Join(p.Capabilities, ", "),
		}, now)
	o.notifyObservers()
}

// closeHandoffPart closes p with result.
func (o *Orchestrator) closeHandoffPart(ctx context.Context, p *HandoffPart, result string, now time.Time) {
	p.Status, p.Result, p.FaultID, p.UpdatedAt, p.DoneAt = HandoffDone, redact.Mask(result), "", now, now
	o.saveHandoffPart(ctx, p)
	o.notifyObservers()
}

// handoffRepository returns the configured repository that p names, by
// name or by its owner/repo project.
func handoffRepository(cfg config.ServiceConfig, named string) (config.SmolvmRepository, bool) {
	for _, r := range cfg.Environment.RepositorySet().AllRepositories() {
		if (r.Name != "" && strings.EqualFold(r.Name, named)) || (r.Project != "" && strings.EqualFold(r.Project, named)) {
			return r, true
		}
	}
	return config.SmolvmRepository{}, false
}

// fileHandoffChange files p, a change in another repository, as an issue
// that runs in that repository, so a run of the same loop opens its pull
// request there. It starts in the release state under plan control, else
// in the first active state, so it is dispatched. When the tracker cannot
// create issues or the workflow opens no such repository, the change is a
// platform fault for a person to open.
func (o *Orchestrator) fileHandoffChange(ctx context.Context, cfg config.ServiceConfig, p *HandoffPart, now time.Time) {
	log := logging.WithIssue(o.logger, p.IssueID, p.Identifier).With(slog.String("handoff_part", p.ID))
	creator, canCreate := o.trackerAdapter.(domain.IssueCreator)
	repo, listed := handoffRepository(cfg, p.Repository)
	if !canCreate || !listed {
		why := "the tracker cannot create issues"
		if canCreate {
			why = "the workflow opens no repository " + p.Repository
		}
		o.raiseHandoffFault(ctx, p, PlatformFaultClassHandoffChange,
			"A session of "+p.Identifier+" needs a change in "+p.Repository+" that Sortie cannot open: "+why+".",
			failcause.PersonAction{
				Action: "Open the change " + p.Identifier + " needs as a pull request to " + p.Repository + " and merge it, then mark this done: " + p.Need,
				Where:  p.Repository,
			}, now)
		return
	}
	state := cfg.Tracker.ReleaseState
	if state == "" && len(cfg.Tracker.ActiveStates) > 0 {
		state = cfg.Tracker.ActiveStates[0]
	}
	tctx, cancel := context.WithTimeout(ctx, handoffTrackerBound)
	defer cancel()
	created, err := creator.CreateIssue(tctx, domain.NewIssue{
		Title:   cleanPullRequestTitle(p.Identifier + ": " + firstLine(p.Need)),
		Body:    handoffChangeBody(p),
		Kind:    domain.IssueKindStory,
		PlanKey: "handoff-" + p.Identifier + "-" + p.ID,
		State:   state,
	})
	if err != nil {
		p.Result, p.UpdatedAt = redact.Mask("the tracker refused the issue: "+err.Error()), now
		o.saveHandoffPart(ctx, p)
		log.Warn("hand-off change could not be filed, trying again on the next poll", slog.Any("error", err))
		return
	}
	if repo.Name != "" && cfg.Environment.RepositorySet().ListsRepositories() {
		if err := o.trackerAdapter.AddLabel(tctx, created.ID, config.RepositoryLabelPrefix+repo.Name); err != nil {
			log.Warn("hand-off change filed without its repository label", slog.String("work_issue", created.Identifier), slog.Any("error", err))
		}
	}
	p.Status, p.WorkIssueID, p.WorkIdentifier, p.FaultID, p.Result, p.UpdatedAt = HandoffWorking, created.ID, created.Identifier, "", "", now
	o.saveHandoffPart(ctx, p)
	log.Info("hand-off change filed as an issue in its repository",
		slog.String("repository", p.Repository), slog.String("work_issue", created.Identifier))
}

// handoffChangeBody is the description of the issue a change is filed as.
func handoffChangeBody(p *HandoffPart) string {
	var b strings.Builder
	fmt.Fprintf(&b, "A session of %s could not make this change in %s, and recorded it as part `%s` of its hand-off. %s does not merge until this issue is done.\n\n", p.Identifier, p.Repository, p.ID, p.Identifier)
	fmt.Fprintf(&b, "## What it needs\n\n%s\n\n## How to check it\n\n%s\n", p.Need, p.Check)
	if strings.TrimSpace(p.Patch) != "" {
		patch := strings.TrimRight(p.Patch, "\n")
		fence := "```"
		for strings.Contains(patch, fence) {
			fence += "`"
		}
		fmt.Fprintf(&b, "\n## The patch\n\nApply it, adjust it to the repository as it is now, and open it as a pull request.\n\n%sdiff\n%s\n%s\n", fence, patch, fence)
	}
	return b.String()
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	return line
}

// closeFiledHandoffParts closes each working pull_request part whose
// filed issue reached a terminal state, which a merged pull request in
// its repository moves it to. A failed read changes nothing. It reports
// whether a part closed.
func (o *Orchestrator) closeFiledHandoffParts(ctx context.Context, cfg config.ServiceConfig, parts []*HandoffPart, now time.Time) bool {
	var ids []string
	for _, p := range parts {
		if p.Kind == HandoffKindPullRequest && p.Status == HandoffWorking && p.WorkIssueID != "" && !slices.Contains(ids, p.WorkIssueID) {
			ids = append(ids, p.WorkIssueID)
		}
	}
	if len(ids) == 0 || o.trackerAdapter == nil {
		return false
	}
	tctx, cancel := context.WithTimeout(ctx, handoffTrackerBound)
	defer cancel()
	states, err := o.trackerAdapter.FetchIssueStatesByIDs(tctx, ids)
	if err != nil {
		o.logger.Warn("hand-off issue state read failed, reading again on the next poll", slog.Any("error", err))
		return false
	}
	closed := false
	for _, p := range parts {
		if p.Kind != HandoffKindPullRequest || p.Status != HandoffWorking {
			continue
		}
		state, ok := states[p.WorkIssueID]
		if !ok || !isTerminalState(state, cfg.Tracker.TerminalStates) {
			continue
		}
		o.closeHandoffPart(ctx, p, p.WorkIdentifier+" reached "+state, now)
		closed = true
		logging.WithIssue(o.logger, p.IssueID, p.Identifier).Info("hand-off change done",
			slog.String("handoff_part", p.ID), slog.String("work_issue", p.WorkIdentifier), slog.String("state", state))
	}
	return closed
}

// handoffFaultSignature is the signature of p's platform fault: one fault
// per part.
func handoffFaultSignature(p *HandoffPart) string {
	return "handoff: " + p.Identifier + "/" + p.ID
}

// raiseHandoffFault records p's platform fault with its person action and
// holds the story under it, so the story waits for the person. A story
// with a running worker is left pending until its run ends, since the
// hold would take its claim.
func (o *Orchestrator) raiseHandoffFault(ctx context.Context, p *HandoffPart, class, cause string, action failcause.PersonAction, now time.Time) {
	if _, running := o.state.Running[p.IssueID]; running {
		p.Status = HandoffPending
		return
	}
	signature := handoffFaultSignature(p)
	var fault *PlatformFault
	for _, f := range o.state.PlatformFaults {
		if f.Signature == signature {
			fault = f
		}
	}
	if fault == nil {
		detail := "Needs: " + p.Need + "\nCheck: " + p.Check
		if p.Repository != "" {
			detail += "\nRepository: " + p.Repository
		}
		fault = &PlatformFault{
			ID:          platformFaultID(signature, now),
			Signature:   signature,
			Class:       class,
			Step:        handoffFaultStep,
			Cause:       redact.Mask(cause),
			Detail:      redact.Mask(detail),
			Action:      &failcause.PersonAction{Action: redact.Mask(action.Action), Where: redact.Mask(action.Where)},
			RecordedAt:  now,
			NextCheckAt: now.Add(PlatformFaultCheckInterval),
		}
		o.state.PlatformFaults[fault.ID] = fault
	}
	log := logging.WithIssue(o.logger, p.IssueID, p.Identifier).With(slog.String("handoff_part", p.ID), slog.String("platform_fault_id", fault.ID))
	holdUnderPlatformFault(ctx, o.state, o.store, fault, PlatformFaultHold{IssueID: p.IssueID, Identifier: p.Identifier, HeldAt: now}, log)
	p.Status, p.FaultID, p.Result, p.UpdatedAt = HandoffFault, fault.ID, redact.Mask(cause), now
	o.saveHandoffPart(ctx, p)
	log.Info("hand-off part waits on a person", slog.String("fault_class", class))
}

// liftHandoffRefusal resolves the refusal fault of a job part once a
// checked-in worker declares what its check needs, and makes the part
// pending again so its check is queued. It reports whether it did.
func (o *Orchestrator) liftHandoffRefusal(ctx context.Context, cfg config.ServiceConfig, p *HandoffPart, now time.Time) bool {
	f := o.state.PlatformFaults[p.FaultID]
	if f == nil || f.Class != PlatformFaultClassHandoffUnplaced || !f.ActionDoneAt.IsZero() {
		return false
	}
	matcher := o.Workers()
	if matcher == nil || !matcher.Capable(o.handoffJobRequires(cfg, p).Capabilities) {
		return false
	}
	if _, resolved := applyFaultCheck(ctx, o.state, o.store, f.ID, nil, now, o.logger); !resolved {
		return false
	}
	p.Status, p.FaultID, p.UpdatedAt = HandoffPending, "", now
	o.saveHandoffPart(ctx, p)
	logging.WithIssue(o.logger, p.IssueID, p.Identifier).Info("a worker can now run the hand-off check",
		slog.String("handoff_part", p.ID))
	return true
}

// handoffFaultResolved closes every part the resolved fault held: a
// person marked its action done, so the part's work is done. Must run on
// the event loop.
func (o *Orchestrator) handoffFaultResolved(ctx context.Context, f *PlatformFault, now time.Time) {
	for _, p := range o.state.HandoffParts {
		if p.FaultID != f.ID || p.Status != HandoffFault {
			continue
		}
		by := f.ActionDoneBy
		if by == "" {
			by = "a person"
		}
		o.closeHandoffPart(ctx, p, "done by "+by, now)
		logging.WithIssue(o.logger, p.IssueID, p.Identifier).Info("hand-off part done by a person",
			slog.String("handoff_part", p.ID), slog.String("platform_fault_id", f.ID))
	}
}

// SnapshotHandoffPart is a value copy of a recorded hand-off part for the
// state API, every text masked.
type SnapshotHandoffPart struct {
	IssueID        string
	Identifier     string
	ID             string
	Kind           string
	Need           string
	Check          string
	Capabilities   []string
	Repository     string
	Status         string
	JobAttempt     int
	WorkIdentifier string
	FaultID        string
	Result         string
	RecordedAt     time.Time
	UpdatedAt      time.Time
}

// snapshotHandoffParts returns a masked copy of every open part, oldest
// first; an empty slice when there is none.
func snapshotHandoffParts(state *State) []SnapshotHandoffPart {
	parts := make([]*HandoffPart, 0, len(state.HandoffParts))
	for _, p := range state.HandoffParts {
		if p.open() {
			parts = append(parts, p)
		}
	}
	sortHandoffParts(parts)
	out := make([]SnapshotHandoffPart, 0, len(parts))
	for _, p := range parts {
		out = append(out, SnapshotHandoffPart{
			IssueID: p.IssueID, Identifier: redact.Mask(p.Identifier), ID: p.ID, Kind: p.Kind,
			Need: redact.Mask(p.Need), Check: redact.Mask(p.Check), Capabilities: slices.Clone(p.Capabilities),
			Repository: redact.Mask(p.Repository), Status: p.Status, JobAttempt: p.JobAttempt,
			WorkIdentifier: redact.Mask(p.WorkIdentifier), FaultID: p.FaultID, Result: redact.Mask(p.Result),
			RecordedAt: p.RecordedAt, UpdatedAt: p.UpdatedAt,
		})
	}
	return out
}
