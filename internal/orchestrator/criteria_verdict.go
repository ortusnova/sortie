package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/workspace"
	"github.com/sortie-ai/sortie/internal/workspacekit"
)

// CriteriaFile is the file in the workspace's .sortie directory where a
// criteria review records its verdict on each acceptance criterion:
//
//	{"criteria": [{"criterion": "...", "verdict": "met", "evidence": "..."}]}
//
// A verdict is one of the Criterion* values.
const CriteriaFile = "criteria.json"

// MaxCriteriaFile bounds the criteria file a run may leave.
const MaxCriteriaFile = 64 << 10

// Per-criterion verdicts a review may record.
const (
	// CriterionMet: the branch meets the criterion, and the evidence
	// names the file, test or command that shows it.
	CriterionMet = "met"
	// CriterionNotMet: the branch does not meet it.
	CriterionNotMet = "not-met"
	// CriterionLiveProof: only a live run the review's machine cannot do
	// settles it.
	CriterionLiveProof = "live-proof"
	// CriterionUnsettled: the review cannot settle it, such as a conflict
	// with a design rule or a question only a person can answer.
	CriterionUnsettled = "unsettled"
)

// Outcomes of a review's verdict as a whole.
const (
	// CriteriaOutcomeMet: every criterion is met with evidence; auto-merge
	// may merge the pull request.
	CriteriaOutcomeMet = "met"
	// CriteriaOutcomeLiveProof: every criterion is met except those only a
	// live proof settles. Auto-merge does not merge; the supervisor
	// requests the live proof and merges once it is proven.
	CriteriaOutcomeLiveProof = "live-proof"
	// CriteriaOutcomeRefused: a criterion is not met or unsettled, or the
	// review left no usable verdict. The story parks with the question.
	CriteriaOutcomeRefused = "refused"
)

// CriterionReasonAfterDeploy is the reason of a live-proof criterion that
// only a run against the deployed main settles, after the merge: Sortie
// merges such a story through merge-and-prove, proves it after the deploy,
// or reverts it.
const CriterionReasonAfterDeploy = "after-deploy"

// CriterionVerdict is one criterion's entry in the criteria file. Reason
// says why a live-proof criterion needs a live run; [CriterionReasonAfterDeploy]
// marks one only a deploy settles.
type CriterionVerdict struct {
	Criterion string `json:"criterion"`
	Verdict   string `json:"verdict"`
	Evidence  string `json:"evidence"`
	Reason    string `json:"reason,omitempty"`
}

// afterDeploy reports whether c is a live-proof criterion that only a run
// after the deploy settles.
func (c CriterionVerdict) afterDeploy() bool {
	return strings.EqualFold(strings.TrimSpace(c.Verdict), CriterionLiveProof) &&
		strings.EqualFold(strings.TrimSpace(c.Reason), CriterionReasonAfterDeploy)
}

// CriteriaVerdict is a review's verdict as a whole. Problem says what is
// wrong with the file itself, empty when it was usable.
type CriteriaVerdict struct {
	Outcome  string
	Criteria []CriterionVerdict
	Problem  string
}

// readCriteriaFile returns the criteria file the run left, nil when it
// left none. A file Sortie cannot read counts as none.
func readCriteriaFile(workspacePath string, logger *slog.Logger) []byte {
	data, err := workspacekit.ReadSortieFile(workspacePath, CriteriaFile, MaxCriteriaFile)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			logger.Warn("criteria file unreadable", slog.Any("error", err))
		}
		return nil
	}
	return data
}

// criteriaRecorded reports whether the run left a criteria file.
func criteriaRecorded(workspacePath string) bool {
	data, err := workspacekit.ReadSortieFile(workspacePath, CriteriaFile, MaxCriteriaFile)
	return err == nil && len(bytes.TrimSpace(data)) > 0
}

// clearCriteriaFile removes a criteria file an earlier run left, so a
// run's verdict is never an earlier run's.
func clearCriteriaFile(workspacePath string, logger *slog.Logger) {
	if err := workspacekit.RemoveSortieFile(workspacePath, CriteriaFile); err != nil && !errors.Is(err, fs.ErrNotExist) {
		logger.Warn("stale criteria file not removed", slog.Any("error", err))
	}
}

// evaluateCriteria judges the criteria file data. Only a file whose every
// criterion is met with evidence is met.
func evaluateCriteria(data []byte) CriteriaVerdict {
	if len(bytes.TrimSpace(data)) == 0 {
		return CriteriaVerdict{Outcome: CriteriaOutcomeRefused, Problem: "the review recorded no verdict per criterion in .sortie/" + CriteriaFile}
	}
	var file struct {
		Criteria []CriterionVerdict `json:"criteria"`
	}
	if err := json.Unmarshal(data, &file); err != nil {
		return CriteriaVerdict{Outcome: CriteriaOutcomeRefused, Problem: "the review's .sortie/" + CriteriaFile + " is not valid JSON: " + err.Error()}
	}
	v := CriteriaVerdict{Criteria: file.Criteria}
	if len(file.Criteria) == 0 {
		v.Outcome, v.Problem = CriteriaOutcomeRefused, "the review's .sortie/"+CriteriaFile+" lists no criterion"
		return v
	}
	v.Outcome = CriteriaOutcomeMet
	for i, c := range file.Criteria {
		switch strings.ToLower(strings.TrimSpace(c.Verdict)) {
		case CriterionMet:
			if strings.TrimSpace(c.Evidence) == "" {
				v.Outcome = CriteriaOutcomeRefused
				v.Problem = fmt.Sprintf("criterion %d is met with no evidence", i+1)
			}
		case CriterionLiveProof:
			if v.Outcome == CriteriaOutcomeMet {
				v.Outcome = CriteriaOutcomeLiveProof
			}
		case CriterionNotMet, CriterionUnsettled:
			v.Outcome = CriteriaOutcomeRefused
		default:
			v.Outcome = CriteriaOutcomeRefused
			v.Problem = fmt.Sprintf("criterion %d has the verdict %q, not one of %s, %s, %s, %s", i+1, c.Verdict, CriterionMet, CriterionNotMet, CriterionLiveProof, CriterionUnsettled)
		}
	}
	return v
}

// onlyAfterDeploy reports whether every criterion the verdict does not
// find met is a live-proof one only a deploy settles, and at least one is.
func (v CriteriaVerdict) onlyAfterDeploy() bool {
	_, _, ok := v.afterDeployCriteria()
	return ok
}

// afterDeployCriteria returns, when every criterion is met with evidence
// or left to a proof after deploy and at least one is, the criteria left
// to that proof, one per line. Otherwise it returns the first criterion
// that keeps the story from merge-and-prove, numbered from 1, and false.
func (v CriteriaVerdict) afterDeployCriteria() (left string, blocking int, ok bool) {
	var b strings.Builder
	for i, c := range v.Criteria {
		switch {
		case strings.EqualFold(strings.TrimSpace(c.Verdict), CriterionMet) && strings.TrimSpace(c.Evidence) != "":
		case c.afterDeploy():
			fmt.Fprintf(&b, "- %s\n", verdictLine(c.Criterion))
		default:
			return "", i + 1, false
		}
	}
	if b.Len() == 0 {
		return "", 0, false
	}
	return b.String(), 0, true
}

// notMet lists the criteria whose verdict is not met, one per line.
func (v CriteriaVerdict) notMet() string {
	var b strings.Builder
	for i, c := range v.Criteria {
		verdict := strings.ToLower(strings.TrimSpace(c.Verdict))
		if verdict == CriterionMet && strings.TrimSpace(c.Evidence) != "" {
			continue
		}
		if reason := verdictLine(c.Reason); reason != "" && verdict == CriterionLiveProof {
			verdict += ", " + reason
		}
		fmt.Fprintf(&b, "- criterion %d (%s): %s", i+1, verdict, verdictLine(c.Criterion))
		if e := verdictLine(c.Evidence); e != "" {
			fmt.Fprintf(&b, ": %s", e)
		}
		b.WriteString("\n")
	}
	return b.String()
}

func verdictLine(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 400 {
		s = s[:400] + "…"
	}
	return s
}

// CriteriaVerdictRecorder is the optional store capability that keeps
// each story's latest criteria verdict.
type CriteriaVerdictRecorder interface {
	UpsertCriteriaVerdict(ctx context.Context, v persistence.CriteriaVerdict) error
}

// CriteriaVerdictLoader is the optional store capability that reads the
// verdict [CriteriaVerdictRecorder] keeps.
type CriteriaVerdictLoader interface {
	LoadCriteriaVerdict(ctx context.Context, issueID string) (persistence.CriteriaVerdict, bool, error)
}

// isCriteriaReview reports whether the run is a story's criteria review
// under plan control: a run of a gate: criteria rule that no reaction
// started. A reaction run on the same rule (a fix, a live proof, the
// click-proofs on a preview) keeps the review's verdict.
func isCriteriaReview(params HandleWorkerExitParams, entry *RunningEntry) bool {
	if params.PlanReleaseState == "" || entry == nil || entry.ReactionKind != "" {
		return false
	}
	rule, ok := params.Dispatch.RuleByName(entry.RuleName)
	return ok && rule.Gate == config.GateCriteria
}

// applyCriteriaVerdict judges a criteria review's verdict when it ended
// normally and records it as the story's latest. A review that stopped
// with a status records a refusal. A refused verdict becomes a blocked
// stop whose statement is the question for the supervisor, so the story
// parks and its pull request is not merged. A verdict that waits for a
// live proof leaves the pull request open for one and says so on the
// story.
func applyCriteriaVerdict(ctx context.Context, params HandleWorkerExitParams, entry *RunningEntry, result *WorkerResult, log *slog.Logger) {
	if result.ExitKind != WorkerExitNormal || !isCriteriaReview(params, entry) {
		return
	}
	var v CriteriaVerdict
	if result.SoftStop {
		v = CriteriaVerdict{Outcome: CriteriaOutcomeRefused, Problem: "the review stopped " + result.SoftStopReason}
	} else {
		v = evaluateCriteria(result.CriteriaFile)
	}
	recordCriteriaVerdict(ctx, params.Store, result.IssueID, entry.DispatchID, result.CriteriaFile, v, log)
	if result.SoftStop {
		return
	}
	switch v.Outcome {
	case CriteriaOutcomeMet:
		log.Info("criteria review passed: every criterion is met")
	case CriteriaOutcomeLiveProof:
		log.Info("criteria review waits for a live proof; auto-merge waits for its record at the head")
		if params.TrackerAdapter != nil {
			comment := "Sortie does not merge this story's pull request yet: its review settles these criteria only by a live proof:\n\n" +
				v.notMet() + "\nRequest the live proof (POST /api/v1/" + entry.Identifier + "/live-proof); once a comment on the pull request records it proven at its head (`" + LiveProofMarker + " <head sha>`), Sortie merges it once its CI passes and it does not conflict."
			if v.onlyAfterDeploy() {
				comment = "Sortie does not merge this story's pull request yet: its review settles these criteria only after a deploy:\n\n" +
					v.notMet() + "\nMerge it through merge-and-prove (POST /api/v1/" + entry.Identifier + "/merge-and-prove) once its CI passes: Sortie merges it, waits for every host to deploy it, runs the live proof against the deployed main, and reverts the merge when no proven record lands by the deadline."
			}
			if err := params.TrackerAdapter.CommentIssue(ctx, result.IssueID, comment); err != nil {
				log.Warn("live-proof hold comment failed", slog.Any("error", err))
			}
		}
	default:
		log.Info("criteria review did not pass; the story parks", slog.String("problem", v.Problem))
		result.SoftStop = true
		result.SoftStopReason = string(workspace.StatusBlocked)
		result.SoftStopStatement = workspace.StatusStatement{Text: criteriaQuestion(v)}
	}
}

// criteriaQuestion is the parked story's question for the supervisor.
func criteriaQuestion(v CriteriaVerdict) string {
	var b strings.Builder
	b.WriteString("The criteria review did not find every acceptance criterion met, so Sortie does not merge the pull request.\n")
	if v.Problem != "" {
		b.WriteString(v.Problem)
		b.WriteString("\n")
	}
	if list := v.notMet(); list != "" {
		b.WriteString("Criteria not met:\n")
		b.WriteString(list)
	}
	b.WriteString("Supervisor: decide how each is met (a fix, a live proof, or a change to the criterion), then release the story for a new review.")
	return b.String()
}

func recordCriteriaVerdict(ctx context.Context, store any, issueID, dispatchID string, raw []byte, v CriteriaVerdict, log *slog.Logger) {
	rec, ok := store.(CriteriaVerdictRecorder)
	if !ok {
		return
	}
	criteria := ""
	if len(v.Criteria) > 0 {
		if b, err := json.Marshal(v.Criteria); err == nil {
			criteria = string(b)
		}
	} else if len(raw) > 0 {
		criteria = string(raw)
	}
	if err := rec.UpsertCriteriaVerdict(ctx, persistence.CriteriaVerdict{
		IssueID: issueID, DispatchID: dispatchID, Outcome: v.Outcome, Criteria: criteria, Problem: v.Problem, RecordedAt: time.Now(),
	}); err != nil {
		log.Warn("criteria verdict record failed", slog.Any("error", err))
	}
}

// criteriaMergeAllowed reports whether the story's latest criteria verdict
// lets auto-merge merge its pull request, and why not. A verdict that
// leaves criteria to a live proof lets it merge only once a pull request
// comment records them proven at its head: liveProof reports that the
// merge waits for that record too.
func criteriaMergeAllowed(ctx context.Context, store any, issueID string) (allowed, liveProof bool, why string, err error) {
	loader, ok := store.(CriteriaVerdictLoader)
	if !ok {
		return false, false, "no criteria verdict store", nil
	}
	v, found, err := loader.LoadCriteriaVerdict(ctx, issueID)
	if err != nil {
		return false, false, "", err
	}
	switch {
	case !found:
		return false, false, "the story's review recorded no verdict per criterion", nil
	case v.Outcome == CriteriaOutcomeLiveProof:
		return true, true, "", nil
	case v.Outcome != CriteriaOutcomeMet:
		why := "the story's review found criteria not met (" + v.Outcome + ")"
		if v.Problem != "" {
			why += ": " + v.Problem
		}
		return false, false, why, nil
	}
	return true, false, "", nil
}
