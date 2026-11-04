package orchestrator

import (
	"context"
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/gitsync"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/workspace"
	"github.com/sortie-ai/sortie/internal/workspacekit"
)

// forgedPR is the pull request number an agent writes into its own
// .sortie/scm.json to point Sortie's reactions at another pull request.
const forgedPR = 999

// trustedPR is the pull request number in the record Sortie keeps.
const trustedPR = 42

// smolvmRepositoryEnvironment returns an environment block under which
// Sortie pushes the issue branch itself.
func smolvmRepositoryEnvironment() config.EnvironmentConfig {
	return config.EnvironmentConfig{
		Kind: config.EnvironmentKindRemote,
		Remote: config.RemoteConfig{Repository: &config.Repository{
			URL: "https://example.com/org/app.git", Base: "main", BranchPrefix: "sortie/", MaxBundleMiB: 512, Project: "org/app",
		}},
	}
}

// writeForgedSCM writes the agent's .sortie/scm.json in wsPath, naming
// [forgedPR] with a fresh push time.
func writeForgedSCM(t *testing.T, wsPath string, pushedAt string) {
	t.Helper()
	data, err := json.Marshal(domain.SCMMetadata{
		Branch: "evil/branch", SHA: "f0f0f0", PushedAt: pushedAt, PRNumber: forgedPR, Owner: "evil", Repo: "other",
	})
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(wsPath, workspacekit.SortieDir)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "scm.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// trustedSCM is the record Sortie keeps in the tests, naming
// [trustedPR].
func trustedSCM(pushedAt string) domain.SCMMetadata {
	return domain.SCMMetadata{
		Branch: "sortie/PROJ-1", SHA: "c0ffee", PushedAt: pushedAt, PRNumber: trustedPR, Owner: "org", Repo: "app",
	}
}

func writeTrustedSCM(t *testing.T, wsPath string, rec domain.SCMMetadata) {
	t.Helper()
	if err := os.MkdirAll(wsPath, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := workspace.WriteHostSCMRecord(wsPath, rec); err != nil {
		t.Fatalf("WriteHostSCMRecord: %v", err)
	}
}

// reactionPRNumber returns the pull request number a pending reaction
// acts on.
func reactionPRNumber(t *testing.T, pr *PendingReaction) int {
	t.Helper()
	switch d := pr.KindData.(type) {
	case *CIReactionData:
		return d.PRNumber
	case *ReviewReactionData:
		return d.PRNumber
	case *BotReviewReactionData:
		return d.PRNumber
	case *AutoMergeReactionData:
		return d.PRNumber
	case *MergeConflictReactionData:
		return d.PRNumber
	case *LabelReviewReactionData:
		return d.PRNumber
	case *LabelFixReactionData:
		return d.PRNumber
	case *MergeCompletionReactionData:
		return d.PRNumber
	}
	t.Fatalf("unknown reaction data %T", pr.KindData)
	return 0
}

// prBackedReactionKinds are every reaction kind seeded from workspace
// SCM metadata.
var prBackedReactionKinds = []string{
	ReactionKindCI, ReactionKindReview, ReactionKindBotReview, ReactionKindAutoMerge,
	ReactionKindMergeConflict, ReactionKindLabelReview, ReactionKindLabelFix, ReactionKindMergeCompletion,
}

func TestSCMMetadataSource(t *testing.T) {
	t.Parallel()

	withRepository := smolvmRepositoryEnvironment()
	withoutRepository := smolvmRepositoryEnvironment()
	withoutRepository.Remote.Repository = nil
	tests := []struct {
		name       string
		cfg        config.EnvironmentConfig
		withRecord bool
		wantPR     int
	}{
		{name: "default kind reads the agent's file", cfg: config.EnvironmentConfig{}, withRecord: true, wantPR: forgedPR},
		{name: "local kind reads the agent's file", cfg: config.EnvironmentConfig{Kind: config.EnvironmentKindLocal}, withRecord: true, wantPR: forgedPR},
		{name: "smolvm without repository reads the agent's file", cfg: withoutRepository, withRecord: true, wantPR: forgedPR},
		{name: "smolvm with repository reads the record", cfg: withRepository, withRecord: true, wantPR: trustedPR},
		{name: "smolvm with repository and no record reads nothing", cfg: withRepository, wantPR: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ws := t.TempDir()
			writeForgedSCM(t, ws, time.Now().UTC().Format(time.RFC3339))
			if tt.withRecord {
				writeTrustedSCM(t, ws, trustedSCM("2026-10-12T10:00:00Z"))
			}

			got := SCMMetadataSource(tt.cfg).Read(ws, discardLogger())

			if got.PRNumber != tt.wantPR {
				t.Errorf("PRNumber = %d, want %d (metadata %+v)", got.PRNumber, tt.wantPR, got)
			}
			if tt.wantPR == 0 && got != (domain.SCMMetadata{}) {
				t.Errorf("metadata = %+v, want none", got)
			}
		})
	}

	t.Run("nil reader reads the agent's file", func(t *testing.T) {
		t.Parallel()
		ws := t.TempDir()
		writeForgedSCM(t, ws, "")
		var r SCMMetadataReader
		if got := r.Read(ws, discardLogger()); got.PRNumber != forgedPR {
			t.Errorf("PRNumber = %d, want %d", got.PRNumber, forgedPR)
		}
	})
}

// allReactionsExitParams returns exit parameters with every PR-backed
// reaction kind configured and SCM metadata read through cfg's source.
func allReactionsExitParams(t *testing.T, cfg config.EnvironmentConfig) HandleWorkerExitParams {
	t.Helper()
	params := defaultExitParams(t, &mockExitStore{})
	params.CIProvider = &ciProviderStubExit{}
	params.SCMAdapter = &scmAdapterStubExit{}
	params.SCMMetadata = SCMMetadataSource(cfg)
	params.AutoMergeReactionConfigured = true
	params.ReviewReactionConfigured = true
	params.BotReviewReactionConfigured = true
	params.MergeConflictReactionConfigured = true
	params.LabelReviewReactionConfigured = true
	params.LabelFixReactionConfigured = true
	params.MergeCompletionReactionConfigured = true
	return params
}

func TestHandleWorkerExit_ReactionsIgnoreAForgedSCMFileUnderARepository(t *testing.T) {
	t.Parallel()

	t.Run("every reaction acts on the recorded pull request", func(t *testing.T) {
		t.Parallel()
		ws := t.TempDir()
		writeForgedSCM(t, ws, "")
		writeTrustedSCM(t, ws, trustedSCM(""))
		state := exitState(t, "SCM-1", nil)

		HandleWorkerExit(state, WorkerResult{
			IssueID: "SCM-1", Identifier: "SCM-1-ident", ExitKind: WorkerExitNormal, AgentAdapter: "mock", WorkspacePath: ws,
		}, allReactionsExitParams(t, smolvmRepositoryEnvironment()))
		t.Cleanup(func() { CancelRetry(state, "SCM-1") })

		for _, kind := range prBackedReactionKinds {
			pr, ok := state.PendingReactions[ReactionKey("SCM-1", kind)]
			if !ok {
				t.Errorf("%s reaction missing, want one for pull request %d", kind, trustedPR)
				continue
			}
			if got := reactionPRNumber(t, pr); got != trustedPR {
				t.Errorf("%s reaction pull request = %d, want %d", kind, got, trustedPR)
			}
		}
	})

	t.Run("without a record no reaction is seeded", func(t *testing.T) {
		t.Parallel()
		ws := t.TempDir()
		writeForgedSCM(t, ws, "")
		state := exitState(t, "SCM-2", nil)

		HandleWorkerExit(state, WorkerResult{
			IssueID: "SCM-2", Identifier: "SCM-2-ident", ExitKind: WorkerExitNormal, AgentAdapter: "mock", WorkspacePath: ws,
		}, allReactionsExitParams(t, smolvmRepositoryEnvironment()))
		t.Cleanup(func() { CancelRetry(state, "SCM-2") })

		if len(state.PendingReactions) != 0 {
			t.Errorf("pending reactions = %v, want none from the agent's file", slices.Collect(maps.Keys(state.PendingReactions)))
		}
	})

	t.Run("local kind still reads the agent's file", func(t *testing.T) {
		t.Parallel()
		ws := t.TempDir()
		writeForgedSCM(t, ws, "")
		writeTrustedSCM(t, ws, trustedSCM(""))
		state := exitState(t, "SCM-3", nil)

		HandleWorkerExit(state, WorkerResult{
			IssueID: "SCM-3", Identifier: "SCM-3-ident", ExitKind: WorkerExitNormal, AgentAdapter: "mock", WorkspacePath: ws,
		}, allReactionsExitParams(t, config.EnvironmentConfig{Kind: config.EnvironmentKindLocal}))
		t.Cleanup(func() { CancelRetry(state, "SCM-3") })

		for _, kind := range prBackedReactionKinds {
			pr, ok := state.PendingReactions[ReactionKey("SCM-3", kind)]
			if !ok || reactionPRNumber(t, pr) != forgedPR {
				t.Errorf("%s reaction = %+v, want one from the agent's file", kind, pr)
			}
		}
	})
}

func TestRecoverPendingReactions_IgnoresAForgedSCMFileUnderARepository(t *testing.T) {
	t.Parallel()

	recoverAll := func(t *testing.T, wsRoot string) *State {
		t.Helper()
		tracker := &recoveryTrackerStub{states: map[string]string{"ISS-1": "In Review"}}
		state := NewState(5000, 4, 0, nil, AgentTotals{})
		params := defaultRecoveryParams(wsRoot, tracker)
		params.SCMMetadata = SCMMetadataSource(smolvmRepositoryEnvironment())
		params.AutoMergeReactionConfigured = true
		params.ReviewReactionConfigured = true
		params.BotReviewReactionConfigured = true
		params.MergeConflictReactionConfigured = true
		params.LabelReviewReactionConfigured = true
		params.LabelFixReactionConfigured = true
		params.MergeCompletionReactionConfigured = true
		run := freshRun("ISS-1", "PROJ-1", "", 1)
		if _, err := RecoverPendingReactions(context.Background(), state, []persistence.RunHistory{run}, params); err != nil {
			t.Fatalf("RecoverPendingReactions: %v", err)
		}
		return state
	}

	t.Run("recovered reactions act on the recorded pull request", func(t *testing.T) {
		t.Parallel()
		wsRoot := t.TempDir()
		ws := filepath.Join(wsRoot, "PROJ-1")
		writeForgedSCM(t, ws, freshSCMTime(1))
		writeTrustedSCM(t, ws, trustedSCM(freshSCMTime(1)))

		state := recoverAll(t, wsRoot)

		if len(state.PendingReactions) == 0 {
			t.Fatal("no reaction recovered, want reactions for the recorded pull request")
		}
		for key, pr := range state.PendingReactions {
			if got := reactionPRNumber(t, pr); got != trustedPR {
				t.Errorf("recovered %s pull request = %d, want %d", key, got, trustedPR)
			}
		}
		if _, ok := state.PendingReactions[ReactionKey("ISS-1", ReactionKindAutoMerge)]; !ok {
			t.Error("auto-merge reaction not recovered")
		}
	})

	t.Run("without a record nothing is recovered", func(t *testing.T) {
		t.Parallel()
		wsRoot := t.TempDir()
		writeForgedSCM(t, filepath.Join(wsRoot, "PROJ-1"), freshSCMTime(1))

		if state := recoverAll(t, wsRoot); len(state.PendingReactions) != 0 {
			t.Errorf("pending reactions = %v, want none from the agent's file", slices.Collect(maps.Keys(state.PendingReactions)))
		}
	})
}

// prRecordingSCM records the pull request numbers comments are fetched
// for.
type prRecordingSCM struct {
	stubSCMForRecovery
	mu  sync.Mutex
	prs []int
}

func (s *prRecordingSCM) FetchPendingReviews(_ context.Context, pr int, _, _ string) ([]domain.ReviewComment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prs = append(s.prs, pr)
	return nil, nil
}

func (s *prRecordingSCM) FetchBotReviewComments(_ context.Context, pr int, _, _ string, _ []string) ([]domain.ReviewComment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prs = append(s.prs, pr)
	return nil, nil
}

func TestFreshRunSeed_IgnoresAForgedSCMFileUnderARepository(t *testing.T) {
	t.Parallel()

	for _, withRecord := range []bool{true, false} {
		name := "with a record"
		if !withRecord {
			name = "without a record"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			ws := filepath.Join(root, "PROJ-1")
			writeForgedSCM(t, ws, "")
			if withRecord {
				writeTrustedSCM(t, ws, trustedSCM(""))
			}
			scm := &prRecordingSCM{}

			freshRunSeed(context.Background(), NewState(5000, 4, 0, nil, AgentTotals{}), freshRunSeedParams{
				SCMAdapter:          scm,
				Store:               newFingerprintModelStore(),
				WorkspaceRoot:       root,
				SCMMetadata:         SCMMetadataSource(smolvmRepositoryEnvironment()),
				ReviewConfigured:    true,
				BotReviewConfigured: true,
			}, domain.Issue{ID: "1", Identifier: "PROJ-1"}, discardLogger())

			want := []int{trustedPR, trustedPR}
			if !withRecord {
				want = nil
			}
			if !slices.Equal(scm.prs, want) {
				t.Errorf("comments fetched for pull requests %v, want %v", scm.prs, want)
			}
		})
	}
}

func TestSweepWorkspaces_AgePassIgnoresAForgedPushTimeUnderARepository(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		withRecord bool
	}{
		{name: "the recorded push time decides", withRecord: true},
		{name: "without a record the agent's push time is no activity", withRecord: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			ws := filepath.Join(root, "PROJ-OLD")
			writeForgedSCM(t, ws, recentSweepTimestamp())
			if tt.withRecord {
				writeTrustedSCM(t, ws, trustedSCM(oldSweepTimestamp()))
			}
			params := defaultSweepParams(t, root, &sweepTracker{statesByKey: map[string]string{}})
			params.RetentionDays = config.WorkspaceRetentionMinDays
			params.Store = &sweepStoreDouble{}
			params.SCMMetadata = SCMMetadataSource(smolvmRepositoryEnvironment())

			SweepWorkspaces(NewState(5000, 4, 0, nil, AgentTotals{}), params)

			if tt.withRecord {
				assertSweepDirRemoved(t, ws)
			} else {
				assertSweepDirExists(t, ws)
			}
		})
	}
}

func TestEvaluateHandoffEvidence_IgnoresAForgedSCMFileUnderARepository(t *testing.T) {
	t.Parallel()

	source := SCMMetadataSource(smolvmRepositoryEnvironment())

	t.Run("forged file alone is no evidence", func(t *testing.T) {
		t.Parallel()
		ws := t.TempDir()
		writeForgedSCM(t, ws, "")
		got := evaluateHandoffEvidence(context.Background(), WorkerResult{WorkspacePath: ws}, nil, source, discardLogger())
		if got.Verdict == handoffWorkObserved {
			t.Errorf("verdict = %q (%s), want the forged file ignored", got.Verdict, got.Reason)
		}
	})

	t.Run("the record is evidence", func(t *testing.T) {
		t.Parallel()
		ws := t.TempDir()
		writeTrustedSCM(t, ws, trustedSCM(""))
		got := evaluateHandoffEvidence(context.Background(), WorkerResult{WorkspacePath: ws}, nil, source, discardLogger())
		if got.Verdict != handoffWorkObserved {
			t.Errorf("verdict = %q (%s), want work observed", got.Verdict, got.Reason)
		}
	})
}

func TestUpdateSCMRecord(t *testing.T) {
	t.Parallel()

	pushedAt := time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)
	push := func(branch, commit string, at time.Time) *gitsync.ExportResult {
		return &gitsync.ExportResult{Branch: branch, Commit: commit, Pushed: true, PushedAt: at}
	}

	t.Run("a push records branch, commit, and time", func(t *testing.T) {
		t.Parallel()
		ws := t.TempDir()
		if err := updateSCMRecord(ws, scmRecordUpdate{Push: push("sortie/P-1", "aaa", pushedAt)}, discardLogger()); err != nil {
			t.Fatal(err)
		}
		want := domain.SCMMetadata{Branch: "sortie/P-1", SHA: "aaa", PushedAt: "2026-10-12T10:00:00Z"}
		if got := workspace.ReadHostSCMRecord(ws, nil); got != want {
			t.Errorf("record = %+v, want %+v", got, want)
		}
	})

	t.Run("a pull request is recorded with the project's owner and repository", func(t *testing.T) {
		t.Parallel()
		ws := t.TempDir()
		if err := updateSCMRecord(ws, scmRecordUpdate{Push: push("sortie/P-1", "aaa", pushedAt)}, discardLogger()); err != nil {
			t.Fatal(err)
		}
		if err := updateSCMRecord(ws, scmRecordUpdate{PRNumber: 7, Project: "group/sub/app"}, discardLogger()); err != nil {
			t.Fatal(err)
		}
		want := domain.SCMMetadata{Branch: "sortie/P-1", SHA: "aaa", PushedAt: "2026-10-12T10:00:00Z", PRNumber: 7, Owner: "group/sub", Repo: "app"}
		if got := workspace.ReadHostSCMRecord(ws, nil); got != want {
			t.Errorf("record = %+v, want %+v", got, want)
		}

		later := pushedAt.Add(time.Hour)
		if err := updateSCMRecord(ws, scmRecordUpdate{Push: push("sortie/P-1", "bbb", later)}, discardLogger()); err != nil {
			t.Fatal(err)
		}
		want.SHA, want.PushedAt = "bbb", "2026-10-12T11:00:00Z"
		if got := workspace.ReadHostSCMRecord(ws, nil); got != want {
			t.Errorf("record after a later push = %+v, want %+v with the pull request kept", got, want)
		}

		if err := updateSCMRecord(ws, scmRecordUpdate{Push: push("feature/P-1", "ccc", later)}, discardLogger()); err != nil {
			t.Fatal(err)
		}
		want = domain.SCMMetadata{Branch: "feature/P-1", SHA: "ccc", PushedAt: "2026-10-12T11:00:00Z"}
		if got := workspace.ReadHostSCMRecord(ws, nil); got != want {
			t.Errorf("record after a push of another branch = %+v, want %+v without the old pull request", got, want)
		}
	})

	t.Run("a branch already upstream repairs the record", func(t *testing.T) {
		t.Parallel()
		upToDate := func(branch, commit string) *gitsync.ExportResult {
			return &gitsync.ExportResult{Branch: branch, Commit: commit, UpToDate: true}
		}
		parse := func(t *testing.T, at string) time.Time {
			t.Helper()
			got, err := time.Parse(time.RFC3339, at)
			if err != nil {
				t.Fatalf("pushed_at %q: %v", at, err)
			}
			return got
		}

		// No record, as after a push whose record write failed.
		ws := t.TempDir()
		start := time.Now().Add(-time.Second)
		if err := updateSCMRecord(ws, scmRecordUpdate{Push: upToDate("sortie/P-1", "aaa")}, discardLogger()); err != nil {
			t.Fatal(err)
		}
		got := workspace.ReadHostSCMRecord(ws, nil)
		if got.Branch != "sortie/P-1" || got.SHA != "aaa" || got.PRNumber != 0 || parse(t, got.PushedAt).Before(start.Truncate(time.Second)) {
			t.Errorf("record = %+v, want sortie/P-1 at aaa pushed now", got)
		}

		// A record naming that commit keeps its push time and pull request.
		want := domain.SCMMetadata{Branch: "sortie/P-1", SHA: "aaa", PushedAt: "2026-10-12T10:00:00Z", PRNumber: 7, Owner: "org", Repo: "app"}
		writeTrustedSCM(t, ws, want)
		if err := updateSCMRecord(ws, scmRecordUpdate{Push: upToDate("sortie/P-1", "aaa")}, discardLogger()); err != nil {
			t.Fatal(err)
		}
		if got := workspace.ReadHostSCMRecord(ws, nil); got != want {
			t.Errorf("record = %+v, want it unchanged: %+v", got, want)
		}

		// A record naming an older commit takes the new one.
		if err := updateSCMRecord(ws, scmRecordUpdate{Push: upToDate("sortie/P-1", "bbb")}, discardLogger()); err != nil {
			t.Fatal(err)
		}
		got = workspace.ReadHostSCMRecord(ws, nil)
		if got.SHA != "bbb" || got.PRNumber != 7 || got.PushedAt == want.PushedAt {
			t.Errorf("record = %+v, want bbb with a new push time and the pull request kept", got)
		}

		// An export that neither pushed nor found the branch upstream
		// changes nothing.
		before := got
		if err := updateSCMRecord(ws, scmRecordUpdate{Push: &gitsync.ExportResult{Branch: "sortie/P-1", Commit: "ccc"}}, discardLogger()); err != nil {
			t.Fatal(err)
		}
		if got := workspace.ReadHostSCMRecord(ws, nil); got != before {
			t.Errorf("record = %+v, want it unchanged: %+v", got, before)
		}
	})

	t.Run("a project that is no owner/repo path fails", func(t *testing.T) {
		t.Parallel()
		for _, project := range []string{"", "app", "/app", "org/"} {
			if err := updateSCMRecord(t.TempDir(), scmRecordUpdate{PRNumber: 7, Project: project}, discardLogger()); err == nil {
				t.Errorf("project %q: error = nil, want one", project)
			}
		}
	})
}
