package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/plan"
	"github.com/sortie-ai/sortie/internal/registry"
	"github.com/sortie-ai/sortie/internal/workflow"
)

// planSyncReport is the "sortie plan sync --format json" envelope.
// Result is null on a dry run and when the apply was refused before its
// first write.
type planSyncReport struct {
	TrackerKind  string                     `json:"tracker_kind"`
	DryRun       bool                       `json:"dry_run"`
	Capabilities []registry.WriteCapability `json:"capabilities"`
	Preview      *plan.Preview              `json:"preview"`
	Result       *plan.Result               `json:"result"`
	Error        *planSyncError             `json:"error"`
}

type planSyncError struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

func runPlan(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer) int {
	if len(args) == 0 || containsHelpFlag(args[:1]) {
		printPlanHelp(stdout)
		return 0
	}
	if args[0] != "sync" {
		fmt.Fprintf(stderr, "sortie plan: unknown command %q; run \"sortie plan --help\"\n", args[0]) //nolint:errcheck // stderr write failure is unrecoverable
		return 1
	}
	return runPlanSync(ctx, args[1:], stdout, stderr)
}

func runPlanSync(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer) int {
	if containsHelpFlag(args) {
		printPlanHelp(stdout)
		return 0
	}

	fs := flag.NewFlagSet("sortie plan sync", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	graphPath := fs.String("graph", "", "Path to the plan graph document (required)")
	dryRun := fs.Bool("dry-run", false, "Print the preview and write nothing")
	format := fs.String("format", "text", `Output format: "text" or "json"`)
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(stderr, "sortie plan sync: %s\n", err) //nolint:errcheck // stderr write failure is unrecoverable
		return 1
	}
	fail := func(format string, a ...any) int {
		fmt.Fprintf(stderr, "sortie plan sync: "+format+"\n", a...) //nolint:errcheck // stderr write failure is unrecoverable
		return 1
	}
	if *format != "text" && *format != "json" {
		return fail("invalid --format value %q: must be \"text\" or \"json\"", *format)
	}
	if *graphPath == "" {
		return fail("--graph is required")
	}

	raw, err := os.ReadFile(*graphPath)
	if err != nil {
		return fail("read graph: %s", err)
	}
	g, err := plan.Decode(raw)
	if err != nil {
		return fail("%s", err)
	}

	path, err := resolveWorkflowPath(fs.Args())
	if err != nil {
		return fail("%s", err)
	}
	tracker, kind, closeTracker, err := newPlanTracker(path)
	if err != nil {
		return fail("%s", err)
	}
	defer closeTracker()

	syncer := plan.NewSyncer(tracker)
	report := planSyncReport{TrackerKind: kind, DryRun: *dryRun, Capabilities: tracker.Capabilities()}

	var preview plan.Preview
	var result plan.Result
	if *dryRun {
		preview, _, err = syncer.Preview(ctx, g)
		if err == nil {
			report.Preview = &preview
		}
	} else {
		preview, result, err = syncer.ApplyNow(ctx, g)
		report.Preview = &preview
		if err == nil || result.Failed != nil {
			report.Result = &result
		}
	}
	if err != nil {
		report.Error = &planSyncError{Kind: errorKind(err), Message: err.Error()}
	}

	if *format == "json" {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(report) //nolint:errcheck // stdout write failure is unrecoverable
	} else {
		writePlanText(stdout, report)
	}
	if err != nil {
		if *format == "text" {
			fmt.Fprintf(stderr, "sortie plan sync: %s\n", err) //nolint:errcheck // stderr write failure is unrecoverable
		}
		return 1
	}
	return 0
}

// trackerWrites returns the write capabilities a tracker kind declared.
func trackerWrites(kind string) []registry.WriteCapability {
	meta, _ := registry.Trackers.Meta(kind)
	return meta.WriteCapabilities
}

// newPlanProjects returns the plan projects behind the HTTP plan
// endpoints. The configured project uses the tracker adapter the
// orchestrator already built. When that adapter can inspect and create
// projects, other projects of the same workspace get adapters of their
// own on first use, built from the current workflow with only the project
// replaced; otherwise only the configured project is served.
func newPlanProjects(adapter domain.TrackerAdapter, cfg config.ServiceConfig, current func() config.ServiceConfig, states func() domain.TrackerStates) *plan.Projects {
	meta, _ := registry.Trackers.Meta(cfg.Tracker.Kind)
	def := plan.NewSyncer(plan.Tracker{Adapter: adapter, Meta: meta, States: states, Repositories: workflowRepositories(current), RequiresRepository: workflowRequiresRepository(current)})
	var factory plan.TrackerFactory
	if _, ok := adapter.(domain.PlanProjectEnsurer); ok {
		factory = func(_ context.Context, project string) (plan.Tracker, func(), error) {
			return projectTracker(current(), project, states)
		}
	}
	return plan.NewProjects(cfg.Tracker.Project, def, factory)
}

// projectTracker builds the tracker adapter of cfg with its project
// replaced by project.
func projectTracker(cfg config.ServiceConfig, project string, states func() domain.TrackerStates) (plan.Tracker, func(), error) {
	ctor, err := registry.Trackers.Get(cfg.Tracker.Kind)
	if err != nil {
		return plan.Tracker{}, nil, err
	}
	cfgMap := trackerConfigMap(cfg.Tracker)
	cfgMap["project"] = project
	cfgMap["user_agent"] = "sortie/" + Version
	config.MergeAdapterExtensions(cfgMap, cfg, cfg.Tracker.Kind)
	adapter, err := ctor(cfgMap)
	if err != nil {
		return plan.Tracker{}, nil, err
	}
	if states != nil {
		adapter.SetTrackerStates(states)
	}
	meta, _ := registry.Trackers.Meta(cfg.Tracker.Kind)
	release := func() {}
	if closer, ok := adapter.(io.Closer); ok {
		release = func() { _ = closer.Close() }
	}
	current := func() config.ServiceConfig { return cfg }
	return plan.Tracker{Adapter: adapter, Meta: meta, States: states, Repositories: workflowRepositories(current), RequiresRepository: workflowRequiresRepository(current)}, release, nil
}

// newPlanTracker is planTracker; tests substitute a fake tracker.
var newPlanTracker = planTracker

// planTracker constructs the workflow's tracker adapter the way the
// orchestrator does, and pairs it with its registered metadata.
func planTracker(workflowPath string) (plan.Tracker, string, func(), error) {
	wf, err := workflow.Load(workflowPath)
	if err != nil {
		return plan.Tracker{}, "", nil, err
	}
	cfg, err := config.NewServiceConfig(wf.Config)
	if err != nil {
		return plan.Tracker{}, "", nil, err
	}
	ctor, err := registry.Trackers.Get(cfg.Tracker.Kind)
	if err != nil {
		return plan.Tracker{}, "", nil, err
	}
	cfgMap := trackerConfigMap(cfg.Tracker)
	cfgMap["user_agent"] = "sortie/" + Version
	config.MergeAdapterExtensions(cfgMap, cfg, cfg.Tracker.Kind)
	adapter, err := ctor(cfgMap)
	if err != nil {
		return plan.Tracker{}, "", nil, err
	}
	meta, _ := registry.Trackers.Meta(cfg.Tracker.Kind)
	closeFn := func() {}
	if closer, ok := adapter.(io.Closer); ok {
		closeFn = func() { _ = closer.Close() }
	}
	states := cfg.Tracker.States()
	current := func() config.ServiceConfig { return cfg }
	return plan.Tracker{
		Adapter: adapter, Meta: meta, States: func() domain.TrackerStates { return states },
		Repositories: workflowRepositories(current), RequiresRepository: workflowRequiresRepository(current),
	}, cfg.Tracker.Kind, closeFn, nil
}

// errorKind names an error's category for reports: a tracker error kind,
// "conflict", "apply_in_progress", "payload", or "error".
func errorKind(err error) string {
	if te, ok := errors.AsType[*domain.TrackerError](err); ok {
		return string(te.Kind)
	}
	if _, ok := errors.AsType[*plan.ConflictError](err); ok {
		return "conflict"
	}
	if _, ok := errors.AsType[*plan.GraphError](err); ok {
		return "payload"
	}
	if errors.Is(err, plan.ErrApplyInProgress) {
		return "apply_in_progress"
	}
	return "error"
}

func writePlanText(w io.Writer, r planSyncReport) {
	p := func(format string, a ...any) { fmt.Fprintf(w, format+"\n", a...) } //nolint:errcheck // stdout write failure is unrecoverable
	if r.Preview != nil {
		pv := r.Preview
		c := pv.Counts
		p("plan sync preview (tracker %s):", r.TrackerKind)
		p("  creates %d, updates %d, parents %d, relations added %d, relations removed %d, orphans %d",
			c.Creates, c.Updates, c.Parents, c.RelationsAdded, c.RelationsRemoved, c.Orphans)
		if c.Releases+c.Labels+c.Approvals > 0 {
			p("  releases %d, review labels %d, approvals %d", c.Releases, c.Labels, c.Approvals)
		}
		for _, op := range pv.Releases {
			p("  release %s to %s", op.PlanKey, op.To)
		}
		for _, u := range pv.Unsupported {
			p("  not written: %s", u)
		}
		for _, u := range pv.UnlistedRepositories {
			p("  refused %s: repository %q is not listed in the workflow's repositories", u.PlanKey, u.Repository)
		}
		for _, key := range pv.MissingRepositories {
			p("  refused %s: names no repository and the workflow sets no default_repository", key)
		}
		for _, op := range pv.Creates {
			line := fmt.Sprintf("  create %s %s %q", op.Kind, op.PlanKey, op.Title)
			if op.ParentKey != "" {
				line += " under " + op.ParentKey
			}
			p("%s", line)
		}
		for _, op := range pv.Updates {
			p("  update %s (%s): %s", op.PlanKey, op.Identifier, strings.Join(op.Fields, ", "))
		}
		for _, op := range pv.Parents {
			p("  set parent of %s (%s) to %s", op.PlanKey, op.Identifier, op.ParentKey)
		}
		for _, op := range pv.RelationsAdded {
			p("  link %s blocks %s", op.Blocker, op.Blocked)
		}
		for _, op := range pv.RelationsRemoved {
			p("  unlink %s blocks %s", op.Blocker, op.Blocked)
		}
		for _, o := range pv.Orphans {
			p("  orphaned %s (%s): no longer in the plan; left in the tracker", o.PlanKey, o.Identifier)
		}
		if len(pv.SkippedDrafts) > 0 {
			p("  skipped, still drafts: %s", strings.Join(pv.SkippedDrafts, ", "))
		}
		if len(pv.UnplannedChanges) > 0 {
			p("  not in the plan: %s", strings.Join(pv.UnplannedChanges, ", "))
		}
		if len(pv.MissingCapabilities) > 0 {
			names := make([]string, 0, len(pv.MissingCapabilities))
			for _, m := range pv.MissingCapabilities {
				names = append(names, string(m))
			}
			p("  missing tracker capabilities: %s", strings.Join(names, ", "))
		}
	}
	if r.Result != nil {
		res := r.Result
		p("applied: created %d, updated %d, parents set %d, relations added %d, relations removed %d",
			len(res.Created), len(res.Updated), len(res.ParentsSet), len(res.RelationsAdded), len(res.RelationsRemoved))
		if res.Failed != nil {
			p("stopped at %s %s: %s", res.Failed.Op, res.Failed.PlanKey, res.Failed.Error)
		}
	}
}

// workflowRepositories returns the names of the repositories the current
// workflow lets Sortie open, for plan sync to refuse any other. A
// workflow that lists none refuses every name.
func workflowRepositories(current func() config.ServiceConfig) func() []string {
	if current == nil {
		return nil
	}
	return func() []string {
		names := current().Environment.RepositorySet().RepositoryNames()
		if names == nil {
			names = []string{}
		}
		return names
	}
}

// workflowRequiresRepository reports whether the current workflow lists
// repositories and names no default_repository, so that plan sync
// refuses a story that names no repository.
func workflowRequiresRepository(current func() config.ServiceConfig) func() bool {
	if current == nil {
		return nil
	}
	return func() bool {
		set := current().Environment.RepositorySet()
		return len(set.Repositories) > 0 && set.Repository == nil
	}
}
