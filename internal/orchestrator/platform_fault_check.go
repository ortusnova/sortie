package orchestrator

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/sortie-ai/sortie/internal/failcause"
	"github.com/sortie-ai/sortie/internal/gitsync"
	"github.com/sortie-ai/sortie/internal/workspace"
)

// faultHostName returns name, or this machine's host name when name is
// empty: the host a person action names.
func faultHostName(name string) string {
	if name != "" {
		return name
	}
	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}
	return "this host"
}

// checkPlatformFault is the default check of a platform fault. A fault
// of the export step is probed by pushing again what a held story's
// refused export imported, which passes once the push does. Otherwise a
// fault only a person can clear passes once its action is marked done,
// and any other fault passes, so its stories run again.
func (o *Orchestrator) checkPlatformFault(ctx context.Context, check PlatformFaultCheck) error {
	if probed, err := o.probePush(ctx, check); probed {
		return err
	}
	if check.PersonAction && !check.ActionDone {
		return errAwaitingPersonAction
	}
	return nil
}

// probePush pushes again, for the first held story that has one, the
// issue branch its refused export imported into the repository mirror.
// It reports false when the fault is not of the export step or no held
// story has an import to push.
func (o *Orchestrator) probePush(ctx context.Context, check PlatformFaultCheck) (bool, error) {
	if check.Step != "export" || o.workflowManager == nil {
		return false, nil
	}
	cfg := o.workflowManager.Config()
	if !configuresRepository(cfg.Environment) {
		return false, nil
	}
	stateDir := ""
	if o.dbPath != "" {
		stateDir = filepath.Dir(o.dbPath)
	}
	for _, story := range check.Stories {
		path, err := workspace.ComputePath(cfg.Workspace.Root, story.Identifier)
		if err != nil {
			continue
		}
		envCfg, _, err := issueRepository(cfg.Environment, path.Path, nil)
		if err != nil || envCfg.RepositorySet().Repository == nil {
			continue
		}
		syncer, err := gitsync.New(gitsync.Params{Repository: *envCfg.RepositorySet().Repository, StateDir: stateDir, Logger: o.logger})
		if err != nil {
			continue
		}
		pushed, err := syncer.PushIncoming(ctx, path.Key)
		if pushed || err != nil {
			return true, err
		}
	}
	return false, nil
}

// faultCheck returns the check in force: the one the params set, or
// [Orchestrator.checkPlatformFault].
func (o *Orchestrator) faultCheck() PlatformFaultChecker {
	if o.faultChecker != nil {
		return o.faultChecker
	}
	return o.checkPlatformFault
}

// runFaultCheck runs check bounded by [platformFaultCheckTimeout] and
// returns its error.
func (o *Orchestrator) runFaultCheck(ctx context.Context, check PlatformFaultCheck) error {
	checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), platformFaultCheckTimeout)
	defer cancel()
	return o.faultCheck()(checkCtx, check)
}

// startDueFaultChecks starts, off the event loop, the check of every open
// fault whose scheduled check is due; each result is applied on the loop.
// Must run on the event loop.
func (o *Orchestrator) startDueFaultChecks(ctx context.Context, now time.Time) {
	for _, check := range dueFaultChecks(o.state, now) {
		o.state.TrackerOpsWg.Go(func() {
			err := o.runFaultCheck(ctx, check)
			o.runOnLoop(ctx, func(loopCtx context.Context) {
				o.applyFaultCheckOnLoop(loopCtx, check.FaultID, err)
			})
		})
	}
}

// applyFaultCheckOnLoop applies a check result and, when it released
// stories, asks for a tick so they dispatch at once. A failed check whose
// fault had its action marked done while it ran starts the next check at
// once. It returns the fault and whether the check resolved it. Must run
// on the event loop.
func (o *Orchestrator) applyFaultCheckOnLoop(ctx context.Context, faultID string, checkErr error) (*PlatformFault, bool) {
	now := time.Now()
	f, resolved := applyFaultCheck(ctx, o.state, o.store, faultID, checkErr, now, o.logger)
	if resolved {
		o.handoffFaultResolved(ctx, f, now.UTC())
	}
	switch {
	case resolved:
		o.RefreshFunc()()
	case f != nil && !now.Before(f.NextCheckAt):
		o.startDueFaultChecks(ctx, now)
	}
	o.notifyObservers()
	return f, resolved
}

// PlatformFaultDoneStatus is what a request to mark a person action done
// did.
type PlatformFaultDoneStatus int

const (
	PlatformFaultDoneChecked   PlatformFaultDoneStatus = iota + 1 // the action was marked done and the fault checked
	PlatformFaultDoneBusy                                         // the action was marked done; a check already ran and decides
	PlatformFaultDoneNotFound                                     // no open fault has the ID
	PlatformFaultDoneDraining                                     // shutdown began before the loop took the request
	PlatformFaultDoneWithdrawn                                    // the caller's context ended before the loop took the request
)

// PlatformFaultDoneResult is the answer to a request to mark a person
// action done. Fault is the fault after the check; Resolved reports that
// the check passed and released its stories.
type PlatformFaultDoneResult struct {
	Status   PlatformFaultDoneStatus
	Resolved bool
	Fault    SnapshotPlatformFault
}

// PlatformFaultDoneFunc returns a function that marks the person action of
// the open fault faultID done by by and checks the fault again at once. It
// returns once the check result is applied, ctx ended before the loop took
// the request, or shutdown began. Safe for concurrent use.
func (o *Orchestrator) PlatformFaultDoneFunc() func(ctx context.Context, faultID, by string) PlatformFaultDoneResult {
	return func(ctx context.Context, faultID, by string) PlatformFaultDoneResult {
		var (
			check  PlatformFaultCheck
			fault  *PlatformFault
			starts bool
		)
		switch o.runOnLoop(ctx, func(loopCtx context.Context) {
			check, fault, starts = markPlatformFaultActionDone(loopCtx, o.state, o.store, faultID, by, time.Now(), o.logger)
			if fault != nil {
				o.logger.Info("platform fault action marked done",
					slog.String("platform_fault_id", faultID),
					slog.String("by", by),
				)
				o.notifyObservers()
			}
		}) {
		case loopDraining:
			return PlatformFaultDoneResult{Status: PlatformFaultDoneDraining}
		case loopWithdrawn:
			return PlatformFaultDoneResult{Status: PlatformFaultDoneWithdrawn}
		}
		if fault == nil {
			return PlatformFaultDoneResult{Status: PlatformFaultDoneNotFound}
		}
		if !starts {
			result := PlatformFaultDoneResult{Status: PlatformFaultDoneBusy}
			o.runOnLoop(context.WithoutCancel(ctx), func(context.Context) { result.Fault = snapshotPlatformFault(fault) })
			return result
		}

		checkErr := o.runFaultCheck(ctx, check)
		result := PlatformFaultDoneResult{Status: PlatformFaultDoneChecked}
		// The check ran, so its result is applied even when the caller
		// has gone; only shutdown drops it.
		if o.runOnLoop(context.WithoutCancel(ctx), func(loopCtx context.Context) {
			f, resolved := o.applyFaultCheckOnLoop(loopCtx, faultID, checkErr)
			if f != nil {
				result.Fault = snapshotPlatformFault(f)
			}
			result.Resolved = resolved
		}) != loopRan {
			return PlatformFaultDoneResult{Status: PlatformFaultDoneDraining}
		}
		return result
	}
}

// faultForge names the forge of the repository cfg's environment pushes
// to, for a person action.
func faultForge(provider string) string {
	return failcause.ForgeName(provider)
}
