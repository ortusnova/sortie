package smolvm

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/sortie-ai/sortie/internal/environment"
	"github.com/sortie-ai/sortie/internal/workspace"
)

// lifecycleTimeout bounds each stop, resume, removal, and sweep, so a
// smolvm that hangs cannot hold a worker's exit or a cleanup forever.
const lifecycleTimeout = 2 * time.Minute

// owns reports whether m is the VM this environment created for the
// workspace key: labeled as managed, for key, and for this workflow.
func (e *Environment) owns(m Machine, key string) bool {
	return e.workflowHash != "" &&
		m.Labels[LabelManaged] == "1" &&
		m.Labels[LabelWorkspace] == key &&
		m.Labels[LabelWorkflow] == e.workflowHash
}

// ownVM looks up the VM of ws. It returns nil without one. A machine
// with the VM's name that this environment does not own is never
// touched: ownVM logs it, returns nil, and reports foreign.
func (e *Environment) ownVM(ctx context.Context, ws environment.Workspace) (vm *Machine, foreign bool, err error) {
	name := VMName(ws.Key)
	machines, err := e.cli.List(ctx)
	if err != nil {
		return nil, false, err
	}
	for i := range machines {
		if machines[i].Name != name {
			continue
		}
		if !e.owns(machines[i], ws.Key) {
			e.logger.WarnContext(ctx, "leaving alone a machine that is not this workflow's VM for the workspace",
				slog.String("vm", name), slog.String("workspace", ws.HostPath))
			return nil, true, nil
		}
		return &machines[i], false, nil
	}
	return nil, false, nil
}

// validKey reports whether key is a sanitized workspace key, safe to
// join under a directory: the key [workspace.SanitizeKey] keeps as it
// is, which holds no path separator and is neither "." nor "..".
func validKey(key string) bool {
	sanitized, err := workspace.SanitizeKey(key)
	return err == nil && sanitized == key
}

// Release stops the workspace's VM. Its disk, and so the guest
// workspace, persists. A workspace without a VM, or whose VM is not
// running, releases successfully.
func (e *Environment) Release(ctx context.Context, ws environment.Workspace) error {
	if e.workflowHash == "" || !validKey(ws.Key) {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, lifecycleTimeout)
	defer cancel()
	vm, foreign, err := e.ownVM(ctx, ws)
	if err != nil {
		return err
	}
	if vm == nil {
		// A command run in the missing VM, such as the session's, left
		// smolvm's residue for its name.
		if !foreign {
			e.removeMissingMachineResidue(VMName(ws.Key))
		}
		return nil
	}
	if !vm.Running() {
		return nil
	}
	if err := e.cli.Stop(ctx, vm.Name); err != nil {
		if !IsNotFound(err) {
			return err
		}
		e.removeMissingMachineResidue(vm.Name)
	}
	e.forgetStart(vm.Name)
	e.logger.DebugContext(ctx, "stopped VM", slog.String("vm", vm.Name), slog.String("workspace", ws.HostPath))
	return nil
}

// Resume starts the workspace's VM when it exists and is not running,
// so a command such as the before_remove hook can run in it. It reports
// false when the workspace has no VM.
func (e *Environment) Resume(ctx context.Context, ws environment.Workspace) (bool, error) {
	if e.workflowHash == "" || !validKey(ws.Key) {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, lifecycleTimeout)
	defer cancel()
	vm, _, err := e.ownVM(ctx, ws)
	if err != nil || vm == nil {
		return false, err
	}
	if vm.Running() {
		return true, nil
	}
	if err := e.ensureMountDirs(ws.Key); err != nil {
		return false, err
	}
	// The hook or the triage command runs without the value of a source
	// that yields none, rather than not at all.
	credEnv, err := e.credentialEnv(ctx)
	if err != nil {
		e.logger.WarnContext(ctx, "starting the VM without a credential: requests to its hosts from the VM will fail",
			slog.String("vm", vm.Name), slog.Any("error", err))
		credEnv = nil
	}
	if err := e.startVM(ctx, vm.Name, credEnv); err != nil {
		return false, err
	}
	if err := e.startToolListener(ctx, vm.Name); err != nil {
		return false, err
	}
	return true, nil
}

// ensureMountDirs creates the state directories the VM of key mounts,
// which a start needs and nothing else may have kept.
func (e *Environment) ensureMountDirs(key string) error {
	if e.stateDir == "" {
		return nil
	}
	stateDir, err := filepath.Abs(e.stateDir)
	if err != nil {
		return fmt.Errorf("resolve state directory: %w", err)
	}
	return os.MkdirAll(filepath.Join(stateDir, stateEnvDir, key), 0o700)
}

// Remove deletes the workspace's VM, then its directory under the
// state directory. The before_remove hook has run in the VM by then,
// and the host workspace directory goes next. A workspace without a VM
// is removed successfully. A machine with the VM's name that this
// environment does not own is left alone, with the state directory.
func (e *Environment) Remove(ctx context.Context, ws environment.Workspace) error {
	if !validKey(ws.Key) {
		return fmt.Errorf("smolvm environment: workspace key %q cannot name a VM", ws.Key)
	}
	if e.workflowHash == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, lifecycleTimeout)
	defer cancel()
	vm, foreign, err := e.ownVM(ctx, ws)
	if err != nil {
		return err
	}
	if foreign {
		return nil
	}
	if vm == nil {
		e.removeMissingMachineResidue(VMName(ws.Key))
		return e.removeStateDir(ws.Key)
	}
	if err := e.deleteVM(ctx, vm.Name); err != nil {
		return err
	}
	e.logger.InfoContext(ctx, "deleted VM", slog.String("vm", vm.Name), slog.String("workspace", ws.HostPath))
	return e.removeStateDir(ws.Key)
}

// deleteVM deletes the VM name. A VM that is already gone, such as one
// deleted between the listing and the deletion, is deleted
// successfully, and the residue smolvm leaves for its name is removed.
func (e *Environment) deleteVM(ctx context.Context, name string) error {
	err := e.cli.Delete(ctx, name)
	if err == nil || IsNotFound(err) {
		e.forgetStart(name)
	}
	if IsNotFound(err) {
		e.removeMissingMachineResidue(name)
		return nil
	}
	return err
}

// removeStateDir removes the directory of key under the state
// directory, which holds the VM's tool socket.
func (e *Environment) removeStateDir(key string) error {
	if e.stateDir == "" {
		return nil
	}
	stateDir, err := filepath.Abs(e.stateDir)
	if err != nil {
		return fmt.Errorf("resolve state directory: %w", err)
	}
	if err := os.RemoveAll(filepath.Join(stateDir, stateEnvDir, key)); err != nil {
		return fmt.Errorf("remove the VM's state directory: %w", err)
	}
	return nil
}

// Sweep deletes every VM labeled as managed for this workflow whose
// workspace directory, <p.WorkspaceRoot>/<its sortie.workspace label>,
// no longer exists, unless that key has a running worker, then removes
// the key's directory under the state directory. A VM whose label does
// not hold a sanitized workspace key, or whose name is not the one that
// key derives, is left alone, as is every machine without the labels.
// Nothing is swept while the workspace root itself is missing. A failed
// deletion does not stop the sweep; the failures are joined.
func (e *Environment) Sweep(ctx context.Context, p environment.SweepParams) error {
	if e.workflowHash == "" {
		return nil
	}
	if !filepath.IsAbs(p.WorkspaceRoot) {
		return fmt.Errorf("smolvm environment: workspace root %q is not absolute", p.WorkspaceRoot)
	}
	// A missing root says the workspaces are out of reach, such as on a
	// volume not mounted yet, not that each one was removed.
	if info, err := os.Stat(p.WorkspaceRoot); err != nil || !info.IsDir() {
		e.logger.InfoContext(ctx, "sweep: workspace root is not a directory, skipping the VM sweep",
			slog.String("workspace_root", p.WorkspaceRoot), slog.Any("error", err))
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, lifecycleTimeout)
	defer cancel()
	machines, err := e.cli.List(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, m := range machines {
		if m.Labels[LabelManaged] != "1" || m.Labels[LabelWorkflow] != e.workflowHash {
			continue
		}
		key := m.Labels[LabelWorkspace]
		if !validKey(key) || m.Name != VMName(key) {
			e.logger.WarnContext(ctx, "sweep: leaving alone a managed VM whose workspace label does not match its name",
				slog.String("vm", m.Name), slog.String("workspace_key", key))
			continue
		}
		if p.Running[key] {
			continue
		}
		path := filepath.Join(p.WorkspaceRoot, key)
		if _, statErr := os.Lstat(path); !errors.Is(statErr, fs.ErrNotExist) {
			continue
		}
		if err := e.deleteVM(ctx, m.Name); err != nil {
			errs = append(errs, err)
			continue
		}
		e.logger.InfoContext(ctx, "sweep: deleted leftover VM",
			slog.String("vm", m.Name), slog.String("workspace", path))
		if err := e.removeStateDir(key); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// SweepUnselected sweeps leftover VMs, as [Environment.Sweep] does, for
// a workflow whose configuration selects another kind. Every VM this
// kind creates has a directory under the state directory until it is
// deleted, so without one there is nothing to sweep, and smolvm is not
// run: a host that never ran a VM never needs smolvm.
func SweepUnselected(ctx context.Context, params environment.Params, p environment.SweepParams) error {
	if params.StateDir == "" {
		return nil
	}
	entries, err := os.ReadDir(filepath.Join(params.StateDir, stateEnvDir))
	if errors.Is(err, fs.ErrNotExist) || (err == nil && len(entries) == 0) {
		return nil
	}
	if err != nil {
		return err
	}
	env, err := New(params)
	if err != nil {
		return err
	}
	return env.Sweep(ctx, p)
}
