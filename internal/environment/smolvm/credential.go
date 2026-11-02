package smolvm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"os"

	"github.com/sortie-ai/sortie/internal/credsource"
)

// credentialEnv reads the value of every credential binding that names
// a file or a command, anew, and returns them as KEY=VALUE entries for
// the host variable each binding names. A binding without a source
// contributes nothing: smolvm reads its variable from Sortie's own
// environment. The first source that yields no value fails with its
// [*credsource.Error].
func (e *Environment) credentialEnv(ctx context.Context) ([]string, error) {
	var env []string
	for _, cred := range e.cfg.Credentials {
		if !cred.Sourced() {
			continue
		}
		value, err := credsource.Read(ctx, credsource.Source{Name: cred.Name, File: cred.File, Command: cred.Command})
		if err != nil {
			return nil, err
		}
		env = append(env, cred.Env+"="+value)
	}
	return env, nil
}

// credentialFingerprint returns a digest of env, so a VM's start values
// can be compared without being kept.
func credentialFingerprint(env []string) string {
	h := sha256.New()
	for _, kv := range env {
		h.Write([]byte(kv)) //nolint:errcheck // hash.Hash.Write never returns an error
		h.Write([]byte{0})  //nolint:errcheck // hash.Hash.Write never returns an error
	}
	return hex.EncodeToString(h.Sum(nil))
}

// startVM starts the VM name with credEnv, the values credentialEnv
// read for this start, and records what it was started with.
func (e *Environment) startVM(ctx context.Context, name string, credEnv []string) error {
	e.warnUnsetCredentials(ctx, name)
	if err := e.cli.Start(ctx, name, credEnv...); err != nil {
		return err
	}
	e.startedMu.Lock()
	e.startedWith[name] = credentialFingerprint(credEnv)
	e.startedMu.Unlock()
	return nil
}

// forgetStart drops what the VM name was started with, once it stopped.
func (e *Environment) forgetStart(name string) {
	e.startedMu.Lock()
	delete(e.startedWith, name)
	e.startedMu.Unlock()
}

// staleCredentials reports whether the running VM name holds other
// values than credEnv for its sourced bindings: it was started with
// different values, or by another Sortie process, whose values are not
// known. A workflow without sourced bindings never has stale ones.
func (e *Environment) staleCredentials(name string, credEnv []string) bool {
	if len(credEnv) == 0 {
		return false
	}
	e.startedMu.Lock()
	defer e.startedMu.Unlock()
	fp, known := e.startedWith[name]
	return !known || fp != credentialFingerprint(credEnv)
}

// warnUnsetCredentials logs each credential binding without a source
// whose host variable is not set. smolvm reads the value when the VM
// starts; without it, the VM starts and every request to the binding's
// hosts fails.
func (e *Environment) warnUnsetCredentials(ctx context.Context, name string) {
	for _, cred := range e.cfg.Credentials {
		if cred.Sourced() {
			continue
		}
		if _, ok := os.LookupEnv(cred.Env); !ok {
			e.logger.WarnContext(ctx, "credential variable is not set; requests to its hosts from the VM will fail",
				slog.String("vm", name), slog.String("credential", cred.Name), slog.String("variable", cred.Env))
		}
	}
}
