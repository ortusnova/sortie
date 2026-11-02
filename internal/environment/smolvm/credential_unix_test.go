//go:build unix

package smolvm

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/credsource"
)

const sourcedCredentialEnv = "SORTIE_SMOLVM_TEST_SOURCED_API_KEY"

// writeLogin writes an opencode-style auth file holding token, as a
// login or its refresh does.
func writeLogin(t *testing.T, path, token string) {
	t.Helper()
	content := `{"anthropic":{"type":"oauth","access":"` + token + `","refresh":"r"}}` + "\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// startEnvs returns, in order, the value of [sourcedCredentialEnv] each
// recorded machine start saw, "" for a start that saw none.
func startEnvs(f prepareFixture) []string {
	var values []string
	for _, c := range f.fake.calls() {
		if len(c.Args) >= 2 && c.Args[0] == "machine" && c.Args[1] == "start" {
			values = append(values, c.Env[sourcedCredentialEnv])
		}
	}
	return values
}

// TestPrepare_ARefreshedLogin is the scenario of a
// login refreshed while Sortie runs: the same environment, never
// rebuilt, gives each launch the token its source holds at that launch.
func TestPrepare_ARefreshedLogin(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{"file", "command"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			login := filepath.Join(dir, "auth.json")
			tokenFile := filepath.Join(dir, "token")
			// The command stands in for a token helper that reads the
			// operator's opencode login.
			command := `sed -n 's/.*"access":"\([^"]*\)".*/\1/p' '` + login + `'`
			refresh := func(token string) {
				writeLogin(t, login, token)
				if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}

			f := newPrepareFixture(t, fakeState{RecordEnv: []string{sourcedCredentialEnv}}, func(cfg *config.SmolvmConfig) {
				cred := config.SmolvmCredential{Name: "anthropic", Env: sourcedCredentialEnv, Hosts: []string{"api.anthropic.com"}}
				if kind == "file" {
					cred.File = tokenFile
				} else {
					cred.Command = command
				}
				cfg.Credentials = []config.SmolvmCredential{cred}
			})
			ctx := context.Background()

			refresh("token-before-refresh")
			if _, err := f.env.Prepare(ctx, f.ws); err != nil {
				t.Fatalf("first Prepare() error = %v", err)
			}
			writeRecord(t, f.ws.HostPath, "smolvm", "sortie-VM-1")
			if err := f.env.Release(ctx, f.ws); err != nil {
				t.Fatalf("Release() error = %v", err)
			}

			refresh("token-after-refresh")
			if _, err := f.env.Prepare(ctx, f.ws); err != nil {
				t.Fatalf("second Prepare() error = %v", err)
			}

			want := []string{"token-before-refresh", "token-after-refresh"}
			if got := startEnvs(f); !slices.Equal(got, want) {
				t.Errorf("machine start saw %s = %q, want %q", sourcedCredentialEnv, got, want)
			}
			for _, c := range f.fake.calls() {
				if strings.Contains(strings.Join(c.Args, " "), "token-") {
					t.Errorf("call %q carries the credential value in its arguments", c.Args)
				}
			}
		})
	}
}

func TestPrepare_ChangedValueRestarts(t *testing.T) {
	t.Parallel()

	tokenFile := filepath.Join(t.TempDir(), "token")
	f := newPrepareFixture(t, fakeState{RecordEnv: []string{sourcedCredentialEnv}}, func(cfg *config.SmolvmConfig) {
		cfg.Credentials = []config.SmolvmCredential{{Name: "c", Env: sourcedCredentialEnv, Hosts: []string{"example.org"}, File: tokenFile}}
	})
	ctx := context.Background()
	write := func(token string) {
		if err := os.WriteFile(tokenFile, []byte(token), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	write("one")
	if _, err := f.env.Prepare(ctx, f.ws); err != nil {
		t.Fatalf("first Prepare() error = %v", err)
	}
	writeRecord(t, f.ws.HostPath, "smolvm", "sortie-VM-1")

	// The VM still runs with the value it was started with: no restart.
	if _, err := f.env.Prepare(ctx, f.ws); err != nil {
		t.Fatalf("second Prepare() error = %v", err)
	}
	if got := startEnvs(f); !slices.Equal(got, []string{"one"}) {
		t.Fatalf("starts after an unchanged value = %q, want only the first", got)
	}

	write("two")
	if _, err := f.env.Prepare(ctx, f.ws); err != nil {
		t.Fatalf("third Prepare() error = %v", err)
	}
	if got := startEnvs(f); !slices.Equal(got, []string{"one", "two"}) {
		t.Errorf("starts = %q, want a second start with the changed value", got)
	}
	argvs := f.fake.argvs()
	stops := slices.IndexFunc(argvs, func(a []string) bool { return len(a) > 1 && a[1] == "stop" })
	if stops < 0 {
		t.Errorf("calls = %q, want the running VM stopped before its new start", argvs)
	}
}

func TestPrepare_ForeignStartIsRestarted(t *testing.T) {
	t.Parallel()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("fresh"), 0o600); err != nil {
		t.Fatal(err)
	}
	f := newPrepareFixture(t, fakeState{}, func(cfg *config.SmolvmConfig) {
		cfg.Credentials = []config.SmolvmCredential{{Name: "c", Env: sourcedCredentialEnv, Hosts: []string{"example.org"}, File: tokenFile}}
	})
	f.fake.setState(fakeState{RecordEnv: []string{sourcedCredentialEnv}, Machines: []fakeMachine{f.ownedVM("VM-1", StateRunning)}})
	writeRecord(t, f.ws.HostPath, "smolvm", "sortie-VM-1")

	if _, err := f.env.Prepare(context.Background(), f.ws); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if got := startEnvs(f); !slices.Equal(got, []string{"fresh"}) {
		t.Errorf("starts = %q, want one start with the value read now", got)
	}
}

func TestPrepare_EmptySourceFails(t *testing.T) {
	t.Parallel()

	for name, cred := range map[string]config.SmolvmCredential{
		"empty command": {Command: "printf ''"},
		"missing file":  {File: "/nonexistent/sortie-test-token"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			f := newPrepareFixture(t, fakeState{}, func(cfg *config.SmolvmConfig) {
				cred.Name, cred.Env, cred.Hosts = "anthropic", sourcedCredentialEnv, []string{"api.anthropic.com"}
				cfg.Credentials = []config.SmolvmCredential{cred}
			})
			_, err := f.env.Prepare(context.Background(), f.ws)
			var srcErr *credsource.Error
			if !errors.As(err, &srcErr) || srcErr.Credential != "anthropic" {
				t.Fatalf("Prepare() error = %v, want a *credsource.Error for anthropic", err)
			}
			if calls := f.fake.argvs(); len(calls) != 0 {
				t.Errorf("calls = %q, want none: no VM is created or started without its credential", calls)
			}
		})
	}
}

func TestResume_EmptySourceStarts(t *testing.T) {
	t.Parallel()

	f := newPrepareFixture(t, fakeState{}, func(cfg *config.SmolvmConfig) {
		cfg.Credentials = []config.SmolvmCredential{{Name: "c", Env: sourcedCredentialEnv, Hosts: []string{"example.org"}, Command: "exit 1"}}
	})
	f.fake.setState(fakeState{RecordEnv: []string{sourcedCredentialEnv}, Machines: []fakeMachine{f.ownedVM("VM-1", StateStopped)}})

	ready, err := f.env.Resume(context.Background(), f.ws)
	if err != nil || !ready {
		t.Fatalf("Resume() = %v, %v, want true, nil", ready, err)
	}
	if got := startEnvs(f); !slices.Equal(got, []string{""}) {
		t.Errorf("starts = %q, want one start without the value", got)
	}
}
