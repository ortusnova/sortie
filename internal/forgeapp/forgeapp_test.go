package forgeapp_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/forgeapp"
	"github.com/sortie-ai/sortie/internal/forgeapp/forgeapptest"
)

var appPermissions = map[string]string{"contents": "write", "pull_requests": "write", "issues": "write", "metadata": "read"}

func newFake(t *testing.T) *forgeapptest.Server {
	t.Helper()
	return forgeapptest.New(t, appPermissions,
		forgeapptest.Installation{ID: 11, Owner: "ortusnova", Repositories: []string{"fleet-vm", "homelab", "ortus-nova-specs"}},
		forgeapptest.Installation{ID: 22, Owner: "briancappello", Repositories: []string{"personal-site"}},
	)
}

func newClient(t *testing.T, fake *forgeapptest.Server) *forgeapp.Client {
	t.Helper()
	c, err := forgeapp.New(forgeapp.Config{AppID: forgeapptest.AppID, Key: fake.Key, APIURL: fake.URL})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// forge-tokens: The request. A mint names exactly the owner's granted
// repositories and permissions, on that owner's installation, under an
// App token GitHub accepts.
func TestMintNamesExactlyTheGrant(t *testing.T) {
	fake := newFake(t)
	c := newClient(t, fake)
	tok, err := c.Mint(context.Background(), "OrtusNova", []string{"fleet-vm"}, map[string]string{"contents": "write"})
	if err != nil {
		t.Fatal(err)
	}
	mints := fake.Mints()
	want := []forgeapptest.Mint{{Installation: 11, Owner: "ortusnova", Repositories: []string{"fleet-vm"},
		Permissions: map[string]string{"contents": "write"}, Token: tok.Value, Status: http.StatusCreated}}
	if !reflect.DeepEqual(mints, want) {
		t.Fatalf("mints = %+v\nwant %+v", mints, want)
	}
	if errs := fake.JWTErrors(); len(errs) != 0 {
		t.Fatalf("App tokens refused: %v", errs)
	}
	if !fake.Valid(tok.Value) || tok.ExpiresAt.IsZero() {
		t.Fatalf("token %v not valid", tok)
	}
	// No length is assumed: the fake's tokens are far longer than 40.
	if len(tok.Value) <= 40 {
		t.Fatalf("the fake's token is %d characters; it must exceed 40 to prove no length is assumed", len(tok.Value))
	}
	for _, f := range []string{"%v", "%+v", "%#v", "%s"} {
		if strings.Contains(fmt.Sprintf(f, tok), tok.Value) {
			t.Errorf("%s of a token prints its value", f)
		}
	}
}

// forge-tokens: No installation. An owner the App is not installed on is
// refused before any mint, with a failure class a platform fault names.
func TestMintForAnOwnerWithoutTheApp(t *testing.T) {
	fake := newFake(t)
	c := newClient(t, fake)
	_, err := c.Mint(context.Background(), "someone-else", []string{"repo"}, map[string]string{"contents": "read"})
	var fe *forgeapp.Error
	if !errors.As(err, &fe) || !errors.Is(err, forgeapp.ErrNotInstalled) || fe.Owner != "someone-else" || fe.FailureClass() != "forge_app_not_installed" {
		t.Fatalf("Mint = %v, want ErrNotInstalled naming someone-else", err)
	}
	if !strings.Contains(err.Error(), "someone-else") {
		t.Errorf("error %q names no owner", err)
	}
	if len(fake.Mints()) != 0 {
		t.Errorf("minted %+v, want nothing", fake.Mints())
	}
}

// An owner installed after the first read is found once the minute the
// installations are kept has passed.
func TestInstallationsAreReadAgainForANewOwner(t *testing.T) {
	fake := newFake(t)
	now := time.Now()
	clock := func() time.Time { return now }
	fake.SetNow(clock)
	c, err := forgeapp.New(forgeapp.Config{AppID: forgeapptest.AppID, Key: fake.Key, APIURL: fake.URL, Now: clock})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Installation(context.Background(), "late"); !errors.Is(err, forgeapp.ErrNotInstalled) {
		t.Fatalf("Installation(late) = %v, want ErrNotInstalled", err)
	}
	fake.Install(forgeapptest.Installation{ID: 33, Owner: "late", Repositories: []string{"r"}})
	if _, err := c.Installation(context.Background(), "late"); !errors.Is(err, forgeapp.ErrNotInstalled) {
		t.Fatalf("Installation(late) within the minute = %v, want the kept answer", err)
	}
	now = now.Add(time.Minute)
	if id, err := c.Installation(context.Background(), "late"); err != nil || id != 33 {
		t.Fatalf("Installation(late) after a minute = %d, %v, want 33", id, err)
	}
}

// forge-tokens: One token per owner... An installation that does not
// grant a repository, or a permission, is refused naming the owner.
func TestMintRefusedByTheInstallation(t *testing.T) {
	fake := newFake(t)
	c := newClient(t, fake)
	for _, tc := range []struct {
		repos []string
		perms map[string]string
	}{
		{[]string{"fleet"}, map[string]string{"contents": "read"}},
		{[]string{"fleet-vm"}, map[string]string{"administration": "write"}},
		{[]string{"fleet-vm"}, map[string]string{"metadata": "write"}},
	} {
		_, err := c.Mint(context.Background(), "ortusnova", tc.repos, tc.perms)
		var fe *forgeapp.Error
		if !errors.As(err, &fe) || !errors.Is(err, forgeapp.ErrNotGranted) || fe.FailureClass() != "forge_app_not_granted" || fe.Owner != "ortusnova" {
			t.Errorf("Mint(%v, %v) = %v, want ErrNotGranted naming ortusnova", tc.repos, tc.perms, err)
		}
	}
}

// forge-tokens: The attempt ends. A revoked token is refused 401 by
// GitHub; revoking it again, or one that expired, is not an error.
func TestRevoke(t *testing.T) {
	fake := newFake(t)
	c := newClient(t, fake)
	tok, err := c.Mint(context.Background(), "ortusnova", []string{"homelab"}, map[string]string{"contents": "read"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Revoke(context.Background(), tok.Value); err != nil {
		t.Fatal(err)
	}
	if fake.Valid(tok.Value) {
		t.Fatal("token still valid after Revoke")
	}
	req, _ := http.NewRequest(http.MethodGet, fake.URL+"/repos/ortusnova/homelab", nil)
	req.Header.Set("Authorization", "Bearer "+tok.Value)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("a call with the revoked token = %d, want 401", resp.StatusCode)
	}
	if err := c.Revoke(context.Background(), tok.Value); err != nil {
		t.Fatalf("revoking again = %v, want nil", err)
	}
}

func TestInstallationsArePaged(t *testing.T) {
	var insts []forgeapptest.Installation
	for i := range 130 {
		insts = append(insts, forgeapptest.Installation{ID: int64(1000 + i), Owner: fmt.Sprintf("owner-%03d", i)})
	}
	fake := forgeapptest.New(t, appPermissions, insts...)
	c := newClient(t, fake)
	if id, err := c.Installation(context.Background(), "owner-129"); err != nil || id != 1129 {
		t.Fatalf("Installation(owner-129) = %d, %v, want 1129 from the second page", id, err)
	}
}

// The App's private key is read from a file only its owner may read.
func TestLoadKey(t *testing.T) {
	fake := newFake(t)
	dir := t.TempDir()
	path := fake.KeyFile(t, dir, 0o600)
	k, err := forgeapp.LoadKey(path)
	if err != nil || !k.Equal(fake.Key) {
		t.Fatalf("LoadKey(0600) = %v", err)
	}
	for _, mode := range []os.FileMode{0o640, 0o604, 0o644, 0o660} {
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		_, err := forgeapp.LoadKey(path)
		if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("%04o", mode)) || !strings.Contains(err.Error(), "chmod 600") {
			t.Errorf("LoadKey(%04o) = %v, want a refusal naming the mode", mode, err)
		}
	}
	if _, err := forgeapp.LoadKey(filepath.Join(dir, "missing.pem")); err == nil {
		t.Error("LoadKey of a missing file succeeded")
	}
}

// PKCS8 keys load too, and a key that is not RSA, or not PEM, is
// refused without quoting it.
func TestParseKey(t *testing.T) {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := forgeapp.ParseKey(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})); err != nil || !got.Equal(k) {
		t.Fatalf("ParseKey(PKCS #8) = %v", err)
	}
	secret := "not-a-key-but-secret-text"
	_, err = forgeapp.ParseKey([]byte(secret))
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("ParseKey(text) = %v, want a refusal that quotes nothing", err)
	}
}
