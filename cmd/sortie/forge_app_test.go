package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/forgeapp/forgeapptest"
)

// forge-tokens: The App's private key is read once, at start, from the
// configured file, and a file another account may read refuses the
// start; a gate needs the control token; no App mints nothing.
func TestResolveForgeApp(t *testing.T) {
	fake := forgeapptest.New(t, map[string]string{"contents": "write"})
	dir := t.TempDir()
	key := fake.KeyFile(t, dir, 0o600)
	app := &config.ForgeApp{AppID: forgeapptest.AppID, PrivateKeyFile: key, APIURL: fake.URL}

	client, seal, gate, err := resolveForgeApp(config.RemoteConfig{ForgeApp: app}, "control")
	if err != nil || client == nil || seal == [32]byte{} || gate != nil {
		t.Fatalf("resolveForgeApp(0600, no gate) = %v, %v, %v", client, gate, err)
	}
	_, _, gate, err = resolveForgeApp(config.RemoteConfig{ForgeApp: app, ForgeGate: &config.ForgeGate{URL: "http://127.0.0.1:7692"}}, "control")
	if err != nil || gate == nil {
		t.Fatalf("resolveForgeApp with a gate = %v, %v", gate, err)
	}
	if _, _, _, err := resolveForgeApp(config.RemoteConfig{ForgeApp: app, ForgeGate: &config.ForgeGate{URL: "http://127.0.0.1:7692"}}, ""); err == nil ||
		!strings.Contains(err.Error(), "server.control_token") {
		t.Fatalf("a gate without the control token = %v, want a refusal", err)
	}
	if err := os.Chmod(key, 0o644); err != nil {
		t.Fatal(err)
	}
	_, _, _, err = resolveForgeApp(config.RemoteConfig{ForgeApp: app}, "control")
	if err == nil || !strings.Contains(err.Error(), "0644") {
		t.Fatalf("resolveForgeApp(0644) = %v, want the start refused", err)
	}
	if strings.Contains(err.Error(), strings.Split(fake.KeyPEM(), "\n")[1]) {
		t.Fatal("the refusal quotes the key")
	}
	if _, _, _, err := resolveForgeApp(config.RemoteConfig{ForgeApp: &config.ForgeApp{AppID: "1", PrivateKeyFile: filepath.Join(dir, "none.pem")}}, ""); err == nil {
		t.Fatal("a missing key file did not refuse the start")
	}
	if client, _, gate, err := resolveForgeApp(config.RemoteConfig{}, ""); client != nil || gate != nil || err != nil {
		t.Fatalf("no App = %v, %v, %v, want nothing", client, gate, err)
	}
}
