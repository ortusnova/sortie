package forge

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const ceilingField = "environment.remote.forge_ceiling"

func ceilingOf(t *testing.T, doc string) Ceiling {
	t.Helper()
	var v any
	if err := yaml.Unmarshal([]byte(doc), &v); err != nil {
		t.Fatal(err)
	}
	c, err := CeilingFromValue(v, ceilingField)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

const homelabCeiling = `
homelab:
  - {repository: ortusnova/fleet-vm, permissions: {contents: write}}
  - {repository: ortusnova/homelab, permissions: {contents: write, pull_requests: write}}
`

func TestCeilingCheck(t *testing.T) {
	c := ceilingOf(t, homelabCeiling)
	for _, tc := range []struct {
		name, repo, doc string
		wantErr         []string
	}{
		{name: "within", repo: "homelab", doc: "version: 1\naccess:\n  - {repository: ortusnova/fleet-vm, permissions: {contents: write}, reason: mirror}\n  - {repository: ortusnova/homelab, permissions: {contents: read}, reason: read}\n"},
		{name: "case does not matter", repo: "homelab", doc: "version: 1\naccess:\n  - {repository: OrtusNova/Fleet-VM, permissions: {contents: read}, reason: mirror}\n"},
		// forge-access: Above the ceiling.
		{name: "a repository the ceiling does not list", repo: "homelab",
			doc:     "version: 1\naccess:\n  - {repository: ortusnova/fleet-vm, permissions: {contents: write}, reason: mirror}\n  - {repository: ortusnova/fleet, permissions: {contents: read}, reason: metal}\n",
			wantErr: []string{"access[1].repository", "ortusnova/fleet is outside the ceiling", ceilingField + ".homelab"}},
		{name: "a level above the ceiling", repo: "homelab",
			doc:     "version: 1\naccess:\n  - {repository: ortusnova/homelab, permissions: {contents: read, pull_requests: write}, reason: ok}\n  - {repository: ortusnova/fleet-vm, permissions: {contents: write, metadata: read}, reason: mirror}\n",
			wantErr: []string{"access[1].permissions.metadata", "grants no metadata"}},
		{name: "every permission at the ceiling's level", repo: "homelab",
			doc:     "version: 1\naccess:\n  - {repository: ortusnova/homelab, permissions: {pull_requests: write, contents: write}, reason: ok}\n",
			wantErr: nil},
		{name: "a repository with no ceiling", repo: "sortie",
			doc:     "version: 1\naccess:\n  - {repository: ortusnova/sortie, permissions: {contents: read}, reason: self}\n",
			wantErr: []string{"access[0].repository", "names no ceiling for sortie"}},
		{name: "nothing declared is always within", repo: "sortie", doc: "version: 1\naccess: []\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := Parse([]byte(tc.doc))
			if err != nil {
				t.Fatal(err)
			}
			err = c.Check(tc.repo, d, ceilingField)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("Check = %v, want it within", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Check = nil, want the entry refused")
			}
			for _, want := range tc.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Check = %q, want it to name %q", err, want)
				}
			}
		})
	}
}

func TestCeilingRefusesWriteAboveRead(t *testing.T) {
	c := ceilingOf(t, "homelab:\n  - {repository: ortusnova/homelab, permissions: {contents: read}}\n")
	d, err := Parse([]byte("version: 1\naccess:\n  - {repository: ortusnova/homelab, permissions: {contents: write}, reason: push}\n"))
	if err != nil {
		t.Fatal(err)
	}
	err = c.Check("homelab", d, ceilingField)
	if err == nil || !strings.Contains(err.Error(), "access[0].permissions.contents") || !strings.Contains(err.Error(), "at most contents read") {
		t.Fatalf("Check = %v, want contents write refused above read", err)
	}
}

func TestCeilingFromValueRefuses(t *testing.T) {
	for _, tc := range []struct{ doc, want string }{
		{"homelab: {repository: ortusnova/fleet-vm}\n", ceilingField + ".homelab: must be a list"},
		{"homelab:\n  - {repository: fleet-vm, permissions: {contents: write}}\n", ceilingField + ".homelab[0].repository"},
		{"homelab:\n  - {repository: ortusnova/fleet-vm, permissions: {contents: admin}}\n", ceilingField + ".homelab[0].permissions.contents"},
		{"homelab:\n  - {repository: ortusnova/fleet-vm, permissions: {contents: write}, reason: x}\n", ceilingField + ".homelab[0].reason: unknown key"},
		{"homelab:\n  - {repository: ortusnova/a, permissions: {contents: read}}\n  - {repository: ortusnova/A, permissions: {contents: read}}\n", "already listed"},
		{"[1, 2]\n", ceilingField + ": must be a mapping"},
	} {
		var v any
		if err := yaml.Unmarshal([]byte(tc.doc), &v); err != nil {
			t.Fatal(err)
		}
		_, err := CeilingFromValue(v, ceilingField)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("CeilingFromValue(%q) = %v, want %q", tc.doc, err, tc.want)
		}
	}
	if c, err := CeilingFromValue(nil, ceilingField); err != nil || len(c) != 0 {
		t.Errorf("CeilingFromValue(nil) = %v, %v, want an empty ceiling", c, err)
	}
}
