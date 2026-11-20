package forge

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

// parseCase is one declaration file. A refused one names, in its error,
// each of wantErr. schemaStates marks a refusal the schema states as well.
type parseCase struct {
	name         string
	doc          string
	wantErr      []string
	schemaStates bool
}

var parseCases = []parseCase{
	{name: "a valid file", doc: `version: 1
access:
  - repository: ortusnova/fleet-vm
    permissions: {contents: write}
    reason: "fleet mirror: push this seed's branch seed/<worktree>"
`},
	{name: "two owners", doc: `version: 1
access:
  - repository: ortusnova/homelab
    permissions: {contents: read, pull_requests: write}
    reason: read the base branch and comment on the story's pull request
  - repository: briancappello/personal-site
    permissions: {issues: write, metadata: read}
    reason: open an issue for what the job finds
`},
	{name: "workflows write", doc: "version: 1\naccess:\n  - {repository: ortusnova/sortie, permissions: {workflows: write}, reason: CI}\n"},
	{name: "no access", doc: "version: 1\naccess: []\n"},
	{name: "an entry with no reason", doc: "version: 1\naccess:\n  - {repository: ortusnova/fleet-vm, permissions: {contents: write}}\n",
		wantErr: []string{"access[0].reason", "ortusnova/fleet-vm", "is required"}, schemaStates: true},
	{name: "an empty reason", doc: "version: 1\naccess:\n  - {repository: ortusnova/fleet-vm, permissions: {contents: write}, reason: \"  \"}\n",
		wantErr: []string{"access[0].reason", "must not be empty"}, schemaStates: true},
	{name: "contents admin", doc: "version: 1\naccess:\n  - {repository: ortusnova/fleet-vm, permissions: {contents: admin}, reason: all}\n",
		wantErr: []string{"access[0].permissions.contents", "must be read or write", "admin"}, schemaStates: true},
	{name: "workflows read", doc: "version: 1\naccess:\n  - {repository: ortusnova/sortie, permissions: {workflows: read}, reason: CI}\n",
		wantErr: []string{"access[0].permissions.workflows", "must be write", "read"}, schemaStates: true},
	{name: "a repository with no owner", doc: "version: 1\naccess:\n  - {repository: fleet-vm, permissions: {contents: write}, reason: mirror}\n",
		wantErr: []string{"access[0].repository", `"fleet-vm"`, "has no owner"}, schemaStates: true},
	{name: "a repository with a path", doc: "version: 1\naccess:\n  - {repository: ortusnova/fleet/vm, permissions: {contents: write}, reason: mirror}\n",
		wantErr: []string{"access[0].repository", "more than one /"}, schemaStates: true},
	{name: "a repository URL", doc: "version: 1\naccess:\n  - {repository: \"https://github.com/ortusnova/fleet\", permissions: {contents: read}, reason: mirror}\n",
		wantErr: []string{"access[0].repository"}, schemaStates: true},
	{name: "a key branches", doc: "version: 1\naccess:\n  - {repository: ortusnova/fleet-vm, permissions: {contents: write}, reason: mirror, branches: [seed/*]}\n",
		wantErr: []string{"access[0].branches", "unknown key"}, schemaStates: true},
	{name: "an unknown top key", doc: "version: 1\naccess: []\ntokens: 2\n",
		wantErr: []string{"tokens", "unknown key"}, schemaStates: true},
	{name: "an unknown permission", doc: "version: 1\naccess:\n  - {repository: ortusnova/fleet-vm, permissions: {members: read}, reason: org}\n",
		wantErr: []string{"access[0].permissions.members", "unknown permission"}, schemaStates: true},
	{name: "no permissions", doc: "version: 1\naccess:\n  - {repository: ortusnova/fleet-vm, permissions: {}, reason: none}\n",
		wantErr: []string{"access[0].permissions", "at least one"}, schemaStates: true},
	{name: "a duplicate repository", doc: "version: 1\naccess:\n  - {repository: ortusnova/fleet-vm, permissions: {contents: read}, reason: a}\n  - {repository: OrtusNova/Fleet-VM, permissions: {contents: write}, reason: b}\n",
		wantErr: []string{"access[1].repository", "already listed at access[0]"}},
	{name: "no version", doc: "access: []\n", wantErr: []string{"version", "is required"}, schemaStates: true},
	{name: "version 2", doc: "version: 2\naccess: []\n", wantErr: []string{"version", "must be 1"}, schemaStates: true},
	{name: "no access list", doc: "version: 1\n", wantErr: []string{"access", "is required"}, schemaStates: true},
	{name: "an empty file", doc: "  \n", wantErr: []string{"empty"}},
	{name: "not YAML", doc: "version: [1\n", wantErr: []string{"not valid YAML"}},
}

func TestParse(t *testing.T) {
	for _, c := range parseCases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse([]byte(c.doc))
			if len(c.wantErr) == 0 {
				if err != nil {
					t.Fatalf("Parse = %v, want it accepted", err)
				}
				return
			}
			if _, ok := errors.AsType[*Error](err); !ok {
				t.Fatalf("Parse = %v, want an *Error", err)
			}
			for _, want := range c.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Parse = %q, want it to name %q", err, want)
				}
			}
		})
	}
}

// forge-access: A valid file.
func TestParseAValidFile(t *testing.T) {
	d, err := Parse([]byte(parseCases[0].doc))
	if err != nil {
		t.Fatal(err)
	}
	want := Declaration{Access: []Access{{Repository: "ortusnova/fleet-vm", Permissions: map[string]string{"contents": "write"},
		Reason: "fleet mirror: push this seed's branch seed/<worktree>"}}}
	if !reflect.DeepEqual(d, want) {
		t.Fatalf("Parse = %+v, want %+v", d, want)
	}
}

func TestParseRefusesAnOversizedFile(t *testing.T) {
	_, err := Parse([]byte("version: 1\naccess: []\n#" + strings.Repeat("x", MaxFileBytes)))
	if err == nil || !strings.Contains(err.Error(), "over the limit") {
		t.Fatalf("Parse = %v, want the size refused", err)
	}
}

// forge-access: No file. The default grants contents and metadata read
// on the repository itself only.
func TestDefault(t *testing.T) {
	d := Default("ortusnova/homelab")
	want := []Access{{Repository: "ortusnova/homelab", Permissions: map[string]string{"contents": "read", "metadata": "read"}, Reason: DefaultReason}}
	if !reflect.DeepEqual(d.Access, want) {
		t.Fatalf("Default = %+v, want %+v", d.Access, want)
	}
	if got := Default("homelab"); len(got.Access) != 0 {
		t.Fatalf("Default of a name that is not owner/name = %+v, want nothing", got)
	}
}

func TestCloneSharesNothing(t *testing.T) {
	d := Default("ortusnova/homelab")
	c := d.Clone()
	c.Access[0].Permissions["contents"] = Write
	if d.Access[0].Permissions["contents"] != Read {
		t.Fatal("Clone shares the permissions map")
	}
	g := d.Grant()
	gc := g.Clone()
	gc.Owners[0].Permissions["contents"] = Write
	gc.Owners[0].Repositories[0].Permissions["contents"] = Write
	if g.Owners[0].Permissions["contents"] != Read || g.Owners[0].Repositories[0].Permissions["contents"] != Read {
		t.Fatal("Grant.Clone shares a permissions map")
	}
}

// forge-access: Two owners. The grant has one entry per owner, each with
// its repositories and the permissions its one token carries.
func TestGrantGroupsByOwner(t *testing.T) {
	d, err := Parse([]byte(`version: 1
access:
  - {repository: ortusnova/homelab, permissions: {contents: read, pull_requests: write}, reason: a}
  - {repository: briancappello/personal-site, permissions: {contents: write}, reason: b}
  - {repository: ortusnova/fleet-vm, permissions: {contents: write}, reason: c}
`))
	if err != nil {
		t.Fatal(err)
	}
	want := Grant{Owners: []OwnerGrant{
		{Owner: "briancappello", Permissions: map[string]string{"contents": "write"},
			Repositories: []RepositoryGrant{{Repository: "briancappello/personal-site", Permissions: map[string]string{"contents": "write"}}}},
		{Owner: "ortusnova", Permissions: map[string]string{"contents": "write", "pull_requests": "write"},
			Repositories: []RepositoryGrant{
				{Repository: "ortusnova/fleet-vm", Permissions: map[string]string{"contents": "write"}},
				{Repository: "ortusnova/homelab", Permissions: map[string]string{"contents": "read", "pull_requests": "write"}},
			}},
	}}
	if got := d.Grant(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Grant = %+v\nwant %+v", got, want)
	}
	if got := Empty().Grant(); got.Owners == nil || len(got.Owners) != 0 {
		t.Fatalf("Grant of nothing = %+v, want an empty, non-nil list", got)
	}
}

func TestGitHubRepository(t *testing.T) {
	for _, c := range []struct{ provider, project, url, want string }{
		{"github", "ortusnova/homelab", "https://github.com/ortusnova/other.git", "ortusnova/homelab"},
		{"", "", "https://github.com/ortusnova/homelab.git", "ortusnova/homelab"},
		{"", "", "git@github.com:briancappello/personal-site.git", "briancappello/personal-site"},
		{"", "", "ssh://git@github.com/ortusnova/sortie", "ortusnova/sortie"},
		{"", "", "https://x-access-token:secret@github.com/ortusnova/sortie.git", "ortusnova/sortie"},
		{"gitea", "homelab/fleet", "https://gitea.local/homelab/fleet.git", ""},
		{"", "", "/srv/git/homelab.git", ""},
	} {
		if got := GitHubRepository(c.provider, c.project, c.url); got != c.want {
			t.Errorf("GitHubRepository(%q, %q, %q) = %q, want %q", c.provider, c.project, c.url, got, c.want)
		}
	}
}
