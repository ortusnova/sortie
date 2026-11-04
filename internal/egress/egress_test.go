package egress

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

// parseCase is one policy file. A refused one names, in its error, each
// of wantErr. schemaStates marks a refusal the schema states as well.
type parseCase struct {
	name         string
	doc          string
	wantErr      []string
	schemaStates bool
}

var parseCases = []parseCase{
	{name: "a valid file", doc: `version: 1
hosts:
  - host: deb.debian.org
    reason: apt installs the build's packages
udp:
  - cidr: 0.0.0.0/0
    port: 123
    reason: NTP keeps the machine's clock
`},
	{name: "every list", doc: `version: 1
hosts:
  - {host: proxy.golang.org, reason: go downloads modules}
  - {host: "*.githubusercontent.com", reason: release assets}
udp:
  - {cidr: 192.168.1.0/24, port: 5000-5010, reason: the lab's media ports}
private:
  - {cidr: 10.20.0.0/16, reason: the lab's registry}
  - {cidr: "fd00:1::/64", reason: the lab's IPv6 network}
`},
	{name: "no hosts", doc: "version: 1\nhosts: []\n"},
	{name: "a bare wildcard", doc: "version: 1\nhosts:\n  - {host: \"*\", reason: everything}\n",
		wantErr: []string{"hosts[0].host", `"*"`, "bare wildcard"}, schemaStates: true},
	{name: "a host with a port", doc: "version: 1\nhosts:\n  - {host: \"ghcr.io:443\", reason: images}\n",
		wantErr: []string{"hosts[0].host", `"ghcr.io:443"`, "has a port"}, schemaStates: true},
	{name: "an entry with no reason", doc: "version: 1\nhosts:\n  - {host: ghcr.io, reason: images}\n  - {host: deb.debian.org}\n",
		wantErr: []string{"hosts[1].reason", "deb.debian.org", "is required"}, schemaStates: true},
	{name: "an empty reason", doc: "version: 1\nhosts:\n  - {host: ghcr.io, reason: \"  \"}\n",
		wantErr: []string{"hosts[0].reason", "must not be empty"}, schemaStates: true},
	{name: "a loopback private range", doc: "version: 1\nhosts: []\nprivate:\n  - {cidr: 127.0.0.0/8, reason: local}\n",
		wantErr: []string{"private[0].cidr", "127.0.0.0/8", "loopback"}, schemaStates: true},
	{name: "a link-local private range", doc: "version: 1\nhosts: []\nprivate:\n  - {cidr: 169.254.0.0/16, reason: metadata}\n",
		wantErr: []string{"private[0].cidr", "link-local"}, schemaStates: true},
	{name: "a private range wider than a private block", doc: "version: 1\nhosts: []\nprivate:\n  - {cidr: 0.0.0.0/0, reason: all}\n",
		wantErr: []string{"private[0].cidr", "loopback"}, schemaStates: true},
	{name: "a public private range", doc: "version: 1\nhosts: []\nprivate:\n  - {cidr: 8.8.8.0/24, reason: dns}\n",
		wantErr: []string{"private[0].cidr", "not a private range"}, schemaStates: true},
	{name: "an unknown key", doc: "version: 1\nhosts: []\nports: [22]\n",
		wantErr: []string{"ports", "unknown key"}, schemaStates: true},
	{name: "an unknown entry key", doc: "version: 1\nhosts:\n  - {host: ghcr.io, reason: images, port: 443}\n",
		wantErr: []string{"hosts[0].port", "unknown key"}, schemaStates: true},
	{name: "a duplicate host", doc: "version: 1\nhosts:\n  - {host: ghcr.io, reason: images}\n  - {host: ghcr.io, reason: again}\n",
		wantErr: []string{"hosts[1].host", "already listed at hosts[0]"}},
	{name: "a duplicate udp entry", doc: "version: 1\nhosts: []\nudp:\n  - {cidr: 0.0.0.0/0, port: 123, reason: ntp}\n  - {cidr: 0.0.0.0/0, port: \"123\", reason: ntp}\n",
		wantErr: []string{"udp[1]", "already listed at udp[0]"}},
	{name: "a duplicate private entry", doc: "version: 1\nhosts: []\nprivate:\n  - {cidr: 10.0.0.0/8, reason: lab}\n  - {cidr: 10.0.0.0/8, reason: lab}\n",
		wantErr: []string{"private[1].cidr", "already listed at private[0]"}},
	{name: "a duplicate key", doc: "version: 1\nhosts: []\nhosts: []\n", wantErr: []string{"hosts", "already defined"}},
	{name: "no version", doc: "hosts: []\n", wantErr: []string{"version", "is required"}, schemaStates: true},
	{name: "another version", doc: "version: 2\nhosts: []\n", wantErr: []string{"version", "must be 1"}, schemaStates: true},
	{name: "no hosts list", doc: "version: 1\n", wantErr: []string{"hosts", "is required"}, schemaStates: true},
	{name: "an IPv4 literal", doc: "version: 1\nhosts:\n  - {host: 10.0.0.1, reason: lab}\n",
		wantErr: []string{"hosts[0].host", "IP literal"}, schemaStates: true},
	{name: "an IPv6 literal", doc: "version: 1\nhosts:\n  - {host: \"::1\", reason: lab}\n",
		wantErr: []string{"hosts[0].host", "IP literal"}, schemaStates: true},
	{name: "an uppercase host", doc: "version: 1\nhosts:\n  - {host: GHCR.io, reason: images}\n",
		wantErr: []string{"hosts[0].host", "lowercase"}, schemaStates: true},
	{name: "a wildcard over a top-level domain", doc: "version: 1\nhosts:\n  - {host: \"*.io\", reason: all of io}\n",
		wantErr: []string{"hosts[0].host", "at least two labels"}, schemaStates: true},
	{name: "an inner wildcard", doc: "version: 1\nhosts:\n  - {host: \"api.*.example.com\", reason: x}\n",
		wantErr: []string{"hosts[0].host", "wildcard"}, schemaStates: true},
	{name: "a URL", doc: "version: 1\nhosts:\n  - {host: \"https://ghcr.io\", reason: images}\n",
		wantErr: []string{"hosts[0].host", "URL"}, schemaStates: true},
	{name: "a port out of range", doc: "version: 1\nhosts: []\nudp:\n  - {cidr: 0.0.0.0/0, port: 70000, reason: x}\n",
		wantErr: []string{"udp[0].port", "70000"}, schemaStates: true},
	{name: "a backward port range", doc: "version: 1\nhosts: []\nudp:\n  - {cidr: 0.0.0.0/0, port: 2000-1000, reason: x}\n",
		wantErr: []string{"udp[0].port", "starts above its end"}},
	{name: "a udp entry with no port", doc: "version: 1\nhosts: []\nudp:\n  - {cidr: 0.0.0.0/0, reason: x}\n",
		wantErr: []string{"udp[0].port", "is required"}, schemaStates: true},
	{name: "a CIDR with host bits", doc: "version: 1\nhosts: []\nprivate:\n  - {cidr: 10.0.0.1/8, reason: lab}\n",
		wantErr: []string{"private[0].cidr", "host bits", "10.0.0.0/8"}},
	{name: "not a CIDR", doc: "version: 1\nhosts: []\nprivate:\n  - {cidr: lab, reason: lab}\n",
		wantErr: []string{"private[0].cidr", "not a CIDR"}, schemaStates: true},
	{name: "hosts not a list", doc: "version: 1\nhosts: ghcr.io\n", wantErr: []string{"hosts", "must be a list"}, schemaStates: true},
}

func TestParse(t *testing.T) {
	for _, c := range parseCases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse([]byte(c.doc))
			if len(c.wantErr) == 0 {
				if err != nil {
					t.Fatalf("Parse: %v", err)
				}
				return
			}
			if _, ok := errors.AsType[*Error](err); !ok {
				t.Fatalf("Parse = %v, want an *Error", err)
			}
			for _, want := range c.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not name %q", err, want)
				}
			}
		})
	}
}

// job-egress-policy: A valid file.
func TestParseAValidFile(t *testing.T) {
	p, err := Parse([]byte(parseCases[0].doc))
	if err != nil {
		t.Fatal(err)
	}
	want := Policy{
		Hosts:   []Host{{Host: "deb.debian.org", Reason: "apt installs the build's packages"}},
		UDP:     []UDP{{CIDR: "0.0.0.0/0", Port: "123", Reason: "NTP keeps the machine's clock"}},
		Private: []Private{},
	}
	if !reflect.DeepEqual(p, want) {
		t.Errorf("Parse = %+v, want %+v", p, want)
	}
}

// job-egress-policy: Invalid entries. Each entry of the scenario is
// refused, the error naming the entry and the reason.
func TestParseInvalidEntries(t *testing.T) {
	for _, c := range []struct{ doc, entry, reason string }{
		{"version: 1\nhosts:\n  - {host: \"*\", reason: all}\n", "hosts[0].host", "bare wildcard"},
		{"version: 1\nhosts:\n  - {host: \"ghcr.io:443\", reason: images}\n", "hosts[0].host", "has a port"},
		{"version: 1\nhosts:\n  - {host: ghcr.io}\n", "hosts[0].reason", "is required"},
		{"version: 1\nhosts: []\nprivate:\n  - {cidr: 127.0.0.0/8, reason: local}\n", "private[0].cidr", "overlaps loopback"},
		{"version: 1\nhosts: []\nports: [443]\n", "ports", "unknown key"},
	} {
		_, err := Parse([]byte(c.doc))
		var perr *Error
		if !errors.As(err, &perr) || perr.Entry != c.entry || !strings.Contains(perr.Message, c.reason) {
			t.Errorf("Parse(%q) = %v, want entry %s and %q", c.doc, err, c.entry, c.reason)
		}
	}
}

func TestFromValueNamesTheField(t *testing.T) {
	_, err := FromValue(map[string]any{"version": 1, "hosts": []any{map[string]any{"host": "models.opencode.ai"}}}, "environment.remote.egress")
	if err == nil || !strings.HasPrefix(err.Error(), "environment.remote.egress.hosts[0].reason: ") {
		t.Errorf("FromValue = %v, want the entry under environment.remote.egress", err)
	}
	_, err = FromValue("hosts", "environment.remote.egress")
	if err == nil || !strings.HasPrefix(err.Error(), "environment.remote.egress: ") {
		t.Errorf("FromValue of a string = %v", err)
	}
}

func TestParseLimits(t *testing.T) {
	if _, err := Parse(nil); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Errorf("Parse(nil) = %v", err)
	}
	if _, err := Parse([]byte("version: 1\nhosts: [")); err == nil || !strings.Contains(err.Error(), "not valid YAML") {
		t.Errorf("Parse of broken YAML = %v", err)
	}
	big := "version: 1\nhosts: []\n# " + strings.Repeat("x", MaxFileBytes) + "\n"
	if _, err := Parse([]byte(big)); err == nil || !strings.Contains(err.Error(), "over the limit") {
		t.Errorf("Parse of a large file = %v", err)
	}
}

func TestEmptyAndClone(t *testing.T) {
	e := Empty()
	if e.Hosts == nil || e.UDP == nil || e.Private == nil {
		t.Errorf("Empty() = %+v, want empty lists", e)
	}
	p := Policy{Hosts: []Host{{Host: "a.example", Reason: "r"}}}
	c := p.Clone()
	c.Hosts[0].Host = "b.example"
	if p.Hosts[0].Host != "a.example" || c.UDP == nil {
		t.Errorf("Clone shares its lists: %+v %+v", p, c)
	}
	if got := p.HostNames(); !reflect.DeepEqual(got, []string{"a.example"}) {
		t.Errorf("HostNames = %v", got)
	}
}
