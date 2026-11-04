// Package egress reads and checks a job's egress policy: the hosts, UDP
// destinations and private ranges a job's machine may reach. A
// repository declares its jobs' policy in [File] at its root, and the
// workflow declares the hosts of the agent harness in the same format.
// The format is published as a JSON Schema in docs/egress.schema.json;
// [Parse] and [FromValue] check every rule of that schema and the rules a
// schema cannot state, such as duplicate entries and private ranges.
package egress

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/sortie-ai/sortie/internal/workspacekit"
)

// File is the path of a repository's egress policy, relative to the
// repository root.
var File = treePath()

// treePath is the path of the policy in a Git tree, which always
// separates its elements with a slash.
func treePath() string {
	return path.Join(workspacekit.SortieDir, "egress.yaml")
}

// Version is the only format version.
const Version = 1

// MaxFileBytes bounds the policy file; a larger one is invalid.
const MaxFileBytes = 64 << 10

// Policy is one egress policy: the hosts the job may reach over the
// egress proxy, the UDP destinations it may reach directly, and the
// private ranges it may reach. Its lists are never nil once read.
type Policy struct {
	Hosts   []Host    `json:"hosts"`
	UDP     []UDP     `json:"udp"`
	Private []Private `json:"private"`
}

// Host is one host a job may reach: a DNS name, or "*." and a name that
// covers every name below it.
type Host struct {
	Host   string `json:"host"`
	Reason string `json:"reason"`
}

// UDP is one UDP destination a job may reach: a CIDR and a port or a
// port range "N-M".
type UDP struct {
	CIDR   string `json:"cidr"`
	Port   string `json:"port"`
	Reason string `json:"reason"`
}

// Private is one private range a job may reach.
type Private struct {
	CIDR   string `json:"cidr"`
	Reason string `json:"reason"`
}

// Empty returns the policy with no entries, its lists empty and not nil.
func Empty() Policy {
	return Policy{Hosts: []Host{}, UDP: []UDP{}, Private: []Private{}}
}

// Clone returns a copy of p that shares no list with it.
func (p Policy) Clone() Policy {
	out := Empty()
	out.Hosts = append(out.Hosts, p.Hosts...)
	out.UDP = append(out.UDP, p.UDP...)
	out.Private = append(out.Private, p.Private...)
	return out
}

// HostNames returns the host of each entry of p, in order.
func (p Policy) HostNames() []string {
	out := make([]string, 0, len(p.Hosts))
	for _, h := range p.Hosts {
		out = append(out, h.Host)
	}
	return out
}

// Error is an invalid policy: Entry names the key or the entry, such as
// "hosts[1].host", and Message why it is invalid.
type Error struct {
	Entry   string
	Message string
}

func (e *Error) Error() string {
	if e.Entry == "" {
		return e.Message
	}
	return e.Entry + ": " + e.Message
}

// topKeys are the keys of a policy.
var topKeys = []string{"version", "hosts", "udp", "private"}

// Parse reads the policy file data. An invalid file returns an [*Error]
// that names the entry and the reason.
func Parse(data []byte) (Policy, error) {
	if len(data) > MaxFileBytes {
		return Policy{}, &Error{Message: fmt.Sprintf("the file is %d bytes, over the limit of %d", len(data), MaxFileBytes)}
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return Policy{}, &Error{Message: "the file is empty; write version: 1 and a hosts list"}
	}
	var doc any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return Policy{}, &Error{Message: "not valid YAML: " + strings.TrimPrefix(err.Error(), "yaml: ")}
	}
	return FromValue(doc, "")
}

// FromValue reads a policy already decoded from YAML, such as the
// workflow's environment.remote.egress. field prefixes the entry each
// error names; empty names the entries from the root of the file.
func FromValue(value any, field string) (Policy, error) {
	m, ok := value.(map[string]any)
	if !ok {
		return Policy{}, &Error{Entry: field, Message: fmt.Sprintf("must be a mapping with version: 1 and a hosts list, got %s", kindOf(value))}
	}
	r := reader{field: field}
	if err := r.unknownKeys(m, "", topKeys); err != nil {
		return Policy{}, err
	}
	if err := r.version(m); err != nil {
		return Policy{}, err
	}
	p := Empty()
	rawHosts, ok := m["hosts"]
	if !ok || rawHosts == nil {
		return Policy{}, r.errf("hosts", "is required: list each host the jobs reach, or write hosts: []")
	}
	hosts, err := r.list(rawHosts, "hosts")
	if err != nil {
		return Policy{}, err
	}
	seenHost := map[string]int{}
	for i, raw := range hosts {
		h, err := r.host(raw, fmt.Sprintf("hosts[%d]", i))
		if err != nil {
			return Policy{}, err
		}
		if first, dup := seenHost[h.Host]; dup {
			return Policy{}, r.errf(fmt.Sprintf("hosts[%d].host", i), "%q is already listed at hosts[%d]", h.Host, first)
		}
		seenHost[h.Host] = i
		p.Hosts = append(p.Hosts, h)
	}
	if raw, ok := m["udp"]; ok && raw != nil {
		entries, err := r.list(raw, "udp")
		if err != nil {
			return Policy{}, err
		}
		seen := map[string]int{}
		for i, raw := range entries {
			u, err := r.udp(raw, fmt.Sprintf("udp[%d]", i))
			if err != nil {
				return Policy{}, err
			}
			key := u.CIDR + " " + u.Port
			if first, dup := seen[key]; dup {
				return Policy{}, r.errf(fmt.Sprintf("udp[%d]", i), "%s port %s is already listed at udp[%d]", u.CIDR, u.Port, first)
			}
			seen[key] = i
			p.UDP = append(p.UDP, u)
		}
	}
	if raw, ok := m["private"]; ok && raw != nil {
		entries, err := r.list(raw, "private")
		if err != nil {
			return Policy{}, err
		}
		seen := map[string]int{}
		for i, raw := range entries {
			pr, err := r.private(raw, fmt.Sprintf("private[%d]", i))
			if err != nil {
				return Policy{}, err
			}
			if first, dup := seen[pr.CIDR]; dup {
				return Policy{}, r.errf(fmt.Sprintf("private[%d].cidr", i), "%s is already listed at private[%d]", pr.CIDR, first)
			}
			seen[pr.CIDR] = i
			p.Private = append(p.Private, pr)
		}
	}
	return p, nil
}

// reader names each entry under its field.
type reader struct{ field string }

func (r reader) path(entry string) string {
	switch {
	case r.field == "":
		return entry
	case entry == "":
		return r.field
	case strings.HasPrefix(entry, "["):
		return r.field + entry
	}
	return r.field + "." + entry
}

func (r reader) errf(entry, format string, args ...any) error {
	return &Error{Entry: r.path(entry), Message: fmt.Sprintf(format, args...)}
}

func (r reader) unknownKeys(m map[string]any, entry string, known []string) error {
	var unknown []string
	for key := range m {
		if !slices.Contains(known, key) {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	slices.Sort(unknown)
	key := unknown[0]
	if entry != "" {
		key = entry + "." + key
	}
	return r.errf(key, "unknown key; valid: %s", strings.Join(known, ", "))
}

func (r reader) version(m map[string]any) error {
	raw, ok := m["version"]
	if !ok || raw == nil {
		return r.errf("version", "is required: write version: %d", Version)
	}
	n, ok := integer(raw)
	if !ok || n != Version {
		return r.errf("version", "must be %d, got %v", Version, raw)
	}
	return nil
}

func (r reader) list(raw any, entry string) ([]any, error) {
	list, ok := raw.([]any)
	if !ok {
		return nil, r.errf(entry, "must be a list, got %s", kindOf(raw))
	}
	return list, nil
}

func (r reader) mapping(raw any, entry string, known []string) (map[string]any, error) {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, r.errf(entry, "must be a mapping with %s, got %s", strings.Join(known, " and "), kindOf(raw))
	}
	return m, r.unknownKeys(m, entry, known)
}

func (r reader) str(m map[string]any, key, entry string) (string, error) {
	raw, ok := m[key]
	if !ok || raw == nil {
		return "", r.errf(entry+"."+key, "is required")
	}
	s, ok := raw.(string)
	if !ok {
		return "", r.errf(entry+"."+key, "must be a string, got %s", kindOf(raw))
	}
	return s, nil
}

// reason reads the entry's reason, which must say something. what names
// the entry in the message.
func (r reader) reason(m map[string]any, entry, what string) (string, error) {
	raw, ok := m["reason"]
	if !ok || raw == nil {
		return "", r.errf(entry+".reason", "is required: say why the jobs reach %s", what)
	}
	s, ok := raw.(string)
	if !ok {
		return "", r.errf(entry+".reason", "must be a string, got %s", kindOf(raw))
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return "", r.errf(entry+".reason", "must not be empty: say why the jobs reach %s", what)
	}
	return s, nil
}

func (r reader) host(raw any, entry string) (Host, error) {
	m, err := r.mapping(raw, entry, []string{"host", "reason"})
	if err != nil {
		return Host{}, err
	}
	name, err := r.str(m, "host", entry)
	if err != nil {
		return Host{}, err
	}
	if msg := checkHost(name); msg != "" {
		return Host{}, r.errf(entry+".host", "%q %s", name, msg)
	}
	reason, err := r.reason(m, entry, name)
	if err != nil {
		return Host{}, err
	}
	return Host{Host: name, Reason: reason}, nil
}

var (
	labelPattern  = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
	numericLabel  = regexp.MustCompile(`^[0-9]+$`)
	maxNameLength = 253
)

// checkHost returns why name is not a host entry, or "".
func checkHost(name string) string {
	switch {
	case name == "":
		return "is empty: write a DNS name"
	case name == "*":
		return "is a bare wildcard: write *. followed by a name of at least two labels, such as *.example.com"
	case strings.Contains(name, "://") || strings.Contains(name, "/"):
		return "is a URL or has a path: write the DNS name alone"
	}
	if _, err := netip.ParseAddr(strings.Trim(name, "[]")); err == nil {
		return "is an IP literal: write a DNS name, or list the range under private or udp"
	}
	if strings.Contains(name, ":") {
		return "has a port: write the DNS name alone"
	}
	if strings.ToLower(name) != name {
		return "is not lowercase"
	}
	wildcard := strings.HasPrefix(name, "*.")
	rest := strings.TrimPrefix(name, "*.")
	if len(rest) > maxNameLength {
		return fmt.Sprintf("is longer than %d characters", maxNameLength)
	}
	labels := strings.Split(rest, ".")
	for _, label := range labels {
		if !labelPattern.MatchString(label) {
			if strings.Contains(label, "*") {
				return "has a wildcard other than a leading *."
			}
			return fmt.Sprintf("is not a DNS name: label %q must be 1 to 63 lowercase letters, digits and inner hyphens", label)
		}
	}
	if numericLabel.MatchString(labels[len(labels)-1]) {
		return "is an IP literal: write a DNS name, or list the range under private or udp"
	}
	if wildcard && len(labels) < 2 {
		return "covers a whole top-level domain: write *. followed by a name of at least two labels"
	}
	return ""
}

func (r reader) cidr(m map[string]any, entry string) (netip.Prefix, string, error) {
	s, err := r.str(m, "cidr", entry)
	if err != nil {
		return netip.Prefix{}, "", err
	}
	p, err := netip.ParsePrefix(s)
	if err != nil {
		return netip.Prefix{}, "", r.errf(entry+".cidr", "%q is not a CIDR such as 10.0.0.0/8", s)
	}
	if p.Masked() != p {
		return netip.Prefix{}, "", r.errf(entry+".cidr", "%q has host bits set: write %s", s, p.Masked())
	}
	return p, p.String(), nil
}

func (r reader) udp(raw any, entry string) (UDP, error) {
	m, err := r.mapping(raw, entry, []string{"cidr", "port", "reason"})
	if err != nil {
		return UDP{}, err
	}
	_, cidr, err := r.cidr(m, entry)
	if err != nil {
		return UDP{}, err
	}
	port, err := r.port(m, entry)
	if err != nil {
		return UDP{}, err
	}
	reason, err := r.reason(m, entry, cidr+" port "+port)
	if err != nil {
		return UDP{}, err
	}
	return UDP{CIDR: cidr, Port: port, Reason: reason}, nil
}

// port reads a port N or a range N-M, each from 1 to 65535 and N not
// above M, and returns it as written.
func (r reader) port(m map[string]any, entry string) (string, error) {
	field := entry + ".port"
	raw, ok := m["port"]
	if !ok || raw == nil {
		return "", r.errf(field, "is required: a port N or a range N-M")
	}
	if n, ok := integer(raw); ok {
		if n < 1 || n > 65535 {
			return "", r.errf(field, "%d is not a port from 1 to 65535", n)
		}
		return strconv.FormatInt(n, 10), nil
	}
	s, ok := raw.(string)
	if !ok {
		return "", r.errf(field, "must be a port N or a range N-M, got %s", kindOf(raw))
	}
	lo, hi, isRange := strings.Cut(s, "-")
	if !isRange {
		hi = lo
	}
	from, errLo := parsePort(lo)
	to, errHi := parsePort(hi)
	if errLo != nil || errHi != nil {
		return "", r.errf(field, "%q is not a port N or a range N-M of ports from 1 to 65535", s)
	}
	if from > to {
		return "", r.errf(field, "%q starts above its end", s)
	}
	return s, nil
}

func parsePort(s string) (int, error) {
	if s == "" || strings.TrimLeft(s, "0123456789") != "" {
		return 0, errors.New("not a number")
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > 65535 {
		return 0, errors.New("out of range")
	}
	return n, nil
}

// privateRanges are the ranges a private entry must fall in.
var privateRanges = []netip.Prefix{
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("fc00::/7"),
}

// refusedRanges are the ranges a private entry must not overlap.
var refusedRanges = []struct {
	prefix netip.Prefix
	name   string
}{
	{netip.MustParsePrefix("127.0.0.0/8"), "loopback"},
	{netip.MustParsePrefix("::1/128"), "loopback"},
	{netip.MustParsePrefix("169.254.0.0/16"), "link-local"},
	{netip.MustParsePrefix("fe80::/10"), "link-local"},
}

func (r reader) private(raw any, entry string) (Private, error) {
	m, err := r.mapping(raw, entry, []string{"cidr", "reason"})
	if err != nil {
		return Private{}, err
	}
	p, cidr, err := r.cidr(m, entry)
	if err != nil {
		return Private{}, err
	}
	for _, refused := range refusedRanges {
		if refused.prefix.Overlaps(p) {
			return Private{}, r.errf(entry+".cidr", "%s overlaps %s (%s): a private entry is a private range", cidr, refused.name, refused.prefix)
		}
	}
	inside := false
	for _, private := range privateRanges {
		if private.Bits() <= p.Bits() && private.Contains(p.Addr()) {
			inside = true
			break
		}
	}
	if !inside {
		return Private{}, r.errf(entry+".cidr", "%s is not a private range: it must fall in 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16 or fc00::/7", cidr)
	}
	reason, err := r.reason(m, entry, cidr)
	if err != nil {
		return Private{}, err
	}
	return Private{CIDR: cidr, Reason: reason}, nil
}

// integer reads a whole number as YAML decodes it.
func integer(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int64:
		return n, true
	case uint64:
		if n > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	case float64:
		if n == math.Trunc(n) && math.Abs(n) < 1<<53 {
			return int64(n), true
		}
	}
	return 0, false
}

func kindOf(v any) string {
	switch v.(type) {
	case nil:
		return "nothing"
	case string:
		return "a string"
	case bool:
		return "a boolean"
	case int, int64, uint64, float64:
		return "a number"
	case []any:
		return "a list"
	case map[string]any:
		return "a mapping"
	}
	return fmt.Sprintf("%T", v)
}
