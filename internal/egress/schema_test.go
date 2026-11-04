package egress

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

// schemaPath is the published format.
const schemaPath = "../../docs/egress.schema.json"

// schemaValidator checks a decoded YAML or JSON value against the
// subset of JSON Schema docs/egress.schema.json uses. A keyword it does
// not know fails the check, so the schema cannot outgrow it unnoticed.
type schemaValidator struct {
	root map[string]any
}

func loadSchema(t *testing.T) schemaValidator {
	t.Helper()
	raw, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatal(err)
	}
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		t.Fatalf("%s: %v", schemaPath, err)
	}
	return schemaValidator{root: root}
}

func (v schemaValidator) validate(value any) error {
	return v.check(v.root, value, "")
}

var annotationKeywords = map[string]bool{"$schema": true, "$id": true, "$defs": true, "title": true, "description": true}

func (v schemaValidator) check(schema map[string]any, value any, at string) error {
	for key, rule := range schema {
		if annotationKeywords[key] {
			continue
		}
		var err error
		switch key {
		case "$ref":
			err = v.check(v.resolve(rule.(string)), value, at)
		case "type":
			if !hasType(value, rule.(string)) {
				err = fmt.Errorf("%s: want %s, got %T", at, rule, value)
			}
		case "const":
			if n, ok := integer(value); !ok || float64(n) != rule.(float64) {
				err = fmt.Errorf("%s: want %v, got %v", at, rule, value)
			}
		case "required":
			m, ok := value.(map[string]any)
			for _, name := range rule.([]any) {
				if _, has := m[name.(string)]; ok && !has {
					err = fmt.Errorf("%s: %s is required", at, name)
				}
			}
		case "properties":
			m, _ := value.(map[string]any)
			for name, sub := range rule.(map[string]any) {
				if child, has := m[name]; has && err == nil {
					err = v.check(sub.(map[string]any), child, at+"."+name)
				}
			}
		case "additionalProperties":
			m, _ := value.(map[string]any)
			props, _ := schema["properties"].(map[string]any)
			for name := range m {
				if _, known := props[name]; !known && rule == false {
					err = fmt.Errorf("%s: unknown key %s", at, name)
				}
			}
		case "items":
			list, _ := value.([]any)
			for i, item := range list {
				if err == nil {
					err = v.check(rule.(map[string]any), item, fmt.Sprintf("%s[%d]", at, i))
				}
			}
		case "pattern":
			if s, ok := value.(string); ok && !regexp.MustCompile(rule.(string)).MatchString(s) {
				err = fmt.Errorf("%s: %q does not match %s", at, s, rule)
			}
		case "minLength", "maxLength":
			if s, ok := value.(string); ok {
				n := utf8.RuneCountInString(s)
				if (key == "minLength" && float64(n) < rule.(float64)) || (key == "maxLength" && float64(n) > rule.(float64)) {
					err = fmt.Errorf("%s: %q breaks %s %v", at, s, key, rule)
				}
			}
		case "minimum", "maximum":
			if n, ok := integer(value); ok {
				if (key == "minimum" && float64(n) < rule.(float64)) || (key == "maximum" && float64(n) > rule.(float64)) {
					err = fmt.Errorf("%s: %v breaks %s %v", at, value, key, rule)
				}
			}
		case "anyOf":
			err = fmt.Errorf("%s: %v matches no alternative", at, value)
			for _, alt := range rule.([]any) {
				if v.check(alt.(map[string]any), value, at) == nil {
					err = nil
					break
				}
			}
		case "not":
			if v.check(rule.(map[string]any), value, at) == nil {
				err = fmt.Errorf("%s: %v matches %v", at, value, rule)
			}
		default:
			return fmt.Errorf("the test's validator does not know the keyword %q", key)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (v schemaValidator) resolve(ref string) map[string]any {
	name, ok := strings.CutPrefix(ref, "#/$defs/")
	if !ok {
		panic("unsupported $ref " + ref)
	}
	return v.root["$defs"].(map[string]any)[name].(map[string]any)
}

func hasType(value any, typ string) bool {
	switch typ {
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	case "string":
		_, ok := value.(string)
		return ok
	case "integer":
		_, ok := integer(value)
		return ok
	case "number":
		switch value.(type) {
		case int, int64, uint64, float64:
			return true
		}
	}
	return false
}

func decodeYAML(t *testing.T, doc string) any {
	t.Helper()
	var v any
	if err := yaml.Unmarshal([]byte(doc), &v); err != nil {
		t.Fatalf("decode %q: %v", doc, err)
	}
	return v
}

func TestSchemaIsValidJSON(t *testing.T) {
	v := loadSchema(t)
	if v.root["$schema"] != "https://json-schema.org/draft/2020-12/schema" {
		t.Errorf("$schema = %v", v.root["$schema"])
	}
	if err := v.validate(map[string]any{"version": 1, "hosts": []any{}}); err != nil {
		t.Errorf("the smallest policy: %v", err)
	}
}

// The schema and the parser agree on every case: what the parser
// accepts the schema accepts, and every case the schema can state that
// the parser refuses the schema refuses.
func TestSchemaAgreesWithTheParser(t *testing.T) {
	v := loadSchema(t)
	for _, c := range parseCases {
		t.Run(c.name, func(t *testing.T) {
			var doc any
			if err := yaml.Unmarshal([]byte(c.doc), &doc); err != nil {
				t.Skip("not YAML a schema can read")
			}
			_, parseErr := Parse([]byte(c.doc))
			schemaErr := v.validate(doc)
			if parseErr == nil && schemaErr != nil {
				t.Errorf("the parser accepts it, the schema refuses it: %v", schemaErr)
			}
			if parseErr != nil && c.schemaStates && schemaErr == nil {
				t.Errorf("the parser refuses it (%v), the schema accepts it", parseErr)
			}
		})
	}
}

// workflowReference documents the format with examples.
const workflowReference = "../../docs/workflow-reference.md"

// repositoryPolicy is this repository's own policy file.
var repositoryPolicy = "../../" + File

// referenceExamples returns each egress policy the workflow reference
// shows, by where it appears: every yaml block that is a policy file, and
// environment.remote.egress of every yaml block that sets it.
func referenceExamples(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(workflowReference)
	if err != nil {
		t.Fatal(err)
	}
	examples := map[string]any{}
	var block []string
	inBlock, start := false, 0
	for i, line := range strings.Split(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case !inBlock && trimmed == "```yaml":
			inBlock, start, block = true, i+1, nil
		case inBlock && trimmed == "```":
			inBlock = false
			var doc any
			if yaml.Unmarshal([]byte(strings.Join(block, "\n")), &doc) != nil {
				continue
			}
			root, _ := doc.(map[string]any)
			if _, hasVersion := root["version"]; hasVersion && root["hosts"] != nil {
				examples[fmt.Sprintf("the file at line %d", start)] = root
			}
			env, _ := root["environment"].(map[string]any)
			remote, _ := env["remote"].(map[string]any)
			if harness, ok := remote["egress"]; ok {
				examples[fmt.Sprintf("environment.remote.egress at line %d", start)] = harness
			}
		case inBlock:
			block = append(block, line)
		}
	}
	return examples
}

// Every example of the workflow reference is valid against the
// published schema, and Sortie accepts it.
func TestWorkflowReferenceExamplesMatchTheSchema(t *testing.T) {
	v := loadSchema(t)
	examples := referenceExamples(t)
	var files, harness int
	for where, doc := range examples {
		if strings.HasPrefix(where, "the file") {
			files++
		} else {
			harness++
		}
		if err := v.validate(doc); err != nil {
			t.Errorf("%s: the schema refuses it: %v", where, err)
		}
		if _, err := FromValue(doc, ""); err != nil {
			t.Errorf("%s: Sortie refuses it: %v", where, err)
		}
	}
	if files == 0 || harness == 0 {
		t.Fatalf("found %d file and %d environment.remote.egress examples in %s, want at least one of each", files, harness, workflowReference)
	}
}

// job-egress-policy: Invalid entries. The schema refuses each invalid
// entry of the scenario, naming it.
func TestSchemaRefusesTheInvalidEntries(t *testing.T) {
	v := loadSchema(t)
	for _, c := range []struct{ doc, names string }{
		{"version: 1\nhosts:\n  - {host: \"*\", reason: all}\n", `hosts[0].host: "*"`},
		{"version: 1\nhosts:\n  - {host: \"ghcr.io:443\", reason: images}\n", `hosts[0].host: "ghcr.io:443"`},
		{"version: 1\nhosts:\n  - {host: ghcr.io}\n", "hosts[0]: reason is required"},
		{"version: 1\nhosts: []\nprivate:\n  - {cidr: 127.0.0.0/8, reason: local}\n", `private[0].cidr: "127.0.0.0/8"`},
		{"version: 1\nhosts: []\nports: [443]\n", "unknown key ports"},
	} {
		err := v.validate(decodeYAML(t, c.doc))
		if err == nil || !strings.Contains(err.Error(), c.names) {
			t.Errorf("schema on %q = %v, want a refusal naming %s", c.doc, err, c.names)
		}
	}
}

// This repository declares the hosts its builds and tests reach, each
// with its reason, and the file is valid against the schema.
func TestRepositoryPolicyMatchesTheSchema(t *testing.T) {
	raw, err := os.ReadFile(repositoryPolicy)
	if err != nil {
		t.Fatal(err)
	}
	if err := loadSchema(t).validate(decodeYAML(t, string(raw))); err != nil {
		t.Errorf("%s: the schema refuses it: %v", repositoryPolicy, err)
	}
	p, err := Parse(raw)
	if err != nil {
		t.Fatalf("%s: %v", repositoryPolicy, err)
	}
	for _, host := range []string{"proxy.golang.org", "sum.golang.org"} {
		if !slices.Contains(p.HostNames(), host) {
			t.Errorf("%s lists no %s", repositoryPolicy, host)
		}
	}
}
