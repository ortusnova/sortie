package forge

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

// schemaPath is the published format.
const schemaPath = "../../docs/forge.schema.json"

// schemaValidator checks a decoded YAML or JSON value against the
// subset of JSON Schema docs/forge.schema.json uses. A keyword it does
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
		case "enum":
			if !slices.Contains(rule.([]any), value) {
				err = fmt.Errorf("%s: %v is not one of %v", at, value, rule)
			}
		case "required":
			m, ok := value.(map[string]any)
			for _, name := range rule.([]any) {
				if _, has := m[name.(string)]; ok && !has && err == nil {
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
				if _, known := props[name]; !known && rule == false && err == nil {
					err = fmt.Errorf("%s: unknown key %s", at, name)
				}
			}
		case "minProperties":
			if m, ok := value.(map[string]any); ok && float64(len(m)) < rule.(float64) {
				err = fmt.Errorf("%s: fewer than %v keys", at, rule)
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
		case "minLength":
			if s, ok := value.(string); ok && float64(utf8.RuneCountInString(s)) < rule.(float64) {
				err = fmt.Errorf("%s: %q breaks minLength %v", at, s, rule)
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
	if err := v.validate(map[string]any{"version": 1, "access": []any{}}); err != nil {
		t.Errorf("the smallest declaration: %v", err)
	}
}

// The schema names exactly the permissions the parser knows, each at the
// levels the parser takes.
func TestSchemaPermissionsAreTheParsers(t *testing.T) {
	v := loadSchema(t)
	access := v.resolve("#/$defs/access")
	props := access["properties"].(map[string]any)["permissions"].(map[string]any)["properties"].(map[string]any)
	var names []string
	for name, sub := range props {
		names = append(names, name)
		def := v.resolve(sub.(map[string]any)["$ref"].(string))
		var levels []string
		for _, l := range def["enum"].([]any) {
			levels = append(levels, l.(string))
		}
		if !reflect.DeepEqual(levels, permissionLevels[name]) {
			t.Errorf("schema levels of %s = %v, parser's %v", name, levels, permissionLevels[name])
		}
	}
	slices.Sort(names)
	if !reflect.DeepEqual(names, Permissions()) {
		t.Errorf("schema permissions = %v\nparser's %v", names, Permissions())
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

// referenceExamples returns each declaration file and each
// environment.remote.forge_ceiling the workflow reference shows, by where
// it appears.
func referenceExamples(t *testing.T) (files, ceilings map[string]any) {
	t.Helper()
	raw, err := os.ReadFile(workflowReference)
	if err != nil {
		t.Fatal(err)
	}
	files, ceilings = map[string]any{}, map[string]any{}
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
			if _, hasVersion := root["version"]; hasVersion && root["access"] != nil {
				files[fmt.Sprintf("the file at line %d", start)] = root
			}
			env, _ := root["environment"].(map[string]any)
			remote, _ := env["remote"].(map[string]any)
			if c, ok := remote["forge_ceiling"]; ok {
				ceilings[fmt.Sprintf("environment.remote.forge_ceiling at line %d", start)] = c
			}
		case inBlock:
			block = append(block, line)
		}
	}
	return files, ceilings
}

// Every declaration the workflow reference shows is valid against the
// published schema, and Sortie accepts it; so is every ceiling it shows.
func TestWorkflowReferenceExamplesMatchTheSchema(t *testing.T) {
	v := loadSchema(t)
	files, ceilings := referenceExamples(t)
	for where, doc := range files {
		if err := v.validate(doc); err != nil {
			t.Errorf("%s: the schema refuses it: %v", where, err)
		}
		if _, err := FromValue(doc, ""); err != nil {
			t.Errorf("%s: Sortie refuses it: %v", where, err)
		}
	}
	for where, doc := range ceilings {
		if _, err := CeilingFromValue(doc, "environment.remote.forge_ceiling"); err != nil {
			t.Errorf("%s: Sortie refuses it: %v", where, err)
		}
	}
	if len(files) == 0 || len(ceilings) == 0 {
		t.Fatalf("found %d file and %d environment.remote.forge_ceiling examples in %s, want at least one of each", len(files), len(ceilings), workflowReference)
	}
}

// forge-access: Invalid entries. The schema refuses each invalid entry
// of the scenario, naming it, and so does Sortie.
func TestSchemaRefusesTheInvalidEntries(t *testing.T) {
	v := loadSchema(t)
	for _, c := range []struct{ doc, schemaNames, parserNames string }{
		{"version: 1\naccess:\n  - {repository: ortusnova/fleet-vm, permissions: {contents: write}}\n",
			"access[0]: reason is required", "access[0].reason"},
		{"version: 1\naccess:\n  - {repository: ortusnova/fleet-vm, permissions: {contents: admin}, reason: all}\n",
			"access[0].permissions.contents: admin", "access[0].permissions.contents"},
		{"version: 1\naccess:\n  - {repository: ortusnova/sortie, permissions: {workflows: read}, reason: CI}\n",
			"access[0].permissions.workflows: read", "access[0].permissions.workflows"},
		{"version: 1\naccess:\n  - {repository: fleet-vm, permissions: {contents: write}, reason: mirror}\n",
			`access[0].repository: "fleet-vm"`, "access[0].repository"},
		{"version: 1\naccess:\n  - {repository: ortusnova/fleet-vm, permissions: {contents: write}, reason: mirror, branches: [seed]}\n",
			"access[0]: unknown key branches", "access[0].branches"},
	} {
		err := v.validate(decodeYAML(t, c.doc))
		if err == nil || !strings.Contains(err.Error(), c.schemaNames) {
			t.Errorf("schema on %q = %v, want a refusal naming %s", c.doc, err, c.schemaNames)
		}
		_, err = Parse([]byte(c.doc))
		if err == nil || !strings.Contains(err.Error(), c.parserNames) {
			t.Errorf("Parse(%q) = %v, want a refusal naming %s", c.doc, err, c.parserNames)
		}
	}
}
