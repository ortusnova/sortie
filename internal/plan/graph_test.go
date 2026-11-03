package plan

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func encode(t *testing.T, g any) []byte {
	t.Helper()
	raw, err := json.Marshal(g)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestDecodeValidGraph(t *testing.T) {
	t.Parallel()

	g := newGraph().epic("billing", ApprovalApproved, "invoice", "tax").block("billing/invoice", "billing/tax").build()
	got, err := Decode(encode(t, g))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if len(got.Nodes) != 3 || len(got.Blocks) != 1 || got.SchemaVersion != 1 {
		t.Errorf("Decode() = %+v", got)
	}
}

func decodeErr(t *testing.T, raw []byte) string {
	t.Helper()
	_, err := Decode(raw)
	if _, ok := errors.AsType[*GraphError](err); !ok {
		t.Fatalf("Decode() error = %v, want *GraphError", err)
	}
	return err.Error()
}

func TestDecodeRejectsUnsupportedVersion(t *testing.T) {
	t.Parallel()

	g := newGraph().build()
	g.SchemaVersion = 3
	msg := decodeErr(t, encode(t, g))
	if !strings.Contains(msg, "schema_version 3") || !strings.Contains(msg, "versions 1 to 2") {
		t.Errorf("error %q does not name both versions", msg)
	}
}

func TestDecodeReadsVersionsOneAndTwo(t *testing.T) {
	t.Parallel()
	for _, v := range []int{1, 2} {
		g := newGraph().build()
		g.SchemaVersion = v
		if _, err := Decode(encode(t, g)); err != nil {
			t.Errorf("Decode(version %d) error = %v", v, err)
		}
	}
}

func TestDecodeVersion2StoryFields(t *testing.T) {
	t.Parallel()
	yes := true
	story := func(edit func(*Node)) Graph {
		g := newGraph().epic("billing", ApprovalApproved, "invoice").build()
		g.SchemaVersion = 2
		for i := range g.Nodes {
			if g.Nodes[i].Kind == KindStory {
				edit(&g.Nodes[i])
				break
			}
		}
		return g
	}
	good := story(func(n *Node) {
		n.Release, n.Review = &yes, ReviewCriteria
		n.ApprovedBy, n.ApprovedAt = &Approver{Name: "Ana", ID: "iss|ana"}, "2026-10-16T14:02:00Z"
	})
	got, err := Decode(encode(t, good))
	if err != nil {
		t.Fatalf("Decode(valid version 2) error = %v", err)
	}
	for _, n := range got.Nodes {
		if n.Kind == KindStory && n.Released() {
			if n.Review != ReviewCriteria || n.ApprovedBy.Name != "Ana" {
				t.Errorf("decoded node = %+v, want the version 2 fields", n)
			}
		}
	}

	bad := map[string]func(*Node){
		"unknown review": func(n *Node) { n.Review = "vibes" },
		"bad time":       func(n *Node) { n.ApprovedAt = "yesterday" },
		"nameless":       func(n *Node) { n.ApprovedBy = &Approver{ID: "x"} },
		"draft released": func(n *Node) { n.Release, n.Approval = &yes, ApprovalDraft },
		"approved ui": func(n *Node) {
			n.Review, n.Covers = ReviewUI, []string{"req"}
			n.ApprovedBy, n.ApprovedAt = &Approver{Name: "Ana", ID: "iss|ana"}, "2026-10-16T14:02:00Z"
		},
		"released ui without a requirement": func(n *Node) { n.Review, n.Release, n.Approval = ReviewUI, &yes, ApprovalDraft },
	}
	for name, edit := range bad {
		if _, err := Decode(encode(t, story(edit))); err == nil {
			t.Errorf("%s: Decode() = nil, want a problem", name)
		}
	}
	ui := story(func(n *Node) {
		n.Review, n.Release, n.Approval, n.Covers = ReviewUI, &yes, ApprovalDraft, []string{"req"}
	})
	if _, err := Decode(encode(t, ui)); err != nil {
		t.Errorf("Decode(released draft ui story covering a requirement) error = %v", err)
	}
	v1 := story(func(n *Node) { n.Review = ReviewHuman })
	v1.SchemaVersion = 1
	if msg := decodeErr(t, encode(t, v1)); !strings.Contains(msg, "need schema_version 2") {
		t.Errorf("version 1 with review: %q, want it to need version 2", msg)
	}
	epic := newGraph().epic("billing", ApprovalApproved, "invoice").build()
	epic.SchemaVersion = 2
	epic.Nodes[0].Release = &yes
	if msg := decodeErr(t, encode(t, epic)); !strings.Contains(msg, "stories only") {
		t.Errorf("released epic: %q, want stories only", msg)
	}
}

func TestDecodeRejectsMalformedDocuments(t *testing.T) {
	t.Parallel()

	valid := func() map[string]any {
		var m map[string]any
		_ = json.Unmarshal(encode(t, newGraph().epic("billing", ApprovalApproved, "invoice").build()), &m)
		return m
	}
	node := func(m map[string]any) map[string]any { return m["nodes"].([]any)[0].(map[string]any) }

	tests := []struct {
		name   string
		mutate func(m map[string]any)
		want   string
	}{
		{"unknown field", func(m map[string]any) { m["extra"] = 1 }, "unknown field"},
		{"missing field", func(m map[string]any) { delete(m, "blocks") }, "missing field blocks"},
		{"bad kind", func(m map[string]any) { node(m)["kind"] = "task" }, `kind "task"`},
		{"missing node field", func(m map[string]any) { delete(node(m), "body") }, "missing field body"},
		{"bad digest", func(m map[string]any) { m["source_digest"] = "md5:x" }, "source_digest"},
		{"dangling block edge", func(m map[string]any) {
			m["blocks"] = []any{map[string]any{"blocker": "billing/nope", "blocked": "billing/invoice"}}
		}, `"billing/nope" is not a node`},
		{"dangling parent edge", func(m map[string]any) {
			m["parents"] = []any{map[string]any{"child": "billing/invoice", "parent": "ghost"}}
		}, `"ghost" is not a node`},
		{"not json", nil, "not a JSON object"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			raw := []byte("[")
			if tt.mutate != nil {
				m := valid()
				tt.mutate(m)
				raw = encode(t, m)
			}
			if msg := decodeErr(t, raw); !strings.Contains(msg, tt.want) {
				t.Errorf("error %q does not contain %q", msg, tt.want)
			}
		})
	}
}
