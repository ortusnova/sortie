package persistence

import (
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
)

// migrationNumberingProblems reports what is wrong with the numbering of
// migrations whose SQL is embedded from the files in sqlDir: a version used
// twice, a migration whose SQL file is named for another version, a SQL file
// no migration embeds, and a gap in the fork's range (versions from 1001).
func migrationNumberingProblems(ms []Migration, sqlDir fs.FS) ([]string, error) {
	var problems []string

	seen := make(map[int]string, len(ms))
	for _, m := range ms {
		if prev, ok := seen[m.Version]; ok {
			problems = append(problems, fmt.Sprintf("version %d is used twice: %q and %q", m.Version, prev, m.Description))
			continue
		}
		seen[m.Version] = m.Description
	}

	entries, err := fs.ReadDir(sqlDir, ".")
	if err != nil {
		return nil, err
	}
	bySQL := make(map[string][]string, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		body, err := fs.ReadFile(sqlDir, e.Name())
		if err != nil {
			return nil, err
		}
		bySQL[string(body)] = append(bySQL[string(body)], e.Name())
	}
	embedded := make(map[string]bool, len(entries))
	for _, m := range ms {
		names := bySQL[m.SQL]
		if len(names) == 0 {
			problems = append(problems, fmt.Sprintf("migration %d (%s) embeds SQL that matches no file in sql/", m.Version, m.Description))
			continue
		}
		for _, name := range names {
			embedded[name] = true
			prefix, _, _ := strings.Cut(name, "_")
			n, err := strconv.Atoi(prefix)
			if err != nil || n != m.Version {
				problems = append(problems, fmt.Sprintf("migration %d (%s) embeds sql/%s, which is named for another version", m.Version, m.Description, name))
			}
		}
	}
	for _, names := range bySQL {
		for _, name := range names {
			if !embedded[name] {
				problems = append(problems, fmt.Sprintf("sql/%s is embedded by no migration", name))
			}
		}
	}

	var fork []int
	for v := range seen {
		if v > 1000 {
			fork = append(fork, v)
		}
	}
	sort.Ints(fork)
	want := 1001
	for _, v := range fork {
		if v != want {
			problems = append(problems, fmt.Sprintf("fork migration %d follows %d: version %d is skipped", v, want-1, want))
		}
		want = v + 1
	}

	sort.Strings(problems)
	return problems, nil
}

// Parallel branches that each take the next fork number collide on merge.
// The branch that merges second must renumber to the next free version, and
// rename its SQL file to match.
func TestMigrations_NumberingIsUniqueContiguousAndMatchesFiles(t *testing.T) {
	t.Parallel()
	problems, err := migrationNumberingProblems(migrations, os.DirFS("sql"))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		t.Error(p)
	}
}

func TestMigrationNumberingProblems_FindsEachDefect(t *testing.T) {
	t.Parallel()
	files := fstest.MapFS{
		"1001_a.sql": {Data: []byte("a")},
		"1002_b.sql": {Data: []byte("b")},
	}
	tests := []struct {
		name string
		ms   []Migration
		want string
	}{
		{
			name: "duplicate version",
			ms:   []Migration{{Version: 1001, SQL: "a"}, {Version: 1002, SQL: "b"}, {Version: 1002, SQL: "b"}},
			want: "version 1002 is used twice",
		},
		{
			name: "file named for another version",
			ms:   []Migration{{Version: 1001, SQL: "a"}, {Version: 1003, SQL: "b"}},
			want: "embeds sql/1002_b.sql, which is named for another version",
		},
		{
			name: "fork gap",
			ms:   []Migration{{Version: 1001, SQL: "a"}, {Version: 1003, SQL: "b"}},
			want: "version 1002 is skipped",
		},
		{
			name: "file embedded by no migration",
			ms:   []Migration{{Version: 1001, SQL: "a"}},
			want: "sql/1002_b.sql is embedded by no migration",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			problems, err := migrationNumberingProblems(tt.ms, files)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(strings.Join(problems, "\n"), tt.want) {
				t.Errorf("problems = %q, want one containing %q", problems, tt.want)
			}
		})
	}
}
