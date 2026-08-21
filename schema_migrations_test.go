package main

import (
	"strings"
	"testing"
)

// The parts of the migration runner worth pinning are the ones that decide
// ORDER and REJECTION, because both fail silently in the worst way: a
// mis-ordered migration runs against a schema it did not expect, and a
// mis-named one gets skipped by a glob nobody re-reads. Neither shows up as an
// error at the time. All three tests below run without a database.

func TestLoadMigrations_ReadsEmbeddedFilesInOrder(t *testing.T) {
	ms, err := loadMigrations()
	if err != nil {
		t.Fatalf("loadMigrations: %v", err)
	}
	if len(ms) == 0 {
		t.Fatal("no migrations loaded — the go:embed pattern matched nothing, " +
			"which would mean every future migration silently never runs")
	}

	for i := 1; i < len(ms); i++ {
		if ms[i-1].version >= ms[i].version {
			t.Errorf("migrations out of order: %q came before %q",
				ms[i-1].version, ms[i].version)
		}
	}

	for _, m := range ms {
		if m.version == "" {
			t.Error("migration with an empty version")
		}
		if strings.HasSuffix(m.version, ".sql") {
			t.Errorf("version %q still carries its .sql suffix — it is the "+
				"primary key in schema_migrations and must be the bare name", m.version)
		}
		if m.checksum == "" {
			t.Errorf("migration %q has no checksum, so an edit after it is "+
				"applied could never be detected", m.version)
		}
		if len(m.checksum) != 64 {
			t.Errorf("migration %q checksum %q is not a SHA-256 hex digest",
				m.version, m.checksum)
		}
	}
}

func TestLoadMigrations_BaselineIsFirst(t *testing.T) {
	ms, err := loadMigrations()
	if err != nil {
		t.Fatalf("loadMigrations: %v", err)
	}
	// Everything else counts from the baseline, so it has to sort first. If a
	// later rename ever pushed it down the list it would be applied after
	// migrations that assume it already ran.
	if got := ms[0].version; got != "001_baseline" {
		t.Errorf("first migration = %q, want %q", got, "001_baseline")
	}
}

func TestLoadMigrations_ChecksumIsStable(t *testing.T) {
	// The checksum is what detects an edit to an already-applied migration.
	// If it were computed over anything that varies between runs (a path, a
	// timestamp) every boot would warn about every migration, and the warning
	// would stop meaning anything.
	first, err := loadMigrations()
	if err != nil {
		t.Fatalf("loadMigrations: %v", err)
	}
	second, err := loadMigrations()
	if err != nil {
		t.Fatalf("loadMigrations (second call): %v", err)
	}
	if len(first) != len(second) {
		t.Fatalf("migration count changed between calls: %d then %d",
			len(first), len(second))
	}
	for i := range first {
		if first[i].checksum != second[i].checksum {
			t.Errorf("checksum for %q is not stable across calls: %q then %q",
				first[i].version, first[i].checksum, second[i].checksum)
		}
	}
}
