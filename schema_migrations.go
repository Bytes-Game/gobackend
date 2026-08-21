package main

// schema_migrations.go — ordered, recorded, run-once database changes.
//
// ════════════════════════════════════════════════════════════════════════════════
// WHY THIS EXISTS
// ════════════════════════════════════════════════════════════════════════════════
//
// runMigrations() in database.go builds the schema by re-running the same
// CREATE TABLE IF NOT EXISTS / ADD COLUMN IF NOT EXISTS statements on every
// boot. That is a good fit for adding things and it is how ~30 tables got
// here, but it has no answer for the other half of schema work:
//
//   * It cannot CHANGE anything. "If not exists" says nothing about a column
//     that exists and now has the wrong type, the wrong name, or the wrong
//     default.
//   * It cannot run a one-time data backfill. Anything written there runs on
//     every single boot, forever.
//   * It has no history and no order. There is no way to ask which change
//     landed before which, or whether a given database has had a change
//     applied at all.
//   * There is no way back. Nothing records what was done, so nothing can
//     describe how to undo it.
//
// None of that hurts while the only operation is "add another table". It
// starts hurting the first time real user data is in the database and a column
// has to change shape underneath it.
//
// So: numbered .sql files in migrations/, applied in order, each recorded by
// name and content hash in a schema_migrations table, each in its own
// transaction, never applied twice. See migrations/README.md for the rules
// this imposes on whoever writes the next one.
//
// The two systems run in sequence, not in competition: runMigrations() creates
// the baseline, then this applies anything outstanding on top. Nothing that
// already works had to move for this to be added.
//
// ════════════════════════════════════════════════════════════════════════════════
// SAFETY
// ════════════════════════════════════════════════════════════════════════════════
//
// Failure is fatal. A migration that did not apply leaves application code
// talking to a schema it does not expect, and the damage from serving traffic
// in that state is worse and far harder to diagnose than a service that
// refused to start with the failing filename in its log. This is the one place
// in this file that deliberately does not degrade gracefully — everywhere else
// in this backend prefers to keep serving, and that instinct is wrong here.
//
// Concurrency is handled by a Postgres advisory lock taken on a single pinned
// connection. Render can run overlapping instances during a deploy; without
// the lock two of them could apply the same migration at the same moment. With
// it, the second waits and then finds the work already recorded.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"log"
	"sort"
	"strings"
	"time"
)

// migrationFiles carries the migrations INTO the binary. Embedding rather than
// reading from disk at runtime means a deployed binary can never be paired
// with a migrations folder from a different commit — the files it applies are
// the files it was built from.
//
//go:embed migrations/*.sql
var migrationFiles embed.FS

// migrationLockKey is the advisory-lock id this runner uses. Arbitrary but
// fixed: any two processes running this code agree on it, and nothing else in
// the codebase takes advisory locks, so it cannot collide.
const migrationLockKey int64 = 8_274_113_905_441

// migrationLockTimeout bounds how long we wait for another instance to finish
// its migration run. Generous, because the wait is legitimate — a rolling
// deploy really can have two instances booting seconds apart — but bounded, so
// a lock leaked by a process that died mid-run surfaces as a clear failure
// rather than a boot that hangs forever with no output.
const migrationLockTimeout = 2 * time.Minute

// migration is one file from migrations/, ready to apply.
type migration struct {
	// version is the filename without its .sql suffix ("002_add_whatever").
	// This is the primary key in schema_migrations.
	version string
	// sqlText is the file's full contents, applied as a single statement
	// batch inside one transaction.
	sqlText string
	// checksum is the SHA-256 of sqlText, recorded so an edit to an
	// already-applied file can be detected later.
	checksum string
}

// loadMigrations reads and orders every embedded migration.
//
// Ordering is a plain lexical sort of the filename, which is why the naming
// convention is zero-padded numeric prefixes: "002_" sorts before "010_", but
// "2_" would sort after it. A file that does not start with a digit is
// rejected rather than quietly sorted somewhere arbitrary.
//
// Split out from the apply path, and returning an error rather than logging,
// so the parts that need no database — ordering, duplicate detection, prefix
// validation — can be tested directly. See schema_migrations_test.go.
func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return nil, fmt.Errorf("reading embedded migrations: %w", err)
	}

	var out []migration
	seenPrefix := map[string]string{} // numeric prefix -> first filename using it

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		version := strings.TrimSuffix(e.Name(), ".sql")

		prefix, _, ok := strings.Cut(version, "_")
		if !ok || prefix == "" || strings.TrimLeft(prefix, "0123456789") != "" {
			return nil, fmt.Errorf(
				"migration %q must be named <number>_<description>.sql (e.g. 002_add_column.sql)",
				e.Name())
		}
		// Two files claiming the same number have no defined order between
		// them, and picking one by filename would mean the order depends on
		// the description text. Refuse instead — this is nearly always two
		// branches that each added a migration and were merged without
		// renumbering.
		if first, dup := seenPrefix[prefix]; dup {
			return nil, fmt.Errorf(
				"migrations %q and %q share the number %q — renumber one of them",
				first, e.Name(), prefix)
		}
		seenPrefix[prefix] = e.Name()

		body, err := fs.ReadFile(migrationFiles, "migrations/"+e.Name())
		if err != nil {
			return nil, fmt.Errorf("reading migration %q: %w", e.Name(), err)
		}
		sum := sha256.Sum256(body)

		out = append(out, migration{
			version:  version,
			sqlText:  string(body),
			checksum: hex.EncodeToString(sum[:]),
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

// applyVersionedMigrations brings the connected database up to date.
//
// Called from runMigrations() AFTER the idempotent baseline schema, so a
// migration here can rely on every baseline table existing. Fatal on any
// failure — see the SAFETY note at the top of this file.
func applyVersionedMigrations() {
	if db == nil {
		return
	}

	pending, err := loadMigrations()
	if err != nil {
		log.Fatalf("Migration setup failed: %v", err)
	}
	if len(pending) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), migrationLockTimeout)
	defer cancel()

	// Pin ONE connection for the whole run. pg_advisory_lock is scoped to a
	// session, so taking it on a pooled *sql.DB and releasing it later could
	// release on a different connection than it was taken on — which silently
	// does nothing and leaves the real lock held until that backend exits.
	conn, err := db.Conn(ctx)
	if err != nil {
		log.Fatalf("Migration failed: could not acquire a database connection: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, migrationLockKey); err != nil {
		log.Fatalf("Migration failed: could not take the migration lock "+
			"(another instance may be mid-migration, or it died holding the lock): %v", err)
	}
	defer func() {
		// Best-effort release on a fresh context: ctx may already be expired
		// by the time we get here, and failing to release would make the NEXT
		// boot wait out the full timeout for no reason.
		rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer rcancel()
		if _, err := conn.ExecContext(rctx, `SELECT pg_advisory_unlock($1)`, migrationLockKey); err != nil {
			log.Printf("Warning: could not release the migration lock: %v "+
				"(it clears on its own when this connection closes)", err)
		}
	}()

	if _, err := conn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT PRIMARY KEY,
			checksum   TEXT NOT NULL DEFAULT '',
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`); err != nil {
		log.Fatalf("Migration failed: could not create schema_migrations: %v", err)
	}

	applied, err := loadAppliedMigrations(ctx, conn)
	if err != nil {
		log.Fatalf("Migration failed: could not read schema_migrations: %v", err)
	}

	ran := 0
	for _, m := range pending {
		if prevSum, done := applied[m.version]; done {
			// Already applied. Check it has not been edited since — see rule 1
			// in migrations/README.md. Warn only: the overwhelmingly common
			// cause is a reworded comment, and refusing to boot for that would
			// cost more than it saves. A real concern shows up as this warning
			// next to a schema that does not match expectations.
			if prevSum != "" && prevSum != m.checksum {
				log.Printf("WARNING: migration %q has changed since it was applied. "+
					"The database still has the OLD version — an already-applied "+
					"migration never runs again. If the change matters, add it as a "+
					"new migration file.", m.version)
			}
			continue
		}

		if err := applyOneMigration(ctx, conn, m); err != nil {
			log.Fatalf("Migration %q failed (rolled back, nothing was applied): %v",
				m.version, err)
		}
		log.Printf("Applied migration %s", m.version)
		ran++
	}

	if ran > 0 {
		log.Printf("Schema migrations: %d applied, %d already present", ran, len(pending)-ran)
	}
}

// loadAppliedMigrations returns version -> checksum for everything already run.
func loadAppliedMigrations(ctx context.Context, conn *sql.Conn) (map[string]string, error) {
	rows, err := conn.QueryContext(ctx, `SELECT version, checksum FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := map[string]string{}
	for rows.Next() {
		var version, checksum string
		if err := rows.Scan(&version, &checksum); err != nil {
			return nil, err
		}
		out[version] = checksum
	}
	return out, rows.Err()
}

// applyOneMigration runs a single migration and records it, both inside one
// transaction. Either the schema change and its bookkeeping row both land, or
// neither does — there is no state where a migration has run but the database
// does not know it ran, which would make the next boot run it a second time.
func applyOneMigration(ctx context.Context, conn *sql.Conn, m migration) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op once committed

	if _, err := tx.ExecContext(ctx, m.sqlText); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO schema_migrations (version, checksum) VALUES ($1, $2)`,
		m.version, m.checksum); err != nil {
		return fmt.Errorf("recording migration: %w", err)
	}
	return tx.Commit()
}
