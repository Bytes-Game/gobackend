# Migrations

## The short version

Anything that changes the database from now on goes in a numbered `.sql` file
in this folder. Name it `002_add_whatever.sql`, then `003_`, and so on. The
backend applies them in number order the next time it starts, remembers which
ones it has already run, and never runs one twice.

## Why there are two systems here

There are two ways the schema gets built, and they are not in competition:

**`runMigrations()` in `database.go`** builds the schema that already existed
before versioned migrations were added — about 30 tables. It runs on every boot
and is written to be safe to re-run: `CREATE TABLE IF NOT EXISTS`,
`ADD COLUMN IF NOT EXISTS`. That is fine for *adding* things. It cannot express
*changing* a thing, because "if not exists" has nothing to say about a column
that already exists and is now wrong.

**This folder** is for everything that is not "add a new thing". Renaming a
column, changing a type, backfilling a value, dropping something, splitting a
table. Those have to happen exactly once, in a known order, and be recorded.

New tables can go in either place. Prefer this folder — it leaves a history.

## Rules

1. **Never edit a file that has already been deployed.** Once a migration has
   run against the production database its version is recorded and it will
   never run again. Editing it changes nothing in production and quietly makes
   your local database different from the real one. Write a new file instead.

2. **One file, one migration, in its own transaction.** If a file fails
   halfway the whole file rolls back and its version is not recorded, so
   fixing the file and redeploying retries it cleanly.

3. **A failed migration stops the service from starting.** This is on purpose.
   The alternative is a server running application code against a schema that
   does not match it, which does worse and much more confusing damage than a
   boot failure. When a deploy fails on a migration, the boot log names the
   file and quotes the database error.

4. **Write migrations that can run against a live database.** The old code may
   still be serving traffic while this runs. Avoid `ALTER TABLE ... SET NOT
   NULL` on a large table, avoid rewriting a big table in place, and use
   `CREATE INDEX CONCURRENTLY` for indexes on large tables — noting that
   `CONCURRENTLY` cannot run inside a transaction, so a migration needing it
   has to be handled deliberately rather than dropped in as another file.

5. **Numbers must be unique.** Two files both starting `004_` is an error, and
   the server refuses to start rather than guess an order.

## How it is tracked

A `schema_migrations` table, one row per applied file:

```
version     TEXT PRIMARY KEY   -- "002_add_whatever"
checksum    TEXT               -- SHA-256 of the file as applied
applied_at  TIMESTAMPTZ
```

The checksum is what catches rule 1. If a file's contents no longer match what
was recorded when it ran, the boot log warns and names the file. It does not
refuse to start over it: the usual cause is an edited comment, and taking
production down for a comment would be worse than the warning.

Files are compiled into the binary with `go:embed`, so a deploy always carries
its own migrations. There is no way to run a binary against a folder of
migrations from some other commit.

## Running them

Nothing to run. `InitDatabase()` applies anything outstanding at startup.

Concurrent replicas are safe: the runner takes a Postgres advisory lock on a
single pinned connection before it looks at anything, so if two instances boot
together one applies and the other waits and then finds the work already done.
