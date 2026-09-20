# Working with this repo

## How to talk to me

**Always explain in easy, plain language.** Every answer, not just the hard
ones. Same rule as the Frontend repo, and it applies to commit messages, PR
descriptions and code comments too.

- No jargon. If a technical word is unavoidable, say what it means in normal
  words in the same breath.
- Short sentences. Short paragraphs.
- Lead with the plain answer. Code details after, and only if they help.
- Do not hide behind function names and column names. Say what is actually
  happening and why anyone would notice.

---

## Three mistakes this repo has already paid for

These are not style opinions. Each one shipped, each one was invisible, and
each one is easy to make again.

### 1. A fake database cannot check your SQL

Most tests here use a **pretend database** (sqlmock). You hand it rows, and it
hands them straight back. **It never reads your query.**

So a query with a wrong column name, a missing bracket, or plain bad grammar
**passes every test** and then fails against the real thing.

And it does not fail small. Postgres does not return a blank for a column that
does not exist — it **refuses the whole statement**. One wrong word and the
query returns nothing at all, for every row. On screen that looks like "there
is no data yet", not like a bug.

This has happened twice:

- The admin analysis page asked for a column called `tags`. The real column is
  `custom_tags`. The page answered `null` for **every video on the platform**,
  and looked exactly like a worker that had never run.
- A query joining two tables was missing the brackets around each half.
  Postgres refuses that outright. The function treats a failed query as "no
  opinion", so the symptom would have been one ranking signal **silently
  ceasing to exist for every user**. Nothing would have logged.

**Every query in this repo is now checked against a real database.** Not most
— every one. Two tests between them leave nothing out:

| | how many | checked by |
|---|---|---|
| readable from the source | 260 call sites | `sql_compiles_test.go` hands each to Postgres to parse |
| built at runtime | 27 | `runtime_queries_db_test.go` RUNS each one |
| several statements in one string | 5 | every database-backed test runs them on the way in |
| **neither** | **0** | — and the test fails if that stops being true |

`sql_compiles_test.go` reads a query held in a constant, glued together with
`+`, produced by `fmt.Sprintf`, returned by a function whose every return is a
literal, passed in as a parameter, or listed in a range loop. What it still
cannot read is paired by name with the test that runs it, and that pairing is
checked BOTH ways: an unreadable query nobody runs fails, and a stale entry
claiming coverage that no longer exists fails too.

The first run found **twelve** broken queries that had shipped:

| what was broken | what it cost |
|---|---|
| the follow-graph feed lane asked `follows` for `followed_id` | following somebody put nothing in your feed |
| two account-suggestion lanes asked `users` for `followers` | no suggestions, ever |
| the mood half of every taste profile called `unnest()` on JSONB | never populated for anybody |
| the engagement-quality score read `users.created_at` | one flat score for every user |
| the creator dashboard asked `posts` for `likes` and `user_id` | failed for every creator |
| trending asked `posts` for four columns it does not have | every trending post silently dropped |

Every one was a single word. CI runs a real Postgres now, so this check runs
on every push.

**If you change SQL, run it against real Postgres.** `TEST_DATABASE_URL` and
the `withDB(t)` helper in `scoring_db_audit_test.go` are the way in.

`withDB` skips when `TEST_DATABASE_URL` is UNSET and **fails** when it is set
but the database does not answer. That difference is the point: when both were
a skip, a CI run whose Postgres never started reported a clean pass with none
of these tests having run. Green, fast, and meaningless — the same shape as
every bug on this page.

If there is no database to hand, start one:

```
initdb -D /var/lib/pgdata -U postgres --auth=trust
pg_ctl -D /var/lib/pgdata -o '-c listen_addresses=127.0.0.1 -p 5433' start
createdb -h 127.0.0.1 -p 5433 -U postgres battlearena_test
TEST_DATABASE_URL="postgres://postgres@127.0.0.1:5433/battlearena_test?sslmode=disable" go test ./...
```

That takes about a minute and it has caught a real bug the first time it was
tried.

### 2. A swallowed error looks exactly like a real answer

This shape is all over this codebase:

```go
rows, err := db.Query(...)
if err == nil {
    ... do all the work ...
}
```

```go
if err != nil {
    return ""   // "no opinion"
}
```

```go
rows.Scan(&a, &b, &c)   // error thrown away
```

In every one of those, **broken** and **empty** produce the same output. A
query that failed looks like a user with no history. A scan that failed looks
like a video with no tags. There is no error, no log line, and nothing to
search for.

`rows.Scan` is the nastiest of the three. It fails for the **whole result set
at once** — if your column list and your variable list do not line up, it fails
on row 1 and on all 500. The loop then builds a profile out of nothing but
zeroes, which is indistinguishable from a brand-new user.

**So: when you touch one of these, make it say something.** `queryFailed` and
`scanFailed` in `query_failures.go` are the two-line way; both take what you
were trying to work out and what the app is doing instead, because "query
failed" on its own leaves the reader to guess whether anything is wrong.

`scanFailed` takes a counter so one broken query is one log line rather than
five hundred — and the count is what tells you which happened. One bad row is
a bad row; every row failing is a broken query.

`silent_failures_test.go` counts what is left and holds it at a baseline, the
same way `.nilaway-baseline` works. **It was 72. It is 0.** Every database
call in this repo says something when it fails.

The check itself was wrong twice before that number meant anything. It counted
`strconv.Atoi` and `time.Parse`, which have nothing to do with a database, and
it counted errors that ARE reported a few lines further down. Both made the
number bigger than the truth, and a checker that cries wolf gets switched off.
It now tracks, per block, whether an error came from a database call and
whether the statement next to it says anything about it. It can go down and it
cannot go up without a reason in the commit message.

### 3. It exists, but nothing calls it

The most common bug in this repo, hit **five** times now:

- a decoder probe with no caller
- a measured budget worked out at startup and never passed on
- a link meter that nothing fed
- a stand-down counter nothing wired up
- columns on a table that nothing ever wrote to

Every single one had tests. Every single one passed. Because the tests called
the far end **by hand**, so they never went through the wire that was missing.

**So: cut the wire and run the tests.** Delete the call, change a table name,
drop a column from a query — then run the suite. If nothing goes red, the test
is testing the wrong thing.

That is the only check that catches this class, and it takes a minute.

---

## Two systems build the schema

`runMigrations()` in `database.go` re-runs `CREATE TABLE IF NOT EXISTS` and
`ADD COLUMN IF NOT EXISTS` on every boot. Good for adding things, cannot change
anything.

`migrations/*.sql` are numbered files applied once each, in order, recorded by
name and content hash. Everything that is not "add a new thing" goes there.
See `migrations/README.md` for the rules.

**A check that reads only one of the two is blind to half the schema** — and
the half it cannot see is the newer half. A test that greps `database.go` for
a column will not find one added by a migration, and will confidently report
that the column does not exist.
