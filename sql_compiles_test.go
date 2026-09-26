package main

// sql_compiles_test.go — every query in this repo, handed to a real Postgres.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY THIS EXISTS
// ════════════════════════════════════════════════════════════════════════════
//
// Almost every test here talks to a PRETEND database. You give it rows and it
// hands the same rows back. It never reads the query.
//
// So a query asking for a column that does not exist passes every test in the
// suite. And when it reaches the real database it does not fail small:
// Postgres does not return a blank for a column it cannot find, it REFUSES
// THE WHOLE STATEMENT. One wrong word and the query returns nothing at all,
// for every row, on every request. On screen that reads as "there is no data
// yet", not as a bug.
//
// The first run of this test found ELEVEN broken queries that had shipped:
//
//   * the follow-graph feed lane asked follows for followed_id. The column is
//     following_id. Following somebody put nothing in your feed.
//   * two account-suggestion lanes asked users for a followers column. That
//     number is counted from the follows table, not stored. Both lanes
//     returned nothing, always.
//   * the mood half of every taste profile called unnest() on JSONB. There is
//     no unnest(jsonb) in any version of Postgres, so it had never once run.
//   * the engagement-quality score read users.created_at, a column that was
//     never added, so every user on the platform got the same flat score.
//   * the creator dashboard asked posts for likes and user_id — neither
//     exists — so it failed for every creator.
//   * trending dropped every POST, because the query loading one named four
//     columns that are not there.
//
// Every one of those was a single word, and not one could be caught by a test
// that does not involve Postgres.
//
// ════════════════════════════════════════════════════════════════════════════
// HOW
// ════════════════════════════════════════════════════════════════════════════
//
// Every Go file is parsed, every call to Query / QueryRow / Exec and their
// Context variants is found, and the query text is lifted out. Each one is
// PREPARED against a real database with the real schema.
//
// Preparing is parse-and-plan without running: Postgres resolves every table
// and column name and reports anything it cannot find, but no row is read and
// nothing is written. So this is safe to point at any schema, and the whole
// repo takes about a second.
//
// Queries are not always one literal. This resolves the ways this repo builds
// them — a named constant, literals added together, fmt.Sprintf with constant
// arguments — so those are checked like any other.
//
// WHERE A NAME HAS SEVERAL VALUES, ALL OF THEM ARE CHECKED. Two functions in
// feed_engine.go both have a local called responseFilter, and they follow
// different conventions: one includes the leading AND, the other does not.
// Picking one value and checking it against the other's query invented a
// statement the code never runs and reported a syntax error that was not
// there. So a name resolves to a SET, and every combination has to compile.
//
// It skips cleanly when TEST_DATABASE_URL is unset, so CI without a database
// stays green. See CLAUDE.md for how to start one; it takes four commands.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// sqlSite is one query found in the source.
type sqlSite struct {
	file string
	line int
	// variants is every statement this call site can send. Usually one.
	variants []string
	// dynamic is true when the query is glued together from something this
	// cannot see — a table name from a struct field, a placeholder list
	// generated in a loop. Those are counted and named rather than checked,
	// because a check that silently skipped them would be the same blind spot
	// in a new place.
	dynamic bool
	// multi is a string holding several statements. Prepare takes exactly
	// one, so these cannot come through here — but they are not a blind spot
	// either: the schema block in database.go is run in full by every
	// database-backed test in this package, on the way in.
	multi bool
	// fn is the function the query sits in. Used to pair an unreadable query
	// with the test that runs it — by name rather than by line, so an edit
	// three hundred lines above does not break the pairing.
	fn string
}

// isDatabaseHandle reports that this is something you run SQL on, rather than
// a Redis pipeline that happens to have a method called Exec.
//
// Thirteen Redis pipeline flushes were being counted as "queries this cannot
// read", which made the blind spot look a quarter bigger than it is. When the
// query text does not fold, the receiver is the only thing left to go on.
func isDatabaseHandle(e ast.Expr) bool {
	root := e
	for {
		sel, ok := root.(*ast.SelectorExpr)
		if !ok {
			break
		}
		root = sel.X
	}
	id, ok := root.(*ast.Ident)
	if !ok {
		return false
	}
	switch id.Name {
	case "db", "tx", "conn", "sqlDB", "stmt":
		return true
	}
	return false
}

// sqlMethods are the database/sql calls whose first argument is a query.
//
// Redis uses Exec too, on a pipeline, but its argument is a context rather
// than a string — so it folds to nothing and drops out on its own.
var sqlMethods = map[string]bool{
	"Query": true, "QueryRow": true, "Exec": true,
	"QueryContext": true, "QueryRowContext": true, "ExecContext": true,
	"Prepare": true, "PrepareContext": true,
}

// maxVariants caps how many combinations one call site may expand into.
//
// A query glued from three names with four values each is sixty-four
// statements, and checking all of them says less than the number suggests.
// Past this it is treated as runtime-built and listed, which is the honest
// answer rather than an arbitrary subset.
const maxVariants = 16

func TestEverySQLStatementCompilesAgainstTheRealSchema(t *testing.T) {
	defer withDB(t)()

	sites := collectSQL(t)
	if len(sites) < 50 {
		t.Fatalf("only found %d queries in the whole repo — the collector is "+
			"broken, and a broken collector passes silently forever", len(sites))
	}

	var checked, statements, skipped, multi int
	var dynamic, coveredElsewhere []string
	for _, s := range sites {
		if s.multi {
			// Several statements in one string. Prepare takes one, so these
			// go through the ordinary database tests instead — the schema
			// block in database.go is run in full by every one of them.
			multi++
			continue
		}
		if s.dynamic {
			skipped++
			key := s.file + ":" + s.fn
			if by, ok := runByATest[key]; ok {
				coveredElsewhere = append(coveredElsewhere, key+"  ("+by+")")
			} else {
				dynamic = append(dynamic, fmt.Sprintf("%s:%d  in %s",
					s.file, s.line, s.fn))
			}
			continue
		}
		checked++
		for _, q := range s.variants {
			statements++
			stmt, err := db.Prepare(q)
			if err == nil {
				_ = stmt.Close()
				continue
			}
			// Postgres cannot always work out what type a bare $1 is when
			// nothing around it says. That is not a broken query — every table
			// and column in it resolved, which is what this is checking.
			if strings.Contains(err.Error(), "could not determine data type") {
				continue
			}
			t.Errorf("\n%s:%d does not compile against the real schema:\n  %v\n\n%s\n",
				s.file, s.line, err, indent(q))
		}
	}

	t.Logf("checked %d call sites (%d statements) against real Postgres; "+
		"%d more are built at runtime and RUN by a database-backed test; "+
		"%d hold several statements and are run whole by those tests; "+
		"%d neither (baseline %d)",
		checked, statements, len(coveredElsewhere), multi,
		len(dynamic), sqlDynamicBaseline)

	sort.Strings(dynamic)
	sort.Strings(coveredElsewhere)

	// The gate is what is left over: a query this cannot read AND nothing
	// runs. Those are the ones where a wrong column name reaches production.
	if len(dynamic) > sqlDynamicBaseline {
		t.Errorf("queries that are neither readable here nor run by a test "+
			"went from %d to %d.\n\n"+
			"A query built from something the source does not show — a table "+
			"name in a parameter, a WHERE clause glued on in a loop — is a "+
			"query no real database has ever seen.\n\n"+
			"Three ways out, best first:\n"+
			"  1. write it out at the call so this can read it (the switch in "+
			"creatorOwnsContent is the pattern)\n"+
			"  2. give it a test that RUNS it — runtime_queries_db_test.go is "+
			"full of them — and add its function to runByATest below\n"+
			"  3. raise sqlDynamicBaseline and say in the commit message why "+
			"neither was possible\n\nAll of them:\n  %s",
			sqlDynamicBaseline, len(dynamic), strings.Join(dynamic, "\n  "))
	}
	if len(dynamic) < sqlDynamicBaseline {
		t.Logf("down to %d from %d — lower sqlDynamicBaseline so it cannot "+
			"creep back up", len(dynamic), sqlDynamicBaseline)
	}

	// A name here that no longer matches anything is a test that has been
	// renamed or deleted, leaving a query nobody checks and a list that says
	// otherwise.
	seen := map[string]bool{}
	for _, c := range coveredElsewhere {
		key := c
		if i := strings.Index(c, "  ("); i > 0 {
			key = c[:i]
		}
		seen[key] = true
	}
	for key := range runByATest {
		if !seen[key] {
			t.Errorf("runByATest lists %q, but no unreadable query sits in "+
				"that function any more. Either it became readable — remove "+
				"the entry — or the function moved and something is now "+
				"unchecked while this list claims otherwise.", key)
		}
	}

	if testing.Verbose() {
		for _, c := range coveredElsewhere {
			t.Logf("  run by a test:    %s", c)
		}
		for _, d := range dynamic {
			t.Logf("  NOT CHECKED:      %s", d)
		}
	}
}

// runByATest pairs a query this file cannot read with the test that runs it
// against a real database.
//
// Keyed by file and enclosing FUNCTION, not by line, so ordinary edits do not
// break the pairing. Every entry is checked both ways: an unlisted unreadable
// query fails the test above, and a listed function that no longer holds one
// fails too — because a stale entry is a claim of coverage that is no longer
// true, which is worse than no claim at all.
var runByATest = map[string]string{
	// A table name arriving as a parameter. Both tables are passed.
	"admin_hls_queue.go:readHLSQueue":                  "TestRuntimeQueries_TableNameFromAParameter",
	"media_requeue.go:requeueByID":                     "TestRuntimeQueries_TableNameFromAParameter",
	"video_analysis.go:storeVideoAnalysis":             "TestRuntimeQueries_TableNameFromAParameter",
	"video_analysis.go:settleCategory":                 "TestSettleCategory_WritesTheModelsAnswerToTheRow",
	"backfill_category_provenance.go:backfillOneTable": "TestBackfill_SettlesVideosTheModelAlreadyWatched",
	"hls_worker_api.go:storeVideoVariants":             "TestRuntimeQueries_WorkerWritesToBothTables",
	"hls_worker_api.go:storeVideoThumbnail":            "TestRuntimeQueries_WorkerWritesToBothTables",
	"hls_worker_api.go:storeLadder":                    "TestRuntimeQueries_WorkerWritesToBothTables",
	"media_analysis_read.go:readAnalysisRows":          "TestAdminAnalysisRead_RunsAgainstARealResponsesTable",

	// A placeholder list built in a loop.
	"database.go:getLikedByMap":                     "TestRuntimeQueries_PlaceholderListsBuiltInALoop",
	"database.go:enrichUsers":                       "TestRuntimeQueries_PlaceholderListsBuiltInALoop",
	"device_fit.go:loadVideoDimensions":             "TestRuntimeQueries_PlaceholderListsBuiltInALoop",
	"feed_engine.go:populateTopResponses":           "TestRuntimeQueries_PlaceholderListsBuiltInALoop",
	"feed_engine.go:populateChallengeCommentCounts": "TestRuntimeQueries_PlaceholderListsBuiltInALoop",
	"feed_engine.go:populateHLSManifestURLs":        "TestRuntimeQueries_PlaceholderListsBuiltInALoop",

	// A clause glued on depending on the caller.
	"search_relevance.go:searchTextIndex":        "TestRuntimeQueries_ClausesGluedOnAtRequestTime",
	"suggested_users.go:pullCategoryCandidates":  "TestRuntimeQueries_ClausesGluedOnAtRequestTime",
	"hls_dispatch.go:hlsWorkWaiting":             "TestRuntimeQueries_ClausesGluedOnAtRequestTime",
	"audition_ladder.go:auditionsDueForReview":   "TestRuntimeQueries_ClausesGluedOnAtRequestTime",
	"topic_graph.go:buildTopicGraph":             "TestTopicGraph_CountsBothHalvesOfABattle",
	"suggest_handlers.go:subjectsPeopleHaveUsed": "TestLeak_APrivateSubjectNeverReachesTheSuggestionList",

	// Built inside an HTTP handler, where the shape depends on the request.
	"profile_handlers.go:UpdateUserProfileHandler":  "TestRuntimeQueries_HandlersThatBuildTheirOwnSQL",
	"profile_handlers.go:GetLikedChallengesHandler": "TestRuntimeQueries_HandlersThatBuildTheirOwnSQL",
	"profile_handlers.go:GetWatchHistoryHandler":    "TestRuntimeQueries_HandlersThatBuildTheirOwnSQL",
	"account_delete.go:DeleteAccountHandler":        "TestRuntimeQueries_AccountDeletionRunsEveryStatement",

	"hls_worker_api.go:HLSNextPendingHandler": "TestRuntimeQueries_WorkerClaimEndpoint",

	// The two struct fields this deliberately does not guess at — see the
	// SelectorExpr case in foldStrings for why.
	"tag_suggestions.go:readTagState":    "TestRuntimeQueries_TagStateAgainstARealDatabase",
	"tag_suggestions.go:saveTagDecision": "TestRuntimeQueries_TagStateAgainstARealDatabase",

	// Runs whatever its caller hands it; every caller is checked above.
	"database.go:queryPosts":      "every caller's query is checked statically",
	"database.go:queryChallenges": "every caller's query is checked statically",

	// Records a migration as applied. It has run for all nine of them in
	// every database this suite has ever built.
	"schema_migrations.go:applyOneMigration": "every database-backed test, on the way in",
}

// sqlDynamicBaseline is how many queries are NEITHER readable here NOR run by
// a database-backed test.
//
// It is ZERO. Every query in this repo is now checked one way or the other:
// 260 call sites handed to a real Postgres to parse, 27 more that cannot be
// read from the source but are RUN by a test in runtime_queries_db_test.go,
// and 5 multi-statement blocks that every database test runs on the way in.
//
// It was 53. Most of that was not real: thirteen were Redis pipeline flushes
// that only look like SQL, and the rest shrank as this learned to read a
// constant, a fmt.Sprintf, a function whose every return is a literal, a
// parameter resolved from its callers, and a list of statements in a range
// loop. What genuinely could not be read got a test that runs it.
//
// Zero is a real gate. A new query that neither can be read nor is run fails
// this test by name, with the three ways out spelled out above.
const sqlDynamicBaseline = 0

// collectSQL parses every non-test Go file and lifts out the query strings.
func collectSQL(t *testing.T) []sqlSite {
	t.Helper()
	fset := token.NewFileSet()
	files := map[string]*ast.File{}
	var order []string

	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			// Vendored or generated trees have their own schemas and are not
			// ours to police.
			if info.Name() == "vendor" || info.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", path, perr)
		}
		files[path] = f
		order = append(order, path)
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	sort.Strings(order)

	// Package-level constants and variables that hold a literal, by name, with
	// every value each is given. A query assigned to a name first is still a
	// query, and half this repo's longer ones are written that way.
	pkg := map[string][]string{}
	for _, path := range order {
		if f := files[path]; f != nil {
			collectStringNames(f, pkg, false)
		}
	}

	// Functions whose every return is a plain string, by name.
	//
	// hlsTableForKind is the one that matters: it answers "challenges" or
	// "challenge_responses", and seven queries in hls_worker_api.go alone are
	// built by pasting its answer into a statement. Those are the queries
	// MOST likely to break, because the two tables do not have the same
	// columns — and they were the largest group this check could not see.
	//
	// Both answers get checked, so a column that exists on one table and not
	// the other is caught rather than depending on which branch a test
	// happened to take.
	funcs := map[string][]string{}
	for _, path := range order {
		f := files[path]
		if f == nil {
			continue
		}
		collectFixedReturns(f, pkg, funcs)
	}
	for name, vals := range funcs {
		pkg[funcCallKey(name)] = vals
	}

	// String parameters of functions that build SQL, resolved from what every
	// caller actually passes.
	//
	// This is where the largest unreadable group lived. hls_worker_api.go has
	// a closure `claim := func(table string)` holding five statements, and
	// every caller passes one of two table names — but a parameter has no
	// value in the source, so all five were invisible here.
	//
	// EVERY call site is unioned, which matters more here than anywhere else
	// in this file: challenges and challenge_responses do not have the same
	// columns, so a statement that works against one can be refused by the
	// other. One caller this cannot read and the parameter resolves to
	// nothing, because checking half the branches while reporting full
	// coverage is the failure this whole file exists to prevent.
	params := map[string][]string{}
	for _, path := range order {
		if f := files[path]; f != nil {
			collectParamValues(f, pkg, params)
		}
	}
	for name, vals := range params {
		if _, taken := pkg[name]; !taken {
			pkg[name] = vals
		}
	}

	// Locals are collected PER FUNCTION and layered on top.
	//
	// A flat package-wide map was the first version of this and it was wrong
	// in a way that looked like a bug in the code rather than in the check.
	// Two functions in feed_engine.go both have a local called
	// responseFilter, following different conventions — one carries the
	// leading AND, the other does not — so one function's value got pasted
	// into the other's query and Postgres reported a syntax error in a
	// statement nothing can produce. Every such report is a false alarm that
	// costs somebody an afternoon, so the scope has to match Go's.
	var out []sqlSite
	for _, path := range order {
		f := files[path]
		if f == nil {
			continue
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			scope := pkg
			var body ast.Node = decl
			if ok && fn.Body != nil {
				scope = map[string][]string{}
				for k, v := range pkg {
					scope[k] = v
				}
				collectStringNames(fn.Body, scope, true)
				body = fn.Body
			}
			name := "(file scope)"
			if ok && fn.Name != nil {
				name = fn.Name.Name
			}
			out = append(out, sqlInNode(body, scope, path, fset, name)...)
		}
	}
	return out
}

// sqlInNode finds the database calls under one node and resolves each query
// with the name scope that is actually in effect there.
func sqlInNode(root ast.Node, names map[string][]string, path string, fset *token.FileSet, fnName string) []sqlSite {
	var out []sqlSite
	ast.Inspect(root, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !sqlMethods[sel.Sel.Name] {
			return true
		}
		arg := call.Args[0]
		// The Context variants take ctx first and the query second.
		if strings.HasSuffix(sel.Sel.Name, "Context") && len(call.Args) > 1 {
			arg = call.Args[1]
		}
		variants, static := foldStrings(arg, names)
		if static && onlyBlank(variants) {
			return true // not a query at all
		}
		if !static && !isDatabaseHandle(sel.X) {
			return true // Redis, not SQL — see isDatabaseHandle
		}
		multi := false
		for _, v := range variants {
			if isMultiStatement(v) {
				multi = true
			}
		}
		out = append(out, sqlSite{
			file:     path,
			line:     fset.Position(call.Lparen).Line,
			variants: variants,
			dynamic:  !static,
			multi:    multi,
			fn:       fnName,
		})
		return true
	})
	return out
}

// collectStringNames records every name bound to a string literal under one
// node, keeping every distinct value.
//
// locals says whether to descend into function bodies. Called with false over
// a whole file it picks up only package-level declarations; called with true
// over one function body it picks up that function's own names and nobody
// else's. Keeping those apart is what stops one function's value being
// checked against another's query.
func collectStringNames(f ast.Node, into map[string][]string, locals bool) {
	// A name is poisoned the moment anything assigns it a value this cannot
	// work out — a += inside a loop, a strconv call, a function's return. From
	// then on it resolves to nothing at all.
	//
	// The alternative is what the first version did: keep whatever earlier
	// literal it had managed to read, and check a query built from a
	// half-finished value. That reported a syntax error in
	// "... WHERE post_id IN ()" — a statement the code never produces, because
	// the placeholders it reads as empty are filled in by a loop. A false
	// alarm in a checker like this is worse than a miss: it costs somebody an
	// afternoon and teaches them to distrust the tool.
	poisoned := map[string]bool{}
	add := func(name, value string) {
		if poisoned[name] {
			return
		}
		// A blank IS a value. An optional filter that is sometimes "" has to
		// be checked with it empty as well as filled, and dropping the blank
		// here while bolting one on at the read end is how this started
		// building INTERVAL '' — a statement no code path can produce.
		for _, existing := range into[name] {
			if existing == value {
				return
			}
		}
		into[name] = append(into[name], value)
	}
	poison := func(name string) {
		poisoned[name] = true
		delete(into, name)
	}
	remember := func(ids []*ast.Ident, values []ast.Expr) {
		for i, id := range ids {
			if i >= len(values) {
				return
			}
			vs, ok := foldStrings(values[i], into)
			if !ok {
				// Only poison something that was going to be a string. A
				// rows/err assignment is not a query and saying so would
				// blacklist half the identifiers in the file.
				if looksLikeStringExpr(values[i]) {
					poison(id.Name)
				}
				continue
			}
			for _, v := range vs {
				add(id.Name, v)
			}
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		if !locals {
			// Package level only: do not walk into a function, whose names
			// belong to it alone.
			if fn, ok := n.(*ast.FuncDecl); ok {
				_ = fn
				return false
			}
		}
		switch d := n.(type) {
		case *ast.RangeStmt:
			// for _, q := range []string{`SELECT ...`, `SELECT ...`}
			//
			// Two statements written as a list and run in a loop. Without
			// this the loop variable has no value here and both are invisible
			// — which is how the two queries that find a deleted challenge's
			// files got missed. They are literals sitting in plain sight.
			lit, ok := d.X.(*ast.CompositeLit)
			if !ok || d.Value == nil {
				return true
			}
			id, ok := d.Value.(*ast.Ident)
			if !ok {
				return true
			}
			for _, el := range lit.Elts {
				if vs, ok := foldStrings(el, into); ok {
					for _, v := range vs {
						add(id.Name, v)
					}
				}
			}
			return true
		case *ast.ValueSpec: // const x = "..." / var x = "..."
			remember(d.Names, d.Values)
		case *ast.AssignStmt: // x := "..." / x = "..." / x += "..."
			if d.Tok != token.DEFINE && d.Tok != token.ASSIGN {
				// += and friends build a value up over statements, often in a
				// loop this cannot count. Whatever the name held before is no
				// longer what it will hold when the query runs.
				for _, lhs := range d.Lhs {
					if id, ok := lhs.(*ast.Ident); ok {
						poison(id.Name)
					}
				}
				return true
			}
			ids := make([]*ast.Ident, 0, len(d.Lhs))
			for _, lhs := range d.Lhs {
				id, ok := lhs.(*ast.Ident)
				if !ok {
					return true
				}
				ids = append(ids, id)
			}
			remember(ids, d.Rhs)
		}
		return true
	})
}

// looksLikeStringExpr reports that an expression was probably meant to be a
// string, so failing to fold it is worth poisoning the name over.
//
// A deliberately narrow test: a literal, a concatenation, or a call to
// something in fmt or strconv. Everything else — a query handle, an error, a
// struct — is left alone, because poisoning every name in the file would turn
// this check off without saying so.
func looksLikeStringExpr(e ast.Expr) bool {
	switch v := e.(type) {
	case *ast.BasicLit:
		return v.Kind == token.STRING
	case *ast.ParenExpr:
		return looksLikeStringExpr(v.X)
	case *ast.BinaryExpr:
		return v.Op == token.ADD &&
			(looksLikeStringExpr(v.X) || looksLikeStringExpr(v.Y))
	case *ast.CallExpr:
		sel, ok := v.Fun.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok {
			return false
		}
		return pkg.Name == "fmt" || pkg.Name == "strconv" || pkg.Name == "strings"
	}
	return false
}

// foldStrings works out every string an expression can be, when it can.
//
// Anything with a value this cannot see comes back as not-static, because
// guessing one would mean checking a query the code never actually runs —
// which is not a weaker check, it is a wrong one.
func foldStrings(e ast.Expr, names map[string][]string) ([]string, bool) {
	switch v := e.(type) {
	case *ast.BasicLit:
		if v.Kind != token.STRING {
			return nil, false
		}
		s, err := strconv.Unquote(v.Value)
		if err != nil {
			return nil, false
		}
		return []string{s}, true

	case *ast.ParenExpr:
		return foldStrings(v.X, names)

	case *ast.BinaryExpr:
		if v.Op != token.ADD {
			return nil, false
		}
		l, lok := foldStrings(v.X, names)
		r, rok := foldStrings(v.Y, names)
		if !lok || !rok {
			return nil, false
		}
		return cross(l, r)

	case *ast.Ident:
		// A query held in a named constant is still a query. This is how the
		// ownership check in creator_insights.go hid a column that does not
		// exist: the statement was assigned to a variable first, so nothing
		// reading the source ever saw it.
		//
		// Exactly the values the code assigns, and nothing invented. A name
		// that is sometimes blank has the blank among them already, because
		// blanks are recorded.
		vs, ok := names[v.Name]
		if !ok || len(vs) == 0 {
			return nil, false
		}
		return append([]string{}, vs...), true

	case *ast.CallExpr:
		// A plain call to a function in this package whose every return is a
		// literal string resolves to that set. Anything else falls through to
		// the Sprintf handling and then to not-static.
		if id, ok := v.Fun.(*ast.Ident); ok {
			if vs, ok := names[funcCallKey(id.Name)]; ok && len(vs) > 0 {
				return append([]string{}, vs...), true
			}
			return nil, false
		}
		return foldSprintf(v, names)
	}
	return nil, false
}

// funcCallKey namespaces a function's return set so it cannot be confused
// with a variable of the same name.
func funcCallKey(name string) string { return "func()" + name }

// collectFixedReturns records functions whose every return is a plain string.
//
// EVERY return, deliberately. A function with one literal return and one
// computed one resolves to nothing: checking only the branch this can read
// would quietly leave the other unchecked while reporting full coverage.
func collectFixedReturns(f *ast.File, pkg map[string][]string, into map[string][]string) {
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || fn.Type.Results == nil {
			continue
		}
		if len(fn.Type.Results.List) != 1 || len(fn.Type.Results.List[0].Names) > 1 {
			continue
		}
		if id, ok := fn.Type.Results.List[0].Type.(*ast.Ident); !ok || id.Name != "string" {
			continue
		}
		var vals []string
		allStatic := true
		found := false
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			ret, ok := n.(*ast.ReturnStmt)
			if !ok || len(ret.Results) != 1 {
				return true
			}
			found = true
			vs, ok := foldStrings(ret.Results[0], pkg)
			if !ok {
				allStatic = false
				return false
			}
			vals = append(vals, vs...)
			return true
		})
		if found && allStatic && len(vals) > 0 {
			into[fn.Name.Name] = vals
		}
	}
}

// cross joins every left with every right, bounded.
func cross(l, r []string) ([]string, bool) {
	if len(l)*len(r) > maxVariants {
		return nil, false
	}
	out := make([]string, 0, len(l)*len(r))
	seen := map[string]bool{}
	for _, a := range l {
		for _, b := range r {
			if s := a + b; !seen[s] {
				seen[s] = true
				out = append(out, s)
			}
		}
	}
	return out, true
}

// foldSprintf works out what fmt.Sprintf will produce, when every piece is
// known here.
//
// Seventeen queries in analytics_job.go alone are a literal with a window
// length pasted into it. Refusing to look at those left the largest single
// group of queries in the repo unchecked.
//
// Only %s, %q and %d, and only with values this can resolve. Anything else is
// not-static rather than a guess.
func foldSprintf(call *ast.CallExpr, names map[string][]string) ([]string, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Sprintf" {
		return nil, false
	}
	if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "fmt" {
		return nil, false
	}
	if len(call.Args) == 0 {
		return nil, false
	}
	formats, ok := foldStrings(call.Args[0], names)
	if !ok {
		return nil, false
	}
	args := call.Args[1:]

	var out []string
	for _, format := range formats {
		built := []string{""}
		used := 0
		bad := false
		var lit strings.Builder
		flush := func() bool {
			if lit.Len() == 0 {
				return true
			}
			next, ok := cross(built, []string{lit.String()})
			lit.Reset()
			if !ok {
				return false
			}
			built = next
			return true
		}
		for i := 0; i < len(format) && !bad; i++ {
			if format[i] != '%' || i+1 >= len(format) {
				lit.WriteByte(format[i])
				continue
			}
			verb := format[i+1]
			i++
			if verb == '%' {
				lit.WriteByte('%')
				continue
			}
			if used >= len(args) || !flush() {
				bad = true
				break
			}
			arg := args[used]
			used++
			switch verb {
			case 's', 'q':
				vs, ok := foldStrings(arg, names)
				if !ok {
					bad = true
					break
				}
				if verb == 'q' {
					quoted := make([]string, len(vs))
					for i, v := range vs {
						quoted[i] = strconv.Quote(v)
					}
					vs = quoted
				}
				next, ok := cross(built, vs)
				if !ok {
					bad = true
					break
				}
				built = next
			case 'd':
				numLit, ok := arg.(*ast.BasicLit)
				if !ok || numLit.Kind != token.INT {
					bad = true
					break
				}
				next, ok := cross(built, []string{numLit.Value})
				if !ok {
					bad = true
					break
				}
				built = next
			default:
				bad = true
			}
		}
		if bad || used != len(args) || !flush() {
			return nil, false
		}
		out = append(out, built...)
		if len(out) > maxVariants {
			return nil, false
		}
	}
	return out, true
}

// collectParamValues works out what a string parameter can be, from what
// every caller passes.
//
// Ordinary functions and closures held in a variable, both. A parameter with
// even one caller this cannot read is dropped entirely.
func collectParamValues(f *ast.File, pkg map[string][]string, into map[string][]string) {
	// function name -> argument position -> parameter name, for string
	// parameters only.
	stringParams := map[string]map[int]string{}
	record := func(name string, params *ast.FieldList) {
		if params == nil {
			return
		}
		at := map[int]string{}
		i := 0
		for _, field := range params.List {
			id, isIdent := field.Type.(*ast.Ident)
			if len(field.Names) == 0 {
				i++
				continue
			}
			for _, n := range field.Names {
				if isIdent && id.Name == "string" {
					at[i] = n.Name
				}
				i++
			}
		}
		if len(at) > 0 {
			stringParams[name] = at
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.FuncDecl:
			record(v.Name.Name, v.Type.Params)
		case *ast.AssignStmt:
			for i, rhs := range v.Rhs {
				lit, ok := rhs.(*ast.FuncLit)
				if !ok || i >= len(v.Lhs) {
					continue
				}
				if id, ok := v.Lhs[i].(*ast.Ident); ok {
					record(id.Name, lit.Type.Params)
				}
			}
		}
		return true
	})

	unreadable := map[string]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		id, ok := call.Fun.(*ast.Ident)
		if !ok {
			return true
		}
		at, ok := stringParams[id.Name]
		if !ok {
			return true
		}
		for i, param := range at {
			if i >= len(call.Args) {
				continue
			}
			vs, ok := foldStrings(call.Args[i], pkg)
			if !ok {
				unreadable[param] = true
				continue
			}
			for _, v := range vs {
				if strings.TrimSpace(v) == "" {
					continue
				}
				dup := false
				for _, e := range into[param] {
					if e == v {
						dup = true
						break
					}
				}
				if !dup {
					into[param] = append(into[param], v)
				}
			}
		}
		return true
	})
	for p := range unreadable {
		delete(into, p)
	}
}

// onlyBlank reports that nothing here is a statement.
func onlyBlank(vs []string) bool {
	for _, v := range vs {
		if strings.TrimSpace(v) != "" {
			return false
		}
	}
	return true
}

// isMultiStatement reports a string holding more than one command.
//
// Prepare takes exactly one, so the schema block in database.go — which is
// thirty CREATE TABLEs in a single string — cannot go through here. It is
// covered by every other database-backed test in this package, all of which
// run it on the way in.
func isMultiStatement(q string) bool {
	var semis int
	for i := 0; i < len(q); i++ {
		if q[i] == ';' {
			semis++
		}
	}
	if semis == 0 {
		return false
	}
	// A single trailing semicolon is still one statement.
	return semis > 1 || strings.TrimSpace(q[strings.LastIndex(q, ";")+1:]) != ""
}

func indent(s string) string {
	var b strings.Builder
	for _, line := range strings.Split(strings.TrimSpace(s), "\n") {
		b.WriteString("      ")
		b.WriteString(strings.TrimSpace(line))
		b.WriteString("\n")
	}
	return b.String()
}
