package main

// silent_failures_test.go — counting the places a failure still looks like an
// answer.
//
// ════════════════════════════════════════════════════════════════════════════
// WHAT IS BEING COUNTED
// ════════════════════════════════════════════════════════════════════════════
//
// Two shapes, both of which make a broken query indistinguishable from an
// empty database:
//
//	rows, err := db.Query(...)
//	if err == nil {              // fails  ->  do nothing, say nothing
//	    ... all the work ...
//	}
//
//	rows.Scan(&a, &b)            // error discarded
//
// The second is the nastier one. Scan fails for the WHOLE result set at once:
// a column list that does not line up with the destination list fails on row
// one and on all five hundred. The loop then finishes normally having built
// its answer out of zero values, which reads as a brand-new account.
//
// Twelve queries in this repo named a column that does not exist and had been
// failing on every request since they were written — the follow-graph feed
// lane, both account-suggestion lanes, the mood half of every taste profile,
// the engagement-quality score, the creator dashboard, trending posts. Not
// one produced a single line of output in all that time, because every one of
// them sat behind one of these two shapes.
//
// ════════════════════════════════════════════════════════════════════════════
// WHY A COUNT AND NOT A BAN
// ════════════════════════════════════════════════════════════════════════════
//
// Some of these are fine. A query whose failure the caller reports some other
// way, a Scan in a test helper, a lookup where empty really is the only
// possible meaning. Demanding zero would mean a wall of exemptions, and a
// wall of exemptions is how a check gets ignored.
//
// So the gate is the NUMBER, the same way .nilaway-baseline works in CI. It
// can go down and it cannot go up without somebody saying why. queryFailed
// and scanFailed in query_failures.go are the two-line way to lower it.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// silentFailureBaseline is how many places still treat a failed database call
// as an empty result.
//
// It was 72. The pass that brought it here wrapped every bare
// db.QueryRow(...).Scan(...) — 38 of them — and the query and row reads on
// the feed and profile paths, which is where the twelve broken queries
// actually were.
//
// What is left is mostly the analytics job and a few admin handlers: places
// where the caller already reports the failure some other way, or where an
// empty answer genuinely is the only meaning. Working through them is
// worthwhile and none of it is urgent.
//
// Lower it whenever you fix one. It must never go up without a reason in the
// commit message.
const silentFailureBaseline = 34

func TestSilentDatabaseFailuresDoNotGrow(t *testing.T) {
	found := findSilentFailures(t)

	if len(found) > silentFailureBaseline {
		sort.Strings(found)
		t.Errorf("places where a failed database call looks like an empty "+
			"result went from %d to %d.\n\n"+
			"A query that fails behind one of these produces the same output "+
			"as a database with nothing in it: no error, no log line, nothing "+
			"to search for. Twelve queries in this repo were broken for "+
			"months behind exactly this.\n\n"+
			"queryFailed and scanFailed in query_failures.go are the two-line "+
			"fix. If this one genuinely cannot say anything, raise "+
			"silentFailureBaseline and say why.\n\nAll of them:\n  %s",
			silentFailureBaseline, len(found), strings.Join(found, "\n  "))
		return
	}
	if len(found) < silentFailureBaseline {
		t.Logf("down to %d from %d — lower silentFailureBaseline to %d so it "+
			"cannot creep back up", len(found), silentFailureBaseline, len(found))
	}
	if testing.Verbose() {
		sort.Strings(found)
		for _, f := range found {
			t.Logf("  still silent: %s", f)
		}
	}
}

// findSilentFailures walks the source for the two shapes.
func findSilentFailures(t *testing.T) []string {
	t.Helper()
	var out []string
	fset := token.NewFileSet()

	err := filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
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
		ast.Inspect(f, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.IfStmt:
				// if err == nil { ... }  with no else, guarding real work.
				if v.Else != nil || !isErrIsNil(v.Cond) {
					return true
				}
				// Only when the block does enough for its absence to matter.
				// A one-line body is usually an assignment nobody misses.
				if len(v.Body.List) < 2 {
					return true
				}
				out = append(out, fmt.Sprintf("%s:%d  if err == nil { ... }",
					path, fset.Position(v.If).Line))

			case *ast.ExprStmt:
				// A Scan called as a statement throws its error away.
				call, ok := v.X.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Scan" {
					return true
				}
				out = append(out, fmt.Sprintf("%s:%d  Scan error discarded",
					path, fset.Position(call.Lparen).Line))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	return out
}

// isErrIsNil matches `err == nil` and nothing else.
func isErrIsNil(e ast.Expr) bool {
	bin, ok := e.(*ast.BinaryExpr)
	if !ok || bin.Op != token.EQL {
		return false
	}
	lhs, ok := bin.X.(*ast.Ident)
	if !ok || !strings.HasSuffix(strings.ToLower(lhs.Name), "err") {
		return false
	}
	rhs, ok := bin.Y.(*ast.Ident)
	return ok && rhs.Name == "nil"
}
