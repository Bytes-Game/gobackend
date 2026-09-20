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
// It is ZERO. Every database call in this repo now says something when it
// fails: what it was working out, and what the app is doing instead.
//
// It was 72 when this check was written. The first pass took it to 34 by
// wrapping every bare db.QueryRow(...).Scan(...). The rest went one at a
// time, and along the way the CHECK itself was wrong twice and had to be
// fixed before the number meant anything:
//
//   - it counted strconv.Atoi and time.Parse, which have nothing to do with a
//     database, because it matched any "if err == nil"
//   - it counted errors that ARE reported a few lines further down
//
// Both made the number bigger than the truth. A checker that cries wolf gets
// switched off, so the number has to mean exactly one thing: a database
// failure nobody hears about. It does now, and it is zero.
//
// Zero is a real gate, not an aspiration. If you add one, this test names the
// file and line. Raise the number only with a reason in the commit message.
const silentFailureBaseline = 0

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
//
// It counts an "if err == nil" block only when BOTH are true:
//
//   * the error came from a database call, and
//   * nothing else in that function does anything about it.
//
// Both conditions were missing from the first version and it cost the check
// its credibility. It flagged strconv.Atoi and time.Parse — which have
// nothing to do with a database — and it flagged errors that ARE reported,
// just a few lines further down. Six of the thirty-four it reported had
// nothing wrong with them.
//
// A checker that cries wolf gets switched off. The number has to mean exactly
// one thing: a database failure nobody hears about.
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
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			out = append(out, silentInFunc(fn, path, fset)...)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	return out
}

func silentInFunc(fn *ast.FuncDecl, path string, fset *token.FileSet) []string {
	var out []string
	walkBlockForSilence(fn.Body, map[string]bool{}, path, fset, &out)
	return out
}

// walkBlockForSilence walks one block, carrying what each error name was last
// assigned from.
//
// Scope matters here, twice over, and both were wrong in earlier versions:
//
//   - A function-wide "was this ever a database error" marks the err in
//     "id, err := strconv.Atoi(...)" as a database error, because some other
//     line in the same long function assigned a query to the same name. That
//     reported parsing a number as a silent database failure.
//   - A function-wide "is this ever handled" excuses ten unhandled queries
//     because an eleventh is handled. computeUserProfile alone would have hidden
//     a dozen.
//
// So: names are tracked statement by statement inside their block, nested
// blocks inherit a copy, and a guard is only excused by the statement right
// next to it.
func walkBlockForSilence(block *ast.BlockStmt, inherited map[string]bool,
	path string, fset *token.FileSet, out *[]string) {
	if block == nil {
		return
	}
	state := make(map[string]bool, len(inherited))
	for k, v := range inherited {
		state[k] = v
	}

	for i, stmt := range block.List {
		switch v := stmt.(type) {
		case *ast.AssignStmt:
			applyAssign(v, state)

		case *ast.ExprStmt:
			// A Scan called as a statement throws its error away. No
			// exceptions: there is no way to have dealt with an error you
			// never took hold of.
			if call, ok := v.X.(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Scan" {
					*out = append(*out, fmt.Sprintf("%s:%d  Scan error discarded",
						path, fset.Position(call.Lparen).Line))
				}
			}

		case *ast.IfStmt:
			inner := make(map[string]bool, len(state))
			for k, val := range state {
				inner[k] = val
			}
			if a, ok := v.Init.(*ast.AssignStmt); ok {
				applyAssign(a, inner)
			}
			if name, ok := errIsNil(v.Cond); ok && inner[name] && v.Else == nil &&
				len(v.Body.List) >= 2 && !dealsWith(neighbours(block.List, i), name) {
				// A one-line body is usually an assignment nobody misses,
				// which is why the length check is there.
				*out = append(*out, fmt.Sprintf("%s:%d  if %s == nil { ... }",
					path, fset.Position(v.If).Line, name))
			}
			walkBlockForSilence(v.Body, inner, path, fset, out)
			if eb, ok := v.Else.(*ast.BlockStmt); ok {
				walkBlockForSilence(eb, inner, path, fset, out)
			} else if ei, ok := v.Else.(*ast.IfStmt); ok {
				walkBlockForSilence(&ast.BlockStmt{List: []ast.Stmt{ei}}, inner, path, fset, out)
			}
			continue
		}

		// Everything else that has a body of its own.
		ast.Inspect(stmt, func(n ast.Node) bool {
			switch b := n.(type) {
			case *ast.IfStmt:
				return false // handled above when it is a direct statement
			case *ast.BlockStmt:
				if b != block {
					walkBlockForSilence(b, state, path, fset, out)
					return false
				}
			}
			return true
		})
	}
}

// applyAssign records, for each error name on the left, whether the right
// side is a database call. A name reassigned from something else stops being
// a database error, which is the whole point of tracking it per statement.
func applyAssign(a *ast.AssignStmt, state map[string]bool) {
	db := hasDBCall(a.Rhs)
	for _, lhs := range a.Lhs {
		if id, ok := lhs.(*ast.Ident); ok && isErrName(id.Name) {
			state[id.Name] = db
		}
	}
}

// neighbours returns the statements immediately before and after index i.
func neighbours(list []ast.Stmt, i int) []ast.Stmt {
	var out []ast.Stmt
	if i > 0 {
		out = append(out, list[i-1])
	}
	if i+1 < len(list) {
		out = append(out, list[i+1])
	}
	return out
}

// dealsWith reports that one of these statements says something about name.
func dealsWith(stmts []ast.Stmt, name string) bool {
	found := false
	for _, st := range stmts {
		ast.Inspect(st, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.IfStmt:
				if got, ok := errNotNil(v.Cond); ok && got == name {
					found = true
					return false
				}
			case *ast.CallExpr:
				id, ok := v.Fun.(*ast.Ident)
				if !ok || (id.Name != "queryFailed" && id.Name != "scanFailed") {
					return true
				}
				for _, a := range v.Args {
					if e, ok := a.(*ast.Ident); ok && e.Name == name {
						found = true
						return false
					}
				}
			case *ast.ReturnStmt:
				for _, r := range v.Results {
					if id, ok := r.(*ast.Ident); ok && id.Name == name {
						found = true
						return false
					}
				}
			}
			return true
		})
		if found {
			return true
		}
	}
	return false
}

// hasDBCall reports that one of these expressions calls the database.
func hasDBCall(exprs []ast.Expr) bool {
	found := false
	for _, e := range exprs {
		ast.Inspect(e, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch sel.Sel.Name {
			case "Query", "QueryRow", "Exec", "QueryContext",
				"QueryRowContext", "ExecContext", "Scan", "Prepare":
				found = true
				return false
			}
			return true
		})
		if found {
			return true
		}
	}
	return false
}

func isErrName(n string) bool {
	return n == "err" || strings.HasSuffix(n, "Err") || strings.HasSuffix(n, "err")
}

// errIsNil matches `err == nil` and returns the name.
func errIsNil(e ast.Expr) (string, bool) { return errCompare(e, token.EQL) }

// errNotNil matches `err != nil` and returns the name.
func errNotNil(e ast.Expr) (string, bool) { return errCompare(e, token.NEQ) }

func errCompare(e ast.Expr, op token.Token) (string, bool) {
	bin, ok := e.(*ast.BinaryExpr)
	if !ok || bin.Op != op {
		return "", false
	}
	lhs, ok := bin.X.(*ast.Ident)
	if !ok || !isErrName(lhs.Name) {
		return "", false
	}
	rhs, ok := bin.Y.(*ast.Ident)
	if !ok || rhs.Name != "nil" {
		return "", false
	}
	return lhs.Name, true
}
