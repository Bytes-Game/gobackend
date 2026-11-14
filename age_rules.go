package main

// age_rules.go — how old somebody must be to join, by country.
//
// Two ages matter:
//
//   - the minimum age: younger than this, nobody can have an account;
//   - the consent age: younger than this, a parent or guardian must agree
//     before the account is made (parent_consent.go).
//
// ════════════════════════════════════════════════════════════════════════
// WHY ONE STRICT RULE, AND A SHORT LIST OF EXCEPTIONS
// ════════════════════════════════════════════════════════════════════════
//
// The laws differ by country and are changing every few months. India
// asks for a parent's consent under 18. The European Union asks for it
// under 13 to 16 depending on the country. Australia (December 2025),
// Indonesia (March 2026) and Malaysia (June 2026) do not allow accounts
// under 16 at all, Brazil (March 2026) allows them only tied to a parent's
// account, and several more countries have said they will follow.
//
// A table with every country's own number would be wrong somewhere within
// months. So the rule here is the strict one that is lawful everywhere:
//
//   - 13 or older to join, and
//   - a parent's consent for everybody under 18,
//
// and the list below holds only the countries whose law is stricter than
// that. A country with a lower age in its law loses nothing by being held
// to the stricter one. When a country raises its age, add a line.
//
// A request whose country is not known gets the same strict rule.

import (
	"net/http"
)

// ageRule is the two ages for one country.
type ageRule struct {
	// MinAge: younger than this, nobody can have an account.
	MinAge int
	// ConsentAge: younger than this, a parent or guardian must agree first.
	ConsentAge int
}

// defaultAgeRule is the rule everywhere not listed in countryAgeRules.
var defaultAgeRule = ageRule{MinAge: 13, ConsentAge: 18}

// countryAgeRules are the countries whose law is stricter than
// defaultAgeRule. Each line says which law, and since when.
var countryAgeRules = map[string]ageRule{
	// Online Safety Amendment (Social Media Minimum Age) Act 2024: no
	// accounts under 16, in force since 10 December 2025.
	"AU": {MinAge: 16, ConsentAge: 18},
	// Indonesia: under-16 accounts closed on the platforms its regulator
	// calls high-risk, from 28 March 2026 (as reported; the rule is known
	// as "PP Tunas"). Held to 16 here, which is safe either way.
	"ID": {MinAge: 16, ConsentAge: 18},
	// Malaysia: no account of one's own under 16, from 1 June 2026 (as
	// reported). Held to 16 here.
	"MY": {MinAge: 16, ConsentAge: 18},
	// Brazil: the "ECA Digital" law (15.211 of 2025, in force March 2026)
	// lets somebody under 16 have an account only when it is joined to a
	// parent's own account, with tools for the parent to watch over it.
	// This app has no such tools, so nobody under 16 joins from Brazil.
	"BR": {MinAge: 16, ConsentAge: 18},
}

// ageRuleFor is the rule for [country] (two capital letters, or "" when
// it is not known).
func ageRuleFor(country string) ageRule {
	if r, ok := countryAgeRules[country]; ok {
		return r
	}
	return defaultAgeRule
}

// SignupRulesHandler — GET /signup/rules.
//
// Tells the sign-up form the rules for where the person is, so it can ask
// for a parent's email as soon as the date of birth shows one is needed,
// and say the right minimum age. Also the one place the country this
// server sees for a caller can be read from outside: "source" is "network"
// when the host passed it, "device" when only the phone said it, and ""
// when nobody did.
func SignupRulesHandler(w http.ResponseWriter, r *http.Request) {
	country, source := countryOf(r)
	rule := ageRuleFor(country)
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]any{
		"country":          country,
		"source":           source,
		"minAge":           rule.MinAge,
		"parentConsentAge": rule.ConsentAge,
	})
}
