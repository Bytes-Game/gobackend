package main

// account_rules.go — what a new account must have, said in plain words.
//
// Sign-up used to take a username and a six-letter password and nothing
// else. No email, so no way to tell one person's tenth account from ten
// people, and nothing to reach them by. No age, so nothing stood between a
// nine-year-old and the app. No terms agreed to. And any name at all, so
// somebody could sign up as "admin" or "devf" and message people as the app.
//
// The rules here are the server's. The app checks the same things as you
// type, so you hear about a problem at once — but the app is code on
// somebody else's phone, and anything can call this server without it.

import (
	"net/mail"
	"regexp"
	"strings"
	"time"
	"unicode"
)

// fieldError is a refusal tied to one box on the form, so the app can show
// it under that box instead of at the bottom of the page.
type fieldError struct {
	Field   string `json:"field"`
	Message string `json:"error"`
}

func (e *fieldError) Error() string { return e.Message }

func refuse(field, msg string) *fieldError { return &fieldError{Field: field, Message: msg} }

// usernameRe: 3 to 20 of a-z, 0-9, underscore and dot.
var usernameRe = regexp.MustCompile(`^[a-z0-9_.]{3,20}$`)

// reservedNames are names nobody may sign up as, because people would take
// them for the app itself or for staff. Checked with dots and underscores
// taken out, so "dev.f" and "_admin_" are caught too.
var reservedNames = map[string]bool{
	"admin": true, "administrator": true, "root": true, "system": true,
	"support": true, "help": true, "helpdesk": true, "staff": true,
	"team": true, "official": true, "moderator": true, "mod": true,
	"security": true, "safety": true, "devf": true, "devfapp": true,
	"devfofficial": true, "devfteam": true, "battlearena": true,
	"flex": true, "flexapp": true, "flexofficial": true, "flexteam": true,
	"flexsupport": true, "teamflex": true,
	"arena": true, "everyone": true, "null": true, "undefined": true,
	"api": true, "www": true, "app": true, "login": true, "signup": true,
	"settings": true, "privacy": true, "terms": true,
}

// usernameProblem says what is wrong with a username, or "" when it is
// fine to use (taken or not — that is the database's to say).
func usernameProblem(name string) string {
	if !usernameRe.MatchString(name) {
		return "Use 3 to 20 characters: letters, numbers, _ and . only."
	}
	if strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") || strings.Contains(name, "..") {
		return "A username can't start or end with a dot, or have two dots in a row."
	}
	if strings.Trim(name, "0123456789_.") == "" {
		return "A username needs at least one letter."
	}
	bare := strings.NewReplacer(".", "", "_", "").Replace(name)
	if reservedNames[bare] {
		return "That name is reserved. Please pick another."
	}
	return ""
}

// commonPasswords are the first ones anybody guessing tries.
var commonPasswords = map[string]bool{
	"password": true, "password1": true, "password123": true, "12345678": true,
	"123456789": true, "1234567890": true, "qwerty123": true, "qwertyuiop": true,
	"iloveyou1": true, "abc12345": true, "11111111": true, "00000000": true,
	"letmein1": true, "welcome1": true, "admin123": true, "football1": true,
}

// minPasswordLen is the shortest password a new account may have.
const minPasswordLen = 8

// passwordProblem says what is wrong with a new password, or "".
func passwordProblem(password, username string) string {
	if len([]rune(password)) < minPasswordLen {
		return "Use at least 8 characters."
	}
	if len(password) > 72 {
		// bcrypt only reads the first 72 bytes; the rest would be ignored.
		return "Use 72 characters or fewer."
	}
	var letter, digit bool
	for _, r := range password {
		switch {
		case unicode.IsLetter(r):
			letter = true
		case unicode.IsDigit(r):
			digit = true
		}
	}
	if !letter || !digit {
		return "Use at least one letter and one number."
	}
	low := strings.ToLower(password)
	if commonPasswords[low] {
		return "That password is too easy to guess."
	}
	if username != "" && strings.Contains(low, username) {
		return "Don't put your username in your password."
	}
	return ""
}

// cleanEmail is the address in one form — trimmed, lower case — and what is
// wrong with it, if anything.
func cleanEmail(raw string) (string, string) {
	email := strings.ToLower(strings.TrimSpace(raw))
	if email == "" {
		return "", "Enter your email."
	}
	if len(email) > 254 {
		return "", "That email is too long."
	}
	addr, err := mail.ParseAddress(email)
	if err != nil || addr.Address != email || !strings.Contains(email, "@") {
		return "", "That doesn't look like an email address."
	}
	domain := email[strings.LastIndex(email, "@")+1:]
	if !strings.Contains(domain, ".") || strings.HasPrefix(domain, ".") ||
		strings.HasSuffix(domain, ".") {
		return "", "That doesn't look like an email address."
	}
	return email, ""
}

// minAge is how old somebody must be to have an account.
const minAge = 13

// parseBirthDate reads a YYYY-MM-DD date of birth and says what is wrong
// with it, if anything. [now] is passed in so tests can fix the day.
func parseBirthDate(raw string, now time.Time) (time.Time, string) {
	d, err := time.Parse("2006-01-02", strings.TrimSpace(raw))
	if err != nil {
		return time.Time{}, "Enter your date of birth."
	}
	if d.Year() < 1900 || d.After(now) {
		return time.Time{}, "Enter your real date of birth."
	}
	if ageOn(d, now) < minAge {
		return time.Time{}, "You need to be 13 or older to join."
	}
	return d, ""
}

// ageOn is how old somebody born on [born] is on [day], in whole years.
func ageOn(born, day time.Time) int {
	age := day.Year() - born.Year()
	if day.Month() < born.Month() || (day.Month() == born.Month() && day.Day() < born.Day()) {
		age--
	}
	return age
}

// maxFullNameLen is the longest name a person may give.
const maxFullNameLen = 50

// cleanFullName trims a name and says what is wrong with it, if anything.
func cleanFullName(raw string) (string, string) {
	name := strings.Join(strings.Fields(raw), " ")
	if len([]rune(name)) > maxFullNameLen {
		return "", "Use 50 characters or fewer."
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return "", "Use letters, numbers and spaces."
		}
	}
	return name, ""
}
