package main

// legal_pages.go — the pages the app stores ask for, at web addresses.
//
// Google Play and the App Store each want links, not just text inside the
// app: to the privacy policy, to a page explaining how to delete an
// account without the app, and (Play, for apps where people post) to the
// app's child safety standards. India's IT Rules want the Terms and the
// Grievance Officer published too. So this server shows, with no sign-in:
//
//   /terms                 the Terms of Service
//   /privacy               the Privacy Policy
//   /community-guidelines  the Community Guidelines
//   /delete-account        how to delete an account, with or without the app
//   /child-safety          the child safety standards
//
// The first three are the app's own words (Frontend:
// lib/pages/legal_texts.dart), copied into legal/texts.json by
// tools/legal_from_app.py. Change them in the app, then run the tool.

import (
	_ "embed"
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"strings"
)

//go:embed legal/texts.json
var legalJSON []byte

type legalSection struct {
	Title string   `json:"title"`
	Body  []string `json:"body"`
}

type legalDocument struct {
	Title    string         `json:"title"`
	Intro    string         `json:"intro"`
	Summary  []string       `json:"summary"`
	Sections []legalSection `json:"sections"`
	Contact  string         `json:"contact"`
}

type legalTexts struct {
	Updated    string        `json:"updated"`
	Terms      legalDocument `json:"terms"`
	Privacy    legalDocument `json:"privacy"`
	Guidelines legalDocument `json:"guidelines"`
}

// legal is the app's documents, read once at start. A copy that cannot be
// read stops the server starting: serving a blank privacy policy to a
// store reviewer is worse than a failed deploy someone notices.
var legal = func() legalTexts {
	var t legalTexts
	if err := json.Unmarshal(legalJSON, &t); err != nil {
		log.Fatalf("legal/texts.json cannot be read: %v", err)
	}
	return t
}()

// legalParagraph is one paragraph: plain, or a bullet point ("• ...").
type legalParagraph struct {
	Text   string
	Bullet bool
}

func paragraphs(body []string) []legalParagraph {
	out := make([]legalParagraph, 0, len(body))
	for _, p := range body {
		if rest, ok := strings.CutPrefix(p, "• "); ok {
			out = append(out, legalParagraph{Text: rest, Bullet: true})
		} else {
			out = append(out, legalParagraph{Text: p})
		}
	}
	return out
}

// legalPage is what one page shows.
type legalPage struct {
	Path     string
	Title    string
	Updated  string
	Intro    string
	Summary  []string
	Sections []legalPageSection
	Contact  string
	Support  string
}

type legalPageSection struct {
	Title string
	Paras []legalParagraph
}

func fromDocument(path string, d legalDocument) legalPage {
	p := legalPage{Path: path, Title: d.Title, Updated: legal.Updated, Intro: d.Intro,
		Summary: d.Summary, Contact: d.Contact, Support: supportEmail}
	for _, s := range d.Sections {
		p.Sections = append(p.Sections, legalPageSection{Title: s.Title, Paras: paragraphs(s.Body)})
	}
	return p
}

// deleteAccountPage is Google Play's "how to delete your account" page:
// the app's name, the steps, what goes and what may stay, and how long.
func deleteAccountPage() legalPage {
	return legalPage{
		Path:    "/delete-account",
		Title:   "Delete your Flexete account",
		Updated: legal.Updated,
		Intro: "You can delete your Flexete account at any time, in the app or by email. " +
			"This page says how, and what happens to your information.",
		Support: supportEmail,
		Sections: []legalPageSection{
			{Title: "In the app", Paras: paragraphs([]string{
				"• Open your profile and tap Settings (the gear).",
				"• Tap Delete account, and confirm.",
			})},
			{Title: "Without the app", Paras: paragraphs([]string{
				"Write to " + supportEmail + " from the email address on your account, " +
					"with your username, and ask us to delete it. We check it is you, " +
					"delete it the same way, and answer within 15 days.",
			})},
			{Title: "What happens", Paras: paragraphs([]string{
				"• Straight away, your account is hidden from everyone and signed out on every phone. " +
					"Battles you are in at that moment end with no result.",
				"• For 30 days, signing in again lets you keep it, with everything back.",
				"• After 30 days it is deleted for good: your profile, photos and videos, battles, " +
					"comments, likes, votes, followers, messages and watch history.",
				"• Some records may be kept longer where the law requires it, or to deal with abuse.",
			})},
			{Title: "Only some of your information", Paras: paragraphs([]string{
				"You do not have to delete your account to delete things in it. In the app you can " +
					"delete any of your videos and comments, and clear your watch history, at any time. " +
					"For anything else, write to " + supportEmail + ".",
			})},
		},
	}
}

// childSafetyPage is Google Play's child safety standards page, for apps
// where people post and talk to each other.
func childSafetyPage() legalPage {
	return legalPage{
		Path:    "/child-safety",
		Title:   "Child safety standards",
		Updated: legal.Updated,
		Intro: "Flexete does not allow child sexual abuse and exploitation of any kind: no child " +
			"sexual abuse material, no sexualising anyone under 18, no grooming, no sextortion, " +
			"and nothing that puts a child in danger.",
		Support: supportEmail,
		Sections: []legalPageSection{
			{Title: "Who can use Flexete", Paras: paragraphs([]string{
				"Flexete is for people aged 13 and over. When we learn that someone under 13 has an " +
					"account, we delete it.",
			})},
			{Title: "Reporting it", Paras: paragraphs([]string{
				"• On any video: tap More (⋯), then Report, then \"A child may be at risk\".",
				"• On an account: open their profile, tap More (⋯), then Report.",
				"• On a comment: press and hold it.",
				"A person reads every report, and reports about a child's safety come first.",
			})},
			{Title: "What we do", Paras: paragraphs([]string{
				"We remove the content, suspend the account, and keep what the law requires us to keep. " +
					"We report child sexual abuse material and child exploitation to the authority each " +
					"country's law names: in India the police or the National Cyber Crime Reporting " +
					"Portal, in the United States the National Center for Missing & Exploited Children, " +
					"and the matching authority elsewhere. We help the police with their investigations.",
			})},
			{Title: "If a child is in danger now", Paras: paragraphs([]string{
				"Contact your local emergency services straight away. A report to us is read later, " +
					"and help should not wait for it.",
			})},
			{Title: "Contact", Paras: paragraphs([]string{
				"Our child safety contact, for anyone, including the police and organisations that " +
					"protect children: " + supportEmail + ".",
			})},
		},
	}
}

// legalPages are every page here, by address.
func legalPages() map[string]legalPage {
	return map[string]legalPage{
		"/terms":                fromDocument("/terms", legal.Terms),
		"/privacy":              fromDocument("/privacy", legal.Privacy),
		"/community-guidelines": fromDocument("/community-guidelines", legal.Guidelines),
		"/delete-account":       deleteAccountPage(),
		"/child-safety":         childSafetyPage(),
	}
}

// legalPageRules: the pages have no script and load nothing.
const legalPageRules = "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"

// LegalPageHandler serves the page at the request's address.
func LegalPageHandler(w http.ResponseWriter, r *http.Request) {
	page, ok := legalPages()[r.URL.Path]
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Security-Policy", legalPageRules)
	w.Header().Set("Cache-Control", "public, max-age=3600")
	if err := legalTmpl.Execute(w, page); err != nil {
		log.Printf("legal page %s: %v", r.URL.Path, err)
	}
}

const legalStyle = `
:root{--bg:#fff;--text:#111;--muted:#5b5f6b;--line:#e4e6eb;--accent:#2f6aff;--card:#f6f7f9}
@media (prefers-color-scheme:dark){:root{--bg:#0b0b0e;--text:#f2f3f5;--muted:#a3a7b3;--line:#24262d;--accent:#6f97ff;--card:#15161b}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font:16px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}
main{max-width:720px;margin:0 auto;padding:28px 16px 56px}
.brand{display:flex;align-items:center;gap:8px;font-weight:800;color:var(--text);text-decoration:none;margin-bottom:24px}
.brand b{width:26px;height:26px;border-radius:8px;background:var(--accent);color:#fff;display:grid;place-items:center;font-size:15px}
h1{font-size:28px;line-height:1.2;margin:0 0 6px}
.updated{color:var(--muted);font-size:14px;margin:0 0 20px}
.short{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:14px 18px;margin:20px 0}
.short h2{font-size:15px;margin:0 0 6px}
h2{font-size:19px;margin:28px 0 8px}
ul{padding-left:22px;margin:6px 0}
li{margin:4px 0}
p{margin:8px 0}
nav{border-top:1px solid var(--line);margin-top:36px;padding-top:16px;font-size:14px;display:flex;flex-wrap:wrap;gap:8px 16px}
a{color:var(--accent)}
`

// "inc" numbers the sections from 1, as the app does.
var legalTmpl = template.Must(template.New("legal").Funcs(template.FuncMap{
	"inc": func(i int) int { return i + 1 },
}).Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{.Title}} · Flexete</title>
<meta name="description" content="{{.Title}} for Flexete">
<style>` + legalStyle + `</style></head>
<body><main>
<a class="brand" href="/terms"><b>f</b>Flexete</a>
<h1>{{.Title}}</h1>
<p class="updated">Last updated {{.Updated}}</p>
<p>{{.Intro}}</p>
{{if .Summary}}<section class="short"><h2>The short version</h2><ul>{{range .Summary}}<li>{{.}}</li>{{end}}</ul></section>{{end}}
{{range $i, $s := .Sections}}<h2>{{if $.Summary}}{{inc $i}}. {{end}}{{$s.Title}}</h2>
{{range $s.Paras}}{{if .Bullet}}<ul><li>{{.Text}}</li></ul>{{else}}<p>{{.Text}}</p>{{end}}{{end}}
{{end}}
{{if .Contact}}<p>Write to us about this page at <a href="mailto:{{.Contact}}">{{.Contact}}</a>.</p>{{end}}
<nav>
<a href="/terms">Terms of Service</a>
<a href="/privacy">Privacy Policy</a>
<a href="/community-guidelines">Community Guidelines</a>
<a href="/child-safety">Child safety</a>
<a href="/delete-account">Delete your account</a>
<a href="mailto:{{.Support}}">{{.Support}}</a>
</nav>
</main></body></html>`))
