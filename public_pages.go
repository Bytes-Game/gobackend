package main

// public_pages.go — what a shared link or a scanned QR code opens.
//
// The app's Share button hands out a link, and its share page draws that
// link as a QR code. Until now the link pointed at a website that did not
// exist, so whoever tapped it or scanned it got an error page. These two
// pages are what they get instead, served by this server at its own
// address, no sign-in needed:
//
//   GET /u/{username}   a person: photo, name, league, record, followers,
//                       bio and their latest public videos
//   GET /v/{id}         one video, playing, with who made it
//
// Both carry link-preview tags, so a link pasted into WhatsApp or iMessage
// shows the photo and the name rather than a bare address.
//
// Only what anybody may already see is shown. A private account shows its
// name and photo and says it is private, the way Instagram's web profile
// does; a friends-only or removed video is "not available", the same as one
// that does not exist, so a link cannot be used to find out which.

import (
	"database/sql"
	"errors"
	"html/template"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
)

// appDownloadURL is where "Get devf" goes, from APP_DOWNLOAD_URL — the
// store listing once there is one. Without it the page says to find the
// person in the app instead of showing a button that goes nowhere.
func appDownloadURL() string {
	return strings.TrimSpace(getEnvFromOS("APP_DOWNLOAD_URL"))
}

type publicVideoTile struct {
	ID    string
	Thumb string
	Title string
}

type publicProfile struct {
	Username, Name, Initial, Avatar, Tag, Bio, League string
	Rating, Wins, Losses, Draws                       int
	Followers, Following, Posts                       int
	Ranked, Private                                   bool
	Videos                                            []publicVideoTile
	Download                                          string
	PageURL                                           string
}

type publicVideo struct {
	ID, Title, Creator, CreatorInitial, CreatorAvatar string
	Poster, Source                                    string
	IsPhoto                                           bool
	Likes, Views                                      int
	Download                                          string
	PageURL                                           string
}

// PublicProfilePageHandler serves GET /u/{username}.
func PublicProfilePageHandler(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(strings.TrimSpace(mux.Vars(r)["username"]), "@")
	u, ok := GetUserByUsername(name)
	if !ok || name == "" {
		publicNotFound(w, "This account isn't available",
			"It may have been deleted, or the link is wrong.")
		return
	}
	p := publicProfile{
		Username: u.Username,
		Name:     u.FullName,
		Avatar:   u.AvatarURL,
		Tag:      u.ProfileTag,
		Bio:      u.Bio,
		League:   u.League,
		Rating:   u.Rating,
		Wins:     u.Wins,
		Losses:   u.Losses,
		Draws:    u.Draws,
		Private:  u.Visibility == "friends",
		Download: appDownloadURL(),
		PageURL:  publicBaseURL(r) + "/u/" + u.Username,
	}
	if p.Name == "" {
		p.Name = u.Username
	}
	p.Initial = strings.ToUpper(string([]rune(u.Username + "?")[0]))
	p.Ranked = p.Wins+p.Losses+p.Draws > 0 && p.League != "" && p.League != "Unranked"
	err := db.QueryRow(`
		SELECT (SELECT COUNT(*) FROM follows WHERE following_id = $1),
		       (SELECT COUNT(*) FROM follows WHERE follower_id = $1),
		       (SELECT COUNT(*) FROM challenges c
		         WHERE c.creator_id = $1 AND `+searchableWhere("c")+`)`,
		u.ID).Scan(&p.Followers, &p.Following, &p.Posts)
	queryFailed("counting followers and posts for the public page of "+u.Username,
		"showing zeroes", err)
	if !p.Private {
		for _, c := range GetChallengesByCreator(u.ID, false, 9, 0) {
			if !isSearchable(c.Visibility, c.Status) {
				continue
			}
			p.Videos = append(p.Videos, publicVideoTile{
				ID: c.ID, Thumb: c.ThumbnailURL,
				Title: challengeTitle(c.Prefix, c.Subject),
			})
		}
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=300")
	if err := publicProfileTmpl.Execute(w, p); err != nil {
		log.Printf("public page: could not draw the profile of %s: %v", u.Username, err)
	}
}

// PublicVideoPageHandler serves GET /v/{id}.
func PublicVideoPageHandler(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	c, ok := GetChallengeByID(id)
	if !ok || !isSearchable(c.Visibility, c.Status) || accountHiddenFrom("", c.CreatorID) {
		publicNotFound(w, "This video isn't available",
			"It may have been deleted, shared only with friends, or the link is wrong.")
		return
	}
	v := publicVideo{
		ID:            c.ID,
		Title:         challengeTitle(c.Prefix, c.Subject),
		Creator:       c.CreatorUsername,
		CreatorAvatar: creatorAvatar(c.CreatorID),
		Poster:        c.ThumbnailURL,
		IsPhoto:       c.MediaType == "photo",
		Likes:         c.Likes,
		Views:         c.Views,
		Download:      appDownloadURL(),
		PageURL:       publicBaseURL(r) + "/v/" + c.ID,
	}
	v.CreatorInitial = strings.ToUpper(string([]rune(c.CreatorUsername + "?")[0]))
	if v.Title == "" {
		v.Title = "A video by @" + c.CreatorUsername
	}
	// A size a browser on any connection can play: the encoded versions
	// before the file that came off the camera, which can be huge.
	v.Source = c.VideoURL
	for _, q := range []string{"720p", "480p", "360p", "1080p"} {
		if u := c.VideoVariants[q]; u != "" {
			v.Source = u
			break
		}
	}
	if v.IsPhoto && v.Poster == "" {
		v.Poster = c.VideoURL
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=300")
	if err := publicVideoTmpl.Execute(w, v); err != nil {
		log.Printf("public page: could not draw video %s: %v", c.ID, err)
	}
}

// creatorAvatar is the photo of the person who posted, for the video page.
func creatorAvatar(creatorID string) string {
	var url string
	err := db.QueryRow(`SELECT COALESCE(avatar_url, '') FROM users WHERE id = CAST($1 AS INT)`,
		creatorID).Scan(&url)
	if errors.Is(err, sql.ErrNoRows) {
		return ""
	}
	queryFailed("reading the photo of user "+creatorID+" for a public page",
		"showing their initial", err)
	return url
}

// publicBaseURL is this server's own address as the visitor reached it,
// for the link-preview tags.
func publicBaseURL(r *http.Request) string {
	scheme := "https"
	if p := r.Header.Get("X-Forwarded-Proto"); p != "" {
		scheme = p
	} else if r.TLS == nil {
		scheme = "http"
	}
	return scheme + "://" + r.Host
}

func publicNotFound(w http.ResponseWriter, title, line string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusNotFound)
	if err := publicMissingTmpl.Execute(w, map[string]string{
		"Title": title, "Line": line, "Download": appDownloadURL(),
	}); err != nil {
		log.Printf("public page: could not draw the not-found page: %v", err)
	}
}

func publicCount(n int) string {
	switch {
	case n >= 1000000:
		return strconv.FormatFloat(float64(n)/1000000, 'f', 1, 64) + "M"
	case n >= 1000:
		return strconv.FormatFloat(float64(n)/1000, 'f', 1, 64) + "K"
	}
	return strconv.Itoa(n)
}

// The look: black, white and the app's blue, the same as the app.
const publicStyle = `
:root{--bg:#000;--card:#1c1c1e;--line:rgba(255,255,255,.08);--text:#fff;--muted:rgba(235,235,245,.6);--blue:#0a84ff}
*{box-sizing:border-box;margin:0;padding:0}
body{background:radial-gradient(120% 60% at 50% -10%,#0a2a4d 0%,#000 60%) fixed,#000;color:var(--text);
font:16px/1.45 -apple-system,BlinkMacSystemFont,"SF Pro Text","Segoe UI",Roboto,sans-serif;min-height:100vh;
display:flex;justify-content:center;padding:28px 16px 48px}
main{width:100%;max-width:520px}
.brand{display:flex;align-items:center;gap:8px;font-weight:800;letter-spacing:.2px;margin-bottom:22px;color:var(--text);text-decoration:none}
.brand b{width:26px;height:26px;border-radius:8px;background:var(--blue);display:grid;place-items:center;font-size:15px}
.card{background:var(--card);border:1px solid var(--line);border-radius:22px;padding:24px;box-shadow:0 30px 80px rgba(0,0,0,.5)}
.head{display:flex;gap:18px;align-items:center}
.av{width:88px;height:88px;border-radius:50%;object-fit:cover;flex:none;border:2px solid var(--blue);padding:3px;background:#000}
.init{width:88px;height:88px;border-radius:50%;flex:none;display:grid;place-items:center;font-size:36px;font-weight:700;
background:linear-gradient(#a5abb8,#858994);border:2px solid var(--blue)}
h1{font-size:22px;line-height:1.2}
.handle{color:var(--muted);font-size:15px}
.tag{display:inline-block;margin-top:6px;font-size:12px;font-weight:700;color:var(--blue);background:rgba(10,132,255,.14);
padding:3px 9px;border-radius:999px}
.stats{display:flex;justify-content:space-between;margin:22px 0 4px;text-align:center}
.stats div{flex:1}.stats strong{display:block;font-size:19px}.stats span{color:var(--muted);font-size:12.5px}
.bio{margin-top:16px;white-space:pre-line}
.league{margin-top:14px;color:var(--muted);font-size:14px}
.private{margin-top:20px;padding:16px;border-radius:14px;background:rgba(255,255,255,.04);color:var(--muted);text-align:center}
.grid{display:grid;grid-template-columns:repeat(3,1fr);gap:2px;margin-top:20px;border-radius:14px;overflow:hidden}
.grid a{aspect-ratio:2/3;background:#2c2c2e center/cover no-repeat;display:block}
.cta{display:block;margin-top:22px;text-align:center;background:var(--blue);color:#fff;text-decoration:none;font-weight:700;
padding:14px;border-radius:14px}
.hint{margin-top:18px;text-align:center;color:var(--muted);font-size:14px}
.media{width:100%;border-radius:16px;background:#000;display:block;max-height:72vh;object-fit:contain}
.by{display:flex;align-items:center;gap:10px;margin-top:16px;color:var(--text);text-decoration:none}
.by img,.by .mini{width:36px;height:36px;border-radius:50%;object-fit:cover;flex:none}
.by .mini{display:grid;place-items:center;background:linear-gradient(#a5abb8,#858994);font-weight:700}
.title{font-size:19px;font-weight:700;margin-top:14px}
.meta{color:var(--muted);font-size:14px;margin-top:4px}
`

var publicFuncs = template.FuncMap{"count": publicCount}

var publicProfileTmpl = template.Must(template.New("u").Funcs(publicFuncs).Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{.Name}} (@{{.Username}}) on devf</title>
<meta name="description" content="{{if .Private}}A private account on devf.{{else}}{{count .Followers}} followers · {{.Posts}} posts{{if .Ranked}} · {{.League}} league{{end}}. Watch their battles on devf.{{end}}">
<meta property="og:type" content="profile">
<meta property="og:title" content="{{.Name}} (@{{.Username}}) on devf">
<meta property="og:description" content="{{if .Private}}A private account on devf.{{else}}{{count .Followers}} followers · {{.Posts}} posts. Watch their battles on devf.{{end}}">
<meta property="og:url" content="{{.PageURL}}">
{{if .Avatar}}<meta property="og:image" content="{{.Avatar}}">{{end}}
<meta name="twitter:card" content="summary">
<style>` + publicStyle + `</style></head>
<body><main>
<a class="brand" href="{{.PageURL}}"><b>d</b>devf</a>
<section class="card">
  <div class="head">
    {{if .Avatar}}<img class="av" src="{{.Avatar}}" alt="{{.Username}}">{{else}}<div class="init">{{.Initial}}</div>{{end}}
    <div>
      <h1>{{.Name}}</h1>
      <div class="handle">@{{.Username}}</div>
      {{if .Tag}}<span class="tag">{{.Tag}}</span>{{end}}
    </div>
  </div>
  <div class="stats">
    <div><strong>{{.Posts}}</strong><span>Posts</span></div>
    <div><strong>{{count .Followers}}</strong><span>Followers</span></div>
    <div><strong>{{count .Following}}</strong><span>Following</span></div>
    <div><strong>{{.Wins}}</strong><span>Wins</span></div>
  </div>
  {{if .Ranked}}<div class="league">{{.League}} league · {{.Rating}} rating · {{.Wins}}W {{.Losses}}L {{.Draws}}D</div>{{end}}
  {{if .Bio}}<p class="bio">{{.Bio}}</p>{{end}}
  {{if .Private}}<div class="private">This account is private. Follow @{{.Username}} in the app to see their videos.</div>{{end}}
  {{if .Videos}}<div class="grid">{{range .Videos}}<a href="/v/{{.ID}}" title="{{.Title}}" style="background-image:url('{{.Thumb}}')"></a>{{end}}</div>{{end}}
  {{if .Download}}<a class="cta" href="{{.Download}}">Get devf</a>
  {{else}}<p class="hint">Open devf and search <strong>@{{.Username}}</strong></p>{{end}}
</section>
</main></body></html>`))

var publicVideoTmpl = template.Must(template.New("v").Funcs(publicFuncs).Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{.Title}} · @{{.Creator}} on devf</title>
<meta name="description" content="By @{{.Creator}} · {{count .Likes}} likes · {{count .Views}} views on devf.">
<meta property="og:type" content="{{if .IsPhoto}}website{{else}}video.other{{end}}">
<meta property="og:title" content="{{.Title}}">
<meta property="og:description" content="By @{{.Creator}} on devf">
<meta property="og:url" content="{{.PageURL}}">
{{if .Poster}}<meta property="og:image" content="{{.Poster}}">{{end}}
<meta name="twitter:card" content="summary_large_image">
<style>` + publicStyle + `</style></head>
<body><main>
<a class="brand" href="/u/{{.Creator}}"><b>d</b>devf</a>
<section class="card">
  {{if .IsPhoto}}<img class="media" src="{{.Poster}}" alt="{{.Title}}">
  {{else}}<video class="media" src="{{.Source}}" {{if .Poster}}poster="{{.Poster}}"{{end}} controls playsinline loop preload="metadata"></video>{{end}}
  <div class="title">{{.Title}}</div>
  <div class="meta">{{count .Likes}} likes · {{count .Views}} views</div>
  <a class="by" href="/u/{{.Creator}}">
    {{if .CreatorAvatar}}<img src="{{.CreatorAvatar}}" alt="{{.Creator}}">{{else}}<span class="mini">{{.CreatorInitial}}</span>{{end}}
    <strong>@{{.Creator}}</strong>
  </a>
  {{if .Download}}<a class="cta" href="{{.Download}}">Get devf</a>
  {{else}}<p class="hint">Open devf to vote, like and answer it with your own video</p>{{end}}
</section>
</main></body></html>`))

var publicMissingTmpl = template.Must(template.New("m").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{.Title}} · devf</title><meta name="robots" content="noindex">
<style>` + publicStyle + `</style></head>
<body><main>
<div class="brand"><b>d</b>devf</div>
<section class="card" style="text-align:center">
  <h1>{{.Title}}</h1>
  <p class="hint">{{.Line}}</p>
  {{if .Download}}<a class="cta" href="{{.Download}}">Get devf</a>{{end}}
</section>
</main></body></html>`))
