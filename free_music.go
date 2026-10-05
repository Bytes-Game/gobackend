package main

// free_music.go — songs anybody may put in their video, for free.
//
// The video editor has a Music button. It searches every song we can find
// whose licence lets anybody put it under their own video, in an app that
// may make money, without paying. That is a few hundred thousand songs,
// found through Openverse (openverse.org) — a free search engine, run by
// WordPress, over Jamendo, Freesound and Wikimedia Commons.
//
// What is NOT here, and cannot be: film songs and chart hits. Those belong
// to record labels and music publishers, and the apps that offer them pay
// for them under private deals. That includes old Bollywood songs: a 1950s
// recording can be out of copyright in India, but the tune and the words
// stay protected until 60 years after the composer and the lyricist die.
//
// Which licences count as free here (freeMusicLicences):
//
//   - cc0: given away, no conditions;
//   - pdm: nobody owns it any more (public domain);
//   - by:  free, as long as the artist is credited.
//
// Left out on purpose: NC (non-commercial only — this app may make money),
// ND (no changes — putting a song under a video counts as a change), and
// SA (the whole video would have to be given away under the same licence).
//
// The credit: a post with a song carries its title, artist and licence
// (MusicCredit), on every screen, so the reel can show "♪ title · artist".
// For a "by" song that credit is the price of using it.
//
// How the server spends Openverse's allowance. Without an account it allows
// 200 searches a day, from the whole server — fine for testing, not for an
// app. With a free registered account (OPENVERSE_CLIENT_ID and
// OPENVERSE_CLIENT_SECRET) it allows thousands. Either way every answer is
// kept for six hours (musicCache), so the hundredth person to search
// "happy" costs nothing, and when the allowance runs out the picker still
// shows the songs people here have already used, and says search is busy.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/lib/pq"
)

// freeMusicLicences are the licences a song may have to be offered. See the
// top of this file for why these three and no others.
var freeMusicLicences = map[string]bool{"cc0": true, "pdm": true, "by": true}

const musicSourceOpenverse = "openverse"

// MusicTrack is a song the picker can offer.
type MusicTrack struct {
	// Our own id, once somebody here has used it. Empty in search results
	// for a song nobody has picked yet.
	ID       string `json:"id,omitempty"`
	Source   string `json:"source"`
	SourceID string `json:"sourceId"`
	Title    string `json:"title"`
	Artist   string `json:"artist"`
	// How long the whole song runs.
	DurationMs int `json:"durationMs"`
	// The song itself, to play in the picker and mix into the video.
	AudioURL       string `json:"audioUrl"`
	License        string `json:"license"`
	LicenseVersion string `json:"licenseVersion,omitempty"`
	LicenseURL     string `json:"licenseUrl,omitempty"`
	// The song's own page, where it came from.
	SourceURL string `json:"sourceUrl,omitempty"`
	// Who hosts it: jamendo, freesound, wikimedia_audio.
	Provider string `json:"provider,omitempty"`
	// The full credit line, as the licence asks for it.
	Attribution string   `json:"attribution,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	UseCount    int      `json:"useCount,omitempty"`
}

// MusicCredit is what a post says about its song.
type MusicCredit struct {
	ID             string `json:"id"`
	Title          string `json:"title"`
	Artist         string `json:"artist"`
	License        string `json:"license"`
	LicenseVersion string `json:"licenseVersion,omitempty"`
	LicenseURL     string `json:"licenseUrl,omitempty"`
	SourceURL      string `json:"sourceUrl,omitempty"`
	Attribution    string `json:"attribution,omitempty"`
}

// ── Openverse ──────────────────────────────────────────────────────────────

// openverseBase is where Openverse answers. A variable so tests can stand
// in for it.
var openverseBase = "https://api.openverse.org"

var openverseHTTP = &http.Client{Timeout: 10 * time.Second}

// errMusicBusy is Openverse saying the day's (or the minute's) searches are
// used up.
var errMusicBusy = errors.New("the music search allowance is used up for now")

// musicPageSize is how many songs one page of results holds — the most
// Openverse gives a visitor without an account.
const musicPageSize = 20

// openverseAudio is one result, as Openverse sends it.
type openverseAudio struct {
	ID                string   `json:"id"`
	Title             string   `json:"title"`
	Creator           string   `json:"creator"`
	License           string   `json:"license"`
	LicenseVersion    string   `json:"license_version"`
	LicenseURL        string   `json:"license_url"`
	Provider          string   `json:"provider"`
	Source            string   `json:"source"`
	Category          string   `json:"category"`
	Genres            []string `json:"genres"`
	Duration          int      `json:"duration"`
	URL               string   `json:"url"`
	ForeignLandingURL string   `json:"foreign_landing_url"`
	Attribution       string   `json:"attribution"`
	Mature            bool     `json:"mature"`
}

// trackFromOpenverse turns one result into a song the picker can offer, or
// says why it cannot be: a licence that is not free enough, a song marked
// for adults, or nothing to play.
func trackFromOpenverse(a openverseAudio) (MusicTrack, string) {
	lic := strings.ToLower(strings.TrimSpace(a.License))
	switch {
	case !freeMusicLicences[lic]:
		return MusicTrack{}, "licence " + a.License
	case a.Mature:
		return MusicTrack{}, "marked for adults"
	case strings.TrimSpace(a.URL) == "":
		return MusicTrack{}, "no file to play"
	case strings.TrimSpace(a.ID) == "":
		return MusicTrack{}, "no id"
	}
	title := strings.TrimSpace(a.Title)
	if title == "" {
		title = "Untitled"
	}
	artist := strings.TrimSpace(a.Creator)
	if artist == "" {
		artist = "Unknown artist"
	}
	return MusicTrack{
		Source:         musicSourceOpenverse,
		SourceID:       a.ID,
		Title:          clipRunes(title, 200),
		Artist:         clipRunes(artist, 200),
		DurationMs:     a.Duration,
		AudioURL:       a.URL,
		License:        lic,
		LicenseVersion: a.LicenseVersion,
		LicenseURL:     a.LicenseURL,
		SourceURL:      a.ForeignLandingURL,
		Provider:       a.Source,
		Attribution:    a.Attribution,
		Genres:         a.Genres,
	}, ""
}

func clipRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n])
}

// openverseLogin holds the registered account's pass, while it lasts.
var openverseLogin struct {
	sync.Mutex
	token string
	until time.Time
	// After a failed login, when to try again — not on every search.
	retryAt time.Time
}

// openverseToken is the registered account's pass, or "" to search as a
// visitor: when no account is set up, or logging in failed (which is said
// once, with what that costs).
func openverseToken(ctx context.Context) string {
	id := strings.TrimSpace(os.Getenv("OPENVERSE_CLIENT_ID"))
	secret := strings.TrimSpace(os.Getenv("OPENVERSE_CLIENT_SECRET"))
	if id == "" || secret == "" {
		return ""
	}
	openverseLogin.Lock()
	defer openverseLogin.Unlock()
	now := time.Now()
	if openverseLogin.token != "" && now.Before(openverseLogin.until) {
		return openverseLogin.token
	}
	if now.Before(openverseLogin.retryAt) {
		return ""
	}
	token, lasts, err := openverseLogIn(ctx, id, secret)
	if err != nil {
		openverseLogin.retryAt = now.Add(10 * time.Minute)
		log.Printf("[music] could not log in to Openverse: %v — searching as a "+
			"visitor (200 searches a day for the whole server) and trying "+
			"again in 10 minutes", err)
		return ""
	}
	openverseLogin.token = token
	// A minute early, so a pass never expires halfway through a search.
	openverseLogin.until = now.Add(lasts - time.Minute)
	return token
}

func openverseLogIn(ctx context.Context, id, secret string) (string, time.Duration, error) {
	form := url.Values{
		"client_id":     {id},
		"client_secret": {secret},
		"grant_type":    {"client_credentials"},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		openverseBase+"/v1/auth_tokens/token/", strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := openverseHTTP.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		return "", 0, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", 0, fmt.Errorf("unreadable answer: %w", err)
	}
	if out.AccessToken == "" {
		return "", 0, errors.New("no pass in the answer")
	}
	lasts := time.Duration(out.ExpiresIn) * time.Second
	if lasts < 2*time.Minute {
		lasts = 2 * time.Minute
	}
	return out.AccessToken, lasts, nil
}

// openverseGet asks Openverse for [path] and decodes its answer into [out].
func openverseGet(ctx context.Context, path string, query url.Values, out any) error {
	u := openverseBase + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	if tok := openverseToken(ctx); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := openverseHTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusTooManyRequests:
		return errMusicBusy
	case http.StatusUnauthorized:
		// The pass stopped working early: log in again next time.
		openverseLogin.Lock()
		openverseLogin.token = ""
		openverseLogin.Unlock()
		return errors.New("Openverse refused the login pass")
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		return fmt.Errorf("Openverse answered %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("unreadable answer from Openverse: %w", err)
	}
	return nil
}

// musicPage is one page of search results.
type musicPage struct {
	Tracks  []MusicTrack
	HasMore bool
}

// openverseSearch is one page of free songs matching [q]; every free song,
// most relevant first, for an empty [q].
func openverseSearch(ctx context.Context, q string, page int) (musicPage, error) {
	query := url.Values{
		"license":   {"by,cc0,pdm"},
		"category":  {"music"},
		"mature":    {"false"},
		"page":      {strconv.Itoa(page)},
		"page_size": {strconv.Itoa(musicPageSize)},
	}
	if q != "" {
		query.Set("q", q)
	}
	var out struct {
		PageCount int              `json:"page_count"`
		Results   []openverseAudio `json:"results"`
	}
	if err := openverseGet(ctx, "/v1/audio/", query, &out); err != nil {
		return musicPage{}, err
	}
	tracks := make([]MusicTrack, 0, len(out.Results))
	refused := map[string]int{}
	for _, a := range out.Results {
		t, why := trackFromOpenverse(a)
		if why != "" {
			refused[why]++
			continue
		}
		tracks = append(tracks, t)
	}
	if len(refused) > 0 {
		// Openverse was asked for these licences only, so this means its
		// filter and its records disagree. Worth knowing; never offered.
		log.Printf("[music] search %q page %d: left out %v", q, page, refused)
	}
	return musicPage{Tracks: tracks, HasMore: page < out.PageCount}, nil
}

// openverseTrack is one song, read fresh from Openverse.
func openverseTrack(ctx context.Context, id string) (MusicTrack, error) {
	var a openverseAudio
	if err := openverseGet(ctx, "/v1/audio/"+url.PathEscape(id)+"/", nil, &a); err != nil {
		return MusicTrack{}, err
	}
	t, why := trackFromOpenverse(a)
	if why != "" {
		return MusicTrack{}, fmt.Errorf("%w (%s)", errMusicNotFree, why)
	}
	return t, nil
}

var errMusicNotFree = errors.New("this song is not free to use")

// ── The six-hour memory of searches ────────────────────────────────────────

type musicCacheEntry struct {
	at   time.Time
	page musicPage
}

var musicCache = struct {
	sync.Mutex
	m map[string]musicCacheEntry
}{m: map[string]musicCacheEntry{}}

const (
	musicCacheFor = 6 * time.Hour
	musicCacheMax = 2000
)

// searchFreeMusic is [openverseSearch], remembered. When Openverse cannot
// answer, a remembered answer is used however old it is, and the second
// result says so.
func searchFreeMusic(ctx context.Context, q string, page int) (musicPage, bool, error) {
	key := strings.ToLower(q) + "|" + strconv.Itoa(page)
	musicCache.Lock()
	hit, ok := musicCache.m[key]
	musicCache.Unlock()
	if ok && time.Since(hit.at) < musicCacheFor {
		return hit.page, false, nil
	}
	fresh, err := openverseSearch(ctx, q, page)
	if err != nil {
		if ok {
			log.Printf("[music] search %q page %d: %v — showing what it found "+
				"%s ago", q, page, err, time.Since(hit.at).Round(time.Minute))
			return hit.page, true, nil
		}
		return musicPage{}, false, err
	}
	musicCache.Lock()
	if len(musicCache.m) >= musicCacheMax {
		var oldest string
		var at time.Time
		for k, e := range musicCache.m {
			if oldest == "" || e.at.Before(at) {
				oldest, at = k, e.at
			}
		}
		delete(musicCache.m, oldest)
	}
	musicCache.m[key] = musicCacheEntry{at: time.Now(), page: fresh}
	musicCache.Unlock()
	return fresh, false, nil
}

// ── Songs people here have used ────────────────────────────────────────────

const musicTrackColumns = `id, source, source_id, title, artist, duration_ms, audio_url,
	license, license_version, license_url, source_url, provider, attribution,
	genres, use_count`

func scanMusicTrack(scan func(dest ...any) error) (MusicTrack, error) {
	var t MusicTrack
	var id int64
	err := scan(&id, &t.Source, &t.SourceID, &t.Title, &t.Artist, &t.DurationMs,
		&t.AudioURL, &t.License, &t.LicenseVersion, &t.LicenseURL, &t.SourceURL,
		&t.Provider, &t.Attribution, pq.Array(&t.Genres), &t.UseCount)
	if err != nil {
		return MusicTrack{}, err
	}
	t.ID = strconv.FormatInt(id, 10)
	return t, nil
}

// popularMusic is the songs most used here, most used first.
func popularMusic(ctx context.Context, limit int) []MusicTrack {
	rows, err := db.QueryContext(ctx, `
		SELECT `+musicTrackColumns+`
		  FROM music_tracks
		 WHERE NOT blocked AND use_count > 0
		 ORDER BY use_count DESC, id DESC
		 LIMIT $1`, limit)
	if queryFailed("reading the most used songs",
		"the picker opens on search results alone", err) {
		return nil
	}
	defer rows.Close()
	var out []MusicTrack
	bad := 0
	for rows.Next() {
		t, err := scanMusicTrack(rows.Scan)
		if scanFailed("a used song", err, &bad) {
			continue
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		queryFailed("reading the most used songs", "the list is cut short", err)
	}
	return out
}

// withoutBlocked drops the songs taken down here (blocked) from [tracks].
func withoutBlocked(ctx context.Context, tracks []MusicTrack) []MusicTrack {
	if len(tracks) == 0 {
		return tracks
	}
	ids := make([]string, 0, len(tracks))
	for _, t := range tracks {
		ids = append(ids, t.SourceID)
	}
	rows, err := db.QueryContext(ctx, `
		SELECT source_id FROM music_tracks
		 WHERE source = $1 AND blocked AND source_id = ANY($2)`,
		musicSourceOpenverse, pq.Array(ids))
	if queryFailed("checking search results against the songs taken down",
		"none are left out, so a song taken down could be offered", err) {
		return tracks
	}
	defer rows.Close()
	blocked := map[string]bool{}
	bad := 0
	for rows.Next() {
		var id string
		if scanFailed("a taken-down song", rows.Scan(&id), &bad) {
			continue
		}
		blocked[id] = true
	}
	if err := rows.Err(); err != nil {
		queryFailed("checking search results against the songs taken down",
			"some may be left in", err)
	}
	if len(blocked) == 0 {
		return tracks
	}
	out := tracks[:0:0]
	for _, t := range tracks {
		if !blocked[t.SourceID] {
			out = append(out, t)
		}
	}
	return out
}

// recordMusicTrack keeps [t] (read fresh from Openverse) and answers it
// with our own id. A song taken down here is refused.
func recordMusicTrack(ctx context.Context, t MusicTrack) (MusicTrack, error) {
	row := db.QueryRowContext(ctx, `
		INSERT INTO music_tracks
			(source, source_id, title, artist, duration_ms, audio_url, license,
			 license_version, license_url, source_url, provider, attribution, genres)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (source, source_id) DO UPDATE SET
			title = EXCLUDED.title, artist = EXCLUDED.artist,
			duration_ms = EXCLUDED.duration_ms, audio_url = EXCLUDED.audio_url,
			license = EXCLUDED.license, license_version = EXCLUDED.license_version,
			license_url = EXCLUDED.license_url, source_url = EXCLUDED.source_url,
			provider = EXCLUDED.provider, attribution = EXCLUDED.attribution,
			genres = EXCLUDED.genres
		RETURNING `+musicTrackColumns+`, blocked`,
		t.Source, t.SourceID, t.Title, t.Artist, t.DurationMs, t.AudioURL, t.License,
		t.LicenseVersion, t.LicenseURL, t.SourceURL, t.Provider, t.Attribution,
		pq.Array(nonNilStrings(t.Genres)))
	var blocked bool
	saved, err := scanMusicTrack(func(dest ...any) error {
		return row.Scan(append(dest, &blocked)...)
	})
	if err != nil {
		return MusicTrack{}, err
	}
	if blocked {
		return MusicTrack{}, errMusicBlocked
	}
	return saved, nil
}

var errMusicBlocked = errors.New("this song has been taken down here")

func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// musicForPost checks the song a new post or answer says it uses: [raw]
// "" is none. It answers the id to store (nil for none), or why the post
// cannot have it.
func musicForPost(raw string) (*int64, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "0" {
		return nil, ""
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		return nil, "musicTrackId must be a song id from the music library"
	}
	var blocked bool
	err = db.QueryRow(`SELECT blocked FROM music_tracks WHERE id = $1`, id).Scan(&blocked)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, "that song is not in the music library — pick it again"
	}
	if queryFailed("checking a post's song", "the post is refused rather "+
		"than posted without its credit", err) {
		return nil, "could not check the song — try again"
	}
	if blocked {
		return nil, "that song has been taken down — pick another"
	}
	return &id, ""
}

// countMusicUse adds one to how many posts use song [id].
func countMusicUse(id *int64) {
	if id == nil {
		return
	}
	if _, err := db.Exec(`UPDATE music_tracks SET use_count = use_count + 1 WHERE id = $1`, *id); err != nil {
		queryFailed("counting a song's use", "it ranks lower among popular songs than it should", err)
	}
}

// ── The credit on every post ───────────────────────────────────────────────

// fillMusic puts the song credit on every post in [chs] that has one, and
// on its top answer. Every list sent to the app comes through
// markViewerState, which calls this.
func fillMusic(chs []*Challenge) {
	if db == nil || len(chs) == 0 {
		return
	}
	posts := map[int64][]*Challenge{}
	answers := map[int64][]*Challenge{}
	for _, c := range chs {
		if c == nil {
			continue
		}
		if id, err := strconv.ParseInt(c.ID, 10, 64); err == nil {
			posts[id] = append(posts[id], c)
		}
		if c.TopResponseID != "" {
			if id, err := strconv.ParseInt(c.TopResponseID, 10, 64); err == nil {
				answers[id] = append(answers[id], c)
			}
		}
	}
	if len(posts) > 0 {
		for id, credit := range musicCredits("challenges", idsOf(posts)) {
			for _, c := range posts[id] {
				c.Music = credit
			}
		}
	}
	if len(answers) > 0 {
		for id, credit := range musicCredits("challenge_responses", idsOf(answers)) {
			for _, c := range answers[id] {
				c.TopResponseMusic = credit
			}
		}
	}
}

// fillResponseMusic puts the song credit on each answer in [resps].
func fillResponseMusic(resps []ChallengeResponse) {
	if db == nil || len(resps) == 0 {
		return
	}
	byID := map[int64][]int{}
	for i, r := range resps {
		if id, err := strconv.ParseInt(r.ID, 10, 64); err == nil {
			byID[id] = append(byID[id], i)
		}
	}
	for id, credit := range musicCredits("challenge_responses", idsOf(byID)) {
		for _, i := range byID[id] {
			resps[i].Music = credit
		}
	}
}

func idsOf[V any](m map[int64]V) []int64 {
	out := make([]int64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// musicCredits reads the song credit for each of [ids] in [table]
// ("challenges" or "challenge_responses") that has a song.
func musicCredits(table string, ids []int64) map[int64]*MusicCredit {
	var q string
	switch table {
	case "challenges":
		q = `SELECT p.id, m.id, m.title, m.artist, m.license, m.license_version,
		            m.license_url, m.source_url, m.attribution
		       FROM challenges p JOIN music_tracks m ON m.id = p.music_track_id
		      WHERE p.id = ANY($1::int[])`
	case "challenge_responses":
		q = `SELECT p.id, m.id, m.title, m.artist, m.license, m.license_version,
		            m.license_url, m.source_url, m.attribution
		       FROM challenge_responses p JOIN music_tracks m ON m.id = p.music_track_id
		      WHERE p.id = ANY($1::int[])`
	default:
		return nil
	}
	rows, err := db.Query(q, pq.Array(ids))
	if queryFailed("reading the songs posts use",
		"they go out with no music credit, though the song plays", err) {
		return nil
	}
	defer rows.Close()
	out := map[int64]*MusicCredit{}
	bad := 0
	for rows.Next() {
		var postID, trackID int64
		var c MusicCredit
		if scanFailed("a post's song", rows.Scan(&postID, &trackID, &c.Title, &c.Artist,
			&c.License, &c.LicenseVersion, &c.LicenseURL, &c.SourceURL, &c.Attribution), &bad) {
			continue
		}
		c.ID = strconv.FormatInt(trackID, 10)
		out[postID] = &c
	}
	if err := rows.Err(); err != nil {
		queryFailed("reading the songs posts use", "some go out with no credit", err)
	}
	return out
}

// ── HTTP ───────────────────────────────────────────────────────────────────

// MusicSearchHandler is the picker's list.
//
//	GET /api/v1/music/search?q=happy&page=1
//
// With no q, the first page also carries the songs most used here
// ("popular"). "busy" means Openverse would not answer: what is shown is
// what was found earlier, or only the popular songs.
func MusicSearchHandler(w http.ResponseWriter, r *http.Request) {
	userID := authUserID(r)
	if !allowAction(userID, "music_search") {
		writeRateLimited(w, "music_search")
		return
	}
	q := clipRunes(strings.TrimSpace(r.URL.Query().Get("q")), 100)
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	if page > 50 {
		page = 50
	}
	ctx := r.Context()
	resp := struct {
		Popular []MusicTrack `json:"popular,omitempty"`
		Tracks  []MusicTrack `json:"tracks"`
		Page    int          `json:"page"`
		HasMore bool         `json:"hasMore"`
		Busy    bool         `json:"busy,omitempty"`
	}{Tracks: []MusicTrack{}, Page: page}
	if q == "" && page == 1 {
		resp.Popular = popularMusic(ctx, musicPageSize)
	}
	found, stale, err := searchFreeMusic(ctx, q, page)
	switch {
	case err != nil:
		log.Printf("[music] search %q page %d for user %s: %v — answering with "+
			"the popular songs only", q, page, userID, err)
		resp.Busy = true
	default:
		resp.Tracks = withoutBlocked(ctx, found.Tracks)
		resp.HasMore = found.HasMore
		resp.Busy = stale
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// PickMusicHandler records the song somebody chose for their video, after
// reading it fresh from Openverse — so a post never credits a song, or a
// licence, the app made up.
//
//	POST /api/v1/music/tracks  {"sourceId": "<openverse id>"}
func PickMusicHandler(w http.ResponseWriter, r *http.Request) {
	userID := authUserID(r)
	if !allowAction(userID, "music_pick") {
		writeRateLimited(w, "music_pick")
		return
	}
	var body struct {
		SourceID string `json:"sourceId"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&body); err != nil ||
		strings.TrimSpace(body.SourceID) == "" {
		http.Error(w, "sourceId is required", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	t, err := openverseTrack(ctx, strings.TrimSpace(body.SourceID))
	if err != nil {
		switch {
		case errors.Is(err, errMusicNotFree):
			log.Printf("[music] user %s picked %s: %v", userID, body.SourceID, err)
			http.Error(w, "that song is not free to use — pick another", http.StatusUnprocessableEntity)
		case errors.Is(err, errMusicBusy):
			log.Printf("[music] user %s picked %s: %v", userID, body.SourceID, err)
			http.Error(w, "the music library is busy — try again in a minute", http.StatusServiceUnavailable)
		default:
			log.Printf("[music] user %s picked %s: could not read it from "+
				"Openverse: %v", userID, body.SourceID, err)
			http.Error(w, "could not reach the music library — try again", http.StatusBadGateway)
		}
		return
	}
	saved, err := recordMusicTrack(ctx, t)
	if err != nil {
		if errors.Is(err, errMusicBlocked) {
			http.Error(w, "that song has been taken down — pick another", http.StatusUnprocessableEntity)
			return
		}
		queryFailed("keeping a picked song", "the person is asked to pick it again", err)
		http.Error(w, "could not keep that song — try again", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(saved)
}

// AdminBlockMusicHandler takes a song down: it stops being offered and
// cannot be added to new posts. For a song that turns out not to be what
// its licence claims.
//
//	POST /api/v1/admin/music/block  {"sourceId": "<openverse id>", "blocked": true}
func AdminBlockMusicHandler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SourceID string `json:"sourceId"`
		Blocked  *bool  `json:"blocked"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&body); err != nil ||
		strings.TrimSpace(body.SourceID) == "" {
		http.Error(w, "sourceId is required", http.StatusBadRequest)
		return
	}
	blocked := true
	if body.Blocked != nil {
		blocked = *body.Blocked
	}
	// A song nobody has used yet is kept too, as taken down, so it is left
	// out of searches from now on.
	_, err := db.Exec(`
		INSERT INTO music_tracks (source, source_id, license, blocked)
		VALUES ($1, $2, '', $3)
		ON CONFLICT (source, source_id) DO UPDATE SET blocked = EXCLUDED.blocked`,
		musicSourceOpenverse, strings.TrimSpace(body.SourceID), blocked)
	if queryFailed("taking a song down", "it is still offered", err) {
		http.Error(w, "could not save that", http.StatusInternalServerError)
		return
	}
	log.Printf("[music] song %s blocked=%v by an admin", body.SourceID, blocked)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"sourceId": body.SourceID, "blocked": blocked})
}
