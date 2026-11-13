package main

// Which country a request is from, which country an account and a video
// are in, the ages that go with a country, and the small lift the feed
// gives a video from the viewer's own country.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestCountry_OnlyRealCountryCodesCount(t *testing.T) {
	for raw, want := range map[string]string{
		"IN": "IN", "in": "IN", " gb ": "GB", "Us": "US",
		"": "", "XX": "", "ZZ": "", "T1": "", "IND": "", "I": "", "1N": "", "I N": "", "ÎN": "",
	} {
		if got := cleanCountry(raw); got != want {
			t.Errorf("cleanCountry(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestCountry_TheNetworkIsBelievedBeforeThePhone(t *testing.T) {
	ask := func(headers map[string]string) (string, string) {
		r := httptest.NewRequest("GET", "/", nil)
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		return countryOf(r)
	}
	for name, c := range map[string]struct {
		headers     map[string]string
		want, where string
	}{
		"both say":                     {map[string]string{"CF-IPCountry": "in", "X-Device-Country": "US"}, "IN", "network"},
		"only the phone":               {map[string]string{"X-Device-Country": "gb"}, "GB", "device"},
		"the network does not know":    {map[string]string{"CF-IPCountry": "XX", "X-Device-Country": "AU"}, "AU", "device"},
		"the network says Tor":         {map[string]string{"CF-IPCountry": "T1"}, "", ""},
		"nobody says":                  {nil, "", ""},
		"another host's header":        {map[string]string{"Fly-Client-Country": "DE"}, "DE", "network"},
		"Cloudflare before the others": {map[string]string{"CF-IPCountry": "IN", "X-Vercel-IP-Country": "US"}, "IN", "network"},
		"the phone sends rubbish":      {map[string]string{"X-Device-Country": "India"}, "", ""},
	} {
		if got, where := ask(c.headers); got != c.want || where != c.where {
			t.Errorf("%s: %q from %q, want %q from %q", name, got, where, c.want, c.where)
		}
	}
}

// No country's rule may be weaker than the rule everywhere else: the list
// exists only to be stricter. A slip in a new line would let children in
// where the law says no.
func TestAgeRules_NoCountryIsWeakerThanTheUsualRule(t *testing.T) {
	if defaultAgeRule.MinAge != 13 || defaultAgeRule.ConsentAge != 18 {
		t.Fatalf("the usual rule is %+v, want 13 to join and a parent under 18", defaultAgeRule)
	}
	for country, rule := range countryAgeRules {
		if cleanCountry(country) != country {
			t.Errorf("%q is not a country code", country)
		}
		if rule.MinAge < defaultAgeRule.MinAge || rule.ConsentAge < defaultAgeRule.ConsentAge {
			t.Errorf("%s: %+v is weaker than the usual rule %+v", country, rule, defaultAgeRule)
		}
		if rule.ConsentAge < rule.MinAge {
			t.Errorf("%s: a parent is asked under %d, but nobody under %d can join", country, rule.ConsentAge, rule.MinAge)
		}
	}
	for _, sixteen := range []string{"AU", "ID", "MY", "BR"} {
		if got := ageRuleFor(sixteen).MinAge; got != 16 {
			t.Errorf("%s: the youngest who can join is %d, want 16", sixteen, got)
		}
	}
	if got := ageRuleFor(""); got != defaultAgeRule {
		t.Errorf("a country nobody knows gets %+v, want the usual rule", got)
	}
	if got := ageRuleFor("IN"); got != defaultAgeRule {
		t.Errorf("India gets %+v, want the usual rule", got)
	}
}

// GET /signup/rules: the ages for where the caller is, and the country
// this server sees for them.
func TestSignupRules_SayTheAgesForWhereYouAre(t *testing.T) {
	ask := func(headers map[string]string) (map[string]any, http.Header) {
		r := httptest.NewRequest("GET", "/signup/rules", nil)
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		SignupRulesHandler(w, r)
		if w.Code != http.StatusOK {
			t.Fatalf("answered %d", w.Code)
		}
		var got map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		return got, w.Header()
	}
	got, headers := ask(map[string]string{"CF-IPCountry": "AU"})
	if got["country"] != "AU" || got["source"] != "network" || got["minAge"] != float64(16) || got["parentConsentAge"] != float64(18) {
		t.Errorf("from Australia: %v", got)
	}
	if headers.Get("Cache-Control") != "no-store" {
		t.Error("the answer may be kept and handed to somebody in another country")
	}
	got, _ = ask(map[string]string{"X-Device-Country": "IN"})
	if got["country"] != "IN" || got["source"] != "device" || got["minAge"] != float64(13) || got["parentConsentAge"] != float64(18) {
		t.Errorf("from a phone set to India: %v", got)
	}
	got, _ = ask(nil)
	if got["country"] != "" || got["source"] != "" || got["minAge"] != float64(13) || got["parentConsentAge"] != float64(18) {
		t.Errorf("from nowhere known: %v", got)
	}
}

func TestCountry_TheFeedsLiftIsSmallAndOnlyForTheSameCountry(t *testing.T) {
	for name, c := range map[string]struct {
		viewer, video string
		want          float64
	}{
		"the same country":        {"IN", "IN", sameCountryBonus},
		"another country":         {"IN", "US", 0},
		"the viewer's is unknown": {"", "IN", 0},
		"the video's is unknown":  {"IN", "", 0},
		"neither is known":        {"", "", 0},
	} {
		if got := countryBonus(c.viewer, c.video); got != c.want {
			t.Errorf("%s: %v, want %v", name, got, c.want)
		}
	}
	// No bigger than the feed's other bounded bonuses: local videos come
	// up among equals, and a bad one never beats a good one from abroad.
	if sameCountryBonus <= 0 || sameCountryBonus > 0.10 {
		t.Errorf("the lift is %v, want more than nothing and at most 0.10", sameCountryBonus)
	}

	// Added to the score, and noted. A creator the viewer blocked stays
	// at exactly nothing, wherever the video is from.
	breakdown := map[string]float64{"negativeMult": 1}
	if got := addCountryBonus(1.0, breakdown, "IN", "IN"); got != 1.0+sameCountryBonus || breakdown["countryBonus"] != sameCountryBonus {
		t.Errorf("a local video: score %v, noted %v", got, breakdown["countryBonus"])
	}
	blocked := map[string]float64{"negativeMult": 0}
	if got := addCountryBonus(0, blocked, "IN", "IN"); got != 0 {
		t.Errorf("a blocked creator's local video scores %v, want exactly 0", got)
	}
	half := map[string]float64{"negativeMult": 0.5}
	if got := addCountryBonus(0.4, half, "IN", "IN"); got != 0.4+sameCountryBonus/2 {
		t.Errorf("an unfollowed creator's local video scores %v, want half the lift", got)
	}
	elsewhere := map[string]float64{"negativeMult": 1}
	if got := addCountryBonus(1.0, elsewhere, "IN", "US"); got != 1.0 {
		t.Errorf("a video from elsewhere scores %v, want it left alone", got)
	}
	if _, noted := elsewhere["countryBonus"]; noted {
		t.Error("a lift of nothing is noted as one")
	}
}

// waitFor polls until [ok], for things written off the response path.
func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s: still not so after 15 seconds", what)
}

func countryOfAccount(t *testing.T, id int) string {
	t.Helper()
	var c string
	if err := db.QueryRow(`SELECT country FROM users WHERE id = $1`, id).Scan(&c); err != nil {
		t.Fatal(err)
	}
	return c
}

// Every signed-in request says where the person is, and the account keeps
// it: through the real sign-in check, not by calling the far end by hand.
func TestCountry_ASignedInRequestKeepsTheAccountsCountry(t *testing.T) {
	defer withDB(t)()
	const id = 9801
	mustExec(t, `INSERT INTO users (id, username, password) VALUES ($1, 'traveller', 'x')`, id)
	if got := countryOfAccount(t, id); got != "" {
		t.Fatalf("a new account starts in %q, want nowhere known", got)
	}

	reached := 0
	h := authed(func(w http.ResponseWriter, r *http.Request) { reached++ })
	call := func(headers map[string]string) {
		r := httptest.NewRequest("GET", "/api/v1/profile", nil)
		r.Header.Set("Authorization", "Bearer "+testToken(t, strconv.Itoa(id), "traveller"))
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		h(httptest.NewRecorder(), r)
	}

	call(map[string]string{"CF-IPCountry": "IN"})
	if reached != 1 {
		t.Fatal("the request did not get through the sign-in check")
	}
	waitFor(t, "the account's country is saved as IN", func() bool { return countryOfAccount(t, id) == "IN" })
	if got := viewerCountry(strconv.Itoa(id)); got != "IN" {
		t.Errorf("the feed is told the viewer is in %q", got)
	}

	// They travel: the next request says so.
	call(map[string]string{"CF-IPCountry": "AE"})
	waitFor(t, "the account's country moves to AE", func() bool { return countryOfAccount(t, id) == "AE" })

	// A request that says nothing changes nothing.
	call(nil)
	time.Sleep(100 * time.Millisecond)
	if got := countryOfAccount(t, id); got != "AE" {
		t.Errorf("a request with no country moved the account to %q", got)
	}

	// After a restart nothing is remembered: the saved country is read.
	forgetCountries()
	if got := viewerCountry(strconv.Itoa(id)); got != "AE" {
		t.Errorf("after a restart the feed is told %q, want what was saved", got)
	}
	// Somebody who is not signed in is nobody's country.
	if got := viewerCountry(""); got != "" {
		t.Errorf("nobody is in %q", got)
	}
}

// A new video keeps the country it was posted from, through the real
// handler; and its score record says so. A video with none follows its
// creator.
func TestCountry_AVideoKnowsWhereItWasPostedFrom(t *testing.T) {
	defer withDB(t)()
	resetActionLimiters(t)
	const maker = 9811
	mustExec(t, `INSERT INTO users (id, username, password, country) VALUES ($1, 'maker_au', 'x', 'AU')`, maker)

	post := func(headers map[string]string) string {
		body, _ := json.Marshal(map[string]any{
			"videoUrl": testUpload(maker, "720p.mp4"), "prefix": "who can", "subject": "juggle",
			"visibility": "public",
		})
		r := withAuth(httptest.NewRequest("POST", "/api/v1/challenges", bytes.NewReader(body)),
			strconv.Itoa(maker), "maker_au")
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		CreateChallengeHandler(w, r)
		var c Challenge
		_ = json.Unmarshal(w.Body.Bytes(), &c)
		if w.Code >= 300 || c.ID == "" {
			t.Fatalf("posting answered %d %s", w.Code, w.Body.String())
		}
		return c.ID
	}
	stamped := func(id string) string {
		var c string
		if err := db.QueryRow(`SELECT country FROM challenges WHERE id::text = $1`, id).Scan(&c); err != nil {
			t.Fatal(err)
		}
		return c
	}

	// Posted while in India, by somebody whose account is in Australia.
	abroad := post(map[string]string{"CF-IPCountry": "IN"})
	waitFor(t, "the video is stamped IN", func() bool { return stamped(abroad) == "IN" })
	// Posted with nobody saying where from.
	home := post(nil)
	time.Sleep(100 * time.Millisecond)
	if got := stamped(home); got != "" {
		t.Fatalf("a video posted from nowhere known is stamped %q", got)
	}

	if got := computeContentScore(abroad, "challenge").Country; got != "IN" {
		t.Errorf("the score record of a video posted from India says %q", got)
	}
	if got := computeContentScore(home, "challenge").Country; got != "AU" {
		t.Errorf("a video with no country of its own says %q, want its creator's (AU)", got)
	}
	// The rest of the record is still read: one extra column must not
	// cost the others (a scan fails for the whole row).
	if cs := computeContentScore(abroad, "challenge"); cs.CreatorID != strconv.Itoa(maker) || cs.CreatedAt.IsZero() {
		t.Errorf("the score record lost its creator or its date: %+v", cs)
	}

	got := videoCountries([]string{abroad, home, "999999999"})
	if got[abroad] != "IN" || got[home] != "AU" || len(got) != 2 {
		t.Errorf("the countries of a page of videos: %v", got)
	}
	// A creator with no country: the video is left out, not guessed.
	mustExec(t, `UPDATE users SET country = '' WHERE id = $1`, maker)
	if got := videoCountries([]string{home}); len(got) != 0 {
		t.Errorf("a video nobody knows the country of: %v", got)
	}
}

// Somebody new is shown their own country's videos a little sooner, and a
// much better video from elsewhere still comes first.
func TestCountry_ANewViewersFirstPagePrefersTheirOwnCountry(t *testing.T) {
	defer withDB(t)()
	const viewer, makerIN, makerUS = 9821, 9822, 9823
	mustExec(t, `INSERT INTO users (id, username, password, country) VALUES
		($1, 'new_in', 'x', 'IN'), ($2, 'maker_in', 'x', 'IN'), ($3, 'maker_us', 'x', 'US')`,
		viewer, makerIN, makerUS)
	video := func(creator int, subject string) string {
		return auditChallenge(t, map[string]any{"creator": creator, "subject": subject})
	}
	item := func(id string, views, likes int) HomeFeedItem {
		return HomeFeedItem{Type: "challenge", Challenge: &Challenge{
			ID: id, Category: "comedy", Views: views, Likes: likes,
			CreatedAt: time.Now().Add(-24 * time.Hour).Format(time.RFC3339),
		}}
	}
	usA, inA, usB := video(makerUS, "a"), video(makerIN, "b"), video(makerUS, "c")
	// The ids of a page, in order; and every video is still on it.
	order := func(out []HomeFeedItem) []string {
		t.Helper()
		if len(out) != 3 {
			t.Fatalf("%d videos came back of 3: none may be dropped, wherever it is from", len(out))
		}
		return []string{getItemID(out[0]), getItemID(out[1]), getItemID(out[2])}
	}

	// Equal in every way but the country: the local one moves to the front.
	out := order(rankColdStartItems(strconv.Itoa(viewer), []HomeFeedItem{
		item(usA, 100, 5), item(inA, 100, 5), item(usB, 100, 5),
	}))
	if out[0] != inA {
		t.Fatalf("the first video for somebody in India is %s, want the one from India (%s)", out[0], inA)
	}
	// The same page for somebody in the United States: theirs lead.
	const viewerUS = 9824
	mustExec(t, `INSERT INTO users (id, username, password, country) VALUES ($1, 'new_us', 'x', 'US')`, viewerUS)
	out = order(rankColdStartItems(strconv.Itoa(viewerUS), []HomeFeedItem{
		item(inA, 100, 5), item(usA, 100, 5), item(usB, 100, 5),
	}))
	if out[2] != inA {
		t.Fatalf("for somebody in the US the video from India is not last of three equals: %v", out)
	}
	// Somebody whose country nobody knows: the order they came in.
	out = order(rankColdStartItems("9829", []HomeFeedItem{item(usA, 100, 5), item(inA, 100, 5), item(usB, 100, 5)}))
	if out[0] != usA {
		t.Fatalf("with no country known the first video is %s, want the order unchanged (%s)", out[0], usA)
	}

	// A much better video from elsewhere still beats a poor local one.
	out = order(rankColdStartItems(strconv.Itoa(viewer), []HomeFeedItem{
		item(inA, 1000, 1), item(usA, 1000, 600), item(usB, 1000, 1),
	}))
	if out[0] != usA {
		t.Fatalf("a far better video from abroad lost to a poor local one: first is %s", out[0])
	}
}

// The main feed, through the real handler: a video from the viewer's own
// country carries the lift, one from elsewhere does not, and both are
// there.
func TestCountry_TheMainFeedGivesLocalVideosTheLift(t *testing.T) {
	defer withDB(t)()
	resetRedis(t)
	const viewer, makerIN, makerUS = 9831, 9832, 9833
	mustExec(t, `INSERT INTO users (id, username, password, country) VALUES
		($1, 'viewer_in', 'x', ''), ($2, 'maker_in2', 'x', 'IN'), ($3, 'maker_us2', 'x', 'US')`,
		viewer, makerIN, makerUS)
	// Videos anybody may be shown, as the app makes them.
	video := func(creator int, subject string) string {
		return strconv.Itoa(mustID(t, `
			INSERT INTO challenges (creator_id, video_url, prefix, subject)
			VALUES ($1, $2, 'who can', $3) RETURNING id`,
			creator, fmt.Sprintf("https://v/c%d-%s.mp4", creator, strings.ReplaceAll(subject, " ", "")), subject))
	}
	local := map[string]bool{}
	for i := 0; i < 4; i++ {
		local[video(makerIN, fmt.Sprintf("local %d", i))] = true
		video(makerUS, fmt.Sprintf("abroad %d", i))
	}
	// Enough history to be ranked as a regular viewer: somebody brand new
	// goes down the other path (the test above).
	for i := 0; i < coldStartThreshold+1; i++ {
		mustExec(t, `INSERT INTO feed_events (user_id, content_id, content_type, event_type, created_at)
			VALUES ($1, 'x', 'challenge', 'view', NOW())`, viewer)
	}

	// The request itself says where the viewer is, through the sign-in
	// check: nothing is set by hand.
	r := httptest.NewRequest("GET", "/api/v1/feed/smart?page=1&limit=8&debug=true", nil)
	r.Header.Set("Authorization", "Bearer "+testToken(t, strconv.Itoa(viewer), "viewer_in"))
	r.Header.Set("CF-IPCountry", "IN")
	w := httptest.NewRecorder()
	authed(SmartFeedHandler)(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("the feed answered %d: %s", w.Code, w.Body.String())
	}
	var page struct {
		Items []struct {
			Item struct {
				Challenge *struct {
					ID string `json:"id"`
				} `json:"challenge"`
			} `json:"item"`
			Challenge *struct {
				ID string `json:"id"`
			} `json:"challenge"`
			ScoreBreakdown map[string]float64 `json:"scoreBreakdown"`
		} `json:"items"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
		t.Fatalf("unreadable feed: %v\n%s", err, w.Body.String())
	}
	lifted, plain := 0, 0
	for _, it := range page.Items {
		ch := it.Challenge
		if ch == nil {
			ch = it.Item.Challenge
		}
		if ch == nil {
			continue
		}
		bonus, noted := it.ScoreBreakdown["countryBonus"]
		switch {
		case local[ch.ID] && bonus == sameCountryBonus:
			lifted++
		case local[ch.ID]:
			t.Errorf("a video from the viewer's country carries a lift of %v (noted: %v), want %v", bonus, noted, sameCountryBonus)
		case noted:
			t.Errorf("a video from another country carries a lift of %v", bonus)
		default:
			plain++
		}
	}
	if lifted == 0 || plain == 0 {
		t.Fatalf("the page has %d local videos with the lift and %d from elsewhere without: want some of each\n%s",
			lifted, plain, w.Body.String())
	}
}

// The three wires, read from the source with the comments taken out: the
// sign-in check notes the country, a new video is stamped, and the feed
// asks where the viewer is.
func TestCountry_IsWiredIn(t *testing.T) {
	comment := regexp.MustCompile(`(?m)^\s*//.*$`)
	code := func(file string) string {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		return comment.ReplaceAllString(string(src), "")
	}
	for file, wants := range map[string][]string{
		"auth.go":              {"noteCountry(claims.Subject, country)"},
		"challenge_handler.go": {"go stampCountry(challenge.ID, country)"},
		"feed_engine.go": {
			"viewerIn := viewerCountry(userID)",
			"score = addCountryBonus(score, breakdown, viewerIn, cs.Country)",
			"s += countryBonus(viewerIn, from[ch.ID])",
		},
		"signup.go":        {"country, _ := countryOf(r)", "ageRuleFor(country)"},
		"social_signin.go": {"country, _ := countryOf(r)", "rule := ageRuleFor(country)"},
	} {
		src := code(file)
		for _, want := range wants {
			if !strings.Contains(src, want) {
				t.Errorf("%s no longer has: %s", file, want)
			}
		}
	}
}
