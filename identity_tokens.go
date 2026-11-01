package main

// identity_tokens.go — checking that a "this is me" token really comes from
// Google or Apple.
//
// When somebody taps "Continue with Google" (or Apple), the phone gets a
// signed token from Google (or Apple) saying who they are, and sends it
// here. Nothing in it can be trusted until it is checked:
//
//   - the signature, against the keys Google (or Apple) publishes;
//   - who made it (the issuer): Google, or Apple, and nobody else;
//   - who it was made FOR (the audience): this app's own ids, from the
//     settings below. A token Google made for some other app is refused,
//     even though Google really signed it;
//   - that it has not run out.
//
// Settings (Render → the service → Environment), each a comma-separated
// list:
//
//	GOOGLE_CLIENT_IDS  the app's OAuth client ids from Google Cloud: the
//	                   "Web" one (the app asks for tokens made for it) and
//	                   the iOS one.
//	APPLE_CLIENT_IDS   the iPhone app's bundle id, and the Services id used
//	                   for Apple sign-in on Android.
//
// Without them, signing in with that provider says it is not set up,
// rather than accepting tokens made for anybody.

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
)

// identity is who a provider says somebody is.
type identity struct {
	Subject       string // the provider's id for the person; never changes
	Email         string
	EmailVerified bool
	Name          string
}

// identityProvider is Google or Apple: where its keys are, which issuers
// it signs as, and which of this app's ids its tokens must be made for.
type identityProvider struct {
	id          string // "google" or "apple"
	name        string // "Google" or "Apple", for people
	issuers     []string
	keysURL     string
	audienceEnv string

	mu      sync.Mutex
	keys    map[string]*rsa.PublicKey
	fetched time.Time
}

var (
	googleProvider = &identityProvider{
		id:          "google",
		name:        "Google",
		issuers:     []string{"https://accounts.google.com", "accounts.google.com"},
		keysURL:     "https://www.googleapis.com/oauth2/v3/certs",
		audienceEnv: "GOOGLE_CLIENT_IDS",
	}
	appleProvider = &identityProvider{
		id:          "apple",
		name:        "Apple",
		issuers:     []string{"https://appleid.apple.com"},
		keysURL:     "https://appleid.apple.com/auth/keys",
		audienceEnv: "APPLE_CLIENT_IDS",
	}
)

// audiences is this app's ids for the provider, from its setting.
func (p *identityProvider) audiences() []string {
	var out []string
	for _, a := range strings.Split(os.Getenv(p.audienceEnv), ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}

// configured is whether the provider's setting is there at all.
func (p *identityProvider) configured() bool { return len(p.audiences()) > 0 }

// keysFresh is how long fetched keys are used before being fetched again.
// The providers change keys every few weeks and publish the new one early.
const keysFresh = 6 * time.Hour

// keysClient fetches the providers' keys, with a deadline so a provider
// that does not answer cannot hold a sign-in forever.
var keysClient = &http.Client{Timeout: 10 * time.Second}

// key is the provider's public key [kid]. Fetched when unknown or stale,
// but not more than once a minute, so tokens naming made-up keys cannot
// make the server hammer the provider.
func (p *identityProvider) key(kid string) (*rsa.PublicKey, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	k, known := p.keys[kid]
	stale := time.Since(p.fetched) > keysFresh
	if known && !stale {
		return k, nil
	}
	if time.Since(p.fetched) > time.Minute || p.keys == nil {
		keys, err := fetchKeys(p.keysURL)
		if err != nil {
			if known {
				return k, nil // the old key still works for now
			}
			return nil, fmt.Errorf("fetching %s's keys: %w", p.name, err)
		}
		p.keys, p.fetched = keys, time.Now()
		k, known = p.keys[kid]
	}
	if !known {
		return nil, fmt.Errorf("%s has no key %q", p.name, kid)
	}
	return k, nil
}

// fetchKeys reads a provider's published keys (a JSON Web Key Set).
func fetchKeys(url string) (map[string]*rsa.PublicKey, error) {
	res, err := keysClient.Get(url)
	if err != nil || res == nil {
		return nil, fmt.Errorf("no answer: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", res.StatusCode)
	}
	var set struct {
		Keys []struct {
			Kid string `json:"kid"`
			Kty string `json:"kty"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(res.Body).Decode(&set); err != nil {
		return nil, err
	}
	keys := map[string]*rsa.PublicKey{}
	for _, k := range set.Keys {
		if k.Kty != "RSA" || k.Kid == "" {
			continue
		}
		n, err1 := base64.RawURLEncoding.DecodeString(k.N)
		e, err2 := base64.RawURLEncoding.DecodeString(k.E)
		if err1 != nil || err2 != nil || len(e) == 0 || len(e) > 4 {
			continue
		}
		exp := 0
		for _, b := range e {
			exp = exp<<8 | int(b)
		}
		keys[k.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: exp}
	}
	if len(keys) == 0 {
		return nil, errors.New("no usable keys")
	}
	return keys, nil
}

// verify checks [raw] and says who it is about.
func (p *identityProvider) verify(raw string) (identity, error) {
	claims := jwt.MapClaims{}
	_, err := jwt.ParseWithClaims(raw, claims, func(t *jwt.Token) (interface{}, error) {
		kid, _ := t.Header["kid"].(string)
		return p.key(kid)
	}, jwt.WithValidMethods([]string{"RS256"}), jwt.WithExpirationRequired(), jwt.WithLeeway(time.Minute))
	if err != nil {
		return identity{}, err
	}
	iss, _ := claims["iss"].(string)
	if !containsString(p.issuers, iss) {
		return identity{}, fmt.Errorf("made by %q, not %s", iss, p.name)
	}
	aud, err := claims.GetAudience()
	if err != nil {
		return identity{}, err
	}
	madeForUs := false
	for _, a := range aud {
		if containsString(p.audiences(), a) {
			madeForUs = true
		}
	}
	if !madeForUs {
		return identity{}, fmt.Errorf("made for %v, not this app", aud)
	}
	sub, _ := claims["sub"].(string)
	if sub == "" {
		return identity{}, errors.New("no subject")
	}
	email, _ := claims["email"].(string)
	name, _ := claims["name"].(string)
	// Google says true; Apple says true or "true".
	verified := claims["email_verified"] == true || claims["email_verified"] == "true"
	return identity{Subject: sub, Email: strings.ToLower(strings.TrimSpace(email)),
		EmailVerified: verified, Name: strings.TrimSpace(name)}, nil
}
