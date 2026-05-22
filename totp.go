package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base32"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// TOTP (RFC 6238) implementation backed by the stdlib so we don't pull
// in a fresh module for 80 lines of well-trodden code.
//
// Why we wrote this in-house:
//   * crypto/hmac + crypto/sha1 + encoding/base32 already ship with Go
//     — there's nothing exotic about RFC 6238, just HMAC-SHA1 over a
//     30-second counter and a 6-digit truncation.
//   * Adding a dependency means a `go mod tidy` round trip on every
//     deploy box. For a feature this small and stable, the stdlib win
//     beats the "battle-tested library" win — the spec hasn't changed
//     since 2011 and there are no known cryptographic surprises.
//   * We don't need extras (HOTP, SHA-256-based TOTP, encrypted secret
//     blobs) so the larger libraries are mostly unused weight.

const (
	// 30 seconds — the RFC 6238 default. Every authenticator app
	// (Google Authenticator, Authy, 1Password) expects this; don't
	// change it without also changing the otpauth:// step= param so
	// new enrollments produce the same code as the secret implies.
	totpStepSeconds = 30
	// 6-digit codes — also the spec default and the universal client
	// expectation. 8-digit is an option but every authenticator app
	// has to be told explicitly to use it.
	totpDigits = 6
	// ±1 window of skew tolerance. With 30s steps that means we accept
	// codes up to ~30s old or ~30s in the future. Necessary because
	// phone clocks drift; standard practice.
	totpSkewWindows = 1
)

// generateTOTPSecret returns a fresh base32-encoded 160-bit secret
// suitable for embedding in an otpauth:// URI. 160 bits matches what
// Google Authenticator emits, gives us a comfortable safety margin,
// and base32-encodes to exactly 32 chars (no padding) — the form most
// authenticator apps expect when scanning or manual-entering.
func generateTOTPSecret() (string, error) {
	raw := make([]byte, 20) // 20 bytes = 160 bits
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	enc := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw)
	return enc, nil
}

// generateTOTPCode returns the 6-digit code for `secret` at time `t`.
// Used both for verification (we compute the expected code and compare)
// and during enrollment-test (server-side sanity check).
func generateTOTPCode(secret string, t time.Time) (string, error) {
	key, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(strings.ToUpper(secret))
	if err != nil {
		return "", err
	}
	counter := uint64(t.Unix() / totpStepSeconds)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, counter)

	mac := hmac.New(sha1.New, key)
	mac.Write(buf)
	sum := mac.Sum(nil)

	// Dynamic truncation — RFC 4226 §5.3.
	offset := sum[len(sum)-1] & 0x0f
	value := binary.BigEndian.Uint32(sum[offset:offset+4]) & 0x7fffffff
	mod := uint32(1)
	for range totpDigits {
		mod *= 10
	}
	code := value % mod
	return fmt.Sprintf("%0*d", totpDigits, code), nil
}

// verifyTOTPCode compares `code` against expected values for the
// current window and ±totpSkewWindows. Constant-time comparison so
// timing differences can't reveal partial code matches.
func verifyTOTPCode(secret, code string) bool {
	if len(code) != totpDigits {
		return false
	}
	now := time.Now()
	for i := -totpSkewWindows; i <= totpSkewWindows; i++ {
		t := now.Add(time.Duration(i*totpStepSeconds) * time.Second)
		expected, err := generateTOTPCode(secret, t)
		if err != nil {
			continue
		}
		// hmac.Equal is the stdlib constant-time byte comparator;
		// using it on string-bytes is the canonical pattern.
		if hmac.Equal([]byte(expected), []byte(code)) {
			return true
		}
	}
	return false
}

// otpauthURI builds the otpauth:// link that QR-scans into any
// authenticator app. `issuer` shows up as the account name in the
// app's list (e.g. "devf"); `account` is usually the username so the
// user can tell their accounts apart.
//
// We deliberately use net/url so a username with spaces or special
// chars doesn't break the QR. The result is a URI string the Flutter
// client can either render as a QR (preferred) or surface as a
// fallback "manual entry" string.
func otpauthURI(issuer, account, secret string) string {
	v := url.Values{}
	v.Set("secret", secret)
	v.Set("issuer", issuer)
	v.Set("algorithm", "SHA1")
	v.Set("digits", fmt.Sprintf("%d", totpDigits))
	v.Set("period", fmt.Sprintf("%d", totpStepSeconds))
	label := url.PathEscape(issuer + ":" + account)
	return "otpauth://totp/" + label + "?" + v.Encode()
}

// generateRecoveryCodes returns `n` single-use backup codes plus
// their SHA-256 hashes. Plaintext is shown to the user ONCE on
// enrollment; only the hashes get persisted, so a DB compromise
// alone can't bypass 2FA.
//
// Format choice: 10 base32 chars (~50 bits of entropy) hyphenated as
// xxxxx-xxxxx. Plenty of brute-force resistance for a single-use
// code, easy to read off a screen, and avoids ambiguous characters
// (no 0/O/1/I) because base32's alphabet skips them.
func generateRecoveryCodes(n int) (plaintext []string, hashes []string, err error) {
	plaintext = make([]string, 0, n)
	hashes = make([]string, 0, n)
	for range n {
		raw := make([]byte, 7) // 7 bytes → 56 bits → ~12 base32 chars
		if _, err = rand.Read(raw); err != nil {
			return nil, nil, err
		}
		enc := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw)
		// Format the code as xxxxx-xxxxx for readability; the user
		// reads it off the screen during enrollment.
		if len(enc) >= 10 {
			enc = enc[:5] + "-" + enc[5:10]
		}
		plaintext = append(plaintext, enc)
		hashes = append(hashes, hashRecoveryCode(enc))
	}
	return plaintext, hashes, nil
}

// hashRecoveryCode returns the lowercase-hex SHA-256 of a recovery
// code. The code itself has 50+ bits of entropy so a fast hash is
// fine here — we're storing it to defend against a DB read, not
// against an offline brute force.
func hashRecoveryCode(code string) string {
	h := sha256.Sum256([]byte(strings.ToUpper(strings.TrimSpace(code))))
	return hex.EncodeToString(h[:])
}
