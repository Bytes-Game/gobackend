package main

// Tests for the chat-send guardrails (rate limit, length cap) added in
// the 2026-07 batch. These exercise testToken/withAuth for their
// documented purposes: testToken drives a request through the real
// authed() middleware; withAuth injects trusted identity for direct
// handler invocation.

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// oversizeChatBody builds a syntactically valid send payload whose
// message exceeds maxChatMessageLen. Oversized on purpose: the length
// check fires before any DB access, so these tests run without sqlmock.
func oversizeChatBody() string {
	return `{"receiverId":"2","message":"` + strings.Repeat("x", maxChatMessageLen+1) + `"}`
}

// TestChatSend_LengthCap_ThroughMiddleware drives POST /chat/send
// through the real authed() wrapper with a real bearer token.
func TestChatSend_LengthCap_ThroughMiddleware(t *testing.T) {
	resetRedis(t)
	h := authed(SendMessageHandler)

	req := httptest.NewRequest("POST", "/api/v1/chat/send", strings.NewReader(oversizeChatBody()))
	req.Header.Set("Authorization", "Bearer "+testToken(t, "9001", "capuser"))
	rec := httptest.NewRecorder()
	h(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized message = %d, want 413", rec.Code)
	}
}

// TestChatSend_RateLimited verifies the "chat" action bucket finally
// gates the send endpoint (burst 10, then 429). Identity is injected
// via withAuth; a distinct user ID keeps this test's bucket isolated
// from the length-cap test above.
func TestChatSend_RateLimited(t *testing.T) {
	resetRedis(t)
	body := oversizeChatBody()

	last := 0
	for i := 0; i < 11; i++ {
		req := withAuth(
			httptest.NewRequest("POST", "/api/v1/chat/send", strings.NewReader(body)),
			"9002", "burstuser")
		rec := httptest.NewRecorder()
		SendMessageHandler(rec, req)
		last = rec.Code
		if i < 10 && rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("request %d = %d, want 413 (within burst, oversized)", i+1, rec.Code)
		}
	}
	if last != http.StatusTooManyRequests {
		t.Fatalf("11th request = %d, want 429 (burst exhausted)", last)
	}
}

// TestChatSend_RejectsUnauthenticated: no token, no injected identity →
// senderID resolves empty → 400 (and never reaches the DB).
func TestChatSend_RejectsUnauthenticated(t *testing.T) {
	resetRedis(t)
	req := httptest.NewRequest("POST", "/api/v1/chat/send",
		strings.NewReader(`{"receiverId":"2","message":"hi"}`))
	rec := httptest.NewRecorder()
	SendMessageHandler(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unauthenticated send = %d, want 400", rec.Code)
	}
}
