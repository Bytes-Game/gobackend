package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestIsHardBlockReason(t *testing.T) {
	cases := map[string]bool{
		"block":           true,
		"abuse":           true,
		"harassment":      true,
		"hate":            true,
		"threats":         true,
		"sexual_content":  true,
		"violence":        true,
		"spam":            false,
		"other":           false,
		"":                false,
		"BLOCK":           false, // case-sensitive on purpose
	}
	for reason, want := range cases {
		if got := isHardBlockReason(reason); got != want {
			t.Errorf("isHardBlockReason(%q)=%v, want %v", reason, got, want)
		}
	}
}

func TestHandleUnblockEvent_RemovesBlockedCreator(t *testing.T) {
	resetRedis(t)
	// Pre-populate: user has blocked a creator.
	MarkBlocked("u_unblock_1", "c_blocked_1")
	if ok, _ := rdb.SIsMember(rctx, "blocked_creators:u_unblock_1", "c_blocked_1").Result(); !ok {
		t.Fatalf("setup: creator not in blocked set")
	}
	body := map[string]string{"userId": "u_unblock_1", "creatorId": "c_blocked_1"}
	js, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/api/v1/unblock", bytes.NewReader(js))
	w := httptest.NewRecorder()
	HandleUnblockEvent(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	// Goroutine — give it a moment.
	time.Sleep(50 * time.Millisecond)
	if ok, _ := rdb.SIsMember(rctx, "blocked_creators:u_unblock_1", "c_blocked_1").Result(); ok {
		t.Errorf("creator still in blocked set after unblock")
	}
}

func TestHandleUnblockEvent_BadJSON(t *testing.T) {
	req := httptest.NewRequest("POST", "/api/v1/unblock", bytes.NewReader([]byte("{not json}")))
	w := httptest.NewRecorder()
	HandleUnblockEvent(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 on bad JSON, got %d", w.Code)
	}
}

func TestHandleUnblockEvent_MissingFields(t *testing.T) {
	body, _ := json.Marshal(map[string]string{"userId": "u1"})
	req := httptest.NewRequest("POST", "/api/v1/unblock", bytes.NewReader(body))
	w := httptest.NewRecorder()
	HandleUnblockEvent(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 on missing creatorId, got %d", w.Code)
	}
}

func TestAdminOnlineUsersHandler_EmptyList(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v1/admin/online", nil)
	w := httptest.NewRecorder()
	AdminOnlineUsersHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var out struct {
		Count     int      `json:"count"`
		Usernames []string `json:"usernames"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Count != len(out.Usernames) {
		t.Errorf("count %d != len(usernames) %d", out.Count, len(out.Usernames))
	}
}
