package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
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
