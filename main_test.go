package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
)

func TestGetUsersHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/users", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(GetUsersHandler)

	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// Check the response body is what we expect.
	expected := `[{"username":"player1","fullName":"John Doe","league":"Gold","caption":" aspiring pro player!","postsCount":3,"wins":150,"losses":95,"followers":1200,"following":250,"posts":[{"type":"image","url":"https://example.com/post1.jpg","caption":"First post!"},{"type":"video","url":"https://example.com/post2.mp4","caption":"Highlight reel"},{"type":"image","url":"https://example.com/post3.jpg","caption":"New setup"}]},{"username":"gamer_girl","fullName":"Jane Smith","league":"Diamond","caption":"Just for fun ","postsCount":5,"wins":300,"losses":120,"followers":5600,"following":500,"posts":[{"type":"image","url":"https://example.com/gg_post1.jpg","caption":"Enjoying the game"},{"type":"image","url":"https://example.com/gg_post2.jpg","caption":"Team up?"},{"type":"video","url":"https://example.com/gg_post3.mp4","caption":"Funny moments"},{"type":"image","url":"https://example.com/gg_post4.jpg","caption":"My cat watching me play"},{"type":"image","url":"https://example.com/gg_post5.jpg","caption":"Just hit Diamond!"}]},{"username":"pro_streamer","fullName":"Alex Johnson","league":"Challenger","caption":"Streaming daily at twitch.tv/pro_streamer","postsCount":2,"wins":500,"losses":50,"followers":100000,"following":100,"posts":[{"type":"video","url":"https://example.com/stream_highlight1.mp4","caption":"1v5 clutch"},{"type":"video","url":"https://example.com/stream_highlight2.mp4","caption":"Tournament win!"}]}]
`
	if rr.Body.String() != expected {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
}

func TestGetUserHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/users/player1", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	// Need to create a router that we can pass in the request with the vars
	r := mux.NewRouter()
	r.HandleFunc("/users/{username}", GetUserHandler)
	r.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// Check the response body is what we expect.
	expected := `{"username":"player1","fullName":"John Doe","league":"Gold","caption":" aspiring pro player!","postsCount":3,"wins":150,"losses":95,"followers":1200,"following":250,"posts":[{"type":"image","url":"https://example.com/post1.jpg","caption":"First post!"},{"type":"video","url":"https://example.com/post2.mp4","caption":"Highlight reel"},{"type":"image","url":"https://example.com/post3.jpg","caption":"New setup"}]}
`
	if rr.Body.String() != expected {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
}

func TestCreateUserHandler(t *testing.T) {
	var jsonStr = []byte(`{"username":"new_user","fullName":"New User","league":"Bronze","caption":"Just starting out!","postsCount":0,"wins":0,"losses":0,"followers":0,"following":0,"posts":[]}`)
	req, err := http.NewRequest("POST", "/users", bytes.NewBuffer(jsonStr))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(CreateUserHandler)

	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusCreated {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusCreated)
	}

	expected := `{"username":"new_user","fullName":"New User","league":"Bronze","caption":"Just starting out!","postsCount":0,"wins":0,"losses":0,"followers":0,"following":0,"posts":[]}
`
	if rr.Body.String() != expected {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
}