package main

// Photo posts, the parts that need no database. See photo_posts.go.

import (
	"strings"
	"testing"
)

func TestPhotoUploadsHaveTheirOwnFileName(t *testing.T) {
	photo, err := buildObjectKey("7", "up1", "photo", "default")
	if err != nil {
		t.Fatalf("a photo upload was refused: %v", err)
	}
	thumb, err := buildObjectKey("7", "up1", "thumbnail", "default")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(photo, "/photo.jpg") {
		t.Errorf("the photo is kept as %q", photo)
	}
	if photo == thumb {
		t.Errorf("the photo and its thumbnail share %q: one would overwrite the other", photo)
	}
}

func TestMediaTypeOf(t *testing.T) {
	for in, want := range map[string]string{
		"": mediaVideo, "video": mediaVideo, "photo": mediaPhoto, " Photo ": mediaPhoto,
	} {
		got, ok := mediaTypeOf(in)
		if !ok || got != want {
			t.Errorf("mediaTypeOf(%q) = %q, %v; want %q", in, got, ok, want)
		}
	}
	if _, ok := mediaTypeOf("gif"); ok {
		t.Error("an unknown kind of post was accepted")
	}
}
