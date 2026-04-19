package main

import (
	"encoding/json"
	"testing"
)

func TestGetGoldenHour_Unknown(t *testing.T) {
	resetRedis(t)
	h, conf := GetGoldenHour("who")
	if h != -1 || conf != 0 {
		t.Errorf("unknown user should yield (-1, 0), got (%d, %v)", h, conf)
	}
}

func TestGetGoldenHour_EmptyUserID(t *testing.T) {
	resetRedis(t)
	h, conf := GetGoldenHour("")
	if h != -1 || conf != 0 {
		t.Errorf("empty userID should yield (-1, 0), got (%d, %v)", h, conf)
	}
}

func TestGetGoldenHour_HappyPath(t *testing.T) {
	resetRedis(t)
	planted := goldenHour{Hour: 21, Confidence: 0.65}
	js, _ := json.Marshal(planted)
	if err := rdb.Set(rctx, "golden_hour:userX", js, 0).Err(); err != nil {
		t.Fatalf("plant failed: %v", err)
	}
	h, conf := GetGoldenHour("userX")
	if h != 21 {
		t.Errorf("hour mismatch: got %d, want 21", h)
	}
	if conf < 0.64 || conf > 0.66 {
		t.Errorf("confidence mismatch: got %v, want ~0.65", conf)
	}
}

func TestGetGoldenHour_InvalidJsonYieldsUnknown(t *testing.T) {
	resetRedis(t)
	_ = rdb.Set(rctx, "golden_hour:userY", "not-json", 0).Err()
	h, conf := GetGoldenHour("userY")
	if h != -1 || conf != 0 {
		t.Errorf("bad json should yield (-1, 0), got (%d, %v)", h, conf)
	}
}
