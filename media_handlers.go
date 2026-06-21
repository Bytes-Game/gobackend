package main

// Media upload presign endpoint. The mobile client calls this once per
// challenge upload to receive presigned R2 PUT URLs for every variant
// (480p / 720p / 1080p video + thumbnail). The client then PUTs the
// transcoded files directly to R2 in parallel — the backend never sees
// the bytes, which keeps Render's free tier from being a bottleneck.
//
// Flow:
//
//   1. Client transcodes the source into N variants locally.
//   2. Client POSTs /api/v1/media/presign with one entry per variant.
//   3. Server validates the user, mints one presigned PUT URL per item,
//      and returns the matching long-lived public URL the client should
//      eventually persist into the Challenge row.
//   4. Client PUTs each variant in parallel.
//   5. Client POSTs /api/v1/challenges with publicUrl values from step 3.

import (
	"encoding/json"
	"net/http"
	"time"
)

// ---------- Wire format ----------

type presignItemRequest struct {
	// Kind is "video" or "thumbnail".
	Kind string `json:"kind"`
	// Variant is "480p", "720p", "1080p", "original" (videos) or
	// "default" (thumbnails). buildObjectKey enforces the closed set.
	Variant string `json:"variant"`
	// ContentType the client will set on the PUT (informational — we
	// don't sign it). Stored back in the response so the client doesn't
	// have to track variant→type mapping in two places.
	ContentType string `json:"contentType"`
}

type presignRequest struct {
	UserID string               `json:"userId"`
	Items  []presignItemRequest `json:"items"`
}

type presignItemResponse struct {
	Kind        string `json:"kind"`
	Variant     string `json:"variant"`
	ContentType string `json:"contentType"`
	Key         string `json:"key"`
	UploadURL   string `json:"uploadUrl"`
	PublicURL   string `json:"publicUrl"`
}

type presignResponse struct {
	UploadID  string                `json:"uploadId"`
	ExpiresIn int                   `json:"expiresIn"`
	Items     []presignItemResponse `json:"items"`
}

// presignMaxItems caps how many URLs one request can ask for. We need
// 4 today (3 video variants + 1 thumbnail); a cap of 8 leaves headroom
// for future quality tiers without letting a buggy client DoS the
// signer with thousands of useless URLs.
const presignMaxItems = 8

// presignExpiry is how long a minted PUT URL stays valid. 10 minutes is
// long enough for a slow connection on the 1080p variant (≈10 MB) and
// short enough that a leaked URL isn't an indefinite write hole.
const presignExpiry = 10 * time.Minute

// PresignMediaUploadHandler mints presigned R2 PUT URLs for every
// requested variant in one round trip.
//
// POST /api/v1/media/presign
func PresignMediaUploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		// CORS preflight — main.go's CORS middleware handles header
		// echo; we just need to return 200.
		w.WriteHeader(http.StatusOK)
		return
	}

	var payload presignRequest
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	// The uploader is the authenticated user, not a client-supplied id.
	payload.UserID = authUserID(r)
	if payload.UserID == "" {
		http.Error(w, "userId is required", http.StatusBadRequest)
		return
	}
	if len(payload.Items) == 0 {
		http.Error(w, "items must contain at least one entry", http.StatusBadRequest)
		return
	}
	if len(payload.Items) > presignMaxItems {
		http.Error(w, "too many items in one request", http.StatusBadRequest)
		return
	}

	// Confirm the user exists. We don't check ownership of any
	// challenge here — the upload happens before the challenge row is
	// created, so the only authn signal we have is "this userId is real".
	if _, ok := GetUserByID(payload.UserID); !ok {
		http.Error(w, "unknown userId", http.StatusUnauthorized)
		return
	}

	cfg, err := loadR2Config()
	if err != nil {
		// Surface the env-config failure clearly so a freshly deployed
		// instance with missing R2_* env vars fails loudly instead of
		// returning malformed URLs. This is a 503 because the
		// dependency (R2 credentials) isn't ready, not the request.
		http.Error(w, "media storage not configured: "+err.Error(), http.StatusServiceUnavailable)
		return
	}

	uploadID := newUploadID()
	out := presignResponse{
		UploadID:  uploadID,
		ExpiresIn: int(presignExpiry.Seconds()),
		Items:     make([]presignItemResponse, 0, len(payload.Items)),
	}

	// Track (kind,variant) duplicates so a buggy client that asks for
	// 720p twice gets a clear error instead of silently overwriting.
	seen := make(map[string]struct{}, len(payload.Items))
	for _, item := range payload.Items {
		key, err := buildObjectKey(payload.UserID, uploadID, item.Kind, item.Variant)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		dedupeKey := item.Kind + "|" + item.Variant
		if _, dup := seen[dedupeKey]; dup {
			http.Error(w, "duplicate (kind,variant) pair: "+dedupeKey, http.StatusBadRequest)
			return
		}
		seen[dedupeKey] = struct{}{}

		uploadURL, err := cfg.PresignPutURL(key, presignExpiry)
		if err != nil {
			http.Error(w, "failed to sign URL: "+err.Error(), http.StatusInternalServerError)
			return
		}
		out.Items = append(out.Items, presignItemResponse{
			Kind:        item.Kind,
			Variant:     item.Variant,
			ContentType: item.ContentType,
			Key:         key,
			UploadURL:   uploadURL,
			PublicURL:   cfg.PublicURL(key),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}
