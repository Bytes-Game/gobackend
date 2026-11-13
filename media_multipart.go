package main

// media_multipart.go — presigned S3 multipart uploads against R2.
//
// The single presigned-PUT path uploads a whole file in one request:
// one network blip = restart from byte zero, and progress is coarse.
// Multipart fixes both: the client splits large files into parts,
// uploads them in parallel with per-part retry, and completes the
// upload with the collected ETags. R2 speaks the standard S3 multipart
// API; the backend only SIGNS the operations (init/part/complete/
// abort) — bytes still go client→R2 directly, same as ever.
//
// One endpoint, action-discriminated, mirroring the existing presign
// handler's philosophy: POST /api/v1/media/multipart
//
//	{action:"init",     kind, variant}                → {key, url, publicUrl}
//	{action:"part",     key, uploadId, partNumber}    → {url}
//	{action:"complete", key, uploadId}                → {url}
//	{action:"abort",    key, uploadId}                → {url}
//
// The client executes the returned URLs itself (init/complete are
// XML-bodied POSTs per the S3 spec). Authorization: the caller must
// own the key — every key we sign is prefix-checked against the
// authenticated user's namespace (u/<userID>/...), so one user can
// never sign operations against another user's objects.

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

const multipartPresignExpiry = 30 * time.Minute

// multipartMaxParts is the most pieces one file may be uploaded in. The
// app cuts a file into pieces of a few megabytes, so 200 pieces is far
// more than the longest video it makes.
const multipartMaxParts = 200

type multipartRequest struct {
	Action     string `json:"action"`
	Kind       string `json:"kind,omitempty"`
	Variant    string `json:"variant,omitempty"`
	Key        string `json:"key,omitempty"`
	UploadID   string `json:"uploadId,omitempty"`
	PartNumber int    `json:"partNumber,omitempty"`
}

// MultipartPresignHandler — POST /api/v1/media/multipart (authed).
func MultipartPresignHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	userID := authUserID(r)
	if userID == "" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	cfg, err := loadR2Config()
	if err != nil {
		http.Error(w, "media storage not configured", http.StatusServiceUnavailable)
		return
	}
	var req multipartRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}

	writeURL := func(u string, extra map[string]string) {
		out := map[string]interface{}{"url": u}
		for k, v := range extra {
			out[k] = v
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	}

	switch req.Action {
	case "init":
		// The same budget as a one-piece upload (media_handlers.go).
		if !allowAction(userID, "upload_start") {
			writeRateLimited(w, "upload_start")
			return
		}
		key, err := buildObjectKey(userID, newUploadID(), req.Kind, req.Variant)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		u, err := cfg.PresignMultipartInitURL(key, multipartPresignExpiry)
		if err != nil {
			http.Error(w, "presign failed", http.StatusInternalServerError)
			return
		}
		writeURL(u, map[string]string{
			"key":       key,
			"publicUrl": cfg.PublicURL(key),
		})

	case "part", "complete", "abort":
		// Ownership: only sign operations inside the caller's namespace.
		if !strings.HasPrefix(req.Key, fmt.Sprintf("u/%s/", userID)) {
			http.Error(w, "key not owned by caller", http.StatusForbidden)
			return
		}
		// A file in pieces has at most multipartMaxParts of them, and one
		// person signs only so many pieces an hour. Storage takes up to
		// 10,000 pieces of 5 GB each: nothing here ever needs that.
		if req.Action == "part" {
			if req.PartNumber < 1 || req.PartNumber > multipartMaxParts {
				http.Error(w, "that file is too big to upload", http.StatusRequestEntityTooLarge)
				return
			}
			if !allowAction(userID, "upload_part") {
				writeRateLimited(w, "upload_part")
				return
			}
		}
		var u string
		var err error
		switch req.Action {
		case "part":
			u, err = cfg.PresignUploadPartURL(req.Key, req.UploadID, req.PartNumber, multipartPresignExpiry)
		case "complete":
			u, err = cfg.PresignMultipartCompleteURL(req.Key, req.UploadID, multipartPresignExpiry)
		default:
			u, err = cfg.PresignMultipartAbortURL(req.Key, req.UploadID, multipartPresignExpiry)
		}
		if err != nil {
			log.Printf("upload in pieces: signing %s for user %s failed: %v", req.Action, userID, err)
			http.Error(w, "that upload could not be signed", http.StatusBadRequest)
			return
		}
		writeURL(u, nil)

	default:
		http.Error(w, "action must be init|part|complete|abort", http.StatusBadRequest)
	}
}
