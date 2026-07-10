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
	"net/http"
	"strings"
	"time"
)

const multipartPresignExpiry = 30 * time.Minute

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
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeURL(u, nil)

	default:
		http.Error(w, "action must be init|part|complete|abort", http.StatusBadRequest)
	}
}
