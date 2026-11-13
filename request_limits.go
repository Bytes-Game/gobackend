package main

// request_limits.go — how much anyone may send this server at once.
//
// Nothing capped it. A request is read into memory as it is decoded, so
// one person with a script could send a few gigabytes to any address and
// take the server down for everybody, signed in or not. Videos and
// pictures never come through here (they go straight to storage), so
// nothing the app really sends is anywhere near the limit: the biggest is
// a batch of 200 activity reports, about a tenth of the limit.

import "net/http"

// maxRequestBody is the most one request may carry: 1 MB.
const maxRequestBody = 1 << 20

// maxEventsPerBatch is the most activity reports one batch may carry. The
// app sends at most 200 (all it keeps while offline); each report becomes
// several database writes, so this bounds the work one request can cause.
const maxEventsPerBatch = 500

// limitRequestBodies refuses a request that says it is bigger than
// maxRequestBody, and stops reading one that turns out to be. A handler
// reading past the limit gets an error, as for any broken request.
func limitRequestBodies(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > maxRequestBody {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
			return
		}
		if r.Body != nil && r.Body != http.NoBody {
			r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
		}
		next.ServeHTTP(w, r)
	})
}
