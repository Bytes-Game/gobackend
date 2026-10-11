package main

// change_password.go — POST /api/v1/auth/password.
//
// Change your password: say the current one, choose a new one. Every other
// phone signed in to the account is signed out (token_cutoff.go); this one
// gets a fresh sign-in in the answer, so it stays in.

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"
)

// ChangePasswordHandler — POST /api/v1/auth/password
// {currentPassword, newPassword} → {token}, or {field, error}.
func ChangePasswordHandler(w http.ResponseWriter, r *http.Request) {
	userID, username := authUserID(r), authUsername(r)
	if userID == "" || username == "" || db == nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	// Each try checks the current password, so this is also a way to guess
	// it: held to a few an hour.
	if !allowAction(userID, "password_change") {
		writeRateLimited(w, "password_change")
		return
	}
	var body struct {
		CurrentPassword string `json:"currentPassword"`
		NewPassword     string `json:"newPassword"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 8<<10)).Decode(&body); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if !IsValidUser(username, body.CurrentPassword) {
		writeFieldError(w, http.StatusForbidden,
			refuse("currentPassword", "That isn't your current password."))
		return
	}
	if why := passwordProblem(body.NewPassword, username); why != "" {
		writeFieldError(w, http.StatusBadRequest, refuse("newPassword", why))
		return
	}
	if body.NewPassword == body.CurrentPassword {
		writeFieldError(w, http.StatusBadRequest,
			refuse("newPassword", "Choose a password you haven't been using."))
		return
	}
	hash, err := hashPassword(body.NewPassword)
	if err != nil {
		log.Printf("change password: could not hash for user %s: %v", userID, err)
		http.Error(w, "could not change the password", http.StatusInternalServerError)
		return
	}
	// The moment of the change, to the second, from this server's clock —
	// the same clock that stamps the fresh sign-in below, so that sign-in
	// can never look older than the change.
	changedAt := time.Now().Truncate(time.Second)
	res, err := db.Exec(`
		UPDATE users SET password_hash = $1, password = '', password_changed_at = $2
		 WHERE id = $3`, hash, changedAt, userID)
	if err != nil {
		log.Printf("change password: could not save for user %s: %v", userID, err)
		http.Error(w, "could not change the password", http.StatusInternalServerError)
		return
	}
	if n, _ := res.RowsAffected(); n != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	forgetCutoff(userID)
	token, err := issueToken(userID, username)
	if err != nil {
		log.Printf("change password: changed for user %s, but no fresh sign-in: %v", userID, err)
		http.Error(w, "Password changed. Please sign in again.", http.StatusInternalServerError)
		return
	}
	log.Printf("user %s changed their password; other sign-ins are signed out", userID)
	writeJSON(w, http.StatusOK, map[string]string{"token": token})
}
