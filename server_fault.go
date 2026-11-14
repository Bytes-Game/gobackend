package main

// server_fault.go — a failure that is the server's own is said plainly to
// the caller, and in full only in the server's log.
//
// About thirty answers here used to end with the raw error:
//
//	http.Error(w, "update failed: "+err.Error(), 500)
//
// For a database error that is the database's own words: table and column
// names, the name of the rule that was broken, sometimes the value. For
// storage, the names of the settings that are missing. None of it helps the
// person holding the phone, and all of it helps somebody working out how
// the server is built.

import (
	"log"
	"net/http"
)

// serverFault answers 500 for something that went wrong on the server
// while [doing] something ("following maya", "saving the profile"). The
// reason goes to the log, never to the caller.
func serverFault(w http.ResponseWriter, doing string, err error) {
	log.Printf("%s failed: %v", doing, err)
	http.Error(w, "Something went wrong on our side. Please try again.", http.StatusInternalServerError)
}
