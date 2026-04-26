package main

import "os"

// getEnvFromOS isolates the actual os.Getenv call so notification_sender.go's
// envLookup indirection has a real implementation. Trivial — exists so we
// can stub envLookup in tests without touching production code.
func getEnvFromOS(k string) string {
	return os.Getenv(k)
}
