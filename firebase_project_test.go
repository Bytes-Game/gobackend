package main

// The server's start-up line names the Firebase project it sends
// notifications through. See firebaseProjectNote.

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"
)

// startedWith runs the server's real start-up step for notifications with
// [settings], and returns what it wrote to the log. The sender in use
// before is put back afterwards.
func startedWith(t *testing.T, settings map[string]string) string {
	t.Helper()
	for _, k := range []string{"NOTIFICATION_SENDER", "FCM_SERVICE_ACCOUNT_JSON", "FCM_PROJECT"} {
		t.Setenv(k, settings[k])
	}
	before := getCurrentSender()
	t.Cleanup(func() { setCurrentSender(before) })
	var logged bytes.Buffer
	log.SetOutput(&logged)
	defer log.SetOutput(os.Stderr)
	initPushSender()
	return logged.String()
}

// A key in the shape Firebase gives it. Nothing reads the private key
// until a notification is sent.
func serviceAccountFor(project string) string {
	return `{"type":"service_account","project_id":"` + project +
		`","client_email":"push@` + project + `.iam.gserviceaccount.com","private_key":"-----BEGIN PRIVATE KEY-----\nstand-in\n-----END PRIVATE KEY-----\n"}`
}

func TestFirebaseProject_TheStartUpLineNamesIt(t *testing.T) {
	said := startedWith(t, map[string]string{
		"NOTIFICATION_SENDER":      "fcm",
		"FCM_SERVICE_ACCOUNT_JSON": serviceAccountFor("battle-adddb"),
	})
	if !strings.Contains(said, "notifications: sender=fcm, Firebase project battle-adddb.") {
		t.Errorf("the start-up line does not name the project:\n%s", said)
	}
	if !strings.Contains(said, "must be built for the same project") {
		t.Errorf("the start-up line does not say why it matters:\n%s", said)
	}
}

func TestFirebaseProject_FCMProjectWinsOverTheKey(t *testing.T) {
	// FCM_PROJECT overrides the project the key names, so the line must
	// name the one sends really go through.
	said := startedWith(t, map[string]string{
		"NOTIFICATION_SENDER":      "fcm",
		"FCM_SERVICE_ACCOUNT_JSON": serviceAccountFor("battle-adddb"),
		"FCM_PROJECT":              "battle-38226",
	})
	if !strings.Contains(said, "Firebase project battle-38226.") {
		t.Errorf("the line does not name the project sends go through:\n%s", said)
	}
}

func TestFirebaseProject_BothKindsOfPhone(t *testing.T) {
	said := startedWith(t, map[string]string{
		"NOTIFICATION_SENDER":      "multi",
		"FCM_SERVICE_ACCOUNT_JSON": serviceAccountFor("battle-adddb"),
	})
	if !strings.Contains(said, "notifications: sender=multi, Firebase project battle-adddb.") {
		t.Errorf("with Android and iPhone both on, the line does not name the project:\n%s", said)
	}
}

func TestFirebaseProject_NothingToNameWithoutAKey(t *testing.T) {
	// No key: the line that says so, and no project named.
	said := startedWith(t, map[string]string{"NOTIFICATION_SENDER": "fcm"})
	if !strings.Contains(said, "FCM_SERVICE_ACCOUNT_JSON missing") {
		t.Errorf("a missing key is not reported:\n%s", said)
	}
	if strings.Contains(said, "Firebase project") {
		t.Errorf("a project was named with no key:\n%s", said)
	}
	// Not sending through Firebase at all: nothing to name.
	said = startedWith(t, map[string]string{"NOTIFICATION_SENDER": "log"})
	if !strings.Contains(said, "notifications: sender=log") || strings.Contains(said, "Firebase project") {
		t.Errorf("with notifications only written to the log:\n%s", said)
	}
}
