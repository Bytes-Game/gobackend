package main

// Email that cannot get out has to say so, and say why: at startup, and
// when a code is sent. On 7 November a phone log showed the reset code
// failing after eleven seconds, with Gmail set up perfectly: Render's free
// plan blocks the email ports, and nothing said so anywhere.

import (
	"bytes"
	"log"
	"net"
	"os"
	"regexp"
	"strings"
	"syscall"
	"testing"
)

// neverAnswers stands in for a network where nothing answers on the email
// ports, the way Render's free plan blocks them: no refusal, just waiting
// until the dial gives up.
func neverAnswers(t *testing.T) {
	t.Helper()
	was := dialMail
	t.Cleanup(func() { dialMail = was })
	dialMail = func(addr string) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}
	}
}

func gmailSetUp(t *testing.T, port string) {
	t.Helper()
	t.Setenv("RESEND_API_KEY", "")
	t.Setenv("SMTP_HOST", "smtp.gmail.com")
	t.Setenv("SMTP_PORT", port)
	t.Setenv("SMTP_USERNAME", "flexete.team@gmail.com")
	t.Setenv("SMTP_PASSWORD", "app password")
	t.Setenv("MAIL_FROM", "Flexete <flexete.team@gmail.com>")
}

const blockedHint = "Render's free plan, that plan blocks the email ports 25, 465 and 587"

func TestMail_ABlockedEmailPortIsCalledThat(t *testing.T) {
	neverAnswers(t)
	for port, said := range map[string]bool{"587": true, "465": true, "25": true, "2525": false} {
		gmailSetUp(t, port)
		err := sendMail("someone@example.com", "subject", "body")
		if err == nil {
			t.Fatalf("port %s: an email went out through a network where nothing answers", port)
		}
		if got := strings.Contains(err.Error(), blockedHint); got != said {
			t.Errorf("port %s: saying the port may be blocked is %v, want %v: %v", port, got, said, err)
		}
		if !strings.Contains(err.Error(), "RESEND_API_KEY") && said {
			t.Errorf("port %s: the reason does not say what to do instead: %v", port, err)
		}
	}

	// A refusal is not a block: something answered, so the hint would
	// send the reader to the wrong place.
	dialMail = func(addr string) (net.Conn, error) {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	}
	gmailSetUp(t, "587")
	if err := sendMail("someone@example.com", "subject", "body"); err == nil || strings.Contains(err.Error(), "Render") {
		t.Errorf("a refused connection says %v", err)
	}
}

func checkMailLog(t *testing.T) string {
	t.Helper()
	var logged bytes.Buffer
	log.SetOutput(&logged)
	defer log.SetOutput(os.Stderr)
	checkMailConfig()
	return logged.String()
}

func TestMail_StartupSaysHowEmailIsSetUp(t *testing.T) {
	t.Run("nothing set up", func(t *testing.T) {
		gmailSetUp(t, "587")
		t.Setenv("SMTP_HOST", "")
		if said := checkMailLog(t); !strings.Contains(said, "email: not set up") || !strings.Contains(said, supportEmail) {
			t.Errorf("with no email service, the start says:\n%s", said)
		}
	})

	t.Run("Gmail on a network that blocks the email ports", func(t *testing.T) {
		neverAnswers(t)
		gmailSetUp(t, "587")
		said := checkMailLog(t)
		if !strings.Contains(said, "email: WILL NOT SEND") || !strings.Contains(said, blockedHint) {
			t.Errorf("the start says:\n%s", said)
		}
		if strings.Contains(said, "app password") {
			t.Error("the SMTP password is in the log")
		}
	})

	t.Run("an SMTP service that answers", func(t *testing.T) {
		smtpStandIn(t)
		said := checkMailLog(t)
		if !strings.Contains(said, "email: goes out through 127.0.0.1:") || !strings.Contains(said, "answered") {
			t.Errorf("the start says:\n%s", said)
		}
	})

	t.Run("Resend", func(t *testing.T) {
		gmailSetUp(t, "587")
		t.Setenv("RESEND_API_KEY", "re_some_key")
		t.Setenv("MAIL_FROM", "Flexete <no-reply@flexete.com>")
		neverAnswers(t)
		dialed, never := false, dialMail // the SMTP service must not even be tried
		dialMail = func(addr string) (net.Conn, error) { dialed = true; return never(addr) }
		said := checkMailLog(t)
		if !strings.Contains(said, "email: goes out through Resend's web API, from Flexete <no-reply@flexete.com>") ||
			!strings.Contains(said, "SMTP_HOST is set too, and not used") || dialed {
			t.Errorf("the start says (SMTP tried: %v):\n%s", dialed, said)
		}
		if strings.Contains(said, "re_some_key") {
			t.Error("the Resend key is in the log")
		}
	})

	t.Run("a MAIL_FROM that is not an address", func(t *testing.T) {
		gmailSetUp(t, "587")
		t.Setenv("MAIL_FROM", "Flexete team")
		if said := checkMailLog(t); !strings.Contains(said, "email: WILL NOT SEND: MAIL_FROM") {
			t.Errorf("the start says:\n%s", said)
		}
	})
}

func TestMail_MainChecksEmailAtStartup(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := regexp.MustCompile(`(?m)^\s*//.*$`).ReplaceAllString(string(src), "")
	if !regexp.MustCompile(`(?m)^\s*go checkMailConfig\(\)\s*$`).MatchString(code) {
		t.Error("main.go does not run checkMailConfig at startup")
	}
}

func TestMail_ResendRefusalsAreReadOut(t *testing.T) {
	for body, want := range map[string]string{
		`{"statusCode":403,"name":"validation_error","message":"The gmail.com domain is not verified."}`: "validation_error: The gmail.com domain is not verified.",
		`{"message":"API key is invalid"}`: "API key is invalid",
		`upstream connect error`:           "upstream connect error",
		``:                                 "(no reason given)",
		strings.Repeat("é", 400):           strings.Repeat("é", 300) + "…",
	} {
		if got := resendSays([]byte(body)); got != want {
			t.Errorf("resendSays(%.40q) = %.60q, want %.60q", body, got, want)
		}
	}
}
