package main

// mailer.go — sending email, through any SMTP service.
//
// Set these on the server (Render → the service → Environment):
//
//	SMTP_HOST      the service's SMTP server, e.g. smtp-relay.brevo.com
//	SMTP_PORT      587 (the usual) or 465. 587 when unset.
//	SMTP_USERNAME  the login the service gives you
//	SMTP_PASSWORD  its password or key
//	MAIL_FROM      who the email is from, e.g. Flex <no-reply@yourdomain.com>
//
// Without SMTP_HOST and MAIL_FROM nothing is sent, and what needs email
// (forgot password) says so to the person instead of pretending to work.

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/mail"
	"net/smtp"
	"os"
	"strings"
	"time"
)

// supportEmail is where people write for help. Kept in one place, so it
// changes in one place.
const supportEmail = "support@devf.app"

type mailSettings struct {
	host, port, username, password, from string
}

// mailFromEnv reads the settings above, and whether there are enough of
// them to send anything.
func mailFromEnv() (mailSettings, bool) {
	s := mailSettings{
		host:     strings.TrimSpace(os.Getenv("SMTP_HOST")),
		port:     strings.TrimSpace(os.Getenv("SMTP_PORT")),
		username: strings.TrimSpace(os.Getenv("SMTP_USERNAME")),
		password: os.Getenv("SMTP_PASSWORD"),
		from:     strings.TrimSpace(os.Getenv("MAIL_FROM")),
	}
	if s.port == "" {
		s.port = "587"
	}
	return s, s.host != "" && s.from != ""
}

// mailConfigured is whether email can be sent at all.
func mailConfigured() bool {
	_, ok := mailFromEnv()
	return ok
}

// sendMail sends a plain-text email to [to]. Replies go to supportEmail.
func sendMail(to, subject, body string) error {
	s, ok := mailFromEnv()
	if !ok {
		return errors.New("email is not set up: SMTP_HOST and MAIL_FROM are needed")
	}
	from, err := mail.ParseAddress(s.from)
	if err != nil {
		return fmt.Errorf("MAIL_FROM %q is not an email address: %w", s.from, err)
	}
	rcpt, err := mail.ParseAddress(to)
	if err != nil {
		return fmt.Errorf("%q is not an email address: %w", to, err)
	}

	addr := net.JoinHostPort(s.host, s.port)
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	var conn net.Conn
	if s.port == "465" {
		// Port 465 speaks TLS from the first byte.
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, &tls.Config{ServerName: s.host})
	} else {
		conn, err = dialer.Dial("tcp", addr)
	}
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", addr, err)
	}
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	c, err := smtp.NewClient(conn, s.host)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("talking to %s: %w", addr, err)
	}
	defer c.Close()
	if ok, _ := c.Extension("STARTTLS"); ok && s.port != "465" {
		if err := c.StartTLS(&tls.Config{ServerName: s.host}); err != nil {
			return fmt.Errorf("securing the connection to %s: %w", addr, err)
		}
	}
	if s.username != "" {
		if err := c.Auth(smtp.PlainAuth("", s.username, s.password, s.host)); err != nil {
			return fmt.Errorf("signing in to the email service: %w", err)
		}
	}
	if err := c.Mail(from.Address); err != nil {
		return fmt.Errorf("the email service refused the sender %s: %w", from.Address, err)
	}
	if err := c.Rcpt(rcpt.Address); err != nil {
		return fmt.Errorf("the email service refused the address %s: %w", rcpt.Address, err)
	}
	w, err := c.Data()
	if err != nil {
		return fmt.Errorf("starting the email: %w", err)
	}
	if _, err := w.Write(mailMessage(from, rcpt, subject, body)); err != nil {
		return fmt.Errorf("writing the email: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("the email service did not take the email: %w", err)
	}
	return c.Quit()
}

// mailMessage is the email itself: its headers, then [body] as plain text.
func mailMessage(from, to *mail.Address, subject, body string) []byte {
	domain := "localhost"
	if at := strings.LastIndex(from.Address, "@"); at >= 0 {
		domain = from.Address[at+1:]
	}
	id := make([]byte, 12)
	_, _ = rand.Read(id)
	var b strings.Builder
	header := func(k, v string) { b.WriteString(k + ": " + v + "\r\n") }
	header("From", from.String())
	header("To", to.String())
	header("Reply-To", supportEmail)
	header("Subject", mime.QEncoding.Encode("utf-8", subject))
	header("Date", time.Now().Format(time.RFC1123Z))
	header("Message-ID", "<"+hex.EncodeToString(id)+"@"+domain+">")
	header("MIME-Version", "1.0")
	header("Content-Type", "text/plain; charset=UTF-8")
	header("Content-Transfer-Encoding", "8bit")
	b.WriteString("\r\n")
	b.WriteString(strings.ReplaceAll(strings.ReplaceAll(body, "\r\n", "\n"), "\n", "\r\n"))
	return []byte(b.String())
}

// maskEmail shows enough of [email] for its owner to recognise it, and not
// enough for anybody else to use it: k•••n@gmail.com.
func maskEmail(email string) string {
	at := strings.LastIndex(email, "@")
	if at < 1 {
		return "your email"
	}
	local := []rune(email[:at])
	if len(local) <= 2 {
		return string(local[0]) + "•••" + email[at:]
	}
	return string(local[0]) + "•••" + string(local[len(local)-1]) + email[at:]
}
