package main

// mailer.go — sending email: over the web with Resend, or through any SMTP
// service.
//
// Set these on the server (Render → the service → Environment). Either:
//
//	RESEND_API_KEY  a key from resend.com → API Keys. Email then goes out
//	                over the web (HTTPS), which Render's free plan allows.
//
// or, through an SMTP service such as Gmail. Not on Render's free plan:
// since September 2025 it blocks the ports email goes out on (25, 465 and
// 587), so there the service simply never answers.
//
//	SMTP_HOST      the service's SMTP server, e.g. smtp.gmail.com
//	SMTP_PORT      587 (the usual) or 465. 587 when unset.
//	SMTP_USERNAME  the login the service gives you
//	SMTP_PASSWORD  its password or key
//
// and with either:
//
//	MAIL_FROM      who the email is from, e.g. Flexete <no-reply@flexete.com>.
//	               With Resend, on a domain added and checked at resend.com.
//
// When both are set, Resend is used. Without MAIL_FROM and one of the two,
// nothing is sent, and what needs email (forgot password) says so to the
// person instead of pretending to work. At every start the server says in
// its log how email is set up, and whether it can get out (checkMailConfig).

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"net/mail"
	"net/smtp"
	"os"
	"strings"
	"time"
)

// supportEmail is where people write for help. Kept in one place, so it
// changes in one place.
const supportEmail = "support@flexete.com"

type mailSettings struct {
	resendKey                            string
	host, port, username, password, from string
}

// mailFromEnv reads the settings above, and whether there are enough of
// them to send anything.
func mailFromEnv() (mailSettings, bool) {
	s := mailSettings{
		resendKey: strings.TrimSpace(os.Getenv("RESEND_API_KEY")),
		host:      strings.TrimSpace(os.Getenv("SMTP_HOST")),
		port:      strings.TrimSpace(os.Getenv("SMTP_PORT")),
		username:  strings.TrimSpace(os.Getenv("SMTP_USERNAME")),
		password:  os.Getenv("SMTP_PASSWORD"),
		from:      strings.TrimSpace(os.Getenv("MAIL_FROM")),
	}
	if s.port == "" {
		s.port = "587"
	}
	return s, s.from != "" && (s.resendKey != "" || s.host != "")
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
		return errors.New("email is not set up: MAIL_FROM, and RESEND_API_KEY or SMTP_HOST, are needed")
	}
	from, err := mail.ParseAddress(s.from)
	if err != nil {
		return fmt.Errorf("MAIL_FROM %q is not an email address: %w", s.from, err)
	}
	rcpt, err := mail.ParseAddress(to)
	if err != nil {
		return fmt.Errorf("%q is not an email address: %w", to, err)
	}
	if s.resendKey != "" {
		return sendWithResend(s, rcpt, subject, body)
	}

	c, err := openSMTP(s)
	if err != nil {
		return err
	}
	defer c.Close()
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

// dialMail opens the connection to an SMTP service, and gives up after ten
// seconds: a blocked email port does not refuse, it just never answers. A
// test stands in for that.
var dialMail = func(addr string) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, 10*time.Second)
}

// openSMTP connects to the SMTP service and signs in: everything before the
// email itself. checkMailConfig uses it at startup, to find out whether
// email can get out at all.
func openSMTP(s mailSettings) (*smtp.Client, error) {
	addr := net.JoinHostPort(s.host, s.port)
	conn, err := dialMail(addr)
	if err != nil {
		return nil, fmt.Errorf("connecting to %s: %w%s", addr, err, blockedPortHint(s.port, err))
	}
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	if s.port == "465" {
		// Port 465 speaks TLS from the first byte.
		secure := tls.Client(conn, &tls.Config{ServerName: s.host})
		if err := secure.Handshake(); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("securing the connection to %s: %w", addr, err)
		}
		conn = secure
	}
	c, err := smtp.NewClient(conn, s.host)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("talking to %s: %w", addr, err)
	}
	if ok, _ := c.Extension("STARTTLS"); ok && s.port != "465" {
		if err := c.StartTLS(&tls.Config{ServerName: s.host}); err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("securing the connection to %s: %w", addr, err)
		}
	}
	if s.username != "" {
		if err := c.Auth(smtp.PlainAuth("", s.username, s.password, s.host)); err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("signing in to the email service: %w", err)
		}
	}
	return c, nil
}

// blockedPortHint is added when an SMTP service never answered on a port
// email goes out on. Render's free plan blocks exactly those, and without
// refusing: the connection waits until it gives up. A Gmail account set up
// perfectly fails this way, so the likely reason has to be said, or the
// settings take the blame.
func blockedPortHint(port string, err error) string {
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		return ""
	}
	switch port {
	case "25", "465", "587":
		return ". Nothing answered on port " + port + ". If this server is on Render's free plan, " +
			"that plan blocks the email ports 25, 465 and 587: set RESEND_API_KEY to send email " +
			"over the web instead (see mailer.go), or move the service to a paid plan"
	}
	return ""
}

// resendURL is where Resend takes an email
// (resend.com/docs/api-reference/emails/send-email). A test points it at a
// stand-in.
var resendURL = "https://api.resend.com/emails"

// resendClient waits up to 15 seconds for Resend.
var resendClient = &http.Client{Timeout: 15 * time.Second}

// sendWithResend sends a plain-text email through Resend's web API. It goes
// out over HTTPS, like everything else the server fetches, so no email port
// is involved.
func sendWithResend(s mailSettings, to *mail.Address, subject, body string) error {
	payload, err := json.Marshal(map[string]any{
		"from":     s.from,
		"to":       []string{to.Address},
		"subject":  subject,
		"text":     body,
		"reply_to": supportEmail,
	})
	if err != nil {
		return fmt.Errorf("writing the email: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, resendURL, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("writing the email: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+s.resendKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "flexete-server")
	res, err := resendClient.Do(req)
	if err != nil {
		return fmt.Errorf("reaching the email service (Resend): %w", err)
	}
	defer res.Body.Close()
	said, _ := io.ReadAll(io.LimitReader(res.Body, 4<<10))
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return fmt.Errorf("the email service (Resend) did not take the email: %d %s",
			res.StatusCode, resendSays(said))
	}
	return nil
}

// resendSays is the reason in one of Resend's refusals: its name and message
// when it comes as JSON, for example "validation_error: The gmail.com domain
// is not verified...", and otherwise the answer as it came.
func resendSays(body []byte) string {
	var refusal struct{ Name, Message string }
	if json.Unmarshal(body, &refusal) == nil && refusal.Message != "" {
		if refusal.Name != "" {
			return refusal.Name + ": " + refusal.Message
		}
		return refusal.Message
	}
	said := strings.TrimSpace(string(body))
	if said == "" {
		return "(no reason given)"
	}
	if r := []rune(said); len(r) > 300 {
		said = string(r[:300]) + "…"
	}
	return said
}

// checkMailConfig says at startup how email is set up and, for an SMTP
// service, whether it answers and takes the sign-in. Otherwise a setup that
// cannot work shows in one place only: the forgot-password screen, to
// somebody locked out of their account. main runs it in the background,
// since a service that never answers takes ten seconds to give up on.
func checkMailConfig() {
	s, ok := mailFromEnv()
	if !ok {
		log.Printf("email: not set up, so \"Forgot password?\" asks people to write to %s "+
			"instead. It needs MAIL_FROM, and RESEND_API_KEY or SMTP_HOST (see mailer.go).", supportEmail)
		return
	}
	if _, err := mail.ParseAddress(s.from); err != nil {
		log.Printf("email: WILL NOT SEND: MAIL_FROM %q is not an email address (%v). "+
			"Write it like Flexete <no-reply@flexete.com>.", s.from, err)
		return
	}
	if s.resendKey != "" {
		also := ""
		if s.host != "" {
			also = " SMTP_HOST is set too, and not used."
		}
		log.Printf("email: goes out through Resend's web API, from %s.%s", s.from, also)
		return
	}
	addr := net.JoinHostPort(s.host, s.port)
	c, err := openSMTP(s)
	if err != nil {
		log.Printf("email: WILL NOT SEND, so no reset code reaches anybody: %v", err)
		return
	}
	_ = c.Quit()
	log.Printf("email: goes out through %s, from %s. The email service answered and took the sign-in.",
		addr, s.from)
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
