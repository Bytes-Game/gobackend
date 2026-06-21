// Package main is a native Go load generator for the devb feed endpoint.
//
// Usage:
//
//	go run ./loadtest -base=https://api.example.com -users=1000 -rps=500 -duration=60s
//
// It ramps concurrent virtual users up to -users, each running a tight loop of
// feed-smart / event / feed-smart calls at the requested total RPS. Prints a
// running p50/p95/p99 every 5 s and a final summary.
//
// Designed for pre-prod load testing — verifies the stack handles the target
// concurrency without touching Redis/Postgres directly. Measures what the user
// actually experiences: HTTP latency and error rate.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
)

type config struct {
	base      string
	users     int
	rps       int
	duration  time.Duration
	endpoints []string
}

type result struct {
	latencyMs float64
	status    int
	err       error
}

type agg struct {
	mu      sync.Mutex
	samples []float64
	ok      int64
	fail    int64
	byCode  map[int]int64
}

func newAgg() *agg {
	return &agg{byCode: make(map[int]int64)}
}

func (a *agg) add(r result) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.samples = append(a.samples, r.latencyMs)
	a.byCode[r.status]++
	if r.err == nil && r.status >= 200 && r.status < 400 {
		a.ok++
	} else {
		a.fail++
	}
}

func (a *agg) snapshot() (int64, int64, float64, float64, float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := len(a.samples)
	if n == 0 {
		return a.ok, a.fail, 0, 0, 0
	}
	cp := make([]float64, n)
	copy(cp, a.samples)
	sort.Float64s(cp)
	p := func(pct float64) float64 {
		idx := int(float64(n-1) * pct)
		return cp[idx]
	}
	return a.ok, a.fail, p(0.50), p(0.95), p(0.99)
}

func main() {
	base := flag.String("base", "http://localhost:8081", "Backend base URL")
	users := flag.Int("users", 100, "Concurrent virtual users")
	rps := flag.Int("rps", 200, "Total requests per second")
	duration := flag.Duration("duration", 30*time.Second, "Test duration")
	endpointsCSV := flag.String("endpoints", "/api/v1/feed/smart", "Comma-separated paths to hit")
	flag.Parse()

	cfg := config{
		base:      strings.TrimRight(*base, "/"),
		users:     *users,
		rps:       *rps,
		duration:  *duration,
		endpoints: strings.Split(*endpointsCSV, ","),
	}

	// Protected endpoints now require a session token. We mint one per virtual
	// user locally (signed with the same JWT_SECRET the server validates with)
	// so the load profile keeps its per-user distribution without funnelling
	// every worker through /login first.
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		fmt.Println("⚠️  JWT_SECRET not set — minted tokens won't validate and every " +
			"request will 401. Set JWT_SECRET to the value the target backend uses.")
	}

	fmt.Printf("── devb loadtest ── base=%s users=%d rps=%d duration=%s endpoints=%v\n\n",
		cfg.base, cfg.users, cfg.rps, cfg.duration, cfg.endpoints)

	a := newAgg()

	// One shared ticker paces the total RPS; workers pull work from a channel.
	work := make(chan string, cfg.rps)
	done := make(chan struct{})

	// Producer: one tick per request.
	go func() {
		interval := time.Second / time.Duration(cfg.rps)
		if interval < time.Millisecond {
			interval = time.Millisecond
		}
		tk := time.NewTicker(interval)
		defer tk.Stop()
		stop := time.After(cfg.duration)
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			select {
			case <-tk.C:
				ep := cfg.endpoints[rnd.Intn(len(cfg.endpoints))]
				select {
				case work <- ep:
				default: // queue full → count as a drop (workers saturated)
					atomic.AddInt64(&dropped, 1)
				}
			case <-stop:
				close(done)
				return
			}
		}
	}()

	// Workers.
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: cfg.users,
			MaxConnsPerHost:     cfg.users * 2,
		},
	}
	var wg sync.WaitGroup
	for i := 0; i < cfg.users; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			userID := fmt.Sprintf("lt_user_%d", idx)
			token := mintToken(userID, secret)
			for {
				select {
				case ep, ok := <-work:
					if !ok {
						return
					}
					a.add(hit(client, cfg.base, ep, userID, token))
				case <-done:
					// Drain remaining work in the channel so we don't lose observations.
					for {
						select {
						case ep := <-work:
							a.add(hit(client, cfg.base, ep, userID, token))
						default:
							return
						}
					}
				}
			}
		}(i)
	}

	// Live progress — every 5 s.
	progressTicker := time.NewTicker(5 * time.Second)
	defer progressTicker.Stop()

progressLoop:
	for {
		select {
		case <-progressTicker.C:
			ok, fail, p50, p95, p99 := a.snapshot()
			fmt.Printf("  t+%-4s  ok=%d fail=%d drop=%d  p50=%.0fms p95=%.0fms p99=%.0fms\n",
				time.Since(start).Round(time.Second), ok, fail, atomic.LoadInt64(&dropped), p50, p95, p99)
		case <-done:
			break progressLoop
		}
	}
	wg.Wait()
	close(work)

	// Final summary
	ok, fail, p50, p95, p99 := a.snapshot()
	total := ok + fail
	fmt.Println()
	fmt.Println("── summary ──")
	fmt.Printf("total requests : %d\n", total)
	fmt.Printf("  ok  (2xx/3xx): %d (%.2f%%)\n", ok, pct(ok, total))
	fmt.Printf("  fail         : %d (%.2f%%)\n", fail, pct(fail, total))
	fmt.Printf("  dropped      : %d\n", atomic.LoadInt64(&dropped))
	fmt.Printf("latency p50    : %.0f ms\n", p50)
	fmt.Printf("latency p95    : %.0f ms\n", p95)
	fmt.Printf("latency p99    : %.0f ms\n", p99)
	fmt.Printf("requested rps  : %d\n", cfg.rps)
	if cfg.duration.Seconds() > 0 {
		fmt.Printf("actual rps     : %.1f\n", float64(total)/cfg.duration.Seconds())
	}
	fmt.Println("by status:")
	for code, n := range a.byCode {
		fmt.Printf("  %d : %d\n", code, n)
	}

	if fail > ok/5 {
		fmt.Println("\n❌ more than 20% failures — backend may be under-provisioned or broken")
		os.Exit(1)
	}
}

var (
	start   = time.Now()
	dropped int64
)

// mintToken builds an HS256 session token for a virtual user, signed with the
// same secret the backend validates against. Mirrors the server's issueToken so
// loadtest can authenticate without a /login round-trip per user.
func mintToken(userID, secret string) string {
	claims := jwt.RegisteredClaims{
		Subject:   userID,
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(2 * time.Hour)),
	}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
	if err != nil {
		return ""
	}
	return tok
}

func hit(client *http.Client, base, path, userID, token string) result {
	url := base + path
	if !strings.Contains(path, "?") {
		url += "?userId=" + userID
	} else {
		url += "&userId=" + userID
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return result{latencyMs: 0, status: 0, err: err}
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	t0 := time.Now()
	resp, err := client.Do(req)
	dur := time.Since(t0).Seconds() * 1000
	if err != nil {
		return result{latencyMs: dur, status: 0, err: err}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return result{latencyMs: dur, status: resp.StatusCode}
}

func pct(n, total int64) float64 {
	if total == 0 {
		return 0
	}
	return 100 * float64(n) / float64(total)
}

var _ = json.Unmarshal // keep import available for future POST-body work
