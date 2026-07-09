package main

// search_ctr.go — click-through learning for search ranking.
//
// The search reranker (search.go) scored on lexical rank, engagement,
// recency, and personalization — but never learned from its OWN
// results. The client has always logged search_result_tap events with
// the query and result position; this module finally consumes them:
//
//	SERVE: SearchHandler logs an impression for each result it returns
//	       (top slice only) into a per-query Redis hash.
//	CLICK: event ingestion routes search_result_tap here; the click is
//	       position-debiased (1/pos^0.7, same propensity family LTR
//	       uses) so a click at rank 8 teaches more than a click at 1.
//	RANK:  rankSearchChallenges/rankSearchAccounts blend a Wilson
//	       lower-bound CTR prior into the score — search self-corrects
//	       toward what people actually pick for each query.
//
// Storage: searchctr:{normalizedQuery} HASH with paired fields
// "c:{key}" (debiased click mass, float) and "i:{key}" (impressions,
// int), where key = "challenge:{id}" | "user:{id}". 30-day TTL
// refreshed on write; per-query field cap so a hot query's hash can't
// grow unbounded. All writes fire-and-forget, all reads fail open —
// house style.

import (
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	searchCTRKeyPrefix = "searchctr:"
	searchCTRTTL       = 30 * 24 * time.Hour
	// searchCTRBoostWeight scales the Wilson-LB CTR into the rerank
	// score. Lexical rank contributes up to 1.0, so 0.25 lets a
	// proven-popular result climb several positions without letting
	// click feedback drown out relevance entirely (self-reinforcement
	// guard: position debiasing already dampens rich-get-richer).
	searchCTRBoostWeight = 0.25
	// searchCTRMaxFields bounds a single query's hash (2 fields per
	// result). Beyond this we stop recording NEW results for the query
	// but keep counting existing ones.
	searchCTRMaxFields = 400
	// searchCTRMinImpressions before a result's CTR means anything —
	// below this the boost is 0 (Wilson would shrink it anyway; this
	// just skips the math and the cold-start noise).
	searchCTRMinImpressions = 5
)

// normalizeSearchQuery canonicalizes a query for the CTR keyspace so
// "Dance ", "dance" and "DANCE" learn together.
func normalizeSearchQuery(q string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(q))), " ")
}

// searchCTRResultKey builds the per-result field suffix. Client
// resultType values are normalized so "account(s)"/"users" land on the
// same key the serve side wrote.
func searchCTRResultKey(contentType, contentID string) string {
	t := strings.ToLower(strings.TrimSpace(contentType))
	switch t {
	case "account", "accounts", "users":
		t = "user"
	case "battle", "short", "challenges":
		t = "challenge"
	}
	return t + ":" + contentID
}

// searchLogImpressions records that these results were SHOWN for the
// query. Call with the final post-rank slice the client will render.
func searchLogImpressions(query string, resultKeys []string) {
	if rdb == nil || len(resultKeys) == 0 {
		return
	}
	nq := normalizeSearchQuery(query)
	if nq == "" {
		return
	}
	rkey := searchCTRKeyPrefix + nq
	// Field-cap guard: one HLEN, then a pipeline. Slightly racy across
	// replicas, but the cap is a memory backstop, not an invariant —
	// at cap we simply stop recording (established CTRs go stale-but-
	// stable rather than the hash growing without bound).
	if n, err := rdb.HLen(rctx, rkey).Result(); err == nil && n >= searchCTRMaxFields {
		return
	}
	pipe := rdb.Pipeline()
	for _, k := range resultKeys {
		pipe.HIncrBy(rctx, rkey, "i:"+k, 1)
	}
	pipe.Expire(rctx, rkey, searchCTRTTL)
	_, _ = pipe.Exec(rctx)
}

// searchObserveClickFromEvent is the ingestion hook for
// search_result_tap events. Metadata carries query + position (the
// client has logged both since the instrumentation pass).
func searchObserveClickFromEvent(e FeedEvent) {
	if rdb == nil || e.ContentID == "" || e.Metadata == nil {
		return
	}
	query, _ := e.Metadata["query"].(string)
	nq := normalizeSearchQuery(query)
	if nq == "" {
		return
	}
	// JSON numbers decode as float64; tolerate string too.
	pos := 1.0
	switch v := e.Metadata["position"].(type) {
	case float64:
		pos = v + 1 // client positions are 0-based
	case string:
		if p, err := strconv.ParseFloat(v, 64); err == nil {
			pos = p + 1
		}
	}
	if pos < 1 {
		pos = 1
	}
	if pos > 50 {
		pos = 50
	}
	// Position debias: a click far down the list is a stronger
	// preference statement than a click on the top result.
	weight := math.Pow(pos, 0.7)

	rkey := searchCTRKeyPrefix + nq
	field := "c:" + searchCTRResultKey(e.ContentType, e.ContentID)
	_ = rdb.HIncrByFloat(rctx, rkey, field, weight).Err()
	_ = rdb.Expire(rctx, rkey, searchCTRTTL).Err()
}

// searchCTRBoosts loads the query's click/impression hash once and
// returns per-result-key score boosts. Empty map on any miss/error —
// callers add boosts[key] unconditionally.
func searchCTRBoosts(query string) map[string]float64 {
	out := map[string]float64{}
	if rdb == nil {
		return out
	}
	nq := normalizeSearchQuery(query)
	if nq == "" {
		return out
	}
	fields, err := rdb.HGetAll(rctx, searchCTRKeyPrefix+nq).Result()
	if err != nil || len(fields) == 0 {
		return out
	}
	for f, v := range fields {
		if !strings.HasPrefix(f, "i:") {
			continue
		}
		key := f[2:]
		imps, _ := strconv.ParseFloat(v, 64)
		if imps < searchCTRMinImpressions {
			continue
		}
		clicks, _ := strconv.ParseFloat(fields["c:"+key], 64)
		if clicks <= 0 {
			continue
		}
		// Debiased click mass can exceed raw impressions for deep
		// clicks; cap the ratio input so Wilson stays in-domain.
		if clicks > imps {
			clicks = imps
		}
		out[key] = wilsonLowerBound(clicks, imps) * searchCTRBoostWeight
	}
	return out
}

// searchIntent classifies a query so the handler can route weight:
// "user" for username-shaped queries that match an account, a
// "category:<name>" hint when the query smells like a content topic,
// else "general". Returned to the client too (section ordering).
func searchIntent(query string, gotAccountHit bool) string {
	q := normalizeSearchQuery(query)
	if q == "" {
		return "general"
	}
	usernameLike := len(q) >= 3 && len(q) <= 20 && !strings.Contains(q, " ")
	if usernameLike {
		ok := true
		for _, r := range q {
			if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' || r == '.') {
				ok = false
				break
			}
		}
		if ok && gotAccountHit {
			return "user"
		}
	}
	if cat := guessSubjectCategory(q); cat != "" && cat != "other" {
		return "category:" + cat
	}
	return "general"
}

// searchIntentCategory extracts the category from a "category:x" intent.
func searchIntentCategory(intent string) string {
	if c, ok := strings.CutPrefix(intent, "category:"); ok {
		return c
	}
	return ""
}
