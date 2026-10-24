# Rate Limiter Design

**What to say early:** This is a very different shape from the last few problems. It's a small component, but it sits on the **critical path of every request**. So the design is governed by three things: **added latency must be tiny (~1 ms), the limiter must never become the reason the system goes down, and counts must be correct enough under concurrency.** The central question is **accuracy vs. latency vs. availability** when state is shared across many servers.

---

## 1. Requirements

**Functional**
- Limit requests per **identity** (user ID, API key, IP, tenant) and per **resource** (endpoint, tier, region)
- Multiple rules per request (e.g., 10/sec per user **and** 1,000/min per IP **and** 100/min for `POST /login`)
- Configurable per customer tier (free vs. paid)
- Return a clear rejection (**HTTP 429**) with `Retry-After` and `X-RateLimit-Limit/Remaining/Reset` headers
- Rules changeable at runtime without a deploy

**Non-functional**
- Adds **< 1-2 ms** to p99 request latency
- **Highly available.** The limiter's failure must not take down the service.
- Accurate enough that limits aren't badly over- or under-enforced
- Works across a distributed fleet (many API servers, multiple regions)
- Memory-efficient (100M+ active keys)

**Clarify with the interviewer:**
- Is this protecting the system (abuse and overload) or enforcing business quotas (billing)? Quotas need accuracy, while abuse protection tolerates approximation.
- Where does it run: client-side, gateway, in each service, or all of them?
- Hard limit (reject) or soft (queue, throttle, degrade)?
- Single region or global? Must the limit be exact globally?
- Fail-open or fail-closed when the limiter breaks?

---

## 2. Estimation (state assumptions)

- 500K requests/sec at peak across the fleet → **500K limit checks/sec**
- 100M distinct active keys (users × rules), each ~100 bytes including overhead → **~10 GB** of state, which fits in memory
- A single Redis node handles ~100K ops/sec → naive central design needs **5+ shards**, so plan 10-20 with headroom
- Latency budget: a Redis round trip in the same AZ is ~0.5 ms. It fits, but it's the largest item in the budget.

**Takeaway to say aloud:** State is small, but the **check rate equals the request rate**, so the limiter's capacity must scale with the whole platform. That motivates **local-first checks** to cut central traffic.

---

## 3. Where does it live? (placement trade-offs)

| Placement | Pros | Cons |
|---|---|---|
| **Client-side** | Free, reduces wasted traffic | Untrusted and easily bypassed. Courtesy only. |
| **Edge / CDN / WAF** | Stops volumetric abuse before it reaches you, IP-level and coarse | Little user/tenant context |
| **API gateway / middleware** (the usual answer) | Central, consistent, sees identity and route, single place to configure | Must be redundant and fast |
| **Sidecar / library in each service** | Fine-grained per-endpoint and downstream protection | Many implementations to keep consistent |
| **Dedicated limiter service** | Reusable, language-agnostic | An extra network hop and a new critical dependency |

**Layered answer to give:** coarse IP and DDoS limits at the edge, identity and tier limits at the gateway, and per-endpoint or per-dependency protection inside services. Each layer catches what the others can't.

**Also distinguish related ideas (interviewers like this):**
- **Rate limiting:** fairness and abuse, per client
- **Load shedding:** global protection when the *system* is overloaded, regardless of who sends
- **Quotas:** long-window business limits (per month), where accuracy and durability matter
- **Concurrency limits:** cap in-flight requests, which is often better for protecting slow backends

---

## 4. API and data model

**Rule config** (stored in DB, cached locally on every limiter):
```
rule: { ruleId, match: {endpoint, method, tier}, keyBy: user|ip|apiKey|tenant,
        algorithm: token_bucket, limit: 100, window: 60s, burst: 20,
        action: reject|throttle, failMode: open|closed, version }
```

**Check call** (internal API or in-process library):
```
check(key, ruleId, cost=1) → { allowed, remaining, retryAfterMs, resetAt }
```

**Rejection response:**
```
HTTP 429 Too Many Requests
Retry-After: 12
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1718000000
```

**State key:** `rl:{ruleId}:{identity}` → small hash or counters, with a **TTL** so idle keys expire automatically (this is what keeps memory bounded).

**Cost parameter:** expensive operations can consume more than one token (a search costs 5, a read costs 1).

---

## 5. High-level architecture

```
Client → CDN/WAF (coarse IP limits, bot filtering)
       → L7 LB → API Gateway
                    │ 1. extract identity (API key / user / IP)
                    │ 2. load matching rules (local cache, hot-reloaded)
                    │ 3. L1: in-process check (deny cache, local token lease)
                    │ 4. L2: central check → Redis cluster (atomic Lua script)
                    │ 5. allowed → forward to backend ; denied → 429 + headers
                    ▼
                 Backend services (can apply their own protective limits)

Control plane:  Rules DB/Config service ──push/poll──► every limiter (versioned, cached)
Observability:  allowed/blocked counters → metrics pipeline → dashboards, alerts, top-blocked keys
```

---

## 6. The core deep dive: algorithms

### Fixed window counter
Count requests per `floor(now / window)`. `INCR` on a key with TTL.
- **Pros:** trivial, tiny memory, fast.
- **Cons:** **boundary burst.** A client can send the full limit at 12:00:59 and again at 12:01:00, getting **2× the limit** in a short span.

### Sliding window log
Store a timestamp per request in a sorted set, drop entries older than the window, and count.
- **Pros:** exact.
- **Cons:** memory is O(limit) **per key** (a 1,000/min limit stores 1,000 entries per user), and cleanup costs CPU. Bad at 100M keys.

### Sliding window counter (the practical compromise)
Keep counts for the current and previous fixed windows and blend:
```
estimate = prevCount × (1 − elapsedFractionOfCurrentWindow) + currCount
```
- **Pros:** two integers per key, smooths the boundary burst. Cloudflare reported this to be very accurate in practice.
- **Cons:** an approximation that assumes the previous window's traffic was evenly spread.

### Token bucket (the most common production choice)
A bucket holds up to `capacity` tokens and refills at `rate` per second. Each request takes tokens, and if there aren't enough it is rejected.
- **Pros:** **allows controlled bursts** (up to capacity) while enforcing the average rate, uses O(1) memory (`tokens`, `lastRefillTs`), and supports variable request cost naturally.
- **Cons:** two parameters to tune (rate and burst), and the burst can surprise people.
- **Trick:** don't run a refill timer per key. **Refill lazily on access:** `tokens = min(cap, tokens + elapsed × rate)`.

### Leaky bucket
Requests enter a queue that drains at a fixed rate, and overflow is rejected.
- **Pros:** **smooth, constant output rate**, good for protecting a backend that hates bursts.
- **Cons:** bursts add latency (queueing), and old requests can starve new ones.

### GCRA (generic cell rate algorithm)
Behaviorally equivalent to a token bucket, but stores a **single timestamp** (the "theoretical arrival time"). It's extremely compact and good for Redis, and it's worth name-dropping.

| Algorithm | Memory/key | Accuracy | Bursts | Best for |
|---|---|---|---|---|
| Fixed window | O(1) | Poor at edges (2× burst) | Uncontrolled at edges | Simple, coarse limits |
| Sliding log | O(limit) | Exact | No | Low limits where exactness matters |
| Sliding window counter | O(1) | Good approximation | Smoothed | General API limits |
| Token bucket | O(1) | Exact for its model | **Controlled** | Most APIs, the default pick |
| Leaky bucket | O(queue) | Exact output rate | Smoothed, adds delay | Traffic shaping |

**State your decision:** "Token bucket for general API limits, because it permits legitimate bursts, costs O(1) memory, and supports weighted costs. Sliding window counter for strict per-window quotas, and fixed window only for coarse low-stakes limits."

---

## 7. The distributed problem

One server with in-memory counters is trivial. The real problem appears when **N gateway instances** each see part of a user's traffic. Options:

### Option A: Local-only counters (limit ÷ N per node)
Each gateway enforces `limit / N` on its own.
- **Pros:** zero network cost, no shared dependency, extremely available.
- **Cons:** only correct if the LB spreads each user's traffic evenly. Uneven traffic wrongly rejects some users while others get more than their share, and any change in N breaks the math.
- **Use it as:** a **fallback or first-line coarse limit**, not the main mechanism.

### Option B: Centralized store (Redis) with atomic operations (the standard answer)
All gateways check one source of truth, sharded by key.
- **Pros:** accurate and simple to reason about.
- **Cons:** an extra network hop per request, and the store is a critical dependency and a throughput bottleneck.

**The race condition (a classic probe):** a naive `GET count → check → SET count+1` lets two gateways read the same value and both allow, so you over-admit. Fixes:
- **Atomic increment** (`INCR` is atomic, but `INCR` followed by `EXPIRE` isn't, so a crash between them leaves a key with no TTL forever. Use a Lua script or `SET NX EX` first.)
- **Lua script** that runs the whole read-compute-write atomically on the Redis shard:

```lua
-- KEYS[1]=bucket key ; ARGV: capacity, refillPerSec, cost, ttlMs
local t = redis.call('TIME')
local now = t[1]*1000 + math.floor(t[2]/1000)          -- use Redis clock, not app server clocks
local d = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local cap, rate, cost = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local tokens = tonumber(d[1]) or cap
local ts = tonumber(d[2]) or now
tokens = math.min(cap, tokens + (now - ts) / 1000 * rate)   -- lazy refill
local allowed = 0
if tokens >= cost then tokens = tokens - cost; allowed = 1 end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], ARGV[4])                      -- idle keys expire
local retry = 0
if allowed == 0 then retry = math.ceil((cost - tokens) / rate * 1000) end
return {allowed, math.floor(tokens), retry}
```
One round trip, atomic, and the **clock comes from Redis**, so skew between gateways doesn't matter.

**Sharding:** hash the key (consistent hashing or Redis Cluster slots) so each identity's state lives on exactly one shard. This is what makes a single-key check atomic without distributed transactions.

### Option C: Hybrid, local-first with periodic sync (what scales best)
Cut Redis traffic by doing most decisions locally:
- **Token leasing / batching:** a gateway requests a **batch of tokens** (say 10% of the remaining budget, or a few seconds' worth) from Redis and serves requests locally until the lease is used up.
- **Local deny cache:** once a key is over limit, remember "denied until reset time T" locally, so **a flood from an abusive client never reaches Redis.** This protects the limiter exactly when it's under attack.
- **Async counter sync:** count locally, flush deltas to Redis every ~100 ms to 1 s.
- **Trade-offs:** you accept **bounded inaccuracy** (a key can overshoot by roughly `numGateways × leaseSize`) and slower convergence, in exchange for one to two orders of magnitude fewer Redis calls and much better latency and availability. For abuse protection that's an excellent deal. For billing quotas it isn't, so use exact counting there.

### Option D: Dedicated limiter cluster with consistent routing
Route each key to **one limiter node** (via consistent hashing at the LB or client), which holds counters in memory and replicates asynchronously.
- **Pros:** no shared store, exact counts per key, very fast.
- **Cons:** you've built a stateful sharded service. Node failure loses recent counts (acceptable for abuse limits), and rebalancing is non-trivial.

**State your decision:** "Redis cluster with atomic Lua token buckets as the source of truth, with a local deny cache and optional token leasing in front for hot keys. That keeps the common path off the network and protects Redis during attacks."

---

## 8. Request path, hop by hop

### 1. Edge / CDN / WAF
- Coarse **per-IP** limits and bot detection absorb volumetric abuse before it consumes your capacity.
- **Trade-off:** IP is a weak identity. NAT and corporate proxies put thousands of users behind one IP, so keep the thresholds generous. Mobile carriers' CGNAT make this worse. Attackers can also rotate IPs, so IP limiting alone is never enough.

### 2. Gateway: identify the caller
- Order of preference for the key: **authenticated user or API key**, then tenant, then IP (the fallback for anonymous traffic).
- **Rate limit unauthenticated endpoints more strictly**, especially `/login`, signup, password reset, and OTP. These attract credential stuffing, so also limit by **target account**, not just source IP.
- **Trade-off:** authenticating before limiting means the limiter sits behind an auth cost, so put a cheap IP-level limit *in front of* auth to protect it.

### 3. Rule matching (local, in-memory)
- Rules are cached in each gateway and **hot-reloaded** via versioned config push or polling. A request never waits on the config DB.
- Match on the most specific rule, or evaluate several and require **all** to pass.
- **Multi-rule check:** evaluate the cheap local rules first, and only then touch Redis. For multiple Redis rules, **pipeline them in one round trip** (and on a rejection from a later rule, don't leave earlier rules' tokens consumed, or use a refund path).

### 4. The check (L1 then L2)
1. **L1:** local deny cache. If the key is known to be blocked until T, reject immediately.
2. **L2:** Redis Lua script (or local lease) returns `allowed, remaining, retryAfter`.
3. **Tight timeout** (e.g., 5-10 ms) on the Redis call, because the limiter must never become a latency problem.
4. On Redis timeout or error: apply the rule's **fail mode** (next section).

### 5. Response
- **Allowed:** forward, and attach `X-RateLimit-Remaining` for client visibility.
- **Denied:** `429` with `Retry-After`. Choose how to treat rejected requests: **don't consume tokens on rejection**, or a client retrying aggressively digs itself deeper.
- **Alternatives to hard rejection:** queue and delay (throttle), degrade (serve cached or lower-quality results), or deprioritize. **Trade-off:** rejection is cheap and predictable, while queueing is friendlier but holds resources and can amplify overload.

### 6. Client behavior (part of the system)
- Clients must honor `Retry-After` and use **exponential backoff with jitter.** Without jitter, every throttled client retries at the same instant and creates a synchronized retry storm.
- Publish limits in docs and headers so well-behaved clients self-regulate.

---

## 9. Reliability: the limiter must not be the outage

### Fail-open vs. fail-closed (the key decision)
When Redis or the limiter is unreachable:
- **Fail-open:** allow the request. Protects availability, but you're unprotected during the failure, and an attacker who can trigger the failure gets free rein.
- **Fail-closed:** reject. Protects the backend but turns a limiter outage into a **full outage.**

**Typical answer: it's per-rule.**
- **Default: fail-open**, usually with a **degraded local fallback** (in-memory per-node limit, generous, e.g. `limit × 2 / N`) so you're approximately protected rather than not at all.
- **Fail-closed or strict-local** for **security-critical or expensive** endpoints (login, OTP/SMS, payments, costly ML calls), where an unlimited flood causes real harm.
- **Circuit breaker** around Redis calls so a sick Redis fails fast instead of piling up threads and slowing every request.

### Redis failure handling
- Primary + replica per shard with automatic failover. **Async replication means a failover can lose a few recent counts**, which briefly lets clients exceed limits. That's acceptable for abuse protection, and not for billing, which should use a durable counter path.
- **Don't put the limiter in the same failure domain** as your main database or cache. Dedicated clusters, so a cache-eviction storm can't evict rate limit keys.
- **Hot key protection:** one huge tenant or attacker can saturate a single shard. The local deny cache, token leasing, and splitting that key into sub-buckets (`key#0..N-1`, each with a fraction of the limit) all help.
- **Cold start after loss:** with empty state, everyone has a full bucket, which is a free burst. That is usually fine, but note that a deliberate Redis flush is a temporary amnesty.

### Avoid making things worse
- **Limiter timeouts must be shorter than request timeouts**, or you queue work behind a limiter that is itself stalled.
- **Config safety:** a bad rule push (limit = 0) can take down production. Use **versioned, validated, staged rollouts** with automatic rollback, and keep a **last-known-good** rule set cached locally.
- **Shadow mode (dry-run):** new rules log "would have blocked" without enforcing, so you can see their impact before turning them on. Ramp enforcement gradually.

---

## 10. Multi-region

- **Per-region limits** (each region gets a share or enforces independently): simple, fast, and available, but a user hitting multiple regions can exceed the global limit in total.
- **Global limit with cross-region sync:** accurate, but adds cross-region latency (~100 ms) to the hot path. Sync **asynchronously** (gossip or periodic delta exchange) and accept bounded overshoot.
- **Home-region ownership:** route each key's authoritative counter to one region and use local approximations elsewhere.
- **Trade-off:** global exactness versus latency and availability. For most APIs, **per-region enforcement with a global budget split** (e.g., 70/30 by traffic share, rebalanced periodically) is good enough. Strict global quotas (billing) use a centralized or periodically reconciled counter.

---

## 11. Observability and operations

- **Metrics:** allowed vs. blocked per rule, p99 limiter latency, Redis latency and error rate, fail-open event count, top blocked keys, shard hot-spots.
- **Alerts:** sudden spikes in blocks (an attack, *or* a bad rule blocking legitimate traffic), any use of fail-open, and limiter latency above budget.
- **Audit log** of rule changes, and the ability to **override** a key (allowlist a partner, blocklist an abuser) instantly.
- **Testing:** load test limiter capacity at 2× peak, and chaos test (kill a Redis shard, add latency, flush state) to verify fail modes behave as designed.

---

## 12. Failure scenarios

| Failure | Impact | Mitigation |
|---|---|---|
| Redis shard down | Limit checks fail for its keys | Replica failover, circuit breaker, **fail-open with local fallback** (strict for sensitive rules) |
| Redis slow (not down) | Added latency on every request | Tight timeout, circuit breaker, serve from L1 and local leases |
| All of Redis lost | Counters reset, no shared state | Fall back to local limits, rebuild naturally from traffic, accept a temporary burst |
| Race between gateways | Over-admission | Atomic Lua script on the owning shard |
| Hot key (abusive client or giant tenant) | Shard saturation | Local deny cache, key splitting, token leasing, edge blocking |
| Clock skew across gateways | Wrong refill math | Use Redis `TIME` inside the script, not app clocks |
| Bad rule pushed (limit too low) | Legitimate users blocked | Validation, staged rollout, shadow mode, auto-rollback, last-known-good |
| Gateway fleet scaled up or down | Local-only limits mis-sized | Don't rely on `limit ÷ N` as the primary control. Use central counters. |
| Retry storm after mass 429s | Synchronized waves of traffic | `Retry-After` plus client jittered backoff, token bucket smoothing |
| DDoS from rotating IPs | IP limits ineffective | Edge WAF and bot scoring, per-account limits, challenges (CAPTCHA), load shedding |
| Distributed attack under per-IP limits | Many IPs each stay under the limit | Aggregate limits per target resource or account, anomaly detection |
| Redis failover loses recent counts | Short-lived over-admission | Acceptable for abuse limits; use durable counters for quotas |
| Limiter itself overloaded | Becomes the outage | Autoscale, deny cache, shed load, fail-open by rule |

**Principle (static stability):** the limiter should degrade to *"less precise"* rather than *"unavailable."* The request path must keep working if the central store, the config service, and the metrics pipeline are all down, using cached rules and local fallback limits.

---

## 13. Consistency decisions

- **Per-key atomicity:** strong, enforced by single-shard Lua execution.
- **Cross-key and cross-region accuracy:** eventual, with bounded overshoot, accepted explicitly.
- **Abuse limits:** approximate is fine. Favor latency and availability.
- **Billing quotas:** exact and durable. Use a separate, strongly consistent counter path, since limits and billing have different correctness needs.
- **Rule updates:** eventually consistent across gateways (seconds). Tolerate a short window where old and new rules coexist.

---

## 14. Scaling summary

| Component | Strategy |
|---|---|
| Gateways | Stateless apart from local caches, autoscaled, multi-AZ |
| Rule config | Central store, versioned push/poll, local in-memory cache with last-known-good |
| Counter store | Redis cluster sharded by key, replicas, dedicated from other caches |
| Hot path | L1 deny cache and token leasing cut Redis calls dramatically |
| Atomicity | Lua scripts, one round trip per check (pipelined for multiple rules) |
| Multi-region | Per-region enforcement with global budget split, async sync |
| Edge | WAF and CDN for volumetric and per-IP protection |

---

## 15. Likely follow-up questions

- **"Why token bucket over fixed window?"** Fixed window allows a 2× boundary burst and has no controlled burst concept. Token bucket gives O(1) memory, an explicit burst knob, and weighted costs.
- **"How do you handle the race condition across servers?"** Atomic read-modify-write in Redis via Lua, keyed so each identity maps to one shard, with Redis's clock as the time source.
- **"What if Redis goes down?"** Circuit breaker plus per-rule fail mode. Default fail-open with a local approximate fallback, and fail-closed or strict for sensitive endpoints.
- **"How do you scale to 10× traffic?"** More Redis shards, but mainly reduce calls with a local deny cache, token leasing, and batched sync. Accept bounded inaccuracy.
- **"How do you rate limit per user across multiple data centers?"** Per-region enforcement with a budget split, or async sync with bounded overshoot. Use a home-region counter for strict quotas.
- **"How do you prevent a distributed attack?"** Per-IP limits aren't enough. Add per-account and per-resource limits, edge bot detection, anomaly detection, and challenge flows.
- **"How would you rate limit a login endpoint?"** Limit by both IP and target account, with exponential lockout and CAPTCHA, fail-closed, and avoid revealing whether the account exists.
- **"How do you roll out a new limit safely?"** Shadow mode first, then a staged percentage rollout with metrics on blocked legitimate traffic, and instant rollback.
- **"Throttle vs. reject?"** Reject is cheap and predictable. Throttle (queue) is friendlier but holds resources and can amplify overload, so bound the queue.
- **"How does this differ from load shedding?"** The rate limiter enforces per-client fairness and quotas. Load shedding drops traffic based on **system health** (CPU, queue depth), prioritizing critical requests, regardless of which client sent them. You want both.
- **"Where do you put the limiter, client, gateway or service?"** Layered: edge for coarse IP, gateway for identity and tier, and in-service for protecting specific dependencies. Never rely on the client.

---

## 16. How to narrate it

1. **Name the dominant problem:** a latency-critical, always-on component whose failure must not cascade. Decisions revolve around where state lives and what happens when it's unavailable.
2. **Start naive:** an in-memory counter on one server. Show it breaks with multiple servers (uneven load, race conditions), then introduce a central Redis store, then atomic Lua, and then explain why you add local-first optimizations (cost and availability).
3. **Pick the algorithm and justify it:** token bucket, with the sliding window counter as the alternative, and the trade-offs of each.
4. **State the contract:** per-key atomic, bounded inaccuracy across nodes, fail-open by default and fail-closed where harm is high.
5. **Call out the knobs:** limit, burst, window, lease size, Redis timeout, fail mode per rule, and shadow-mode ramp.
6. **For each component, say what happens when it fails,** and end with "the limiter can degrade to approximate, but it can never take the service down."

**Patterns reused from our toolbox:** atomic operations and idempotency, consistent hashing and sharding, hot-key mitigation, TTL-based state expiry, local caching in front of a shared store, circuit breakers and timeouts, fail-open vs. fail-closed, graceful degradation, jittered backoff, thundering-herd control, and static stability.

Next, I can play interviewer on this (for example, "Redis just failed over and your gateways are all timing out on limiter calls, walk me through it"), or we can move on. **Uber/proximity** is a good contrast, since it adds geospatial indexing and real-time matching. **Web crawler** and **notification system** are also good picks that lean heavily on queues, rate limiting, and politeness.
