# Preparing for the URL Shortener Design Interview

Since this is meant to be a conversation, the goal is to drive it: state assumptions, propose a design, name the trade-offs, and invite pushback.

## Suggested 60-minute flow

| Time | Phase |
|---|---|
| 0-8 min | Clarify requirements and scope |
| 8-14 min | Back-of-envelope estimates |
| 14-24 min | API and data model |
| 24-40 min | High-level design and deep dive (key generation, storage, caching) |
| 40-55 min | Scaling, reliability, and trade-offs |
| 55-60 min | Wrap-up and extensions |

## 1. Requirements

**Functional:** create a short URL from a long one, redirect short to long, optional custom aliases, optional expiry, optional click analytics.

**Non-functional:** very high availability for redirects, low latency (tens of ms), links should not be guessable or enumerable, and the system is heavily read-skewed.

Ask the interviewer about scale, whether links expire, and whether analytics are in scope.

## 2. Estimation (state assumptions out loud)

- 100M new URLs/month → ~40 writes/sec
- 100:1 read/write ratio → ~4,000 redirects/sec average, maybe 10x at peak
- 5-year retention → ~6B records × ~500 bytes ≈ 3 TB, which is small enough to shard easily
- Key length: base62 with 7 characters gives 62⁷ ≈ 3.5 trillion keys, far more than needed

The takeaway to say aloud is that the system is read-heavy and latency-sensitive, but storage is modest.

## 3. API

```
POST /api/v1/urls   { longUrl, customAlias?, expiresAt? } → { shortUrl }
GET  /{shortKey}    → 301/302 redirect to long URL
DELETE /api/v1/urls/{shortKey}
```

**301 vs 302** is a classic trade-off. 301 (permanent) lets browsers cache, which reduces load but means you lose click analytics and the ability to change the target. 302 gives you control and analytics at the cost of more traffic.

## 4. Data model

A simple key-value shape: `shortKey (PK) → longUrl, userId, createdAt, expiresAt`.

There are no joins and access is almost always by primary key, so a NoSQL store (DynamoDB, Cassandra) fits well. A sharded SQL database also works. Be ready to justify either choice.

## 5. The core deep dive: generating the short key

This is where interviewers usually probe hardest. Know the options:

1. **Hash and truncate** (MD5/SHA, take the first 7 chars of the base62 encoding). It is simple, but collisions require a check-and-retry loop, and the same long URL always yields the same key unless you salt it.
2. **Global counter encoded in base62.** No collisions, but sequential keys are guessable and a single counter is a bottleneck and single point of failure.
3. **Range allocation (my recommended answer).** Each app server leases a block of IDs (say 1M at a time) from a coordinator like ZooKeeper or etcd, or from a ticket database. Servers then generate keys locally with no coordination per request, which scales well and avoids collisions. To prevent guessability, apply a reversible bijective scramble to the ID before base62 encoding.
4. **Pre-generated Key Generation Service (KGS).** A service fills a pool of unused keys that app servers draw from. It is simple and fast at request time, but it needs care around key handoff and failure.

Custom aliases need a uniqueness check, using a conditional write (`PUT if not exists`).

## 6. High-level architecture

```
Client → CDN/Edge → Load Balancer → Stateless App Servers
                                        ├─ Cache (Redis) ← hot keys
                                        ├─ Key allocator (range leases)
                                        └─ DB (sharded/partitioned KV store)
Redirect events → Kafka → analytics pipeline → OLAP store
```

**Read path:** check the cache, and on a miss read the DB and populate the cache, then redirect. With an 80/20 access pattern, caching the hot 20% (LRU eviction) gives a very high hit rate.

**Write path:** get an ID from the local range, encode it, write to the DB, and return the URL.

## 7. Scaling and reliability

- **Sharding:** partition by hash of shortKey for even distribution. Mention consistent hashing to ease resharding.
- **Replication:** multi-AZ replicas, with multi-region for availability. Redirects can tolerate eventual consistency, but custom alias creation needs stronger guarantees.
- **Cache failure:** the DB must survive a cold cache. Use request coalescing to avoid thundering herds.
- **Analytics:** log clicks asynchronously through a queue so they never slow down the redirect.
- **Expiry and cleanup:** TTLs, plus lazy deletion on read and a background sweeper.
- **Abuse:** rate limiting per user and IP, malware and phishing URL checks, blocklists.
- **Security:** non-guessable keys, URL validation, and protection against redirect-loop abuse.

## 8. Likely follow-up questions

- What happens if a key-range coordinator goes down? (Servers keep working until their block runs out, so lease in advance.)
- How would you support the same long URL always mapping to the same short URL? (A reverse index or hash lookup, trading write cost for deduplication.)
- How do you handle a viral link getting millions of hits? (CDN and edge caching, replicated hot keys.)
- How would you add real-time analytics? (Kafka, stream processing, a columnar store.)
- How would you handle multi-region writes? (Region-prefixed key ranges avoid cross-region coordination.)

## Interview tips

- Say your trade-offs explicitly: "I'm choosing X over Y because..., at the cost of...".
- Don't jump to the diagram before requirements and numbers.
- Treat the interviewer's questions as steering. They show where they want depth.
- It's fine to say "I'd start simple and evolve." Begin with one DB and one cache, then scale out as the numbers demand.

If you'd like, I can run a mock interview where I play the interviewer and push back on your answers, or go deeper on any one section, such as key generation or sharding.



====================================================

# End-to-End Workflow: URL Shortener

A useful framing to state early in the interview: **the redirect path and the creation path have different availability needs, so I'll design them as separate paths.** Redirects need 99.99%+ availability and tens of milliseconds of latency. Creation can tolerate 99.9% and a slower response.

## The big picture

```
                         READ PATH (redirect)
Client → DNS → CDN/Edge → Load Balancer → App Server (Redirect Svc)
                                              │ 1. local L1 cache
                                              │ 2. Redis cluster (L2)
                                              │ 3. DB (replica read)
                                              └─ async: click event → Kafka → Analytics

                         WRITE PATH (create)
Client → DNS → CDN/WAF → Load Balancer → API Gateway (auth, rate limit)
       → App Server (Create Svc) → validate → key allocation (local range)
       → DB conditional write → (optional) cache warm → response
                                   ▲
                     Range coordinator (etcd/ZooKeeper), only touched once per ~1M keys
```

Capacity is not the hard part. At ~4K redirects/sec average and ~40K at peak, a handful of servers could handle the load. **We run many instances for fault tolerance, not throughput.** Saying this out loud shows good judgment.

---

## Read path, hop by hop

### 1. DNS
**Role:** map `sho.rt` to the nearest healthy entry point.

**Consider:**
- Geo/latency-based routing sends users to the closest region.
- Health-checked failover removes an unhealthy region from rotation.
- Anycast IPs can front the edge, so failover doesn't depend on DNS TTLs.

**Reliability:** use multiple DNS providers or a managed, globally replicated service. DNS is a classic hidden single point of failure.

**Trade-off:** low TTL (30-60s) gives fast failover but more DNS queries and slightly higher latency on first hit. High TTL is cheap but slow to react to outages.

### 2. CDN / Edge
**Role:** TLS termination, DDoS/WAF protection, and optionally caching redirects at the edge.

**Consider:**
- Hot or viral links can be served entirely from the edge, which is the best defense against a thundering herd.
- Edge caching works with either redirect code, as long as you set `Cache-Control` explicitly.
- Edge rate limiting and bot filtering absorb abusive traffic before it reaches your servers.

**Reliability:** if the edge cache misses or is unavailable, requests fall through to origin. Origin must be able to survive full traffic without the CDN (or at least degrade gracefully).

**Trade-offs:**
- **Staleness vs. offload:** a long edge TTL gives great offload, but a deleted or malicious link keeps redirecting until it expires. You need a purge API for takedowns, and a short TTL (minutes) as the default.
- **Analytics:** edge-served redirects never reach your app servers, so you need CDN logs as an analytics source, or you accept undercounting.

### 3. Load Balancer
**Role:** distribute traffic across app servers and remove unhealthy ones.

**Consider:**
- **L4 vs. L7:** L4 is faster and simpler. L7 can route by path (`/api/*` to Create Svc, `/{key}` to Redirect Svc), which lets you scale and fail the two paths independently.
- Active health checks plus passive outlier detection (eject servers returning 5xx).
- Connection draining on deploys so in-flight requests finish.
- **Algorithm:** least-connections or round-robin is the default. Consistent hashing on `shortKey` improves local-cache hit rates but risks hot-spotting a single server on a viral link.

**Reliability:** the LB must not be a single point of failure. Run redundant LBs across AZs (active-active) behind anycast or DNS, or use a managed LB. Spread backends across at least 3 AZs, sized so that losing one AZ doesn't overload the rest (N+1 capacity).

**Trade-off:** L7 gives flexibility and observability but costs more CPU and latency. For a latency-sensitive redirect path, some teams use L4 for redirects and L7 only for the API.

### 4. App Server (Redirect Service)
**Role:** resolve `shortKey` to `longUrl` and return the redirect.

**Consider:**
- **Stateless**, so any instance can serve any request. This makes autoscaling and replacement trivial.
- **Cheap validation first:** reject malformed keys (wrong length or alphabet) before touching cache or DB.
- **Bloom filter** of existing keys (optional) to cheaply reject nonexistent keys. This protects the DB from **cache penetration**, where attackers request random keys that always miss.
- **In-process L1 cache:** a small LRU with a very short TTL (seconds) absorbs the hottest keys without a network hop.
- **Timeouts and retries:** set tight timeouts (e.g., 50ms to cache, 100ms to DB). Retry only idempotent reads, with exponential backoff and jitter, and cap retries to avoid retry storms.
- **Circuit breakers and bulkheads:** if the DB is slow, fail fast or serve stale rather than piling up threads.
- **Load shedding:** under overload, reject excess requests early rather than degrading everyone.
- **Graceful degradation:** analytics, abuse checks, or anything non-essential must never block the redirect.

**Reliability:** deploy with rolling or canary releases, run across AZs, and autoscale on CPU/RPS. Keep the redirect service small with few dependencies, because fewer dependencies means fewer failure modes.

**Trade-offs:**
- **L1 cache:** it cuts latency and Redis load but adds staleness and memory duplication across instances. A seconds-level TTL keeps this acceptable.
- **Separate Redirect and Create services:** better isolation (a write-path bug can't take down redirects) at the cost of more services to operate.

### 5. Distributed Cache (Redis Cluster)
**Role:** serve the vast majority of redirects without touching the DB.

**Consider:**
- **Pattern:** cache-aside (read-through on miss). It's simple, and a cache failure doesn't cause data loss.
- **Sizing:** under an 80/20 access pattern, the hot set is small. Assume ~100M hot entries × ~500B ≈ 50 GB, which fits in a modest cluster. State this as an assumption.
- **TTL with jitter:** randomize expiry so many keys don't expire simultaneously and stampede the DB.
- **Negative caching:** cache "not found" results with a short TTL (30-60s) to blunt penetration attacks.
- **Request coalescing (single-flight):** when many requests miss on the same key, only one goes to the DB and the rest wait for its result.
- **Hot-key problem:** one viral key can saturate a single Redis shard. Mitigations are the L1 cache, replicating hot keys across shards, and edge caching.
- **Eviction:** LRU or LFU. LFU handles "viral then forgotten" patterns better.
- **Invalidation:** on delete or expiry, remove the key explicitly in addition to relying on TTL.

**Reliability:**
- Primary plus replicas per shard with automatic failover (Redis Sentinel or Cluster).
- The cache is **not** the source of truth. The system must survive a cold or fully lost cache, which means the DB must be provisioned for the miss storm, or protected by coalescing and load shedding.
- After a cache restart, warm gradually and don't route full traffic to cold nodes at once.
- Use client-side consistent hashing or cluster-aware clients so that adding or removing a node only remaps a fraction of keys.

**Trade-offs:**
- **Cache-aside vs. write-through:** write-through keeps the cache fresh but pollutes it with links that are never clicked. Cache-aside is lazier but efficient.
- **Consistency vs. latency:** after an edit or delete, there is a brief window where the cache serves the old value. For a URL shortener this is generally acceptable, but it matters for takedowns, so use explicit invalidation.

### 6. Database
**Role:** durable source of truth: `shortKey → longUrl, userId, createdAt, expiresAt`.

**Consider:**
- **Access pattern:** point lookups by primary key, with no joins. That points to a distributed KV/wide-column store (DynamoDB, Cassandra), though sharded MySQL/Postgres is defensible.
- **Partitioning:** hash of `shortKey` for even distribution, with consistent hashing to make resharding cheap. Avoid range partitioning on sequential IDs, which creates hot shards.
- **Replication:** RF=3 across AZs. Redirect reads can go to any replica (eventual consistency is fine). Alias creation needs a conditional write with stronger consistency.
- **Hot partitions:** a viral key is a read hotspot, which the cache layers should absorb before it reaches the DB.
- **Backups and recovery:** point-in-time recovery, plus periodic snapshots to object storage.
- **Expiry:** native TTL if available, otherwise a background sweeper plus lazy deletion on read.

**Reliability:** quorum replication tolerates the loss of a node or AZ. Test failover regularly, and monitor replication lag.

**Trade-offs:**
- **NoSQL vs. SQL:** NoSQL gives horizontal scale and simple operations, but weaker ad-hoc querying (e.g., "all links by user" needs a secondary index or separate table). SQL gives richer queries and transactions, but you shard manually.
- **Consistency level:** quorum reads cost latency but reduce stale reads. Single-replica reads are fastest but may lag.

### 7. The response
- **301 vs. 302:** 301 is cacheable by browsers (less load, but no analytics and no ability to change the target). 302 keeps control at the cost of more traffic. A common answer is 302 (or 301 with a short `max-age`) if analytics matter.
- **Status for bad links:** 404 for never-existed, 410 for expired or deleted. Serve a safe interstitial for flagged URLs.

---

## Write path, hop by hop

### 1. Edge, LB, and API Gateway
- **Authentication:** API keys or OAuth for creation.
- **Rate limiting:** token bucket per user and per IP, backed by Redis. This prevents spam and key-space abuse.
- **Idempotency:** accept an `Idempotency-Key` header, because clients retry on timeouts and you don't want duplicates.

### 2. Create Service: validation and abuse checks
- Validate scheme (http/https only), length, and normalize the URL.
- Check against blocklists and a malware/phishing reputation service.
- **Trade-off:** synchronous checks are safer but add latency and a dependency. Asynchronous scanning is faster but lets a bad link live briefly. A common compromise is a sync check against a local blocklist plus an async deep scan that can quarantine the link afterwards.

### 3. Key allocation (range leasing)
- Each app server leases a block (say 1M IDs) from a coordinator (etcd/ZooKeeper, or a small DB with an atomic increment), then generates keys locally with no per-request coordination.
- Apply a **reversible bijective scramble** before base62 encoding so keys aren't sequential or guessable.
- **Failure behavior:** if a server crashes, its unused range is simply lost. That's fine, because 62⁷ ≈ 3.5T keys.
- **Coordinator outage:** servers keep working until their block is exhausted. Mitigate by **prefetching the next block** when the current one is ~20% remaining, and by running the coordinator as a 3- or 5-node quorum.
- **Trade-offs:** larger blocks mean fewer coordinator calls but more waste on crashes. Smaller blocks are the reverse. Also, **keys are not globally ordered**, which is fine here.

### 4. DB write
- Use a **conditional write** (`PUT if not exists`). It guards against collisions, and it's mandatory for custom aliases.
- On conflict, a generated key means retry with the next ID. A custom alias means return `409 Conflict`.
- Wait for the write quorum before acknowledging success, because losing an acknowledged link is unacceptable.
- **Trade-off:** quorum writes add latency, but creation is only ~40/sec and latency-tolerant, so favor durability.

### 5. Cache warm (optional) and response
- Many links are clicked right after creation (shared immediately), so warming can help. But most links are rarely clicked, so warming everything wastes memory. Warm with a short TTL, or skip it.
- Return `201 Created` with the short URL.

---

## Async analytics path (never in the critical path)

```
Redirect Svc → async producer (local buffer) → Kafka → stream processor → OLAP store (ClickHouse/Druid) → dashboards
```

- **Delivery:** at-least-once, with dedupe downstream, because exactly-once is costly.
- **Failure policy:** if Kafka is down, buffer briefly in memory, then **drop events rather than block redirects**. Analytics is best-effort, while redirects are critical.
- **Considerations:** bot filtering, privacy (IP truncation, GDPR), and partitioning by `shortKey` for aggregation.
- **Trade-off:** accuracy vs. availability. Dropping events under stress undercounts clicks, but it's the right default.

---

## Failure scenarios (great to walk through live)

| Failure | Impact | Mitigation |
|---|---|---|
| App server dies | Some in-flight requests fail | Stateless, LB health checks eject it, client retry |
| AZ outage | Lose ~1/3 capacity | N+1 sizing, multi-AZ everywhere, autoscale |
| Redis shard fails | Misses spike for its keys | Replica failover, coalescing, DB headroom, load shedding |
| Entire cache lost | DB sees full read traffic | Gradual warm-up, rate limiting at origin, CDN and L1 absorb part of the load |
| DB node/replica down | Reduced redundancy | Quorum reads/writes continue, auto-repair |
| Hot/viral key | One shard or key overloaded | CDN, L1 cache, hot-key replication |
| Key coordinator down | Can't lease new ranges | Prefetch, quorum coordinator, existing blocks last a while |
| Kafka/analytics down | No click data | Drop or buffer, redirects unaffected |
| Bad deploy | Errors across fleet | Canary, automatic rollback, feature flags |
| Region outage | Regional traffic lost | DNS/anycast failover, multi-region replicated data |
| Cache penetration attack | DB overload on random keys | Bloom filter, negative caching, rate limits |

---

## Cross-cutting concerns

**Availability principle: static stability.** The redirect path should depend only on cache and DB reads. It should keep working even if the coordinator, Kafka, auth service, and abuse scanner are all down.

**Multi-region:**
- Reads are served locally from replicated data (eventual consistency is acceptable).
- Writes can be region-local with **region-prefixed key ranges**, so two regions never generate the same key and need no cross-region coordination. Custom aliases are the exception: they need a global uniqueness check, or a "home region" for alias ownership.
- Define RPO/RTO (e.g., RPO of seconds, RTO of minutes).

**Observability:**
- Track p50/p99 redirect latency, cache hit ratio, DB error rate, replication lag, and key-range exhaustion.
- Alert on SLO burn rate rather than raw thresholds.
- Distributed tracing and structured logs help debug across hops.

**Capacity and protection:** autoscaling, per-client rate limits, and load shedding. Test regularly with load tests and chaos experiments (kill a cache shard, kill an AZ).

---

## How to narrate this in the interview

1. **Start simple:** one app server, one DB, one cache. Show it works.
2. **Walk the request** through each hop and ask at each one, "what if this fails, and what's the trade-off?"
3. **Name the invariants:** redirects must be fast and always available, links must never be lost once acknowledged, and non-essential features must never block redirects.
4. **Close with evolution:** "If traffic grew 10x I'd add X, and if we needed Y I'd change Z."

If you'd like, I can play the interviewer for a mock round, pushing on a specific stage such as "your Redis cluster just lost a shard, walk me through it."
