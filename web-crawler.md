# Web Crawler Design

**What to say early:** This is a **graph traversal problem at internet scale**, with an adversarial, unreliable environment. The interesting parts are not the fetching itself. They are the **URL frontier** (what to crawl next, and when), **politeness** (never hurt the sites you crawl), and **deduplication** (the web is full of duplicates and infinite traps). Throughput is a design goal here, not a threat, so the limiting factors are **per-host politeness limits and network latency**, not CPU.

---

## 1. Requirements

**Functional**
- Start from seed URLs, fetch pages, extract links, and crawl newly discovered URLs
- Store page content for downstream consumers (search indexing, ML training, archival)
- Respect `robots.txt` and crawl-delay hints
- **Recrawl** pages periodically to keep content fresh
- Handle HTML first, with optional support for JS-rendered pages, PDFs, and images

**Non-functional**
- **Politeness:** never overload any single host
- **Scalable:** add machines to raise throughput
- **Robust:** survive malformed HTML, hung servers, redirect loops, and spider traps
- **Extensible:** new content types and processing modules plug in easily
- **Efficient:** don't waste bandwidth on duplicates or low-value pages
- Fault tolerant: a crashed worker loses little work and never corrupts state

**Clarify with the interviewer:**
- Purpose: a general search engine, a focused crawler (one domain or topic), or a one-time archive? This changes freshness and priority needs.
- Scale and time budget: how many pages, and over what period?
- Is JavaScript rendering required? It is roughly 10-100× more expensive per page.
- Freshness requirements: do news sites need re-crawling every few minutes?
- Do we also crawl non-HTML content?

---

## 2. Estimation (state assumptions)

- **1B pages/month** target ≈ 400 pages/sec average. Plan for **~1K/sec** peak for catch-up capacity.
- Average page ~100 KB (HTML only) → **~100 TB/month raw**, so ~1.2 PB/year uncompressed. Compression (3-5×) and dedupe bring this down, and it goes to object storage.
- Bandwidth: 1K pages/sec × 100 KB = **100 MB/s ≈ 800 Mbps**. This is manageable, but it is a real cost line.
- **Link extraction:** ~50 outlinks per page → ~20K-50K candidate URLs/sec to dedupe-check. This is the real hot path.
- **Seen-URL set:** ~10B URLs × 8-byte fingerprint ≈ **80 GB**, or a Bloom filter at ~10 bits/URL ≈ **12.5 GB** (1% false positive rate). This is small enough to partition across a few nodes in memory.
- **Concurrency (Little's law):** 1K pages/sec × ~1 s per fetch (DNS + connect + download) = **~1,000 in-flight fetches**. One async fetcher node handles several hundred to a few thousand, so **tens of fetcher nodes** suffice.

**Takeaway to say aloud:** Compute is modest. The constraints are **network I/O, storage volume, and politeness**. Politeness is the real throughput limiter, because crawling 1K pages/sec while limiting each host to 1 request/sec means you need **at least 1K distinct hosts in flight at all times**. A crawl of a few giant sites is slow no matter how many machines you have.

---

## 3. API and data model

A crawler is mostly internal, so the "API" is the **pipeline contracts** plus a small control plane.

```
Control plane:  POST /crawls {seeds[], scope rules, limits}   GET /crawls/{id}/stats
                POST /hosts/{host}/policy {maxQps, blocked}   (manual overrides, abuse complaints)

Internal:       Frontier.next(workerId, n) → [URL tasks with lease]
                Frontier.ack(taskId, result) / nack(taskId, retryAfter)
                Frontier.add(urls[], depth, priority)
```

**Data model**

| Entity | Shape | Store |
|---|---|---|
| URL metadata | `urlHash → url, status, lastCrawlTs, nextCrawlTs, etag, lastModified, contentHash, failCount, priority, depth` | Wide-column (Cassandra/Bigtable), partitioned by host-hash |
| Frontier queues | per-host queue of pending URLs + `host → nextAllowedTime` | Frontier service (memory + durable log/disk) |
| Seen-URL set | fingerprints or Bloom filter | In-memory, partitioned by URL hash, snapshotted to disk |
| Host state | `host → robots rules, crawlDelay, ipAddr, errorRate, lastFetch` | Redis or local cache + DB |
| Raw content | WARC-style files, keyed by `contentHash` | Object store (S3/HDFS) |
| Content fingerprints | `contentHash/simhash → canonical urlHash` | KV store |
| Link graph | `(srcUrl → dstUrl)` edges (for PageRank/ranking) | Object store or graph/columnar store |

**Key design points:**
- **Everything is keyed by host** wherever politeness matters. Partitioning by host puts all URLs for one site on one node, so rate limiting is a *local* decision, not a distributed one.
- **Store raw content once, by content hash**, and let URL metadata point to it. This gives dedupe for free.
- **Save conditional-GET headers** (`ETag`, `Last-Modified`), so recrawls can send `If-None-Match` and receive a cheap `304 Not Modified`.

---

## 4. High-level architecture

```
                ┌──────────────────── URL FRONTIER (the brain) ────────────────────┐
Seeds ─────────►│ Front queues (priority) → router → Back queues (one per host)    │
                │                                      ▲  min-heap by nextAllowedTime│
New URLs ──────►│  dedupe filter (seen-set) ─ normalize                             │
                └──────────────┬───────────────────────────────────────────────────┘
                               │ lease (host-partitioned)
                               ▼
              Fetcher fleet (async I/O) ── DNS cache/resolver ── robots.txt cache
                   │ HTTP GET (conditional), timeouts, size caps, redirect limits
                   ▼
              Kafka "page.fetched"  (raw bytes → object store, metadata → message)
                   │
        ┌──────────┼───────────────────────┬─────────────────────┐
        ▼          ▼                       ▼                     ▼
  Parser/Extractor  Content dedupe     Content store         Downstream
  (links, text,     (hash/SimHash)     (S3/WARC)            (indexer, ML, archive)
   canonical URL)
        │
        └─ extracted links → normalize → filter (scope, robots, blocklist, trap rules)
                          → seen-set check → Frontier.add()

Scheduler / Recrawl planner ─► re-enqueue URLs by nextCrawlTs
Monitoring: crawl rate, per-host errors, frontier depth, duplicate ratio, trap detector
```

---

## 5. The core deep dive: the URL frontier

This is where interviewers probe hardest. The frontier must do three jobs at once: **prioritize** (important pages first), **be polite** (rate limit per host), and **scale** (billions of URLs). A single FIFO queue fails all three.

### Naive: one BFS queue
- A single FIFO queue crawls in discovery order.
- **Fails on politeness:** a big site's links cluster together in the queue (page A links to 200 pages on the same host), so you hammer one host with 200 back-to-back requests.
- **Fails on priority:** an important news page is stuck behind millions of low-value URLs.
- **Doesn't scale:** one queue means one machine and one lock.

### The Mercator design (front queues + back queues)

**Front queues (priority):** F queues numbered by priority. A prioritizer scores each URL (see below) and puts it in a queue. Selection is biased toward higher-priority queues.

**Back queues (politeness):** B queues, where **each back queue holds URLs from exactly one host at a time.** A **min-heap keyed by `nextAllowedTime`** tracks which host may be fetched next.

**Fetch loop (per worker):**
1. Pop the host with the earliest `nextAllowedTime` from the heap. If it is in the future, sleep or work on another host.
2. Take the next URL from that host's back queue and give it to a fetcher.
3. After the fetch completes, set `nextAllowedTime = now + max(crawlDelay, k × fetchDuration)` and push the host back onto the heap. (A fetch-time-proportional delay makes slow, struggling sites get less load automatically.)
4. If a back queue empties, refill it from the front queues, **picking a URL from a new host** and mapping that host to this back queue.

**Why this works:** the heap guarantees per-host spacing, the back-queue-per-host mapping guarantees only one in-flight request per host (or a small configured limit), and the front queues give priority without breaking politeness.

### Partitioning the frontier across machines
- **Partition by hash(host).** All URLs for a host go to one frontier shard, so its back queue, politeness timer, and robots cache live in one place. No distributed coordination is needed for rate limiting.
- **Trade-off:** hot hosts. One giant domain (a huge site with 1B pages) is bound to one shard, which caps its crawl rate at the politeness limit, and a big site's backlog can bloat one node. Mitigations: **sub-partition mega-hosts** by subdomain or path, and accept that the politeness cap *is* the correct bottleneck. Spreading a single host's load across nodes would just violate politeness.
- **Rebalancing:** consistent hashing so adding or removing a shard moves only a fraction of hosts. Moved hosts lose their in-memory timers, so they restart with a conservative delay.

### Prioritization (what gets crawled first)
Score each URL with some mix of:
- **Domain/page importance:** PageRank-like signal, inbound link count, domain authority
- **Depth from seed:** shallower pages tend to be more important (BFS bias), and limiting depth also bounds traps
- **Freshness need:** known-frequently-changing pages get a boost (news homepages, feeds)
- **Novelty:** URLs from unexplored hosts get a boost, to avoid over-crawling known sites
- **Cost:** deprioritize large or slow resources

**Trade-off:** crawling by importance gets the best content early but needs a ranking signal (which comes from the link graph you are still building). BFS is simple and approximates importance well in practice. A common start is BFS plus domain-level priors, then add richer scoring later.

### Durability of the frontier
The frontier holds billions of URLs, which is too much for RAM alone.
- **Hot working set in memory** (heap + the front of each back queue), **bulk on disk or in a log-structured store** (RocksDB or similar), with batched refills.
- **Leases, not deletes:** when a fetcher takes a URL, mark it **leased with a timeout**. On ack, remove it. If the fetcher dies, the lease expires and the URL is re-served. This yields **at-least-once crawling**, and duplicates are harmless because the downstream is idempotent.
- **Crash recovery:** frontier shards persist their state (WAL + snapshots). A restart rebuilds the heap from disk. Politeness timers can be conservative after restart.

---

## 6. Pipeline, hop by hop

### 1. Fetcher fleet
- **Async, non-blocking I/O** (event loop or lightweight threads) because the work is network-bound. One node can sustain thousands of concurrent connections.
- **Must-have safeguards:**
  - **Timeouts everywhere:** connect (a few seconds), total request (e.g., 30 s), and slow-loris protection (minimum bytes/sec). A single hung server must not tie up a worker.
  - **Response size cap** (e.g., 10 MB for HTML). Stop reading and discard beyond it.
  - **Redirect limit** (e.g., 5-10 hops), with redirect-loop detection.
  - **Content-type check** from headers before downloading the body. Skip media or binaries you don't want.
  - **Compression support** (gzip/br) and **connection reuse** (keep-alive) per host. Reuse reduces handshake cost and is gentler on the target.
- **Honest identification:** a descriptive `User-Agent` with a contact URL, so site operators can reach you or block you. Hiding it is both rude and a reason to get banned.
- **Conditional GETs** on recrawl (`If-None-Match`, `If-Modified-Since`) to save bandwidth.
- **Reliability:** fetchers are stateless workers pulling leased tasks, so a dead fetcher just means its leases expire. Scale on in-flight concurrency, not CPU.

### 2. DNS
- DNS is a **sneaky bottleneck**. Standard resolvers are synchronous, rate-limited, and slow (tens to hundreds of ms), and at 1K fetches/sec they will throttle you or become your latency floor.
- **Dedicated caching resolver** (local to the fleet) with a big cache, async resolution, and aggressive prefetching for hosts about to be crawled.
- **Cache the IP in host state**, with a TTL, so most fetches skip DNS entirely.
- **Trade-off:** long-cached IPs go stale if a site moves. Respect TTLs loosely, and re-resolve on connection failure.
- **Politeness is by host *and* IP:** many hostnames share one IP (shared hosting, CDNs). Rate-limit by IP too, or you accidentally flood a shared server.

### 3. robots.txt
- Fetch and cache `robots.txt` per host **before** crawling that host. Cache with a TTL (~24 h) and re-check periodically.
- Respect `Disallow`, `Allow`, and `Crawl-delay` (not all crawlers honor `Crawl-delay`, but you should treat it as a floor on the politeness delay).
- **Failure semantics matter:**
  - `404` on robots.txt → everything allowed
  - `5xx` or timeout → **treat as temporarily disallowed** and retry later (don't assume allowed, since the site may be having problems)
  - `4xx` other than 404 → generally treat as allowed (follow convention)
- Cap robots.txt size (e.g., parse the first 500 KB) to protect against abuse.
- **Trade-off:** robots checks add one more fetch per new host, which is cheap after the first. This is both an ethical and a practical requirement, because violating it gets you blocked and can have legal consequences.

### 4. Handoff to Kafka and the content store
- The fetcher writes raw bytes to the **object store** (batched into large WARC-style files, not millions of tiny objects) and publishes a small `page.fetched` message with the metadata and storage pointer to Kafka.
- **Why a queue between fetch and parse:** fetch is network-bound and parse is CPU-bound. Decoupling lets each scale independently, and a parser backlog doesn't stall fetchers (or vice versa).
- **Trade-off:** Kafka adds a hop and operational cost, but it absorbs bursts, enables replay (reprocess pages with a new parser without refetching), and isolates failures.

### 5. Parser / extractor
- Parse HTML defensively. The web is full of broken markup, so use a tolerant parser and bound memory and CPU time per document (a pathological page must not hang a worker).
- Extract: outlinks (`<a href>`, plus optionally `<link>`, canonical tags), text for indexing, metadata, and `<meta robots>` / `nofollow` directives.
- **Resolve relative URLs** against the base URL (honor `<base href>`), then normalize (next section).
- **Stateless and horizontally scalable**, with CPU-bound autoscaling.

### 6. URL normalization and filtering
Before dedupe, make equivalent URLs identical:
- Lowercase scheme and host, remove default ports, strip fragments (`#section`)
- Resolve `.` and `..` path segments, decode unreserved percent-encodings
- **Sort or strip query parameters** that don't change content (tracking params like `utm_*`, session IDs), applying per-site rules where known
- Honor `<link rel="canonical">` as a hint, not a command (sites get it wrong)
- **Filter:** scheme must be http/https, scope rules (allowed domains, TLDs), blocklists, max URL length, file extension rules

**Trade-off:** aggressive normalization improves dedupe but risks merging genuinely distinct pages (`?id=1` vs `?id=2`). Be conservative by default and learn per-site rules.

### 7. Seen-URL check (URL dedupe)
At 20K-50K candidate URLs/sec, you can't hit a database for each one.
- **In-memory fingerprint set or Bloom filter,** partitioned by URL hash across dedupe nodes (or co-located with the frontier shards).
- **Bloom filter:** ~10 bits/URL gives ~1% false positives. A false positive means **a new URL is wrongly skipped as "seen"**, so you silently lose ~1% of the web. A counting or cuckoo filter allows deletes, but costs more space.
- **Exact fingerprint set (64-bit hashes):** 8 bytes/URL, collision odds are negligible at 10B (a birthday-bound probability, small but non-zero). Costs more memory than a Bloom filter, with no false negatives and no meaningful false positives.
- **Tiered approach:** a Bloom filter as a fast first check ("definitely new" is trusted, "maybe seen" triggers an exact lookup in the URL metadata store). This gives exactness at near-Bloom cost.
- **Persistence:** snapshot periodically, and rebuild from the URL metadata DB on crash. Losing the filter only causes some re-crawling, not corruption.

### 8. Content dedupe
Different URLs often serve the same content (mirrors, `www` vs. non-`www`, print versions, boilerplate-only differences).
- **Exact duplicates:** hash the body (SHA/MD5) and look it up. If it exists, link this URL to the canonical content record and **skip storing and processing it again.**
- **Near-duplicates:** **SimHash / MinHash** over shingles or normalized text. Compare fingerprints by Hamming distance. Pages that differ only by ads, timestamps, or comments are caught.
- **Trade-off:** near-dup detection needs more compute and a similarity index, and it risks merging pages that are legitimately different. Use it on extracted main content, not raw HTML, and use a conservative threshold.

---

## 7. Hazards unique to crawlers

### Spider traps
Infinite or hugely expanding URL spaces: calendars (`/cal?month=next` forever), session IDs in URLs, repeating path segments (`/a/b/a/b/a/b`), faceted search with combinatorial filters, or deliberately malicious generators.
- **Defenses (layered):**
  - **Max URL length** and **max path depth**
  - **Detect repeating segments** in paths
  - **Per-host page budget:** cap URLs crawled per host per period, proportional to site value
  - **Max crawl depth** from seed
  - **Query-parameter limits** (number of params, known tracking/session params stripped)
  - **Content-based detection:** a host whose recent pages are all near-duplicates or all have very low content is flagged and throttled
  - **Anomaly alerts:** a host's discovered-URL count growing exponentially
- **Trade-off:** budgets and depth limits protect you but can cut off genuinely huge legitimate sites (Wikipedia, big forums). Make budgets adaptive using site quality signals, and allow manual overrides.

### Soft 404s and low-quality content
Pages that return `200` but say "not found." Detect with content heuristics (or by probing a random nonexistent URL on the host) so you don't store and index garbage.

### JavaScript-rendered pages
Modern sites build content client-side, so a plain HTTP fetch returns an empty shell.
- **Headless browser rendering** (Chromium) can execute JS, but costs **10-100× more** CPU/memory per page, adds seconds per page, and is flaky.
- **Strategy:** a **two-tier crawl.** Fetch plain HTML first, and **detect pages that need rendering** (low text-to-script ratio, framework markers), then route only those to a separate, smaller **rendering fleet**. Optionally prefer discovering the site's underlying JSON APIs or sitemaps.
- **Trade-off:** coverage of the modern web versus a large cost increase, so render selectively and budget it.

### Politeness beyond rate limits
- Prefer **sitemaps** (`sitemap.xml`) where available. They give you the canonical URL list and change hints in one fetch, and they reduce discovery crawling.
- **Back off adaptively:** on `429`, `503`, or rising latency, increase the host's delay exponentially and honor `Retry-After`. If a site's error rate spikes after your crawl, treat it as a signal that **you are the problem.**
- **Time-of-day awareness:** crawl a site more gently during its peak hours if you know them.
- **Abuse contact:** provide a way to opt out, and honor complaints quickly (the manual host policy override in the control plane).

### Crawler security
- You are fetching **untrusted content from arbitrary servers.** Parsing is a security surface (decompression bombs, malformed archives, giant documents). **Sandbox parsers**, cap resource usage, and don't execute fetched content outside a sandbox.
- **SSRF:** a malicious page can link to `http://169.254.169.254/` or internal IPs. **Block private, loopback, and link-local ranges** at the fetcher (after DNS resolution, to defeat DNS rebinding), and run fetchers in a locked-down network segment.

---

## 8. Recrawl and freshness

Crawling once isn't enough. Pages change, and stale content hurts downstream quality.

**Goal:** maximize freshness within a bandwidth budget. You cannot recrawl everything constantly, so **recrawl frequency should follow change rate and importance.**

- **Adaptive schedule:** track how often each page changes (compare content hash across crawls). If it changed on 4 of the last 5 visits, shorten the interval. If it never changes, lengthen it (exponential backoff on the revisit interval, with a cap).
- **Importance weighting:** a high-traffic news homepage might be recrawled every few minutes. A deep archive page, every few months.
- **Cheap revalidation:** conditional GET (`304`) or a HEAD request costs far less than a full fetch. Use sitemap `lastmod` and feed (RSS/Atom) changes as hints.
- **Scheduler:** URL metadata holds `nextCrawlTs`, and a planner feeds due URLs back into the frontier through the same prioritizer and politeness path. **Recrawl and discovery share the frontier and compete for the same capacity**, so a budget split (e.g., 70% discovery / 30% recrawl) prevents either from starving.
- **Trade-off:** aggressive freshness burns bandwidth and risks politeness issues, while lazy freshness means stale results. This is a tunable knob driven by the product's needs (news search vs. archival).
- **Failed URLs:** retry with exponential backoff, and after N consecutive failures (e.g., DNS failure, repeated 404/410), **decay the priority or tombstone the URL** rather than hammering a dead link forever.

---

## 9. Scaling summary

| Component | Strategy |
|---|---|
| Frontier | Partition by host hash (consistent hashing), memory-hot + disk-backed, leases for crash safety |
| Fetchers | Stateless async workers, scaled by in-flight concurrency, leased tasks, autoscaled |
| DNS | Dedicated caching resolver, IP cached in host state, prefetch |
| robots/host state | Cached per host (colocated with the frontier shard), TTL-refreshed |
| Seen-URL set | Partitioned in-memory Bloom/fingerprint set, exact fallback in the metadata DB |
| Parsers | Stateless, CPU-autoscaled, sandboxed, fed from Kafka |
| Content storage | Object store (WARC batches), deduplicated by content hash |
| URL metadata | Wide-column store partitioned by host-hash |
| Recrawl | Scheduler on `nextCrawlTs`, adaptive intervals, budget split with discovery |
| JS rendering | Separate small fleet, selective routing |

---

## 10. Failure scenarios

| Failure | Impact | Mitigation |
|---|---|---|
| Fetcher crashes mid-fetch | In-flight URLs unacked | **Lease expires, URL re-served**, idempotent downstream |
| Frontier shard dies | Its hosts stop being crawled | Restore from WAL/snapshot or replica, other shards unaffected, conservative politeness timers after restart |
| Seen-set lost | May re-crawl known URLs | Rebuild from the URL metadata DB, the cost is wasted bandwidth, not wrong data |
| Spider trap | Frontier floods with junk, crawl stalls on one site | URL length/depth limits, per-host budget, anomaly detection |
| Target site slows or errors | Your fetchers hang or hammer a struggling site | Timeouts, adaptive backoff on 429/503/latency, per-host circuit breaker |
| DNS resolver throttled or down | Fetch rate collapses | Local caching resolver, cached IPs, multiple upstream resolvers |
| Hot host (huge site) | One shard bloated | Sub-partition by subdomain/path, accept the politeness cap as the true limit |
| Parser hang or crash on a bad page | Worker stalls | Per-document CPU/memory/time caps, sandboxing, poison messages to DLQ after N retries |
| Kafka backlog | Parse and downstream lag | Autoscale parsers, buffer in Kafka (fetchers are not blocked), alert on lag |
| robots.txt unreachable | Unclear permission | Treat as temporarily disallowed, retry later |
| Duplicate-heavy site | Wasted bandwidth and storage | Content-hash dedupe, per-host quality scoring, reduce budget |
| Bad config pushes (e.g., politeness delay set to 0) | You DDoS sites | **Hard-coded safety floor** on per-host delay that config cannot override, staged rollout |
| Network partition or region outage | Crawl rate drops | Frontier state is durable, other regions continue, rebalance hosts when it recovers |
| Legal/abuse complaint | Reputation and legal risk | Instant per-host/domain block via control plane, audit log |

**Principle (graceful degradation and "do no harm"):** a crawler can run slowly or even stop entirely with no real damage, because it is an offline, best-effort pipeline. What it must never do is **harm other people's servers** or **corrupt its own state**. So safety limits (politeness floors, size caps, timeouts) are hard-coded and fail toward doing less.

---

## 11. Consistency decisions

- **Crawl coverage:** eventually complete. Missing a page for a while is fine.
- **URL dedupe:** approximate is acceptable (Bloom filter), with bounded false-positive loss. Tiering to an exact check recovers exactness cheaply.
- **Delivery to downstream:** at-least-once, with idempotent writes keyed by `urlHash` and `contentHash`.
- **Per-host politeness:** strong and local, enforced by single-shard ownership of each host. This is the one guarantee you must not violate.
- **Frontier state:** durable with leases. Losing a few recent additions is OK, because links will be rediscovered.
- **robots.txt freshness:** cached for hours. A newly added `Disallow` may be violated for up to the TTL, so use shorter TTLs for sensitive hosts and honor complaints immediately.

---

## 12. Multi-region / distributed crawling

- **Geographic placement:** crawl from regions close to target servers to reduce latency and increase fetch throughput. Assign hosts to regions (by TLD, IP geolocation, or measured latency).
- **Host ownership is still single:** each host belongs to exactly one frontier shard globally (or one region), to keep politeness local. **Never let two regions crawl the same host independently**, or you double the load on it.
- **Cross-region link exchange:** URLs discovered in region A for hosts owned by region B are **forwarded in batches** to B's frontier, not fetched locally.
- **Trade-off:** locality improves fetch speed, but cross-region URL forwarding adds delay and complexity. Batching and async forwarding make this cheap, and the delay is irrelevant for a crawl.
- **Fault tolerance:** replicate frontier snapshots across regions, so a region loss means another region takes over its hosts with conservative timers.

---

## 13. Likely follow-up questions

- **"How do you ensure politeness at scale?"** Partition by host so that one shard owns each host's timer. Back queues hold one host each, and a min-heap on `nextAllowedTime` enforces spacing. Also rate-limit by IP and adapt delays to response times and errors.
- **"How do you avoid crawling the same URL twice?"** Normalize, then check a partitioned in-memory Bloom filter with an exact fallback. Content dedupe by hash and SimHash catches the same content under different URLs.
- **"How do you handle spider traps?"** URL length/depth limits, repeating-segment detection, per-host budgets, low-quality-content detection, and alerts on exponential URL discovery.
- **"What if a fetcher crashes?"** Leases with timeouts. The URL is re-served, and downstream operations are idempotent.
- **"How do you decide what to crawl first?"** Front queues with priority scores (domain importance, depth, freshness need), with BFS as a simple baseline.
- **"How do you keep content fresh?"** Adaptive recrawl intervals based on observed change rate and importance, conditional GETs, sitemap/feed hints, and a budget split between discovery and recrawl.
- **"How do you handle JavaScript-heavy sites?"** A selective two-tier approach. Plain fetch by default, with a smaller headless-rendering fleet for pages that need it, accepting the higher cost.
- **"How would you scale this 10×?"** More fetchers and frontier shards (consistent hashing), more Kafka partitions, a larger partitioned dedupe set, and regional crawling. Politeness remains the limit per host, so scaling means **more distinct hosts in flight**.
- **"How do you crawl a single huge site fast?"** You can't, beyond its politeness limit. Use sitemaps and APIs where available, negotiate higher limits with the site owner, and distribute across IPs only with permission.
- **"What about security?"** Block private IP ranges (SSRF), sandbox parsers, cap sizes and decompression ratios, and isolate the fetcher network.
- **"How would you make this a focused crawler?"** Add a relevance classifier in the pipeline and use its score as the URL priority. Only enqueue links from pages judged relevant, which gives a best-first search over the web graph instead of BFS.
- **"DFS or BFS?"** BFS, because DFS can get lost deep inside one site (a trap) and hurts politeness by hammering one host. Best-first by priority is better still.

---

## 14. How to narrate it

1. **Name the dominant problem:** an adversarial, huge graph, where the hard parts are the frontier (priority plus politeness), dedupe, and robustness. Raw compute is not the challenge.
2. **Start naive:** a single loop with a FIFO queue and a `visited` set. Show it fails on politeness (hammering one host), scale (one machine), and traps. Then introduce back queues per host, then front queues for priority, then partitioning by host, then the Bloom filter as the seen-set.
3. **State the contract:** per-host politeness is a hard guarantee, crawling is at-least-once with idempotent downstream, and dedupe is approximate with exact fallback.
4. **Separate stages with queues:** fetch (I/O-bound) → parse (CPU-bound) → dedupe and enqueue, so each scales independently.
5. **Call out the knobs:** politeness delay and multiplier, per-host budget, depth limit, Bloom filter size, recrawl intervals, discovery/recrawl split, lease timeout.
6. **For each box, say what happens when it fails,** and end with "workers can die and nothing is lost, and the system fails toward doing less, never toward harming a site."

**Patterns reused from our toolbox:** partitioning by a key that localizes coordination (host) so rate limiting needs no distributed state, consistent hashing, queues with leases and idempotent consumers, Bloom filters (from URL shortener and Top-K), content-addressed storage and dedupe, backoff with jitter, circuit breakers, rate limiting (from the previous design, but applied *outbound*), batch/stream decoupling through Kafka, and graceful degradation.

Next, I can play interviewer on this (for example, "your crawler has been running for a week and one host now accounts for 40% of your frontier, walk me through it"), or we can move to another question. **Uber/proximity** adds geospatial indexing and real-time matching, **notification system** reuses queues, retries, and rate limits in a different shape, and **Dropbox/Drive** brings chunking, sync, and conflict resolution.
