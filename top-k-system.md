# Top-K Popular Articles Design

**What to say early:** This is an *aggregation and serving* problem, not a storage or fan-out problem. Events arrive at high volume, but the answer is tiny (K = 10-100 IDs) and **identical for every user**. So the design splits into two halves: a **streaming aggregation pipeline** that maintains counts over a time window, and a **trivially cacheable read path** that serves precomputed results.

---

## 1. Requirements

**Functional**
- Return the top K articles for a window: last 1 hour, 24 hours, 7 days, all-time
- Slice by dimension: global, category (sports, tech), region or language
- Count signals: views, plus optionally likes, shares, comments (weighted)
- Optionally "trending" (rising fast) in addition to "popular" (highest total)

**Non-functional**
- Reads: low latency (< 50ms), very high availability
- Freshness: results are a few seconds to a minute stale at most. Exact real-time is not needed.
- Write ingestion must never slow down the user's page load
- Counts can be approximate, but must resist bots and manipulation

**Clarify with the interviewer:**
- Popular (cumulative) or trending (velocity)?
- Is exactness required (a leaderboard shown publicly or paid on) or are approximate counts fine?
- How many dimensions (category × region × window)?
- Is it per-user personalized, or global?

---

## 2. Estimation (state assumptions)

- 100M DAU × ~10 article views/day = 1B events/day ≈ **12K events/sec avg, ~50K peak**
- Event ~100 bytes → **100 GB/day** raw, which is easily a log in object storage
- Active articles in a 24h window: ~1M
- Window state: 1M articles × 8 bytes × 60 one-minute buckets ≈ **~500 MB**
- Reads: every page showing "Top stories" → ~20K+/sec, but only a handful of *distinct* keys (window × category × region)

**Takeaway to say aloud:** ingest volume is modest and the exact counter state *fits in memory on a small cluster*. Sketches and heavy machinery are only needed if cardinality is huge (search queries, hashtags, URLs). Showing that you do the math before reaching for fancy structures is a good signal.

---

## 3. API and data model

```
POST /v1/events           { articleId, type: view|like|share, ts, sessionId }   (beacon, fire-and-forget)
GET  /v1/top?window=1h&category=tech&region=US&k=20  → { articles[], computedAt }
```

**Data model**

| Entity | Shape | Store |
|---|---|---|
| Raw events | append-only log, partitioned by time | Kafka, then object storage (S3/Parquet) |
| Bucket counts | `(bucketStart, dimensionKey, articleId) → count` | Redis / stream processor state |
| Rolling window totals | `window:{1h}:{category}` → sorted set (articleId → score) | Redis |
| Top-K snapshot | `topk:{window}:{category}:{region}` → list of (articleId, score) + `computedAt` | Redis, fronted by CDN |
| Article metadata | `articleId → title, url, image, category` | DB + cache |

---

## 4. High-level architecture

```
                         INGEST PATH (async, never blocks the page)
Client beacon → CDN/LB → Event Collector (stateless) → Kafka "article.events"
                              │ validate, bot-filter, local pre-aggregate       (partition by articleId)
                              ▼
                        Stream Processor (Flink/Kafka Streams)
                          1. dedupe + enrich (category, region)
                          2. 1-min tumbling buckets per (article, dimension)
                          3. emit bucket counts ─► Window Store (Redis)
                              │
                        Rolling window sorted sets (add new bucket, subtract expired)
                              │
                        Top-K Computer (every 10-60s): ZREVRANGE 0..K-1
                              ▼
                        Snapshot Store (Redis)  ──►  CDN (TTL 30-60s)
                              ▲
                         READ PATH
Client → CDN → LB → Top-K Service → snapshot (Redis) → hydrate metadata (article cache) → response

Raw events ──► S3 (data lake) ──► Batch job (recompute, backfill, reconcile, all-time/7d)
```

---

## 5. The core deep dive: counting and ranking over a window

This is where interviewers probe. Walk through the options and name what breaks.

### Naive: `SELECT articleId, COUNT(*) ... GROUP BY ... ORDER BY ... LIMIT K`
At 50K events/sec, scanning a window of billions of rows on every request is impossible. Even with an index, it runs for every reader. Dead on arrival.

### Option A: Single counter per article + a heap
Maintain `count[article]` and a min-heap of size K.
- **Works for all-time top-K:** counts only go up, so the heap is correct.
- **Breaks for sliding windows:** old events must *expire*, which decrements counts. An article that fell out of the heap may now belong back in it, and you've thrown away its count. You need counts for everything.

### Option B: Bucketed sliding window (my recommended answer)
- Divide time into **1-minute tumbling buckets**. Count events per `(bucket, article)`.
- Maintain a **rolling sorted set** for each window (e.g. 1h): when a bucket closes, `ZINCRBY` the new bucket's counts into the rolling set, and subtract the expired bucket's counts (the one now > 60 minutes old).
- Top-K is then `ZREVRANGE rolling 0 K-1`, which is O(log N + K).
- Windows of 24h and 7d reuse the same idea with coarser buckets (hourly, daily). Don't use 1-minute buckets for 7 days.

**Why not union 60 sorted sets on every refresh?** `ZUNIONSTORE` over 60 sets of ~1M members is tens of millions of operations per refresh. Incrementally adding and subtracting touches only the articles in the buckets being added and removed.

**Trade-offs:**
- **Bucket granularity:** finer buckets mean smoother windows but more state and more merge work. Coarser buckets are cheaper but the window edge is blurrier (a "1h" window is really 60 to 65 minutes).
- **Flink's built-in sliding (HOP) windows** do this for you, but each event belongs to size/slide windows (60 copies), which inflates state and CPU. Manual bucketing is cheaper.

### Option C: Approximate heavy hitters (when cardinality is huge)
- **Count-Min Sketch (CMS):** a 2D array of counters with d hash functions. Add: increment d cells. Estimate: take the min across d cells. It only *overestimates*, and error is bounded (ε·N with probability 1−δ).
- **Space-Saving / Misra-Gries:** keep only k counters, evicting the smallest. Deterministic error bounds, tiny memory.
- **Key properties:** CMS is **mergeable** (add arrays) and **linear** (you can subtract an expired bucket's sketch), so bucketed windows still work. Pair it with a small candidate heap.
- **When to use:** billions of distinct keys (search queries, hashtags, IPs). **For ~1M articles, exact counting is cheap enough, so I'd choose exact and mention the sketch as the escape hatch.**
- **Trade-offs:** bounded error and far less memory, versus the approximation, harder debugging, and a candidate set that can miss rising items.

### Distributed correctness: how do you merge top-K across partitions?
- **Partition by `articleId`:** all events for an article land on one worker, so its count is complete. The **global top-K is the merge of each worker's local top-K** (K × P candidates, pick the top K). This is exact.
- **Partition randomly:** one article's count is split across workers, so local top-Ks can be wrong. You'd need to merge full counts or mergeable sketches.
- **Trade-off:** partitioning by articleId has exact merges but creates **hot partitions** for viral articles (see below).
- **Per-dimension shortcut:** if each article belongs to exactly one category, the global top-K is a subset of the **union of per-category top-Ks**. Compute per category, then merge. This avoids recomputing every combination from scratch.

### The hot-key problem (a viral article)
One article can receive a large share of all events, overwhelming one Kafka partition and one worker.
- **Pre-aggregate before the network:** the collector (or a local combiner in the stream job) sums events over ~1-5 seconds and emits `(article, +N)` rather than N events. This cuts load by orders of magnitude for hot keys.
- **Two-stage aggregation:** stage 1 aggregates by `(articleId, salt 0..S)`, stage 2 sums the S partials per article. This spreads the load across workers.
- **Trade-off:** a few seconds of extra latency and more moving parts, in exchange for removing the hottest bottleneck.

---

## 6. Ingest path, hop by hop

### 1. Client beacon and Event Collector
- Use `navigator.sendBeacon` or an async call, so it **never blocks page render**.
- The collector is stateless, validates the schema, **acks immediately**, and buffers to Kafka.
- **Reliability:** run across AZs with autoscaling. If Kafka is slow, buffer briefly in memory, then **drop**. Losing a small fraction of view events is acceptable for popularity ranking. Never make the user's page wait.
- **Trade-off:** fire-and-forget loses some events and has no per-event confirmation. For ranking, that's fine. For billing or ad impressions it wouldn't be.

### 2. Abuse and bot filtering (often skipped, and it matters)
Popularity rankings get gamed (view farms, bot refresh loops).
- **Dedupe:** count one view per `(session/user, article)` per short window, using a Redis `SETNX` with TTL or a Bloom filter in the stream job.
- **Bot signals:** known bot user-agents, datacenter IPs, abnormal event rates per IP/session, signed or tokenized events.
- **Weighting:** share > comment > like > view, which is harder to fake at scale than raw views.
- **Unique viewers:** count distinct viewers per article with **HyperLogLog** (mergeable per bucket, ~12 KB per counter) instead of raw views.
- **Trade-off:** stronger filtering improves integrity but adds latency and a dependency, and risks dropping real traffic. Do cheap checks inline and deep fraud analysis asynchronously (retroactively subtract flagged traffic by recomputing the affected buckets).

### 3. Kafka
- Partition by `articleId`. RF=3, retention of several days to allow replay.
- **Monitor consumer lag.** Lag is the staleness of the ranking, so it's the key SLI for this path.
- **Trade-off:** the queue decouples producers from the aggregator, absorbs traffic spikes (breaking news), and enables replay. The cost is added latency (usually < seconds) and operational overhead.

### 4. Stream processor (Flink / Kafka Streams)
- Reads events, dedupes, enriches with category/region (cached lookup), and aggregates into one-minute buckets using **event time** with **watermarks**.
- **Event time vs. processing time:** mobile clients batch and send late, so event time gives correctness. Allow ~1-2 minutes of lateness. Events later than that are dropped from the stream result and picked up in batch reconciliation.
- **Checkpointing:** state is snapshotted periodically, so a crash resumes from the last checkpoint and replays from Kafka.
- **Idempotent sinks (important):** write **absolute bucket totals** (`SET bucket:article = N`), not increments (`INCR`). Then replays after a crash or at-least-once redelivery can't double-count.
- **Trade-offs:** true exactly-once is expensive. At-least-once plus idempotent sinks gives the same practical result more cheaply.

---

## 7. Computing and serving the Top-K

### Top-K Computer
- Every 10-60 seconds, for each `(window, dimension)`, run `ZREVRANGE rolling 0 K-1 WITHSCORES` and write the result as one small snapshot key with `computedAt`.
- Write snapshots **atomically** (write a new key then swap, or use a versioned key) so readers never see a half-written list.
- **Trade-off, refresh interval:** shorter means fresher but more compute and cache churn. Popularity rarely changes meaningfully in 5 seconds, so 30-60s is usually plenty.

### Read path
1. **CDN** caches the response for 30-60s. Since every user sees the same list, hit rates are near 100% and origin sees a trickle.
2. **Top-K Service** (stateless) reads the snapshot from Redis. A single key read, with an in-process L1 cache for a few seconds.
3. **Hydrate** titles, thumbnails and URLs from the article metadata cache. This is K = 20 items in one batched multi-get. Better still, **store the hydrated response in the snapshot** so reads are a single fetch. Refresh metadata with the snapshot.
4. Return `computedAt` so clients and monitors can see staleness.

**Reliability:** if the pipeline stalls, **serve the last good snapshot** (stale but available) and alert on `now − computedAt`. If a dimension key is missing, fall back to the global list. The read path depends only on CDN, Redis snapshots, and the metadata cache. It's statically stable with respect to Kafka, Flink, and the abuse checks.

**Trade-off:** pre-computing every `(window × category × region)` combination is cheap to read but multiplies compute and storage. Limit the combinations to what the product actually shows, and compute rare ones on demand with a short TTL.

---

## 8. Popular vs. trending (a common follow-up)

"Most views in 24h" favors evergreen content that's always near the top. **Trending** means *rising fast relative to baseline*.

- **Time-decayed score:** `score = Σ weight × e^(−λ·age)`. It can be maintained incrementally: store `(score, lastUpdated)` and decay lazily on update. No buckets are needed, and recent events count more.
- **Velocity ratio:** `views in last 15 min ÷ (average per 15 min over the past 24h)`. It needs a minimum-volume threshold to avoid tiny articles jumping from 1 view to 5.
- **Z-score / anomaly:** compare current rate to the article's historical mean and variance.
- **Trade-offs:** trending surfaces breaking news but is noisier and easier to spike with a few bots. Popular is stable but stale. Many products show both as separate tabs, or blend them.

---

## 9. Lambda vs. Kappa: the batch layer

- **Streaming path:** fast, approximate-ish, with the late-event and bot-filter limits above.
- **Batch path:** raw events in S3, with a Spark job that recomputes exact counts hourly or daily, applies full fraud filtering, and **overwrites or reconciles** the stream's numbers. Longer windows (7d, 30d, all-time) can be served entirely from batch plus the streaming delta for the latest hours.
- **Lambda** (both paths) gives accuracy plus freshness, at the cost of maintaining two code paths. **Kappa** (stream only, replay Kafka for reprocessing) is simpler, but replaying a long history is slow and retention-bound.
- **My pick:** streaming for short windows, batch for long windows and corrections. That's a pragmatic hybrid, and the operational duplication is limited to the aggregation logic.

---

## 10. Multi-region

- **Counts are additive**, so each region aggregates locally and a global aggregator sums the regional partial results (or mergeable sketches). Per-region top-K is purely local.
- **Alternatives:** ship all raw events to a central region (simple and exact, but costly in cross-region bandwidth and a single point of failure), or aggregate regionally and replicate only the small bucket tables.
- **Trade-off:** global rankings lag by the replication delay, which is acceptable here.

---

## 11. Failure scenarios

| Failure | Impact | Mitigation |
|---|---|---|
| Collector instance dies | Some events lost | Stateless, LB ejects it, client beacons are best-effort |
| Kafka slow or down | Counts stall | Collector buffers briefly then drops; serve last snapshot; alert on staleness |
| Stream worker crashes | Temporary lag | Restart from checkpoint, replay from Kafka, idempotent sinks avoid double count |
| Redis shard fails | Part of the window state lost | Replica failover; rebuild buckets from Kafka/S3 replay |
| Hot (viral) article | Partition/worker overload | Local pre-aggregation, salted two-stage aggregation |
| Late or out-of-order events | Slightly wrong buckets | Watermarks with allowed lateness; batch reconciliation |
| Bot attack or gaming | Fake articles in top-K | Dedupe, rate limits, weighted signals, anomaly detection, retroactive subtraction |
| Entire pipeline down | Rankings freeze | Serve last good snapshot (with `computedAt`), never an error |
| Bad deploy changing scoring | Wrong ranking | Canary, compare new vs. old output offline, rollback |
| Metadata service down | No titles/images | Store hydrated snapshots; fall back to cached metadata |
| Takedown (illegal or removed article) | Removed content stays on the list | Blocklist filter applied at read time, plus CDN purge |

**Principle (static stability):** the read path depends only on a precomputed snapshot. Everything upstream can be slow or down, and users still see a (slightly stale) list.

---

## 12. Consistency decisions

- **Counts:** eventually consistent (seconds). Favor availability and cost.
- **Ranking:** a stale-by-a-minute list is fine. Staleness is exposed as `computedAt`, not hidden.
- **Takedowns and safety:** enforced at read time via a blocklist, not only by waiting for the next recompute. This is the exception where staleness matters.
- **Public/paid rankings:** if exactness matters, use batch-reconciled counts as the source of truth.

---

## 13. Scaling summary

| Component | Strategy |
|---|---|
| Collector | Stateless, autoscaled, multi-AZ, local pre-aggregation |
| Kafka | Partition by articleId, RF=3, retention for replay |
| Stream processing | Parallelism by partition, two-stage aggregation for hot keys, checkpoints |
| Window state | Redis sorted sets (sharded by dimension), bucketed, incremental add/subtract |
| Snapshots | Small keys, replicated, CDN-fronted |
| Raw log | Object storage (S3/Parquet) for batch and audit |
| Read path | CDN + L1 + Redis; reads cost essentially nothing |

---

## 14. Likely follow-up questions

- **"How would you scale to 100x events?"** Local pre-aggregation, more Kafka partitions, salted two-stage aggregation, and if cardinality explodes, switch to CMS + Space-Saving instead of exact counts.
- **"Why not just use a database `GROUP BY`?"** It re-scans the window per request. Pre-aggregation makes the read O(K).
- **"How do you handle a sliding window exactly?"** Bucket it, adding new buckets and subtracting expired ones. Mention the cost of window granularity.
- **"What if the Top-K must be per-user personalized?"** Take the global top-N candidates (~200), then re-rank per user at read time with user features. Don't compute per-user top-K from scratch.
- **"How do you prevent manipulation?"** Dedupe, weighted signals, unique viewers via HLL, bot detection, anomaly alerts, and retroactive correction through the batch path.
- **"What if the counter store loses data?"** Rebuild by replaying Kafka (within retention) or from S3. This is a reason to keep raw events as the source of truth.
- **"Exact or approximate?"** Exact at ~1M articles, since it's only ~500 MB of state. Approximate when cardinality is in the billions.
- **"How is the 'all-time' top-K different?"** Counts only increase, so a heap/sorted set works directly, but it's a long-lived state that must be periodically reconciled against the batch result.

---

## 15. How to narrate it

1. **Name the dominant problem:** shared-result aggregation, not per-user reads. The answer is tiny and the same for everyone, so precompute and cache.
2. **Start naive** (`GROUP BY` on a database), show why it fails, then introduce streaming counters, then bucketed sliding windows, then discuss hot keys, then sketches as the escape hatch.
3. **Separate the paths:** ingestion is async and lossy, serving is a snapshot read and always available.
4. **Call out the knobs:** bucket size, refresh interval, lateness window, event weights, K, decay rate.
5. **For each stage, say what happens on failure**, and end with "the pipeline can fail and users still see a slightly stale list."

**Patterns reused from our toolbox:** queues with idempotent sinks, hot-key mitigation, caching layers with a CDN, probabilistic structures (CMS, HLL, Bloom filter), batch vs. stream (Lambda/Kappa), graceful degradation, and static stability.

Next, I can play interviewer on this (for example, "one article gets 30% of all traffic and your Flink job is lagging, walk me through it"), or we can move to the next question. A **rate limiter** or **chat system** would be a good contrast, since they're latency-critical per-request problems rather than aggregation pipelines.
