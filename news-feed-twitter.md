# News Feed / Twitter Design

**What to say early:** Unlike the URL shortener, storage and raw QPS aren't the hard part here. The hard part is **fan-out**: one write (a tweet) must show up in the feeds of many readers, and reads vastly outnumber writes. The core question is *when* you do that work, at write time or at read time.

---

## 1. Requirements

**Functional**
- Post a tweet (text, optionally media)
- Follow/unfollow users
- View the **home timeline** (tweets from people I follow)
- View a **user timeline** (one person's tweets)
- Like/retweet/reply (counters and interactions)

**Non-functional**
- Feed load is fast (p99 < ~200-300ms) and highly available
- **Eventual consistency is acceptable.** A tweet appearing a few seconds late is fine.
- Highly read-heavy
- Handles skewed users (celebrities with 100M followers)

**Clarify with the interviewer:** Is the feed chronological or ranked? Are media and search in scope? Is it global or single-region?

---

## 2. Estimation (state assumptions)

- 200M DAU, each opens the feed ~10x/day → 2B feed reads/day ≈ **~23K/sec avg, ~100K/sec peak**
- 100M tweets/day ≈ **~1.2K writes/sec avg, ~5K peak**
- Average follower count ~200 → fan-out writes ≈ 1.2K × 200 = **~240K feed inserts/sec** (peak ~1M)
- Tweet metadata ~1 KB → 100 GB/day ≈ 36 TB/year, which is sharded-DB territory but not enormous
- Media is the real storage hog (TBs/day), so it goes in object storage and CDN
- Feed cache: 200M users × 800 tweet IDs × 8 bytes ≈ **1.3 TB**. It stores IDs only, so it fits in a Redis cluster.

**Takeaway to say aloud:** Read-heavy (~20:1), but the *write amplification* from fan-out is the real cost driver.

---

## 3. API and data model

```
POST /v1/tweets            { text, mediaIds[] }      → { tweetId }
POST /v1/follows/{userId}  / DELETE
GET  /v1/timeline/home?cursor=&limit=20              → { tweets[], nextCursor }
GET  /v1/users/{id}/tweets?cursor=&limit=20
POST /v1/tweets/{id}/like
```

**Pagination:** use **cursor-based** (last seen tweetId), not offset. Offsets shift as new tweets arrive and get slow on deep pages. Time-sortable IDs (Snowflake) make this natural.

**Core tables**

| Entity | Key / shape | Sharded by |
|---|---|---|
| Tweets | `tweetId (Snowflake) → userId, text, mediaRefs, createdAt` | tweetId (even distribution) |
| User timeline | `(userId, tweetId)` sorted desc | userId |
| Follows (two copies) | `following: (followerId → followeeId)` and `followers: (followeeId → followerId)` | each by its leading key |
| Home timeline (cache) | `userId → sorted list of tweetIds` | userId |
| Users, counters | `userId → profile`, `tweetId → likes, retweets` | id |

Storing follows in both directions is **denormalization for read speed**. Fan-out needs "who follows X," and feed generation needs "who does Y follow." The cost is double writes on follow/unfollow.

**Snowflake IDs** (timestamp + machine + sequence) are time-sortable, generated without coordination, and double as the sort key for timelines. They need clock discipline.

---

## 4. High-level architecture

```
                 ┌──────────────── WRITE PATH ────────────────┐
Client → CDN/WAF → LB → API Gateway → Tweet Service → Tweet DB (sharded)
                                          │ (outbox / event)
                                          ▼
                                        Kafka "tweet.created"
                                          ▼
                                   Fan-out Workers ──► Graph Service (followers)
                                          ▼
                                   Home-timeline cache (Redis, per user)

                 ┌──────────────── READ PATH ─────────────────┐
Client → CDN/WAF → LB → API Gateway → Feed Service
            1. fetch precomputed feed IDs (Redis)
            2. pull + merge celebrity tweets (hybrid)
            3. rank / filter (blocks, mutes, deleted)
            4. hydrate: tweets, authors, counts, media URLs (batched cache reads)
            → response

Media: client → pre-signed URL → Object Store (S3) → async processing → CDN
Side systems: Search indexer, Notifications, Trends, Analytics (all consume Kafka)
```

---

## 5. The core deep dive: fan-out

### Option A: Fan-out on write (push)
When Alice tweets, workers insert the tweet ID into every follower's home-timeline list.
- **Pros:** reads are trivial (one cache lookup), so the latency-critical path stays cheap.
- **Cons:** a celebrity with 100M followers triggers 100M writes per tweet, causing huge lag and wasted work, since most followers are inactive. It also writes for users who never read.

### Option B: Fan-out on read (pull)
At read time, fetch the latest tweets from everyone the user follows and merge.
- **Pros:** writes are cheap, no wasted work, and there are no celebrity write storms.
- **Cons:** reads are expensive. A user following 500 accounts means 500 lookups plus a merge on every refresh, which is hard to hold within a latency budget at 100K reads/sec.

### Option C: Hybrid (the standard answer)
- **Normal users** (say < 100K followers): push to followers' feeds.
- **Celebrities:** *don't* fan out. Their tweets live only in their own user timeline.
- **At read time:** fetch the precomputed feed, then pull recent tweets from the (few) celebrities this user follows, and merge.

**Trade-offs of hybrid:**
- Added complexity: two code paths, plus a threshold to tune. Use a threshold with hysteresis so accounts near the line don't flap.
- Read cost is bounded, because users follow only a handful of celebrities, and those timelines are heavily cached (one hot key, replicated).
- **Optimization:** only fan out to **active users** (e.g., seen in the last 30 days). Inactive users' feeds are rebuilt lazily by pull if they return. This can cut write amplification dramatically.

**State your decision:** "I'd go hybrid, push for the long tail, pull for celebrities, and skip inactive users. The threshold is a tunable knob I'd set from measured fan-out cost."

---

## 6. Write path, hop by hop

### 1. Edge, LB, API Gateway
- Auth, per-user rate limiting (token bucket) to stop spam, and request size limits.
- **Idempotency key:** clients retry on timeouts, so without one you get duplicate tweets.
- **Reliability:** redundant gateways across AZs. Writes are less latency-critical than reads, so they can tolerate slightly higher latency.

### 2. Tweet Service
- Validate (length, content policy), generate the Snowflake ID, write to the Tweet DB and the author's user timeline.
- **Acknowledge once the tweet is durably stored**, not after fan-out completes. Fan-out is async.
- **Trade-off:** acking before fan-out means followers may see the tweet seconds later (eventual consistency). Waiting for fan-out would make post latency unpredictable and tie availability to the fan-out system. Async is clearly right here.

### 3. Reliably publishing the event (the dual-write problem)
If the service writes to the DB *and then* publishes to Kafka, a crash in between loses the fan-out. Use the **transactional outbox**: write the tweet and an outbox row in one DB transaction, and a relay (or CDC) publishes to Kafka. This gives at-least-once publishing without a distributed transaction.

### 4. Kafka (tweet.created)
- Partition by `authorId` (keeps one author's tweets ordered) or by tweetId (better balance). Ordering across tweets barely matters since feeds sort by ID.
- Replication factor 3, `acks=all` for durability.
- **Monitor consumer lag.** Lag is *feed staleness*, so it's the key SLI for this path.
- **Trade-off:** the queue adds latency (usually < seconds) but buys buffering for write spikes (e.g., World Cup goals), decoupling, and replay.

### 5. Fan-out workers
- Look up the author's followers from the Graph Service (paginated, since there can be millions).
- Skip inactive users, and skip entirely if the author is above the celebrity threshold.
- Insert `tweetId` into each follower's feed in Redis, batched and pipelined, **trimmed to the latest ~800 entries**.
- **Idempotent by design:** use a sorted set with the tweetId as member, so replays after a crash or at-least-once redelivery cause no duplicates.
- Split large follower lists into chunks distributed across many workers, so one big job doesn't block the queue. Use separate queues or priority lanes so a celebrity-adjacent burst doesn't delay everyone else.
- **Failure behavior:** a failed chunk is retried. After N retries it goes to a DLQ, and the feed is repaired later by read-time reconciliation or a repair job.

**Trade-offs:**
- **Memory vs. depth:** capping at 800 entries bounds cost. Older pages fall back to pull-based reconstruction.
- **Store IDs only, not full tweets:** a tweet edit or delete doesn't require rewriting millions of feeds. The cost is a hydration step at read time.

---

## 7. Read path, hop by hop

### 1. Edge, LB, API Gateway
- CDN doesn't help much for personalized feeds, but it serves media and static assets, and it provides TLS and DDoS protection.
- L7 routing separates the Feed Service from the Tweet Service so they scale and fail independently.
- **Reliability:** multi-AZ, N+1 capacity, and load shedding. Feed reads are the dominant traffic.

### 2. Feed Service (the orchestrator)
Stateless. For each request:
1. **Fetch feed IDs** from Redis (`ZREVRANGEBYSCORE` below the cursor, limit 20-50).
2. **Fetch celebrity tweets:** get the list of celebrities the user follows (cached), then read their recent tweets from a hot cache.
3. **Merge** by time (or feed into ranking).
4. **Always include the user's own recent tweets** from their user timeline, which gives read-your-writes. Without it, a user posts and doesn't immediately see their own tweet, which feels broken even if followers' delivery is eventual.
5. **Filter:** deleted, blocked/muted, protected accounts, and policy-removed tweets.
6. **Hydrate** with parallel batched fetches (tweets, author profiles, like/retweet counts, media URLs).
7. Return the page and the next cursor.

**Reliability considerations:**
- **Tight timeouts and parallel fan-out of hydration calls.** Tail latency is dominated by the slowest dependency, so use hedged requests for slow calls where safe.
- **Graceful degradation:** if counts are unavailable, show tweets without counts. If ranking is down, fall back to chronological. If hydration of one tweet fails, skip that tweet instead of failing the page.
- **Circuit breakers and bulkheads** around each dependency, so a slow counter service can't exhaust the feed service's threads.

### 3. Home-timeline cache (Redis)
- **Structure:** a sorted set per user (score = tweetId or timestamp, member = tweetId), capped length.
- **Sharding:** by userId using consistent hashing, with replicas per shard and automatic failover.
- **Cache miss / cold start** (new device, evicted inactive user, cache loss): *rebuild* the feed by pulling recent tweets from the user's followings (with a cap on followings scanned), write it back, then serve. This is expensive, so protect it:
  - **Request coalescing** so concurrent loads for one user trigger one rebuild
  - **Rate limit and bound rebuild concurrency**, with a global budget so a cache failure can't crush the DB
  - Fall back to a smaller feed (fewer followings) under stress
- **Trade-off:** full-feed-in-cache means fast reads but a large memory footprint (~TBs) and complex rebuild logic. Caching only active users keeps cost sane.

### 4. Hydration layer (the unsung hot path)
A feed page of 20 tweets requires roughly 20 tweets, ~20 authors, and 20 counter sets, so use **batched multi-gets** against caches:
- **Tweet cache:** hot tweets by ID. Tweets are near-immutable, so cache hit rates are very high and invalidation is easy (delete → tombstone).
- **User cache:** profiles by ID.
- **Counter cache:** likes/retweets, which are mutable and approximate.
- **Trade-off:** more caches mean more hops and more memory, but the DBs cannot take ~2M+ lookups/sec directly. Misses fall through to the DB (with coalescing).

### 5. Ranking (if the feed isn't purely chronological)
```
Candidates (feed + celebrities + recommendations ~1000)
   → light ranker (cheap model, trims to ~200)
   → heavy ranker (ML model, features from feature store)
   → filters/diversity (dedupe, mix, safety) → top N
```
- **Trade-offs:** ranking improves engagement but adds latency (budget it, e.g. < 50-100ms), requires a feature store, and makes the feed harder to reason about and debug. Pre-computing scores offline is cheaper but staler.
- **Reliability:** the ranker is *non-critical*. If it times out, serve chronological.

---

## 8. Supporting components

### Social graph service
- Needs: "followers of X" (for fan-out), "who does Y follow" (for pull and filtering), follower counts, and "does A follow B."
- **Storage:** sharded relational (e.g., Vitess/MySQL) or a purpose-built graph store. Two tables, one per direction.
- **Caching:** following lists for active users are read on almost every feed request, so cache them. Follower lists are read during fan-out and paginated.
- **Hot issue:** celebrities have massive follower lists. Paginate and stream them to workers rather than loading them whole.
- **Consistency:** a follow takes effect eventually. After a new follow, backfill that account's recent tweets into the feed, or pull them at read time for a while. Unfollow should be filtered at read time right away, since stale posts after an unfollow feel like a bug.

### Media
- Client uploads **directly to object storage via pre-signed URL**, so large bytes never pass through app servers.
- Async pipeline (queue → workers) for resizing, transcoding, and virus/content scanning. The tweet references the media ID, and the tweet is only shown with media once processing completes.
- Served via **CDN**, which absorbs nearly all media traffic. Store only metadata in the DB.
- **Trade-off:** async processing delays media availability but keeps uploads fast and resilient. CDN adds cost and a purge need for takedowns.

### Likes, retweets, counters
- Naive `UPDATE count = count + 1` on one row **hot-spots** on viral tweets.
- **Fixes:** sharded counters (split into N sub-counters and sum on read), or buffer increments and batch-aggregate through Kafka/stream processing into the counter store. Store individual likes in a table for dedupe ("did I like this?") and the counter separately.
- **Trade-off:** exact counts versus throughput. Slightly stale or approximate counts are fine, and nobody can tell 1,002,411 from 1,002,398.

### Search, trends, notifications
All are **downstream consumers of the Kafka tweet stream**, which is the same decoupling pattern as everywhere else.
- **Search:** inverted index (Elasticsearch-style), sharded and eventually consistent. It's never the source of truth.
- **Trends:** stream processing with sliding windows plus Count-Min Sketch or approximate top-K.
- **Notifications:** separate fan-out with per-user rate limiting and dedupe.

---

## 9. Scaling summary

| Component | Strategy |
|---|---|
| Tweet DB | Shard by tweetId; replicate RF=3 across AZs; hot tweets served from cache |
| User timeline | Shard by userId (one author's tweets colocated for range scans) |
| Follow graph | Two sharded tables; cache hot lists |
| Home-timeline cache | Redis cluster sharded by userId, replicas, capped length, active users only |
| Fan-out | Kafka partitions + many stateless workers; chunked follower lists; priority lanes |
| Media | Object storage + CDN |
| Feed Service | Stateless, autoscaled, multi-AZ |

---

## 10. Failure scenarios

| Failure | Impact | Mitigation |
|---|---|---|
| Fan-out workers lag / Kafka backlog | Feeds stale | Autoscale consumers, priority lanes, alert on lag; read-your-writes covers the author |
| Redis shard fails | Those users' feeds miss | Replica failover; rebuild-on-miss with coalescing and rate limits |
| Entire feed cache lost | Massive rebuild load on DB | Throttle rebuilds, serve reduced feeds, warm gradually, protect DB with load shedding |
| Celebrity tweets | Write storm if pushed | Hybrid: no push for celebrities; hot-cache their timelines |
| Viral tweet | Hot key in tweet/counter cache | L1 caches, key replication, sharded counters, CDN for media |
| Graph service slow | Fan-out and filtering degrade | Cache following lists; skip non-essential filters; circuit breaker |
| Ranker down | Lower relevance | Fall back to chronological |
| Counter service down | Missing counts | Render without counts |
| DB shard down | Subset of tweets unreadable | Replicas, quorum reads, auto-failover; tweet cache covers hot content |
| Duplicate/replayed events | Duplicate feed entries | Idempotent sorted-set inserts keyed by tweetId |
| Region outage | Regional traffic lost | DNS/anycast failover, replicated data, feed rebuild in the surviving region |
| Tweet deleted | Stale copies in feeds | Don't rewrite feeds. Tombstone and filter at hydration, plus purge caches and search |

**Principle (static stability):** the critical path for reading a feed is *feed cache → hydration caches → DB*. Ranking, counters, recommendations, search, and analytics are all optional and must degrade gracefully.

---

## 11. Consistency decisions

- **Feed delivery:** eventual (seconds). Favor availability and latency.
- **Read-your-writes** for the author: merge their own tweets at read time.
- **Monotonic reads:** a user shouldn't see a tweet, refresh, and see it vanish. Pin a user to the same replica/cache region, or use cursors that move only forward.
- **Deletes and blocks:** these are the cases where staleness matters (privacy and safety), so enforce them at read time, not just via async cleanup.
- **Follow graph:** eventual, except unfollow/block, which should be filtered immediately.

---

## 12. Multi-region

- Users are routed to their nearest region. Tweets replicate asynchronously across regions.
- Each region has its own feed cache, built by region-local fan-out workers consuming replicated events.
- Writes either go to a user's **home region** (simpler, with cross-region latency on travel) or are accepted anywhere with Snowflake IDs avoiding collisions (more complex).
- **Trade-off:** more regions improve latency and survivability, but add replication lag and operational complexity. Cross-region lag means a follower in another region sees tweets slightly later, which is acceptable.

---

## 13. Likely follow-up questions

- **"A celebrity with 100M followers tweets, what happens?"** No push. Their tweet is written once, and hot-cached in their user timeline. Followers' feed services pull it at read time and merge. Notifications go through a separate rate-limited path.
- **"Why store tweet IDs, not full tweets, in the feed?"** Memory efficiency, and edits/deletes don't require rewriting millions of feeds. The cost is hydration reads.
- **"How do you handle a user who follows 10,000 accounts?"** Cap the scanned followings in cold rebuilds, rely on push (they're likely active), and apply ranking to prioritize.
- **"How would you make the feed ranked?"** Candidate generation, light and heavy rankers, a feature store, an offline training pipeline, and chronological fallback.
- **"What if fan-out falls behind during a huge event?"** Autoscale workers, apply priority lanes, shed fan-out to inactive users, and increase reliance on read-time pull for a while.
- **"How do you delete a tweet everywhere?"** Tombstone in the Tweet DB, filter at hydration, purge tweet cache/search index/CDN, and let capped feed entries age out.
- **"How do you test this?"** Load tests with a realistic follower distribution (power law), plus chaos tests such as killing a Redis shard or pausing the fan-out consumers.

---

## 14. How to narrate it

1. **Name the dominant problem:** read-heavy, with fan-out amplification and skewed users.
2. **Start with the naive design** (pull from the DB on read), show it fails at scale, and introduce push, then explain why pure push fails for celebrities, then land on hybrid. Interviewers love this progression.
3. **Separate the critical path** (feed read) from async work (fan-out, search, analytics), and state your consistency choices explicitly.
4. **Call out the knobs:** celebrity threshold, feed cap, active-user window, TTLs. They show you think about operating the system.
5. **For each box, say what happens when it fails.**

**Patterns this reused from our toolbox:** cache-aside plus rebuild, sharding with consistent hashing, Snowflake IDs, queues with idempotent consumers, the transactional outbox, hot-key handling, graceful degradation, and eventual consistency with targeted exceptions.

Next up, I can play interviewer on this design (for example, "fan-out is 10 minutes behind, walk me through your response"), or we can move to the next question in your list. Chat or Uber/proximity would be a nice contrast, since they're real-time and geospatial rather than feed-oriented.
