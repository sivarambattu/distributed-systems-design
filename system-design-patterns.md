# Reusable System Design Patterns

Most system design questions (Twitter feed, chat, Uber, Dropbox, rate limiter, web crawler) are remixes of about 15-20 building blocks. If you know each block's problem, solution, and trade-off, you can assemble most designs on the fly.

## The universal framework

1. **Clarify** functional and non-functional requirements (scale, latency, consistency, availability).
2. **Estimate** QPS, storage, and bandwidth. This tells you whether you have a *read problem*, a *write problem*, a *storage problem*, or none of them.
3. **Define** the API and data model.
4. **Draw** the simple version, then find the bottleneck and fix it with a pattern below.
5. **Discuss** failures, trade-offs, and evolution.

The key skill is **diagnosing which problem you have**, then picking the matching pattern.

---

## 1. Scaling patterns

### Stateless services + horizontal scaling
- **Problem:** one server can't handle the load or survive a failure.
- **Solution:** keep servers stateless (state lives in DB, cache, or tokens) and put them behind a load balancer.
- **Trade-offs:** it's simple and resilient, but state must go somewhere, which adds network hops. Sticky sessions are easier but hurt failover and balance.

### Database replication
- **Problem:** read load, or the DB is a single point of failure.
- **Solution:** leader-follower replication, with reads going to followers.
- **Trade-offs:**
  - **Sync vs. async replication:** sync is durable but slow and less available. Async is fast, but you can lose recent writes on failover and serve stale reads.
  - **Read-your-writes:** route a user's own reads to the leader briefly after they write.
  - Multi-leader and leaderless setups improve write availability but introduce conflict resolution.

### Sharding (partitioning)
- **Problem:** data or write throughput exceeds one machine.
- **Solution:** split data across nodes by key.
  - **Hash-based:** even distribution, but range queries are expensive.
  - **Range-based:** efficient scans, but prone to hot shards (e.g., timestamp keys).
  - **Directory-based:** flexible, but the lookup service becomes a dependency.
- **Trade-offs:** cross-shard joins and transactions get hard, and resharding is painful (consistent hashing and virtual nodes help). A bad shard key creates hotspots, so pick one with high cardinality that matches your dominant access pattern.

### Consistent hashing
- **Problem:** adding or removing nodes with plain `hash % N` remaps almost every key.
- **Solution:** place nodes and keys on a ring, so only ~1/N of keys move. Virtual nodes smooth the distribution.
- **Used in:** caches, sharded DBs, load balancing, CDNs.

### Separate read and write paths (CQRS)
- **Problem:** reads and writes have very different shapes and scale.
- **Solution:** a write-optimized store plus read-optimized views (denormalized tables, search indexes, caches), synced asynchronously.
- **Trade-offs:** reads are fast and independently scalable, but you accept eventual consistency and operational complexity.

---

## 2. Caching patterns

| Pattern | How it works | Trade-off |
|---|---|---|
| **Cache-aside** | App checks the cache, on a miss reads the DB and fills the cache | Simple and tolerates cache failure, but misses are slow and data can go stale |
| **Read-through** | The cache layer loads from the DB itself | Cleaner app code, but the cache must know the DB |
| **Write-through** | Write to cache and DB together | Fresh cache, but slower writes and pollution with unread data |
| **Write-back** | Write to cache, flush to DB later | Very fast writes, but you can lose data if the cache dies |

**Recurring cache problems:**
- **Stampede (thundering herd):** many requests miss at once. Fix with request coalescing, TTL jitter, and locking.
- **Penetration:** requests for nonexistent keys. Fix with negative caching and Bloom filters.
- **Hot key:** one key overloads one shard. Fix with local caches, key replication, or key splitting (`key#1..N`).
- **Invalidation:** "the two hard things." Use TTLs as a safety net plus explicit invalidation on writes.

**Layers:** browser → CDN → API gateway → app (L1) → distributed cache (L2) → DB. Each layer trades freshness for speed.

---

## 3. Consistency and distributed data

### CAP and PACELC
- During a partition you choose **consistency or availability**. Otherwise you trade **latency vs. consistency**.
- **Say it concretely:** "Payments need strong consistency, so I'd accept unavailability during partitions. A feed or like counter can be eventually consistent, so I'd favor availability and latency."

### Consistency spectrum
Strong → linearizable → read-your-writes → monotonic reads → eventual. Match the level to the *business cost of staleness*.

### Quorums
With N replicas, write to W and read from R. If **R + W > N**, reads overlap the latest write. It's tunable: low W gives fast writes, and low R gives fast reads.

### Distributed transactions
- **2PC:** atomic across services, but it blocks on coordinator failure and is slow. Avoid it across services where possible.
- **Saga:** a sequence of local transactions with compensating actions on failure. It's available and scalable, but only eventually consistent, and compensation logic is hard to get right.
- **Transactional outbox:** write the business data and an "event to publish" in the same DB transaction, then a relay publishes it. This solves the "update DB *and* send message" dual-write problem.

### Consensus and coordination
- **Raft/Paxos** (via etcd, ZooKeeper, Consul) handle leader election, distributed locks, config, and metadata.
- **Rule of thumb:** use consensus for small, critical metadata, never for the high-volume data path.
- **Distributed locks** need fencing tokens, because a paused client can wake up and believe it still holds the lock. Leases with expiry reduce the risk but don't eliminate it.

---

## 4. Asynchronous processing

### Message queues and logs (Kafka, SQS, RabbitMQ)
- **Problem:** slow or spiky work blocks users, or services are tightly coupled.
- **Solution:** producers enqueue, and consumers process at their own pace.
- **Benefits:** decoupling, load smoothing (buffering), retries, and fan-out.
- **Trade-offs:**
  - **Delivery semantics:** at-most-once (may lose), at-least-once (may duplicate), exactly-once (expensive, usually "at-least-once plus idempotent consumers").
  - **Ordering:** guaranteed only within a partition, so choose the partition key carefully.
  - **Queue vs. log:** queues delete after consumption. Logs retain data and allow replay and multiple consumer groups.
  - It adds latency, operational burden, and the need for monitoring (consumer lag).

### Idempotency
- **Problem:** retries and at-least-once delivery cause duplicate effects (double charges).
- **Solution:** idempotency keys, deduplication tables, and conditional writes.
- **Apply it to:** every write API that clients may retry, and every queue consumer.

### Backpressure and dead-letter queues
- Bound queues, shed or slow producers when consumers lag, and send poison messages to a DLQ after N retries so they don't block the stream.

---

## 5. Reliability patterns

| Pattern | Purpose | Watch out for |
|---|---|---|
| **Timeouts** | Don't wait forever | Too tight causes false failures, too loose ties up resources |
| **Retries + exponential backoff + jitter** | Survive transient errors | Retry storms, and non-idempotent operations |
| **Circuit breaker** | Stop calling a failing dependency | Tuning thresholds, and what to return instead |
| **Bulkheads** | Isolate resource pools so one failure doesn't spread | Lower utilization from partitioned pools |
| **Load shedding** | Reject excess work early | Deciding who to drop (prioritize critical traffic) |
| **Graceful degradation** | Serve a reduced experience (stale data, no recommendations) | Needs designing up front |
| **Health checks + failover** | Remove bad nodes automatically | Flapping, and split-brain |
| **Replication across AZ/region** | Survive infrastructure loss | Cost, and consistency complexity |

**Key idea: static stability.** The critical path should depend on as few components as possible, and keep working when auxiliary systems (analytics, auth refresh, recommendations) are down.

**Define SLOs** (e.g., 99.99% availability, p99 < 100ms) and use them to justify design choices.

---

## 6. Traffic management

### Load balancing
- **L4 vs. L7:** speed vs. content-aware routing.
- **Algorithms:** round-robin, least-connections, consistent hashing (for cache locality, with hot-spot risk).
- The LB itself must be redundant.

### Rate limiting
- **Algorithms:**
  - **Token bucket:** allows bursts and is the most common.
  - **Leaky bucket:** smooths output.
  - **Fixed window:** simple, but allows 2x bursts at window edges.
  - **Sliding window:** accurate, but costs more memory.
- **Distributed:** a central Redis counter is accurate but adds a hop and a dependency. Local counters with periodic sync are faster but approximate.
- **Failure policy:** fail-open (availability) or fail-closed (protection)?

### CDN
Offloads static and cacheable content. The trade-off is staleness versus offload, and you need a purge strategy.

### Service discovery and API gateway
The gateway centralizes auth, rate limiting, and routing, but can become a bottleneck or single point of failure if it isn't redundant.

---

## 7. Storage selection

| Need | Typical choice | Why / trade-off |
|---|---|---|
| Relational, transactions, complex queries | PostgreSQL / MySQL | Strong guarantees, but harder to scale writes |
| Massive key-value, simple access | DynamoDB, Cassandra, Redis | Scales horizontally, but weak ad-hoc queries |
| Document, flexible schema | MongoDB | Flexible, but weaker joins and consistency options |
| Full-text and faceted search | Elasticsearch / OpenSearch | Fast search, but eventually consistent and not a source of truth |
| Large blobs (images, video) | Object storage (S3) + CDN | Cheap and durable, but high latency per object. Store metadata elsewhere |
| Time-series / metrics | Prometheus, InfluxDB, TimescaleDB | Efficient for time data, limited general queries |
| Analytics / OLAP | ClickHouse, Druid, BigQuery | Fast aggregates, poor point updates |
| Graph relationships | Neo4j or adjacency tables | Great traversals, harder to shard |
| Geospatial | PostGIS, geohash/quadtree/S2 indexes | Efficient proximity queries |

**Principle:** use **polyglot persistence** (each store for what it's best at), but keep **one source of truth** and derive the rest (search indexes, caches) from it.

**Storage engines:** B-tree (read-optimized, in-place updates) vs. LSM-tree (write-optimized, background compaction). Cassandra and RocksDB use LSM, while most relational DBs use B-trees.

---

## 8. Common problem-specific patterns

### ID generation
- UUID: no coordination, but large and unsortable.
- Auto-increment: simple, but a bottleneck and guessable.
- **Snowflake-style** (timestamp + machine ID + sequence): sortable, scalable, no central call, but needs clock discipline.
- Range leasing, as in our URL shortener.

### Fan-out (feeds, notifications)
- **Fan-out on write (push):** precompute each follower's feed. Reads are fast, but writes are expensive for users with millions of followers.
- **Fan-out on read (pull):** compute at read time. Writes are cheap, but reads are slower.
- **Hybrid:** push for normal users, pull for celebrities. This is the classic Twitter answer.

### Real-time communication
- Polling (simple, wasteful), long polling, **SSE** (server to client), **WebSockets** (bidirectional).
- WebSockets are stateful, so you need a connection registry or pub/sub to route messages to the right server, and you need to handle reconnects and presence.

### Large file handling
Chunked and resumable uploads, direct-to-object-store via pre-signed URLs (so bytes bypass your servers), content-addressed dedupe, async processing (thumbnails, transcoding) through queues.

### Search and ranking
Inverted index, sharding by document or by term, an indexing pipeline fed by the source of truth, and a freshness vs. cost trade-off.

### Geo and proximity
Geohash, quadtree, or S2/H3 cells. Geohash is simple but has edge-boundary issues, so query neighboring cells.

### Approximate and probabilistic structures
- **Bloom filter:** "definitely not present" checks (false positives possible).
- **HyperLogLog:** approximate distinct counts.
- **Count-Min Sketch:** approximate frequencies and top-K.
- Use these when exactness isn't worth the memory or latency.

### Batch vs. stream processing
Batch (Spark) is simple and accurate. Stream (Flink, Kafka Streams) gives low latency. **Lambda** runs both (duplicate logic), **Kappa** uses streaming only (simpler, but replay-dependent).

### Scheduling and delayed work
Delay queues, time-bucketed tables, or hierarchical timing wheels. The hard parts are exactly-once execution and handling scheduler failover.

### Data deduplication and sync
Content hashing, versioning with vector clocks or last-write-wins (LWW), and CRDTs for conflict-free merging. LWW is simple but silently loses concurrent updates.

---

## 9. Trade-off vocabulary

These come up in nearly every design discussion:

- **Latency vs. throughput:** batching improves throughput but adds latency.
- **Consistency vs. availability/latency:** stronger guarantees cost speed and uptime.
- **Space vs. time:** precompute and denormalize (fast reads, more storage) vs. compute on demand.
- **Read vs. write optimization:** indexes and caches speed reads but slow writes.
- **Simplicity vs. flexibility:** monolith vs. microservices. Start simple and split when needed.
- **Cost vs. redundancy:** every extra 9 of availability costs substantially more.
- **Freshness vs. offload:** caching and CDNs.
- **Push vs. pull:** who does the work, and when.
- **Exactness vs. approximation:** precise counts vs. probabilistic structures.

---

## 10. Mapping patterns to classic questions

| Question | Key patterns |
|---|---|
| **URL shortener** | ID generation, caching, KV store, read-heavy scaling |
| **News feed / Twitter** | Fan-out (hybrid), caching, sharding, async queues |
| **Chat / WhatsApp** | WebSockets, pub/sub, message ordering, presence, offline delivery |
| **Rate limiter** | Token bucket, distributed counters, fail-open vs. closed |
| **Web crawler** | Queues, politeness, dedupe (Bloom filter), partitioning by domain |
| **Uber / proximity** | Geospatial indexing, real-time location streams, matching |
| **Dropbox / Drive** | Chunking, dedupe, object storage, sync and conflict resolution, metadata DB |
| **YouTube / Netflix** | Object storage, CDN, async transcoding pipeline, adaptive bitrate |
| **Notification system** | Queues, fan-out, idempotency, retries, per-channel rate limits |
| **Payment system** | Idempotency, strong consistency, saga/outbox, ledger, reconciliation |
| **Distributed cache / KV store** | Consistent hashing, replication, quorum, eviction, failure detection |
| **Typeahead / search** | Trie or inverted index, caching top queries, offline aggregation |
| **Ticket booking** | Inventory locking, holds with expiry, contention handling, idempotency |
| **Metrics / logging system** | Stream ingestion, time-series storage, downsampling, alerting |

---

## 11. Numbers worth memorizing

| Item | Rough value |
|---|---|
| Memory read | ~100 ns |
| SSD random read | ~100 µs |
| Same-datacenter round trip | ~0.5 ms |
| Cross-continent round trip | ~100-150 ms |
| Disk seek (HDD) | ~10 ms |
| Seconds per day | ~86,400 (≈ 10⁵) |
| Single Redis node | ~100K ops/sec |
| Typical relational DB node | ~1-10K QPS (workload-dependent) |
| Availability 99.9% / 99.99% | ~8.8 hrs / ~53 min downtime per year |

Use them to sanity-check ("one cross-region call per request blows a 50ms budget").

---

## How to practice

1. **Pick a question and name the dominant problem** (read-heavy? write-heavy? real-time? consistency-critical?). That immediately narrows the patterns.
2. **For every component you add, say what it costs**: a cache adds staleness, a queue adds latency and ordering issues, sharding adds cross-shard complexity.
3. **Always ask "what if this fails?"** at each box in the diagram.
4. **Evolve rather than front-load:** start with the simplest design that works, then add patterns in response to specific numbers or failure modes.
5. **Practice by pattern recombination:** redesign the same question under different constraints (strong consistency, 100x scale, multi-region) and see which patterns change.

If you'd like to practice, pick any question from the table above and I'll play the interviewer, or we can go deeper on one area such as feed fan-out, distributed rate limiting, or consistency models.