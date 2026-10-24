# Distributed Cache / KV Store Design

**What to say early:** This is the infrastructure we've treated as a black box in every earlier design ("put it in Redis"). Here you open the box. A distributed KV store is a **single-node storage engine** wrapped in **partitioning, replication, membership, and failure handling**. Almost every interesting question maps to one of five problems:

1. **Where does a key live?** (partitioning, routing)
2. **How many copies exist, and how do they stay in sync?** (replication, consistency)
3. **Who is alive, and who decides?** (failure detection, membership, failover)
4. **What do you do when memory runs out?** (eviction, TTL, persistence)
5. **What happens when the cluster changes?** (rebalancing, hot keys, thundering herds)

**Clarify the scope first**, because "cache" and "KV store" are different points on one spectrum, and the answer changes the design:

| | **Distributed cache** (Memcached, Redis as cache) | **Durable KV store** (DynamoDB, Cassandra, etcd) |
|---|---|---|
| Source of truth? | No, a copy of data elsewhere | **Yes** |
| Data loss acceptable? | Yes (a miss just reloads) | **No** |
| Storage | RAM | RAM + disk (LSM/B-tree), SSD |
| Eviction | Core feature (LRU/LFU, TTL) | Rare, TTL only |
| Replication | Optional (1 replica for availability) | Mandatory (RF=3, quorum) |
| Consistency | Eventual, best-effort | Tunable, sometimes strong |
| Failure mode | Degrade to a cold cache | Must preserve acknowledged writes |

**My framing:** I'll design a **Dynamo-style, tunable KV store** and then show which knobs turn it into a pure cache. That covers both interpretations. I'll call out the cache-specific parts (eviction, TTL, stampedes) explicitly.

---

## 1. Requirements

**Functional**
- `get(key)`, `put(key, value, ttl?)`, `delete(key)`
- Optional: atomic operations (`incr`, compare-and-set), batch ops (`mget`), TTL expiry
- Horizontal scalability (add nodes online) and fault tolerance
- Configurable consistency per request or per namespace

**Non-functional**
- **Latency:** p99 < ~1-5 ms in-datacenter (sub-ms for the cache tier)
- **Throughput:** millions of ops/sec in aggregate
- **Availability:** survives node, rack, and AZ loss without client-visible outage
- **Scalability:** linear scale-out, with minimal data movement when nodes join or leave
- **Durability** (store mode): acknowledged writes survive node failures
- **Predictability:** no multi-second pauses, and noisy tenants can't hurt others

**Clarify with the interviewer:**
- Cache or source of truth? Can we lose data?
- Value sizes (bytes vs. MBs)? Do we need range scans or only point lookups?
- Consistency needs: strong, or is eventual fine? Do we need transactions or CAS?
- Single region or multi-region? Multi-tenant?
- Read:write ratio and hot-key skew?

---

## 2. Estimation (state assumptions)

- **5M ops/sec peak**, 90% reads
- Average key 50 B, value 1 KB. **20 TB** hot dataset (about 20B entries)
- A node has 64 GB RAM, with ~48 GB usable for data (the rest goes to OS, fragmentation, and buffers)
- **Capacity:** 20 TB / 48 GB ≈ **~420 primaries.** With one replica each, **~850 nodes.**
- **Throughput:** a node handles ~50-100K ops/sec on 1 KB values (network-bound), so 5M ops/sec needs only ~50-100 nodes. **This workload is memory-bound, not CPU-bound.**
- **Per-key replication:** with RF=3 and W=2, each write costs ~3 network messages, so a 500K/sec write rate means ~1.5M replica writes/sec across the cluster.
- **Network:** 5M ops/sec × ~1 KB ≈ 40 Gbps aggregate, spread over hundreds of NICs, which is fine.
- **Memory overhead:** per-entry metadata (hash bucket, pointers, TTL, LRU bits) is ~50-100 B. For tiny values (< 100 B) overhead can double memory use, so state that.

**Takeaway to say aloud:** The data model is trivial. The cost drivers are **RAM capacity, tail latency, and failure behavior.** Sizing is dominated by memory, so **fragmentation, per-entry overhead, and replication factor** directly set the bill.

---

## 3. API and data model

```
GET    key                    → value, version | MISS
SET    key value [TTL] [IF_VERSION=v | NX | XX]     → OK | CONFLICT
DEL    key
MGET / MSET keys...            (batched, routed per node)
INCR / CAS / append            (atomic, single-key only)
Consistency: ONE | QUORUM | ALL   (per request, store mode)
```

**Stored record:** `key → { value (opaque bytes), version/timestamp, expiresAt, flags }`

**Design points:**
- Values are **opaque bytes.** The store doesn't interpret them, which keeps the engine simple.
- **Atomicity is per key only.** Multi-key transactions across partitions need 2PC or sagas, which are expensive. Say "single-key atomic, multi-key is out of scope or expensive" and offer **hash tags** (force related keys onto the same shard) as the workaround.
- **Max value size** is capped (e.g., 1 MB). Large values hurt tail latency, block the event loop, and cause fragmentation, so large objects belong in object storage with a reference.
- **Namespaces and quotas** per tenant for isolation.

---

## 4. High-level architecture

```
Clients (smart client library: cluster map cache, consistent hashing, pooling, retries)
   │  (or via a stateless Proxy tier: Envoy/mcrouter/twemproxy style)
   ▼
┌─────────────────────── DATA PLANE (sharded, replicated) ───────────────────────┐
│ Shard A: [Primary A] ─replicates─► [Replica A1] [Replica A2]   (different AZs)   │
│ Shard B: [Primary B] ─replicates─► [Replica B1] [Replica B2]                     │
│  ...   each node: network layer → command executor → in-memory index             │
│                   → eviction/TTL → WAL + snapshot (optional) → (LSM on SSD)      │
└──────────────────────────────────────────────────────────────────────────────────┘
        ▲ gossip / heartbeats (failure detection)        ▲ slot map + epochs
        └────────────────────────────────────────────────┴──── CONTROL PLANE
                                     Config/Coordination service (Raft: etcd/ZK)
                                     membership · slot assignment · failover orchestration
Side systems: Metrics/Monitoring · Admin/Rebalancer · Backup/Restore · Auth/ACL
```

**Separate the planes:** the **data plane** handles every request and must be fast and decentralized. The **control plane** holds small, critical metadata (who owns which slot) and tolerates low throughput, so a consensus store is appropriate there. This is the "consensus for metadata, never for the data path" rule from our patterns toolbox.

---

## 5. Core deep dive 1: partitioning and routing

### Naive: `node = hash(key) % N`
Adding or removing a node changes N, so **almost every key remaps.** In a cache that means a near-total miss storm. In a store it means moving nearly all the data. Dead on arrival.

### Consistent hashing with virtual nodes
- Place nodes (and keys) on a hash ring. A key belongs to the first node clockwise.
- Adding or removing a node moves only **~1/N of keys.**
- **Virtual nodes (vnodes):** each physical node owns many (e.g., 100-256) points on the ring. This **smooths load**, lets bigger machines take more vnodes, and **spreads a failed node's load across many peers** instead of dumping it on one neighbor.
- **Used by:** Dynamo, Cassandra, Memcached clients (ketama).

### Fixed hash slots (Redis Cluster style)
- Hash space is pre-divided into **16,384 slots** (`CRC16(key) mod 16384`). Slots are assigned to nodes, and **a slot is the unit of migration.**
- **Pros:** explicit, easy to reason about, simple to move (migrate slot by slot), and the mapping is a small table that every client can cache.
- **Cons:** a fixed slot count caps the max cluster size (16K nodes at one slot each, realistically ~1,000), and slots are a coarse balancing unit.

### Range partitioning
Good when you need **range scans** (Bigtable, HBase, Spanner). Prone to hot ranges (sequential keys), so it needs auto-splitting. For point-lookup KV and caches, hash partitioning wins.

**State your decision:** "Hash partitioning with fixed slots (or vnodes) because the workload is point lookups, load is evenly spread, and migration is controlled. Range partitioning only if scans are required."

### Routing: who knows where the key lives?
| Approach | How it works | Pros | Cons |
|---|---|---|---|
| **Smart client** | Client caches the slot map, connects directly to the owner | One hop, lowest latency, no extra tier | Logic in every language, stale maps need redirect handling, connection fan-out (N clients × M nodes) |
| **Proxy tier** | Clients talk to a stateless proxy that routes | Simple clients, central pooling and policy, isolates topology | Extra hop (~100s of µs), proxy is another fleet to run |
| **Server-side redirect** | Any node accepts, replies `MOVED slot → node` | No separate tier, simple clients | Extra round trip on stale maps |

- **Handling stale maps:** the client sends to the believed owner, and the node replies `MOVED` (permanent: refresh map) or `ASK` (temporary: slot being migrated, retry once on the target). Maps carry an **epoch/version** so clients can tell which is newer.
- **Common answer:** smart clients for latency-critical paths, with proxies where many languages or serverless clients make connection counts explode.
- **Batching:** `mget` of 100 keys becomes parallel per-node sub-requests. Tail latency is the **slowest node**, so fan-out amplifies tails (a 1% slow rate per node becomes ~63% for a 100-node fan-out). Mitigate with hedged requests and smaller fan-outs.

---

## 6. Core deep dive 2: replication and consistency

### Replication topologies

**Leader-follower (single leader per shard)**
- Writes go to the primary, which replicates to followers. Reads come from the primary (strong) or replicas (stale).
- **Async replication:** low latency, but **a failover can lose acknowledged writes** (writes the primary acked but hadn't shipped). Redis Cluster behaves this way.
- **Sync replication:** no loss, but write latency includes the slowest replica, and availability drops if replicas are down.
- **Semi-sync (W of N):** wait for k replicas, a middle path.
- **Pros:** simple, no conflicts, easy ordering and atomic ops (single writer per key).
- **Cons:** a failover event is required to recover write availability, so there's a short write outage.

**Leaderless quorum (Dynamo/Cassandra style)**
- A key is stored on N replicas (the next N nodes on the ring, in distinct failure domains). The client or a coordinator writes to **W** replicas and reads from **R**.
- **If R + W > N, reads overlap the latest successful write.** N=3, W=2, R=2 is the classic setting.
- **Tunable per request:** `W=1` for fast and lossy, `W=N` for durable and slow, `R=1` for fast reads, `R=quorum` for fresher reads.
- **No failover event:** any replica can serve, so a node failure is just "one fewer response." Excellent availability.
- **Cons:** conflicts need resolution, there's no single ordering, and atomic CAS across replicas needs extra protocol (Paxos-based lightweight transactions, which are slow).

**Consensus-based (Raft/Paxos per shard)**
- Each shard is a Raft group. Writes are committed on a majority, giving **linearizable** semantics and automatic leader election (etcd, CockroachDB, Spanner, TiKV).
- **Pros:** strong consistency with no data loss on failover. **Cons:** higher write latency (majority round trip), reduced availability in a minority partition, and operational complexity.

| | Async leader-follower | Leaderless quorum | Raft per shard |
|---|---|---|---|
| Consistency | Eventual (can lose acked writes) | Tunable, eventual by default | **Strong** |
| Write latency | Lowest | Low-medium | Medium (majority RTT) |
| Failure handling | Failover event | Seamless | Leader election |
| Conflicts | None | **Must resolve** | None |
| Best for | Caches | High-availability stores | Metadata, strong-consistency stores |

**State your decision:** "For the **cache tier**, async leader-follower with one replica, since loss is tolerable and I want simplicity and speed. For the **durable store tier**, leaderless quorum with N=3, W=2, R=2 for high availability, and Raft-based shards where strong consistency is needed (config and metadata)."

### Making leaderless replication converge
Quorums alone don't guarantee convergence. Dynamo adds three repair mechanisms:

1. **Sloppy quorum + hinted handoff.** If a replica is down, write to the next healthy node on the ring, which **holds a hint** and forwards it when the target returns. *Trade-off:* higher write availability, but a "quorum" may not include the key's real replicas, so reads can miss recent writes until the hints land.
2. **Read repair.** On a quorum read, if replicas return different versions, the coordinator returns the newest and **writes it back to stale replicas.** Cheap, but only repairs keys that are read.
3. **Anti-entropy with Merkle trees.** Replicas periodically compare **hash trees** over their key ranges, find the differing subranges quickly (log-time descent), and sync only those. This repairs cold keys that read repair never touches. *Trade-off:* background CPU and I/O, so throttle it.

### Conflict resolution: which write wins?
- **Last-write-wins (LWW)** by timestamp: simple and used by Cassandra. **Danger:** clocks skew, and concurrent writes silently lose one. Mitigate with **hybrid logical clocks (HLC)**, but it still drops concurrent updates.
- **Vector clocks:** track causality per replica. Detect true concurrency and keep **siblings** for the application (or a merge function) to resolve, as in the original Dynamo. **Cost:** clocks grow, clients must handle merges, and complexity is high.
- **CRDTs:** data types designed to merge automatically (counters, sets, LWW-registers). Great for specific structures like counters, but not general.
- **Single-writer-per-key** (leader-based) avoids conflicts entirely, which is why leader-based designs are simpler.

**Trade-off to name:** LWW is simple but lossy under concurrency. Vector clocks are correct but push complexity to the application. **For a cache, LWW (or just "last write from the primary") is fine. For a store used as a shopping cart (the Dynamo origin story), siblings and merge are justified.**

### Consistency model summary
- **Cache:** eventual. Replica reads can be stale, and that's acceptable.
- **Store (quorum):** tunable. `R+W>N` gives read-your-writes *in the absence of sloppy quorum and concurrent writes*, but it is **not linearizable** (clock skew, partial writes, and failed writes that appear later can all surface).
- **Strong:** needs consensus (Raft) or a single leader with synchronous replication.

---

## 7. Core deep dive 3: failure detection, membership, and failover

### Failure detection
- **Heartbeats with timeouts:** simple, but a fixed timeout is brittle (too short means false positives under GC pauses and network jitter, too long means slow detection).
- **Phi-accrual detector** (Cassandra/Akka): outputs a **suspicion level** based on the observed heartbeat distribution, adapting to network conditions instead of a binary alive/dead.
- **Gossip protocols:** each node periodically exchanges state with a few random peers, so membership and failure info **spreads in O(log N)** rounds with no central bottleneck. **Trade-off:** eventual convergence (seconds), but there's no single point of failure. A flaky node can still produce **false suspicions**, so require **multiple independent observers** (majority of masters) before declaring failure.

### Membership and the source of truth for the slot map
- **Option A: gossip only** (Cassandra, Redis Cluster's bus). Fully decentralized, but harder to reason about split-brain.
- **Option B: external coordination service** (etcd/ZooKeeper/Raft) stores the authoritative slot map and epoch. Nodes and clients watch it. **Cleaner correctness,** and an extra dependency (kept off the data path).
- **My pick:** a Raft-backed config service for the **authoritative topology**, and gossip or heartbeats for **liveness hints.** Data-plane reads and writes continue even if the control plane is briefly unavailable, because clients use their cached map (static stability).

### Failover (leader-follower mode)
1. A primary is suspected failed by a **majority** of observers.
2. Its replicas hold an **election.** The one with the **most up-to-date replication offset** wins, and the election is gated by a **new epoch number** (a fencing token).
3. The promoted replica announces the new ownership. The map updates, and clients are redirected.
4. **The old primary, if it comes back, must demote itself** (it sees a higher epoch) and resync as a replica, discarding writes the cluster didn't accept.

**Split-brain prevention:**
- **Majority quorum** for failover decisions, so the minority side can't promote anyone.
- **Fencing via epochs:** stale primaries are rejected by replicas and clients.
- **Self-fencing:** a primary that can't reach a majority (or enough replicas, e.g., `min-replicas-to-write`) **stops accepting writes.** *Trade-off:* sacrifices availability on the minority side to avoid divergent data, which is the CAP choice.
- **Accepted loss window:** with async replication, writes acked by the old primary but not yet replicated are lost on failover. Say so. Reduce it with `WAIT`-style sync acks for critical writes.

### Detecting and surviving a cache-tier node loss
For a **pure cache** you may skip replicas entirely: a dead node's keys just miss and reload from the database. That's cheaper, but the DB sees a **miss spike** proportional to that node's share (1/N). Mitigations: more, smaller nodes (so each failure is a smaller share), replicas for the hottest tiers, **gutter/failover pools** (see below), and request coalescing.

---

## 8. Core deep dive 4: the single-node engine

This is where cache-specific interview questions live.

### In-memory data structure
- **Hash table** (open addressing or chained) for O(1) lookup, with **incremental rehashing** (resize gradually across operations so there's no multi-second pause at growth).
- Each entry: key, value pointer, expiry, eviction metadata, and version.

### Concurrency model
- **Single-threaded event loop** (Redis): no locks, trivially atomic commands, very predictable. **Cost:** uses one core per instance, and a **slow command blocks everyone** (`KEYS *`, big `SORT`, huge values). Scale by running many shards per machine.
- **Multi-threaded** (Memcached, newer Redis I/O threads): uses all cores via per-bucket or striped locks. **Cost:** lock contention, harder atomicity, and more complex code.
- **Shared-nothing, thread-per-core** (Dragonfly, Seastar-style): each core owns a shard of the keyspace, with no locks and message passing between cores. Best utilization and tail latency, but more engineering effort.
- **My pick for the interview:** shared-nothing partitions (one shard per core). Say you'd reach for single-threaded-per-shard for simplicity and scale out with shards.

### Memory management
- **Slab allocator (Memcached):** pre-divide memory into size classes. Fast with no external fragmentation, but **internal fragmentation** (a 130 B item uses a 192 B slot) and **slab calcification** (memory stuck in the wrong classes after a workload shift).
- **General allocator (jemalloc):** flexible, but **fragmentation grows over time** (resident memory exceeds logical data), so plan headroom and offer **active defragmentation.**
- **Trade-off:** slabs give predictable speed and simpler eviction per class, while jemalloc gives flexibility and needs defrag. Budget **~20-30% overhead** either way.

### Eviction (when memory is full)
| Policy | How it works | Trade-off |
|---|---|---|
| **LRU (exact)** | Doubly linked list, move to head on access | Pointer overhead (16 B/entry), **a lock on every read** (contention) |
| **Approximate LRU (sampling)** | Sample ~5 random keys, evict the oldest | No list, no lock, near-LRU quality (Redis) |
| **CLOCK** | Circular buffer with a reference bit | Cheap approximation of LRU, scan-friendly |
| **LFU** | Evict least frequently used (with decaying counters) | Better for stable hot sets, slow to adapt (needs aging) |
| **W-TinyLFU** | Small window LRU + frequency-based admission via a **Count-Min Sketch** | Best hit ratio in practice (Caffeine), more complex |
| **TTL-based / random / FIFO** | Simple policies | Lower hit ratios |

- **Scan pollution:** a one-off scan of a million cold keys can **flush the hot set** under plain LRU. Admission policies (TinyLFU) or segmented LRU resist this.
- **Policy per use case:** `allkeys-lru/lfu` for a pure cache. `volatile-*` (evict only keys with TTL) if the store mixes cache and durable data. **`noeviction`** (reject writes when full) for a source-of-truth store.
- **Trade-off:** smarter eviction raises hit ratio (which is worth real money in DB load saved) but costs CPU, memory, and complexity.

### Expiration (TTL)
- **Lazy expiry:** check on access and delete if expired. Free, but **never-read expired keys linger** and waste memory.
- **Active expiry:** a background loop samples keys with TTLs and deletes expired ones (Redis samples ~20 keys, repeating if >25% were expired). Bounds the waste without scanning everything.
- **Timing wheel / min-heap** gives exact timing, but costs memory and per-op work, so it's rarely needed for caches.
- **TTL jitter:** a million keys written together with the same TTL expire together, causing a **synchronized miss storm** and DB spike. Add ±10-20% jitter.
- **Trade-off:** precision versus overhead. For caches, "approximately on time" is fine.

### Durability and persistence (optional for a cache, mandatory for a store)
- **Write-ahead log (WAL/AOF):** append every write. `fsync` policy is the key knob: `always` (durable, slow), `everysec` (lose ≤1 s, the common compromise), `no` (OS decides). **Trade-off:** durability versus write latency and disk wear.
- **Snapshots (RDB):** periodic point-in-time dump. Redis `fork()`s and uses **copy-on-write**, which can cause **latency spikes and up to 2× memory** during heavy write loads, so provision headroom.
- **Combine:** snapshot + WAL tail for fast recovery. Replay time bounds recovery.
- **LSM-tree engine (disk-based stores):** writes go to a WAL and an in-memory **memtable**, flushed to immutable sorted **SSTables**, with background **compaction.** **Bloom filters** skip SSTables that can't contain a key. Excellent write throughput, but **read amplification** and **write amplification** (compaction), plus compaction latency spikes. B-trees give better read predictability with worse write throughput.
- **Restart behavior:** a cache restarting empty (a cold cache) vs. a persisted cache restarting warm. A warm restart is much safer for the DB but takes disk and restore time.

---

## 9. Core deep dive 5: cache-specific problems at scale

*(These reuse the cache patterns from our toolbox, now from the server side.)*

### Hot keys
One key (a celebrity profile, a flash-sale item) can exceed a single node's capacity, since the key hashes to exactly one shard, **and sharding doesn't help.**
- **Detect:** sampled access counting or a Count-Min Sketch of top keys, and per-key rate alerts.
- **Mitigate:**
  - **Client-side L1 cache** with a short TTL (1-5 s), which absorbs most of the load at the source.
  - **Replicate hot keys** to multiple shards (`key#0..k`) and have clients **read a random copy.** Writes update all copies, so the trade-off is write amplification and brief inconsistency.
  - **Read from replicas** (spread reads, accepting staleness).
  - **Edge/CDN caching** where the data is shareable.
- **Trade-off:** staleness and extra write cost versus protecting the shard.

### Cache stampede / thundering herd
A popular key expires, and thousands of requests miss at once and hammer the DB.
- **Request coalescing (single-flight):** one request recomputes, the rest wait.
- **Leases (Facebook's Memcache design):** on a miss the server hands out a **lease token** to one client, who is the only one allowed to fill. Others wait briefly or get a slightly stale value. Leases also **prevent stale sets** (a slow writer overwriting a fresher value after an invalidation), because the lease is invalidated by a concurrent delete.
- **Stale-while-revalidate:** serve the expired value while one background refresh runs. **Probabilistic early refresh:** refresh before expiry with rising probability.
- **TTL jitter** to avoid synchronized expiry.

### Cache penetration (misses that never fill)
Requests for nonexistent keys always miss and hit the DB. Fix with **negative caching** (cache "not found" briefly) and a **Bloom filter** of valid keys at the front.

### Invalidation and consistency with the database
The classic race: reader misses and reads old DB value, writer updates DB and deletes the cache entry, reader then sets the **stale** value into the cache. Mitigations:
- **Delete (don't update) on write**, and keep a **TTL as the safety net.**
- **Leases or versioned sets (CAS)** so a stale fill is rejected.
- **Invalidation via the DB change stream (CDC)** instead of application dual writes, which is more reliable and covers all writers.
- **Trade-off:** you will always have a small inconsistency window, so choose TTLs and invalidation guarantees to match what the business tolerates.

### Big keys and multi-tenancy
- **Big values/collections** block the event loop and spike latency on replication and eviction. Cap sizes and alert on them.
- **Noisy neighbors:** per-tenant memory quotas, per-tenant rate limits (the rate limiter design), and **separate pools** for critical vs. best-effort workloads, so a batch job can't flush a user-facing cache.

### Facebook-scale cache ideas worth name-dropping
- **Look-aside cache with mcrouter (proxy tier)** for routing, pooling, and replication.
- **Gutter pool:** a small set of spare servers that take over when a node fails, with short TTLs, so a failure doesn't flood the DB.
- **Regional pools and cross-region invalidation streams** driven by the database commit log.
- **Leases** (above) to cut stampedes and stale sets.

---

## 10. Cluster changes: scaling out, rebalancing, and rolling upgrades

### Adding a node
1. The control plane assigns it a set of slots (or vnode ranges) taken proportionally from existing nodes.
2. **Slot migration:** the source streams keys of the slot to the target in the background, **throttled** to protect foreground traffic.
3. During migration, a request for a key in the slot goes to the source. If the key has already moved, the source replies `ASK` (redirect to the target) for that request.
4. **Cutover:** the slot ownership flips, the epoch increments, and the map is pushed. Clients refresh on `MOVED`.
5. **Pure-cache shortcut:** skip data migration and let the target start cold, since consistent hashing only invalidates ~1/N of keys. The DB absorbs a bounded miss spike. Warm gradually if that's too risky.

### Removing or replacing a node
Reverse the process: drain slots to other nodes, then decommission. For unplanned loss in leader-follower mode, failover handles it, and replication restores the missing replica copies by **re-replicating** from survivors (throttled).

### Operational concerns
- **Rate-limit rebalancing** and run it off-peak. A bulk move can saturate NICs and cause the very latency spikes you're trying to avoid.
- **Rolling upgrades:** one node at a time, failing over primaries first, with health checks gating the next step. Mixed-version protocol compatibility is mandatory.
- **Avoid correlated failure:** place replicas in **different racks/AZs**, and never put two replicas of one shard on one host or AZ (rack-aware placement).
- **Trade-off:** more shards per node make rebalancing smoother, but multiply per-shard overhead and gossip metadata.

---

## 11. Hop by hop: reliability and trade-offs

### 1. Client library
- **Timeouts** (a few ms to tens of ms), bounded **retries** with jitter (reads are safe to retry, writes only if idempotent), **hedged reads** to a replica for tail latency, and **connection pooling.**
- **Circuit breaker:** if the cache is slow or down, **skip it and go to the DB** (with load shedding and coalescing) rather than stacking timeouts. For caches, the system should function (more slowly) without them.
- **Local L1 cache** for hot keys.
- **Trade-off:** aggressive retries and hedging cut p99 but add load, and a retry storm during an incident makes outages worse, so cap retry budgets.

### 2. Proxy tier (if used)
- Stateless, horizontally scaled, with the slot map cached. Handles pooling, pipelining, and batching, and can enforce quotas.
- **Trade-off:** extra hop and extra failure domain versus simpler clients and fewer connections.

### 3. Primary node
- Accepts writes, appends to the WAL (store mode), applies to memory, and replicates. Enforces **memory limits, eviction, quotas, and slow-command guards.**
- **Back-pressure:** at the memory limit, evict (cache) or reject (store, `OOM` errors), and never crash.
- **Reliability:** run with headroom (≤70-80% memory), monitor fragmentation, and limit client output buffers so a slow consumer can't consume all RAM.

### 4. Replicas
- Receive a replication stream (command log or WAL shipping). **Initial sync** uses a snapshot plus a catch-up stream, which is expensive for large shards. Smaller shards make resyncs cheaper.
- **Replication backlog** sized so that a brief disconnect resumes incrementally instead of triggering a full resync (which is a classic failure amplifier).
- **Reads from replicas:** offloads the primary, with replication-lag staleness.

### 5. Control plane
- Small Raft cluster (3 or 5 nodes across AZs). Stores the slot map, epochs, and node roster.
- **Keep it off the hot path.** If it's down, **no topology changes or automatic failovers,** but reads and writes continue with cached maps.

---

## 12. Failure scenarios

| Failure | Impact | Mitigation |
|---|---|---|
| Single node dies (cache) | 1/N of keys miss, DB spike | Replica promotion or gutter pool, coalescing, more/smaller nodes |
| Primary dies (store, async repl.) | Writes unavailable briefly, **possible loss of unreplicated acks** | Fast failure detection, promote the most up-to-date replica, `WAIT`/sync replication for critical writes |
| Replica dies | Reduced redundancy | Re-replicate from survivors (throttled), rack-aware placement |
| AZ outage | Lose ~1/3 capacity and replicas | Replicas in different AZs, N+1 sizing, quorum still reachable |
| Network partition | Split brain risk | Majority-based failover, epochs/fencing, self-fencing on minority side |
| False failure suspicion (GC pause) | Unnecessary failover and churn | Phi-accrual, multi-observer agreement, grace periods, tune GC |
| Failed node returns with stale data | Divergent copies | Demote on higher epoch, resync, discard unaccepted writes |
| Full resync storm | Disk, network, CPU saturation | Adequate replication backlog, throttled resync, smaller shards |
| Hot key | Single shard saturated | Client L1 cache, key replication, read replicas, edge caching |
| Cache stampede on expiry | DB overloaded | Leases/single-flight, TTL jitter, stale-while-revalidate |
| Mass expiry (same TTL) | Synchronized miss storm | TTL jitter, staggered warming |
| Memory exhaustion / fragmentation | OOM, evictions spike, latency | Headroom, eviction policy, defrag, cap value sizes, alerts on RSS vs. data size |
| Slow command blocks event loop | Latency spike for all on that shard | Ban dangerous commands, max value size, slow-log, shard per core |
| Snapshot fork latency/memory doubling | Pauses, OOM kills | Headroom, snapshot from replicas, schedule off-peak |
| Rebalancing saturates network | Latency regression | Throttle migration, off-peak, one slot at a time |
| Control plane down | No failover or topology change | Cached maps keep data plane running, Raft across AZs, alerts |
| Client retry storm | Amplified overload | Retry budgets, jittered backoff, circuit breakers, load shedding |
| Noisy tenant | Others starved or evicted | Quotas, per-tenant rate limits, separate pools |
| Bad deploy / protocol mismatch | Cluster-wide errors | Canary, rolling upgrade with compatibility, rollback |
| Stale cache vs. DB | Wrong data shown | Delete-on-write, leases/CAS, CDC invalidation, TTL safety net |
| Clock skew (LWW) | Wrong "latest" wins | HLC, NTP monitoring, or leader-assigned versions |

**Principle (the cache must never be the outage):** a cache or KV tier fails toward **being slower, not wrong.** Clients treat the cache as optional (circuit break to the source of truth), caches avoid being a single point of failure, and the data plane keeps working when the control plane doesn't. For a durable store, the principle flips: **fail toward unavailability for a minority partition rather than accept divergent writes.**

---

## 13. Consistency decisions

- **Cache mode:** eventual, best-effort. Staleness is bounded by TTL and invalidation. Loss is acceptable.
- **Store mode (leaderless):** tunable quorums. Default N=3/W=2/R=2 for availability with good freshness, **not linearizable.** Conflicts are resolved by LWW/HLC (simple) or vector clocks (correct but heavier).
- **Strongly consistent mode:** Raft-replicated shards for metadata and anything needing linearizability (locks, leader election, config). Pay in latency and minority-side availability.
- **Per-key atomicity:** guaranteed by the single owner (leader) or CAS/lightweight transactions, **never across keys** without 2PC.
- **Topology metadata:** strongly consistent (Raft), with epochs to order changes.
- **Replica reads:** stale by replication lag, so route reads needing freshness (read-your-writes) to the primary or use `R=quorum`.
- **TTL expiry:** approximate. Don't build correctness on exact expiry times. (Distributed locks with TTLs need **fencing tokens**.)

---

## 14. Multi-region

- **Cache tier is regional:** each region has its own cache fleet. Cross-region cache replication is usually **not** worth it, since a cold regional cache just fills from the (replicated) database. **Invalidation must cross regions** (via the DB change stream), or stale entries persist in remote regions until TTL.
- **Durable KV across regions:**
  - **Leaderless multi-DC** (Cassandra/Dynamo): replicas in each region, with `LOCAL_QUORUM` for low latency and `EACH_QUORUM`/global quorum for stronger guarantees. Cross-region conflicts need LWW/CRDT.
  - **Single-leader per key with a home region** (simple, with cross-region write latency for remote users).
  - **Globally consistent** (Spanner/CockroachDB-style, Raft across regions): linearizable, with ~100 ms commit latency.
- **Trade-offs:** local-quorum writes are fast but may diverge during a partition, then converge. Strong global writes are correct and slow. Pick per dataset.
- **Failover:** reroute clients to another region and accept a cold or slightly stale cache.
- **Data residency:** pin keyspaces to allowed regions.

---

## 15. Security and multi-tenancy

- **AuthN/AuthZ:** TLS between clients and nodes and between nodes, authentication, and per-namespace ACLs. An open cache port is a classic breach (unauthenticated Redis exposed to the internet).
- **Isolation:** per-tenant quotas (memory, ops/sec, connections), key prefixes or namespaces, and separate clusters for sensitive data.
- **Encryption at rest** for persisted stores, and **no secrets in keys** (they appear in logs and metrics).
- **Abuse:** command restrictions (disable `FLUSHALL`, `KEYS`, `DEBUG` for most users), max value sizes, connection limits.

---

## 16. Observability and operations

- **Cache SLIs:** **hit ratio** (global and per keyspace, the most important number), p50/p99/p999 latency per command, eviction rate, expired-key rate, memory used vs. RSS (fragmentation), connection counts, replication lag, slot/key distribution skew, hot-key reports.
- **Alerts:** sudden hit-ratio drop (a bad deploy, key-format change, or node loss), eviction spikes, replication lag, failover events, memory over threshold, rising p99.
- **Capacity planning:** track working-set size versus capacity. Hit ratio typically follows a curve, so measure the marginal gain of more memory.
- **Safety tooling:** kill switches to bypass the cache, key-pattern invalidation tools (with guards), canary reads/shadow traffic for new clusters, and **game days** (kill nodes, partition AZs).
- **Debugging:** per-key tracing sampled, and slow-query logs.

---

## 17. Scaling summary

| Component | Strategy |
|---|---|
| Partitioning | Hash slots or vnodes, rack-aware placement, many small shards for smooth rebalancing |
| Routing | Smart client with cached slot map (+ proxy tier for polyglot/serverless), MOVED/ASK redirects, epochs |
| Replication | Async leader-follower for caches, leaderless quorum (N=3, W=2, R=2) for stores, Raft for strong needs |
| Repair | Hinted handoff, read repair, Merkle-tree anti-entropy |
| Failure handling | Gossip + phi-accrual, majority-based failover, epoch fencing, self-fencing |
| Node engine | Shared-nothing shards, incremental rehash, slab/jemalloc, approximate LRU or TinyLFU, lazy + active TTL |
| Durability | WAL (`everysec`) + snapshots, or LSM-tree for disk-based stores |
| Hot keys / herds | Client L1, key replication, leases, coalescing, TTL jitter |
| Control plane | Small Raft cluster, off the hot path |
| Multi-tenancy | Quotas, rate limits, isolated pools |
| Regions | Regional caches with cross-region invalidation, quorum or home-region stores |

---

## 18. Likely follow-up questions

- **"How do you distribute keys across nodes?"** Hash partitioning via consistent hashing with vnodes (or fixed slots), so adding or removing a node moves ~1/N of keys.
- **"What happens when you add a node?"** It takes ownership of some slots, data streams from the current owners (throttled), and ownership flips with an epoch bump. For a pure cache it can start cold.
- **"How do you handle a node failure?"** Detect via heartbeats/gossip with majority agreement. In leader-follower mode, promote the most up-to-date replica using a new epoch. In leaderless mode, quorum keeps serving and hinted handoff and repair catch up.
- **"How do you prevent split brain?"** Majority quorum for decisions, epoch-based fencing, and self-fencing for isolated primaries.
- **"LRU implementation?"** Hash map plus doubly linked list for exact LRU, but locks hurt, so use approximations (sampling, CLOCK) or TinyLFU for scan resistance.
- **"How do you handle hot keys?"** Detect via sampling, then client-side L1 caching, replicate across shards with suffixes, and spread reads to replicas.
- **"Cache stampede?"** Single-flight or leases, TTL jitter, stale-while-revalidate, and probabilistic early refresh.
- **"How do you keep cache and DB consistent?"** Cache-aside with delete-on-write, leases/CAS to prevent stale fills, CDC-based invalidation, and a TTL safety net. Accept a small inconsistency window.
- **"Quorum details: why R+W>N?"** Read and write sets must overlap, so a read sees at least one replica with the latest successful write. It's not linearizable once sloppy quorums, clock skew, and partial failures are included.
- **"Vector clocks vs. LWW?"** LWW is simple and lossy under concurrency. Vector clocks detect concurrency and keep siblings but burden the application. CRDTs merge automatically for specific types.
- **"Consistent hashing vs. hash slots?"** Both limit data movement. Slots are explicit and easy to migrate, while vnodes need no central map and flex well with heterogeneous nodes.
- **"Redis single-threaded: is that a problem?"** It removes locking and makes commands atomic, and you scale by sharding, but slow commands block the shard. Mitigate with shard-per-core and command limits.
- **"How would you make it durable?"** WAL with fsync policy, snapshots, replication with quorum acks, and an LSM engine if the data exceeds RAM.
- **"How do you do distributed locks with it?"** Use a lease with TTL and **fencing tokens** (monotonic) so a paused client can't corrupt state. For strict correctness, use a Raft-backed store, not a single cache node.
- **"How do you scale reads vs. writes?"** Reads scale via replicas and client caches, and writes scale only via more shards, since each key has a single write path.
- **"What if the cache goes down entirely?"** The DB takes the load, so protect it with load shedding, coalescing, rate limits, gradual warming, and a circuit breaker on the client.
- **"How would you add multi-key transactions?"** Hash tags to colocate keys on one shard, or 2PC/Percolator-style across shards. Name the latency and availability cost.

---

## 19. How to narrate it

1. **Name the spectrum and pick a point:** cache versus source of truth decides replication, durability, and eviction. State your choice and assumptions.
2. **Start naive:** one server with a hash map. Show the limits (capacity, throughput, single point of failure), then add `hash % N` sharding. Show why it breaks on resize, then introduce consistent hashing/slots, then replication, then failure detection and failover.
3. **Separate the planes:** data plane (decentralized, fast) versus control plane (small, Raft-backed, off the hot path).
4. **State the contracts:** per-key atomicity, tunable consistency (quorum math), at-most-N-seconds staleness for a cache, and what is lost on failover.
5. **Call out the knobs:** N/R/W, replication mode, eviction policy, TTL and jitter, fsync policy, shard size, memory headroom, hedging and retry budgets, migration throttles.
6. **For each box, say what happens when it fails,** and end with "a cache node can vanish and users see a slower request, never a wrong one, and a partitioned minority stops writing rather than diverge."

**Patterns reused from our toolbox:** consistent hashing and sharding (URL shortener, feed), cache-aside, stampede and penetration defenses (caching patterns, URL shortener), hot-key mitigation (feed, Top-K), Bloom filters and Count-Min Sketch (crawler, Top-K), quorums and replication trade-offs (consistency toolbox), consensus for small metadata only (Uber, Dropbox), lease-based ownership with fencing tokens (chat, Uber), gossip and heartbeats with jittered reconnects (chat), WAL and snapshot durability and LSM trees (storage selection), rate limiting for tenant isolation (rate limiter), circuit breakers, retry budgets and hedging (reliability patterns), request coalescing (CDN, URL shortener), static stability and graceful degradation.

Next, I can play interviewer on this (for example, "a rolling restart just emptied a 100-node cache cluster and your database is at 20× normal load, walk me through containment and prevention"), or we can move on. **Ticket booking** is the natural follow-up for strong consistency and contention: inventory holds, expiry, and payment interplay. **Typeahead/search autocomplete** opens up the trie, top-K, and offline aggregation side. **Metrics/logging system** adds time-series ingestion and downsampling, and **Google Docs-style collaborative editing** would take the OT/CRDT thread we only touched in Dropbox.
