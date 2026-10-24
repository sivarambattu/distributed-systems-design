# Distributed Message Queue Design (Kafka / SQS / Pulsar style)

**What to say early:** This is the black box we've leaned on in every earlier design ("put it on Kafka"). Opening it up, a distributed message queue is a **replicated, partitioned, append-only log** with a thin layer of **consumer position tracking** on top. Almost every interesting question maps to five problems:

1. **How do you make writes fast and durable?** (sequential I/O, batching, replication)
2. **What ordering do you promise, and how does it scale?** (partitions, keys)
3. **What does "delivered" mean?** (at-most-once, at-least-once, exactly-once, offsets, acks)
4. **How do consumers share work and recover?** (consumer groups, rebalancing, lag)
5. **What happens when brokers die or the cluster changes?** (leader election, ISR, reassignment)

**Clarify the model first**, because "queue" covers two different designs:

| | **Log-based (Kafka, Pulsar, Kinesis)** | **Queue-based (SQS, RabbitMQ, ActiveMQ)** |
|---|---|---|
| Core abstraction | Immutable ordered log, consumers track an **offset** | Messages are individually **leased and acked, then deleted** |
| Retention | Time or size based, independent of consumption | Until consumed (or TTL) |
| Replay | **Yes.** Rewind the offset. | No (once acked, gone) |
| Multiple consumers | Many independent groups read the same data | Competing consumers split the messages |
| Ordering | Per partition | Best-effort, or FIFO queues with limits |
| Per-message features | Weak (no per-message retry or priority) | Strong (visibility timeout, DLQ, delay, priority) |
| Throughput | Very high (sequential I/O, batching) | Moderate |

**My framing:** I'll design a **Kafka-style distributed log** as the core, since it's the harder and more general system, and then show how to layer **queue semantics** (per-message ack, retry, delay) on top, and where a true queue is the better fit.

---

## 1. Requirements

**Functional**
- **Producers** publish messages to named **topics**, optionally with a key
- **Consumers** subscribe and read messages in order, at their own pace
- **Consumer groups:** scale out consumption, with each message processed by one member per group
- **Multiple independent consumers** of the same topic (fan-out)
- **Replay:** re-read from an earlier offset or timestamp
- **Retention:** keep data for a configured time or size, and optionally keep the latest value per key (compaction)
- Optional: transactions (atomic multi-partition writes), delayed messages, priorities, schema management

**Non-functional**
- **High throughput:** millions of messages/sec, GB/s aggregate
- **Low latency:** single-digit to tens of ms end to end (p99)
- **Durability:** an acknowledged message survives broker failures
- **Availability:** survives broker and AZ loss without data loss and with short disruption
- **Ordering:** per-key (per-partition) order preserved
- **Horizontal scalability** of both storage and throughput
- **Isolation:** noisy producers or consumers can't starve others

**Clarify with the interviewer:**
- Replayable log or classic work queue? Both?
- Ordering needs: global (almost always no), per-key (usually yes)?
- Delivery guarantee: is at-least-once with idempotent consumers acceptable, or is exactly-once required?
- Message size, retention period, number of topics and consumers?
- Latency versus throughput priority? Durability versus availability priority?
- Single region or cross-region?

---

## 2. Estimation (state assumptions)

- **Peak 3M messages/sec, average 1M/sec**, ~1 KB each → **3 GB/s peak ingest, ~1 GB/s average**
- **Replication factor 3:** disk writes amplify to **~9 GB/s peak** cluster-wide, and replication traffic is ~6 GB/s of internal network
- **Consumer fan-out:** 3 independent consumer groups → **~9 GB/s of egress** at peak (mostly served from page cache)
- **Retention 7 days:** 1 GB/s × 86,400 × 7 ≈ **600 TB** raw. With ~3× compression ≈ 200 TB, × RF 3 ≈ **~600 TB on disk**
- **Brokers:** a broker sustains ~100-300 MB/s of mixed traffic in practice on good hardware and NICs. For 9 GB/s of disk writes plus reads, that's **~60-100 brokers.** Storage at ~20 TB per broker would need only ~30, so **throughput, not capacity, sizes the cluster** (state which one binds).
- **Partitions:** per-partition throughput is practically ~10-50 MB/s. 3 GB/s peak needs a few hundred partitions at minimum. For consumer parallelism and headroom, topics get dozens to hundreds each, and the cluster holds **tens of thousands** of partitions total. Every partition costs file handles, replication state, and failover work.
- **Latency budget:** a producer batch with `linger` of 5 ms plus a leader append plus follower replication (~1-3 ms in-AZ) gives **~10-15 ms** for an `acks=all` write, in-datacenter.

**Takeaway to say aloud:** the data model is trivial and the workload is **sequential I/O plus network**. Performance comes from *not doing expensive things*: sequential disk access, batching, zero-copy transfers, and leaning on the OS page cache. The cost drivers are **replication factor, retention, and partition count.**

---

## 3. API and data model

```
Producer API
  send(topic, key?, value, headers?, partition?) → future<{partition, offset}>
  config: acks = 0 | 1 | all,  linger.ms, batch.size, compression, enable.idempotence, retries

Consumer API
  subscribe(topics, groupId)  /  assign(partitions)
  poll(timeout) → batch of records (from last committed or sought position)
  commit(offsets)  /  seek(partition, offset | timestamp)

Admin API
  createTopic(name, partitions, replicationFactor, retention, cleanupPolicy)
  alter configs · reassign partitions · describe cluster · quotas · ACLs
```

**Record on disk:** `offset (monotonic per partition) | timestamp | key | value | headers | CRC`, stored in **batches** (a batch is the unit of compression, replication, and I/O).

| Entity | Shape | Store |
|---|---|---|
| **Partition log** | Ordered, immutable sequence of record batches, split into **segment files** (e.g., 1 GB each) | Broker local disk (and tiered object storage for old segments) |
| **Offset index** | Sparse `offset → file position` per segment (one entry per ~4 KB) | Broker disk, memory-mapped |
| **Time index** | Sparse `timestamp → offset` per segment | Broker disk |
| **Topic metadata** | `topic → partitions → {leader, replicas, ISR, leaderEpoch}` | **Controller quorum** (Raft-replicated metadata log) |
| **Consumer offsets** | `(groupId, topic, partition) → committed offset, metadata` | An internal compacted topic (the system stores its own state in itself) |
| **Group membership** | `groupId → members, generation, assignment` | Group coordinator broker, persisted in the internal topic |
| **Producer state** | `(producerId, partition) → last sequence numbers` | Broker, rebuilt from the log |
| **Quotas / ACLs / schemas** | Per-client and per-topic config | Controller metadata, schema registry (separate service) |

**Key design points:**
- **The offset is the entire consumer state.** Consumers are cheap to add or remove, because the broker doesn't track per-message delivery state. This is the central contrast with queue-style brokers.
- **The log is the only storage abstraction.** Even internal state (offsets, transactions) is stored as topics, so replication and durability logic is written once.
- **Messages are immutable** and never edited in place. Compaction and retention remove whole segments or rewrite them in the background.

---

## 4. High-level architecture

```
Producers (client lib: batching, compression, partitioner, metadata cache, retries)
   │ 1. fetch cluster metadata (which broker leads each partition)
   │ 2. send batches directly to the partition LEADER
   ▼
┌─────────────────────────── BROKER CLUSTER (data plane) ───────────────────────────┐
│ Broker 1                 Broker 2                 Broker 3          ...  Broker N  │
│  P0 (leader)             P0 (follower, ISR)       P0 (follower)                    │
│  P1 (follower)           P1 (leader)              P1 (follower)                    │
│  P2 (follower)           P2 (follower)            P2 (leader)                      │
│  each: network threads → request queue → I/O threads → log segments (page cache)   │
│        replica fetchers (followers PULL from leaders) · group coordinator · quotas │
└────────────────────────────────────────────────────────────────────────────────────┘
        ▲ heartbeats / metadata updates                 ▲ rack/AZ-aware replica placement
        └──────────────── CONTROL PLANE ────────────────┘
          Controller quorum (Raft, 3 or 5 nodes): topic/partition metadata, leader election,
          ISR changes, broker membership, config. Off the produce/consume hot path.

Consumers (consumer group): coordinator assigns partitions → each member PULLS from the leader
   (or a follower in the same rack), processes, then commits offsets to __consumer_offsets

Side systems: Schema registry · Connectors (source/sink) · Stream processors · MirrorMaker (cross-region)
              Tiered storage (object store) · Monitoring · Admin/rebalancer
```

**Separate the planes:** the **data plane** (brokers moving bytes) must be fast and decentralized, and clients talk **directly to partition leaders** with no central router. The **control plane** is a small Raft quorum holding metadata, kept off the hot path. This is the "consensus for small metadata, never for the high-volume data path" rule again. (Notice that Kafka does use consensus for metadata and a lighter ISR protocol for data. More on that in section 6.)

---

## 5. Core deep dive 1: why the log is fast (the storage engine)

### The naive design
Store each message as a row in a database or a separate file, with per-message delivery status and indexes. At millions of messages/sec this means random I/O, per-message bookkeeping, and write amplification. Dead on arrival.

### The log design
1. **Append-only, per-partition log.** A write is an append to the end of the active segment file. **Sequential disk writes are orders of magnitude faster than random ones** (hundreds of MB/s on HDD, GB/s on SSD), because there's no seeking and the OS and disk coalesce well.
2. **Segments.** The log is split into fixed-size files (by size or time). Only the **active segment** is written. Closed segments are immutable.
   - **Retention is cheap:** delete a whole old segment file in O(1), with no per-message deletes.
   - **Recovery is bounded:** after a crash, only the active segment needs checking.
3. **Sparse indexes.** An offset index with one entry per few KB maps `offset → file position`. A lookup binary-searches the index, then scans a short run of the file. The time index maps timestamps to offsets, enabling "rewind to 10:00 AM."
   - **Trade-off:** a sparse index is tiny and memory-mappable, at the cost of a small scan after the index hit. A dense index is faster per lookup but much larger.
4. **The OS page cache is the cache.** The broker doesn't maintain its own heap cache. Writes go to the page cache and are flushed by the OS, and **recently written data is read straight from memory**, so a consumer that is caught up (the common case) causes **zero disk reads.**
   - **Benefits:** no duplicate caching, no JVM garbage-collection pressure from a giant heap, and the cache **survives a broker process restart.**
   - **Cost:** a lagging consumer reading old data evicts hot pages and hits disk, which can hurt everyone (noisy-neighbor reads). Mitigate with quotas and tiered storage.
5. **Zero-copy reads.** On fetch, the broker uses `sendfile` to move bytes from the page cache directly to the network socket, **never copying through user space.** This works only if the on-disk format equals the wire format, which is why the batch format is shared end to end.
6. **Batching everywhere.** Producers batch records (by size or `linger.ms`), compress the **whole batch**, and the broker stores it **without decompressing.** Followers and consumers receive the same compressed batch. Compression cost is paid once, by the producer, and saves disk, replication bandwidth, and egress.
   - **Trade-off:** `linger.ms` and batch size trade **latency for throughput and compression ratio.** `linger=0` sends immediately (low latency, tiny batches, poor throughput). `linger=5-20ms` often multiplies throughput several times at negligible latency cost.
7. **Durability by replication, not `fsync`.** Brokers typically don't `fsync` each write. They rely on **replication to other brokers** for durability and let the OS flush. **Trade-off:** much higher throughput, but if all replicas lose power simultaneously before flush, recent data can be lost, so rack and AZ spreading matters. For stricter needs, configure flush intervals at a throughput cost.

### Log compaction (an alternative retention mode)
Instead of deleting by age, **keep the latest record per key** and discard older ones in the background. The topic becomes a **durable changelog / table snapshot** (the latest state per key), used for CDC, KV-style state, and the consumer-offsets topic itself. A `null` value acts as a **tombstone** to delete a key. **Trade-off:** compaction costs background I/O and means the log is no longer a complete history, but it bounds storage by the number of keys instead of the number of updates.

### Tiered storage
Keep recent segments on local SSD and **offload older segments to object storage**, keeping them readable transparently.
- **Benefits:** decouples **retention from broker disk size**, so you can retain months cheaply, and **speeds up broker recovery and rebalancing** (less local data to copy).
- **Trade-offs:** reads of old data are slower and cost per-request fees, and it adds a subsystem. Worth it when retention is long or rebalancing pain dominates.

---

## 6. Core deep dive 2: partitioning and ordering

### Why partitions
A single log on a single machine caps throughput and storage. Splitting a topic into **partitions** gives:
- **Parallelism:** partitions spread across brokers, with throughput scaling by partition count
- **Consumer scaling:** each partition is consumed by at most one member per group
- **Ordering within a partition**, with no coordination across partitions

### Ordering guarantee (say it precisely)
- **Messages in one partition are totally ordered by offset.** All consumers see the same order.
- **There is no ordering across partitions.** Global ordering needs a single partition (single-writer, single-reader bottleneck), so **don't promise it**, and design around **per-key ordering** instead.
- **Producer ordering caveat:** with retries and multiple in-flight requests, a retried batch can land *after* a later batch. Fixes are **idempotent producers** (below), which preserve order across retries with up to 5 in-flight requests, or limiting in-flight to 1 (slow).

### Choosing a partition
- **Keyed:** `partition = hash(key) mod P`. All messages for the same key (user, order, account) land on the same partition in order. This is how you get **per-entity ordering.**
- **Unkeyed:** round-robin or "sticky" (fill one batch for a partition, then switch), which balances load and maximizes batch size.
- **Custom partitioner** for special routing.

### The partition-count decision (a real trade-off)
| More partitions | Fewer partitions |
|---|---|
| Higher max throughput and consumer parallelism | Less per-partition overhead |
| Smoother load balancing | Lower end-to-end latency (fewer replication streams) |
| **More file handles, memory, replication threads, controller load, and slower failover** (each partition's leader must be re-elected) | Faster failover and recovery |
| Longer rebalances | Hard ceiling on consumer parallelism |

- **Heuristic:** size by `max(target throughput ÷ per-partition throughput, desired consumer parallelism)` with headroom (2-3×), and avoid tens of thousands of partitions on a single broker.
- **Changing partition count later is dangerous for keyed data:** `hash(key) mod P` changes, so **the same key maps to a new partition, breaking per-key ordering** across the change, and older data for that key remains on the old partition. Plan for growth up front, or migrate by creating a new topic and dual-writing or replaying.
- **Hot partitions:** a hot key (a celebrity user, a giant tenant) overloads one partition and one consumer. Mitigate by **salting the key** (`key#0..k`, with consumers re-merging if order matters), splitting hot tenants onto dedicated topics, or accepting the ordering loss for that key. Name the trade-off: **ordering versus parallelism for that key.**

---

## 7. Core deep dive 3: replication, durability, and failover

### Leader-follower per partition
- Each partition has a **leader** and **followers** (replication factor, typically 3), placed on **different brokers and different racks/AZs**.
- **All reads and writes go to the leader** (followers optionally serve reads to consumers in the same rack, with slight lag). Followers **pull** from the leader with ordinary fetch requests, which keeps the protocol simple and lets the leader batch.
- **Pros:** a single writer per partition means no conflict resolution and a trivial ordering story. **Cons:** a leader failure needs an election, so there's a brief unavailability per affected partition.

### In-Sync Replicas (ISR), Kafka's key idea
- The **ISR** is the set of replicas (including the leader) that are **caught up** within a configured lag/time window.
- A write is **committed** when it has been replicated to **all replicas in the ISR**, and only committed messages are visible to consumers. The **high watermark (HW)** is the offset up to which data is committed everywhere in the ISR.
- A slow or dead follower is **removed from the ISR**, so it can't stall writes. It rejoins after catching up.
- **Why not plain majority quorum (Raft per partition)?** ISR needs only `f+1` replicas to tolerate `f` failures, versus `2f+1` for majority quorum, so it **uses fewer replicas and less storage and bandwidth** for the same fault tolerance. The cost: it depends on a **separate controller** to manage ISR membership and elections, and its guarantees rest on correct configuration (`min.insync.replicas`).

### The producer's durability knob: `acks`
| `acks` | Meaning | Risk | Use |
|---|---|---|---|
| `0` | Don't wait | **Silent loss** on any failure | Metrics/log firehose where loss is OK |
| `1` | Leader appended | **Loss if leader dies before followers copy** | Latency-sensitive, loss-tolerant |
| `all` | All **in-sync** replicas have it | No loss as long as one ISR member survives | **Default for anything important** |

**`acks=all` alone isn't enough.** If the ISR has shrunk to just the leader, `all` means "the leader only", which is `acks=1` in disguise. The guard is **`min.insync.replicas` (say 2 with RF=3):** the leader **rejects writes** (returns an error) if the ISR is smaller than this.
- **State the standard durable configuration:** `RF=3, min.insync.replicas=2, acks=all`. This tolerates **one** broker failure with **no data loss and no write unavailability.** Losing two replicas blocks writes (availability sacrificed to protect durability).
- **Trade-off:** the CAP choice is explicit. Higher `min.insync.replicas` gives stronger durability and lower availability. Lower gives the reverse.

### Leader election and failover
1. The controller detects a broker failure (missed heartbeats or session timeout).
2. For each partition the broker led, the controller **elects a new leader from the ISR** (so the new leader has all committed data).
3. It bumps the **leader epoch** (a fencing counter), updates metadata, and clients refresh and redirect.
4. The old leader, if it returns, sees the higher epoch, **steps down, and truncates** any uncommitted suffix of its log back to the common point before rejoining as a follower.

### Unclean leader election (the durability versus availability dial)
If **all ISR members are dead** and only an out-of-sync replica survives:
- **Wait (unclean election disabled, the safe default):** the partition stays **unavailable** until an ISR member returns. No data loss.
- **Elect the out-of-sync replica (unclean enabled):** **available again immediately, but committed messages the stale replica never received are lost**, and consumers may see offsets rewind or diverge.
- **Trade-off, stated clearly:** a financial pipeline disables it (consistency over availability). A metrics or log pipeline may enable it (availability over consistency).

### Why the leader epoch matters (a subtle point worth knowing)
Truncating based only on the high watermark can leave replicas **divergent after rapid back-to-back failovers** (the classic "log divergence" bug). Tracking the **leader epoch for each offset range** lets a returning replica ask the leader "where did epoch N end?" and truncate to exactly the right point. That is the same fencing-token idea we've used for leases, now applied to replica logs.

### Controller and metadata (the Raft part)
- A small **Raft quorum (KRaft)** stores cluster metadata in a replicated log (itself a log!). One active controller handles elections and metadata changes, and brokers consume the metadata log to learn the topology.
- **Older design:** an external ZooKeeper ensemble. The built-in Raft quorum removes the extra system, scales to **more partitions** (faster controller failover, since state is replayed from a log rather than re-read in bulk), and simplifies operations.
- **Static stability:** if the controller quorum is briefly down, **existing leaders keep serving produce and fetch** from their cached state. What stops is **topology change and failover**, so a controller outage plus a broker failure is the dangerous combination.

---

## 8. Core deep dive 4: producers and delivery semantics

### The three delivery guarantees (name the failure behind each)
- **At-most-once:** never retry, or commit the offset *before* processing. A failure can **lose** a message. Use for lossy telemetry.
- **At-least-once:** retry until acknowledged, and commit the offset *after* processing. A failure can produce **duplicates.** The default, and the right one when consumers are idempotent.
- **Exactly-once (effectively):** each message's **effect** happens once. It's built from **idempotent writes plus atomic offset commits**, because exactly-once *delivery* over a network is impossible.

### Idempotent producer (kills producer-retry duplicates)
- The broker assigns each producer a **producer ID (PID)**, and the producer attaches a **monotonic sequence number per partition** to each batch.
- The leader tracks the last sequence per `(PID, partition)`. A **retry of an already-written batch** (the ack was lost) is detected by its sequence number and **dropped silently**, with the original offset returned.
- **Out-of-order sequences are rejected**, which **preserves ordering across retries.**
- **Scope:** per-producer-session and per-partition. It doesn't dedupe across *different* producers, or across a producer restart that gets a new PID (transactions with a stable `transactional.id` cover that).
- **Cost:** negligible, and it should be on by default. (Same idea as the payment and notification idempotency keys, made automatic.)

### Transactions (atomic multi-partition write plus offset commit)
The "consume → process → produce" pipeline needs: **write output to several partitions *and* advance the input offset atomically**, or a crash between them duplicates or loses work.
- A producer with a stable **`transactional.id`** begins a transaction, writes to multiple partitions, **adds the consumer offsets to the transaction**, then commits.
- A **transaction coordinator** (a broker, with its state in an internal log) drives a **two-phase commit** with **commit/abort marker** records written to every involved partition.
- **Consumers with `read_committed`** only see messages from committed transactions (they wait at the **last stable offset** past any open transaction, so an open transaction delays visibility).
- **Fencing:** a restarted instance with the same `transactional.id` bumps an **epoch**, **fencing out zombie** producers from before (the fencing-token pattern again).
- **Trade-offs:** adds latency (marker writes, coordinator round trips), delays visibility for `read_committed` consumers, and **only covers data flowing through the log.** A side effect to an external database or an HTTP call is **not** part of the transaction, so end-to-end exactly-once into an external system still needs **idempotent writes or a transactional sink** (the same conclusion as every earlier design: **at-least-once plus idempotency**).

### Other producer concerns
- **Metadata caching:** the client caches `partition → leader`, and on a `NOT_LEADER` error it refreshes and retries.
- **Retry policy:** bounded retries with backoff, and a delivery timeout. **Unbounded retries** against a dead cluster eventually exhaust client memory.
- **Buffering and backpressure:** a bounded in-memory buffer. When full, `send()` **blocks or fails**, so a slow broker pushes backpressure to the application instead of growing memory without limit.
- **Large messages:** cap size (say ≤1 MB). For big payloads, **store the blob in object storage and send a reference** (the claim-check pattern).
- **Schema management:** a **schema registry** (Avro/Protobuf) enforces compatibility rules (backward/forward), so producers can't break consumers by changing a message shape. **Trade-off:** governance versus agility.

---

## 9. Core deep dive 5: consumers, groups, offsets, and rebalancing

### Pull, not push
Consumers **pull** from the broker at their own rate.
- **Pros:** natural **backpressure** (a slow consumer simply falls behind rather than being overwhelmed), the consumer controls batch size, **efficient batching and zero-copy**, and trivial replay.
- **Cons:** needs **long polling** (the fetch waits up to a timeout or minimum-bytes threshold, instead of busy-polling) to avoid latency and wasted requests when idle.
- Push-based brokers (RabbitMQ, SNS) give lower latency for single messages but require **flow control** to protect slow consumers.

### Consumer groups
- A group is a set of consumers sharing a `groupId`. **Each partition is assigned to exactly one consumer in the group**, so a message is processed once per group, and **different groups each get the full stream independently** (fan-out).
- **Parallelism ceiling = number of partitions.** A 12-partition topic can use at most 12 active consumers in a group, and extra consumers sit idle.
- A **group coordinator** (a broker) tracks membership via **heartbeats** and triggers reassignment when members join, leave, or are declared dead.

### Offsets and commit semantics (where delivery guarantees live)
- A consumer **commits offsets** to the internal `__consumer_offsets` topic (a compacted log keyed by group, topic, partition).
- **Commit after processing** → at-least-once (a crash replays from the last commit, causing duplicates).
- **Commit before processing** → at-most-once (a crash skips unprocessed messages).
- **Auto-commit** on a timer is convenient but risky (it can commit messages you haven't finished processing). For correctness, **commit manually after processing**, in batches.
- **Trade-off:** frequent commits shrink the duplicate window but add write load, and infrequent commits are cheaper with bigger replay on failure.
- **Idempotent consumers** (dedupe by message ID or business key with a unique constraint, or conditional writes) turn at-least-once into exactly-once *effect*, exactly as in the payment and notification designs.
- **Store offsets with the results** (in the same database transaction as the output) for an exactly-once sink without Kafka transactions: write the result and the offset atomically, and on restart `seek` to the stored offset.

### Rebalancing (the operational pain point)
When membership changes, partitions are redistributed.
- **Eager rebalancing:** everyone **stops, revokes all partitions, and reassigns** ("stop-the-world"). Simple but causes a **full pause** and, if triggered repeatedly (a flapping consumer), a **rebalance storm.**
- **Cooperative / incremental rebalancing:** only partitions that **actually move** are revoked, and the others keep processing. Much smoother.
- **Static membership** (stable `group.instance.id`): a restarting consumer within a session timeout **gets its partitions back without triggering a rebalance**, which makes rolling deploys painless.
- **Causes of unwanted rebalances:** processing taking longer than `max.poll.interval` (the consumer is declared dead because it didn't call `poll` in time), GC pauses or network blips exceeding the session timeout, and rolling restarts. **Mitigate** by keeping per-batch processing short (smaller `max.poll.records`), tuning timeouts, using static membership, and moving slow work to a separate thread pool.
- **Duplicates after rebalance:** a partition revoked mid-batch is reprocessed by its new owner from the last commit, which is one more reason for idempotent processing. **Fencing zombie consumers** (a paused consumer waking up and committing after losing its partitions) uses a **generation ID**, so stale commits are rejected.

### Consumer lag: the master SLI
**Lag = log end offset − committed offset**, per partition. It measures **how far behind a consumer is** (in messages, and converted to time). It is the single most important health signal.
- **Alert on lag growth rate and lag age**, not only size.
- **Fixes for rising lag:** add consumers (up to the partition count), optimize processing, **increase partitions** (planning for the ordering caveat), shed work, or fix a poison message blocking progress.

### Failure handling: poison messages, retries, and DLQ
A log has **no per-message ack**, so a message that always crashes the consumer **blocks the partition** forever (head-of-line blocking).
- **Bounded in-process retries**, then **publish the failing message to a dead-letter topic** with the error context, and **advance the offset.**
- **Retry topics with delays** (`retry-1m`, `retry-10m`, `retry-1h`) implement **non-blocking retries**: a failed message moves to a delay topic and returns to the main flow later, while the main partition continues. **Trade-off:** retries **break strict ordering** for that key, so use them only when ordering can be relaxed, or pause the key's partition.
- Distinguish **transient** errors (retry with backoff) from **permanent** ones (DLQ immediately).
- **Replay tooling for the DLQ**, with safeguards against reprocessing stale or already-handled messages.

---

## 10. Layering queue semantics on a log (and when to use a true queue)

Some workloads (task queues, job dispatch) need **per-message acknowledgment, retries, delays, and priorities** that an offset-based log doesn't offer natively.

### Per-message semantics a queue broker provides (SQS/RabbitMQ style)
- **Visibility timeout / lease:** a received message becomes invisible for N seconds. If the consumer doesn't **ack (delete)** it in time, it reappears for redelivery. This gives **at-least-once with natural retry** and **no head-of-line blocking**, because other messages continue to flow.
- **Per-message retry counts and DLQ** after N receives.
- **Delayed delivery** (message invisible until time T) and **scheduled messages.**
- **Priority queues** and **competing consumers** with no partition-ownership ceiling (any worker can take any message, so scaling workers isn't limited by partition count).
- **FIFO queues** (message group IDs) give per-group ordering with limited throughput.

### Implementing this on top of a log (if you're building it)
- Keep **per-message state** (in-flight leases, attempt counts, delay timestamps) in an **index separate from the log**, such as a KV store or a per-partition in-flight table, and treat the log as the durable source of messages.
- **Delayed messages:** a time-bucketed structure or delay topics (the scheduler pattern from the notification design), releasing messages when due.
- **Trade-off:** this reintroduces per-message bookkeeping, which is exactly the cost the log design avoided, so throughput drops and complexity rises.

### How to choose
| Need | Pick |
|---|---|
| Event streaming, replay, multiple consumers, very high throughput, stream processing | **Log (Kafka/Pulsar/Kinesis)** |
| Task/job queue, per-message retry and delay, worker pool that scales independently of partitions | **Queue (SQS/RabbitMQ)** |
| Both | A log as the backbone and a queue (or workflow engine) downstream for job execution |

---

## 11. Hop by hop: reliability and trade-offs

### 1. Producer client
- **Idempotence on, `acks=all`, bounded retries and delivery timeout**, a bounded buffer with backpressure, and compression on.
- **Fallback for broker unavailability:** buffer locally within limits, then fail the send visibly (the app decides: drop, spill to disk, or retry). **Never** assume the send succeeded without the callback.
- **Trade-off:** aggressive batching and compression increase throughput and add a few ms of latency.

### 2. Network and request handling (broker)
- A **reactor pattern:** a few **network threads** read and write sockets, a **request queue** feeds a pool of **I/O threads** doing log appends and fetch reads, with a **response queue** back. The separation lets slow disk work not block connection handling.
- **Bounded queues and backpressure:** when the request queue is full, the broker slows reading from sockets, so **TCP back-pressure** reaches the clients.
- **Quotas** (below) prevent a single client from monopolizing threads or bandwidth.

### 3. Partition leader
- Appends to the log, serves fetches, tracks follower progress and the ISR, and advances the high watermark. A **purgatory** (a delayed-operation structure) holds produce requests waiting for `acks=all` and fetch requests waiting for `min.bytes`, avoiding busy loops.
- **Reliability:** if the leader loses followers below `min.insync.replicas`, it **rejects writes** with a retriable error rather than risking data loss.

### 4. Followers
- A **fetch loop** continuously pulls from the leader. **Lag** is monitored, and a slow replica is evicted from the ISR (and re-added when caught up).
- **Replica throttling** during reassignment prevents re-replication from saturating the network and hurting production traffic.

### 5. Group coordinator
- Manages membership, heartbeats, and offset commits for a set of groups, hosted on the broker that leads the corresponding `__consumer_offsets` partition. **If that broker fails, coordination moves with leader election**, and consumers re-discover their coordinator.

### 6. Controller quorum
- Raft-replicated, 3 or 5 nodes across AZs. Handles elections, ISR updates, and topic changes. **Off the hot path** (data plane keeps running if it's briefly unavailable).

### 7. Storage and disks
- **Separate disks (JBOD) or RAID:** JBOD with Kafka's own replication is cost-efficient, but a **single disk failure** takes down the partitions on it (and with older configs the whole broker), so modern deployments handle per-disk failure and re-replicate.
- **Disk-full is an outage:** alert early, enforce retention, and use tiered storage. A broker at 100% disk stops accepting writes.
- **Filesystem and kernel tuning:** XFS/ext4, large file-descriptor limits, adequate page cache (leave most RAM to the OS), and avoid swapping.

---

## 12. Cluster operations: scaling, rebalancing, and upgrades

### Adding brokers
New brokers start **empty**, and partitions don't move automatically. An operator or automation (like Cruise Control) generates a **reassignment plan** to move partition replicas onto the new broker, balancing **disk, network, leader count, and rack diversity.**
- **Throttle replication** so the data movement doesn't consume bandwidth needed by production traffic. A rebalance can take hours for large clusters, so tiered storage (less local data) helps a lot.
- **Cost:** moving data is expensive, which is why **partition sizing** and **tiered storage** matter for operability.

### Removing or replacing a broker
Reassign its partitions elsewhere first (planned), or let replication restore the missing replicas after a failure (unplanned), again throttled.

### Preferred leaders and balance
Each partition has a **preferred leader** (first in the replica list). After failures, leaders cluster on surviving brokers, so a **preferred-leader election** later restores balance. Unbalanced leadership means unbalanced load, since leaders do the most work.

### Rolling upgrades
One broker at a time: **controlled shutdown** (migrate leadership away first, then stop), upgrade, restart, **wait for it to rejoin the ISR for all its partitions** before touching the next. Doing the next broker too early can drop partitions below `min.insync.replicas` and cause write unavailability. Maintain protocol compatibility between versions.

### Rack/AZ awareness
Place replicas in different racks and AZs. With RF=3 across 3 AZs, an **entire AZ can fail with no data loss and no write outage** (ISR of 2 remains, satisfying `min.insync.replicas=2`). With fewer AZs, you must choose between durability and availability during an AZ loss.

---

## 13. Multi-tenancy and quotas

- **Per-client quotas** on produce bytes/sec, fetch bytes/sec, and request rate. When exceeded, the broker **throttles** by delaying responses (a soft, self-regulating approach, the rate limiter design applied inside the broker) rather than rejecting.
- **Topic-level ACLs, authentication (mTLS/SASL), and encryption in transit.** Isolation of sensitive topics, and optionally **payload encryption** at the application layer for end-to-end confidentiality.
- **Noisy-neighbor controls:** quotas, **separate clusters for critical versus bulk workloads** (a debug-log firehose shouldn't share brokers with payment events), and limits on partitions and topics per tenant.
- **Trade-off:** one shared cluster is cheaper and simpler to operate, but isolation requires quotas and discipline. Separate clusters give hard isolation at a higher cost and operational surface.

---

## 14. Failure scenarios

| Failure | Impact | Mitigation |
|---|---|---|
| **Follower broker dies** | Reduced redundancy, ISR shrinks | Writes continue if ISR ≥ `min.insync.replicas`, re-replicate after replacement (throttled) |
| **Leader broker dies** | Brief unavailability for its partitions | Controller elects a new leader **from the ISR**, bumps the epoch, clients refresh metadata and retry |
| **ISR shrinks below `min.insync.replicas`** | Writes rejected | Intentional: protects durability. Restore replicas, alert on under-replicated partitions |
| **All ISR replicas lost** | Partition unavailable or data loss | Unclean election disabled (wait) for critical data, enabled only where loss is acceptable, plus replicas across AZs |
| **Whole AZ outage** | Lose ~1/3 of replicas | RF=3 across 3 AZs and `min.insync.replicas=2` keeps writes and data intact |
| **Network partition (leader isolated)** | Possible split-brain | ISR and `acks=all` mean the isolated leader can't commit, controller elects a new leader with a higher epoch, old leader fenced and truncated |
| **Producer retry after lost ack** | Duplicate message | Idempotent producer (PID + sequence) dedupes at the broker |
| **Producer crash mid-transaction** | Partial writes | Transaction timeout aborts, `read_committed` consumers never see aborted data, epoch fences zombies |
| **Consumer crashes after processing, before commit** | Reprocessing | At-least-once by design, so idempotent consumer handling |
| **Rebalance storm** | Processing pauses repeatedly | Cooperative rebalancing, static membership, tune session/poll timeouts, shorter processing per poll |
| **Poison message** | Partition blocked | Bounded retries, dead-letter topic, advance offset, alert |
| **Consumer lag growing** | Staleness, retention risk | Scale consumers (up to partition count), optimize, alert on lag age, **retention must exceed worst-case lag** or data expires before it's read |
| **Hot partition / hot key** | One broker or consumer saturated | Key salting, dedicated topics for hot tenants, better partitioner, more partitions (with ordering caveat) |
| **Disk full** | Broker stops accepting writes | Early alerts, retention enforcement, tiered storage, add capacity |
| **Slow disk / failing disk** | Replication lag, ISR flaps | Detect slow I/O, evict from ISR, replace disk/broker, spread load |
| **Page cache thrash from lagging consumers** | Latency for everyone | Quotas, isolate replay workloads, tiered storage, separate consumer clusters |
| **Controller quorum unavailable** | No failovers, no topology changes | Data plane continues with cached leadership, Raft across AZs, alert and recover quickly |
| **Reassignment saturates network** | Production latency | Replication throttles, run off-peak, limit concurrent moves |
| **Bad rolling upgrade** | Under-replicated partitions, write failures | One broker at a time, wait for ISR recovery, canary, rollback |
| **Client misconfiguration (`acks=1`, no idempotence)** | Silent loss or duplicates | Cluster-enforced defaults and policies, linting, config audits |
| **Schema break** | Consumers crash or misparse | Schema registry compatibility checks, staged rollout, DLQ |
| **Region outage** | Regional pipeline lost | Cross-region replication (MirrorMaker), consumer failover with offset translation, accept replication lag as RPO |
| **Retention misconfigured too short** | Data deleted before consumption | Alerts on lag versus retention, review retention changes, tiered storage |

**Principle (durability first, then ordering, then availability, and never silently):** the broker **refuses a write rather than weakening a durability guarantee** (`min.insync.replicas`), **fences stale leaders and producers with epochs**, and **surfaces failure to the client instead of dropping data.** The cost is brief unavailability for a partition. The design choice is explicit and tunable per topic, with the loss-tolerant, high-availability setting available for data that can afford it.

---

## 15. Consistency decisions

- **Per-partition order:** **total order** by offset, identical for every consumer.
- **Cross-partition order:** **none.** Use keys to get per-entity ordering, and don't promise global order.
- **Committed data:** visible only after replication to the full ISR (high watermark), so consumers never read data that could later be lost to a failover.
- **Durability:** governed by `acks`, `min.insync.replicas`, and unclean-election settings, **per topic.**
- **Delivery:** **at-least-once** by default. **Exactly-once effect** through idempotent producers, transactions (within the log), and idempotent or transactional sinks.
- **Metadata (topics, leaders, ISR):** **strongly consistent** through the Raft controller quorum, with epochs to order and fence changes.
- **Consumer offsets:** durable, replicated, and compacted. Group generation IDs reject stale commits.
- **Reads after write:** a consumer sees a message only after it is committed, so read-your-writes through the log has latency of the replication round trip.
- **Compacted topics:** eventually consistent snapshots (the latest per key, with a window during which older values may still appear before compaction).

---

## 16. Multi-region

- **Within a region (the default):** a single cluster across 3 AZs, where synchronous ISR replication across AZs gives **zero data loss and high availability** at ~1 ms cross-AZ latency.
- **Across regions:** latency is too high (tens to hundreds of ms) for synchronous per-write replication, so use **asynchronous replication** (MirrorMaker 2, Cluster Linking, Pulsar geo-replication):
  - **Active-passive:** one primary region, with a standby cluster replicating topics and **translated consumer offsets.** On failover, consumers resume near where they left off. **RPO = replication lag**, so some recent messages may be missing or duplicated at the standby.
  - **Active-active:** both regions accept writes to local topics (often region-prefixed names), replicate to each other, and consumers handle **conflicts and duplicates** (idempotent, mergeable processing). Avoid replication loops by tagging origin.
  - **Stretch cluster (one cluster across regions):** synchronous replication, zero RPO, but every `acks=all` write pays the inter-region RTT, and a region partition can block writes. Only for **close regions** (low-latency links) and strict durability needs.
- **Aggregation pattern:** local clusters per region feed a **central aggregate cluster** for global analytics, keeping local latency for local traffic.
- **Trade-offs:** async replication is fast and available but loses the tail on a regional failure. Synchronous is lossless but slow and fragile to partitions. **Say which RPO the business requires.**
- **Data residency:** pin topics containing personal data to allowed regions, and replicate only anonymized or aggregated streams globally.

---

## 17. Observability and operations

- **Broker SLIs:** produce and fetch **request latency (p50/p99/p999)**, request queue depth and network/IO thread idle ratio (saturation), bytes in/out per broker and per topic, **under-replicated partitions** (the key health signal, which should be zero), **offline partitions** (outage), ISR shrink/expand rate (flapping), active controller count (exactly one), disk usage and I/O wait, page cache hit rate, and leader count balance.
- **Consumer SLIs:** **lag and lag age per group and partition**, commit rate, rebalance frequency and duration, processing time per batch, and DLQ rate.
- **Producer SLIs:** send error rate, retry rate, batch size and compression ratio, buffer exhaustion, and end-to-end latency.
- **End-to-end canary:** a synthetic producer and consumer measuring **produce-to-consume latency and loss** continuously. Component health can look fine while the pipeline is broken.
- **Alerts:** under-replicated and offline partitions, ISR below minimum, lag exceeding a fraction of retention, disk above 70-80%, controller instability, and rebalance storms.
- **Tooling:** partition and offset browsers, consumer-group inspection, a message viewer for debugging (with access controls), automated rebalancing (Cruise-Control style), and runbooks for broker loss and unclean-election decisions.
- **Capacity planning:** track bytes/day, retention, partition counts per broker, and consumer fan-out (egress often exceeds ingress). Plan headroom for **failure modes** (an AZ loss must not overload the survivors).
- **Testing:** chaos (kill brokers and AZs, partition the network, fill disks, slow disks), load tests at 2× peak, and rolling-upgrade rehearsals.

---

## 18. Scaling summary

| Component | Strategy |
|---|---|
| Storage engine | Append-only segmented log, sparse indexes, page cache, zero-copy, batch compression end to end |
| Partitioning | Hash by key for per-entity order, enough partitions for throughput and consumer parallelism, plan for growth |
| Replication | RF=3, ISR with `min.insync.replicas=2`, `acks=all`, leader epochs, rack/AZ-aware placement |
| Control plane | Raft controller quorum, off the hot path, epoch-fenced metadata |
| Producers | Batching, compression, idempotent producer, optional transactions, bounded buffers |
| Consumers | Pull model, consumer groups, manual offset commits, cooperative rebalancing, DLQ and retry topics |
| Retention | Time/size-based deletion, compaction for changelogs, tiered storage for long retention |
| Scaling out | Add brokers, throttled partition reassignment, preferred-leader balancing |
| Isolation | Client quotas, ACLs, separate clusters for critical and bulk workloads |
| Regions | Sync across AZs, async replication across regions with offset translation |
| Observability | Under-replicated partitions, lag age, end-to-end canaries, disk and I/O saturation |

---

## 19. Likely follow-up questions

- **"Why is Kafka fast?"** Sequential append-only I/O, batching and end-to-end compression, the OS page cache instead of a heap cache, zero-copy `sendfile`, and replication instead of per-write `fsync`.
- **"How does Kafka guarantee no data loss?"** `acks=all` with `min.insync.replicas=2` and RF=3, so a write is committed only after replication to the ISR. Elections happen only from the ISR (unclean election off), and leader epochs prevent divergence.
- **"What is the ISR and why not use a plain quorum?"** The set of replicas caught up with the leader. It needs `f+1` replicas to survive `f` failures versus `2f+1` for majority quorum, cheaper in storage and bandwidth, at the cost of a controller managing membership and configuration discipline.
- **"How do you get exactly-once?"** Delivery is at-least-once, but idempotent producers (PID + sequence) dedupe retries, transactions make consume-process-produce plus offset commit atomic, and `read_committed` hides aborted data. Writes to an *external* system still need idempotent or transactional sinks.
- **"How do you guarantee ordering?"** Per partition only. Use a key so related messages share a partition, enable idempotence to preserve order across retries, and don't expect global order.
- **"How do you choose the partition count?"** From target throughput and desired consumer parallelism with headroom, balanced against per-partition overhead and slower failover. Changing it later breaks key-to-partition mapping, so plan ahead.
- **"What happens when a consumer dies?"** The coordinator detects missed heartbeats and reassigns its partitions. The new owner resumes from the last committed offset, so there may be duplicates, which is why consumers are idempotent.
- **"How do you handle a slow consumer?"** Lag grows. Scale out up to the partition count, optimize processing, and monitor lag against retention. If the problem is a poison message, send it to a dead-letter topic.
- **"Push or pull?"** Pull gives natural backpressure, consumer-controlled batching, and easy replay. Long polling avoids busy waiting.
- **"What if the leader fails mid-write?"** Unacknowledged writes are retried by the idempotent producer. Acknowledged (committed) writes exist on all ISR members, so the new leader has them. The old leader truncates uncommitted data when it rejoins.
- **"How is this different from RabbitMQ/SQS?"** Those track per-message state (ack, visibility timeout, delay, priority) and delete after consumption, which suits task queues. A log retains data, supports replay and many independent consumers, and trades per-message features for very high throughput.
- **"How would you build a delayed or priority queue?"** Per-message state outside the log (time-bucketed release or delay topics for delays, separate topics or lanes per priority), or use a queue-native broker.
- **"What are the downsides of too many partitions?"** More file handles and memory, slower leader election and recovery, heavier controller load, higher end-to-end latency, and longer rebalances.
- **"How do you do cross-region replication?"** Asynchronous mirroring with offset translation (active-passive or active-active), accepting RPO equal to replication lag. A stretch cluster gives zero RPO at the cost of inter-region latency on every write.
- **"How do you scale a cluster?"** Add brokers and run throttled partition reassignment, balance leaders, and use tiered storage to cut the amount of data to move.
- **"How do you prevent a noisy tenant from hurting others?"** Per-client byte and request quotas, partition limits, ACLs, and separate clusters for critical workloads.
- **"What's compaction and when would you use it?"** Retaining only the latest record per key, for changelogs, state snapshots, and CDC, so storage scales with key count, with tombstones for deletes.
- **"What if you needed true message-level ack and retry semantics?"** Use a queue-native system (visibility timeouts, DLQs, delays), or layer per-message state over the log, accepting the throughput cost.

---

## 20. How to narrate it

1. **Name the dominant problems:** very high write throughput with durability, per-key ordering that scales, and clear delivery semantics. The data model is trivial, and the engineering is in the I/O path, replication, and consumer coordination.
2. **Start naive:** a single server with a queue table in a database, or an in-memory queue. Show the failures (random I/O and write amplification, a single point of failure, a single consumer, lost messages on crash, per-message state that can't scale). Then introduce the append-only log, then partitions for scale and ordering, then leader-follower replication with ISR, then consumer groups and offsets, then idempotence and transactions, then the controller and operational concerns.
3. **Separate the planes:** a decentralized data plane (clients talk directly to leaders) and a small Raft-backed control plane kept off the hot path.
4. **State the contracts:** per-partition total order, committed means replicated to the ISR, durability tunable per topic (`acks`, `min.insync.replicas`, unclean election), at-least-once by default with exactly-once effect through idempotence and transactions, and consumer position as the only consumer state.
5. **Call out the knobs:** partition count, replication factor, `acks`, `min.insync.replicas`, `linger.ms` and batch size, compression, retention and compaction, segment size, commit strategy, session and poll timeouts, quotas, and replication throttles.
6. **For each box, say what happens when it fails,** and end with "a broker or an entire AZ can fail and no acknowledged message is lost, while the worst case is a brief pause for the affected partitions and some duplicate processing that idempotent consumers absorb."

**Patterns reused from our toolbox:** append-only immutable logs, segments, and sequential I/O (Dropbox blocks, metrics TSDB, LSM trees), consistent hashing and key-based sharding with hot-key mitigation (KV store, feed), leader-follower replication, quorums, and the durability-versus-availability CAP dial (KV store), Raft for small critical metadata only (Uber, Dropbox, KV store), epoch and fencing tokens against zombies (chat sequencing, Uber, payments), idempotency keys with sequence numbers (payments, notifications, ticket booking), at-least-once plus idempotent consumers and DLQs (crawler, notifications), transactional outbox and two-phase commit with markers (feed, payments), consumer lag as the freshness SLI (feed fan-out, metrics pipeline), backpressure and bounded queues (rate limiter, notifications), quotas and noisy-neighbor isolation (rate limiter, metrics), tiered hot/cold storage (video, metrics), thundering-herd control in rebalancing and jittered recovery (chat, notifications), compaction as write-verify-then-delete discipline (Dropbox GC, metrics compactor), static stability and graceful degradation.

---

You now have a full toolbox: the building blocks (cache, KV store, queue, rate limiter) and the applied systems (URL shortener, feed, Top-K, chat, crawler, Uber, Dropbox, video, notifications, payments, ticket booking, typeahead, metrics).

Next, I can play interviewer on this (for example, "a broker's disk fills at 3 AM, ISR shrinks across 400 partitions, and producers start timing out, walk me through triage, mitigation, and prevention"), or we can continue. **A distributed job scheduler / workflow engine** (reusing leases, fencing, and exactly-once-ish execution) and **Google Docs-style collaborative editing** (the OT/CRDT thread we only touched in Dropbox) are the two natural remaining picks. We could also do a **cross-cutting review**: one-page cheat sheets, a "which pattern for which problem" decision guide, and a mock-interview round that mixes questions from different designs.
