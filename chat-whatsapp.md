# Chat / WhatsApp Design

**What to say early:** Unlike the feed or Top-K problems, the hard part here is **stateful, long-lived connections** and **delivery guarantees**. Users must receive messages exactly once in order, even when phones go offline, switch networks, or the server holding their connection dies. The system is really a **store-and-forward router** sitting on top of millions of persistent connections.

---

## 1. Requirements

**Functional**
- 1:1 messaging and group chats (say up to 1,000 members)
- Delivery status: sent, delivered, read
- Offline delivery (messages wait until the recipient reconnects)
- Presence ("online / last seen") and typing indicators
- Media (images, video, documents)
- Multi-device support (phone plus web/desktop)
- Push notifications when the app is closed

**Non-functional**
- Low latency (< 200ms end-to-end for online users)
- **Never lose an acknowledged message**
- **Per-conversation ordering**
- High availability, with graceful behavior on flaky mobile networks
- End-to-end encryption (affects the design, see section 9)

**Clarify with the interviewer:** Is E2EE required? How large can groups get? Is message history stored server-side long term (Slack/Messenger style) or only until delivery (WhatsApp style)? Multi-device? Is search in scope?

---

## 2. Estimation (state assumptions)

- 500M DAU × ~40 messages sent/day = 20B messages/day ≈ **230K msgs/sec avg, ~700K peak**
- Group fan-out raises deliveries by perhaps 1.5-2x → **~1M deliveries/sec at peak**
- Concurrent connections: assume 200M. At ~250K connections per gateway server (conservative; ~20 KB of memory per connection ≈ 5 GB), that's **~800 gateways**, so plan ~1,000 with headroom.
- Storage: ~200 bytes per message with metadata → **~4 TB/day**. If you keep history for a year, that's ~1.5 PB. If you only hold messages until delivered (WhatsApp model), the store stays small.
- Media is the real bandwidth hog and goes through object storage and CDN.

**Takeaway to say aloud:** Throughput is manageable. The distinctive constraints are **connection count** (a memory and file-descriptor problem, not a CPU problem) and **delivery correctness under failure**.

---

## 3. API and data model

**Protocol:** a persistent **WebSocket** (or MQTT / a custom binary protocol over TCP) for messaging. Plain HTTPS is used for everything non-realtime (login, profile, media pre-signed URLs, history sync).

```
Client → Server (over WebSocket)
  SEND   { clientMsgId, convId, type, ciphertext, mediaRef? }
  ACK    { msgId }                       // I received it (delivered)
  READ   { convId, upToSeq }
  TYPING { convId }                      // ephemeral
  SYNC   { convId → lastSeenSeq }        // on reconnect

Server → Client
  SENT_ACK { clientMsgId, serverMsgId, seq, ts }   // "single tick"
  MSG      { convId, seq, senderId, body, ts }
  RECEIPT  { convId, userId, deliveredUpTo | readUpTo }
  PRESENCE / TYPING events
```

**Data model**

| Entity | Shape | Store / sharded by |
|---|---|---|
| Messages | `(convId, seq) → senderId, clientMsgId, ciphertext, type, ts` | Wide-column (Cassandra/HBase), partition by `convId`, cluster by `seq` |
| Pending inbox | `(userId, deviceId) → ordered list of undelivered msg refs` | Same store or Redis + durable log, by `userId` |
| Conversations / membership | `convId → members, metadata`; `userId → convIds` | Sharded DB, cached |
| Receipts | `(convId, userId) → lastDeliveredSeq, lastReadSeq` | Wide-column or Redis |
| Session registry | `(userId, deviceId) → gatewayId, connectedAt` (TTL) | Redis cluster |
| Devices | `deviceId → pushToken, platform, keys` | DB + cache |
| Presence | `userId → lastSeen, status` (TTL) | Redis |

**Key design points:**
- **Client-generated `clientMsgId`** makes sends idempotent. If a send times out and the client retries, the server dedupes.
- **Receipts as watermarks** (`lastReadSeq`) rather than one record per message per reader. "Read up to 4,812" is one tiny write and covers hundreds of messages.
- Store **one copy of a message per conversation** and track per-user cursors, rather than copying a message into every group member's mailbox. This is a trade-off, covered in the group section.

---

## 4. High-level architecture

```
                 ┌────────────── CONNECTION LAYER ──────────────┐
Client ⇄ (WebSocket/TLS) ⇄ L4 LB ⇄ Chat Gateways (stateful, ~1000)
                                      │ auth, heartbeat, per-connection rate limit
                                      │ registers (user,device) → gatewayId
                                      ▼
                          Session Registry (Redis, TTL)

                 ┌────────────── MESSAGE PATH ─────────────────┐
Gateway → Chat Service (stateless)
             1. validate, dedupe on clientMsgId
             2. assign seq for conversation (single writer per conv)
             3. persist → Message Store (Cassandra)        ← durability point
             4. SENT_ACK back to sender
             5. resolve recipients (membership cache)
             6. for each recipient device:
                  online  → registry lookup → route to that gateway → push down socket
                  offline → write to pending inbox + enqueue push notification
                  ▼
              Kafka (async consumers): push notifications, search/indexing,
                                       analytics, abuse detection

Presence Service  ·  Group Service  ·  Key Service (E2EE prekeys)  ·  Push Service (APNs/FCM)
Media: client → pre-signed URL → Object Store → CDN
```

---

## 5. The core deep dive: delivery, ordering, and offline handling

### The delivery protocol (this is the heart of the interview)

1. **Sender → gateway → Chat Service.** Service dedupes on `clientMsgId`, assigns the next `seq` for that conversation, and **persists the message durably before acknowledging**.
2. **SENT_ACK to the sender** (one tick). This is the contract: *once the sender sees this, the message will not be lost.*
3. **Routing:** for each recipient device, look up the registry.
   - **Online:** forward to the gateway holding the connection, which writes it to the socket.
   - **Offline:** message stays in the pending inbox, and a push notification is triggered.
4. **Recipient client sends an ACK** when it has the message stored locally (two ticks). Only then does the server remove it from the pending inbox (or advance the device's cursor).
5. **Read receipts** flow back the same way, as a watermark.

**Why ack-based delivery?** A socket write succeeding doesn't mean the phone received the message. The connection may be dead for a minute before TCP notices, so only an application-level ACK counts.

### Delivery semantics
- **At-least-once delivery + client-side dedupe by `(convId, seq)` = effectively exactly-once.** The server retransmits unacked messages (on reconnect or after a timeout), and the client discards duplicates.
- **Trade-off:** true exactly-once is not achievable over unreliable networks. At-least-once plus idempotence is the pragmatic standard.

### Ordering
- Ordering is only needed **per conversation**, never globally.
- Assign a **monotonic `seq` per conversation**, from a single writer. Route all writes for a `convId` to the same shard or Kafka partition, with one owner at a time.
- Clients render by `seq`, not by device timestamps (clocks lie).
- **Alternatives and trade-offs:**
  - *Client timestamps:* simple, but clock skew means wrong order.
  - *Server timestamps:* ties and clock drift between servers make this fragile.
  - *Lamport/vector clocks:* needed only for peer-to-peer or multi-writer setups.
  - *Single-writer sequence per conversation* (my pick): simple and exact, but the owner is a bottleneck for one very hot conversation and needs failover care. Use leases or a consensus-backed owner map, with fencing tokens so a stale owner can't write.

### Reconnects and sync (making failure boring)
Mobile connections drop constantly (tunnels, Wi-Fi to LTE, app backgrounded). Design so that **a reconnect is a normal event, not an error**:
- On connect, the client sends its **last seen `seq` per conversation** (or a single per-device sync token).
- The server replays everything newer, and the client dedupes.
- Because of this, **correctness never depends on the registry or any socket being accurate.** If a message is routed to a stale gateway and lost, the recipient fetches it on its next sync. Real-time push is an **optimization over a reliable pull-based sync.**

This is the single most important design idea to state aloud.

### Offline delivery: two storage models
- **Transient mailbox (WhatsApp):** the server holds a message only until the recipient devices ack, then deletes it (with a TTL like 30 days). Small storage, better privacy, but no server-side history, so a new device can't recover old chats without a backup.
- **Persistent history (Messenger, Slack, Telegram):** the server keeps all messages. Multi-device sync and search are easy, but storage is huge and privacy exposure is larger, and it conflicts with E2EE unless the server stores ciphertext only.
- **Trade-off to name:** storage and privacy versus convenience. State your choice and the assumption behind it.

---

## 6. Connection layer, hop by hop

### 1. Client and protocol
- **WebSocket vs. alternatives:** long polling is simple but wasteful, SSE is one-way, and WebSocket is bidirectional and efficient. MQTT is extremely lightweight and used by Facebook Messenger on mobile.
- **Heartbeats / keepalives:** needed to detect dead connections and to keep mobile NAT mappings alive. **Trade-off:** frequent pings detect failures quickly but drain battery and add load. Adaptive intervals help (longer when the app is backgrounded).
- **Reconnect with exponential backoff and jitter**, or a gateway restart causes a reconnect storm.

### 2. Load balancer
- **L4 (TCP)** is the right layer for long-lived connections. It's cheap, and it doesn't need to inspect WebSocket frames. Terminate TLS at the LB or the gateway.
- Once a connection is established it is **pinned** to one gateway for its life. There's no per-message balancing, so balance on **connection count**, with least-connections for new ones.
- **Reliability:** redundant LBs across AZs. Connection draining on deploys (below).

### 3. Chat Gateway (stateful)
- **Role:** terminate the connection, authenticate (token validated at connect), maintain the socket, parse frames, apply per-connection rate limits, and register in the session registry.
- **Keep it dumb:** no business logic and little CPU work, so it can hold hundreds of thousands of connections. Pass messages to the Chat Service.
- **Tuning:** raise file-descriptor limits, use an event-driven runtime (Erlang, Netty, epoll), and keep per-connection buffers tiny. This is where "1M connections on one box" comes from.
- **Backpressure:** if a slow client's send buffer fills up, drop the connection (it will resync) rather than buffer unboundedly.

**Reliability:**
- A gateway crash drops its connections. Clients reconnect to other gateways and sync, so no message loss, only a brief latency blip.
- **Deploys:** a rolling restart that drops everyone at once causes a thundering herd. Drain gradually (disconnect a small percentage per minute, with jittered reconnect hints), and cap the new-connection rate per gateway.
- **Blast radius:** spread each user's connections across many gateways, so one gateway failing affects a small slice of users.

**Trade-offs:**
- Stateful gateways are harder to scale and deploy than stateless services, but persistent connections require it. Isolating the state into a thin layer is what keeps it manageable.
- Connection density versus blast radius: 1M connections per box is cost-efficient but means one crash hits 1M users at once.

### 4. Session registry
- Maps `(userId, deviceId) → gatewayId`, with a **TTL refreshed by heartbeats**.
- **Staleness is tolerated** (see sync above). A stale entry causes a failed delivery attempt, and the message falls back to the offline path.
- **Reliability:** Redis cluster with replicas. If the registry is lost, gateways re-register as clients reconnect or heartbeats refresh entries. Meanwhile messages queue in inboxes and push notifications cover the gap.
- **Alternative:** skip the registry and use **pub/sub per user** or consistent hashing of users to gateways. This avoids registry lookups but couples placement to the hash ring and complicates rebalancing.

### 5. Routing between servers
Gateway A holds the sender and Gateway B holds the recipient. Options:
- **Direct RPC** from the Chat Service to Gateway B (using the registry lookup). Lowest latency.
- **Pub/sub topic per gateway** (each gateway subscribes to its own channel). Decoupled and simple.
- **Trade-off:** direct RPC is faster but needs service discovery and retries. Pub/sub adds a hop but tolerates gateway churn better.

---

## 7. Message path, hop by hop

### 1. Chat Service (stateless)
- Validates the sender's membership in the conversation and applies rate limits and abuse checks (spam throttling, blocked users).
- **Dedupes** on `clientMsgId`, so retries are harmless.
- Assigns `seq` and persists, then acks the sender. **Durability happens before the ack.**
- **Reliability:** stateless, multi-AZ, autoscaled. Tight timeouts to the store. If the store is down, **fail the send visibly** (client shows a retry clock icon). Never ack unpersisted data.

### 2. Message store
- **Access pattern:** append by conversation, read recent N by conversation, range scan after `seq`. This fits **wide-column** (Cassandra/HBase) well. Partition by `convId`, cluster by `seq` descending.
- **LSM-tree storage** is write-optimized, which suits an append-heavy workload.
- **Hot partitions:** a giant group's partition can get huge. **Bucket by time** (`(convId, month)`) to bound partition size.
- **Replication:** RF=3 across AZs, quorum writes (W=2). Durability matters more than a few milliseconds of write latency.
- **Trade-offs:** wide-column gives scale and write throughput but weak ad-hoc queries, so search needs a separate index. Sharded SQL gives richer queries but requires manual shard management.

### 3. Pending inbox and fan-out
- For offline devices, record "device D still needs messages up to seq X in conversation C." A **per-device cursor** is cheaper than copying messages.
- On reconnect, the server replays from the cursor.
- **Multi-device:** each device has its own connection, inbox cursor, and push token, so fan-out is per device. Sending to a user with 4 devices means 4 deliveries.

### 4. Push notifications (APNs / FCM)
- Used when the recipient has no live connection.
- **Best-effort, not reliable.** Providers throttle, collapse, and drop. The app must **sync on open** regardless. Push is a wake-up hint, not the delivery mechanism.
- **With E2EE,** the server can't put message text in the payload, so it sends a minimal "you have a message" push, and the app wakes, connects, and decrypts locally.
- **Trade-offs:** richer payloads give a better UX but leak content to the push provider. Also, a push on every message in an active group is annoying and costly, so **batch, collapse, and rate-limit** notifications.
- **Reliability:** run push sending as an async consumer off Kafka, with retries, backoff, and invalid-token cleanup. A push provider outage must never block message delivery.

---

## 8. Group chat

### Fan-out strategies
- **Small groups (≤ ~1,000): fan-out on write.** The Chat Service stores the message **once** (in the conversation log) and delivers to each member's connected devices, creating pending-inbox cursors for the offline ones.
- **Large groups and channels (10K to millions of members):** fan-out on write would be a write storm. Use **fan-out on read / pull**: members fetch from the conversation log as they open it, and an online-members subscription pushes lightweight "new message" hints. This is the same hybrid logic as celebrities in the Twitter design.

### Store once vs. copy per recipient
- **Store once + per-user cursors** (my pick): storage scales with messages rather than messages × members, and edits or deletes touch one record. Reads need the membership and cursor lookups.
- **Copy into each member's mailbox:** reads are trivially simple (one inbox per user), but storage and write amplification are enormous, and group edits are painful.

### Membership
- Cache the member list in the Chat Service (it's read on every message). A membership change must apply promptly, so a removed user can't keep receiving messages. Use **versioned membership**: each message is stamped with the membership version it was sent under.
- **Trade-off:** a membership cache that's too stale leaks messages to removed members. A cache that's too tight adds a DB read per message. A short TTL plus explicit invalidation on membership change is the usual compromise.

### Receipts in groups
- Per-member read receipts at scale are expensive (a 1,000-member group means up to 1,000 receipt events per message). Use **watermarks** and **aggregate or sample** ("read by 37"), or only show per-member receipts in small groups.

---

## 9. Presence, typing, media, and E2EE

### Presence ("online" and "last seen")
- **Naive:** every status change is broadcast to all contacts. With hundreds of contacts per user, it's a fan-out storm.
- **Better:** a user's online state is just "has a live connection plus a recent heartbeat," stored in Redis with a TTL. **Only subscribe to presence for the chats currently on screen**, or **pull on demand** when the user opens a conversation.
- **Debounce flapping:** a flaky connection shouldn't flip online/offline repeatedly. Wait a few seconds before declaring offline.
- **Trade-off:** real-time accuracy versus cost. Presence is a **non-critical, droppable feature**, so degrade it first under load. Also expose a privacy setting (hide last seen).

### Typing indicators
Ephemeral, never persisted, sent only to the other participants in an open chat, and throttled (one event per few seconds). Losing them is fine.

### Media
- Client requests a **pre-signed URL** and uploads **directly to object storage**, so large bytes never touch the chat servers.
- The message carries only a reference (URL, size, hash, thumbnail, and the decryption key if E2EE).
- Download goes through the **CDN**. Large files use **chunked, resumable uploads**.
- **E2EE twist:** the client encrypts the file with a random key *before* uploading. The server only stores ciphertext, so no server-side transcoding or scanning is possible. Thumbnails are generated client-side.
- **Trade-off:** direct upload scales and is cheap, but you lose server-side processing and dedupe over encrypted blobs.

### End-to-end encryption (be ready to sketch it)
- **Signal protocol:** each device publishes **identity and prekeys** to a **Key Service**. A sender fetches the recipient's prekey bundle, establishes a session (X3DH), and ratchets keys forward (Double Ratchet) for forward secrecy.
- **The server only sees ciphertext and metadata** (who, when, size). It routes and stores blobs.
- **Groups:** typically use **sender keys** (encrypt once for the group with the sender's key, distributing that key to each member individually), which avoids encrypting N times per message.
- **Multi-device:** each device has its own keys, so the sender encrypts per recipient device.
- **Consequences to name:** no server-side search, spam scanning, or content moderation (only metadata and user reports). Backups need client-held keys. Adding a device or member requires key distribution. Key changes need user-visible verification ("safety numbers").
- **Trade-off:** privacy and security versus features and operational simplicity.

---

## 10. Scaling summary

| Component | Strategy |
|---|---|
| Gateways | Many stateful nodes at 250K-1M connections each, L4 LB, drain gradually, connection-rate caps |
| Session registry | Redis cluster, TTL from heartbeats, staleness tolerated |
| Chat Service | Stateless, autoscaled, single-writer sequencing per conversation |
| Message store | Wide-column, partition by `convId` (time-bucketed), RF=3 across AZs |
| Pending inbox | Per-device cursors, replay on reconnect |
| Group fan-out | Push for small groups, pull/hint for large ones |
| Presence | Redis with TTL, on-demand, lowest priority |
| Push | Async via Kafka, retries, collapse keys |
| Media | Pre-signed direct upload, object store, CDN |
| Async consumers | Kafka for push, search, analytics, abuse |

---

## 11. Failure scenarios

| Failure | Impact | Mitigation |
|---|---|---|
| Gateway crashes | Its connections drop | Clients reconnect with jitter, sync from cursor, no message loss |
| Mass reconnect (deploy, network blip, regional LB flap) | Thundering herd on gateways, auth, and store | Jittered backoff, connection-rate caps, gradual drain, shed non-essential work |
| Session registry stale or lost | Messages routed to wrong gateway | Delivery falls back to inbox plus push; sync on reconnect; registry rebuilds from heartbeats |
| Message store node/AZ down | Writes/reads degrade | Quorum replication, retries, fail sends visibly rather than ack-and-lose |
| Chat Service crash mid-send | Sender gets no ack | Client retries with the same `clientMsgId`, server dedupes |
| Crash after persist, before ack | Sender doesn't know it was stored | Retry returns the existing seq (idempotent) |
| Recipient offline for days | Pending messages accumulate | Cursor-based sync, TTL on mailbox, push reminders |
| Push provider outage | No wake-ups | Messages still sync on next app open; presence of retry queue |
| Conversation owner node fails | Sequencing paused for those convs | Lease-based ownership with fast failover, fencing tokens to prevent two writers |
| Hot group or viral channel | Hot partition and fan-out storm | Pull-based fan-out, time-bucketed partitions, batch deliveries |
| Spam/abuse flood | Resource exhaustion | Per-user and per-connection rate limits, new-account throttles, reports |
| Region outage | Regional users disconnected | Reconnect to a healthy region, replicated store, accept brief delivery delay |
| Clock skew | Wrong message order | Order by server `seq`, not timestamps |
| Bad deploy | Message loss or errors | Canary, protocol versioning, rollback, backward-compatible clients |

**Principle (static stability):** the critical path is *gateway → Chat Service → message store*. Presence, typing, push, search, analytics, and receipts aggregation are all optional and degrade independently. And because delivery is anchored in durable storage plus sync-on-reconnect, even a broken real-time layer only means higher latency, never lost messages.

---

## 12. Consistency decisions

- **Per-conversation total order** (via `seq`). This is the one strong guarantee users feel.
- **Durability before ack.** An acknowledged message is never lost.
- **Delivery:** at-least-once plus client dedupe, giving an effectively exactly-once experience.
- **Cross-conversation ordering:** none, and it isn't needed.
- **Presence, typing, and receipts counts:** eventually consistent and best-effort.
- **Membership and blocks:** enforce promptly. These are the cases where staleness has privacy consequences.
- **Multi-device:** devices converge via cursors, so each device may be at a different point temporarily.

---

## 13. Multi-region

- Each user has a **home region** for their inbox and conversation state. Users connect to the nearest gateway region, which forwards cross-region only when needed.
- **1:1 across regions:** the Chat Service in the conversation's owning region sequences and persists, then forwards to the recipient's region for delivery.
- **Store replication:** async cross-region replication for DR, with RPO of seconds.
- **Regional failover:** clients reconnect to another region, and sync fills the gap. Sequence ownership for affected conversations moves with fencing.
- **Trade-off:** pinning conversations to one region keeps ordering simple but adds cross-region latency for participants elsewhere. Multi-region active-active writes need conflict resolution or per-conversation ownership. Ownership is the simpler answer.

---

## 14. Likely follow-up questions

- **"How do you guarantee a message is never lost?"** Persist before ack, replicate with quorum, and have the client retry idempotently with `clientMsgId`. Delivery is ack-based with cursors, so unacked messages are replayed on reconnect.
- **"What if the recipient is connected to a server that just died?"** The registry entry is stale. The delivery attempt fails or times out, the message stays in the pending inbox, and a push is sent. The client reconnects and syncs from its cursor.
- **"How do you keep 200M connections up?"** Event-driven gateways with tiny per-connection state, L4 balancing, kernel and FD tuning, and horizontally adding gateways. The bottleneck is memory and file descriptors, not CPU.
- **"How do you order messages in a group?"** A single-writer sequence per conversation, with lease-based ownership and fencing.
- **"How would you scale to a 1M-member channel?"** Don't push to every member. Store once, use pull on open, push lightweight hints only to currently online members, and batch.
- **"How do you add a new device?"** Register it with new keys, existing contacts fan out to it from then on, and history arrives via encrypted transfer from another device or a user-held backup. The server can't read old E2EE messages.
- **"How do you show read receipts cheaply?"** Watermarks per `(conversation, user)`, aggregated in groups.
- **"How do you handle a reconnect storm?"** Jittered exponential backoff, connection-rate limits per gateway, a gradual drain on deploys, and auth/token validation that doesn't hit a central DB per connection.
- **"How do you prevent spam?"** Per-user send rate limits, new-account reputation, report and block features, and with E2EE, behavioral and metadata signals instead of content scanning.
- **"How would you support message search with E2EE?"** Client-side local index only. Server-side search isn't possible without breaking the encryption guarantee.
- **"Delete for everyone?"** A tombstone event is delivered like a normal message. Clients that process it delete locally. It's best-effort for offline devices and can't undo screenshots or forwards.

---

## 15. How to narrate it

1. **Name the dominant problem:** persistent connections plus reliable, ordered delivery over unreliable mobile networks. Throughput is secondary.
2. **Start naive:** clients poll an HTTP API that reads from a DB. Show the latency and load problems, then introduce WebSockets, then gateways, then the registry, then explain why correctness must not depend on the registry (sync cursors).
3. **State the contract:** persist-before-ack, at-least-once delivery with client dedupe, and per-conversation ordering via `seq`.
4. **Separate critical from optional:** messages and sync are critical. Presence, typing, push, and receipts aggregation are best-effort.
5. **Call out the knobs:** heartbeat interval, connections per gateway, group fan-out threshold, mailbox TTL, retry and backoff settings, rate limits.
6. **For each box, say what happens when it fails,** and end with "a gateway can die and the user loses a second of latency, not a message."

**Patterns reused from our toolbox:** idempotency keys, at-least-once plus idempotent consumers, single-writer sequencing with fencing, hybrid push/pull fan-out (from the feed), watermarks instead of per-item state, thundering-herd control (jitter and rate caps), cache with TTL (registry, presence), async consumers off Kafka, graceful degradation, and static stability.

Next, I can play interviewer on this (for example, "a region just lost its LBs and 50M clients reconnect at once, walk me through it"), or we can move to the next question. A **rate limiter** or **Uber/proximity** would be a good contrast, since a rate limiter is a small, latency-critical component, and Uber adds geospatial indexing and real-time matching.
