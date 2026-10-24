# Notification System Design

**What to say early:** On a whiteboard this looks like "put a message on a queue and call an API." The real problems are elsewhere:

1. **Reliability across third parties.** APNs, FCM, SMS carriers, and email providers are slow, rate-limited, and flaky, and you don't control them. Most failures originate outside your system.
2. **Isolation of priorities.** A 100M-user marketing blast must never delay a login OTP or a fraud alert.
3. **Protecting users and yourself.** Preferences, consent, quiet hours, frequency caps, and deduplication keep you from spamming people, getting blocked, or violating the law.
4. **Duplicates vs. loss.** You can't have exactly-once delivery to a phone. You choose which failure to prefer per notification type.

So this is a **pipeline of queues and policy checks in front of unreliable external providers.** Notifications are **best-effort and time-sensitive** (a stale OTP is worthless), but some classes (security, payments) carry high stakes. State that tension early.

---

## 1. Requirements

**Functional**
- Send notifications over multiple channels: **mobile push (APNs/FCM), SMS, email, in-app inbox, web push, and webhooks**
- Triggered by events (a like, a shipped order), by API calls (OTP), by schedules (reminders), and by campaigns (bulk to a segment)
- **User preferences:** per-channel and per-category opt-in/out, quiet hours, language, time zone
- **Templates** with localization and variables
- **Delivery tracking:** queued, sent, delivered, opened/clicked, failed, bounced
- **Scheduling**, **deduplication**, **frequency capping**, and optional **aggregation** ("Alice and 12 others liked your post")
- Multi-device users (one user has several push tokens)

**Non-functional**
- **Latency by priority:** critical (OTP, security) delivered in **seconds**. Social/transactional within ~10-30 s. Marketing and digests tolerate minutes to hours.
- **High availability of ingestion.** Producers must always be able to hand off a notification.
- **Durability once accepted:** an accepted notification is not silently lost.
- **Scalability** for spikes (a flash sale, a breaking-news blast, an outage that triggers alerts for everyone)
- **Compliance:** honor opt-outs, consent, and regional rules (GDPR, CAN-SPAM, TCPA)
- **Cost control.** SMS costs real money per message, which makes this a *financial* system in part.

**Clarify with the interviewer:**
- Which channels, and is this internal-platform (many producer teams) or a single product?
- Which priority classes exist? Are OTP/transactional in scope?
- Campaign/bulk sends in scope, or only event-driven?
- Scale: peak per second, and size of the largest blast?
- Is delivery tracking (opens, clicks) needed? Analytics?
- Delivery guarantee expectations: at-least-once acceptable? Duplicates tolerable?

---

## 2. Estimation (state assumptions)

- 100M DAU × ~20 notifications/day = 2B/day ≈ **23K/sec average**, with a daily peak of maybe **100K/sec**
- **Campaign blast:** 100M pushes in a 10-minute window ≈ **~170K/sec** *on top of* the baseline. This is the number that shapes the design.
- **Channel mix** (illustrative): 80% push/in-app, 15% email, <5% SMS, which is small in volume but dominant in cost (SMS is often cents per message while push is nearly free)
- Notification record ~1 KB × 2B/day = **~2 TB/day** of metadata and status events. Keep ~30-90 days hot, then archive to object storage.
- **Device tokens:** 100M users × ~2 devices = **200M tokens** (~100 bytes each ≈ 20 GB), small and fits in a sharded KV
- **Preferences:** 100M × ~1 KB ≈ **100 GB**, cacheable
- **Provider concurrency:** at 170K pushes/sec with ~100 ms provider latency, you need **~17K in-flight requests.** Providers use multiplexed HTTP/2 connections, so this is a **connection-pool and backpressure** problem, not a CPU problem.

**Takeaway to say aloud:** Storage and compute are modest. **Spikes, provider limits, and isolation between traffic classes** are what matter. The key capacity number is the **blast rate** versus the **provider throughput**, and the gap between them is bridged by queues.

---

## 3. API and data model

```
POST /v1/notifications
  { idempotencyKey, templateId, recipients: [userId] | segmentId,
    channels?: [push, email, sms] (or "auto" = per preferences),
    priority: critical | high | normal | bulk,
    category: security | transactional | social | marketing,
    data: {...template variables}, dedupeKey?, collapseKey?,
    ttl: 300s, scheduleAt?, respectQuietHours: true }
  → 202 Accepted { notificationId }

GET  /v1/notifications/{id}                    → status per channel/attempt
PUT  /v1/users/{id}/preferences                → channel/category opt-in, quiet hours, locale
POST /v1/users/{id}/devices { token, platform } / DELETE
GET  /v1/users/{id}/inbox?cursor=              → in-app notifications (paginated)
POST /v1/webhooks/provider/{name}              → delivery receipts, bounces, complaints (inbound)
```

Return **`202 Accepted`**, not `200`. The notification is durably accepted, and delivery is asynchronous.

| Entity | Shape | Store |
|---|---|---|
| **Notification** | `notificationId → idempotencyKey, userId, category, priority, templateId, payload, ttl, state, createdAt` | Wide-column/sharded KV by `userId` or `notificationId` |
| **Delivery attempt** | `(notificationId, channel, attempt#) → provider, providerMsgId, status, error, ts` | Same, or append-only log |
| **User preferences** | `userId → {channel×category opt-ins, quietHours, tz, locale, caps}` | Sharded KV + cache |
| **Devices** | `userId → [deviceId, platform, token, lastSeen, valid]` | Sharded KV by `userId` |
| **Suppression list** | `address (email/phone) → reason (bounce, complaint, unsubscribed), ts` | KV, cached, **global** |
| **Templates** | `templateId, locale, version → body, metadata` | Config DB + cache, versioned |
| **In-app inbox** | `(userId, ts/seq) → notification` | Wide-column by `userId` (like a mailbox) |
| **Idempotency / dedupe** | `(producerId, idempotencyKey) → notificationId`, TTL | KV with conditional write |
| **Frequency counters** | `(userId, category, window) → count` | Redis, TTL |
| **Campaigns** | `campaignId → segment, template, schedule, throttle, progress cursor` | Relational/KV |

**Key design points:**
- **Idempotency key at ingestion** makes producer retries safe (the pattern from Uber and Dropbox).
- **Notification ≠ delivery.** One notification fans out into N channel/device deliveries, each with its own state machine and retries.
- **Tokens and addresses are perishable.** Uninstalls, bounces, and number reassignment mean you must continuously prune.

---

## 4. High-level architecture

```
Producers (services, event streams, schedulers, campaign tool)
      │ POST /notifications  (or events via Kafka)
      ▼
Notification API / Ingestion (stateless)
   validate · authn/quotas per producer · idempotency check · persist (durable) · ack 202
      ▼
Kafka: ingest topics, SEPARATE per priority  [critical] [high] [normal] [bulk]
      ▼
Orchestrator / Router (stateless consumers, scaled per lane)
   1. resolve recipient(s), explode segments (bulk via Campaign Service)
   2. load preferences, consent, suppression list, quiet hours
   3. dedupe + frequency caps + aggregation/collapse
   4. choose channel(s) and devices; render template (locale)
   5. write delivery records (state = PENDING)
      ▼
Per-channel × per-priority queues (Kafka/SQS)
   [push-crit] [push-norm] [push-bulk] [sms-crit] [sms-norm] [email-trans] [email-bulk] [inapp] [webhook]
      ▼
Channel Workers ── per-provider rate limiter · circuit breaker · retry/backoff
      ▼
Provider Adapters:  APNs · FCM · SMS aggregators (≥2) · Email providers/SMTP (≥2) · Web push · Webhooks
      ▼
External providers ──► device/phone/inbox

Feedback loop:  provider callbacks/webhooks ─► Status Service ─► delivery DB, metrics,
                token/address invalidation, suppression list updates, retry/failover triggers

Side systems: Scheduler (delayed/recurring) · Campaign Service (bulk fan-out) · Preference Service ·
              Template Service · Analytics (opens/clicks) · DLQ + replay tooling
```

---

## 5. Core deep dive 1: isolation of priorities and traffic classes

**The failure to prevent:** a 100M-message marketing blast sits in the same queue ahead of an OTP, and users can't log in for 10 minutes.

### Naive: one queue, one worker pool
Head-of-line blocking is guaranteed, a slow provider stalls everything, and there is no way to protect critical traffic.

### Recommended: physical separation by priority and channel
- **Separate topics/queues per priority lane** (critical, high, normal, bulk) **and per channel**, with **separate consumer pools** and capacity for each.
- **Why not just a priority queue?** Kafka has no native priority, and even where it exists (e.g., priority-aware queues), low-priority work still shares workers and connections. **Physical isolation is the strong guarantee.** Priorities within a single queue only help ordering, not resource protection.
- **Reserved capacity:** critical lanes have dedicated workers and provider connections that bulk traffic **can't borrow**. Bulk lanes can use leftover capacity and are throttled when critical lanes are busy.
- **Trade-off:** isolation costs idle headroom (reserved capacity sits partly unused) and more moving parts. It buys predictable latency for critical traffic, which is worth it.

### Per-producer quotas and fairness
- Each producing team or tenant gets **rate quotas and burst limits** at ingestion (the rate-limiter design applied inbound), so one buggy service looping on "send notification" can't flood the platform.
- **Fairness within a lane:** shard or weight by producer/tenant so one noisy producer doesn't starve the rest.
- **Reject early with 429 and a clear error** instead of accepting work you can't deliver in time.

### Isolation of providers
- **One slow provider must not block others.** Each channel/provider has its own queue and workers, with **bulkheads** (separate pools and connection limits) and **circuit breakers.** If the SMS provider slows, the email and push lanes are unaffected.

---

## 6. Core deep dive 2: reliable delivery, retries, and duplicates

### The honest guarantee
End-to-end exactly-once is impossible. The phone may receive a push while the ack is lost, so you can't tell whether to retry. The pragmatic contract is:

- **At-least-once** through the internal pipeline (never silently drop an accepted notification)
- **Idempotency and dedupe** to make duplicates rare
- A per-category policy for the leftover ambiguity:
  - **Social/marketing:** an occasional duplicate is acceptable, loss is worse.
  - **OTP/security:** a duplicate OTP is harmless and loss is bad, so at-least-once is right.
  - **Financial alerts or costly SMS:** duplicates cost money and annoy, so use stronger dedupe and accept a tiny loss window.

### Ingestion: persist before ack, then publish reliably
- Validate, check the **idempotency key** (conditional write: `PUT if not exists`), **persist the notification**, then return `202`.
- Publish to Kafka via the **transactional outbox** (the write and the "to publish" record in one transaction, with a relay), or write directly to Kafka with `acks=all` for simpler systems. This avoids the dual-write problem from the feed and Uber designs.
- **Trade-off:** persisting before ack adds a few ms and a store dependency, but guarantees accepted notifications survive crashes. Without it, you ack and then lose work.

### Delivery state machine
```
PENDING → SENDING → SENT(to provider) → DELIVERED → (OPENED/CLICKED)
              │            │
              └─► RETRY ───┴─► FAILED (permanent) / EXPIRED (ttl) / SUPPRESSED / DROPPED
```
Transitions use conditional updates, so a stale worker can't regress state.

### The crash window (name this explicitly)
A worker calls the provider, the provider accepts, and the worker **crashes before recording `SENT`.** On retry, the message is sent again.
- **Mitigations:**
  - **Provider-side idempotency/collapse keys where available** (APNs `apns-collapse-id`, FCM collapse keys, ESP idempotency tokens) so duplicates collapse on the device or at the provider.
  - **Record intent before sending** (`SENDING` with a lease), and on recovery **check provider status by `providerMsgId`** where the provider supports lookups.
  - Otherwise, **accept the rare duplicate** as the price of not losing the message.
- **Trade-off:** "at-most-once" (mark sent first, then send) has no duplicates but loses messages on crashes. For notifications, **loss is usually worse than duplicates,** except for costly or sensitive sends.

### Retries
- **Retry only transient failures** (timeouts, 5xx, 429) with **exponential backoff and jitter** and a **max attempts or TTL cap.** Never retry permanent errors (invalid token, hard bounce, unregistered device).
- **Honor `Retry-After` from providers.**
- **Retry topology:** use **delay queues or retry topics with increasing delays** (1 s, 10 s, 1 min...) instead of sleeping in the worker (which wastes capacity and blocks the partition). After N attempts, send to a **DLQ** for inspection and replay.
- **TTL is a hard stop:** a retry that would deliver an OTP after it expired is worse than no retry. **Drop expired messages** (state `EXPIRED`) rather than delivering stale content. A "your driver is arriving" notification 20 minutes late is harmful.
- **Retry storms:** when a provider recovers, all backed-off retries hit together. Jitter, per-provider rate limits, and gradual ramp-up prevent a self-inflicted outage.

### Provider failover
- **SMS and email:** keep **at least two providers**, with health-based routing. On sustained errors, the circuit breaker opens and traffic shifts to the secondary. **Trade-off:** failover protects availability but multiplies integration and ops work, and sender reputation (especially for email) has to be warmed on each provider. Critical categories (OTP) justify it.
- **Push:** APNs and FCM have no alternative (each is the only gateway to its platform), so the strategy is **retry, queue, and fall back to another channel** (e.g., SMS or in-app) for critical messages.
- **Channel fallback/escalation:** push → (if undelivered after N seconds) → SMS for OTP-like cases. Trade-off: higher delivery rate versus higher cost and the risk of double notification if the push arrives late.

---

## 7. Core deep dive 3: throttling outbound, backpressure, and bulk fan-out

### Provider limits are the true bottleneck
| Channel | Practical constraints |
|---|---|
| **APNs** | HTTP/2 multiplexed connections. Persistent connections must be reused, as frequent reconnects are penalized. Per-device limits and payload size caps (~4 KB). |
| **FCM** | Project quotas and per-device throttling, with payload limits. |
| **SMS** | **Per-sender/number throughput limits** (messages per second per long code or short code), carrier filtering, registration requirements, regional regulations. Costs real money. |
| **Email** | **Sender reputation** governs deliverability. New IPs and domains need **warm-up**. Bounce and complaint rates must stay low. SPF/DKIM/DMARC setup is mandatory. ESP rate limits apply. |

So the system needs a **token-bucket rate limiter per provider, per credential/sender** (the rate-limiter design again, now *outbound*), enforced centrally or per worker with leased budgets.

### Backpressure, not drops
- When a provider limit is hit, the worker **slows consumption and lets the queue absorb the surplus.** This is a core benefit of queues. Consumer lag is the signal.
- **Autoscale on lag and in-flight concurrency,** within provider limits (scaling workers doesn't raise a provider's rate cap, so autoscaling beyond the cap is wasted).
- **Bound queue growth and age:** if the backlog's age exceeds the message TTLs, **shed** by dropping lowest-priority or expired items, or pausing bulk.

### Bulk and campaign fan-out
A blast to a segment of 100M users should **not** be one API call expanded inline.
- **Campaign Service** resolves the segment (a query or precomputed audience), **paginates it into chunks** (e.g., 10K users), and enqueues **chunk jobs.** Many workers expand chunks into per-user notifications in parallel (the fan-out pattern from the feed design, in batch form).
- **Throttle the campaign** to a configured rate (say 50K/sec) so it doesn't starve everything else, **and spread it over time** (jitter or a gradual ramp) to avoid load spikes on your own systems **and the app backend** (millions of simultaneous opens after a push can create a thundering herd against the product's API, so rate-shaping marketing pushes protects the product).
- **Resumability:** a campaign stores a **progress cursor** per chunk, so a crash resumes instead of restarting or double-sending. Chunk jobs are idempotent via `(campaignId, userId)` as a dedupe key.
- **Send-time optimization:** deliver at the user's local morning (time zones), which naturally flattens load (and respects quiet hours).
- **Priority:** bulk always yields to critical and transactional traffic.
- **Trade-off:** slower, throttled campaigns finish later but protect everything else. Specify the SLA per class (bulk may take hours).

### Batching to the provider
Where providers support it (FCM multicast/batch, email batch APIs), **batch** multiple messages per request to cut overhead. **Trade-off:** batching raises throughput and lowers cost but increases per-message latency and complicates partial-failure handling (a batch of 500 where 3 fail needs per-item results), so use it on bulk lanes, not on critical ones.

---

## 8. Core deep dive 4: preferences, consent, and policy (the "don't be spammy or illegal" layer)

The orchestrator runs policy checks **before** any send:

1. **Consent and opt-out.** Per-category, per-channel preferences. **Marketing requires opt-in** in many jurisdictions. **Unsubscribes must take effect fast.** Honor global suppression (hard bounces, complaints, STOP replies for SMS). **On ambiguity or lookup failure, fail closed for marketing** (don't send), and fail open for security alerts and OTPs the user must receive. Legally required (transactional/security) messages may bypass marketing opt-outs, but should be **clearly categorized** to prevent abuse of the "transactional" label.
2. **Quiet hours and time zones.** Defer non-urgent notifications to the user's local daytime. Critical categories bypass. A deferred notification enters the scheduler with a `notBefore`, and its TTL still applies.
3. **Frequency capping.** Per user, per category, per window (e.g., max 3 marketing pushes/day, max 10 total). Implemented with Redis counters (TTL windows). **Trade-off:** caps protect user trust (and avoid opt-outs and uninstalls) but may suppress valuable messages, so rank by importance and apply caps mostly to low-value categories.
4. **Deduplication.** `dedupeKey` per user within a window: "don't send two identical shipping updates." Also **collapse keys** so the device shows only the latest ("3 new messages" replaces earlier ones).
5. **Aggregation / digest.** Combine bursts ("Alice and 12 others liked your post") by holding notifications for a short window and merging. **Trade-off:** reduces noise and volume dramatically, but adds delay (so it applies only to low-urgency categories) and needs per-user, per-aggregation-key state with a timer.
6. **Priority and relevance.** For high-volume products, an ML scorer decides *whether* a notification is worth sending (reducing notification fatigue), the same two-stage logic as a ranker.

**Preference reads must be fast and available.** The Preference Service serves from a cache (a per-user record, TTL plus explicit invalidation on change). **Reliability choice:** if the preference store is unavailable, **critical categories proceed using cached or default policy**, and marketing waits or drops. This is a *per-category failure mode*, the same idea as fail-open/fail-closed in the rate limiter.

**Trade-off:** strict policy checks add a few ms and dependencies on every send, but sending against a user's wishes is a legal and trust failure, so this layer is non-optional.

---

## 9. Hop by hop: reliability and trade-offs

### 1. Ingestion API
- **Stateless, multi-AZ, behind an LB.** Authenticate producers, enforce quotas, validate payloads (size limits, template exists, recipient well-formed).
- **Idempotency** on `(producerId, idempotencyKey)`.
- **Reliability:** this is the one component producers must always reach. **Producers should have a client-side fallback:** a local buffer or retry with the same idempotency key, so the notification platform being down doesn't lose their event. Where producers emit events anyway (e.g., "order shipped" on Kafka), the notification system can **consume the event stream directly** and decouple from API availability.
- **Trade-off:** API-based ingestion is explicit and easy to control, while event-based ingestion is decoupled and replayable but harder to govern (who is allowed to trigger which notification?).

### 2. Kafka lanes
- Partition by `userId` so one user's notifications flow through one partition (enabling per-user ordering, dedupe windows, and aggregation state to be local). **Trade-off:** user-keyed partitions can hot-spot on a single heavy user, which is rare.
- Replication factor 3, `acks=all`. Retention of days for replay.
- Monitor **consumer lag and oldest-message age per lane.** That *is* the latency SLI.

### 3. Orchestrator
- Stateless consumers (state in caches/stores). Fetch preferences, tokens, templates with batched, cached lookups.
- **Idempotent processing:** at-least-once consumption means it may run twice, so delivery records are keyed by `(notificationId, channel, deviceId)` with conditional create, so duplicates become no-ops.
- **Timeouts and degradation:** if the template service is slow, use cached templates. If the preference service is down, apply the per-category fail mode.
- **Poison messages** (bad payload) go to the DLQ after N attempts and never block the partition.

### 4. Template rendering
- Render once per `(template, locale)` structure and substitute variables. **Version templates**, and pin notifications to a version for reproducibility. Sanitize and escape variables (injection into HTML emails is a real risk).
- **Render late (at send time) vs. early (at accept time):** late rendering uses current data and templates but adds latency and failure points in the send path. Early rendering fixes content at accept time (good for audit) but can go stale. **Common compromise:** render in the orchestrator, then store the rendered payload with the delivery record.

### 5. Channel workers and provider adapters
- **Adapters hide provider differences** behind one interface (`send(delivery) → result`), so adding or switching a provider is local. Results are normalized to `ACCEPTED | RETRYABLE | PERMANENT_FAILURE | RATE_LIMITED`.
- **Connection management:** long-lived pooled HTTP/2 connections to APNs/FCM, **reused and warm**. Churning connections gets you throttled.
- **Per-provider circuit breaker + token-bucket limiter + bulkhead** as described above.
- **Timeouts shorter than the visibility/lease timeout,** so a stuck call doesn't trigger a duplicate send from another worker.

### 6. Inbound provider callbacks (the feedback loop)
- Delivery receipts, bounces, complaints, and unsubscribes arrive asynchronously, **sometimes late, duplicated, or out of order** (so handlers must be idempotent and state transitions monotonic).
- **Actions:**
  - **Invalid/unregistered push token** → mark token invalid and stop sending (APNs/FCM tell you).
  - **Hard bounce / spam complaint** → add to the **suppression list** immediately. Continuing to mail them destroys sender reputation.
  - **SMS STOP** → opt-out immediately.
- **Webhook endpoints must be authenticated** (signature verification) and scalable, since a campaign generates a wave of receipts.
- **Trade-off:** tracking opens/clicks (pixels, redirect links) gives engagement data but raises privacy concerns, and mail clients' privacy features make open rates unreliable. Treat opens as a weak signal.

### 7. In-app inbox
- It is a **durable per-user mailbox** (wide-column keyed by `userId`, ordered by time/seq) plus a real-time push to connected clients (WebSocket/SSE, the chat gateway pattern) for live updates.
- **It doubles as a reliability fallback:** even if push delivery fails or is disabled, the notification is waiting in the app, and the client **syncs from a cursor** on open, the same hint-versus-truth principle from chat and Dropbox.
- Track **unread counts** with a counter or watermark (read-up-to), not per-item flags where possible.

---

## 10. Scheduling and delayed delivery

Needs: "send at 9 AM local," "remind me in 2 hours," recurring digests, and deferred quiet-hours sends.

- **Approach:** a **time-bucketed store** (e.g., a table keyed `(bucket = minute, shard)` → pending items) plus pollers that scan due buckets and enqueue, **or** a delay-queue facility (SQS delay, Kafka delay topics per interval, or a hierarchical timing wheel for very large scale). Use a durable scheduler store as the source of truth, with in-memory timers only as an optimization.
- **Execution guarantees:** multiple scheduler instances must not both fire the same item. Use **leases per bucket, and idempotent enqueue** keyed by `(notificationId, scheduledTime)`. The hard parts are **exactly-once-ish firing and failover** (the scheduler pattern from our toolbox).
- **Spikes:** everyone schedules for "9:00 AM," so smooth with jitter inside a tolerance window (e.g., 9:00-9:05) unless the exact time matters.
- **Cancellation and updates:** support cancelling a scheduled notification if the triggering condition changes (the order was cancelled, so don't send "your order ships today"). **Re-check relevance at send time** for conditional notifications.

---

## 11. Special cases worth naming

- **OTP / 2FA:** the highest-priority lane with a **dedicated provider path, short TTL (e.g., 60-120 s), and strict rate limits** (to prevent SMS pumping fraud, where attackers trigger OTPs to premium-rate numbers). Limit per number, per account, and per IP, and monitor for anomalous destinations or countries. Delivery speed is critical, so use direct carrier routes where possible, with a fallback provider and voice-call fallback. **Never log the OTP value.**
- **Security alerts:** bypass marketing opt-outs and quiet hours, deliver on multiple channels simultaneously, and treat as at-least-once.
- **Push for logged-out or re-registered devices:** tokens rotate. Update on every app open and handle invalidation feedback.
- **Silent/data pushes** (wake-ups for sync, as in the chat design): they are best-effort and heavily throttled by the OS, so don't rely on them for correctness.
- **Notification storms from incidents:** an outage can trigger a flood of alerts. Apply **aggregation and per-recipient rate limits**, escalate rather than repeat, and never let alerting overload the system that reports on its own health (keep the paging path isolated).
- **Cost control:** SMS budgets and alerts per team, with a hard daily cap that fails closed for non-critical categories, because a retry loop bug can burn real money quickly.
- **Privacy and security:** don't put sensitive content in push payloads or lock-screen previews by default. Encrypt PII at rest, redact logs, and honor GDPR deletion (purge tokens, history, preferences).

---

## 12. Failure scenarios

| Failure | Impact | Mitigation |
|---|---|---|
| Ingestion API down/slow | Producers can't submit | Multi-AZ, producer-side retry with idempotency key, event-stream ingestion as backup path |
| Worker crashes mid-send | Possible duplicate or lost-ack | Lease and visibility timeout, idempotent delivery records, provider collapse/idempotency keys, accept rare duplicate |
| APNs/FCM slow or throttling | Push backlog | Backpressure into queues, honor `Retry-After`, backoff with jitter, shed bulk first, in-app inbox as fallback |
| SMS/email provider outage | Messages not delivered | Circuit breaker + **secondary provider**, queue buffering, channel fallback for critical messages |
| Provider returns permanent errors (invalid token, hard bounce) | Wasted sends, reputation damage | Stop retries, invalidate token, update suppression list |
| Campaign blast floods the pipeline | Latency for everyone | **Separate lanes and workers**, campaign rate throttling, reserved capacity for critical |
| Retry storm after recovery | Re-overload provider | Jittered backoff, per-provider token buckets, gradual ramp |
| Kafka lag grows | Delayed delivery | Autoscale consumers (within provider limits), alert on oldest-message age, drop expired, pause bulk |
| Preference/consent service down | Can't verify consent | Cached prefs, per-category fail mode (marketing fails closed, security fails open) |
| Duplicate event from producer | Double notification | Idempotency key at ingest, `dedupeKey` window in orchestrator, collapse keys on device |
| Stale notification delivered late | Confusing or harmful | TTL enforcement at every stage, drop `EXPIRED`, re-check relevance at send time |
| Provider callbacks lost or delayed | Unknown delivery status | Status is best-effort. Reconcile by polling provider APIs for critical sends, and time out to `UNKNOWN` |
| Scheduler double-fires or misses | Duplicate or missed reminders | Bucket leases, idempotent enqueue, catch-up scan after failover |
| Bad template/config deploy | Wrong or broken content to millions | Template versioning, staged rollout, preview/test sends, dry-run, kill switch per template and campaign |
| Sending to wrong audience (bug) | Reputational and legal harm | Segment size sanity checks, approval flows and test sends for large campaigns, **circuit-breaker on unusual volume** |
| SMS pumping / fraud | Large unexpected cost | Per-number, per-account, per-country limits, anomaly detection, budget caps |
| Region outage | Regional sends stop | Multi-region active-active ingestion, replicated queues/state, regional provider endpoints, failover |
| Secrets/credentials leak (provider keys) | Abuse via your sender identity | Secrets manager, rotation, scoped credentials, audit and alerting on send spikes |

**Principle (isolate and degrade gracefully):** a notification system must fail **toward doing less, in priority order.** Under stress, shed bulk and marketing first, then normal traffic, and **protect critical (OTP/security) at all costs.** Because the product's core functions don't depend on notifications being sent, failures cost engagement, but **a duplicate or stale OTP is a worse failure than a delayed marketing push.**

---

## 13. Consistency decisions

- **Accepted notifications:** **durable**, with persist-before-ack and an idempotent ingestion key.
- **Delivery semantics:** **at-least-once** internally, with dedupe and collapse keys to minimize duplicates, and an explicit per-category stance on loss versus duplicates.
- **Ordering:** **not guaranteed globally,** and only loosely per user (partitioned by `userId`). Content should be self-contained and time-stamped, and collapse keys make the latest win.
- **Preferences and consent:** **strong enough to honor opt-outs promptly.** Use short cache TTLs and invalidate on change. A user who unsubscribes should not get another marketing message beyond a small propagation window.
- **Suppression lists (bounces, complaints):** strongly propagated and checked on every send, because mailing suppressed addresses damages reputation.
- **Delivery status:** eventually consistent and best-effort (providers report late or never), so expose it as such.
- **Counters (frequency caps, quotas):** approximate is fine, with local-plus-central hybrids as in the rate limiter. Cost caps should be exact enough to prevent runaway spend.
- **In-app inbox:** durable per user, synced by cursor.

---

## 14. Multi-region

- **Ingestion is active-active** across regions (producers hit the nearest), with durable local persistence and replication.
- **Per-user state** (preferences, tokens, inbox) has a **home region** with async replication. Sends can be processed in any region by reading replicated state, with a risk of slightly stale prefs (mitigated by short TTLs and invalidation messages).
- **Avoid double-sends during failover:** use **per-notification ownership** (partition by `userId` → home region) or **dedupe at the delivery-record level** with a conditional write on `(notificationId, channel, deviceId)` in a replicated or globally consistent store. This is the multi-region duplicate-risk to call out.
- **Provider endpoints:** use regional ones where possible for latency and data-residency (some SMS regulations require local routes and sender IDs per country).
- **Data residency:** pin PII (tokens, phone numbers, content) to allowed regions for GDPR-like constraints.
- **Trade-off:** multi-region gives resilience and lower latency but adds replication lag and duplicate-send risk. For critical OTP traffic, accepting a rare duplicate during failover is better than dropping.

---

## 15. Observability and operations

- **SLIs:** end-to-end latency (accept → provider-accepted → delivered) **per priority and channel**, oldest-message age per lane, delivery success rate, provider error rates, DLQ depth, duplicate rate, opt-out and complaint rates.
- **Alert on lane age, not just errors.** A silent backlog is the typical failure mode.
- **Per-provider dashboards and automated health scoring** feed routing decisions.
- **Tracing** a notification across stages with its `notificationId` (support will constantly ask, "why didn't I get this?"). Provide a **debug view** showing each decision: suppressed by quiet hours, capped, token invalid, provider rejected.
- **Test and safety tooling:** sandbox mode, test sends, canary segments (send to 1% first and watch metrics), per-template and per-campaign **kill switches**, and a global pause for bulk.
- **Replay tooling** for DLQs with safeguards (so replaying doesn't resend stale or already-delivered messages).

---

## 16. Scaling summary

| Component | Strategy |
|---|---|
| Ingestion | Stateless, multi-AZ, per-producer quotas, idempotency keys, persist-before-ack |
| Queues | Kafka topics **per priority × channel**, partitioned by `userId`, RF=3, dedicated consumer pools |
| Orchestrator | Stateless, cached prefs/tokens/templates, idempotent, DLQ for poison messages |
| Channel workers | Per-provider pools with token-bucket limits, circuit breakers, pooled long-lived connections |
| Providers | Multiple for SMS and email, health-based routing, APNs/FCM with backoff and fallback channels |
| Campaigns | Chunked, resumable, throttled fan-out, time-zone-aware, lowest priority |
| Preferences/tokens | Sharded KV by `userId`, cached, invalidated on change |
| Scheduling | Time-bucketed durable store with leases, jittered release |
| Status/feedback | Idempotent webhook handlers, suppression list, token invalidation |
| In-app inbox | Wide-column by `userId`, cursor sync, real-time via gateway |
| Storage | Hot 30-90 days for status, archive to object storage |
| Regions | Active-active ingestion, home-region user state, dedupe at delivery records |

---

## 17. Likely follow-up questions

- **"How do you make sure an OTP isn't delayed by a marketing blast?"** Physically separate priority lanes with dedicated consumers and provider capacity, so bulk can't borrow critical capacity, plus campaign throttling.
- **"How do you avoid duplicate notifications?"** An idempotency key at ingestion, a `dedupeKey` window and conditional delivery records in the orchestrator, provider collapse keys, and idempotent consumers. The residual crash window is accepted for most categories.
- **"Exactly-once?"** Not achievable end to end. At-least-once plus idempotence gives practically exactly-once, with a stated policy per category.
- **"What happens when APNs/FCM is down or slow?"** Backpressure into queues, backoff with jitter honoring `Retry-After`, TTL-based expiry, shedding bulk, and in-app inbox or alternate channel for critical messages.
- **"How do you handle 100M-user campaigns?"** Chunked, resumable, throttled fan-out through a campaign service, spread over time and time zones, lowest priority, with a protective rate on the downstream app backend.
- **"How do you respect user preferences at scale?"** A cached preference service checked in the orchestrator, per-category fail modes, fast unsubscribe propagation, suppression lists, quiet hours, and frequency caps.
- **"How do you retry safely?"** Only on transient errors, exponential backoff with jitter via delay queues, TTL cap, DLQ after N attempts, and a per-provider rate limiter to avoid retry storms.
- **"How do you ensure delivery for critical messages?"** Multiple providers, circuit-breaker failover, channel escalation (push to SMS), dedicated capacity, and monitoring of lane age.
- **"How do you track delivery status?"** Provider receipts via authenticated webhooks, idempotent and monotonic state updates, polling for critical sends, and treating status as best-effort.
- **"How do you design scheduling?"** A time-bucketed durable store with lease-based pollers (or delay queues), idempotent enqueue, jitter, and relevance rechecks at send time.
- **"How would you reduce notification fatigue?"** Frequency caps, aggregation and digests, collapse keys, relevance scoring, and send-time optimization.
- **"How do you prevent SMS fraud and runaway cost?"** Per-number, per-account, and per-country limits, anomaly detection, budget caps with fail-closed behavior for non-critical categories.
- **"What if a bad campaign is sent to the wrong audience?"** Dry runs, canary sends, approval for large audiences, volume anomaly circuit breakers, and a per-campaign kill switch that stops further chunks.
- **"How would you add a new channel (e.g., WhatsApp)?"** Write a new adapter with its own queues, limiter, and callback handler, and add it to the channel selection and preferences schema. The pipeline stays unchanged.
- **"How do you scale this 10×?"** More partitions and consumers per lane, more provider credentials/senders (where allowed), more shards for state, and tighter bulk throttling. Remember that **provider throughput, not your infrastructure, is usually the real limit.**

---

## 18. How to narrate it

1. **Name the dominant problems:** unreliable third-party providers, priority isolation under spiky load, and user-protection policy. Raw QPS isn't the challenge.
2. **Start naive:** a service calls the push/SMS API synchronously inside the request path. Show the failures (provider latency blocks the caller, outages lose messages, blasts starve urgent ones, no retries, no preferences). Then add a durable queue, async workers, retries and DLQ, priority lanes, policy checks, provider adapters and failover, then bulk fan-out and scheduling.
3. **State the contract:** accepted means durable (persist-before-ack), at-least-once with dedupe, TTL-bounded freshness, and consent always honored.
4. **Separate lanes by priority and channel,** with bulkheads per provider, and say that **shedding order is bulk → normal → high, never critical.**
5. **Call out the knobs:** lane capacities, provider rate limits, retry schedule and max attempts, TTLs per category, frequency caps, quiet-hour rules, aggregation windows, campaign throttle, circuit-breaker thresholds.
6. **For each box, say what happens when it fails,** and end with "any provider or worker can fail and we retry, fall back, or expire the message, while critical traffic stays isolated from everything else."

**Patterns reused from our toolbox:** idempotency keys and persist-before-ack (Uber, Dropbox, chat), the transactional outbox, at-least-once plus idempotent consumers with DLQs (queues pattern, crawler), the rate limiter (now outbound, per provider), circuit breakers, bulkheads and load shedding, backoff with jitter and thundering-herd control, hybrid push/pull and chunked fan-out (feed), cursor-based sync as the truth with push as a hint (chat, Dropbox), scheduler with leases and fencing, sliding-window and TTL counters (rate limiter, Top-K), state machines with conditional writes (Uber trips), fail-open versus fail-closed per category (rate limiter), and static stability and graceful degradation.

Next, I can play interviewer on this (for example, "a bug caused a retry loop and 5M duplicate SMS were sent in 10 minutes, walk me through detection, containment, and prevention"), or we can move on. **Payment system** and **ticket booking** go deep on strong consistency, idempotency, and contention, which have mostly been avoided so far by using eventual consistency. **Distributed cache / KV store** and **typeahead/search autocomplete** are good picks if you want to drill into core infrastructure pieces we've used as black boxes.
