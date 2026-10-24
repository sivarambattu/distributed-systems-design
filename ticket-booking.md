# Ticket Booking System Design (Ticketmaster / BookMyShow style)

**What to say early:** Every earlier design could tolerate some staleness or lost work. Here the defining problem is **contention on scarce, indivisible inventory under a violent traffic spike**. Normal volume is tiny, but when a stadium tour goes on sale, millions of people want the same 50,000 seats in the same five minutes. Three guarantees organize the design:

1. **Never sell the same seat twice.** This needs a single authority and an atomic state transition per seat.
2. **Holds must expire.** A person who picks seats and wanders off can't lock inventory forever, but a person who is mid-payment must not lose their seat.
3. **Protect the booking path from the crowd.** You don't scale to 5M concurrent buyers. You **admit them at a rate the core can handle** and make the waiting experience cheap and fair.

Scope it as two inventory models, because the data model differs:
- **Reserved seating** (concerts, theaters): each seat is a unique row, and contention is per seat.
- **General admission or capacity-based** (festivals, airline fare buckets, hotel room types): inventory is a **counter**, and contention is on one hot row.

I'll design reserved seating and note the counter variant at the end.

---

## 1. Requirements

**Functional**
- Browse events and search by artist, venue, date, and city
- View a **seat map** with availability and prices
- **Select seats** (or request "best available" for N tickets) and **hold** them temporarily
- Check out (payment), then receive **confirmed tickets** (QR/barcode)
- Cancel, refund, and release seats; optionally transfer or resell
- Waiting room for high-demand on-sales
- Venue-side: entry validation by scanning tickets

**Non-functional**
- **Zero double-sells** (the hard guarantee)
- **Fairness and bot resistance** during on-sales
- **Availability under flash load.** The system must degrade into a queue, not fall over.
- **Latency:** browsing in milliseconds. Hold and checkout within a second or two.
- **Durability:** a paid order is never lost
- **Auditability:** every seat state change is traceable

**Clarify with the interviewer:** Reserved seats, GA, or both? Is overbooking allowed (airlines yes, concerts no)? Expected peak concurrent users for one event? Is resale/transfer in scope? Is payment in scope (I'll treat it as an integration with the payment design from earlier)? How long is the hold?

---

## 2. Estimation (state assumptions)

**Steady state**
- 20M DAU browsing, **~1M tickets sold/day ≈ 12/sec average.** Trivial.
- Browse/search/seat-map reads dominate and are heavily cacheable.

**The flash on-sale (this sizes the system)**
- One event: **70K seats.** 5M users arrive in the first minutes.
- Edge requests: **~1M/sec at the peak** (refreshing, polling seat maps, queue heartbeats)
- Sellout in ~5 minutes → **~230 successful purchases/sec**, but hold attempts run **10-50× higher** (people fight over the best seats and most attempts lose), so **~5-10K hold attempts/sec** on that one event
- **Inventory for the event:** 70K seats × ~100 B ≈ **7 MB.** Tiny. It fits in one DB shard, one Redis node, or one process's memory.
- Seat-map payload: ~70K seats as a compact bitmap/state array is **~100-200 KB compressed**, or far less as deltas.
- Held-seat count at any moment: ~(attempts × hold duration), bounded by the seat count itself.

**Takeaway to say aloud:** Storage and average load are trivial, and sharding by event doesn't help the hot event, because **all the contention is on one tiny dataset.** The design is about (a) a **serialized, correct authority** for that dataset, (b) **admission control** so 5M users become ~5K attempts/sec, and (c) cheap, cacheable reads for everyone else.

---

## 3. API and data model

```
GET  /v1/events?query=&city=&date=                      → search/browse (cached)
GET  /v1/events/{id}/seatmap?version=                   → seat states (CDN, 1-2s TTL) 
POST /v1/events/{id}/queue/join                          → { queueTicket }     (waiting room)
GET  /v1/events/{id}/queue/status?ticket=                → { position?, admitToken? }
POST /v1/holds       { eventId, seatIds[] | {qty, section, bestAvailable}, admitToken, idemKey }
                     → { holdId, seats[], expiresAt } | 409 SEATS_TAKEN
POST /v1/holds/{id}/extend | DELETE /v1/holds/{id}      (release)
POST /v1/orders      { holdId, paymentMethodToken, idemKey } → { orderId, status: PROCESSING|CONFIRMED|FAILED }
GET  /v1/orders/{id}  /  GET /v1/orders/{id}/tickets     → QR/barcode payloads
POST /v1/orders/{id}/cancel
```

| Entity | Shape | Store |
|---|---|---|
| **Event / venue / seat template** | `eventId → venue, time, onSaleAt, pricing tiers`; `venueId → sections, rows, seat coordinates` | Relational/document DB, heavily cached, near-immutable |
| **Seat inventory** | `(eventId, seatId) → state (AVAILABLE/HELD/SOLD/BLOCKED), holdId?, holdExpiresAt?, orderId?, version, priceTier` | **Strongly consistent DB, partitioned by eventId**, the source of truth |
| **Hold** | `holdId → userId, eventId, seatIds[], expiresAt, state (ACTIVE/CONVERTING/RELEASED/EXPIRED/CONVERTED), idemKey` | Same DB shard as the event's inventory |
| **Order** | `orderId → userId, holdId, items[], amount, status, paymentId, version` | Relational (same shard or via saga) |
| **Ticket** | `ticketId → orderId, eventId, seatId, barcodeSecret, state (VALID/SCANNED/VOID/TRANSFERRED)` | Relational, indexed by owner |
| **Queue state** | `(eventId, ticketId) → joinedAt/rank, userId, admitState, admittedUntil` | Redis (fast) with periodic durable checkpoint |
| **Seat-map snapshot** | `(eventId, version) → compact state array` | Cache/CDN, derived from inventory |
| **Purchase limits** | `(eventId, userId/paymentFingerprint) → count` | KV with conditional increments |
| **Idempotency + outbox** | As in the payment design | Same DB |

**Key design points:**
- **Partition by `eventId`.** All seats of an event live on one shard, so **a multi-seat hold is a single-shard transaction.** That's the property that makes atomic "all N seats or none" cheap.
- **Seat state is a small state machine** with compare-and-set transitions: `AVAILABLE → HELD → SOLD`, and `HELD → AVAILABLE` on release or expiry. `SOLD` is terminal except for refund/void flows.
- **Expiry is stored as a timestamp on the row** (`holdExpiresAt`), not only as a timer. Correctness is evaluated at write time against the clock (next section).

---

## 4. High-level architecture

```
                   READ / BROWSE PATH (cacheable, huge fan-out)
Client ──► CDN (event pages, static assets, seat-map snapshots, waiting-room page)
        ──► API Gateway ──► Catalog/Search Service ──► Search index + Catalog DB (cached)
                       └─► Seat-Map Service ──► in-memory snapshot (updated from inventory change stream)

                   ADMISSION (protects the core)
Client ──► Waiting Room (edge-hosted, stateless token verification + Redis queue)
              │  admits users at a controlled rate (e.g. 3-5K/sec) with signed, short-lived admitTokens
              ▼
                   BOOKING PATH (strongly consistent, rate-limited)
API Gateway (verify admitToken, bot checks, per-user limits, idempotency)
   ──► Booking Service (stateless)
          1. validate hold request, purchase limits
          2. atomically acquire seats  ──►  Inventory Authority (per-event shard)
          3. create hold (expiresAt), return seats
   ──► Checkout/Order Service (saga orchestrator)
          4. mark hold CONVERTING (extends expiry while payment is in flight)
          5. Payment Service (authorize/capture; idempotent; unknown-outcome handling)
          6. confirm: seats HELD→SOLD + order CONFIRMED + tickets issued (one transaction)
          7. outbox ──► Kafka: order.confirmed, seat.changed, hold.expired
                              ▼
        Notifications · Ticket delivery · Analytics · Seat-map updater · Fraud/Bot analysis

Sweeper: releases expired holds, resolves stuck CONVERTING holds, reconciles with payments
```

---

## 5. Core deep dive 1: preventing double-booking

### Why it's hard
Two users click the same seat in the same millisecond, and each request reads "available" and then writes "mine." This is a **check-then-act race**, the same shape as the over-refund and double-driver-assignment races in earlier designs.

### Naive: read, check, then write
```
if seat.state == AVAILABLE: seat.state = HELD
```
Under concurrency, both requests pass the check. **Double-sell.** Dead on arrival.

### Option A: Pessimistic locking (`SELECT ... FOR UPDATE`)
Lock the seat rows, check, update, commit.
- **Pros:** simple and correct. The database does the arbitration.
- **Cons:** locks are held while the transaction runs, so contention on popular seats **queues threads and connections.** Multi-seat requests that lock rows in different orders **deadlock.** Fix by always locking in sorted `seatId` order.
- **Never hold a lock across the user's think time or the payment call.** Locks cover only the milliseconds of the state transition. The long-lived claim is the **hold record**, not a database lock.

### Option B: Optimistic concurrency / conditional update (my default)
A single atomic statement is the arbiter:
```sql
UPDATE seats
SET state='HELD', holdId=:h, holdExpiresAt=:t, version=version+1
WHERE eventId=:e AND seatId IN (:s1,:s2,:s3)
  AND (state='AVAILABLE' OR (state='HELD' AND holdExpiresAt < now()))
-- success only if rows_affected == number of seats requested; otherwise ROLLBACK
```
- **Atomic all-or-nothing:** if fewer rows updated than requested, roll back, and the user hears `409 SEATS_TAKEN` and picks again.
- **No application-level race,** because the `WHERE` clause is evaluated atomically with the write.
- **Expiry is evaluated inline** (the `OR holdExpiresAt < now()` clause), so an expired hold is **takeable immediately**, with no dependence on a sweeper having run. The sweeper is cleanup and bookkeeping, not correctness.
- **Trade-off:** under extreme contention many attempts fail and retry. That's acceptable, because failure is cheap, and it's *why* we put admission control in front.

### Option C: Redis in front (fast holds), DB as the truth
Use Redis `SET seat:{e}:{s} holdId NX EX 480` (or a Lua script setting several seats atomically), then persist to the DB.
- **Pros:** very fast and absorbs the hold-attempt storm in memory.
- **Cons:** now there are two systems to keep consistent. If Redis fails over, an asynchronously replicated primary can **lose a hold** and hand the same seat to someone else. A hold in Redis that isn't durably reflected in the DB can lead to a confirmed order for a seat that was double-granted.
- **Rule if you use it:** **Redis is an optimization layer, and the final `HELD → SOLD` transition is always a conditional write in the DB** that checks the `holdId`. Redis may lose a hold, and the DB still prevents the double-sell at commit. Treat Redis hold loss as a bad user experience (seat lost at checkout), never as data corruption.
- **Distributed locks** (Redlock-style) are unnecessary here, and dangerous without **fencing tokens**. A conditional write on the row is strictly safer.

### Option D: Single-writer per event (the in-memory authority)
Route every operation for an event to **one owner process** (partition by `eventId`) that holds the seat states in memory and processes commands **sequentially** (LMAX/actor style), with a **write-ahead log** and replication.
- **Pros:** **no lock contention at all.** Throughput of tens of thousands of ops/sec on one core for 70K seats, trivially linearizable per event, and it makes batching and "best available" search fast.
- **Cons:** the owner is a **single point of failure per event** (needs leader election with fencing and log replication, i.e. Raft), plus more custom infrastructure.
- **When to choose it:** for the very hottest events, as an upgrade path. Start with Option B on a relational shard.

**State your decision:** "Seat inventory partitioned by event in a strongly consistent relational DB. Holds and sales are conditional atomic updates (optimistic), multi-seat requests are single-shard transactions in sorted order, and expiry is checked inline at write time. Redis can front the read path or the hold storm, but the DB conditional write is the final arbiter. For the hottest events, I'd move to a single-writer in-memory authority with a replicated log."

### Seat selection and hot spots
- **Everyone wants the same 200 best seats,** so contention is concentrated and most picks lose.
- **Best-available assignment (server-side):** the user asks for "2 tickets in the 100 level," and the server finds and holds contiguous seats. **Contention drops sharply,** because the server can spread load across all available seats and avoids the thundering herd on specific seats, and it can also optimize venue fill (avoid leaving single orphan seats).
- **Trade-off:** user choice and perceived control versus contention and fill efficiency. Many on-sales offer best-available only during the first minutes, then allow full seat-map picking.
- **On conflict, suggest alternatives** immediately (nearest available seats) so the user doesn't have to start over.

---

## 6. Core deep dive 2: holds, expiry, and the payment race

### Hold semantics
- A hold is a **time-boxed reservation** (commonly **5-10 minutes**). It has a record (`holdId`, seats, `expiresAt`) and sets the seats to `HELD`.
- **Hold duration is a business knob:**
  - **Short** (3-5 min): inventory recycles quickly and fewer seats sit idle, but slow payers (3-D Secure, typing a card) lose their seats and get frustrated.
  - **Long** (10-15 min): friendlier, but abandoned holds starve other buyers in a sell-out.
- **Show a countdown** and offer a one-time **extension** if the user is actively progressing.

### Expiry: lazy plus active
- **Lazy (correctness):** the conditional update treats expired holds as available (above). This means **no stale `HELD` seats can block a purchase,** even if background jobs are lagging.
- **Active (hygiene and UX):** a **sweeper** (or delayed-queue consumer or time-bucketed scan) flips expired holds to `RELEASED`, emits `seat.changed` events so the seat map updates, and releases purchase-limit counters.
- **Trade-off:** timers alone are fragile (lost on crash, drift), and lazy alone means seat maps show phantom "held" seats until someone tries. Using both gives correctness plus a live UI.
- **Clock caution:** compare against **one authoritative clock** (the DB server's `now()`), not application servers' clocks, so skew can't cause an early or late expiry.

### The classic race: payment completes after the hold expires
Timeline: hold expires at 10:00:00 → another buyer grabs the seat at 10:00:01 → the first buyer's payment (started 9:59:58, slow 3DS) succeeds at 10:00:03. You've **charged someone for a seat they no longer have.**

**Fix: freeze the hold when payment starts.**
1. On checkout, transition the hold to **`CONVERTING`** (conditional: only if it is still `ACTIVE` and not expired) and **extend or suspend its expiry** for a payment window (e.g., +3-5 min, long enough for authorization and 3DS).
2. Only a `CONVERTING` hold can be confirmed, and **the seats can't be taken by others while `CONVERTING`.**
3. If the hold already expired and was taken before checkout began, the `CONVERTING` transition **fails** and the user is told right away, *before* any money is touched.
4. **Confirm** by one conditional transaction: `seats HELD→SOLD WHERE holdId=:h` + `order → CONFIRMED` + tickets inserted + outbox event.
5. **If payment fails or the user abandons:** release the hold (`CONVERTING → RELEASED`) and seats return to `AVAILABLE`.
6. **Backstop:** if payment somehow succeeded but the seats can't be confirmed (a bug or a lost hold), **void/refund automatically** and tell the user. Never keep money for tickets you can't deliver.

**Trade-off:** freezing the seat during payment prevents the race but also lets a slow or stuck payment keep a seat out of circulation, so the payment window is **bounded and short.** The alternative, "optimistic payment" (charge first, hope the seat is still there), is simpler but produces refund churn and angry customers in a sell-out.

### Unknown payment outcomes
This inherits the payment design's rule: a timeout means **"I don't know."** The hold stays in `CONVERTING` while the sweeper queries the payment provider (by idempotency key) and drives it to confirmed or released. **Don't release seats on a timeout that might have succeeded** (you could sell the seat twice and charge both buyers). Hold them until the payment state is resolved.

---

## 7. Core deep dive 3: surviving the flash sale (traffic shaping)

### The failure mode
5M people hit "buy" at 10:00:00. Without protection:
- The CDN origin, seat-map service, and Booking Service saturate.
- Everyone refreshes, which **multiplies load** (a retry storm).
- The DB melts under hold attempts, and no one succeeds. Worse, **the system fails randomly**, which feels unfair and lets bots win.

### The virtual waiting room (admission control)
Instead of letting everyone in, **hold them in an edge-served queue and admit a controlled flow.**
- **Pre-queue (optional):** users arrive before the on-sale and receive a **random position** (lottery) for everyone who arrives in the early window. Arriving at 9:59:59 vs. 9:59:30 shouldn't matter. **Fairness trade-off:** lottery is fairer and removes the "fastest bot" advantage, FIFO is simpler and what people expect, so say which you'd choose and why.
- **Queue mechanics:**
  - The waiting page is **static and served from the CDN**, so it costs almost nothing.
  - The client **long-polls or opens a lightweight connection** for status at a **jittered, server-instructed interval** (the server controls how often clients check, to prevent a polling storm).
  - Queue state lives in **Redis** (a sorted set by rank or an integer counter pair: "next to admit" and "my ticket number").
- **Admission:** a controller admits users at the rate the booking path can sustain (e.g., 3-5K/sec), adjusted **by feedback** (hold success rate, DB latency, seats remaining). It issues **signed, short-lived `admitTokens`** (JWT-like, tied to user/session and event, valid for N minutes).
- **Token verification is stateless at the edge/gateway,** so the Booking Service does no queue lookups, and **requests without a valid token are rejected cheaply before reaching the core.**
- **Admit by capacity, not by headcount:** with 70K seats and many people dropping out, the target is "enough active shoppers to keep seats contested but not enough to melt the system," and it should **slow down as the event sells out.**
- **Sell-out behavior:** when seats hit zero, **flip the page to "sold out"** at the edge (a flag in config the CDN honors), **drain the queue with a clear message,** and stop admitting. This also removes the load of 4M people still waiting.
- **Trade-off:** admission control adds a visible wait and an extra system, but it **converts an unbounded spike into a bounded flow.** The alternative is an outage plus an unfair outcome. Waiting rooms are the standard answer.

### Shed load before it matters
- **Read path:** cache the seat map snapshot at the CDN with a **1-2 s TTL**, or serve it from an in-memory tier updated from the inventory change stream. For better UX, push **deltas over SSE/WebSocket** (seat X just got taken), so clients don't poll. **Trade-off:** staleness means a user clicks a seat that's gone, which is fine (the authoritative check is the hold). Optimistic UI plus a quick `409` and nearest-alternatives response keeps it smooth.
- **Rate limit** per user, session, IP, device, and payment fingerprint (the rate limiter design), and **fail closed** on the booking path for abuse protection.
- **Idempotency keys** on holds and orders, so client retries don't create duplicates.
- **Backpressure:** if the inventory authority slows, the admission controller **reduces the admit rate** automatically, a feedback loop rather than a fixed number.
- **Static stability:** the CDN-served waiting page, event pages, and a cached seat map keep working even if the booking core is struggling.

### Scalpers and bots (they are a core requirement, not an extra)
- **Signals:** CAPTCHA or proof-of-work challenges at queue entry, device and browser fingerprinting, behavior and timing analysis, IP/ASN reputation (datacenter ranges, residential proxies), account age and history.
- **Verified-fan / pre-registration:** register in advance, verify with a phone and payment method, and receive **invite codes or priority tokens.** Strongest fairness lever, but reduces spontaneity.
- **Purchase limits:** max N tickets per event per account, **per payment fingerprint, per address, per device.** Enforce atomically with conditional counters at hold time.
- **Single active session per account,** and one queue entry per verified identity.
- **Dynamic pricing** (priced by demand) captures the scalpers' margin for the seller, as an economic rather than technical answer, with a PR cost.
- **Anti-resale controls:** non-transferable tickets, rotating barcodes, name-on-ticket.
- **Trade-off:** every bot control adds friction for real fans and false positives (a legitimate user blocked at the worst moment), so **tier the controls** and keep an appeal path.

---

## 8. Hop by hop: reliability and trade-offs

### 1. CDN / edge
- Serves event pages, assets, the waiting-room page, and seat-map snapshots. Short TTLs plus request coalescing so a TTL expiry doesn't create a herd on origin.
- **Edge flags** (sold out, on-sale not started) are the cheapest load-shedding mechanism there is.

### 2. Waiting room
- **Stateless verification** (signed tokens), with queue position in Redis. A Redis failure must not lose everyone's place, so **checkpoint** the queue state (or make positions deterministic from join timestamps in a durable log) and let clients re-validate with their ticket.
- **Reliability:** if the waiting room itself fails, **fail closed** (send people to a static "please wait" page, not directly to the booking path), because an open door during an on-sale is the worst outcome.

### 3. API gateway and Booking Service
- Verify admit tokens, enforce limits, require idempotency keys, and **bound concurrency** to the inventory authority (a bulkhead, so one event's flood can't consume resources for all events).
- **Timeouts and bounded retries only on idempotent calls.** A hold retry with the same idempotency key returns the original result.
- **Isolation per event:** big on-sales get **dedicated capacity or separate deployments (cell-based architecture)**, so one blockbuster can't degrade ticket sales for every other event.

### 4. Inventory authority (per-event shard)
- A **strongly consistent relational DB** with **synchronous replication** across AZs (RPO = 0 for sold seats), and automatic failover. Sales are low-volume in absolute terms, so favor correctness over raw throughput.
- **Pre-provision** shards or dedicated instances for known big events (you know the on-sale date weeks ahead). **Pre-warm** caches and connection pools, and load-test at 2× expected demand.
- **Hot row contention:** the "best 200 seats" are hot rows. The conditional update fails fast, there's no long lock queue, and best-available assignment spreads the load.
- **Trade-off:** one shard per event means a shard failure affects exactly one event (small blast radius), while the hot event itself can't scale beyond a single primary's throughput, which is exactly why admission control exists.

### 5. Checkout / saga orchestrator
- A **state machine per order** with persisted state (`HOLD_CONVERTING → PAYMENT_PENDING → CONFIRMING → CONFIRMED / FAILED`), every transition a compare-and-set, **driven to completion by a sweeper** if the orchestrator crashes (the same recovery pattern as the payment and Uber designs).
- **Compensations:** payment failure → release hold. Confirm failure after successful payment → void/refund. These must be **idempotent and retryable.**
- **Don't use 2PC** between the inventory DB and the payment provider (you can't enroll a PSP in 2PC). Use the saga plus the outbox.

### 6. Seat-map and read tier
- Built from the inventory change stream (outbox → Kafka → in-memory snapshot per event). Stateless nodes hold the current snapshot and serve it compressed.
- **Eventually consistent by design,** since the hold is the authority. **Degradation:** if the seat-map service lags, serve the last snapshot with a "may be out of date" flag.

### 7. Ticket issuance and entry
- **Ticket = signed payload** (eventId, seatId, ticketId, nonce) with a server-side secret, rendered as a QR. **Rotating barcodes** (refresh every ~15-30 s via the app) defeat screenshot sharing and many resale scams.
- **Entry scanning:** scanners validate the signature and mark the ticket `SCANNED` with a **conditional write** (first scan wins, so a duplicate scan is rejected). **Gates must work offline:** pre-download the event's ticket list or the signing keys, sync scans when connectivity returns, and accept a small duplicate-entry risk during partitions.
- **Trade-off:** strict online validation is the safest but fails when venue Wi-Fi collapses (common at 70K-person events), so design for **offline-first scanning with eventual reconciliation.**

### 8. Notifications and post-purchase
Async consumers off Kafka (order confirmation email, wallet pass, reminders), **at-least-once with idempotent handlers**, never on the critical path of confirming the order.

---

## 9. Variants

### General admission / capacity-based inventory
- There are no unique seats, only a **counter** (`available = 5000`).
- **Atomic decrement with a floor:** `UPDATE inv SET avail = avail - :n WHERE eventId=:e AND avail >= :n`, which is a conditional update on a **single hot row.**
- **Hot-row problem:** all buyers update one row, which **serializes** at the row lock. Fixes: **shard the counter** into K buckets (each with a slice of capacity) and try buckets randomly, or use a **single-writer in-memory counter** per event, or reserve in **batches** per Booking node (a leased block of tickets, like range allocation in the URL shortener).
- **Trade-off of bucketed counters:** high throughput, but near sell-out you can have **"phantom sold out"** (every bucket low, though the total is not zero), so add a **rebalance** step for the tail or fall back to a single authoritative counter when remaining capacity is small.

### Airlines and hotels
- Inventory is **by date range and fare class** (a hotel room over a multi-night span means **multiple date rows that must be reserved atomically**).
- **Overbooking is intentional** (statistical no-shows), so inventory limits are soft with compensation policy for bumping. Concerts forbid it.
- **Pricing and availability change continuously** (revenue management), so fare buckets and quotes with short validity matter (same locked-quote idea as surge pricing in the Uber design).

### Resale and transfer
Transfers invalidate the old barcode and issue a new one atomically (conditional write on `ticket.version`). Resale marketplaces add listings, escrow, and fee ledgers (the double-entry ledger from the payment design).

---

## 10. Failure scenarios

| Failure | Impact | Mitigation |
|---|---|---|
| Two users grab the same seat | Double-sell | Atomic conditional update (state + hold check), single-shard transaction, DB is the arbiter |
| Hold expires, then payment succeeds | Charged for a seat someone else owns | `CONVERTING` freeze before payment, confirm only with matching `holdId`, auto-void as backstop |
| Payment call times out | Unknown outcome | Keep seats held in `CONVERTING`, query PSP by idempotency key, sweeper resolves |
| Booking Service crashes mid-order | Stuck hold or order | State persisted in DB, sweeper drives to terminal state, idempotent retries |
| Redis (hold cache) failover loses holds | Seat lost at checkout | DB conditional confirm prevents double-sell, user gets a clean failure + alternatives |
| Inventory DB primary fails | Writes blocked briefly | Synchronous replica promotion (no lost sold seats), retries with idempotency keys |
| Thundering herd at on-sale | Meltdown, unfair outcomes | **Waiting room**, admission rate controlled by feedback, edge static pages |
| Waiting room component fails | Open floodgates | **Fail closed** to a static page, redundancy, signed tokens verifiable statelessly |
| Retry/refresh storm | Load multiplies | Server-controlled poll intervals with jitter, 429 with `Retry-After`, rate limits |
| Bots dominate inventory | Real fans locked out | Verified-fan registration, challenges, purchase limits, fingerprinting, anomaly detection |
| Abandoned holds starve a sell-out | Phantom unavailability | Short TTL, lazy expiry inline, active release, extension only on progress |
| Stale seat map | User clicks taken seats | Delta push, short snapshot TTL, fast `409` with nearest alternatives |
| Sweeper lags | Seat map shows phantom held seats | Correctness is lazy-expiry-based, so only UI freshness suffers, alert on lag |
| Overselling due to a bug | Angry customers, legal and financial cost | Invariant monitors (sold ≤ capacity, one SOLD per seat), DB unique constraints, reconcile continuously |
| Clock skew between servers | Early or late expiry | Use the DB's clock for all expiry comparisons |
| Venue gate loses connectivity | Entry stalls | Offline-capable scanners with pre-synced data, later reconciliation |
| One blockbuster starves other events | Cross-event impact | Per-event isolation (cells), bulkheads, dedicated capacity |
| Region outage mid-on-sale | Event unavailable | Event-home-region failover with synchronous replica, waiting room at edge keeps queue state |
| Bad deploy during on-sale | Mass errors | Change freezes during on-sales, canaries beforehand, kill switches |
| Duplicate order from client retry | Double purchase | Idempotency key + unique constraint on `(userId, idemKey)` |

**Principle (fail toward "no" instead of "double yes"):** when uncertain, the system should **refuse to sell** or **keep the seat reserved**, never grant the same seat twice or take money without a seat. An unsold seat can be sold in the next second, while a double-sold seat is a customer service, legal, and brand incident.

---

## 11. Consistency decisions

- **Seat state, holds, orders, and sold tickets:** **strong consistency**, serializable per event (single-shard conditional writes, with synchronous replication for sold state).
- **Cross-service workflows (inventory ↔ payment ↔ notifications):** **saga + outbox** with explicit pending states and compensations.
- **Seat-map display and search:** **eventually consistent,** because the authoritative check happens at hold time.
- **Queue position:** best-effort fairness. Occasional minor reordering is acceptable, **but admission tokens are the strict gate.**
- **Purchase limits:** enforced **atomically** at hold time, because they are anti-fraud and fairness controls.
- **Entry scanning:** first-scan-wins conditional write, with **offline tolerance** and reconciliation.
- **Analytics and recommendations:** eventual and derived.

---

## 12. Multi-region

- **Each event has a home region** that owns its inventory (single writer), with synchronous replication to a nearby standby for sold-seat durability. This avoids split-brain double-selling, the same logic as the payment design.
- **Edge and CDN are global:** the waiting room, event pages, and seat-map snapshots are served near users, and only admitted, rate-limited requests travel to the home region for holds.
- **Cross-region latency** (~100-200 ms) on the booking path is acceptable, because the actions are infrequent and the user is already queued.
- **Failover:** promote the standby with synchronous replication (no lost sales), keep holds in `CONVERTING` resolvable by the sweeper, and accept brief unavailability over a risk of duplicates.
- **Data residency and payments:** pin PII and payment data to the allowed regions.
- **Trade-off:** home-region ownership is simple and safe, but users far from the home region see higher latency. Global strong consistency (Spanner-style) is possible at higher commit latency and cost, which usually isn't justified.

---

## 13. Observability and operations

- **Correctness invariants (alert on any violation):** sold seats ≤ capacity per event, exactly one active `SOLD` ticket per seat, every `CONFIRMED` order has a captured payment, every captured payment has a confirmed order (reconciliation).
- **Flash-sale SLIs:** admit rate versus target, queue depth and wait time, hold success rate, hold-to-order conversion, DB latency and lock waits, `409` rate by seat tier, payment success and timeout rates, sweeper lag, and edge cache hit ratio.
- **Fairness and abuse:** bot-challenge rates, tickets-per-account distribution, suspicious payment fingerprints, resale-listing velocity.
- **Runbooks and tooling:** a **kill switch** (pause admissions, flip to sold-out, stop holds), **admit-rate override**, per-event feature flags, a seat/order timeline viewer ("why does this seat show held?"), and manual release tools with audit logs.
- **Readiness:** **load-test at 2-3× expected peak** with realistic user behavior (including retries), **game days** (kill the inventory primary mid-sale, drop Redis, delay payments), capacity pre-provisioning, and **change freezes** around major on-sales.

---

## 14. Scaling summary

| Component | Strategy |
|---|---|
| Browse/search/catalog | CDN + cache + search index, near-immutable data, scales trivially |
| Seat map | In-memory snapshots from a change stream, CDN with 1-2 s TTL, delta push |
| Admission | Edge-served waiting room, Redis queue, signed admit tokens, feedback-driven admit rate |
| Inventory authority | Strongly consistent DB partitioned by `eventId`, conditional atomic updates, optional single-writer in-memory owner for hot events |
| Booking/Checkout services | Stateless, per-event bulkheads/cells, idempotency everywhere |
| Holds | Timestamped expiry, lazy evaluation at write time, sweeper for hygiene |
| Payments | Saga via the payment service, unknown-outcome handling, void/refund backstop |
| Bot defense | Verified-fan, challenges, purchase limits, fingerprinting, anomaly detection |
| Tickets/entry | Signed/rotating barcodes, offline-capable scanners, first-scan-wins |
| Regions | Event home-region ownership, synchronous standby, global edge |
| Events (async) | Outbox → Kafka → idempotent consumers (notifications, analytics, seat-map) |

---

## 15. Likely follow-up questions

- **"How do you prevent two people from booking the same seat?"** An atomic conditional update on the seat row (`AVAILABLE` or expired hold → `HELD`), as a single-shard transaction partitioned by event. Multi-seat holds are all-or-nothing, in sorted order. The final `HELD → SOLD` transition is another conditional write keyed by `holdId`.
- **"Pessimistic or optimistic locking?"** Optimistic (conditional updates), because locks would queue under hot-seat contention and tempt you to hold them across slow steps. Pessimistic row locks are acceptable for the short transaction, in a fixed order to avoid deadlocks.
- **"How do holds expire?"** A timestamp on the hold, evaluated inline at write time (lazy), plus a sweeper for cleanup and events. Never rely on timers alone, and use the database clock.
- **"What if payment completes after the hold expired?"** Prevent it by transitioning to `CONVERTING` before payment (which freezes the seats). Backstop with an automatic void/refund if the confirm can't succeed.
- **"How do you handle 5M users at once?"** A virtual waiting room that admits at a rate the core can handle, CDN-served static pages, server-controlled poll intervals, signed admit tokens, cached seat maps, and feedback-based admission.
- **"How do you stop bots and scalpers?"** Verified-fan registration, challenges at queue entry, fingerprinting, per-identity and per-payment purchase limits enforced atomically, anomaly detection, and non-transferable or rotating tickets.
- **"How would you scale the database for one hot event?"** You don't shard a single event's 7 MB of inventory. You **bound the load with admission control**, use conditional updates, isolate the event on dedicated capacity, and for extreme cases use a single-writer in-memory authority with a replicated log.
- **"Is Redis safe for holds?"** As an accelerator, yes. As the source of truth, no, because failover can lose a hold. Keep the DB conditional write as the final arbiter so loss is a UX problem, not an integrity problem.
- **"How do you design for general admission?"** A counter with an atomic conditional decrement, sharded into buckets or leased in blocks to avoid a hot row, with a fallback to an authoritative counter near sell-out.
- **"How would this differ for airline or hotel booking?"** Inventory is per date range and fare class (multi-row atomic reservation), overbooking is intentional, and prices move, so quotes are time-limited.
- **"How do you keep the seat map accurate?"** It's eventually consistent by design. Deltas are pushed from the inventory change stream, and the authoritative check happens when holding.
- **"What if the inventory database fails mid-sale?"** Synchronous replication means no sold seat is lost. Promote the replica, in-flight requests retry safely through idempotency keys, and the sweeper reconciles holds and orders.
- **"How do you handle refunds and cancellations?"** A conditional transition `SOLD → AVAILABLE` (or into a resale pool) plus ledger reversal and a refund saga, all idempotent. Policies determine whether the seat returns to general sale.
- **"How do you validate tickets at the gate with bad connectivity?"** Signed barcodes verified locally, a pre-synced ticket list, offline-first scanning with first-scan-wins, and reconciliation after reconnect.
- **"How would you test this?"** Load tests at several times peak with retry behavior, concurrency tests hammering the same seats (asserting no double-sells), fault injection around the hold/payment/confirm crash windows, and invariant checks running continuously in staging and production.

---

## 16. How to narrate it

1. **Name the dominant problem:** contention on tiny, scarce inventory during a flash crowd, with correctness (no double-sell) non-negotiable. Storage and average QPS are irrelevant.
2. **Start naive:** read-check-write on a seats table, direct to the database. Show the race, then the stampede at on-sale. Then introduce the atomic conditional update, then holds with expiry, then the payment race and the `CONVERTING` freeze, then the saga with compensation, and finally the waiting room, bot defenses, and caching to protect it all.
3. **Separate the paths:** a cacheable, eventually consistent read path, a rate-limited and strongly consistent booking path, and an async downstream path (tickets, notifications, analytics).
4. **State the contracts:** one authority per event, atomic all-or-nothing seat acquisition, DB-clock-based expiry, no money moves without a confirmable seat (and vice versa), idempotency everywhere, and fail toward "no."
5. **Call out the knobs:** hold duration, payment-window extension, admit rate and its feedback signals, queue ordering (lottery vs. FIFO), purchase limits, best-available versus free selection, seat-map TTL, and bot-challenge strictness.
6. **For each box, say what happens when it fails,** and end with "the worst case is a user who didn't get seats or has to retry, never two owners of one seat and never money taken for nothing."

**Patterns reused from our toolbox:** conditional writes and compare-and-set state machines (Uber trips, Dropbox commits, payments), idempotency keys and unique constraints (payments, notifications), saga + transactional outbox (feed, Uber, payments), unknown-outcome handling and recovery sweepers (payments, Uber dispatch), lease and TTL-based reservations with fencing (Uber offers, KV store locks), rate limiting and fail-closed protection (rate limiter), admission control and load shedding (reliability patterns, notifications), CDN and short-TTL snapshot caching with request coalescing (URL shortener, video), delta push over SSE/WebSocket with the cursor as truth (chat), sharded counters and range leasing for hot rows (feed counters, URL shortener), single-writer-per-partition with a replicated log (chat sequencing, KV store Raft), cell-based isolation and bulkheads, static stability, and graceful degradation.

Next, I can play interviewer on this (for example, "the on-sale opens, the waiting room admits 5K users/sec, but hold success rate collapses to 2% and the DB CPU is at 95%, walk me through diagnosis and mitigation"), or we can move on. **Metrics/logging system** adds time-series ingestion, downsampling, and alerting. **Google Docs-style collaborative editing** takes the OT/CRDT thread we only touched in Dropbox. **Distributed message queue (Kafka-like)** opens up another black box we've relied on throughout the series.
