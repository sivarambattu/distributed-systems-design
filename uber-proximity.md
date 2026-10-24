# Uber / Proximity Service Design

**What to say early:** This problem is dominated by **high-frequency, ephemeral location writes** and **spatial queries** ("who is near this point, right now?"), followed by a **matching problem with contention**. One driver can only take one ride, and many riders are competing for the same few drivers. Storage is tiny and the data is perishable. The hard parts are the **geospatial index**, **dispatch correctness** (no double-booking), and **real-time two-way communication**.

It helps to separate two kinds of "proximity" early:
- **Static proximity** (Yelp, "restaurants near me"): points rarely move and reads dominate. A spatial index in a database or search engine is enough.
- **Dynamic proximity** (Uber, "drivers near me"): points move every few seconds and are write-heavy. The index must live in memory and tolerate constant churn.

This design is the dynamic case, and I'll cover the static one briefly at the end.

---

## 1. Requirements

**Functional**
- Drivers go online/offline and continuously report location
- Riders see nearby available drivers and an ETA, and request a ride with pickup and destination
- The system **matches** a rider to a suitable driver, who can accept or decline
- Both sides see live location and ETA during pickup and the trip
- Trip lifecycle: requested → matched → arriving → in trip → completed/cancelled
- Fare estimate and dynamic (surge) pricing
- Payment, ratings, and history (mention briefly and scope out)

**Non-functional**
- **Match latency:** a rider gets a driver in seconds
- **Location freshness:** driver positions no more than ~5 seconds stale
- **No double-assignment:** a driver is never committed to two trips and a trip never has two drivers
- **High availability** in each city, and **never lose a trip in progress**
- Scales to hot spots (concerts, airports, New Year's Eve)
- Handles flaky mobile networks and noisy GPS

**Clarify with the interviewer:** Just ride-hailing, or also pooling, scheduled rides, food delivery? Global or a single region? Do we own routing and ETA, or call a maps provider? Is surge pricing in scope? Is payment in scope?

---

## 2. Estimation (state assumptions)

- **1M concurrent online drivers** at peak, reporting every ~4 seconds → **~250K location writes/sec**
- **20M trips/day** ≈ 230/sec average, **~1K/sec peak** match requests
- Riders browsing the map: ~1M concurrent × a refresh every 5 s → **~200K nearby-reads/sec**
- **Current-location state:** 1M drivers × ~200 bytes ≈ **200 MB.** This fits in memory on one machine.
- **Location history:** 250K/sec × ~50 bytes ≈ 12 MB/s ≈ **~1 TB/day** → Kafka → object storage (disputes, ML, ETA models, audits)
- **Trips:** 20M/day × ~2 KB ≈ 40 GB/day, a modest sharded-DB workload

**Takeaway to say aloud:** The live index is tiny, so we shard for **throughput and fault isolation, not capacity**. The expensive parts are write rate, fan-out of live updates, and routing and ETA computation, not storage. Also, **the index is ephemeral.** If it's wiped, it rebuilds from driver heartbeats within seconds. That one property drives most reliability decisions.

---

## 3. API and data model

```
Driver (persistent connection: WebSocket/gRPC stream, HTTPS fallback)
  → LOCATION   { lat, lng, heading, speed, ts, accuracy }       every ~4s (adaptive)
  → GO_ONLINE / GO_OFFLINE
  → ACCEPT / DECLINE { offerId }
  ← OFFER      { offerId, tripId, pickup, estFare, expiresAt }
  ← TRIP_UPDATE

Rider
  GET  /v1/nearby?lat=&lng=                    → drivers (anonymized), ETA
  POST /v1/fare-quote { pickup, dest, type }   → { quoteId, price, surge, expiresAt }
  POST /v1/trips { quoteId, pickup, dest, idempotencyKey } → { tripId }
  ← live stream: driver location, ETA, trip state changes
  POST /v1/trips/{id}/cancel
```

| Entity | Shape | Store |
|---|---|---|
| Driver live state | `driverId → lat, lng, heading, status (OFFLINE/AVAILABLE/OFFERED/ON_TRIP), cellId, ts, tripId?` | In-memory store (Redis or custom service), TTL from heartbeats |
| Spatial index | `cellId → set/zset of available driverIds` (score = last-update ts) | Same in-memory tier, sharded by cell |
| Trips | `tripId → riderId, driverId, state, pickup, dest, quote, timestamps, version` | Strongly consistent sharded DB (conditional writes) |
| Location history | `(tripId or driverId, ts) → lat, lng` | Kafka → object store, time-series store |
| Surge | `cellId → multiplier, computedAt` | Cache, refreshed every ~1 min |
| Users, payment methods, driver docs | Standard relational | Sharded SQL |
| Road graph and traffic | Graph with live edge weights | Routing service (in memory, versioned) |

**Key design point: separate the volatile from the durable.** Live location is high-churn and disposable (in memory, TTL). Trip state is low-churn and must never be lost (durable, strongly consistent). Mixing them is the classic mistake.

---

## 4. High-level architecture

```
                 ┌───────────── LOCATION INGEST (write-heavy, ephemeral) ─────────────┐
Driver app ⇄ (WebSocket/gRPC) ⇄ L4 LB ⇄ Edge Gateways (stateful connections)
                                              │ auth, throttle, validate, drop stale/out-of-order
                                              ▼
                                     Location Service (stateless)
                                        1. update driver:{id} (latest position)
                                        2. if cell changed → move driver between cell sets
                                        3. async: publish to Kafka "driver.location"
                                              ▼
                          Supply Index (in-memory, sharded by cell, replicated)
                                              │                          └► Kafka → object store, surge, ML
                 ┌───────────── DISPATCH (read + contention) ──────────────────────────┐
Rider app → API Gateway → Trip Service ──► Dispatch/Matching Service
                          (create trip,         1. candidates = cells (k-ring) → drivers
                           durable)             2. filter (type, rating, status)
                                                3. rank by ETA (Routing/ETA service)
                                                4. reserve driver (atomic AVAILABLE→OFFERED)
                                                5. send OFFER via gateway; wait for accept/timeout
                                                6. confirm: trip → DRIVER_ASSIGNED (conditional write)
                 ┌───────────── LIVE TRACKING ────────────────────────────────────────┐
Driver location → Tracking Service → pub/sub topic per active trip → Rider's gateway → app

Side services: Pricing/Surge · ETA/Routing · Payments · Notifications · Fraud/Safety · Analytics
```

---

## 5. The core deep dive: geospatial indexing

**The problem:** given (lat, lng), find the nearest N available drivers within ~2 km, with **250K position updates per second** modifying the data. Scanning every driver is O(N), and a SQL `WHERE` on lat/lng bounds doesn't use a 1D index efficiently across two dimensions.

### Option A: Plain lat/lng columns with a B-tree
Range filter on latitude, then filter on longitude. It's only efficient in one dimension. Slow, and index updates at 250K/sec would crush a relational DB. Dead on arrival.

### Option B: Geohash
Encode (lat, lng) as a base-32 string. A longer shared prefix means closer cells (the plane is recursively split into rectangles).
- **Pros:** simple, string-prefix lookups work in any KV store or DB, and adjustable precision by prefix length.
- **Cons:** **edge problem.** Two points meters apart can sit in cells with totally different prefixes (across a boundary like the equator or a cell edge), so you must query the cell **plus its 8 neighbors**. Cells are rectangles whose width shrinks toward the poles. Neighbor distances are non-uniform (edge vs. corner neighbors).

### Option C: Quadtree
Recursively split a region into 4 children when a node has more than T points.
- **Pros:** **adaptive density.** Dense downtown areas subdivide deeply, and empty regions stay coarse. It gives efficient nearest-neighbor and range queries.
- **Cons:** in a distributed system with **constant moves**, rebalancing (splits and merges) is complicated. Each update may modify the tree structure, which makes concurrency and sharding painful. It's excellent as an in-memory structure on one node and awkward to distribute. It's a better fit for **static points** (Yelp).

### Option D: Fixed hierarchical cells (Google S2 or Uber H3), my recommended answer
Tile the globe into cells with an ID. Map each point to a cell ID in O(1).
- **S2:** projects onto a cube and uses a Hilbert curve to order cells, and **cell IDs are 64-bit integers** that preserve locality. Good for range scans and covering arbitrary regions.
- **H3:** **hexagonal** cells. Every neighbor is the same distance from the center, which makes "radius ≈ k rings of cells" uniform and gives smoother aggregates (great for surge). Hexagons are more awkward for exact containment hierarchies (a parent doesn't perfectly contain its children).
- **Why it works for moving points:** an update is O(1): compute the new cell ID and, **only if the cell changed**, move the driver between two sets. Most updates (the driver is still inside the same ~0.5 km cell) touch only the driver's own record.

### The query (the "k-ring" expansion)
```
cell      = toCell(riderLat, riderLng, resolution=8)       // H3 res 8: ~0.5 km edge, ~0.7 km² area
cells     = kRing(cell, k=1)                               // 7 cells ≈ 1+ km radius
drivers   = union of available drivers in cells            // zset by last-update ts, drop stale (>15-30s)
if drivers < needed: k += 1 and repeat                     // expand rings up to a max radius/time
exact     = sort by real distance → take top ~20-50 candidates → ETA ranking
```

**Cell resolution is a tuning trade-off:**
- **Smaller cells:** more precise, fewer candidates per cell, but more cells to query and more cell-change writes (drivers cross boundaries often).
- **Larger cells:** fewer queries and fewer index moves, but big candidate sets and more post-filtering. Dense downtown may deserve finer resolution than suburbs. Adaptive resolution is possible but increases complexity, so start with a fixed resolution and add a second tier if the data shows it's needed.

**State your decision:** "H3 (or S2) cells as the spatial key, with an in-memory set per cell. A proximity query is a k-ring lookup plus exact distance filter, and an update is O(1), usually touching only the driver's own record."

### Distributing the index: shard by cell or by driver?
| | Shard by **cell** | Shard by **driver ID** |
|---|---|---|
| Update | Needs a cross-shard move when a driver crosses cells owned by different shards | Always one shard, even |
| Nearby query | Touches a few shards | **Scatter-gather across all shards** (every shard might have nearby drivers) |
| Hot spots | A dense downtown cell loads one shard | Even load |

**Common answer:** shard by **cell** (hash of a coarse parent cell, or of the cell ID itself), because queries are local and the read path is latency-critical. To fix hot spots, **use fine cells and hash them across many shards** (a downtown spreads over many shards because adjacent cells hash to different ones), and a k-ring query becomes a small **parallel fan-out to a handful of shards** (7 cells, a few shards). **Trade-off:** locality of parent cells simplifies queries but concentrates hot cities and events. Hashing fine cells balances load but raises the fan-out per query.

Since the whole dataset is about 200 MB, **replicate the index** (primary + replicas) to scale reads and to survive failure. Sharding is for write throughput, not size.

### Handling the write firehose
250K updates/sec is the real load. Reduce it:
- **Client-side adaptive frequency:** report every 4 s while moving, slower when stationary or offline, faster when on a trip or near pickup (when it matters).
- **Batching at the gateway:** coalesce updates per shard into one pipelined write.
- **Drop out-of-order and stale updates** by comparing timestamps (mobile networks reorder).
- **Last-write-wins is exactly right here:** only the latest position matters, so there's no need for conflict resolution or durable writes on the hot path.
- **Durable copies are async:** publish to Kafka for history, so the live path never waits on disk.

### The driver-moves-between-cells race
Moving a driver from cell A to cell B is two operations (remove from A, add to B). A crash between them leaves the driver in neither or both.
- Do it in one **Lua script/transaction** if both cells are on one shard. Otherwise **add to the new cell first, then remove from the old** (briefly in both beats briefly in neither), and make reads tolerant: dedupe by `driverId` and verify against the driver's authoritative record (`cellId`, `ts`) before using a candidate.
- **Stale entries** from crashed or disconnected drivers are pruned by **timestamp scores** (`ZRANGEBYSCORE now-30s`) or a TTL on the driver record, not by relying on the driver to say goodbye.

---

## 6. The second core problem: matching and dispatch

### Step 1: Candidate generation (geo filter)
k-ring lookup gives ~20-50 nearby available drivers matching the vehicle type, rating, and so on.

### Step 2: Rank by ETA, not straight-line distance
A driver 300 m away across a river or on the wrong side of a divided highway may take 10 minutes. Use the **ETA service** (road graph routing with live traffic).
- **Two-stage:** straight-line distance cuts to ~20 candidates, then a **batched ETA matrix call** ranks them. Calling routing for hundreds of candidates per request would be too slow and costly.
- **Add other factors:** driver's heading, whether they're finishing a trip nearby (**chained rides**), acceptance probability, fairness (don't starve drivers), and trip value.

### Step 3: Offer strategy (how to pick and ask)
- **Sequential offers (nearest first):** offer to the best driver, wait ~10-15 s, then move to the next on decline or timeout. Simple, but a long tail of timeouts delays the rider.
- **Parallel offers (broadcast to the top N):** first to accept wins. Fastest, but wastes drivers' attention, creates "lost the race" frustration, and wastes offers.
- **Batched global matching:** collect requests and available drivers in a short window (~2 s), then solve an **assignment problem** (min-cost bipartite matching, Hungarian-style or a greedy approximation) to minimize total pickup time across *all* riders.
- **Trade-off:** greedy per-request matching is instant but locally optimal (one rider can steal the driver that was ideal for another). Batching adds up to ~2 seconds of wait but improves overall efficiency, especially under heavy demand. **My pick:** batched matching in dense, busy zones and greedy where supply and demand are sparse. Name this as a tunable knob (window length).

### Step 4: Preventing double assignment (the correctness crux)
Two concurrent match requests may pick the same best driver. Fix with an **atomic reservation**:
```
// Conditional state transition on the driver record (Lua script or conditional write)
if driver.status == AVAILABLE:
    driver.status = OFFERED; driver.offerId = X; driver.offerExpiry = now + 15s
    return OK
else return CONFLICT   → matcher moves to the next candidate
```
- **On accept:** transition `OFFERED → ON_TRIP` and write the trip as `DRIVER_ASSIGNED` using a **conditional write on `trip.version`** (compare-and-set). Only one wins.
- **On decline or timeout:** release the driver back to AVAILABLE (also via expiry, so a crashed matcher can't leave a driver stuck in OFFERED forever).
- **Idempotent offers and accepts:** `offerId` makes retries safe, since a driver's accept may be retransmitted over a flaky link.
- **Fencing:** if matching for a trip is owned by one dispatcher instance at a time (partitioned by `tripId`), a **lease plus fencing token** prevents a paused old owner from acting after losing ownership.

### Step 5: Delivering the offer
Push over the driver's persistent connection (same gateway pattern as the chat design). If the connection is dead, fall back to a **push notification** (best-effort), and treat the offer as timed out quickly. Because the offer has an `expiresAt`, a late delivery can't create a stale commitment.

### Cancellations, no-shows, and re-dispatch
Driver cancels or times out → the trip returns to MATCHING with that driver excluded. Rider cancels → release the driver, and fees may apply depending on state. Every transition goes through the state machine below.

---

## 7. Trip state machine (the source of truth)

```
REQUESTED → MATCHING → OFFERED → DRIVER_ASSIGNED → DRIVER_ARRIVING → ARRIVED → IN_TRIP → COMPLETED
                │          │             │                 │            │
                └──────────┴─────────────┴──── CANCELLED / NO_DRIVERS_FOUND / FAILED ◄─────────┘
```
- **Persist every transition** in a strongly consistent store with a `version` column and **conditional writes**, so illegal or concurrent transitions are rejected (e.g., can't complete a cancelled trip).
- **Emit an event per transition** through the **transactional outbox** to Kafka (same pattern as the feed design), consumed by notifications, payments, analytics, and fraud. This avoids the dual-write problem.
- **Shard by `tripId`** (even distribution). Keep a second index by `riderId` and `driverId` for "current trip" lookups, which enforces "one active trip per driver/rider."
- **Why strong consistency here:** this is money and physical-world commitments. A rider must never see "driver assigned" for a trip that isn't, and the same driver must never be on two trips.

**Durability rule:** *acknowledge a trip request only after it's persisted.* If matching crashes, a recovery process scans for trips stuck in MATCHING/OFFERED beyond their timeouts and re-drives them. That makes the dispatcher **crash-safe and restartable** because the DB, not the dispatcher's memory, holds the truth.

---

## 8. Live tracking and real-time communication

- Both apps hold a **persistent connection** (WebSocket/gRPC stream, or MQTT). All gateway concerns from the chat design apply: L4 balancing, stateful gateways, heartbeats, jittered reconnects, and drain-on-deploy.
- **During a trip:** the driver's location goes to the Tracking Service and is published on a **pub/sub topic per `tripId`**. The rider's gateway subscribes while the rider's app is open. Don't poll.
- **Rate-shape updates:** rider UI doesn't need 4 s updates when the car is far away. Send faster near pickup and slower otherwise. Clients **interpolate** between updates for smooth animation.
- **GPS noise:** apply **map matching** (snap to the most likely road segment, e.g., with a hidden-Markov-model approach) and filter outliers (impossible speeds, jumps) before display or fare calculation.
- **Poor connectivity:** the driver app **buffers locations locally** and uploads on reconnect. This matters for **trip distance and fare accuracy**, so the trip record uses the buffered track with its original timestamps.
- **Reliability:** pub/sub and the push path are **best-effort.** On reconnect, the client fetches current trip state from the Trip Service (like the "sync on reconnect" principle in chat). State correctness never depends on a live message arriving.

---

## 9. ETA, routing, and pricing

### ETA / routing service
- **Road network graph** with live traffic edge weights, served from memory.
- **Algorithms:** raw Dijkstra is too slow at continental scale. Use **preprocessed hierarchies (Contraction Hierarchies)** or A\* with landmarks for fast point-to-point queries in milliseconds. Partition the graph by region.
- **Live traffic:** derived from driver location streams (an aggregation pipeline like Top-K: bucketed speeds per road segment), blended with historical patterns.
- **ML correction:** a model adjusts routing-based ETAs with learned corrections (time of day, weather, events).
- **Caching:** cache ETAs between coarse cell pairs for the candidate stage, and compute exact routes only for finalists and active trips.
- **Trade-off:** accuracy vs. latency and cost. Use cheap approximations for filtering and precise computation only where it matters. **Degradation path:** if the ETA service is slow or down, fall back to straight-line distance × a road factor, so dispatch continues with worse matches rather than none.

### Surge / dynamic pricing
- **Supply and demand aggregation per cell:** stream-process rider requests (demand) and available drivers (supply) over a sliding window (same bucketed-window approach as the Top-K design). Surge multiplier is a function of the demand/supply ratio.
- **Smooth it:** blend with neighboring cells and over time to avoid price whiplash at cell borders and flicker.
- **Quote locking:** the rider gets a **quote with an expiry** (`quoteId`), and the price is honored if they book before it expires. That keeps the transaction deterministic even though the multiplier moves.
- **Trade-offs:** surge rebalances supply by drawing drivers to busy areas, but it can feel unfair or exploitative, and there are regulatory limits and caps in some regions. Cache multipliers (refresh ~1 min) so pricing doesn't sit on the critical path. If the pricing service fails, fall back to the last known multiplier or a base fare.

### Payments (brief)
Authorize at request, capture at completion. Use **idempotency keys** on every payment call, a **saga/outbox** for the "trip completed → charge → payout" chain, and an append-only **ledger** with reconciliation. Payments are async and **never block the trip**. A failed charge becomes a retry queue and a rider debt, not a stuck car.

---

## 10. Hop-by-hop reliability and trade-offs

### Gateways and connection layer
- Stateful, with many connections per node. Same reconnect-storm and drain concerns as chat. After a regional blip, **millions of drivers reconnect at once**, so use jittered backoff and connection-rate caps.
- **Trade-off:** high density per node saves cost but widens the blast radius. Spread each city's drivers across many gateways.

### Location Service and Supply Index
- **Stateless service over an in-memory index.** Writes are idempotent, latest-wins.
- **If a shard dies:** a replica takes over (or a new node starts empty), and **the index refills from the next heartbeat (~4 s).** Meanwhile that area sees fewer visible drivers. This degrades match quality briefly but loses no durable data.
- **Don't spend durability effort on this tier.** Persisting every position synchronously would add cost and latency to protect data that expires in seconds. This is a good place to state "durability is a cost, so I only buy it where loss hurts."
- **Replica lag:** nearby reads from a replica may be a second stale, which is acceptable. The **reservation step always checks the authoritative driver record**, so a stale candidate is simply rejected at that point.

### Dispatch service
- **Partition by `tripId` (or by city/region)** so one owner coordinates each trip's offers. Lease-based ownership with fencing.
- **Timeouts at every call** (index, ETA, driver offer), and a **per-trip deadline budget** ("find a driver within 60 s, else NO_DRIVERS_FOUND with a clear message"). Don't leave riders waiting forever.
- **Admission control** under overload (a stadium letting out): queue requests briefly, batch them, and fail fast with an honest message rather than letting latency explode.

### Trip DB
- Strongly consistent, replicated across AZs (quorum or consensus-backed). It's the critical durable component, so favor correctness over raw speed. Volume is low enough (1K/sec peak) to afford it.
- Active-trip queries must be fast, so keep **active trips** in a small hot table or cache and archive completed ones.

### Hot-spot management
- **Events and airports:** demand spikes in a few cells. Mitigations are batched matching, surge to rebalance, **virtual queues at airports** (FIFO order for fairness and to reduce cruising), pickup zones, and pre-positioning supply using demand prediction.
- **Cell hot-spots in the index:** fine cells hashed across shards. Cache the "nearby cars" read per cell for 1-3 seconds, since thousands of riders in one cell see essentially the same answer.

---

## 11. Failure scenarios

| Failure | Impact | Mitigation |
|---|---|---|
| Location index shard lost | Fewer drivers visible in an area | Replica takeover, **refill from heartbeats in seconds**; index is ephemeral by design |
| Gateway dies | Connections drop | Clients reconnect with jitter and fetch current state; no durable loss |
| Mass reconnect (regional blip, deploy) | Herd on gateways, auth, index | Jittered backoff, connection-rate caps, gradual drain, shed non-critical work |
| Dispatcher crashes mid-match | Trip stuck in MATCHING/OFFERED | Trip state in DB, recovery sweeper re-drives timed-out trips, offer expiry frees driver |
| Two requests grab the same driver | Double-booking | Atomic AVAILABLE→OFFERED reservation, conditional write on trip version |
| Driver accepts offer but link drops | Rider unsure, driver unsure | Idempotent accept with offerId, client re-syncs trip state on reconnect |
| ETA/routing service down | Poorer ranking | Fallback to straight-line distance × factor, cached ETAs |
| Surge/pricing service down | No dynamic price | Last known multiplier or base fare, quote locking limits exposure |
| Trip DB node/AZ failure | Writes blocked briefly | Quorum replication, automatic failover, retries with idempotency |
| Noisy/spoofed GPS | Wrong match or fare | Map matching, speed/jump sanity checks, fraud signals (mock-location detection) |
| Driver phone loses connectivity mid-trip | Rider sees frozen car | Client-side buffering, show last known with "reconnecting," upload track later |
| Event-driven demand spike | Match latency and failure rates climb | Admission control, batched matching, surge, queueing with honest wait estimates |
| Region outage | City service down | Fail over to a standby region: rebuild index from heartbeats, trip DB from replicated copies (async replication lag is the risk) |
| Payment provider down | Can't capture | Complete trip anyway, queue the charge with idempotent retry, never strand a rider |
| Bad deploy in matching | Poor matches or no matches | Canary by city, shadow-compare new vs. old matcher offline, fast rollback |

**Principle (static stability):** the **in-trip experience must not depend on the matching, pricing, or analytics systems.** A trip already underway needs only the trip record, the tracking channel, and the maps. And the volatile, perishable location index is deliberately reconstructible, so its failure costs seconds of match quality, not data.

---

## 12. Consistency decisions

- **Trip state and driver commitment:** **strong consistency** with conditional writes. This is the unforgivable-failure zone.
- **Driver live location:** eventual and latest-wins. Seconds of staleness are fine.
- **Nearby-driver display:** approximate, from replicas or cache.
- **Offers:** time-bounded with expiry, so a stale offer can't bind anyone.
- **Surge and ETAs:** eventually consistent, with quoted prices locked via `quoteId` + expiry.
- **Location history, analytics:** at-least-once via Kafka, idempotent consumers.
- **Payments:** idempotent, durable, reconciled, strongly consistent ledger.

---

## 13. Multi-region

- **Ride-hailing is naturally local:** drivers, riders, and trips for a city live in one region. **Partition by city/region** (a "cell" of the system), so each region is a **self-contained stack** (gateways, index, dispatch, trip DB). That limits the blast radius and avoids cross-region traffic on the critical path.
- **Global data** (accounts, payment methods, driver documents) replicates across regions asynchronously, so a user landing in another city works immediately.
- **Region failure:** clients reconnect to a standby region. The **index repopulates from heartbeats** within seconds, and in-flight trips resume from the replicated trip DB. **The risk is replication lag:** a trip updated in the last second may be missing, so the clients and drivers' apps hold local trip state to reconcile ("you're still on trip X"), and the system errs toward preserving the trip.
- **Trade-off:** per-city isolation is simpler and resilient, but cross-border and airport-spanning areas need careful boundary rules (which region owns a trip that crosses?). **Rule:** the trip is owned by the region where it was created.

---

## 14. Variant: static proximity (Yelp / "places near me")

The contrast is worth mentioning because interviewers swap the question:
- **Data:** millions of places, **updated rarely**, read constantly. Read:write is huge.
- **Index:** a **quadtree** (adaptive to density) or geohash/S2 cell IDs in a DB or search engine (PostGIS, Elasticsearch geo queries, DynamoDB with a cell-ID key). Reads go through cache and CDN.
- **Query:** cell covering of the search radius → candidates → exact distance filter → rank by relevance, rating, and distance.
- **Scaling:** shard by cell (or region), replicate for reads, cache popular areas. Updates (a new business) are batch or async and trigger index rebuilds of just the affected cells.
- **Trade-off:** here a **precomputed, rebuildable-on-demand quadtree wins**, because points don't move, whereas in Uber the constant movement favors fixed cells with O(1) updates.

---

## 15. Scaling summary

| Component | Strategy |
|---|---|
| Gateways | Many stateful nodes, L4 LB, jittered reconnects, gradual drain |
| Location ingest | Adaptive client frequency, batch per shard, latest-wins, async Kafka for history |
| Spatial index | H3/S2 cells, in-memory sets, fine cells hashed over shards, replicated, TTL/ts pruning |
| Nearby reads | k-ring lookup, 1-3 s per-cell cache, replicas |
| Dispatch | Partitioned by trip/city, candidate filter → ETA rank → atomic reservation, batched matching in dense areas |
| Trip DB | Strongly consistent, sharded by tripId, conditional writes, outbox events |
| Tracking | Pub/sub per trip, rate-shaped updates, client interpolation and buffering |
| ETA/routing | In-memory road graph partitioned by region, contraction hierarchies, ML correction, caching |
| Surge | Stream aggregation per cell, smoothing, cached multipliers, locked quotes |
| History/ML | Kafka → object store, batch and stream pipelines |
| Regions | One self-contained stack per city/region, async replication of global data |

---

## 16. Likely follow-up questions

- **"How do you find nearby drivers efficiently?"** Index by H3/S2 cell, query the rider's cell plus k-ring neighbors, then exact-filter by distance and rank by ETA.
- **"Geohash vs. quadtree vs. S2/H3?"** Geohash is simple but has edge and non-uniform-neighbor problems. Quadtree adapts to density but is awkward for constantly moving, distributed points. S2/H3 give fixed-cell O(1) updates with uniform neighbor handling. I'd pick H3 or S2 for moving points and a quadtree for static ones.
- **"250K updates/sec, how do you cope?"** In-memory index, latest-wins, update the cell index only on cell change, adaptive client frequency, batching, and async durable copies.
- **"How do you prevent assigning one driver to two riders?"** Atomic reservation AVAILABLE→OFFERED with expiry, and a conditional write on the trip version. Idempotent offers and accepts.
- **"What if the location store dies?"** It's ephemeral. A replica or fresh node refills from heartbeats in seconds. Durable truth is the trip DB.
- **"Nearest driver or best driver?"** Rank by road-based ETA plus other factors, not straight-line distance. Consider batched global matching to optimize across riders.
- **"How do you handle surge?"** Per-cell supply/demand over sliding windows, smoothed over neighbors and time, cached, with price locked at quote.
- **"How does the rider see the car moving?"** Persistent connection, pub/sub per trip, rate-shaped updates, client interpolation, map-matched positions.
- **"What if a driver goes offline mid-trip?"** Buffer on the device, show "reconnecting," and use the uploaded track for the fare. Trip state is in the DB, so nothing is lost.
- **"How do you scale to 10× or a new city?"** Add regions (each self-contained), fine cells hashed across shards, more gateways and replicas. Per-city isolation means growth is additive.
- **"How would you add pooling (shared rides)?"** The matching problem becomes route-insertion optimization (can this rider be inserted into an existing driver's route with bounded detour?), so you need an ETA matrix and constraint solving, and batching is nearly essential.
- **"How do you handle an airport or stadium?"** Virtual queues, designated pickup zones, batched matching, surge, demand-prediction pre-positioning, and admission control.
- **"How would you detect GPS spoofing or fraud?"** Impossible-speed and teleport checks, mock-location flags, cross-checks against cell tower and Wi-Fi signals, and behavioral anomaly models, handled asynchronously.
- **"Privacy?"** Don't expose exact driver positions or IDs to riders before assignment (show anonymized vehicles), limit retention of location history, restrict internal access, and encrypt in transit and at rest.

---

## 17. How to narrate it

1. **Name the dominant problems:** high-rate ephemeral location writes, spatial proximity queries, and contention-safe matching. Storage isn't the issue.
2. **Start naive:** drivers in a SQL table with lat/lng columns and a `WHERE` clause. Show the write load and 2D query problem, then move to in-memory cells (geohash, then S2/H3 with reasons), then sharding, then replicas.
3. **Separate volatile from durable:** the location index is disposable and rebuildable from heartbeats. Trip state is durable, strongly consistent, and drives everything else.
4. **State the contracts:** persist-before-ack for trips, atomic driver reservation with expiry, latest-wins for locations, and best-effort for tracking and push, with reconnect fetching true state.
5. **Call out the knobs:** cell resolution, k-ring radius, location update frequency, offer timeout, batching window, surge smoothing, candidate count for ETA ranking.
6. **For each box, say what happens when it fails,** and end with "the location index can vanish and rebuild in seconds, and an in-progress trip never depends on anything but the trip record."

**Patterns reused from our toolbox:** geospatial indexing (the new one), sharding with consistent hashing, hot-key and hot-cell mitigation, stateful WebSocket gateways with jittered reconnect and sync-on-reconnect (from chat), atomic conditional writes and idempotency keys (from rate limiter and URL shortener), transactional outbox and sagas, lease-based ownership with fencing, stream aggregation over sliding windows (from Top-K, reused for surge and traffic), TTL-based ephemeral state, load shedding and admission control, graceful degradation, and static stability.

Next, I can play interviewer on this (for example, "a stadium event ends and 20,000 riders request rides within three minutes in the same four cells, walk me through it"), or we can move on. **Notification system** reuses queues, retries, and per-channel rate limits in a different shape, **Dropbox/Drive** brings chunking, sync, and conflict resolution, and **payment system** or **ticket booking** goes deep on the strong-consistency and idempotency side that Uber only touched.
