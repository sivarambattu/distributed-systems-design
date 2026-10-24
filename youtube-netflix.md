# YouTube / Netflix Design

**What to say early:** This is two very different systems joined by object storage and a small metadata tier:

1. An **asynchronous, compute-heavy ingest pipeline** (upload, validate, transcode, package). It's a batch workflow problem.
2. A **bandwidth-dominated playback system** where the **CDN is the product**. It's a delivery problem.

The numbers shape everything. A video is encoded **once** but watched thousands to millions of times, so the design follows three principles:
- **Spend compute at encode time to save bandwidth at watch time.**
- **Make everything served immutable and cacheable**, so the CDN needs no invalidation.
- **Keep the origin and app servers out of the video-byte path.** Video bytes flow from the CDN to the client, and the control plane only decides *which* bytes.

**Netflix vs. YouTube** is a common interviewer pivot, so name the contrast early:

| | **Netflix** | **YouTube** |
|---|---|---|
| Catalog | Thousands of licensed titles, curated | Hundreds of millions to billions of user-uploaded videos |
| Ingest | Few studio masters, deliberate, high value | Hundreds of hours uploaded per minute, mostly rarely watched |
| Encoding | Heavy offline optimization (per-title/per-shot, many codecs). Latency doesn't matter. | Fast first, optimize later based on views. Upload-to-play latency matters. |
| Distribution | **Proactive push**: pre-fill appliances inside ISPs off-peak | **Pull-through caching** at edge, with a long tail that falls to origin |
| Demand | Predictable (release schedules) | Unpredictable virality plus a huge long tail |
| Extras | DRM, territory licensing, subscriptions | Content ID, ads, comments, live, Shorts |

I'll design the general system and flag where the two diverge.

---

## 1. Requirements

**Functional**
- Upload videos (up to many GB), resumable
- Process into multiple resolutions, bitrates, and codecs for many devices
- **Stream with adaptive bitrate** to any device, with seek, resume, and subtitles/captions
- Search, browse, and recommendations (I'll scope these as downstream consumers)
- Likes, comments, view counts, watch history, subscriptions
- Content protection (DRM, signed URLs) and moderation or copyright checks
- Optionally: live streaming and offline downloads

**Non-functional**
- **Playback start < ~2 s** and **rebuffering < ~0.5%** of watch time. These are the QoE metrics users feel.
- **Playback availability is the top priority.** An outage in comments or recommendations is tolerable, but "video won't play" is not.
- **Durability of uploads:** never lose an acknowledged upload (the source file is the master).
- Upload-to-watchable in minutes for typical videos
- Global, with graceful degradation on bad networks
- Cost-efficient. At this scale **egress bandwidth is the dominant cost.**

**Clarify with the interviewer:**
- User-generated (YouTube) or licensed catalog (Netflix)? Live too?
- DRM required? Offline downloads? Ads?
- Scope: playback pipeline only, or also recommendations, search, and comments?
- Global, or specific regions? Can we place caches inside ISPs?

---

## 2. Estimation (state assumptions)

**Watching**
- 300M DAU × ~1 hour/day = 300M hours/day → **~12.5M concurrent viewers average, ~30M peak**
- Average stream ~3 Mbps → **~40 Tbps average, ~100 Tbps peak** of egress
- Play starts: ~20 per user/day × 300M = 6B/day ≈ **70K/sec avg, ~200K peak** (that's the *control-plane* load)
- Segment requests: 12.5M viewers fetching a ~4 s segment every 4 s → **~3M requests/sec average, 8M+ peak** at the CDN

**Uploading**
- ~500 hours of video uploaded per minute (an assumption in the ballpark of what YouTube has published) → **~720K hours/day**
- Source at ~8 Mbps ≈ 3.6 GB/hour → **~2.6 PB/day of source video**
- With all renditions and codecs, stored output is roughly **5-8 PB/day**, so **exabytes per year**
- Transcoding at, say, ~10 core-hours per video-hour → ~7M core-hours/day ≈ **~300K cores** continuously. This is why YouTube built custom transcoding ASICs.

**Metadata**
- ~1B videos × ~2 KB = **~2 TB**. Tiny. Metadata is a correctness problem, not a capacity problem.

**CDN math**
- Edge servers push perhaps ~100 Gbps each. 100 Tbps peak means **thousands of servers across hundreds of sites.**
- If edge and mid-tier caches hit 99%, **origin sees ~1% ≈ 1 Tbps.** The cache hit ratio is the most important number in the system, and every point of hit ratio is worth real money.

**Takeaway to say aloud:** Metadata is small, transcoding is heavy but parallel and asynchronous, and **delivery bandwidth dominates cost and complexity.** Views follow a **Zipf distribution** (a small fraction of videos gets most views, with an enormous rarely-watched tail), so caching strategy and per-video encoding effort should depend on popularity.

---

## 3. API and data model

**Upload (resumable)**
```
POST /v1/videos                    { title, desc, visibility, size, contentType, idemKey } → { videoId, uploadUrl }
PUT  {uploadUrl}  (Content-Range chunks; resumable, idempotent)          → direct to object storage
POST /v1/videos/{id}/complete                                            → 202 Accepted (processing begins)
GET  /v1/videos/{id}                                                     → { status: PROCESSING|READY|FAILED, ... }
```

**Playback**
```
GET  /v1/videos/{id}/playback  → { manifestUrl (HLS/DASH), cdnHosts[], drmLicenseUrl, token, captions[], resumePos }
GET  {cdn}/{id}/master.m3u8 (or .mpd)       → lists renditions
GET  {cdn}/{id}/{rendition}/seg_0042.m4s    → the actual bytes (this is 99.9% of traffic)
POST /v1/events/playback       (heartbeats, QoE: startup time, rebuffers, bitrate, errors)
POST /v1/progress  { videoId, positionSec }  (debounced, every ~30-60 s)
```

| Entity | Shape | Store |
|---|---|---|
| **Video metadata** | `videoId → ownerId, title, status, visibility, duration, regionsAllowed, createdAt, version` | Sharded SQL or Bigtable-style by `videoId`, heavily cached |
| **Asset map** | `videoId → renditions[{codec, resolution, bitrate, storagePath, segmentIndex}]` | Same DB |
| **Source (mezzanine)** | Original upload, kept forever (the master) | Object storage, erasure coded, never deleted |
| **Renditions/segments** | Immutable media files under versioned paths | Object storage origin, tiered, fronted by CDN |
| **Watch progress** | `(userId, videoId) → position, ts` | KV by `userId`, last-write-wins |
| **View counters** | `videoId → count` | Sharded counters plus stream aggregation |
| **Comments, likes** | `(videoId, ts)` etc. | Wide-column/sharded, separate from the playback path |
| **Search index, recs** | Derived | Elasticsearch-style index, feature store, precomputed lists |
| **Keys / DRM** | `videoId → content keys (wrapped)` | KMS-backed secret store |

**Key design points:**
- **Media files are immutable and versioned by path** (`/{videoId}/v3/1080p/seg_0042.m4s`). A re-encode writes a *new path*, and only the manifest pointer changes. The CDN never needs invalidation for segments.
- **Manifests are the only mutable, short-TTL objects** (and in live they change constantly).
- **The metadata DB is not on the byte path.** Playback needs it once per play start (cached), not once per segment.

---

## 4. High-level architecture

```
                     INGEST / PROCESSING (async, compute-heavy)
Client ─(resumable, direct)─► Object Store: raw uploads (source of truth)
   │ POST /complete
   ▼
Upload Service ─► Kafka "video.uploaded" ─► Workflow Orchestrator (DAG, retries, priorities)
        ┌────────────────┬──────────────────────┬─────────────────────┬───────────────┐
        ▼                ▼                      ▼                     ▼               ▼
   Validate/probe   Split into GOP-aligned   Content checks:      Thumbnails,     Captions
   (format, virus)  chunks ► PARALLEL        copyright fingerprint, sprites,       (ASR),
                    TRANSCODE (chunk×        moderation           previews         subtitles
                    rendition×codec)
                         ▼
                    Stitch + package (HLS/DASH, CMAF) + encrypt (DRM) ─► Origin Object Store
                         ▼
                    Metadata: status = READY (progressively) ─► events: search, recs, notifications

                     PLAYBACK (latency-critical, read-dominated)
Client ─► Playback API (auth, entitlement, geo/license checks, CDN steering, DRM token)
   │            ▲ (reads metadata cache)
   │
   ├─► DRM License Server (key delivery) ─────────────────────────────────────┐
   │                                                                          │
   └─► CDN: Edge (ISP/PoP) ─miss─► Regional mid-tier ─miss─► Origin shield ─miss─► Origin storage
          ▲ segments served here ≥ 95-99%                                    │
          └─────────────── control plane steers, pre-fills, monitors ◄── QoE telemetry from clients
```

---

## 5. Core deep dive 1: the transcoding pipeline

### Why transcode at all?
Uploaded files come in arbitrary codecs, resolutions, and bitrates. Devices differ (phones, TVs, browsers), and networks vary from 500 kbps to 1 Gbps. So we produce a **ladder** of renditions (illustrative: 240p, 360p, 480p, 720p, 1080p, 1440p, 4K, each at a tuned bitrate) in several **codecs**, and the client picks per segment.

### The naive approach and why it fails
One worker transcodes one file start to finish. A 2-hour 4K movie takes **many hours**, a worker crash restarts everything, one slow file blocks a worker, and you can't parallelize.

### The recommended design: a DAG of parallel, idempotent tasks
1. **Probe and validate:** container, codecs, duration, and corruption checks, plus a malware and format-safety check (treat uploads as untrusted input, and sandbox the decoders).
2. **Split into chunks** aligned to **keyframes (GOP boundaries)**, ideally at scene cuts, ~10 s to a few minutes each.
3. **Transcode chunks in parallel,** one task per `(chunk, rendition, codec)`. Hundreds of workers can work on one movie, which cuts latency from hours to minutes.
4. **Stitch and package** the encoded chunks into streaming segments plus manifests.
5. **Encrypt** (if DRM), write to origin, and flip metadata to READY.
6. **Side tasks in parallel:** thumbnails and scrub-preview sprites, captions via speech recognition, fingerprinting for copyright, moderation classifiers, and loudness normalization.

**Why this shape works:**
- **Tasks are idempotent and outputs are content/path-addressed,** so retries are harmless (the same pattern as the Dropbox chunks).
- **Spot/preemptible instances** can be used for cheap compute, because a killed worker only loses one small chunk.
- **A workflow engine** (Temporal, Airflow-style, or custom) provides retries with backoff, timeouts, dependency tracking, and visibility. Failed tasks go to a **DLQ** after N attempts, and a poison video must never block the queue.

**Trade-offs and details:**
- **Chunk size:** small chunks give more parallelism and a faster finish, but more overhead (startup cost per task, stitching, and quality variance at boundaries). Large chunks are the reverse. Boundaries must align with keyframes, or you get visible glitches.
- **Rate-control consistency:** independent chunk encodes can drift in quality or bitrate at boundaries. Mitigate with a **first-pass analysis** over the whole video that sets per-chunk targets (a small serial step in an otherwise parallel flow).
- **Stateless workers plus a durable queue** means worker death is boring.

### Progressive availability (a user-facing win)
Don't wait for the full ladder. **Publish as soon as a usable rendition set exists** (e.g., 360p/480p within minutes), then add higher resolutions and better codecs as they complete. Players pick whatever the manifest currently lists. **Trade-off:** more complex status handling (PROCESSING → partially READY → fully READY) in exchange for much faster time-to-watch, which is crucial for UGC.

### Compute-vs-bandwidth economics (the insight worth saying aloud)
Better codecs (VP9, then AV1) can cut bitrate by tens of percent at equal quality, but cost **several times more CPU to encode** and need device support. Since bandwidth savings **multiply by number of views**:
- **Everything gets a cheap, universally playable encode first** (H.264, a modest ladder).
- **Re-encode with expensive codecs only when a video proves popular** (views cross a threshold), because only then does the saving exceed the compute cost.
- **Netflix** goes further with **per-title and per-shot encoding:** the bitrate ladder is optimized for each title's visual complexity (an animated show needs far fewer bits than a grainy action movie for the same perceived quality). Worth it because each title is watched by millions.
- **Trade-off:** more encoding compute, complexity, and storage for more renditions, against a lower egress bill and better quality on bad networks. For a never-watched video, the extra effort is pure waste, so effort scales with expected popularity.

### Packaging: pre-package vs. just-in-time (JIT)
- **Pre-package** into HLS and DASH segment files: simple origin, trivially cacheable, but duplicates storage per format.
- **Store one fragmented-MP4 (CMAF) copy** that serves **both** HLS and DASH, with the manifest generated per protocol. This largely removes the duplication, and is the common answer.
- **JIT packaging at the origin** (store mezzanine renditions, package on request): minimal storage, flexible for new formats and DRM schemes, but adds origin CPU and latency on cache misses. The CDN absorbs most of that cost.

### Uploading (the front of the pipeline)
- **Resumable chunked upload** direct to object storage (pre-signed URL), so app servers never carry video bytes. Same pattern as Dropbox: the client can resume after a flaky connection.
- **Durability before ack:** `complete` returns only after the source object is durably stored (replicated or erasure coded). The source is the **master** and is never deleted, since all renditions can be regenerated from it.
- **Priorities and fairness:** a priority queue favors short videos and premieres with deadlines, and keeps one user's 500 uploads from starving everyone else (per-user quotas on concurrent processing).
- **Abuse and cost:** rate-limit uploads, cap size and duration for new accounts, and reject unsupported formats early.

### Moderation and copyright (the publish-time trade-off)
- **Fingerprint matching** (audio/video fingerprints against a reference database, as in YouTube's Content ID) and ML moderation classifiers run in the pipeline.
- **Gate or not?** Blocking publish until all checks complete is safest but slow. A common compromise: **fast automated checks gate public visibility, and deeper review and fingerprinting continue asynchronously** with the ability to **take down after the fact.**
- **Takedown mechanics:** because segments are immutable and cached everywhere, you don't purge segments. You **revoke access at the manifest and token layer** (metadata flips to blocked, the playback API stops issuing tokens, the CDN manifest is purged, and tokens expire in minutes). That gives prompt takedown without cache invalidation storms.

---

## 6. Core deep dive 2: adaptive bitrate streaming

### Why not just serve one MP4?
A single file can't adapt to network changes: it either stalls or wastes bandwidth. A custom streaming protocol (RTSP, proprietary UDP) isn't cacheable by standard CDNs and often hits firewalls. The industry answer is **HTTP-based adaptive streaming (HLS and MPEG-DASH):**
- Video is cut into **short segments (~2-6 s)** per rendition.
- A **manifest** lists the renditions and the segment URLs.
- The **client decides** which rendition to fetch for each next segment, based on measured throughput and buffer level.
- Segments are **plain HTTP objects**, so they're **cacheable by any CDN, stateless, and firewall-friendly.** The server holds no per-viewer state, which is why this scales.

### Client-side ABR logic (the "brain" lives in the player)
- **Throughput-based:** pick the highest rendition below the recent download rate (with a safety margin). It reacts fast but is noisy and unstable.
- **Buffer-based:** use the buffer level: a low buffer picks a low bitrate, a full buffer picks a higher one. It's stable and avoids stalls, but slower to ramp up.
- **Hybrid (MPC-style):** combine both with a short lookahead optimizing a QoE score (maximize quality, minimize rebuffering and switching).
- **Startup:** begin at a low or moderate rendition to **start fast**, then ramp up as the buffer grows. Trade-off: slightly lower initial quality for much lower time-to-first-frame.
- **Rebuffer vs. quality:** viewers hate stalls far more than a lower resolution, so ABR is biased toward **avoiding rebuffering**.

### Segment duration: a classic trade-off
| Shorter (~2 s) | Longer (~6-10 s) |
|---|---|
| Faster adaptation and quicker startup | Better compression (fewer forced keyframes) |
| Lower latency for live | Fewer requests and less manifest overhead |
| More requests, bigger manifests, more overhead | Slower to react to bandwidth drops |

~4-6 s is a typical VOD compromise, and **chunked CMAF** gets live latency down without tiny full segments.

### Seeking and resuming
Seek = look up the segment index for the target time and fetch that segment (and the init segment). **Byte-range requests** into packed rendition files are an alternative to millions of tiny objects (fewer objects to store, but the CDN must cache ranges well). **Trade-off:** many small segment objects are the most CDN-friendly but create a huge object count. Packed files plus range requests reduce object count at some CDN complexity.

### Startup latency budget (state it as a budget)
```
steering/DNS ~50ms → playback API ~100ms → manifest ~50ms (CDN)
DRM license (in parallel with manifest) ~100-300ms → first segment ~200-500ms → decode/render
Target: < ~2 s total
```
**Optimizations:** parallelize license and manifest fetch, preconnect and reuse connections (HTTP/2, HTTP/3 over QUIC with 0-RTT), smaller first segments, a low initial bitrate, prefetch on hover or likely-next-episode, and cache the playback decision briefly.

---

## 7. Core deep dive 3: the CDN (this is the product)

### Why the CDN design dominates
At ~100 Tbps peak, **no origin can serve this.** The CDN serves segments from servers close to viewers, and the whole design's job is to maximize the hit ratio near the edge and keep traffic off expensive backbone links.

### Tiered cache hierarchy
```
Viewer → Edge (ISP/PoP) → Regional mid-tier → Origin shield → Origin storage
         ~95-99% cumulative hit ratio at the first two layers for popular content
```
- **Edge:** closest to the viewer (inside ISP networks or at IXPs/PoPs), holding the hot head of the catalog.
- **Mid-tier:** aggregates misses from many edges, so a segment fetched once per region serves many edge misses.
- **Origin shield:** a single choke point in front of origin storage so that **N regions asking for the same segment cause one origin read** (request coalescing).
- **Why tiers:** each layer multiplies the effective hit ratio and shrinks origin load, at the price of extra latency on deep misses.

### Why caching works so well here
- **Segments are immutable,** with versioned paths, so there's no invalidation problem and cache coherence is trivial.
- **Zipf popularity:** the head is tiny relative to total bytes yet drives most traffic, so a modest edge cache covers most requests. The tail is rarely requested and is served by deeper tiers.
- **Cache key = path** (strip signature and token query params from the key, or tokens fragment the cache).

### Cache policy details
- **Eviction + admission:** plain LRU lets **one-hit wonders** (a long-tail video watched once) evict genuinely hot content. Use **admission control** (e.g., TinyLFU, or a Bloom filter that admits an object only on its second request) plus LFU-ish eviction. Trade-off: a slightly colder cache on first requests for a new viral video, in exchange for a much better hit ratio overall.
- **Request coalescing:** many viewers requesting the same uncached segment at once (a new release or live event) must produce **one** upstream fetch at each tier. Without it you get a thundering herd on the origin.
- **Consistent hashing within a site:** map each segment to a specific server in a PoP, so the PoP's total cache is the **sum** of its servers' disks rather than N duplicate copies of the same head. **Trade-off:** a viral object makes one server a hotspot, so replicate very hot objects across several servers.
- **Storage tiers inside the edge:** RAM for the hottest, SSD for warm, HDD for the long tail.

### Push vs. pull distribution
- **Pull-through (YouTube-like):** the edge fetches on a miss and caches. It handles the unpredictable long tail naturally, but the first viewer pays miss latency, and a viral surge can stress the tiers behind the edge.
- **Proactive push/pre-positioning (Netflix-like):** during off-peak hours, **fill appliances in ISP networks with content predicted to be popular** (new releases and regional favorites from viewing forecasts). Peak-hour traffic then rarely touches the backbone or origin. It works because the catalog is small (thousands of titles) and demand is predictable. The cost is storage and some wasted fills.
- **Hybrid (my answer for a general design):** push the predictable head (new releases, trending, recently uploaded by big creators) and pull-through for everything else.

### Placing caches: where the money is
- **Inside ISP networks** (Netflix Open Connect appliances, Google Global Cache): the best latency and QoE, and bandwidth is served **without crossing the ISP's transit links.** The ISP saves money too, which is why they host the boxes for free.
- **At IXPs and your own PoPs** for the rest, and **third-party CDNs** for capacity and regions where you have no footprint.
- **Build vs. buy:** third-party CDN gets you started quickly and flexibly, but at exabyte-per-month egress your own edge is far cheaper per GB, and gives control over caching policy and steering. A reasonable path is to start on a commercial CDN and build your own as volume justifies.
- **Multi-CDN:** used for resilience and for **cost arbitrage.** The control plane splits traffic by real-time performance, cost, and capacity.

### Steering: which edge serves this viewer?
The playback API returns an **ordered list of CDN hosts** (or the manifest uses multiple base URLs). The decision uses client location (IP to ISP/region), edge health and load, cache content (is this title on that appliance?), and measured QoE.
- **Mechanisms:** DNS-based steering, anycast, or **API-level steering** (best, because you decide per play with full context).
- **Client-side failover:** if segment requests fail or slow down, the player **switches to the next host mid-stream**, with no user-visible interruption if the buffer holds.
- **Trade-off:** API-level steering is the most precise but puts a dependency on the playback API (mitigate with TTL-cached decisions and DNS fallback).

### Signed URLs and access control
- Playback tokens (signed, short-lived, scoped to the video and optionally to region and device) are validated **at the edge**, so unauthorized hotlinking and sharing of URLs fail.
- **Trade-off:** tokens that expire quickly are safer but force token refresh mid-playback, and tokens in the URL can break cache keys unless you normalize them out. Use signed cookies or ignore-token cache keys.

---

## 8. Playback path, hop by hop

### 1. Client and steering
- Player requests `/playback` with auth. If the playback API is slow or down, a cached recent decision (manifest URL, hosts, token) lets many plays still start.
- **Reliability:** retries with exponential backoff and jitter, and multiple API endpoints.

### 2. Playback API (small, critical, stateless)
- Verifies **entitlement** (subscription, purchase, age rating), **geo/territory rights** (a licensing issue for Netflix), and **visibility** (private, unlisted, blocked).
- Fetches metadata from a **cache** (not the DB per request), picks CDN hosts, mints a signed token, and returns the DRM license URL and resume position.
- **Keep the dependency set minimal.** The playback API should depend only on the metadata cache, the entitlement check, and the token signer. Recommendations, ads decisions, counters, and comments must **not** be on this path.
- **Graceful degradation:** if entitlement is slow, a recent cached positive answer can be served briefly. If a non-essential call (ads, resume position) fails, **play anyway.**
- **Scaling:** ~200K peak plays/sec is modest for stateless services behind multi-AZ load balancers with N+1 capacity.

### 3. DRM license server (Netflix and premium content)
- Content is **encrypted at packaging** with **Common Encryption (CENC/CBCS)**, so one set of encrypted CMAF segments works with Widevine, FairPlay, and PlayReady. Only the license exchange differs per DRM.
- The player requests a license after the license server checks entitlement and device security level (e.g., HD or 4K only on hardware-protected devices).
- **This is on the critical path of playback start,** so it must be highly available, fast, and multi-region. Cache or persist licenses where policy allows (offline playback uses persistent licenses with expiry).
- **Trade-off:** stronger protection (hardware-backed DRM, forensic watermarking) restricts devices and adds complexity and latency. UGC platforms often skip DRM and rely on signed URLs.
- **Key storage:** wrapped keys in KMS/HSM. A key leak is catastrophic for licensed content, so rotate and scope keys per title.

### 4. Manifest delivery
- The master manifest and rendition manifests are tiny and cached on the CDN with a **short TTL for VOD** (to enable quick takedown or rendition additions) and **very short or no TTL for live.**
- **Trade-off:** a longer TTL means better hit ratio but slower updates (takedowns, newly finished renditions). A short TTL on tiny files is cheap, so keep it short.

### 5. Segment delivery
- Edge hit: served directly. Miss: mid-tier, then shield, then origin, with coalescing at each hop.
- **Origin protections:** per-tier rate limiting, load shedding on origin reads, and prioritizing playback-critical reads over background fills or pre-warm traffic.
- **Reliability:** if origin is slow, the tiers **serve stale** where possible, and clients lean on their buffer and ABR to ride it out.

### 6. Telemetry (the feedback loop)
- Clients send **QoE beacons:** startup time, rebuffer events, bitrate, CDN host, errors. This feeds **steering decisions, CDN health detection, ABR tuning, and capacity planning.**
- **Best-effort and lossy:** never block playback on telemetry. Sample under load. Trade-off: full-fidelity data for analysis versus client and network cost.
- **QoE as the SLI:** alert on rebuffer ratio and startup time by ISP and CDN, not just server error rates, because a CDN can be "up" and still deliver poor video.

---

## 9. Supporting systems

### Metadata and its cache
- Playback reads are cache-heavy and highly skewed to popular videos. Use a **sharded DB by `videoId`** with a multi-tier cache (local L1, then distributed cache), and **negative caching** for nonexistent IDs.
- **Status as a state machine** (`UPLOADING → PROCESSING → READY → BLOCKED/DELETED`) with **conditional writes (compare-and-set on version),** the same pattern as the trip and file-commit designs.
- **Visibility and takedown changes need prompt effect,** so enforce at the playback API and via short-lived tokens rather than waiting for caches to age out.

### Search
A downstream **consumer of video events via Kafka** feeding an inverted index (title, description, captions, tags). It's eventually consistent and never the source of truth. Use ranking signals such as freshness, engagement, and quality.

### Recommendations and the home page
- **Two-stage:** candidate generation (collaborative filtering, embeddings, recent history) → ranking (ML model with features) → business rules and diversity. The same structure as the feed design's ranker.
- **Precompute per-user rows** (Netflix-style) in batch and cache them. The home page then loads from a cache, with **real-time re-ranking** on top.
- **Graceful degradation:** if the recommendation service is down, fall back to **popular and trending lists** (identical for everyone, and CDN-cacheable, reusing the Top-K design). The playback path never depends on it.

### View counts, likes, and watch history
- **View counts:** a "view" is counted after a watch-time threshold, de-duplicated per session, and **bot-filtered.** Aggregated through a **stream pipeline into sharded counters** (not `UPDATE count+1` on a hot row), shown as approximate and cached. Exact figures for creator analytics and payouts come from **batch reconciliation** (the Lambda pattern from Top-K).
- **Trending/Top-K:** the same bucketed sliding-window aggregation from the earlier design.
- **Watch progress:** heartbeats every ~30-60 s go to a KV store keyed by `userId`, last-write-wins by timestamp, and **debounced** client-side. Eventual consistency is fine ("resume where you left off" a few seconds stale is OK).
- **Comments and likes** live in separate services, sharded by `videoId`, and are **non-critical.** Degrade them first.

### Thumbnails, previews, captions
Generated asynchronously. Thumbnails and scrub-preview sprites are small, highly cacheable images. Captions are stored as sidecar files (WebVTT or similar), generated by ASR and optionally translated, and delivered like any other static asset.

### Origin storage tiering (the long tail problem)
Most uploaded videos are rarely watched, yet their renditions occupy exabytes.
- **Hot:** recent and popular videos on fast storage.
- **Warm/cold:** erasure-coded HDD, then archival tiers for old, unpopular content.
- **Keep the source forever, and consider deleting high-cost renditions of never-watched videos,** then **re-transcode on demand** (lazy or JIT) if one is requested again.
- **Trade-off:** storage savings versus a slow first play (re-encode latency) for rare cold requests. Since most of the long tail is *never* watched again, the economics favor lazy regeneration, but keep at least a **basic low-res rendition** always available to avoid a bad worst case.

---

## 10. Variant: live streaming

If the interviewer pivots, the shape changes:
- **Ingest:** the broadcaster pushes via **RTMP/SRT/WebRTC** to an ingest PoP (redundant dual ingest for failover).
- **Live transcoding** produces the ladder **in real time** (no time for parallel offline chunking, so it's latency-bound and needs dedicated capacity).
- **Packaging** into **low-latency HLS/DASH (chunked CMAF):** manifests update continuously, and parts are published as they're encoded.
- **Distribution:** the same CDN tree, but **no pre-warming is possible** and requests are **perfectly synchronized** (everyone wants the *latest* segment). Request coalescing at every tier is essential, and the origin shield protects the encoder.
- **Latency vs. stability trade-off:** glass-to-glass latency of ~3-5 s with LL-HLS, ~10-30 s for standard HLS (fewer stalls, better cacheability), and sub-second with WebRTC (not CDN-cacheable and much more expensive at scale).
- **DVR/rewind:** the live stream is recorded into segments, so it becomes VOD afterwards (the live-to-VOD handoff, with a quick re-encode for quality).
- **Flash crowds** (a big event) are the major risk: pre-provision capacity, scale mid-tier caches, and degrade by capping the top bitrate rather than letting the system fall over.
- **Chat** is its own system (the chat design: pub/sub fan-out, rate limits, and moderation).

---

## 11. Failure scenarios

| Failure | Impact | Mitigation |
|---|---|---|
| Transcode worker dies | One chunk task lost | Idempotent tasks, retries, spot-friendly design, durable queue |
| Poison video crashes encoders | Pipeline stalls | Sandboxed workers, per-task timeouts, DLQ after N attempts, alerts |
| Transcoding backlog (upload surge) | Slow time-to-watch | Autoscale, priority queues, progressive availability, shed low-priority renditions and codecs |
| Origin storage slow/unavailable | Cache misses fail | Edge/mid-tier serve cached content, serve-stale-on-error, replicated origin, origin shield rate limiting |
| Edge server or PoP fails | Local viewers affected | Steering removes it, client fails over to next CDN host mid-stream |
| Whole CDN provider degrades | Regional QoE drop | Multi-CDN, real-time QoE-based traffic shifting, client-side failover |
| Cache miss storm (new viral video) | Origin overload | Request coalescing, origin shield, pre-positioning for predicted hits, admission policy tuned for surges |
| Playback API down or slow | New plays can't start | Multi-AZ/region, cached playback decisions, minimal dependencies; **plays already in progress are unaffected** |
| DRM license service down | Protected content can't start | Multi-region, license caching, persistent licenses, high capacity headroom |
| Metadata DB shard down | Some videos' metadata unreadable | Replicas, cache absorbs hot reads, serve stale metadata for popular videos |
| Recommendations/search/comments down | Degraded discovery and social features | Fall back to popular/trending, hide comments. **Playback unaffected.** |
| Counter pipeline lag | Stale view counts | Eventually consistent by design, show approximate or cached counts |
| Bad deploy (e.g., broken manifest generation) | Mass playback failures | Canary releases, per-region rollout, kill switch, rollback, **keep the previous manifest version addressable** |
| Hot viral video | Edge/origin hotspot | Replicate hot objects across servers, CDN tiers, per-object rate limits |
| Takedown needed | Illegal/blocked content still playable | Revoke metadata + tokens, purge manifest, short token TTL (segments need no purge) |
| Thundering herd after outage | Reconnect/replay surge | Jittered retries, per-tier rate limits, shed non-critical traffic, staged recovery |
| Region outage | Regional control plane lost | Steer clients to another region's API; **CDN keeps serving cached segments while the origin region is down**; replicated origin storage |
| Corrupt or bad rendition published | Playback errors at a specific bitrate | Per-rendition validation before publish, QoE alerts by rendition, remove from the manifest and ABR drops to the next rendition |

**Principle (static stability):** playback of an already-published video depends only on **(token, manifest, license, CDN segments).** Upload, transcoding, search, recommendations, comments, counts, and analytics can all be down and viewers keep watching. Even the playback API being briefly unavailable doesn't break streams already in flight, because the player has a buffer and cached manifest. **A video platform should degrade by losing features, never by failing to play.**

---

## 12. Consistency decisions

- **Uploaded source:** durable before ack (persist-before-ack). Never lost.
- **Video status and visibility:** **strongly consistent** metadata writes (compare-and-set), enforced at the playback API and token issuance. Takedowns and privacy changes are the cases where staleness matters.
- **Media segments:** immutable, so consistency is trivial. The only rule is that **a manifest never references a segment that isn't durably stored and replicated.** Publish the manifest after the segments (same ordering as Dropbox's commit).
- **CDN content:** eventually consistent by design. Short manifest TTLs bound staleness.
- **View counts, likes:** eventual, approximate, reconciled later.
- **Watch progress:** last-write-wins, eventual across devices.
- **Search index and recommendations:** eventually consistent, derived data.
- **Entitlements/subscriptions:** strong enough that a cancelled or ineligible user can't mint new tokens, with a bounded grace window for cached decisions.

---

## 13. Multi-region

- **Compute is fungible,** so **transcoding follows capacity:** schedule batch jobs wherever spare or cheap capacity exists (including spot), reading source from replicated storage.
- **Origin storage:** replicate renditions (and always the source) to at least two regions. Because objects are immutable and content/path-addressed, **replication needs no conflict resolution.**
- **CDN is global** and caches serve through regional origin outages.
- **Playback API:** active-active in all regions, reading local metadata replicas. Eventual replication lag of seconds only affects "just published" videos.
- **Metadata writes:** home-region ownership per video or per owner, async replication elsewhere. Uploads go to the nearest region.
- **Region evacuation:** steer client traffic away from a failing region (practised regularly, as Netflix does). The CDN and cached manifests keep most viewers uninterrupted.
- **Data residency and licensing:** territory rules are enforced in the playback API and encoded in tokens, so licensed content isn't served outside licensed regions, and some content may be pinned to specific regions.

---

## 14. Scaling summary

| Component | Strategy |
|---|---|
| Upload | Resumable chunked, direct-to-object-store via signed URLs, source durably stored, per-user quotas |
| Transcoding | DAG of idempotent chunk tasks, parallel across chunks/renditions, spot/preemptible capacity, priority queues, popularity-driven re-encoding |
| Origin storage | Immutable versioned objects, erasure coding, hot/warm/cold tiering, lazy regeneration of cold renditions |
| CDN | Tiered (edge → mid-tier → shield → origin), ISP-embedded caches, consistent hashing, admission policies, coalescing, multi-CDN |
| Streaming protocol | HLS/DASH with CMAF, ABR in the client, 4-6 s segments, short-TTL manifests |
| Playback API | Stateless, multi-AZ/region, minimal dependencies, cached metadata, signed tokens |
| DRM | Common encryption, multi-region license servers, KMS-backed keys |
| Metadata | Sharded by `videoId`, cached, state machine with conditional writes |
| Counters/trending | Stream aggregation, sharded counters, batch reconciliation |
| Search/recs | Kafka-fed indexes, precomputed recs, fall back to popular lists |
| Telemetry | Lossy, sampled QoE beacons feeding steering and alerting |
| Regions | Active-active control plane, replicated origin, global CDN, region evacuation |

---

## 15. Likely follow-up questions

- **"How do you get a video playable quickly after upload?"** Parallel chunked transcoding, **progressive availability** (low renditions first), and priority queues. The cheap, universal encode ships first, and expensive codecs come later for popular videos.
- **"Why adaptive bitrate over one MP4?"** The network varies, and a single file either stalls or wastes bandwidth. HLS/DASH segments let the client adapt per segment, and they're plain HTTP objects that any CDN caches.
- **"How does the CDN keep origin load low?"** Tiered caching, immutable versioned segments, admission policies, request coalescing, an origin shield, and pre-positioning of predicted-hot content.
- **"How do you handle a viral video?"** Edge and mid-tier caching absorbs it, coalescing protects origin, hot objects get replicated across edge servers, and you can trigger a re-encode with better codecs once views cross a threshold.
- **"What if the origin goes down?"** Cached content keeps playing (serve-stale), the replicated origin in another region takes over, and the only affected videos are cold ones not cached at the edge.
- **"How do you reduce egress cost?"** Better codecs (AV1) for popular content, per-title encoding, ISP-embedded caches to avoid transit, higher hit ratios, ABR tuned for efficiency, and multi-CDN cost steering.
- **"How do you scale transcoding?"** Chunk-level parallelism, a stateless worker pool on spot capacity, a workflow engine with retries, priority and fairness, and possibly custom hardware at huge scale.
- **"How do you prevent unauthorized access and hotlinking?"** Short-lived signed tokens validated at the edge, DRM for premium content, entitlement and geo checks in the playback API, and rate limits per token and IP.
- **"How do you do a takedown?"** Flip metadata to blocked, stop issuing tokens, purge the manifest, and let short token TTLs expire access. Segments are never purged individually.
- **"How would you handle live?"** RTMP/SRT ingest, real-time transcoding, low-latency chunked CMAF, request coalescing, and a latency-versus-stability choice. Flash-crowd capacity is the main risk.
- **"How do you count views accurately?"** Watch-time threshold, session dedupe, bot filtering, a stream pipeline into sharded counters, and batch reconciliation. The displayed counts are approximate and eventually consistent.
- **"How does resume-watching work across devices?"** Debounced heartbeats to a KV by `userId`, last-write-wins by timestamp.
- **"What if a new video format or codec appears?"** The source is stored forever, so re-encode from it. Packaging with CMAF and JIT packaging makes adding formats cheap.
- **"How do you decide which videos get the expensive encode?"** View velocity and expected lifetime views, because bandwidth savings multiply by views, so effort follows popularity.
- **"How do you measure quality?"** QoE metrics from client beacons (startup time, rebuffer ratio, bitrate, error rate) sliced by ISP, CDN, device, and region, and used for steering and alerts.

---

## 16. How to narrate it

1. **Name the dominant problems:** delivery bandwidth and cost, the compute-heavy asynchronous encode pipeline, and keeping playback always available. Metadata is small and not the bottleneck.
2. **Start naive:** upload to an app server, store one MP4 in a DB or file server, and stream it directly. Show why it fails (app servers carrying bytes, no adaptation, no parallelism, no resumability, an origin that can't serve millions). Then introduce direct-to-storage upload, chunked parallel transcoding, adaptive segments, then the CDN tiers.
3. **Separate the planes:** the data plane (immutable segments through the CDN) and the control plane (small, cacheable, strongly consistent metadata and tokens).
4. **State the contracts:** persist-before-ack for the source, manifest-after-segments publishing, immutable versioned media, and signed short-lived access.
5. **Call out the knobs:** segment duration, ladder and codec choices, chunk size, re-encode threshold, cache admission policy, TTLs, push versus pull split, and token lifetime.
6. **For each box, say what happens when it fails,** and end with "everything except the CDN, token, license, and manifest path can be down and people can still watch."

**Patterns reused from our toolbox:** immutable content-addressed or versioned objects and direct-to-storage uploads (Dropbox), commit-after-upload visibility ordering, idempotent task workers with retries and DLQs (queues, crawler), request coalescing and thundering-herd control (URL shortener, chat), hot-key replication, multi-tier caching with admission control (Bloom-style, from the caching patterns), sharded counters and stream aggregation (Top-K, feed counters), Lambda-style batch reconciliation, state machines with conditional writes (Uber trips), two-stage candidate and ranker recommendations (feed), Zipf-aware tiering and lazy regeneration, graceful degradation, jittered retries, and static stability.

Next, I can play interviewer on this (for example, "a highly anticipated release goes live at 9 PM and 20M people press play within five minutes, walk me through your cache and origin plan"), or we can move on. **Notification system** reuses queues, retries, and per-channel rate limits in a different shape. **Payment system** and **ticket booking** go deep on strong consistency, idempotency, and contention, which this design mostly avoids by making everything immutable.
