# Typeahead / Search Autocomplete Design

**What to say early:** This is the Top-K design from earlier, wrapped around a very different serving requirement. Two halves with opposite characteristics:

1. **An offline / near-real-time pipeline** that turns billions of raw query logs into a compact, scored suggestion index. It is a batch and stream aggregation problem, and latency doesn't matter.
2. **An online serving path** that answers "what are the best 5-10 completions for this prefix?" in **single-digit milliseconds**, **hundreds of thousands to millions of times per second**. Nothing is computed per request. It is a **precomputed lookup**.

Three principles organize the design:
- **Precompute, don't compute.** Rank at build time, so serving is a prefix lookup returning an already-sorted list.
- **Suggestions are optional.** If the service is slow or down, the search box still works. The whole design degrades toward "show nothing," never toward "block the user."
- **Every keystroke is a request.** Request volume is a multiple of search volume, so **client-side behavior and caching are as important as the backend.**

---

## 1. Requirements

**Functional**
- As the user types, return the **top 5-10 completions** for the prefix
- Ranked by popularity, recency/trending, and optionally personalization and location
- Support multiple languages and scripts
- Filter offensive, illegal, private, or otherwise unsuitable suggestions
- Optionally: typo tolerance, mid-string matching, entity suggestions (people, products), rich suggestions (with images)

**Non-functional**
- **End-to-end latency under ~100 ms** (users perceive lag past ~100-150 ms), so **server p99 under ~10-20 ms**
- **Very high availability**, but with graceful degradation
- **Freshness:** a breaking-news query should appear in minutes. A weekly cadence is too slow for trending, though fine for the stable bulk.
- **Relevance:** the right suggestion in the top 3 is what matters. Accepting a suggestion saves keystrokes, and that is the product metric.
- **Safety and privacy:** never leak private queries, never suggest harmful completions

**Clarify with the interviewer:**
- Web search, e-commerce (product catalog, inventory-aware), or in-app search (contacts, files)? The data source differs: query logs versus a catalog.
- Is personalization in scope? Typo tolerance? Multi-language?
- Scale: QPS, number of distinct queries, how fresh must trending be?
- Prefix-only matching, or also infix ("iphone" for "case for iphone")?

---

## 2. Estimation (state assumptions)

- 500M DAU × ~8 searches/day = **4B searches/day**
- With debouncing and a minimum prefix length, say ~4 suggestion requests per search → **16B requests/day ≈ 185K/sec average, ~500K-1M/sec peak**
- **Edge/CDN caching** absorbs a large share, because short prefixes are few and extremely hot. Assume 60-80% hit rate, so **origin sees ~200K/sec peak**
- **Query logs:** 4B/day × ~100 B = **~400 GB/day**, which goes to Kafka then object storage
- **Distinct queries:** ~1B/month, but a **power-law (Zipf) distribution** means a few hundred million queries above a minimum-frequency threshold cover the vast majority of volume. Say **~100M retained suggestion candidates** × ~25 B ≈ **2.5 GB of raw strings**
- **Index size:** a trie with top-K lists per node is a few times the raw size, so **~10-50 GB**. This **fits in the RAM of one machine.**
- **Serving capacity:** an in-memory prefix lookup takes microseconds, so one node handles ~20-50K requests/sec (network-bound), and **~10-20 nodes** cover origin peak. We run more for redundancy and regions.

**Takeaway to say aloud:** The index is **small, read-only, and replicable in full on every serving node.** That changes the sharding discussion completely. We don't need scatter-gather across shards. We replicate the whole index and scale by adding copies. The hard parts are **index build quality, freshness, safety, and client behavior**, not storage.

---

## 3. API and data model

```
GET /v1/suggest?q=how+to&limit=8&lang=en&region=US&sid=<session>&seq=7
 → { query: "how to", suggestions: [ { text, score?, type }, ... ], indexVersion }
   Headers: Cache-Control: public, max-age=60   (personalization-free responses)

POST /v1/suggest/feedback   { query, shown[], selected, position }   (batched, fire-and-forget)
```

Design notes:
- **GET, not POST,** so responses are CDN- and browser-cacheable.
- The **`seq` number** lets the client discard out-of-order responses (a slow response for "ho" must not overwrite the one for "how").
- Personalization-free responses are the cacheable ones. Personalized results come from a separate path or are merged client-side.

| Entity | Shape | Store |
|---|---|---|
| **Raw query logs** | `(ts, userHash, query, locale, device, clickedResult?)` | Kafka → object store (Parquet) |
| **Aggregated counts** | `(query, locale, window) → count, distinctUsers` | Batch outputs and a stream state store |
| **Candidate table** | `query → normalized, score components, flags (blocked, adult, private)` | Build pipeline working set |
| **Serving index** | Compact immutable structure: prefix → top-K suggestion IDs, plus a string table | **Versioned snapshot files** in object store, loaded into RAM (mmap) on serving nodes |
| **Trending overlay** | `(locale, prefix or query) → rising score` with a short TTL | Redis or in-process delta, fed by the stream job |
| **Blocklist** | `normalized query/pattern → reason` | Small, replicated everywhere, hot-reloadable |
| **User history (personalization)** | `userId → recent queries` | KV store, or client-local |

**Key design point:** the **serving index is immutable and versioned.** It is never updated in place. A new version is built, validated, and swapped in atomically. This gives easy rollback, no write path on the hot tier, and trivial replication (it's just a file).

---

## 4. High-level architecture

```
                      OFFLINE / NEAR-REAL-TIME (builds the index)
Search app ──► query logs ──► Kafka "search.queries"
                                  │
                ┌─────────────────┴──────────────────┐
                ▼                                    ▼
      Batch aggregation (Spark, hourly/daily)   Stream aggregation (Flink, sliding windows)
        normalize · dedupe bots · count            trending detection, velocity
        distinct users · decay · filter                        │
                │                                              ▼
                ▼                                      Trending overlay store (Redis)
      Candidate scoring + safety filters
                │
                ▼
      Index Builder: build trie / FST / prefix map, top-K per prefix
                │ validate (size, diff vs. previous, golden queries, blocklist audit)
                ▼
      Versioned snapshot ──► Object store ──► canary ──► rollout to all serving nodes

                      ONLINE (serving, latency-critical)
Client (debounce, local cache, cancel stale)
   ▼
CDN / Edge cache (key = normalized prefix + locale)  ──miss──►  LB
                                                                 ▼
                                    Suggest Service (stateless; full index in RAM)
                                       1. normalize prefix
                                       2. lookup top-K from index
                                       3. merge trending overlay + (optional) personal history
                                       4. apply live blocklist, dedupe, final rank
                                       5. return top N
```

---

## 5. Core deep dive 1: the serving data structure

**The problem:** given a prefix, return the top-K highest-scored completions in microseconds.

### Naive: `SELECT query FROM queries WHERE query LIKE 'how t%' ORDER BY freq DESC LIMIT 10`
Even with an index on `query`, `LIKE 'prefix%'` finds the matching range, but then **sorting all matches by frequency is expensive** for short prefixes (the prefix "a" matches hundreds of millions of rows). At 500K/sec it's hopeless.

### Option A: Plain trie, compute top-K at query time
Walk the prefix (O(L)), then DFS the subtree to find the top-K by score.
- **Fatal for short prefixes:** the subtree under "a" is enormous. Latency is unbounded.

### Option B: Trie with **top-K cached at every node** (the classic answer)
Each node stores the **precomputed top-K suggestion IDs** for its prefix.
- **Query:** walk L characters (O(L)) and return the node's list. **No subtree traversal.**
- **Space:** each node stores K IDs (e.g., 10 × 4 B = 40 B). Memory = nodes × 40 B + the string table.
- **Build:** bottom-up. A node's top-K is the merge of its children's top-Ks and its own query if it is a complete string.
- **Optimizations:**
  - **Radix / path-compressed trie:** collapse single-child chains, which cuts nodes by several times.
  - **Store suggestion IDs, not strings,** pointing into a shared string table, so each string is stored once.
  - **Skip the cache on deep nodes** that have fewer than K descendants, and just enumerate them on the fly (space saved with negligible time cost).
  - **Compact layouts** (array-based, succinct tries, or an **FST** as in Lucene) for better cache locality and smaller memory.
- **Trade-off:** much more memory than a plain trie, in exchange for constant-time lookup. Since the whole thing is ~10-50 GB and read-only, that's the right trade.

### Option C: Flat hash map `prefix → top-K list`
Precompute a list for every prefix (up to a max length) and store them in a KV store or in-memory hash map.
- **Pros:** dead simple, works in any KV/cache (Redis, memcached, DynamoDB), trivially shardable by prefix hash, and trivially cacheable.
- **Cons:** **space blowup.** Each query contributes up to L prefix entries, so redundant lists are repeated. Mitigate by **storing lists only for prefixes whose top-K differs** from the parent's (or cap prefix depth at ~15-20 chars), and for longer prefixes fall back to a smaller structure.
- **When to prefer:** smaller companies that want to avoid building a custom structure. Also a good fit when the serving tier is a distributed cache.

### Option D: Search engine (Elasticsearch/OpenSearch completion suggester, edge n-grams)
- **Pros:** features out of the box (fuzzy matching, contexts, analyzers for many languages), and nothing to build.
- **Cons:** heavier per-query cost, harder to guarantee single-digit ms at 500K QPS, and less control over ranking and memory. Reasonable for **in-app search with modest QPS**, less so for web-scale.

### Comparison

| Structure | Lookup | Memory | Update | Best for |
|---|---|---|---|---|
| Trie + DFS | Unbounded for short prefixes | Low | Easy | Small datasets only |
| **Trie + top-K per node** | **O(L)** | Medium-high | Rebuild | **Web-scale, read-only serving** |
| Prefix → list hash map | O(1) | Highest | Rebuild | KV/cache-backed, simple |
| FST (Lucene) | O(L) | Very compact | Rebuild | Memory-tight, built-in tooling |
| Search engine | ms-level | Medium | Near-real-time | Rich features, modest QPS |

**State your decision:** "A path-compressed trie (or FST) with precomputed top-K per node, built offline into an immutable snapshot, loaded into RAM on every serving node. A flat prefix map is the simpler alternative if I'm serving out of a KV store."

### Sharding: replicate, don't partition (as long as it fits)
- **Because the index is ~10-50 GB, replicate the full index to every serving node.** Any node answers any request, with no scatter-gather, no routing layer, and trivial scaling and failover.
- **If the index outgrows RAM** (huge catalogs, many languages), partition:
  - **By first character(s) / prefix range:** natural, but **hot shards**, because traffic is wildly skewed (many more queries start with "s" or "a" than with "x"). Fix by sizing shards by **observed traffic**, not alphabet, and splitting heavy prefixes.
  - **By hash of a short prefix:** even load, but a query's lookups land on one shard per prefix (still a single hop). Fine, since one prefix maps to exactly one shard.
  - **By locale/language:** very natural, since a Japanese index and an English index rarely interact.
- **Trade-off:** replication costs memory (N full copies) but gives simplicity and availability. Partitioning saves memory and adds routing, hot shards, and rebalancing.

---

## 6. Core deep dive 2: building the suggestion list (the quality engine)

**Most of the product quality is decided here, offline.**

### Step 1: Normalize
- Lowercase, trim and collapse whitespace, **Unicode normalization (NFKC)**, strip or fold diacritics (where appropriate for the language), normalize punctuation.
- **Language-specific handling:** CJK has no spaces (needs segmentation or character n-grams), Arabic/Hebrew are right-to-left, and some scripts have transliteration (typing Latin letters for Hindi queries).
- Normalization must be **identical in the build pipeline and the serving path**, or lookups silently miss. A shared library with golden tests prevents drift.

### Step 2: Aggregate and score
- Count occurrences per `(query, locale)` over windows (e.g., 1 day, 7 days, 30 days) and combine with **time decay** (exponential decay, so recent traffic counts more).
- **Count distinct users, not raw frequency,** to resist a single bot or user inflating a query. Use **HyperLogLog** for approximate unique counts at scale.
- **Score** = weighted blend: popularity (decayed count), **trending velocity** (recent rate versus baseline), **click-through/acceptance rate** (did users actually pick this suggestion, or search it?), quality signals (results-page success: did the search lead to a click, or was it abandoned?), and freshness.
- **Minimum thresholds:** require a minimum number of **distinct users** (e.g., 50) before a query is a candidate. This both cuts the long tail and provides **privacy protection** (below).

### Step 3: Filter (safety, legal, privacy, quality)
- **Blocklist and classifiers:** offensive, hateful, adult (depending on policy), dangerous, and legally restricted queries. Pattern-based and ML-based.
- **PII removal:** emails, phone numbers, credit-card-like numbers, national IDs, and full names of private individuals. A user typing their own SSN must never become a suggestion for others.
- **k-anonymity threshold:** only queries searched by at least *k* distinct users get in. This prevents **leaking one person's unusual query** ("symptoms of [rare condition] near [small town]").
- **Spam/manipulation:** detect coordinated query inflation (**suggestion bombing**, where attackers repeat queries to push a brand or slur into suggestions), with bot filtering, rate caps per user/IP/device, and anomaly detection on sudden spikes from few sources.
- **Dedupe near-duplicates:** "iphone 15 case", "iphone15 case", "iphone 15 cases" collapse to a canonical form (using normalization plus a similarity or stemming policy).
- **Trade-off:** aggressive filtering protects users and the brand but removes legitimate suggestions (false positives, such as medical or news queries). Maintain an **audit and appeal path**, and keep the blocklist as a **live override** that doesn't wait for an index rebuild.

### Step 4: Build and validate the index
- Build the trie/FST, compute top-K per node (K larger than needed, e.g., 20-50, to leave room for runtime re-ranking and filtering).
- **Validate before shipping:**
  - Size and entry count within expected bounds of the previous version
  - **Golden queries:** known prefixes must return expected results (and known-bad ones must be absent)
  - Blocklist audit over the entire output
  - **Diff against the previous index** to catch catastrophic regressions (a bad log parse that dropped half the data)
- **Staged rollout:** canary a small percentage of nodes, compare quality metrics (accept rate, latency) against the old version, then roll out. Roll back by re-pointing to the previous snapshot.
- **Trade-off:** validation delays freshness slightly, but a corrupted index served to a billion users is far worse than a slow one.

### Freshness: two speeds
| Layer | Cadence | Content |
|---|---|---|
| **Base index** | Daily or hourly rebuild | Stable, well-aggregated, fully filtered, high quality |
| **Trending overlay** | Seconds to minutes | Queries rising sharply right now (news, events, outages) |

- The **stream job** (Flink) maintains sliding-window counts per query (the **bucketed window technique from the Top-K design**), computes velocity (recent rate ÷ baseline), and emits "rising" queries with a score.
- At serve time, the Suggest Service **merges** the overlay's matches for the prefix into the base top-K. The overlay is small (thousands to hundreds of thousands of entries), so it fits in memory and can be pushed to all nodes, or fetched from a fast cache.
- **Trending passes the same safety filters,** and arguably more scrutiny, because fast-rising queries are exactly what attackers and breaking tragedies produce. Often a **faster, stricter automated filter plus human review for sensitive categories.**
- **Trade-off:** freshness versus safety and stability. A rapid overlay surfaces breaking news but is noisier, easier to manipulate, and can show half-baked or distressing queries.

### Time-sensitive and seasonal content
Queries have time structure ("super bowl score" on Sunday, "tax forms" in April). Use **time-of-day, day-of-week, and seasonal features** in scoring, and shorter decay for categories that move fast (news), longer for evergreen ones.

---

## 7. Core deep dive 3: the online ranking path

The base index gives the top-K by global score. The final list is shaped at request time, cheaply:

1. **Normalize the prefix** (same library as the build).
2. **Lookup** the node's precomputed list (K ≈ 20-50 candidates).
3. **Merge** trending overlay candidates for this prefix.
4. **Personalize** (optional): boost the user's own recent or frequent queries that match the prefix. This list is tiny (a few hundred entries per user), held in memory from a KV fetch or sent from the client.
5. **Contextualize:** locale and region boosts (a UK user typing "foo" prefers UK entities), device, and optionally current app context.
6. **Apply the live blocklist** (fast override), dedupe, and enforce diversity.
7. **Return the top N** (5-10).

This is the same **two-stage candidate → re-rank structure** as the feed and video designs, with a very small, fast second stage.

**Personalization trade-offs:**
- **Pros:** a large relevance win for returning users.
- **Cons:** it **breaks CDN cacheability** (every user's response differs), adds a per-request KV lookup, and raises privacy concerns.
- **Standard answer: split the response.** Serve the **generic, cacheable** list from the CDN and the **personal** suggestions **from the client's local history** (or a tiny per-user fetch), and merge in the client. That keeps the expensive part cacheable and the sensitive part local.

**Latency budget (server side):**
```
LB/TLS reuse ~1ms → normalize ~0.1ms → trie lookup ~0.05ms → merge/re-rank ~0.5ms → serialize ~0.2ms
Target server p99 < 10ms. The network round trip dominates, which is why edge caching and proximity matter.
```

### Matching beyond exact prefixes
- **Typo tolerance:** users misspell. Options: edit-distance automata (Levenshtein automaton over an FST), **symmetric-delete** (precompute deletions to match within distance 1-2), or **n-gram indexes.** Fuzzy matching is far more expensive than exact prefix lookup, so **use it only as a fallback** when the exact prefix returns few or no results, or run it in a separate, smaller index. **Trade-off:** better recall for typos versus complexity, latency, and noisier suggestions.
- **Infix/word-level matching:** "case iphone" should match "best iphone 15 case". Index each word-suffix of a query (a **multi-entry** approach: index "iphone 15 case" and "15 case" and "case" all pointing to the full query), at the cost of a bigger index and more ranking noise. Often applied to e-commerce and less to web search.
- **Query rewriting/synonyms/spelling correction:** a separate "did you mean" system rather than inside autocomplete.
- **Entity suggestions:** for e-commerce or contacts, suggestions come from a **catalog or directory index** (products, people, files), often **mixed with query-log suggestions** and ranked by a blended score. Inventory and permissions matter here (don't suggest sold-out items or documents the user can't access, which means **per-user ACL checks** for enterprise search).

---

## 8. Client-side design (half the system)

Because every keystroke is a potential request, the client controls the load.

- **Debounce** (~100-200 ms after the last keystroke), so fast typists don't fire a request per character. **Trade-off:** a longer debounce reduces load but makes suggestions feel laggy, and ~100-150 ms is the usual balance.
- **Minimum prefix length** (often 1-2 characters). A single-character prefix is a huge, popular, **highly cacheable** key. Some products skip suggestions below 2 chars.
- **Cancel in-flight requests** when a new keystroke arrives, and use sequence numbers or request IDs to **drop out-of-order responses.**
- **Client-side cache:** cache results per prefix, and **reuse them while typing.** If the user typed "ho" and you have results, then "how" can be filtered from the "ho" list locally, while waiting for the fresh response. Backspacing hits the cache for free.
- **Prefetch:** pre-load results for the most common next characters or the previous prefix.
- **Persistent connections:** HTTP/2 or HTTP/3 with keep-alive and **connection pre-warming** when the search box is focused, because a TLS handshake on the first keystroke would blow the budget.
- **Send the cheap path first:** show local/personal history instantly (zero network), then merge server suggestions as they arrive.
- **Mobile considerations:** on slow or metered networks, back off (longer debounce, fewer requests), and avoid wasting battery.
- **Trade-off:** aggressive client optimization cuts server load by multiples, but complicates client code across platforms and makes behavior harder to change (staged app releases). Keep tunables **server-driven (remote config)**, e.g., debounce interval and minimum length, so you can change them without an app release.

---

## 9. Caching layers (the traffic absorber)

```
Browser/app cache → CDN edge → regional cache / service-level L1 → in-memory index
```

- **CDN edge:** cache key = **normalized prefix + locale (+ a few coarse dimensions).** The key distribution is extremely skewed: a few thousand short prefixes account for a huge share of requests, so hit rates are high. TTL of **30-120 s** balances freshness (trending) against offload.
- **Cardinality control:** strip tracking params, normalize case and whitespace **before** forming the cache key, and exclude per-user parameters. Otherwise the CDN fragments into useless one-off entries.
- **Long prefixes are the long tail:** they are rarely repeated, so the CDN misses and the origin serves them. But those lookups are the cheapest, since the trie node has few descendants and the lookup is trivial.
- **In-process caches** on the Suggest Service are of limited value, because the index lookup is already in memory. Cache the *merged/re-ranked* result for hot prefixes if the re-rank step is non-trivial.
- **Trade-off:** a longer TTL gives a better hit rate but slower trending, and a shorter TTL is fresher but pushes more load to the origin. Because the **hottest prefixes are short and trending matters most for them,** you can set **shorter TTLs on hot short prefixes** (they get hit constantly, so even a 15-second TTL has a ~100% hit rate) and longer TTLs on rare long prefixes.

---

## 10. Hop by hop: reliability and trade-offs

### 1. Client
- Fails silent: if suggestions don't arrive within a short timeout (~200-300 ms), show none (or local history) and let the user type. **Never block the search box on suggestions.**
- Retries are pointless for something this ephemeral. A newer keystroke supersedes it anyway.

### 2. CDN / edge
- Absorbs the majority of traffic, and serves **stale-if-error** (stale-while-revalidate), so an origin outage shows slightly stale suggestions instead of failing. Request coalescing at the edge prevents stampedes on a freshly expired hot prefix.
- **Trade-off:** serve-stale improves availability, but stale suggestions might include something that should have been taken down. Takedowns therefore use an explicit **purge** path.

### 3. Load balancer
- L7 with health checks, multi-AZ, N+1 capacity. Because every node has the full index, **any node can serve any request**, so simple round-robin or least-connections works. Retry on a different node for transient failures.

### 4. Suggest Service (stateless apart from the loaded index)
- **Memory-mapped immutable index:** loading a new snapshot means mapping a new file and **atomically swapping a pointer**, with the old version kept until in-flight requests drain. No pause, no write contention.
- **Warm-up:** before taking traffic, a new node loads the index and touches pages (or runs warm-up queries), because a cold mmap causes page-fault latency spikes.
- **Tight timeouts** on every optional dependency (trending store, personalization KV). **If they fail, skip them** and return the base list. The base index alone is always enough.
- **Load shedding:** under overload, drop requests for long, unpopular prefixes first (least valuable and least cacheable), or reduce work (skip personalization). Never queue unboundedly.
- **Deploys:** rolling, with the previous index version pinned until the new binary is healthy.
- **Trade-off:** holding the full index in every node means memory cost per node and a slower cold start (loading tens of GB), versus the simplicity and availability benefits. Keep indexes lean (compact trie/FST) to make this cheap.

### 5. Index distribution
- Snapshots live in object storage. Nodes **pull** the new version (versioned, checksummed), validate, and swap. A **control plane** coordinates the rollout (canary → regional waves), tracks the version per node, and can roll back by pointing everyone at the previous version.
- **Mixed versions across nodes are temporarily visible** (a user may see slightly different suggestions from consecutive requests). That's acceptable. Where it matters, include **`indexVersion`** in the response and in the cache key, so versions don't mix in the CDN.
- **Trade-off:** a gradual rollout is safer but briefly inconsistent. A big-bang swap is consistent but risks a global bad-index event.

### 6. Pipeline reliability
- **Idempotent batch jobs** that can be rerun on the same inputs, with **checkpointed** streaming jobs (Kafka offsets).
- **A failed or late build never replaces the live index.** Serving continues with the last good snapshot, plus an alert on **index age.** This is static stability.
- **Late events and duplicates:** the log pipeline is at-least-once. Counting distinct users and using windowed aggregation make slight duplication harmless for ranking.
- **Backfill/replay:** raw logs in object storage let you rebuild any index version or re-run with new scoring.

---

## 11. Feedback loop and evaluation

- **Log impressions and selections:** which suggestions were shown, at which positions, which was chosen, and what the user finally searched. Send **batched, lossy, fire-and-forget** (never block typing).
- **Core metrics:**
  - **Acceptance rate / CTR** of suggestions and **position distribution**
  - **MRR** (mean reciprocal rank) of the accepted suggestion
  - **Keystrokes saved** per search
  - **Zero-suggestion rate** per prefix length
  - Downstream **search success** (did the user click a result?)
- **Position bias:** items shown first get clicked more regardless of quality. Use **position-debiased** metrics or randomized exploration to learn true relevance, rather than letting the model reinforce its own ordering.
- **Feedback into ranking:** acceptance rate becomes a scoring feature in the next build (careful about **feedback loops**, where popular suggestions become more popular simply because they are shown, drowning out new or niche ones). Add exploration or boosts for new candidates.
- **A/B testing:** compare index versions, ranking changes, debounce values, and TTLs on live traffic with guardrail metrics (latency, errors, abuse rate).
- **Trade-off:** optimizing hard for click-through can favor sensational or clickbait queries, so add **quality and safety guardrails** alongside engagement.

---

## 12. Privacy, safety, and abuse

- **Privacy:** the suggestion corpus is derived from user queries, so apply **k-anonymity (minimum distinct users)**, PII scrubbing, and retention limits on raw logs. Don't use sensitive categories (health, sexuality, religion, politics) for personalized suggestions without consent. Honor deletion requests (GDPR) in per-user history stores, and make sure query logs keyed to users can be purged.
- **Differential privacy** is an option for aggregate statistics at the highest privacy bar, at some accuracy cost.
- **Harmful suggestions:** autocomplete can imply defamatory or harmful associations ("[person] is a..."). Policies and legal obligations vary by jurisdiction, so build **removal tooling** (a takedown request becomes a blocklist entry that takes effect in minutes via the live override, not the next index build).
- **Manipulation and poisoning:** attackers craft or bot queries to promote a link or insult a brand. Defenses: distinct-user counting, per-source caps, anomaly detection, delayed inclusion for fast-rising low-diversity queries, and human review for sensitive trends.
- **Scraping:** autocomplete endpoints leak popularity data and are scraped to harvest query logs. Rate limit per IP/device/token (the **rate limiter design**), require lightweight tokens, and watch for enumeration patterns (systematically walking the prefix space).
- **Trade-off:** each safety layer reduces coverage and adds latency or operational load, so allocate the strictest controls to the highest-harm categories.

---

## 13. Failure scenarios

| Failure | Impact | Mitigation |
|---|---|---|
| Suggest node dies | Capacity loss | Stateless nodes, LB ejection, any node serves any request, N+1 sizing |
| Whole origin tier down | No fresh suggestions | **CDN serve-stale-if-error**, client shows none or local history. Search still works |
| Bad index published (corruption, bad logs) | Wrong/empty/harmful suggestions at scale | Pre-publish validation (golden queries, size/diff checks), canary rollout, instant rollback to the previous snapshot |
| Index build job fails or is late | Staleness | Keep serving the last good version, alert on index age, rerun idempotently |
| Offensive suggestion surfaces | Brand/legal harm | **Live blocklist override** at serve time + CDN purge, then fix the pipeline filter |
| Trending overlay store down | No fresh trends | Skip overlay, serve base index, alert |
| Personalization store slow | Latency | Tight timeout, skip personalization, rely on client-local history |
| Cache stampede on a hot prefix | Origin spike | Edge request coalescing, stale-while-revalidate, TTL jitter |
| Traffic spike (breaking news, viral event) | Load surge | CDN absorbs most, shed long-tail prefixes first, autoscale, shorter TTL only on trending prefixes |
| Cold node (new/restarted) | Latency spikes from page faults | Warm-up before joining the LB, preloaded/mmap'd index |
| Normalization mismatch (build vs. serve) | Silent missing suggestions | Shared normalization library, golden tests, monitor zero-result rate by prefix |
| Suggestion bombing / poisoning | Manipulated suggestions | Distinct-user counting, caps per source, anomaly detection, review gate for fast risers |
| Scraping/enumeration | Data leakage, load | Rate limits, tokens, anomaly detection |
| Out-of-order responses on client | UI flicker, wrong suggestions | Sequence numbers, cancel in-flight, drop stale responses |
| Region outage | Regional latency/availability | Multi-region replicas of the index, DNS/anycast failover, CDN cross-region |
| Bad deploy of the service | Errors/latency regression | Canary, automated rollback on latency/error SLO burn |

**Principle (suggestions are optional, so degrade to less, never to blocking):** autocomplete should fail toward showing **fewer, staler, or no suggestions**, never toward slowing or breaking search. The base index is an immutable snapshot, so serving depends on nothing but memory. Everything else (trending, personalization, feedback logging) is an optional layer that is simply skipped when it misbehaves.

---

## 14. Consistency decisions

- **Index:** eventually consistent by design (refreshed hourly/daily, with a trending overlay in minutes). Nodes may briefly serve different versions.
- **Per-user view:** may see slightly different suggestions across consecutive requests during a rollout or cache-TTL boundary. Acceptable.
- **Takedowns and safety:** **the exception.** They must take effect in minutes, so they use a live blocklist and cache purge rather than waiting for a rebuild.
- **Personalization:** the user's own recent queries should appear promptly, which is why they are **client-local or read-your-writes from a per-user store**, not baked into the index.
- **Counts and scores:** approximate (HyperLogLog, sampling) and never used for money or correctness decisions.
- **Feedback logs:** at-least-once and lossy, used only statistically.

---

## 15. Multi-region and multi-language

- **Indexes are built centrally (or per language/region) and distributed to every serving region**, so users hit nearby nodes. Serving is read-only, so there is **no cross-region write coordination.**
- **Locale-specific indexes:** separate suggestion corpora per language/country, with global fallbacks. A prefix in one language shouldn't surface another's content unless users commonly cross over.
- **Regional trending:** the trending overlay is computed per region (local news, local events), then merged with global trends where appropriate.
- **Data residency:** query logs can contain personal data, so regional pipelines may need to stay in-region, with only aggregated, anonymized candidates shared globally.
- **Failover:** route to the nearest healthy region, and the CDN smooths over blips. The cached edge plus serve-stale handles most single-region trouble.
- **Trade-off:** per-region indexes improve relevance and latency, but multiply build and validation work. Share the pipeline and parameterize by locale.

---

## 16. Observability and operations

- **Serving SLIs:** latency p50/p99 (server and end-to-end), error rate, **CDN hit ratio** (the most important cost lever), requests/sec by prefix length, load shed count, node memory and page-fault rate.
- **Quality SLIs:** acceptance rate, MRR, zero-suggestion rate, keystrokes saved, abandonment, and **per-locale breakdowns** (a regression often hides in one language).
- **Pipeline SLIs:** **index age** (alert when stale), build duration and success, entry count drift, validation failures, overlay lag.
- **Safety dashboards:** blocklist hit rate, flagged-query rate, rising-query review queue, and complaint/takedown turnaround time.
- **Tooling:** a **"why was this suggested?" explainer** (scores, sources, filters applied), a **suggestion preview** for any prefix across index versions, kill switches (disable trending, personalization, or whole suggestions per region), and index version pinning and rollback.
- **Testing:** golden-query regression suites, **shadow traffic** for a new index or service version compared against production, load tests at 2× peak, and chaos exercises (kill nodes, break the trending store, corrupt a snapshot to verify validation catches it).

---

## 17. Scaling summary

| Component | Strategy |
|---|---|
| Client | Debounce, min prefix length, cancel/drop stale responses, local cache, server-driven config |
| Edge/CDN | Normalized prefix cache keys, short TTLs on hot prefixes, serve-stale, request coalescing |
| Suggest Service | Stateless, full immutable index in RAM (mmap), atomic version swap, tight timeouts, load shedding |
| Index structure | Path-compressed trie/FST with top-K per node (or prefix → list map for KV serving) |
| Sharding | **Replicate the full index** while it fits in RAM. Otherwise shard by locale or traffic-balanced prefix |
| Build pipeline | Batch (Spark) for quality + stream (Flink) for trending, validation gates, canary rollout |
| Freshness | Daily/hourly base index + minutes-level trending overlay |
| Personalization | Client-local history or tiny per-user KV, merged outside the cached path |
| Safety | Offline filters + live blocklist override + k-anonymity + PII scrubbing |
| Regions | Per-locale indexes distributed to all regions, regional trending, residency-aware logs |
| Observability | Quality and index-age SLIs, explainers, kill switches, shadow traffic |

---

## 18. Likely follow-up questions

- **"How do you return top suggestions fast?"** A trie (or FST) with the top-K precomputed at every node, so a lookup is O(prefix length) with no subtree traversal. Built offline into an immutable snapshot and held in RAM.
- **"Why not compute the top-K at query time?"** For short prefixes the subtree is enormous. Precomputing trades memory (which is cheap here) for constant-time reads.
- **"How do you update the trie with new queries?"** Don't update in place. Rebuild periodically from aggregated logs into a new versioned snapshot and swap atomically, and use a small trending overlay for minutes-level freshness.
- **"How do you shard it?"** The index is small enough to replicate fully on every node. If it grows, shard by locale or by traffic-aware prefix ranges (not alphabetical, which creates hot shards), or hash the short prefix.
- **"How do you handle trending/breaking news?"** A stream job with sliding-window velocity detection feeds an overlay merged at query time, with a stricter safety review.
- **"How do you reduce backend load?"** Client debouncing, minimum prefix length, local caching, CDN caching on normalized prefixes, and serving the hottest prefixes from the edge. Short prefixes are few and huge in traffic, so caching works extremely well.
- **"How do you personalize without killing the cache?"** Cache the generic list at the edge and merge user-specific suggestions (from local history or a small per-user store) on the client or just before the response.
- **"How do you handle typos?"** Fuzzy matching as a fallback (edit-distance automata, symmetric delete, or n-grams) only when exact prefix results are sparse, since it's far more expensive.
- **"How do you keep offensive or private suggestions out?"** Offline filters (blocklists, classifiers, PII scrubbing, k-anonymity thresholds), plus a live blocklist override that takes effect within minutes.
- **"How do you prevent manipulation?"** Count distinct users instead of raw volume, bot filtering, per-source caps, anomaly detection on fast risers, and review gates for sensitive trends.
- **"What if the index is bad?"** Pre-publish validation, canary rollout, and rollback to the previous snapshot. The versioned immutable design makes rollback just a pointer change.
- **"What if the suggestion service goes down?"** The search box keeps working. The CDN serves stale responses, and the client shows nothing or local history. Suggestions are never on the critical path.
- **"How do you measure success?"** Acceptance rate, keystrokes saved, MRR, zero-suggestion rate, and downstream search success, with position-bias correction and A/B tests.
- **"How would this differ for e-commerce?"** Suggestions come from a product catalog and query logs combined, and are filtered by inventory and availability, personalized by category, and ranked by conversion rather than raw popularity.
- **"How would you support multiple languages?"** Per-locale indexes, language-specific normalization and tokenization (CJK segmentation, transliteration), and locale-aware ranking.
- **"How do you scale 10×?"** More CDN offload, more replicas of the index (stateless scale-out), and sharding by locale if memory grows. The pipeline scales with standard batch and stream scaling.

---

## 19. How to narrate it

1. **Name the dominant problems:** ultra-low-latency lookups at high QPS, driven by the fact that every keystroke is a request, plus the quality, freshness, and safety of the suggestion corpus. Storage is small.
2. **Start naive:** a SQL `LIKE 'prefix%' ORDER BY freq LIMIT 10`. Show why it fails (sorting huge match sets on every keystroke). Then introduce a trie, show why computing top-K by traversal is too slow, add precomputed top-K per node, then argue for building it offline into an immutable snapshot and replicating it everywhere, and finally add the CDN and client behavior.
3. **Split the system into two halves:** the offline/near-real-time pipeline (quality, safety, freshness) and the online serving path (read-only, in-memory, trivially scalable).
4. **State the contracts:** server p99 in single-digit milliseconds, eventual consistency for the index with minutes-level trending, takedowns in minutes via a live blocklist, and suggestions as an optional, best-effort feature.
5. **Call out the knobs:** K per node, debounce interval and minimum prefix length, CDN TTLs (per prefix popularity), rebuild cadence, decay rate, distinct-user threshold, trending sensitivity, and blocklist policy.
6. **For each box, say what happens when it fails,** and end with "any part of this can fail, and the worst case is showing fewer, staler, or no suggestions, while search itself is unaffected."

**Patterns reused from our toolbox:** bucketed sliding-window aggregation, Count-Min/HyperLogLog, and the Lambda-style batch-plus-stream split (Top-K), precompute-and-cache serving with a CDN (URL shortener, Top-K), immutable versioned artifacts with atomic swap and rollback (Dropbox, video), two-stage candidate and re-rank (feed, video recommendations), hot-key and stampede defenses with request coalescing and TTL jitter (distributed cache), consistent hashing and partitioning with hot-shard awareness (KV store), rate limiting against scraping (rate limiter), per-category fail modes and graceful degradation, canary rollouts and kill switches, normalization and idempotent pipelines, and static stability.

Next, I can play interviewer on this (for example, "a breaking tragedy makes a harmful query the top trending suggestion and it is being shown to millions, walk me through containment, root cause, and prevention"), or we can move on. **Ticket booking** is still the natural step for inventory contention, holds with expiry, and payment interplay. **Metrics/logging system** adds time-series ingestion and downsampling. **Google Docs-style collaborative editing** opens up the OT/CRDT thread we only touched in Dropbox.
