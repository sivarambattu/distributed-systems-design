# Metrics / Logging System Design (Datadog / Prometheus / ELK style)

**What to say early:** This is the first design in our series where **the write path is the whole problem**. Data arrives continuously from every machine and process, it is **append-only and almost never updated**, and most of it is never read. The reads that do happen are either tiny and urgent (an alert evaluating every 30 seconds) or large and exploratory (an engineer scanning a day of logs during an incident). Four ideas organize the answer:

1. **Metrics, logs, and traces are three different data shapes** and want three different storage engines. Metrics are compact numeric time series, logs are high-volume text events, and traces are causal graphs of spans.
2. **Cost is the dominant constraint.** At this scale you cannot index everything, keep everything, or query everything at full resolution. The design is a set of deliberate decisions about what to throw away: cardinality limits, sampling, downsampling, and tiered retention.
3. **The monitoring system must be more reliable than the systems it monitors.** If it dies during an outage, you are blind exactly when you need it most.
4. **Telemetry is best-effort, with a priority order.** Losing some debug logs is fine, and losing the metric that fires the page is not.

Scope it first. I'll design a **multi-tenant observability platform** with a metrics pipeline (the deepest part), a logging pipeline, and alerting, and I'll treat tracing briefly.

---

## 1. Requirements

**Functional**
- **Metrics:** ingest numeric time series (counters, gauges, histograms) with labels or tags. Query with aggregation (`sum by (service) (rate(http_requests[5m]))`), graph on dashboards.
- **Logs:** ingest log lines (structured and unstructured), search by time range, field filters, and full text. Support live tail and aggregation ("count of errors by endpoint").
- **Alerting:** evaluate rules against metrics (and log-derived metrics), then notify through routing, grouping, silencing, and escalation.
- **Dashboards** and ad-hoc exploration
- **Retention policies** per data class, with downsampling for old metrics
- **Multi-tenancy:** per-team quotas, isolation, and access control
- Optionally: distributed tracing, anomaly detection, and SLO tracking

**Non-functional**
- **Ingestion availability above all.** Never push back on the app being monitored, and never lose alert-critical data.
- **Freshness:** metrics queryable in under ~10-30 s. Logs searchable in under ~30-60 s.
- **Query latency:** dashboards in 1-2 s for typical queries. Heavy log searches may take longer.
- **Alert latency:** a breached condition produces a notification in about a minute
- **Scale linearly** with sources, and **bound cost** per tenant
- **Durability:** best-effort for most telemetry, stronger for audit and security logs
- **Isolation:** one tenant's cardinality explosion or log flood must not hurt others

**Clarify with the interviewer:**
- Metrics only, logs only, or both? Traces?
- Internal platform for one company, or SaaS for many tenants?
- Scale: number of hosts, series, log volume per day?
- Retention: days of full-resolution data, and how long for rollups?
- Are logs for debugging (lossy is fine) or compliance and audit (must not lose)?
- Push-based or pull-based collection preferred?

---

## 2. Estimation (state assumptions)

**Metrics**
- 500K sources (hosts, containers, pods), each emitting ~200 metrics every 10 s → **~10M data points/sec**
- A data point is 16 B raw (8 B timestamp + 8 B value), but **time-series compression (Gorilla-style) gets ~1.3-2 B/point**, so ~15-20 MB/s ≈ **~1.5 TB/day** compressed
- **Active series (cardinality):** ~100M. Each needs an index entry plus in-memory head state (~3-5 KB per series in a naive TSDB, so ~300-500 GB of RAM across the cluster).
- Dashboards and alert rules: ~50K rules evaluated every 30 s ≈ **~1.7K queries/sec** from alerting alone, plus dashboard refreshes and ad-hoc queries.

**Logs**
- ~2M lines/sec × ~500 B = **~1 GB/s ≈ ~86 TB/day raw**
- Compression gets 8-10× on text, so **~9-10 TB/day** stored. With a full-text index it can be 2-3× raw. Retention of 15 days hot and 90 days cold means **hundreds of TB to PB** total.
- Query volume is low, but each query can scan terabytes.

**Takeaway to say aloud:** Metrics storage is surprisingly cheap per point but **bounded by cardinality** (the number of distinct series, not the number of points). Logs are **orders of magnitude larger in bytes** and dominate the bill. Both are write-heavy at roughly **1,000:1 write-to-read byte ratios**. So the design problems are **ingestion throughput, compression, indexing cost, and tiering**, not read scaling.

---

## 3. API and data model

**Metrics data model (Prometheus/Datadog style)**
```
series identity = metric name + sorted label set
  http_requests_total{service="checkout", region="us-east", status="500", pod="a1b2"}
sample = (timestamp_ms, float64)
```
Metric types:
- **Counter:** monotonically increasing (resets on restart). Query with `rate()`, which must handle resets.
- **Gauge:** goes up and down (memory used, queue depth).
- **Histogram:** counts per latency bucket (`le="0.1"`, `le="0.5"`, ...), which is **mergeable across instances**.
- **Summary or sketch:** quantile structures (t-digest, DDSketch), also mergeable.

**Ingest APIs**
```
POST /v1/metrics   (batched, compressed protobuf: [{name, labels, [(ts, value)]}])
POST /v1/logs      (batched, compressed: [{ts, level, service, host, trace_id, message, fields{}}])
Auth: per-tenant API key → tenantId (stamped on every record server-side)

GET  /v1/query?expr=...&start=&end=&step=      (metrics, range or instant queries)
GET  /v1/logs/search?q=&start=&end=&limit=      (log search)
GET  /v1/logs/tail?q=                           (live tail via stream)
PUT  /v1/alerts/rules                           (rule definitions, versioned)
```

| Entity | Shape | Store |
|---|---|---|
| **Samples** | `(seriesId, ts) → value`, compressed in time-ordered chunks | **Time-series DB** (in-memory head + immutable blocks on SSD/object storage) |
| **Series index** | Inverted index: `label=value → postings list of seriesIds`, and `seriesId → labels` | TSDB index, per block |
| **Log chunks** | Compressed batches of log lines grouped by stream or time window | **Object storage** (columnar or chunked) |
| **Log index** | Either full inverted index (ES-style) or small label index + per-chunk filters (Loki-style) | Search cluster or KV |
| **Rollups** | `(seriesId, resolution, ts) → min, max, sum, count` | TSDB / object storage, long retention |
| **Alert rules and state** | `ruleId → expr, for, labels, receivers`; state per (rule, label set): pending/firing/resolved | Relational (config) + in-memory state with checkpoint |
| **Tenant config** | Quotas, retention, routing, limits | Config DB, cached |
| **Dashboards, users, ACLs** | Standard relational | Relational |

**Key design points:**
- **Series identity is the unit of cost.** Every distinct label combination is a new series with its own index entry, memory, and compression stream.
- **Tenant ID is stamped server-side from the credential**, never trusted from the payload.
- **Timestamps come from the source** (event time), but the server also records ingest time, because source clocks drift.

---

## 4. High-level architecture

```
                         COLLECTION (edge)
Apps/hosts ──► Agent / SDK / OTel Collector (local)
                 • scrape or receive, batch, compress, local disk buffer
                 • add metadata (host, pod, service), scrub PII, sample, drop rules
                       │  (push, over TLS, retries with jittered backoff)
                       ▼
                 Regional Gateway / Distributor (stateless)
                 • authn → tenantId, validate, enforce quotas and cardinality limits
                 • rate limit per tenant, shard by series hash (metrics) or stream (logs)
                       ▼
                 Kafka (durable buffer, separate topics: metrics / logs / traces,
                        partitioned by seriesHash or stream; per-priority lanes)
       ┌───────────────┼────────────────────────┬──────────────────────┐
       ▼               ▼                        ▼                      ▼
  Metric Ingesters   Log Ingesters/Indexers   Stream processors     Raw archive
  (in-mem head,      (parse, enrich, batch    (log→metrics, alert   (object store,
   WAL, flush        into chunks, index)       pre-aggregation,      compliance/replay)
   blocks)                  │                  sampling)
       │                    ▼
       ▼              Log chunk store (object store) + index
  TSDB blocks (SSD → object storage), compaction, downsampling
       │
       └─────────── QUERY LAYER ────────────────────────────────────────────┐
  Query Frontend (split by time, cache, dedupe, queue per tenant)           │
        ▼                                                                    │
  Queriers (fan out to ingesters for recent data + store-gateways for blocks, merge)
        ▼
  Dashboards · Explore UI · API clients

                         ALERTING (separate, isolated)
  Rule Evaluators (sharded, query recent data every 15-60s)
        ▼ state machine (pending → firing → resolved)
  Alert Manager: dedupe, group, silence, inhibit, route ──► Notification system
                                                            (pager, chat, email, webhooks)
  Dead-man's-switch / meta-monitoring in a separate failure domain
```

---

## 5. Core deep dive 1: ingestion, and the push-versus-pull choice

### Push vs. pull
| | **Pull (Prometheus-style scrape)** | **Push (agents send to a gateway)** |
|---|---|---|
| Who initiates | Collector scrapes each target's `/metrics` | Source sends batches |
| Pros | Collector knows what *should* be up, so "target down" is detectable for free. Easy to reason about load (you control the scrape rate). Simple targets. | Works through NAT and firewalls, short-lived jobs (batch, serverless) can push before dying, scales with gateway fleet |
| Cons | Needs service discovery, struggles with ephemeral targets and network boundaries, scrape fan-out needs sharding | Harder to detect a *silent* source, so you need heartbeats. Gateways must absorb bursts. |

**Common answer:** a **local agent that scrapes locally** (pull within the host or pod, where it's cheap and reliable) and **pushes batches upstream.** You get the discoverability of pull and the topology flexibility of push. Short-lived jobs use a push gateway or a direct SDK push.

### The agent (the first reliability boundary)
- **Batch and compress** (e.g., snappy or zstd), since sending point-by-point is wasteful.
- **Bounded local buffer** (memory plus optional disk). If the backend is unreachable, buffer and retry. When the buffer fills, **drop by priority** (oldest low-priority data first), never block the application or consume unbounded memory.
- **Retries with exponential backoff and jitter**, because a backend recovery with all agents retrying simultaneously is a self-inflicted outage.
- **Local processing:** drop unwanted metrics, relabel, aggregate or pre-aggregate (e.g., emit histograms rather than raw per-request latencies), scrub PII from logs, sample logs.
- **Trade-off:** a smart agent cuts bandwidth and cost dramatically but is code on every machine (rollout, upgrade, and resource-usage risk). It must be **lightweight with strict CPU/memory caps**, so monitoring never causes the outage.

### The gateway/distributor
- **Stateless** and horizontally scaled. It authenticates, stamps the tenant, validates (timestamps not absurdly old or in the future, label name and value limits), and enforces **quotas and rate limits per tenant** (the rate limiter design, applied to ingestion).
- **Cardinality enforcement happens here** (next section).
- **Shards by `hash(tenant, seriesLabels)`** for metrics so all samples of one series land on the same ingester (essential for compression and ordering). For logs, shard by stream (a label set like `{service, host}`) or round-robin, since log lines don't need per-series locality.
- **Respond with `429` plus `Retry-After`** when over quota, so well-behaved agents back off.

### Kafka as the durable buffer
- **Why put a queue in the middle:** ingestion spikes (a deploy restarts 10K pods at once, or an incident log storm) are absorbed by Kafka while ingesters and indexers catch up at their own pace. It also gives **replay** (rebuild an index, recover an ingester) and **fan-out** (the same stream feeds storage, alerting pre-aggregation, and archive).
- **Trade-off:** an extra hop and an extra system, adding seconds of latency. Some systems (Prometheus, Cortex in direct mode) skip it and write straight to ingesters with replication. The queue wins at larger scale and for logs. Direct write is simpler and fresher for metrics when scale allows.
- **Priority lanes:** keep **alert-critical metrics** on a separate topic and consumer group from bulk debug logs, so a log flood cannot delay the metric that pages someone. (The same lane isolation as the notification design.)
- **Partition counts and consumer lag:** lag *is* the freshness SLI.

### At-least-once and idempotency
Retries mean duplicates. For metrics, writing `(seriesId, timestamp) → value` is **naturally idempotent** (a duplicate with the same timestamp and value is dropped or overwritten). For logs, duplicates are tolerable (an occasional repeated line) and can be deduped by a content/offset hash at indexing time where it matters. Say: *"at-least-once plus idempotent writes."*

---

## 6. Core deep dive 2: the time-series database and cardinality

### Why not a general-purpose database?
10M inserts/sec of tiny rows, with queries that are almost always "give me series matching these labels over a time range, aggregated." Row-oriented B-tree databases pay huge write amplification per tiny row and store timestamps and values inefficiently. Even Cassandra-style wide-column stores work but waste space without time-series-specific compression. A **purpose-built TSDB** wins.

### Storage engine (Prometheus TSDB / Gorilla / M3 / VictoriaMetrics shape)
1. **Head block in memory:** the most recent ~2 hours of samples per active series, held in memory in compressed chunks. Writes append to the series' open chunk (O(1)). Recent queries and alerts read from here, so they are fast.
2. **Write-ahead log (WAL):** every sample batch is appended to disk first, so a crash can replay the head. (Group commit and `fsync` policy trade durability against latency. For metrics, losing a second of data on a hard crash is usually acceptable, and replication across ingesters covers it.)
3. **Flush into immutable blocks:** every ~2 hours the head is written as an immutable block (chunks plus an index) on local SSD, and later uploaded to **object storage.**
4. **Compaction:** merge small blocks into larger ones (2h → 12h → 24h...), dropping deleted data and improving query locality and index efficiency.
5. **Retention enforcement:** delete whole blocks past retention. Block-granularity deletion is cheap, which is another reason for immutable time-partitioned blocks.

### Compression (the reason this is affordable)
Observed pattern: timestamps arrive at near-regular intervals, and consecutive values are often similar. **Gorilla encoding:**
- **Timestamps:** store the *delta of deltas*. With a regular 10 s scrape, most entries are 0 and take **1 bit.**
- **Values:** **XOR with the previous value** (as float bits). Equal or similar values produce long runs of zeros, stored in very few bits.
- Result: **~1.3-2 bytes per sample** versus 16 raw, roughly a **10× reduction**. This also makes the head small enough to keep in RAM.
- **Trade-off:** sequential, append-only chunks are compact and fast to scan, but **random access and out-of-order inserts are expensive.** Hence strict handling of late data.

### Out-of-order and late data
Compressed chunks assume in-order appends. Options: reject samples older than the head's window (simple, loses late data), allow a **bounded out-of-order window** by buffering in a separate structure and merging at flush (used by modern Prometheus and Mimir), or write late data to a **backfill path.** **Trade-off:** correctness for lagging agents versus ingester complexity and memory.

### The inverted index (how queries find series)
A query selects series by labels: `{service="checkout", status=~"5.."}`.
- Maintain a **postings list** per `label=value` (sorted lists of seriesIds), and answer multi-label selectors by **intersecting sorted lists** (the same inverted-index technique as search engines). Regex matchers scan the label's values and union the matching postings.
- Index memory and per-query cost grow with **series count**, so cardinality is the bottleneck.

### Cardinality: the thing that actually breaks metrics systems
**Cardinality = number of distinct series = product of label value counts.** The cost of one metric `http_requests{service, endpoint, status, pod, user_id}` is the *product* of those dimensions. Adding a high-cardinality label (`user_id`, `request_id`, `pod` in an autoscaling cluster, raw URL paths with IDs) multiplies the series by millions. Consequences:
- Memory in the head and index explodes, ingesters get OOM-killed, and queries slow dramatically.
- It's usually **accidental** (a developer adds `user_id` as a label) and happens fast.

**Defenses (layered):**
- **Per-tenant, per-metric active-series limits** enforced at the gateway, with *rejection of the new series* (and a clear error) rather than a crash. **Fail the offender, protect the others.**
- **Label rules:** max label count, name and value length, and a deny-list of known-bad label names (`user_id`, `session_id`).
- **Cardinality analytics** ("top metrics by series count, top labels by distinct values") so owners can find and fix explosions.
- **Churn awareness:** ephemeral labels (pod name, container ID) create *churn*, where short-lived series keep arriving even if active count is stable. Index growth tracks churn, so prefer stable labels or drop `pod` on aggregated views.
- **Pre-aggregation at ingest** (drop the offending label and sum, with agent-side or stream-processor rules) when per-instance detail isn't needed.
- **Trade-off:** hard limits protect the platform but can drop a team's data at an inconvenient moment, so make limits **visible, alerted on, and adjustable**, and shed the *new* series rather than silently losing *existing* ones.

**State your decision:** "A TSDB with an in-memory head, WAL, immutable compressed blocks, an inverted label index, and **cardinality quotas enforced at the edge**, because cardinality, not sample rate, is what takes these systems down."

### Sharding and replication of the metrics tier
- **Shard by series hash** across ingesters (consistent hashing with a ring), so each series lives on a stable set of ingesters.
- **Replication factor 3** across AZs on the write path, with a write quorum of 2. The querier **merges and dedupes** replicas. (Cortex/Mimir/M3 do this.) A single ingester loss then loses nothing.
- **Trade-off:** replication triples ingest cost but removes the data-loss and availability risk of single-copy heads. Pure Prometheus runs a single copy per instance and relies on **HA pairs** (two identical scrapers, dedupe at query time), which is cheaper but weaker at scale.
- **Shuffle-sharding per tenant:** each tenant uses only a subset of ingesters, so a poisonous tenant (a query or ingest pattern that crashes nodes) impacts only a few others. It's an **isolation versus utilization** trade-off.

---

## 7. Core deep dive 3: querying, rollups, and percentiles

### Query path
1. **Query frontend** (stateless): splits a long range into **sub-queries by time** (e.g., a 30-day query becomes 30 daily pieces) that run in parallel, **caches results** per split (hot dashboards repeat constantly, and historical splits are immutable so they cache forever), **queues per tenant** for fairness, and enforces limits (max range, max series touched, timeouts).
2. **Queriers** read **recent data from ingesters** (the head, not yet flushed) and **older data from blocks** via **store-gateways** (which cache the block index and fetch chunks from object storage), then **merge, dedupe replicas, and evaluate** the expression.
3. **Aggregation pushdown:** where possible compute partial aggregates close to the data (per-shard `sum`), then combine. Without it you pull every raw series across the network.

### Downsampling and rollups
Querying a year at 10-second resolution (3M points per series) is absurd for a dashboard that shows 1,000 pixels of width.
- **Rollup in the background:** for blocks older than N days, compute **5-minute and 1-hour aggregates** (**min, max, sum, count** per window, not just average, so you can recompute averages and keep spike visibility). Keep raw data for short retention (e.g., 15 days), 5-minute data for 90 days, and hourly for 1-2 years.
- **Automatic resolution selection:** the query layer picks the coarsest resolution that satisfies the requested step.
- **Trade-off:** storage drops by 30-100× for old data and long-range queries become fast, but **you permanently lose fine detail** (a 10-second spike is gone after the raw window). Keep `max` rollups so spikes remain visible even when the exact shape is not.

### Percentiles: the classic trap
**You cannot average percentiles.** The p99 of the average of two hosts' p99s is meaningless. Solutions:
- **Histograms with fixed buckets:** each instance reports counts per bucket. Bucket counts **sum correctly across instances**, and the quantile is estimated from the merged histogram (linear interpolation inside the bucket). **Accuracy depends on bucket boundaries** chosen up front, so poorly placed buckets give wrong p99s.
- **Mergeable sketches (t-digest, DDSketch, HDR histogram):** compact, mergeable, with **bounded relative error** (DDSketch guarantees e.g. 1% error regardless of distribution). Better accuracy, more complex than fixed buckets.
- **Never store only per-host p99 and "average" it.** This is a favorite interview probe.
- **Trade-off:** histograms add series (one per bucket per label set, so they multiply cardinality), while sketches are compact but need support in the storage and query engine.

### Counters, rates, and staleness
- `rate()` over a counter must **detect resets** (a drop means restart, so treat as a reset to zero and add the new value).
- **Staleness:** when a series disappears (pod killed), the system should mark it stale rather than extrapolate its last value for 5 minutes, or graphs and alerts show ghost data.
- **Scrape/ingest gaps** and clock skew affect `rate()` windows, so use windows of at least 2-4× the scrape interval.

### Protecting the query tier
A single `sum(rate(...[30d]))` over 10M series can consume a cluster. Defenses: **max series per query, max samples, max range, per-tenant concurrency and queue limits, query timeouts, a cache of expensive results,** and **recording rules** (below). Treat dashboards that auto-refresh with heavy queries as a hidden load generator and give heavy dashboards pre-aggregated data sources.

### Recording rules (precomputation)
A frequently used expensive query (`sum by (service) (rate(http_requests_total[5m]))`) can be **precomputed on a schedule** and stored as a new, low-cardinality series. Dashboards and alerts then read the cheap series. This is the **precompute-don't-compute** principle from the Top-K and typeahead designs. Trade-off: extra storage and slight staleness versus massively cheaper reads.

---

## 8. Core deep dive 4: the logging pipeline

### Different problem, different engine
Logs are **unstructured or semi-structured text events**, ~100× the bytes of metrics, queried mostly by **time range plus a few filters**, with occasional full-text search. The central trade-off is **how much to index**.

### Pipeline
```
App stdout/files → agent (tail, multiline join, parse, enrich, sample, scrub)
   → gateway (auth, tenant, quotas) → Kafka (partitioned by stream)
   → indexers/ingesters: batch into chunks (e.g. 1-8 MB), compress, write
   → object storage (chunks) + index (labels, bloom filters, or inverted index)
   → query: prune by time and index, scan matching chunks, filter, return
```

### Structured logging (the best lever you have)
- Emit **JSON or key-value logs** with consistent fields (`ts, level, service, trace_id, user_hash, msg, error`). Parsing free text with regexes at ingest is CPU-expensive and brittle, and it silently breaks when the log format changes.
- Always include **correlation IDs** (`trace_id`, `request_id`) so logs link to traces and to each other across services.
- **Scrub PII and secrets at the agent** (emails, tokens, card numbers), because once written to a searchable store, they are very hard to remove and a compliance risk.

### Indexing strategies (the core trade-off)
| Approach | How it works | Pros | Cons |
|---|---|---|---|
| **Full inverted index** (Elasticsearch/OpenSearch, Splunk) | Index every token of every line | Fast arbitrary full-text search | Index is 1-3× the raw size, heavy CPU and I/O at ingest, expensive at 80+ TB/day |
| **Label-only index + brute-force scan** (Grafana Loki) | Index only a few labels (`service`, `env`, `host`); store compressed log chunks in object storage; filter content by scanning chunks | **Tiny index, cheap ingest, cheap storage on object stores** | Content searches scan data (slow without good label and time narrowing) |
| **Columnar store** (ClickHouse, Parquet + engine) | Store logs as columns with sparse or skip indexes, bloom filters, strong compression | Excellent for **aggregations over logs** ("errors per endpoint") and for structured fields, good compression | Weaker at free-text relevance search, schema discipline needed |
| **Hybrid** | Small index for hot recent data, cheap scan for cold | Balances cost and speed | More moving parts |

**Pruning tricks that make scans fast:** time-partitioned chunks with **min/max time metadata**, **bloom filters per chunk** for tokens or field values (skip chunks that definitely don't contain `request_id=abc`), and label-based chunk selection. Together these turn "scan everything" into "scan a few chunks."

**State your decision:** "Given cost, I'd use a **label-index plus compressed chunks on object storage** for the bulk of logs, with a **columnar/structured path** for aggregation-heavy fields, and a **full-text indexed tier only for a small high-value subset** (errors, security, audit). Indexing every byte is the thing that bankrupts logging platforms."

### Hot, warm, cold tiering
| Tier | Age | Storage | Query speed | Cost |
|---|---|---|---|---|
| Hot | 0-3 days | SSD, indexed | Fast | $$$ |
| Warm | 3-15 days | HDD / cheaper nodes | Moderate | $$ |
| Cold | 15-90 days | Object storage, scan-based | Slow (seconds to minutes) | $ |
| Archive | Compliance period | Glacier-class, compressed | Restore on demand | ¢ |

Move data by **index lifecycle policies**, and delete at retention end. Different retention by class (security and audit logs long, debug logs short).

### Controlling log volume (cost levers)
- **Log levels and sampling:** drop `DEBUG` in production by default. Sample high-volume, low-value lines (1 in 100 successful health-check requests) while **always keeping errors and warnings.** Prefer **head-based per-key sampling** (keep all logs for a sampled trace ID) so sampled data stays coherent.
- **Per-tenant and per-service quotas** with alerts before cutoff, and "noisy neighbor" reports (top log producers by bytes).
- **Dedup and rate-limit repeated lines** at the agent ("this error repeated 50,000 times in 10 s").
- **Log-derived metrics:** compute counts and rates from logs in a stream processor (e.g., `errors_total{service}`), then **alert on the cheap metric** rather than querying logs constantly. This is the log-to-metric bridge.
- **Trade-off:** aggressive sampling and dropping saves a fortune but can erase the one line you needed during a rare incident. Mitigate with **dynamic sampling** (increase retention around errors or for sampled traces) and **tail-based decisions**, and be explicit about what is never dropped (errors, audit).

### Live tail
A streaming endpoint (WebSocket/SSE) that subscribes to the ingest stream with a filter, **bypassing the index.** Rate-limit and cap (a busy service can emit 100K lines/s, which no human or browser can consume), and sample for display. Best-effort.

---

## 9. Distributed tracing (briefly)

- A **trace** is a tree of **spans** (one per service call) tied by `trace_id` and `parent_span_id`, with timings and attributes. Context propagates through request headers (W3C Trace Context).
- **Volume is huge** (every request produces many spans), so **sampling is mandatory:**
  - **Head-based sampling:** decide at the root (e.g., 1%). Cheap and simple, but you might miss rare errors.
  - **Tail-based sampling:** buffer spans, decide after the trace completes, and **keep all slow or errored traces** plus a baseline sample. Better insight, but needs a stateful buffering tier (spans of one trace must reach the same collector, so route by `trace_id`), with extra memory and complexity.
- **Storage:** keyed by `trace_id` (a wide-column or object store) with a small index on service, duration, and error flags. Exemplars link a metric spike (a latency bucket) to a representative trace. Together with correlation IDs in logs, this gives the **metrics → traces → logs** debugging flow.

---

## 10. Core deep dive 5: alerting (the part that must not fail)

### Why alerting deserves its own design
Dashboards are optional, but **alerts are the reason the system exists.** A missed alert means a long outage nobody noticed. A noisy alert system means engineers mute it. Both are failures.

### Rule evaluation
- A **rule** is a query plus a condition plus a `for` duration: `error_ratio > 0.05 for 5m`, with labels and annotations.
- **Evaluators** (sharded by rule, with leader/replica pairs) run each rule at a fixed interval (15-60 s) against **recent data** (ingester heads or a fast tier), not the cold store.
- **State machine per alert instance (per label set):** `inactive → pending (condition true, waiting for `for`) → firing → resolved`. The `for` clause **suppresses flapping** from momentary blips.
- **Rule state is checkpointed** so an evaluator restart doesn't reset `pending` timers (otherwise a failover delays a real alert by another `for` period).
- **Evaluation must be cheap and bounded.** Use recording rules and pre-aggregated series for alert inputs, and per-rule query time limits.

### Streaming vs. polling alert evaluation
- **Polling queries** every 30 s are simple and flexible, but add up to the interval plus ingest lag to detection time.
- **Streaming evaluation** (stateful stream processing over the ingest stream) gives lower latency and doesn't add query load, but supports a narrower set of expressions.
- **Common approach:** polling for general rules, with a streaming fast path for the small set of critical, simple threshold alerts.

### Alert manager: turning signals into good notifications
- **Deduplication:** the same alert firing from replicated evaluators produces one notification.
- **Grouping:** 500 pods failing at once produces **one** notification ("checkout: 500 pods unhealthy"), not 500 pages. Grouping by label (service, cluster) is essential during outages.
- **Inhibition:** if the "datacenter unreachable" alert is firing, suppress the hundreds of downstream "service X unreachable" alerts.
- **Silencing and maintenance windows:** planned work shouldn't page anyone.
- **Routing:** by labels to teams, with **escalation policies** (page primary, then secondary after 10 minutes unacknowledged) and severity (page vs. ticket vs. chat).
- **Delivery goes through a dedicated, highly reliable notification path** (the notification system design: provider failover, retries, and a **separate critical lane**). A paging path that depends on the same infrastructure that's failing defeats the purpose.
- **Trade-off:** aggressive grouping and inhibition reduce noise but can hide distinct problems inside one grouped alert, so tune carefully and keep details one click away.

### Missing data and the dead-man's switch
- **"No data" is itself a signal.** A silent source (dead host, broken agent) emits nothing, so a threshold alert never fires. Rules need explicit absence detection (`absent(up{job="x"})`) or heartbeat-based liveness.
- **Dead-man's switch:** an always-firing "watchdog" alert sent to an *external* system that **pages you if it stops arriving.** This detects "the alerting pipeline itself is dead," which in-system alerts can't.
- **Meta-monitoring:** the observability platform is monitored by a **separate, minimal instance in a different failure domain** (different cluster, ideally a different region or even provider), watching ingest rate, lag, evaluator health, and notification success.

### Alert quality (the human side)
- Alert on **symptoms** (user-visible error rate, latency SLO burn) rather than **causes** (CPU at 85%), and page only for actionable, urgent conditions.
- **SLO-based burn-rate alerts** (multi-window, multi-burn-rate) detect "we're consuming the error budget too fast" with far fewer false positives than raw thresholds.
- **Anomaly detection** (seasonal baselines) helps for metrics with strong daily or weekly patterns, but is noisier and harder to trust than explicit thresholds. Use it as a complement.
- **Trade-off:** sensitive alerts catch problems early but cause fatigue, and conservative alerts miss incidents. Measure **time-to-detect, false-positive rate, and pages per on-call shift**, and treat noisy alerts as bugs.

---

## 11. Hop by hop: reliability and trade-offs

### 1. Agent / SDK
- **Must never harm the host:** CPU and memory caps, non-blocking, drop on overflow, fail silent. Instrumentation that takes down the app it observes is the worst case.
- **Buffering and replay:** disk buffer survives short backend outages. **Rolling agent upgrades** with staged rollout and a kill switch (a bad agent release touches *every* host).

### 2. Gateway / distributor
- Stateless, multi-AZ, autoscaled on ingest rate. **Admission control per tenant** prevents one tenant's flood from starving others.
- **Fail behavior:** when the backend is slow, **accept into Kafka if possible**, else `429`/`503` with `Retry-After` so agents buffer. **Never silently drop acknowledged data.**
- **Trade-off:** acking only after Kafka durably accepts is safer but adds latency. Acking at the gateway is faster but loses data on a gateway crash. For metrics and normal logs, **ack after Kafka write**, and agents' buffers cover failures before that.

### 3. Kafka
- RF=3, `acks=all` for durable topics, retention of several hours to days for replay. **Separate priority lanes**, per-tenant partitioning or quotas to prevent noisy tenants from monopolizing partitions.
- **Consumer lag is the master SLI.** Alert on lag *age*, not just bytes.

### 4. Ingesters (metrics) and indexers (logs)
- **Stateful** (the in-memory head, WAL, and open chunks), so they need **ordered, graceful shutdown** (flush the head on SIGTERM), **fast restart** (WAL replay, which can take minutes for big heads, so keep heads bounded), and **replication** so one node loss is invisible.
- **Memory is the limiter.** Cardinality limits, head size limits, and backpressure to the queue when full (stop consuming, let Kafka absorb) keep them alive.
- **Autoscaling stateful tiers is slow** (ring rebalances, data handoff), so over-provision for headroom and scale proactively on trend, not reactively on spikes.
- **Trade-off:** large heads give faster recent queries but longer recovery. Smaller heads flush more often (more small blocks, more compaction).

### 5. Long-term storage
- **Object storage** (S3-class) is the durable, cheap tier for blocks and log chunks: ~11 nines durability, effectively infinite, cheap per GB, but **high per-request latency** and **per-request cost.** Mitigate with large objects, **local caches** (store-gateway index cache, chunk cache), and prefetching.
- **Compactor** is a background singleton-ish job (leased ownership) that merges blocks, applies retention, and downsamples. It must be **idempotent and resumable**, and must never delete a block before its replacement is durably written (the same **write new, verify, then delete** discipline as GC in the Dropbox design).

### 6. Query tier
- **Stateless frontends and queriers**, per-tenant concurrency limits, **query result caching**, and **load shedding** that drops or delays low-priority (ad-hoc, batch) queries before alerting queries.
- **Dedicated capacity or priority lane for alert-evaluation queries**, separate from human dashboards, so a dashboard storm during an incident can't delay alerts. (During an incident, everyone refreshes dashboards, which is precisely when you can least afford slow alert evaluation.)
- **Graceful degradation:** serve partial results with a visible warning if some shards time out, rather than failing the whole dashboard.

### 7. Alerting path
As above: isolated, redundantly deployed, separate failure domain for the watchdog, dedicated notification lane.

---

## 12. Multi-tenancy, security, and cost governance

- **Isolation:** per-tenant quotas (ingest rate, active series, log bytes/day, query concurrency and cost), **shuffle sharding** so noisy or crashing tenants affect few others, and separate clusters for the largest or most sensitive tenants.
- **Access control:** tenant ID stamped from credentials, **row/stream-level ACLs** for logs (a team sees only its services), and audit logs of who queried what. Logs often contain sensitive data, so treat access carefully.
- **PII and secrets:** scrub at the agent and again at ingest. Offer field-level redaction, encryption at rest, and retention-limited storage. Support **right-to-erasure** workflows where logs are keyed to users (hard with immutable chunks, so keep user identifiers out of logs or tokenized).
- **Cost attribution and governance:** show each team its ingest volume, cardinality, and storage cost ("showback/chargeback"). Cost visibility changes behavior more than any limit does. Offer **budget alerts and automatic throttling.**
- **Trade-off:** strict limits protect the platform but frustrate teams, so pair limits with **self-service tooling** (cardinality explorer, usage dashboards, drop rules) and clear escalation paths.

---

## 13. Failure scenarios

| Failure | Impact | Mitigation |
|---|---|---|
| Backend unreachable | Agents can't send | Agent disk/memory buffer, jittered backoff, priority-based drop on overflow, never block the app |
| Retry storm after recovery | Re-overload | Jitter, per-tenant rate limits, Kafka absorbs bursts, gradual ramp |
| **Cardinality explosion** (bad label) | Ingester OOM, index bloat, slow queries | Per-tenant series limits at the gateway, reject new series, cardinality analytics, drop/aggregate rules |
| Log flood (debug left on, error loop) | Pipeline and cost blowup | Per-tenant/service quotas, agent rate limits and sampling, log-line dedup, separate priority lanes |
| Ingester crashes | Loss of in-memory head | WAL replay, RF=3 replication, quorum writes, queue replay |
| Kafka lag grows | Stale data, delayed alerts | Autoscale consumers, shed low-priority lanes, alert on lag age, isolate alert-critical metrics |
| Out-of-order or late data | Rejected or misplaced samples | Bounded out-of-order window, backfill path, event-time vs. ingest-time tracking |
| Clock skew on sources | Wrong timestamps, broken `rate()` | Server-side ingest timestamp, reject extreme skew, NTP monitoring |
| Object storage slow/unavailable | Old data unqueryable | Local caches, serve recent data from ingesters, retries, multi-region buckets |
| Compaction bug or crash mid-way | Corrupt or missing blocks | Write-then-verify-then-delete, idempotent compactor, block integrity checks, versioned blocks |
| Expensive query overload | Dashboards/alerts slow | Query limits, per-tenant queues, caching, separate lane for alert evaluation, recording rules |
| Evaluator crash or restart | Delayed or reset alert state | Replicated evaluators, checkpointed pending state, leader election with fencing |
| **Alerting pipeline silently dead** | Outage goes unnoticed | **Dead-man's switch to an external system**, meta-monitoring in a separate failure domain |
| Source goes silent (dead agent) | Threshold alerts never fire | Absence detection, heartbeat/`up` metrics, alert on "no data" |
| Alert storm during outage | Pager overload, hidden root cause | Grouping, inhibition, rate-limited notifications, escalation, per-service dedupe |
| Notification provider outage | Pages not delivered | Multiple providers and channels, failover, retry (notification system) |
| Bad agent release | Fleet-wide impact | Staged rollout, canary hosts, kill switch, resource caps |
| Tenant crashes shared node with poison query | Cross-tenant impact | Shuffle sharding, query limits, sandboxing |
| Region outage | Regional telemetry/alerts lost | Regional stacks with local alerting, cross-region watchers, buffers at agents |
| Retention/delete bug | Premature data loss | Soft-delete grace, object-store versioning, retention changes reviewed and staged |
| PII leaks into logs | Compliance incident | Scrubbing at agent and ingest, scanning/detection, access controls, purge procedures |

**Principle (the observer must outlive the observed, and degrade by priority):** under stress the platform sheds in a defined order, **debug logs and ad-hoc queries first, then info logs and dashboards, and alert-critical metrics and the paging path last.** Ingestion never blocks the application, alerting runs on isolated capacity, and an external watchdog guards the guard.

---

## 14. Consistency decisions

- **Ingestion:** **at-least-once with idempotent writes** (samples keyed by series and timestamp). Duplicates are harmless.
- **Queries:** **eventually consistent** with seconds of lag. Recent data may be partially replicated, so queriers merge and dedupe replicas, and a query in flight may miss the newest samples.
- **Ordering:** per-series ordering by timestamp within a bounded window. Cross-series and cross-host ordering is **not guaranteed**, and clock skew means log ordering across hosts is approximate, so use trace IDs and causality (parent spans) instead of timestamps where order matters.
- **Alert state:** strongly protected (leader or fenced evaluation, checkpointed), because duplicated or lost alert transitions cause pages or silence. Notifications are at-least-once with dedupe.
- **Config and rules:** versioned, strongly consistent in the config store, eventually applied to evaluators (seconds).
- **Retention and compliance logs:** **durable**, with replicated storage and stronger acknowledgment, where lossy handling is not acceptable.
- **Rollups:** derived data, eventually consistent, and recomputable from raw blocks while they exist.

---

## 15. Multi-region

- **Collect and store regionally.** Each region runs a full stack (gateways, Kafka, ingesters, storage, alert evaluators). Telemetry stays in-region, which keeps cross-region bandwidth low (it's a lot of data) and satisfies data residency. **Alerts for a region evaluate in that region**, so they work even if the inter-region network is down.
- **Global views via federation:** a global query layer fans out to regional stores and merges, or regional systems forward **pre-aggregated, low-cardinality rollups** to a global tier. **Never ship raw high-volume data cross-region** unless required.
- **Cross-region watchers:** each region's meta-monitor is watched by another region and by an external dead-man's switch.
- **Disaster recovery:** object storage replicated across regions (async), Kafka mirrored only for critical topics, with agent buffers bridging gaps. Accept that a regional loss may lose recent unflushed telemetry.
- **Trade-off:** regional isolation gives resilience, locality, and lower egress cost at the price of more clusters to run and harder cross-region queries.

---

## 16. Observability of the observability platform

- **Golden signals for the platform itself:** ingest rate and error rate, **end-to-end freshness** (event time to queryable time), Kafka consumer lag age, ingester memory and series counts, WAL replay time, query latency p50/p99 and failure rate, cache hit ratios, compaction backlog, alert evaluation lag and missed evaluations, notification success rate.
- **Synthetic canaries:** emit a known metric and a known log line every minute, and assert it appears in query results within the freshness SLO. This measures the **entire pipeline end to end**, which component-level health checks can miss.
- **SLOs:** e.g., 99.9% of samples queryable within 30 s, 99.99% alert-evaluation success, p99 dashboard query under 3 s.
- **Run it on a separate, minimal "observer of the observer"** stack, as described above.
- **Capacity planning:** track series growth, bytes/day by tenant, and query cost. Cardinality and log volume grow faster than headcount, so plan headroom and review retention economics regularly.

---

## 17. Scaling summary

| Component | Strategy |
|---|---|
| Agents | Lightweight, local scrape, batch and compress, bounded buffer, local sampling and scrubbing, staged rollout |
| Gateway | Stateless, per-tenant quotas, cardinality limits, shard by series hash or stream |
| Buffer | Kafka with priority lanes, RF=3, lag-age SLI, replay |
| Metrics storage | In-memory head + WAL + immutable compressed blocks, RF=3 across AZs, inverted index, shuffle sharding |
| Long-term metrics | Blocks in object storage, compaction, downsampling (5m/1h with min/max/sum/count) |
| Log storage | Compressed chunks in object storage, label or inverted index by tier, bloom filters, hot/warm/cold lifecycle |
| Query | Frontend splits and caches by time, queriers merge ingesters + blocks, per-tenant limits, recording rules |
| Alerting | Sharded evaluators on isolated capacity, checkpointed state, grouping/inhibition, dedicated notification path, dead-man's switch |
| Cost control | Cardinality quotas, sampling, drop rules, tiered retention, showback |
| Tenancy | Quotas, shuffle sharding, ACLs, per-tenant limits on queries |
| Regions | Regional full stacks, regional alerting, federated or rolled-up global queries |

---

## 18. Likely follow-up questions

- **"Why a special-purpose time-series DB?"** The workload is millions of tiny, in-order appends with label-based queries. TSDBs get ~10× compression via delta-of-delta and XOR encoding, cheap time-based retention via immutable blocks, and an inverted label index for selection.
- **"What's cardinality and why does it matter?"** The number of distinct label combinations. Each is a separate series with memory and index cost, so one high-cardinality label (like `user_id`) multiplies series by millions and can crash ingesters. Enforce per-tenant series limits at the edge and expose cardinality analytics.
- **"Push or pull?"** Pull gives discoverability and easy "target down" detection. Push works through firewalls and for short-lived jobs. A local agent scraping locally and pushing upstream gets both.
- **"How do you compute p99 across hosts?"** Not by averaging per-host p99s. Use mergeable histograms (sum bucket counts, then estimate) or sketches (t-digest, DDSketch) with bounded error.
- **"How do you store a year of metrics cheaply?"** Downsample into 5-minute and hourly rollups (keeping min/max/sum/count), keep raw data short-term, and tier blocks to object storage with caching.
- **"Elasticsearch or Loki for logs?"** Full inverted indexing gives fast arbitrary search at 1-3× raw storage and heavy ingest cost. Label-only indexing with compressed chunks on object storage is far cheaper but scans content. Pick by query patterns and budget, often a hybrid with full-text only for high-value logs.
- **"How do you stop one tenant from taking down the platform?"** Quotas and rate limits at the gateway, cardinality caps, per-tenant query limits and queues, shuffle sharding, and priority lanes.
- **"How do you make sure alerts fire when the system is down?"** Isolated alert evaluation capacity, replicated and checkpointed evaluators, a separate notification path, a dead-man's switch to an external service, and meta-monitoring in a separate failure domain.
- **"How do you avoid alert fatigue?"** Alert on symptoms and SLO burn rates, use `for` durations, group and inhibit related alerts, route by severity, and track pages per shift and false-positive rate.
- **"What if the backend is down?"** Agents buffer locally, back off with jitter, and drop by priority when full. Kafka absorbs ingestion bursts, and ingesters catch up on recovery.
- **"How do you handle late or out-of-order data?"** A bounded out-of-order window in the head, a backfill path for older data, and server-side ingest timestamps alongside event time.
- **"How do you search logs fast without indexing everything?"** Time-partitioned chunks with min/max metadata, label indexes, per-chunk bloom filters, and parallel scans over compressed data.
- **"How do you reduce log cost?"** Structured logging, drop debug in production, sampling with errors always kept, dedup and rate limits at the agent, log-derived metrics instead of repeated queries, tiered retention, and per-team cost visibility.
- **"How do you do sampling for traces?"** Head-based sampling is cheap but misses rare failures. Tail-based sampling keeps all slow or errored traces but needs buffering routed by trace ID. Often combine both.
- **"How do you scale queries?"** Time-splitting with parallel execution, result caching for immutable historical ranges, aggregation pushdown, recording rules for expensive repeated queries, and per-tenant limits.
- **"How would you monitor the monitor?"** Synthetic end-to-end canaries, platform golden signals, a minimal independent meta-monitoring stack in another failure domain, and an external dead-man's switch.

---

## 19. How to narrate it

1. **Name the dominant problems:** a massive, continuous, append-only write stream, with cost governed by cardinality and retention. Reads are small by comparison, and alerting reliability is the highest-stakes part.
2. **Start naive:** agents writing rows into a relational DB, and engineers `grep`ing log files on hosts. Show the failures (write amplification, no retention strategy, queries that scan everything, no correlation across hosts, and no alerting if the box dies). Then introduce agents with buffers, a queue, a TSDB with compression and an inverted index, rollups, object-storage tiering, a log engine with a deliberate indexing strategy, and finally the alerting design with its independence guarantees.
3. **Separate the data types:** metrics (compact, aggregated), logs (voluminous, searched), and traces (sampled graphs), linked by labels and IDs.
4. **State the contracts:** ingestion never blocks the app, at-least-once with idempotent writes, bounded freshness, a defined priority order for shedding, and alerting on isolated capacity with an external watchdog.
5. **Call out the knobs:** scrape interval, series limits, label rules, head block size, retention per tier, rollup resolutions, sampling rates, index scope, Kafka lane capacities, alert `for` durations, evaluation interval, and per-tenant quotas.
6. **For each box, say what happens when it fails,** and end with "any component can fail and we lose some fidelity or freshness in a defined priority order, but the pipeline never blocks the application, and the alert that matters still reaches a human."

**Patterns reused from our toolbox:** queues as durable buffers with priority lanes and replay (notifications, crawler), at-least-once with idempotent writes (payments, Top-K), consistent hashing, sharding, and shuffle-sharding for isolation (KV store), immutable versioned blocks with write-verify-delete compaction (Dropbox, typeahead), bucketed time windows and Count-Min or HyperLogLog sketches (Top-K), Bloom filters for pruning (URL shortener, crawler), Lambda-style stream plus batch (Top-K), hot/warm/cold tiering and lazy regeneration (video), rate limiting and quotas per tenant (rate limiter), the notification system as the alert delivery path, leader election with fencing and checkpointed state (chat, Uber), backpressure, load shedding, and circuit breakers (reliability patterns), precompute-and-cache via recording rules (typeahead, Top-K), fail-closed versus fail-open decisions per category, graceful degradation, and static stability.

Next, I can play interviewer on this (for example, "a developer ships a label containing the user ID, your active-series count goes from 100M to 900M in 20 minutes and ingesters are OOM-looping, walk me through containment, recovery, and prevention"), or we can move on. **Distributed message queue (Kafka-like)** opens up the last major black box we've relied on throughout the series. **Google Docs-style collaborative editing** takes the OT/CRDT thread we only touched in Dropbox. **A distributed job scheduler or workflow engine** is another good one that reuses leases, fencing, and exactly-once-ish execution.
