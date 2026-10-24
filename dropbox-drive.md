# Dropbox / Google Drive Design

**What to say early:** This problem is really **three systems in a trench coat**:

1. A **block store**: huge, immutable, content-addressed, and where durability is the whole job.
2. A **metadata service**: small, mutable, and strongly consistent. It holds the file tree, versions, and permissions.
3. A **sync engine**: keeps N devices converged through unreliable networks, offline edits, and concurrent changes.

The key architectural move is separating the **data plane** (bytes never pass through app servers) from the **control plane** (metadata decides what exists). The hard parts are **chunking and dedupe**, **atomic commit of a file version**, **sync and conflict resolution**, and **durability at exabyte scale**.

---

## 1. Requirements

**Functional**
- Upload, download, and delete files and folders (files up to ~50 GB)
- **Automatic sync** across a user's devices, including offline edits that sync when reconnected
- Sharing: shared folders with permissions (view/edit), and shareable links (expiry, password)
- **Version history** and trash/restore
- Efficient sync: changing one paragraph of a 1 GB file shouldn't re-upload 1 GB
- Notifications of changes, plus previews/thumbnails and optionally search

**Non-functional**
- **Durability above all** (11 nines). Losing a user's file is the unforgivable failure.
- **Strongly consistent metadata:** a committed file is visible to all of a user's devices, with no phantom or half-written files
- Sync latency of seconds for small changes
- Bandwidth efficiency (delta sync, dedupe, compression)
- Resumable transfers over flaky networks
- High availability, secure by default (encryption, access control)

**Clarify with the interviewer:** Dropbox-style file sync only, or Google Docs-style real-time collaborative editing too (a very different problem, see section 13)? Is E2EE required? Max file size? How long is version history kept? Is cross-user dedupe allowed? Is search in scope?

---

## 2. Estimation (state assumptions)

- 500M registered users, 100M DAU, ~1 GB average stored → **~500 PB logical**. Dedupe and compression give maybe 1.5-2×, and erasure coding adds ~1.5× overhead, so **~400-450 PB of raw disk**.
- Chunk size 4 MB → ~125B logical chunks. With a ~100-byte index entry each, the **chunk index is ~12 TB**, which is sharded-KV territory.
- ~200 files per user → ~100B file entries × ~500 B ≈ **~50 TB of metadata**. Small compared to the blocks, but it's the hot, strongly consistent part.
- 100M DAU × ~5 file changes/day = 500M/day ≈ **6K commits/sec average, ~20K peak**
- Delta sync means ~1 MB uploaded per change on average → ~500 TB/day ≈ **~50 Gbps sustained**. That is the real cost line.
- ~50M concurrent devices holding a notification connection (a connection-count problem like chat)

**Takeaway to say aloud:** Bytes dominate storage and bandwidth, but **metadata dominates correctness**. Blocks are immutable and idempotent, so they're easy to reason about. All the hard consistency questions live in the small metadata tier. Also note that unlike most designs we've done, this one is **not heavily read-skewed**, because every write generates sync reads on the user's other devices.

---

## 3. API and data model

**Control-plane APIs (small JSON, strongly consistent)**
```
POST /v1/files/commit_check  { namespaceId, path, baseRev, chunkHashes[] } → { missingHashes[], uploadUrls[] }
POST /v1/files/commit        { namespaceId, path, baseRev, manifest[], size, mtime, idemKey } → { fileId, rev } | 409 conflict
GET  /v1/changes?namespaceId=&cursor=     → { entries[], nextCursor, hasMore }
GET  /v1/files/{fileId}/manifest?rev=     → { chunkHashes[], size, ... }
POST /v1/share { ... }   POST /v1/links { ... }   POST /v1/files/{id}/restore?rev=
```
**Data-plane APIs (bytes, direct to the block tier)**
```
PUT  {preSignedUrl}/chunks/{sha256}       (idempotent: same hash = same bytes)
GET  {signedUrl}/chunks/{sha256}          (supports ranges; served via CDN where useful)
```

| Entity | Shape | Store / sharded by |
|---|---|---|
| **File entry (metadata)** | `(namespaceId, fileId) → parentId, name, rev, manifest[chunkHashes], size, mtime, deleted?, version history` | Strongly consistent sharded DB, **by namespaceId** |
| **Namespace journal** | `(namespaceId, seq) → change record (add/edit/move/delete, fileId, rev)` append-only | Same shard as the namespace |
| **Namespace** | `namespaceId → owner/members, ACLs, quota` (one per user root and per shared folder) | Same DB, cached |
| **Chunk index** | `sha256 → location (extent/object, offset, length), refcount, size, createdAt` | Sharded KV, **by hash** |
| **Chunk bytes** | Immutable, compressed (and encrypted) blobs | Block store (object storage or custom, erasure coded) |
| **Devices / sessions** | `deviceId → userId, cursors per namespace, push endpoint` | KV + cache |
| **Share links** | `token → fileId/folderId, perms, expiry, passwordHash` | KV, cached |

**Key design points:**
- **The namespace is the unit of consistency.** Each user's root folder and each shared folder is a namespace, and all its metadata lives on one shard with one ordered journal. Operations inside a namespace are atomic and cheap, and cross-namespace operations are the hard case.
- **Files are addressed by `fileId`, not path.** Renames and moves become cheap metadata edits that don't touch bytes, and sync can tell "moved" apart from "deleted and re-created."
- **A file version is just a manifest:** an ordered list of chunk hashes. Versions are cheap because unchanged chunks are shared.
- **The journal with a monotonically increasing sequence is the backbone of sync.** It's the same idea as the per-conversation `seq` in the chat design.

---

## 4. High-level architecture

```
                         DATA PLANE (bytes; never through app servers)
Client ──(pre-signed URL, TLS)──► Block Servers / Object Store ◄── CDN (shared links, hot chunks)
                                      │ chunk index (hash → location), erasure-coded extents, tiering
                                      ▼
                         CONTROL PLANE (metadata, strongly consistent)
Client → LB → API Gateway (auth, rate limit, quota) → Metadata Service (stateless)
                                      │ 1. dedupe check: which chunks are missing?
                                      │ 2. commit: verify chunks exist → atomic manifest write
                                      │ 3. append to namespace journal (same transaction)
                                      ▼
                         Metadata DB (sharded by namespace, replicated, consistent)
                                      │ outbox → Kafka "namespace.changed"
                  ┌───────────────────┼─────────────────────────────┐
                  ▼                   ▼                             ▼
         Notification Service   Async workers                  Audit/analytics
         (long-poll/WebSocket   thumbnails, previews, virus scan,
          "namespace N changed")  search indexing, version GC, chunk GC

SYNC: other devices get a hint → GET /changes?cursor → download only chunks they lack
```

---

## 5. The core deep dive: chunking, dedupe, and the upload protocol

### Why chunk at all?
Treating a file as one blob means any edit re-uploads everything, a failure at 99% restarts from zero, and large files can't be parallelized. Chunking gives **delta sync**, **resumability**, **parallel transfer**, and **dedupe** all at once.

### Fixed-size vs. content-defined chunking
- **Fixed-size (e.g., 4 MB):** trivial and fast. **Fatal weakness: the insertion problem.** Insert one byte at the start of a file and every following chunk boundary shifts, so every chunk hash changes and you re-upload everything.
- **Content-defined chunking (CDC):** compute a **rolling hash** (Rabin fingerprint, or FastCDC) over a sliding window, and cut a chunk boundary wherever the hash matches a pattern (e.g., low 22 bits are zero, giving ~4 MB average). Boundaries depend on **content, not offsets**, so an insertion only changes the one or two chunks around it, and the rest still hash identically.
- **Trade-offs:** CDC costs CPU on the client (usually fine, and FastCDC is quick), has variable chunk sizes (enforce min/max bounds, e.g., 1 MB to 8 MB), and is more complex. For already-compressed or encrypted files (video, zips), nothing helps either way.
- **My pick:** fixed 4 MB chunks are common and defensible (Dropbox uses them and gets delta efficiency from other tricks for specific cases). CDC gives better delta sync on files edited in the middle. Say you'd start with fixed chunks and move to CDC if telemetry shows poor delta efficiency.

### Chunk size is a tunable trade-off
- **Small chunks (e.g., 256 KB):** better dedupe and finer deltas, but a bigger index, a larger manifest, more requests and metadata overhead per file.
- **Large chunks (e.g., 16 MB):** fewer requests and a smaller index, but coarse deltas and wasted re-upload when one byte changes.
- **4 MB** is a pragmatic middle. Small files (< 4 MB, the majority) are a single chunk, so for them the system degenerates to "upload a blob."

### Content-addressed storage
Chunk ID = **SHA-256 of its contents.** Consequences:
- **Idempotent uploads:** uploading the same chunk twice is harmless, so retries and resumption are trivial.
- **Immutability:** a chunk never changes, so it can be cached and replicated without invalidation.
- **Built-in integrity:** clients and servers verify the hash on read.
- **Free dedupe** (next section).

### The upload protocol (the atomic-commit idea)
```
1. Client scans file → splits into chunks → computes SHA-256 per chunk
2. commit_check(path, baseRev, [h1..hn])
      server → { missing: [h3, h7] }               // what it does NOT already have
3. Client PUTs only chunks h3, h7 directly to the block tier (parallel, resumable, retried freely)
4. commit(path, baseRev, manifest=[h1..hn], idemKey)
      server verifies every chunk exists, then in ONE transaction:
         writes the new file revision (manifest) + appends a journal entry
      → returns rev, or 409 if baseRev is stale (conflict)
5. Journal append fires an event → notify the user's other devices
```
**Why this order matters:** chunks are uploaded **before** metadata references them, and the commit is the single **visibility point.** A crash at any earlier step leaves only unreferenced orphan chunks (harmless, garbage collected later) and never a half-visible file. This mirrors "persist before ack" from the chat design.

**Trade-offs and details:**
- **Orphan chunks** from abandoned uploads cost storage until GC. Track an `uploadedAt` and a "no references after N days" rule.
- **Large files:** an **upload session** records which chunks are done, so a resumed upload just asks "which of these do you already have?" The manifest of a 50 GB file is ~12K hashes (~400 KB), which is fine, though for much larger files you can store the manifest hierarchically as blocks themselves.
- **Parallelism:** upload several chunks concurrently, with bounded concurrency and bandwidth throttling so sync doesn't saturate the user's connection.
- **Stable-file detection:** clients wait until a file stops changing (debounce, check mtime and size twice) before chunking, so half-written files don't sync.

### Dedupe: levels and the security catch
- **Within a file/version:** unchanged chunks are shared across revisions (this is what makes version history cheap).
- **Across a user's files:** copied or renamed files cost nothing.
- **Across users (global dedupe):** the same popular file stored by a million users is stored once. Huge savings (often 20-40% in consumer systems).
- **The side-channel risk:** if `commit_check` says "already have it" for a hash, an attacker can **confirm whether anyone has stored a specific file** (or, with a known hash, effectively obtain it without having the content). Mitigations:
  - **Proof of ownership:** require the client to prove it has the bytes (e.g., server challenges with random byte-range hashes) before granting access via dedupe.
  - **Scope dedupe** per user or per organization (less savings, no cross-user leak).
  - **Rate-limit and monitor** hash probing.
- **Dedupe vs. encryption:** client-side/E2EE encryption with random keys defeats dedupe. **Convergent encryption** (key derived from content) preserves dedupe but leaks "same file" equality. This is the classic privacy vs. cost trade-off.

---

## 6. Metadata service, hop by hop

### The metadata store
- **Needs:** transactions within a namespace, ordered change feed, strong consistency, and high availability. The volume (20K commits/sec peak) is modest, so **favor correctness over exotic scale.**
- **Choices:** sharded **MySQL/Postgres** (Dropbox's approach, with a graph-like layer on top), or a distributed SQL DB such as **Spanner/CockroachDB.** Avoid eventually consistent stores here, because a file tree with races and phantoms is a support nightmare.
- **Shard by namespaceId.** All operations within one namespace are a single-shard transaction. A hot namespace (a huge company shared folder) is the hot-shard risk. Mitigate with namespace splitting at subtree boundaries, read caching, and batching journal appends.
- **Replication:** synchronous within a region across AZs (quorum or consensus-backed), async to a DR region.

### Commit semantics: optimistic concurrency
`commit` carries `baseRev` (the revision the client edited from). The transaction does:
```
if file.currentRev == baseRev:   write new rev; append journal; ack
else:                            reject (409) → client resolves (see conflicts)
```
**Trade-off:** optimistic concurrency (compare-and-set) avoids holding locks across a long upload and never blocks other devices, at the cost of the client handling the occasional rejection. Pessimistic file locks would be awful for offline-capable sync (what if the locker disappears?) and are only used as an optional feature for formats that can't merge (e.g., CAD files).

### The journal and cursor (the sync backbone)
- Every change appends `(namespaceId, seq, op, fileId, rev, ...)` in the **same transaction** as the metadata write, so the journal and the tree can never disagree.
- A device stores **one cursor per namespace** (the last `seq` it has processed). `GET /changes?cursor=` returns everything newer, paginated.
- **Why a journal and not "diff the whole tree"?** Cost is proportional to the number of changes rather than the number of files, and it supports offline devices naturally: a laptop that's been closed for a month just asks for changes since its cursor.
- **Compaction:** if a cursor is older than the retained journal, the client does a **full tree resync** (list and compare). Rare, but must work.

### Cross-namespace operations
Moving a file from your private namespace into a shared folder crosses two shards.
- **Options:** two-phase commit (atomic but slow and blocking on coordinator failure), or a **saga:** copy-into-destination, then delete-from-source, with a recovery process finishing interrupted moves. Since blocks are shared by hash, the "copy" is a **metadata-only operation** (just manifests), which makes the saga cheap.
- **Trade-off:** brief visible states (file exists in both places for a moment) in exchange for no cross-shard locks. Make it idempotent and resumable via an operation record.

### Notification service
- Holds long-lived connections (long-poll, WebSocket, or push on mobile), the same stateful-gateway pattern as the chat design, with a registry of `(device) → gateway` and **subscription per namespace.**
- **The hint contains no data:** just "namespace N advanced to seq S." The device then pulls changes. This means **correctness doesn't depend on notifications being delivered**: lost hints are covered by periodic polling (e.g., every few minutes) and by sync-on-reconnect.
- **Fan-out:** a change in a big shared folder notifies thousands of devices. **Batch and coalesce** (many commits within ~1 s produce one hint), and let clients debounce too.
- **Reliability:** reconnect storms need jittered backoff and connection-rate caps, as in chat.

### Permissions and sharing
- **Shared folder = a namespace with an ACL.** Each member **mounts** it in their own tree, so a share doesn't duplicate data, and edits are one change stream in one journal.
- **Check permissions on every metadata operation** (cached ACLs with short TTLs and explicit invalidation). **Revocation must take effect promptly:** a removed member must stop receiving journal entries and downloading chunks. Signed chunk URLs have short expiry (minutes) for this reason.
- **Share links:** unguessable random tokens (128+ bits), optional password, expiry, view-only versus edit, and revocation. Link downloads go through the CDN with signed URLs and rate limiting, since a viral link is a hot-object scenario.
- **Trade-off:** inherited permissions through folder trees are convenient but expensive to evaluate and to answer "who can see this file?" Cache effective permissions per namespace and recompute on ACL change.

### Quota and billing
Count **logical bytes per user** (not deduped physical bytes) for predictable billing. Enforce quota at `commit_check` or `commit`. Soft over-quota behavior (read-only, grace period) beats data deletion.

---

## 7. The sync engine (client side)

A real sync client is a distributed system by itself, and interviewers often probe it.

**Components**
- **File watcher** (inotify, FSEvents, ReadDirectoryChangesW), with a periodic full scan as a safety net because watchers miss events.
- **Local state DB** (SQLite): for each file, the last-synced revision, the manifest, the local hash, and the mtime. This is the **"base"** for three-way comparison.
- **Chunker and hasher,** a **transfer manager** (parallel, throttled, resumable), and a **reconciler.**

**Reconciliation = three-way compare** of *local state*, *remote state* (via journal), and *last-synced base*:
| Local vs. base | Remote vs. base | Action |
|---|---|---|
| changed | unchanged | **Upload** (commit with baseRev = base) |
| unchanged | changed | **Download** |
| changed | changed | **Conflict** |
| unchanged | unchanged | Nothing |

**Details that matter:**
- **Download only missing chunks:** when a remote file changes, the client diffs the new manifest against locally held chunks and fetches just the delta. It reconstructs the file by writing to a **temp file and atomically renaming**, so apps never see a half-written file.
- **Moves and renames** are detected by `fileId` (or by matching hashes), so they sync as metadata edits, with no byte transfer.
- **Echo suppression:** a change the client itself just wrote (from a download) must not be re-uploaded as a "new local change." Track expected writes.
- **Ignore patterns and special files:** temp files, lock files, symlinks, and files an app has open exclusively.
- **Backpressure and fairness:** throttle bandwidth and CPU, and prioritize small and recently modified files over huge background transfers.
- **LAN sync (optional):** devices on the same network exchange chunks directly, saving WAN bandwidth, and chunk hashes make it safe (verify on receipt).

---

## 8. Conflict resolution

**What a conflict is:** two devices (or users) edit the same file from the same base revision. The second commit gets a **409** because `baseRev` is stale.

**Policies and trade-offs:**
- **Last-writer-wins:** simple, but **silently destroys someone's work.** Unacceptable as a default for user files.
- **Conflicted copy (the standard answer):** keep the winning commit as the file, and save the loser as `report (Alice's conflicted copy 2026-10-17).docx`. **No data is lost,** and the humans merge. The cost is clutter and manual work.
- **Automatic merge:** works for text with 3-way merge but **corrupts binary or structured formats** (a merged .docx or .psd is nonsense), so it's only safe for known mergeable types.
- **OT / CRDT (real-time collaborative editing):** needs knowledge of the document model, with operations rather than file blobs. This is how Google Docs works and it's a **different architecture** (op log, a collaboration server, cursor presence). Name it as the alternative for "docs," not for "files."
- **Locking:** an optional advisory lock for formats that can't merge. It must expire and handle offline.

**Edge cases to name:**
- **Edit vs. delete:** keep the edit (resurrect the file), since deleting someone's unsynced work is worse than a ghost file.
- **Move vs. move** (same file moved to two places), or **a folder moved into its own descendant** (cycle): the server rejects any move that would create a cycle, and the client resolves the other.
- **Name collisions** (two devices create `notes.txt`): rename one with a suffix.

**Principle:** *when in doubt, preserve both versions and tell the user.* Duplicated bytes are cheap compared with lost work.

---

## 9. The block store: durability at exabyte scale

**Requirements:** immutable blobs, extreme durability, low cost per byte, high throughput.

### Where bytes live
- **Managed object storage (S3/GCS):** fastest to build, with durability handled for you. Cost and control limits appear at exabyte scale, which is why Dropbox moved to its own system (Magic Pocket).
- **Custom block store:** commodity disks, your own erasure coding, and tuned layouts. Big savings at scale, but a large engineering and operations investment. **Interview answer:** start on object storage, and mention the custom path only if the scale justifies the cost.

### Layout
- **Pack many chunks into large append-only extents/objects** (e.g., ~1 GB). Billions of tiny objects cost too much in metadata, per-object overhead, and request pricing. The chunk index maps `hash → (extent, offset, length)`.
- **Immutability means no in-place updates,** so replication and caching are trivial and repair is simple.

### Durability techniques
- **Replication (3×)** for the hot tier: simple, fast reads and repair, but 200% overhead.
- **Erasure coding** (e.g., 8 data + 4 parity shards, or LRC) for warm/cold data: **~1.5× overhead** while tolerating multiple simultaneous failures. The trade-off is reconstruction cost (reading many shards on a failure) and slower degraded reads.
- **Spread shards across failure domains** (disks, racks, AZs; optionally regions) so no single domain loss is fatal.
- **Continuous scrubbing:** background re-reading and checksum verification to detect **bit rot**, and automatic repair from redundancy. **Durability is an active process, not a static property.**
- **Verify hashes** at write, at read, and in scrubbing. The content address is a built-in checksum.
- **Cross-region replication** of extents for DR (async), at extra cost. Often applied only to a subset or via an erasure-coded geo layout.

### Tiering
- **Hot:** recently written/read chunks on SSD or replicated disks, fronted by cache/CDN.
- **Warm/cold:** erasure-coded HDD, then archival storage for old versions and untouched data.
- **Policy:** age and access-based migration. **Trade-off:** cheaper storage means slower first-byte latency and retrieval costs, so keep the *latest version of recently used files* hot and push old versions cold.

### Garbage collection (the dangerous part)
Chunks are shared across files, versions, and users, so you can't delete on file delete.
- **Reference counting:** exact and prompt, but **every commit/delete mutates counts on many chunks,** and concurrent increments racing with decrements can wrongly free a chunk, which means **data loss.**
- **Mark-and-sweep:** periodically walk all manifests to compute the live set, then delete unreferenced chunks. Simple and robust against counter bugs, but heavy at scale.
- **Safe practice (hybrid):** approximate refcounts to *nominate* candidates, then a **grace period** (days) plus a **verification pass** before deletion, and **soft-delete first** (move to a quarantine and delete later). Never delete a chunk newer than the oldest in-flight upload session.
- **Trade-off:** aggressive GC reclaims space sooner but raises the risk of irreversible loss. **Conservative GC wastes some storage and is the right default,** because storage is cheap compared to losing a customer's file.

---

## 10. Download path

1. Client asks the metadata service for the file's manifest (permission checked).
2. The service returns **signed, short-lived URLs** (or a token) for the chunks.
3. Client downloads chunks in parallel, **verifies each hash**, reassembles, and atomically renames into place.
4. **Skip chunks already local** (delta download).

**Optimizations and trade-offs:**
- **CDN** for shared-link downloads and viral files. Content-addressed immutable chunks cache perfectly (no invalidation), but signed URLs vary per request, so use **signed cookies or cache keys that ignore the signature.**
- **Range requests** enable previews of huge files and streaming video without full downloads, and resume after interruption.
- **Hot-object protection:** a viral link can overload a block-store shard, so use CDN, replicated hot extents, and per-link rate limits.
- **Smart/online-only files** (placeholders that hydrate on demand, like Drive's streaming mode): the file appears locally but chunks are fetched at open. This saves disk but adds latency and requires reliable network handling.

---

## 11. Async processing (never on the critical path)

Everything below consumes the Kafka stream of namespace changes, so a slow consumer never blocks sync:
- **Thumbnails and previews:** generated from chunks, cached.
- **Virus/malware scanning:** async, quarantining on a hit. Trade-off: scan-before-share is safer but delays availability. A common compromise is to allow the owner immediate access and **gate sharing/link access** on scan completion.
- **Search indexing:** text extraction, OCR, an inverted index. It's eventually consistent and never the source of truth.
- **Version retention and trash purge:** apply retention policy, then hand chunk candidates to GC.
- **Audit logs, analytics, billing metering.**

**Delivery:** at-least-once with idempotent consumers keyed by `(fileId, rev)`, the same pattern as every previous design.

---

## 12. Security and privacy

- **TLS in transit; encryption at rest** with **envelope encryption:** per-chunk or per-extent data keys, wrapped by a KMS-managed key hierarchy. This enables fast key rotation and crypto-shredding.
- **End-to-end encryption (optional):** client-side keys mean the server holds only ciphertext. **Consequences:** no cross-user dedupe, no server-side previews/search/virus scanning/web editing, and key recovery is the user's problem. Real trade-off of privacy versus features and cost, so state your choice by product tier.
- **Access control** on every API and chunk URL. Signed URLs scoped to specific chunks with short expiry.
- **Abuse:** rate limits (per user, per link), takedown tooling for illegal content (hash-based blocklists work well with content-addressed chunks), and bandwidth-abuse detection on public links.
- **Ransomware recovery:** version history plus a "restore account to point in time" feature is a real selling point, and cheap because versions are only manifests.

---

## 13. Variant: Google Docs-style collaborative editing

If the interviewer pivots, the shape changes:
- Files are **structured documents,** so edits are **operations** (insert char at position X), not byte chunks.
- **Operational Transformation (OT)** with a central server serializing operations, or **CRDTs** for decentralized merge, give **real-time convergence** without conflicted copies.
- **Architecture:** a collaboration session service (stateful, WebSocket, one owner per document, like the single-writer-per-conversation pattern), an **op log** as the source of truth, periodic **snapshots** to bound replay, presence/cursors, and permissions.
- **Trade-off:** far better UX for simultaneous editing, but requires format-specific logic and a stateful session owner, so it's a separate subsystem from generic file sync.

---

## 14. Failure scenarios

| Failure | Impact | Mitigation |
|---|---|---|
| Client crashes mid-upload | Partial chunks uploaded | Chunks are idempotent and content-addressed, so resume and ask "which are missing?"; orphans GC'd |
| Crash between chunk upload and commit | Unreferenced chunks | Harmless orphans; client re-commits with the same idempotency key |
| Commit retried after timeout | Possible duplicate revision | `idemKey` dedupes; commit is compare-and-set on `baseRev` |
| Two devices edit the same file | Conflict | 409 on stale `baseRev`, **conflicted copy**, no data lost |
| Notification missed or connection dropped | Device is stale | Hints are optional; periodic poll plus sync-from-cursor on reconnect |
| Metadata shard down | Namespaces on it unavailable for writes | Synchronous replicas with automatic failover; clients queue and retry; local files still usable offline |
| Hot namespace (giant shared folder) | Shard overload, notification storm | Batch/coalesce journal appends and hints, cache reads, split large namespaces |
| Block store node/disk/rack loss | Reduced redundancy | Erasure-coded shards across failure domains, **auto-repair from surviving shards** |
| Silent corruption (bit rot) | Bad bytes served | Hash verification on read, background scrubbing, repair from redundancy |
| Wrong GC (refcount bug) | **Irreversible data loss** | Mark-and-sweep verification, grace period, quarantine before delete, never delete newer than in-flight sessions |
| Cross-namespace move interrupted | File in both places or neither | Idempotent saga with operation record and recovery sweeper |
| Viral shared link | Block-store hot spot, bandwidth spike | CDN, rate limits, replicated hot extents |
| Reconnect storm (outage recovery) | Herd on notification and metadata tiers | Jittered backoff, connection-rate caps, staged recovery, shed non-critical sync |
| Journal retention exceeded (device offline for months) | Cursor too old | Full tree resync (list and compare) |
| Account compromise / ransomware | Mass overwrite or delete | Version history, trash, point-in-time restore, anomaly detection on bulk changes |
| Region outage | Region unavailable | Failover to DR region (metadata async replication lag is the risk), block store cross-region copies, clients reconcile via cursors |
| Bad client release | Mass sync bugs or deletions | Staged rollout, kill switch, server-side guardrails (rate limits on deletes), versions allow recovery |

**Principle (durability first):** **fail toward keeping data.** Orphaned chunks, extra copies, and conflicted files are acceptable outcomes. Silent loss never is. So uploads commit last, deletes are soft and delayed, and GC is conservative.

---

## 15. Consistency decisions

- **Metadata within a namespace:** **strong consistency** (transactions, compare-and-set on `baseRev`, journal appended atomically).
- **Per-namespace total order** via `seq`, the one ordering users feel.
- **Cross-namespace operations:** eventually consistent via sagas, with idempotent and recoverable steps.
- **Chunks:** immutable, so consistency is trivial. The only rule is *"no manifest references a chunk that isn't durably stored."*
- **Sync propagation:** eventual (seconds), with notifications as hints and the cursor as truth.
- **Permissions and revocation:** enforced promptly on every metadata call and with short-lived URLs, because staleness here is a security issue.
- **Search, previews, analytics:** eventually consistent.
- **Quota:** approximately enforced at commit, reconciled asynchronously.

---

## 16. Multi-region

- **Metadata:** each namespace has a **home region** with synchronous multi-AZ replication and async replication to a DR region. A distributed SQL database such as Spanner can provide multi-region consistency at higher write latency. **Trade-off:** home-region ownership is simpler and faster for most users, and users far from home pay cross-region latency on commits.
- **Blocks:** write to the nearest region and replicate asynchronously (or erasure-code across regions for DR). Reads are served from the closest copy or the CDN. Because chunks are immutable and content-addressed, **replication needs no conflict resolution at all,** which is a big simplification.
- **Failover:** devices reconnect to the DR region, then reconcile via cursors. **Replication lag is the risk:** a just-committed change may be absent, and the client's local state still has it, so the client will simply re-upload and commit (idempotent). That is why client-side state matters for recovery.
- **Data residency:** some customers require data to stay within a region (GDPR). Pin namespaces and their blocks to allowed regions, which constrains dedupe and DR choices.

---

## 17. Scaling summary

| Component | Strategy |
|---|---|
| Block tier | Content-addressed immutable chunks packed into large extents, erasure coding, hot/warm/cold tiers, scrubbing |
| Chunk index | Sharded KV by hash, cached, replicated |
| Metadata DB | Sharded by namespace, strongly consistent, sync replication across AZs, per-namespace journal |
| Metadata Service | Stateless, autoscaled, ACL and quota checks with caching |
| Notification service | Stateful gateways, per-namespace subscriptions, coalesced hints, jittered reconnects |
| Transfers | Direct-to-blockstore via signed URLs, parallel chunks, resumable, CDN for shared links |
| Async workers | Kafka consumers (thumbnails, scan, search, GC), idempotent, independently scaled |
| GC | Conservative mark-and-sweep with grace periods and quarantine |
| Regions | Home-region metadata with DR replication, cross-region block replication, residency pinning |

---

## 18. Likely follow-up questions

- **"Why chunk files?"** Delta sync, resumable and parallel transfers, and dedupe, all from one mechanism.
- **"Fixed vs. content-defined chunking?"** Fixed is simple but suffers from the insertion problem. CDC with a rolling hash keeps boundaries stable under edits, at some CPU cost and with variable sizes.
- **"How do you make an upload atomic?"** Upload immutable chunks first, then a single metadata transaction commits the manifest and journal entry. The commit is the visibility point, and earlier failures leave only harmless orphans.
- **"How do you sync a change to other devices?"** The journal append triggers a hint over the notification channel. Devices pull changes since their cursor and download only the chunks they lack. Hints can be lost, and polling and reconnect sync cover it.
- **"What happens when two people edit the same file?"** Optimistic concurrency on `baseRev`: the loser gets a conflict and a **conflicted copy** is saved, so nothing is lost. Real-time collaborative docs use OT/CRDT, a different system.
- **"How do you guarantee you never lose a file?"** Chunk durability (erasure coding across failure domains, scrubbing, hash verification), commit-after-upload ordering, soft deletes and versioning, and conservative GC with grace periods.
- **"How do you delete data when chunks are shared?"** Reference tracking plus mark-and-sweep verification, a grace period, and quarantine. Never rely on a single refcount alone.
- **"Cross-user dedupe: any risk?"** Yes. It can leak whether a file exists. Use proof-of-ownership or scope dedupe, and note the conflict with E2EE.
- **"How do you handle a 50 GB file?"** Chunked parallel upload with a resumable session, a ~12K-entry manifest, and range reads on download.
- **"How do you handle renames of huge folders?"** It's a metadata edit by `fileId` (one parent pointer change), not a byte copy. Moves across namespaces use an idempotent saga on manifests.
- **"What if a device has been offline for a month?"** It resumes from its cursor. If the journal was compacted past it, it does a full tree resync, then three-way reconciles local versus remote versus base.
- **"How do you scale metadata?"** Shard by namespace. Operations inside a namespace stay single-shard, and sagas handle the rare cross-namespace ones. Hot namespaces are split, batched, and cached.
- **"How would you support E2EE?"** Client-side keys and ciphertext chunks. You lose cross-user dedupe, server previews, search and virus scanning, and need key management and recovery.
- **"How do you reduce storage costs?"** Dedupe, compression, erasure coding (1.5× instead of 3×), tiering old versions to cold storage, and version retention limits.
- **"How do you share a folder?"** A shared namespace with an ACL, mounted in each member's tree. One journal, no data copies, with prompt revocation and short-lived chunk URLs.

---

## 19. How to narrate it

1. **Name the dominant problems:** durability, atomic visibility of file versions, and multi-device convergence. Raw QPS isn't the challenge.
2. **Start naive:** upload whole files through an app server into a DB blob, and have clients poll for a file list. Show the failures (bandwidth waste on every edit, no resume, app servers carrying bytes, no way to handle conflicts), then introduce chunking, content addressing, direct-to-storage upload, the commit protocol, the journal and cursors, and finally notifications.
3. **Split the planes:** data plane (immutable, content-addressed, durable) versus control plane (small, strongly consistent, transactional).
4. **State the contracts:** commit-after-upload atomicity, compare-and-set on `baseRev`, per-namespace ordered journal, hints versus truth, and no-data-loss conflict handling.
5. **Call out the knobs:** chunk size and chunking method, erasure coding parameters, hot/cold tiering thresholds, version retention, GC grace period, signed URL TTL, notification coalescing window, journal retention.
6. **For each box, say what happens when it fails,** and end with "any component can fail and the worst case is a delayed sync or an extra conflicted copy, never a lost file."

**Patterns reused from our toolbox:** content-addressed storage and dedupe (from the crawler), idempotency keys and compare-and-set (rate limiter, Uber), the transactional outbox and per-namespace sequence (feed, chat), sync-on-reconnect with cursors and notifications as hints (chat), stateful notification gateways with jittered reconnect, pre-signed direct-to-storage uploads (feed media), sagas for cross-shard operations (Uber payments), erasure coding and tiering (storage selection), Bloom/hash-based existence checks, hot-key mitigation via CDN, Kafka consumers for async derivation, conservative GC and soft deletes, and graceful degradation.

Next, I can play interviewer on this (for example, "your GC job just deleted chunks that were still referenced, walk me through detection, recovery, and prevention"), or we can move on. **Notification system** reuses queues, retries, and per-channel rate limits in a different shape. **Payment system** and **ticket booking** go deep on the strong-consistency, idempotency, and contention side that Uber and Dropbox only touched. **YouTube/Netflix** builds on the blob-storage and CDN ideas here with transcoding pipelines and adaptive bitrate streaming.
