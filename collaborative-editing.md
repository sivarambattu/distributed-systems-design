# Collaborative Editing Design (Google Docs / Figma / Notion style)

**What to say early:** Every earlier design avoided or sidestepped concurrent edits to the same data. Dropbox gave you a conflicted copy, tickets gave you a `409`. Here, two people type in the same paragraph at the same instant, and **both edits must survive and both screens must converge to the identical document, with no locks and no conflicted copies.** Five ideas organize the answer:

1. **Convergence is the core problem.** It is solved by one of two algorithm families, **Operational Transformation (OT)** or **CRDTs**, and you should be able to explain both and pick one.
2. **Edits are operations, not file blobs.** The system is an **ordered operation log** plus periodic snapshots, not a file store.
3. **Local-first UX.** The client applies your keystroke immediately and reconciles in the background. Latency to the server must never be felt while typing.
4. **Per-document isolation.** Documents never interact, so the system partitions perfectly by `docId`, and each active document has **one owner** (a single-writer session).
5. **Real-time is a hint, the log is the truth.** The same principle as chat and Dropbox: sockets are an optimization over a durable, replayable log.

Scope it first. I'll design a **rich-text document editor** (Google Docs-like) and note where spreadsheets, whiteboards (Figma), and code editors differ.

---

## 1. Requirements

**Functional**
- Multiple users edit one document simultaneously, with changes visible in **well under a second**
- **Presence:** see collaborators' cursors, selections, and names
- Rich text: formatting, lists, tables, images, comments and suggestions anchored to text
- **Version history** and restore, plus named versions
- **Offline editing** with merge on reconnect
- **Undo/redo** that undoes *your* changes, not other people's
- Sharing and permissions (view, comment, edit), links, and revocation
- Export, search, and notifications (mentions, comment replies)

**Non-functional**
- **Convergence:** all replicas reach the same state, always
- **Intention preservation:** if I bold "hello" while you insert text in the middle, the result should do what each of us meant
- **Local latency ~0:** typing never waits on the network. Remote edits arrive in ~100-300 ms.
- **Durability:** an acknowledged edit is never lost
- **Availability:** a document opens and accepts edits even through backend hiccups (offline tolerance)
- Scale to thousands of concurrent viewers on a popular doc, with ~100 active editors as a realistic cap

**Clarify with the interviewer:** Plain text, rich text, spreadsheet, or canvas? Max concurrent editors? Offline editing required? Is E2EE required (it changes the server's role dramatically)? Comments and suggestion mode in scope? Version history retention?

---

## 2. Estimation (state assumptions)

- 100M DAU, **~10M concurrently open docs at peak**, mostly a single viewer or editor. Typical active docs have 1-3 editors.
- **~5M users actively typing** at peak. Clients **coalesce keystrokes** into one batched op every ~50-100 ms, so ~5M ops/sec cluster-wide at ~100-200 B each → **~0.5-1 GB/s** total ingest.
- **Per-document load is tiny:** even 50 editors at ~2 ops/sec each is ~100 ops/sec on one document, which one process handles trivially.
- **Storage:** 1B documents × ~50 KB snapshot = **~50 TB.** Operation history is larger, say 5-10× the snapshot size, so **a few hundred TB**, which is cheap blob and wide-column territory.
- **Connections:** ~20M concurrent WebSockets (editors plus viewers), the same stateful-gateway problem as the chat design.
- **Large docs:** a 500-page document is a few MB of snapshot, and loading it must not require replaying a million ops.

**Takeaway to say aloud:** Throughput in aggregate is large, but **per-document it is trivial and perfectly shardable.** There is no cross-document contention, no hot-row problem, and no need for distributed consensus on the data path. The difficulty is **algorithmic (convergence), stateful (sessions and connections), and in the long tail of product features** (undo, comments, offline, history).

---

## 3. API and data model

```
WebSocket  /v1/docs/{docId}/session?rev=1432&token=...
  Client → Server
    OP        { clientId, baseRev, opId, ops: [...] }          // batched edit
    CURSOR    { selection }                                    // ephemeral
    SYNC      { rev }                                          // on reconnect
  Server → Client
    ACK       { opId, rev }                                    // your op is committed at rev
    OP        { rev, clientId, ops: [...] }                    // someone else's op, already ordered
    PRESENCE  { users: [{id, color, cursor, selection}] }
    SNAPSHOT  { rev, content }                                 // on join or when too far behind

HTTP
  GET  /v1/docs/{id}                      → metadata + latest snapshot URL (fast open)
  GET  /v1/docs/{id}/history?cursor=      → revision list
  POST /v1/docs/{id}/restore?rev=
  POST /v1/docs/{id}/share  /  /comments  /  /export
```

| Entity | Shape | Store |
|---|---|---|
| **Op log** | `(docId, rev) → { clientId, opId, op, ts, userId }` append-only | Wide-column / sharded KV by `docId`, ordered by `rev` |
| **Snapshot** | `(docId, rev) → full document state` (periodic checkpoint) | Object store, pointer in metadata |
| **Doc metadata** | `docId → owner, title, ACL, latestRev, latestSnapshotRev, state` | Relational/strongly consistent KV, cached |
| **Session registry** | `docId → ownerNode, epoch (lease)` | Coordination store (etcd/Redis with fencing) |
| **Presence** | `(docId, clientId) → cursor, selection, lastSeen` | In-memory on the session owner (TTL), never persisted |
| **Comments/suggestions** | `(docId, commentId) → anchor, thread, state` | Separate service, anchors transformed with edits |
| **Version names** | `(docId, rev) → name, createdBy` | Metadata DB |
| **Search index, previews** | Derived from snapshots/ops | Async consumers |

**Key design points:**
- **`rev` is the document's logical clock:** a monotonically increasing integer assigned by the session owner. It plays the same role as the per-conversation `seq` in chat and the namespace journal in Dropbox.
- **The op log plus snapshots is the source of truth.** The current document is *derived* as `snapshot(rev N) + ops(N+1..latest)`.
- **Presence is ephemeral.** It's never written to the log and is lost safely on restart.

---

## 4. High-level architecture

```
Client (editor, local replica, pending-op buffer, offline queue, IndexedDB cache)
   │  WebSocket (batched ops every ~50-100 ms) + HTTPS for load/export
   ▼
L4/L7 LB (sticky by docId via consistent hash, or routed by the Session Router)
   ▼
┌─────────────────────────── COLLABORATION TIER (stateful) ──────────────────────────┐
│ Edge Gateways: auth, per-connection rate limits, ACL check, multiplex connections   │
│        ▼ route by docId                                                              │
│ Document Session Owner (one per active doc, leased, with epoch):                    │
│   • in-memory document state + recent ops window                                    │
│   • receives ops → TRANSFORM/ORDER → assign rev → append to log (durable) → ACK     │
│   • broadcasts to all connected clients of the doc                                  │
│   • tracks presence; checkpoints snapshots; idle-evicts after last client leaves    │
└──────────────────────────────────────────────────────────────────────────────────────┘
   │ append (quorum write)                     │ lease/ownership
   ▼                                           ▼
Op Log Store (sharded by docId, RF=3)     Coordination store (leases + fencing epochs)
   │ outbox / change stream
   ▼
Kafka "doc.changed" ─► Snapshotter/Compactor · Search indexer · Comment/notification service
                       · Version-history builder · Analytics · Export/preview workers
Snapshots/attachments ─► Object store (+ CDN for images/exports)

Side services: Auth/Sharing (ACLs) · Comments · Presence fan-out · Image upload (pre-signed) · Admin tools
```

**Separate the paths:** the **hot path** (keystroke → session owner → log → broadcast) is stateful and latency-critical. Everything else (search, notifications, history browsing, export) is **asynchronous, derived, and best-effort.**

---

## 5. Core deep dive 1: the convergence problem

### The naive approaches and why they fail

- **Last-write-wins on the whole document:** one user's save overwrites the other's. This is the old "file save" model and is exactly what we're replacing.
- **Locking** (check out the document, or lock the paragraph): kills the collaboration experience, and a disconnected lock-holder blocks everyone (the lock-with-expiry and offline problems we've seen before).
- **Diff-and-merge on whole-document snapshots** (like `git merge`): produces conflicts a human must resolve, and is far too heavy and slow for per-keystroke sync.
- **Sending absolute positions without transformation:** Alice and Bob both start from `"abc"`. Alice inserts `X` at position 0, Bob deletes the character at position 2 (`c`). If the server applies Alice's op and then Bob's *unchanged* op, position 2 is now `b`, so **Bob deletes the wrong character.** The documents diverge from intent.

The fix is to make concurrent operations **commute**, either by *transforming* them (OT) or by *designing the data structure so they commute inherently* (CRDT).

### Option A: Operational Transformation (OT)

**Idea:** each client sends operations expressed against a document version. When two operations were made concurrently, the system **transforms one against the other** so that applying them in either order yields the same result and preserves intent.

**The example, fixed:**
```
Doc: "abc"     A: insert('X', pos 0)     B: delete(pos 2)    (concurrent, both based on "abc")
Server applies A first → "Xabc".
Transform B against A: A inserted before position 2, so B's position shifts → delete(pos 3).
Apply → "Xab".   Client B receives A, transforms A against its own B: insert('X', 0) still pos 0 → "Xab". Converged.
```

**The centralized (Jupiter-style) architecture used in practice:**
- A **single server per document** defines the **total order** of operations (the `rev` counter).
- Each client keeps a simple state machine, so it only ever needs to transform against **one stream** (the server's):
  1. **Synchronized:** no unacknowledged ops. A local edit is applied immediately and sent with `baseRev`, moving to state 2.
  2. **Awaiting ACK:** one op is in flight. Further local edits are **composed into a buffer** (one pending op), not sent yet.
  3. **Awaiting ACK with buffer:** when the ACK arrives, send the buffered op.
  - When a **remote op** arrives while ops are in flight, the client **transforms the remote op against its in-flight and buffered ops** (and transforms those against the remote op), then applies the result locally.
- The server, on receiving an op with `baseRev = r`, **transforms it against all ops committed after `r`**, assigns `rev`, appends to the log, ACKs the sender, and broadcasts the transformed op to the others.
- **Why one in-flight op:** it keeps the transformation bounded to a single pair at each step, so the *only* property you need is **TP1** (transforming two concurrent ops in either order converges). The notoriously difficult **TP2** property, required for fully decentralized, peer-to-peer OT, is **avoided** because the server linearizes everything. (Say this. It's the key to why server-centric OT is tractable.)

**Pros:** compact operations (no per-character metadata), **document size stays small** (text plus formatting only), and it is a proven design (Google Docs, Etherpad, early Wave).
**Cons:**
- **Transform functions are hard.** For plain text it's easy. For **rich text, tables, nested structures, and moves** the number of operation-pair combinations explodes, and subtle bugs cause permanent divergence.
- **Needs a central sequencer per document**, so it is awkward offline and impossible peer-to-peer.
- Hard to test exhaustively, so correctness leans on extensive property-based testing.

### Option B: CRDTs (Conflict-free Replicated Data Types)

**Idea:** design the data structure so that operations are **commutative, associative, and idempotent**, so replicas converge **regardless of the order or duplication of delivery**, with no transformation and no central sequencer.

**Sequence CRDTs for text (RGA / YATA-Yjs / Logoot / Fugue / Automerge):**
- Every character gets a **globally unique, immutable ID** (e.g., `(clientId, counter)`), and positions are expressed **relative to other characters' IDs**, not as integer indices. "Insert `X` **after** character `(A,7)`" is the same operation no matter what else changed.
- **Deletion = tombstone:** mark the character deleted but keep it, so concurrent inserts anchored to it still resolve. This is essential for commutativity.
- **Concurrent inserts at the same spot** are ordered by a deterministic rule (e.g., compare IDs/timestamps), so every replica picks the same order.
- Delivery needs only **causal order** (an op arrives after the ops it depends on), and duplicates are harmless.

**Pros:**
- **Naturally offline-first and peer-to-peer capable.** Merge any two replicas at any time, in any order.
- **No transform functions to get wrong,** so correctness is easier to reason about and prove.
- **No central sequencer required** (a server is still useful for persistence, access control, and fan-out, but it isn't an ordering authority).
- Strong libraries exist (**Yjs, Automerge, Loro**).

**Cons:**
- **Metadata overhead:** every character carries an ID, and tombstones accumulate, so memory and storage can be **10-100× the plain text size** in naive implementations. Modern libraries compress runs of consecutive characters into a single item, which helps greatly.
- **Tombstone garbage collection** is hard: you can only drop a tombstone once you know **no replica will ever reference it** (causal stability), which is difficult with offline clients. In practice, servers compact when safe, and offline clients too old to merge cleanly resync from a snapshot.
- **Interleaving anomalies:** older algorithms could interleave two users' concurrent typing character by character ("hweolrlldo"). Modern ones (YATA, Fugue) avoid this.
- **History semantics** (a "meaningful" per-revision timeline, restore, and selective undo) require extra layers, since the structure is a merge of everything.
- **Rich-text and structural moves** (dragging a paragraph, reparenting nodes) need additional care (tree CRDTs, move operations).

### Comparison

| | **OT (server-sequenced)** | **CRDT** |
|---|---|---|
| Convergence by | Transforming concurrent ops | Commutative data structure |
| Central authority | **Required** (sequencer per doc) | Optional |
| Offline / P2P | Awkward | **Natural** |
| Per-element overhead | Low | Higher (IDs, tombstones) |
| Hard part | Transform function correctness (rich text, TP1 for every op pair) | Metadata growth, GC, history, UX semantics |
| Memory/storage | Small | Larger (mitigated by run-length encoding) |
| Used by | Google Docs, Etherpad | Figma (CRDT-inspired, server-authoritative), Notion/Linear-style tools, Yjs/Automerge apps, Apple Notes |

### The pragmatic hybrid (what most real systems do)
Many production systems are **server-authoritative with CRDT-like or OT-like operation semantics:** a central server per document sequences operations (simple ordering, access control, durability), while the data model uses **unique IDs for objects and properties** so that most conflicts resolve with **last-writer-wins per property** (Figma's approach for design objects). For text, a sequence CRDT or OT handles character-level merging.

**State your decision:** "A **server-authoritative design with one session owner per document** that assigns a total order and persists an op log. For text, I'd use a **sequence CRDT (Yjs-style)** if offline editing and future P2P or local-first are priorities, because there are no transform functions to get wrong. I'd use **OT** if document size and memory matter most and offline is limited, since it has minimal per-character overhead. Either way the *system architecture* (sessions, log, snapshots, presence) is the same. I'd call out that I'm choosing for correctness and simplicity, and that this is a trade-off, not a free lunch."

---

## 6. Core deep dive 2: sessions, ownership, and the hot path

### Why one owner per document
A single authority per active document makes ordering trivial (a counter), allows in-memory state, and enables fast transform and broadcast. This is the **single-writer-per-partition** pattern from chat sequencing, Uber dispatch, and ticket booking. Aggregate scale comes from **millions of independent documents** spread across thousands of owner nodes.

### Placing and routing
- **Document → owner node** by **consistent hashing on `docId`** over the collaboration fleet, with a **lease and epoch** in a coordination store so that exactly one node believes it owns a given document at a time.
- **Gateways route** each client's session to the owner (a registry lookup, or a deterministic hash). Multiple clients of the same document **converge on the same owner**, which is the whole point.
- **Fencing:** every write to the op log carries the **owner's epoch**. If a paused or partitioned old owner wakes up after losing its lease, the log store **rejects its writes** (the fencing-token pattern again), so you never get two writers sequencing the same document (**split-brain**).

### The edit path (the critical path)
1. Client applies the edit **locally and instantly** (optimistic), adds it to its pending buffer, and sends it with `baseRev` and a client-generated **`opId`** (idempotency key).
2. Gateway checks the connection's authorization (edit permission, cached) and forwards to the owner.
3. **Owner:** dedupes on `(clientId, opId)`, **transforms or integrates** the op against concurrent committed ops, assigns the next `rev`, and **appends to the durable op log** (quorum write).
4. **Only after the durable append,** the owner **ACKs the sender** and **broadcasts** the op to the other connected clients.
5. Other clients integrate it into their local replica (transforming against their own pending ops).
6. Asynchronously: a change event goes to Kafka for snapshots, search, notifications, and history.

**Durability rule:** *never ACK or broadcast before the op is durably logged.* Otherwise a crash could leave clients showing text the server doesn't have. This mirrors persist-before-ack from chat and payments.

**Trade-off:** waiting for a quorum append adds ~2-10 ms to the ACK. That's invisible to the typist, because **the local edit was already applied.** The ACK only affects when the pending buffer clears. For other collaborators, it adds a few ms before they see the change, which is acceptable.

### Batching and flow control
- **Clients coalesce** keystrokes into one op per ~50-100 ms (and compose adjacent inserts into one run), cutting message volume by an order of magnitude.
- **Owner batches log appends** (group commit) under load.
- **Backpressure:** if a client's send buffer fills (slow connection), drop the connection and let it resync from `rev` (same rule as chat), instead of buffering unboundedly.
- **Rate limits** per connection and per user, so a buggy client looping on edits can't flood a document.

### In-memory state and the active-document lifecycle
- The owner keeps the **current document state plus a window of recent ops** in memory (needed to transform ops with an old `baseRev`).
- **Cold start:** on first open, the owner loads the **latest snapshot + replays ops since it** from the log, then starts serving. Bound this by snapshotting every N ops or T seconds, so opening a heavily edited document never requires replaying millions of ops.
- **Idle eviction:** when the last client leaves (after a grace period), the owner **flushes a snapshot**, releases the lease, and frees memory.
- **Hot documents** (a 5,000-viewer all-hands doc) are mostly **readers.** Fan out via a **broadcast tier** (viewers subscribe to a pub/sub topic for the doc) so the owner only deals with editors and not thousands of sockets.

---

## 7. Core deep dive 3: persistence, snapshots, history, and compaction

### Op log plus snapshots
- **Op log:** append-only, ordered by `rev`, the **ground truth for history and recovery.**
- **Snapshots:** periodic full-state checkpoints (every ~500-1,000 ops or a few minutes of activity). A document load is **latest snapshot + a short tail of ops.**
- **Trade-off:** more frequent snapshots speed up loading and shorten recovery but cost storage and CPU, and fewer snapshots save resources but slow cold starts.

### Version history ("see who changed what, restore")
- **Per-revision history comes nearly free from the op log:** rebuild any past state by replaying to `rev N` (from the nearest earlier snapshot).
- **Presentation:** raw ops are too granular, so the **history builder** (async) groups ops by user and time window into human-readable **revisions** ("Alice, 10:02-10:14"), with **named versions** pinned by users.
- **Restore** is itself a **new operation** (or set of ops) that sets the document to the old content, appended to the log. History is **never rewritten**, so a restore is undoable, and collaborators converge normally.
- **Attribution:** each op carries `userId`, which powers the "who wrote this" colored view.

### Compaction and retention
- **Old ops can be compacted** (merging consecutive ops from the same author into one, dropping superseded intermediate states) once they're beyond the fine-grained history window, trading granularity for storage.
- **CRDT-specific:** garbage collect **tombstones** once causal stability allows, which in practice means snapshotting to a compact "GC'd" state and requiring very-old offline clients to **resync from a snapshot** rather than merge.
- **Retention:** keep fine-grained history for a bounded window (e.g., 30 days), coarser afterward, and honor legal-hold or deletion policies.
- **Rule:** compaction is **write-new, verify, then delete the old**, the same safe-GC discipline as Dropbox and the metrics compactor.

### Storage design
- **Op log:** wide-column or sharded KV keyed by `(docId, rev)`, **partitioned by `docId`** so a document's ops are co-located for range scans. **RF=3, quorum writes**, spread across AZs.
- **Snapshots and large assets:** object storage with metadata pointers (the Dropbox pattern).
- **Images and attachments:** **direct-to-object-store via pre-signed URL**, referenced in the doc by ID, served through the CDN, with permission checks on access.

---

## 8. Core deep dive 4: presence, cursors, and the ephemeral layer

- **Presence** (who's here, their cursor and selection, colors, "typing" status) is **ephemeral and lossy.** It's held **in memory on the session owner** (or a presence service), with a short TTL refreshed by heartbeats, and **never written to the op log.**
- **Throttle updates** (~10-20/sec max per user) and send **only to others in the same document.** Coalesce, since only the latest position matters (**last-write-wins** is exactly right, as in the Uber location design).
- **Cursor positions must be transformed:** if Bob inserts three characters before Alice's cursor, Alice's cursor index must shift. In OT, transform the cursor like an operation. In a CRDT, **anchor cursors to character IDs** (a "relative position"), so they follow the content naturally. This is a classic detail interviewers like.
- **Large audiences:** with 1,000 viewers, don't broadcast every cursor to everyone. Show only the **nearest N collaborators or active editors**, or sample, since presence is **non-critical** and degrades first under load.
- **Failure behavior:** if presence is down, editing continues untouched. Users just don't see cursors.

---

## 9. Core deep dive 5: offline editing, reconnect, and undo

### Reconnect (a normal event, not an error)
Mobile and laptop connections drop constantly. The protocol makes reconnect boring:
1. Client reconnects with its **last acknowledged `rev`** and its **unacknowledged pending ops** (still identified by `opId`).
2. Server replies with all ops **since that `rev`**, and the client **rebases its pending ops** over them (transform/merge).
3. The client **re-sends** pending ops, and the server **dedupes by `(clientId, opId)`**, so ops that actually did arrive before the drop aren't applied twice.
4. If the client is too far behind (the server's recent-op window or retained log no longer covers its `baseRev`), the server sends a **fresh snapshot**, and the client rebases or re-applies its local changes on top.

This is the "**sync from cursor, hints versus truth**" principle from chat and Dropbox again.

### Long offline sessions
- The client keeps editing against its **local replica**, persisted in **IndexedDB / local storage** so a tab crash or browser restart doesn't lose work.
- **CRDTs shine here:** merging a replica that diverged for days is the native operation. **OT** must transform a long queue of ops against a long server history (expensive, and error-prone for very large divergences), so systems often cap offline duration or merge heuristically.
- **Conflicts of *intent*** (you rewrote a paragraph offline, a colleague deleted it) can't be perfectly resolved by any algorithm. Show a **review UI** or a preserved copy for large divergences, rather than silently dropping content.
- **Trade-off:** generous offline support requires larger client-side state and a more careful merge, and strict online-only editing is simpler and safer for structured documents.

### Undo/redo (the subtle feature)
- **Global undo is wrong** in collaboration: pressing Ctrl+Z must undo **your last change**, not the last change anyone made.
- **Mechanism:** each client keeps its own **undo stack of its own operations.** To undo, compute the **inverse** of your op and **transform it over everything that has happened since**, then apply it as a *new* operation (undo is just another edit, so it syncs normally).
- **Edge cases:** if someone else deleted the text you're trying to undo the insertion of, the inverse becomes a no-op. "Selective undo" is genuinely hard and a classic source of bugs. In CRDTs, undo similarly operates by **toggling** or **inverting** your own ops (Yjs has an UndoManager scoped by origin).
- **Trade-off:** per-user undo is correct but more complex than a global stack.

---

## 10. Rich text, comments, suggestions, and other structure

- **Document model:** a **tree** (blocks → paragraphs → inline spans) with **attributes** (bold, link, font). Operations include `insert text`, `delete`, **`format range`** (apply attribute to a range), and structural ops (split/merge paragraph, insert table row, move block).
- **Formatting conflicts:** two users bolding and un-bolding the same range concurrently need a deterministic rule (usually **last-writer-wins per attribute**, ordered by `rev` or a logical timestamp). Range formatting is a major source of OT transform complexity, and in CRDTs it is modeled with **format marks** anchored to character IDs (Peritext-style) to preserve intent when text is inserted at span boundaries (does text typed at the end of a bold word become bold?).
- **Tables and lists:** structural ops (insert/delete row, reorder) are the hardest cases for both families. Mitigate by modeling rows and cells with **stable IDs**, so concurrent edits to a cell don't depend on volatile indices.
- **Comments and suggestions:** **anchored to a range**, so the anchor must **move with the text** (transform anchors exactly like cursors, or anchor to character IDs). A comment whose anchored text was deleted becomes "orphaned" and resolves gracefully. **Suggestion mode** stores edits as marked insertions/deletions awaiting accept/reject, which are themselves operations on the document.
- **Spreadsheets:** cells are the unit, so most conflicts are **last-writer-wins per cell**, plus formula dependency recomputation, structural ops (insert row/column) that shift references, and the need for deterministic recalculation across clients.
- **Whiteboards/design tools (Figma):** objects with unique IDs and properties, **last-writer-wins per property** by server order, and a fractional-index scheme for z-order and sibling ordering. Simpler than text, because concurrent edits usually touch different properties of different objects.
- **Code editors (VS Code Live Share, Replit):** plain text sequence plus language services, usually CRDT or OT on text with cursors and selections.

---

## 11. Permissions, security, and E2EE

- **ACL checks on connect and per message** (cached with short TTL and explicit invalidation). **Revocation must take effect promptly:** when a collaborator is removed or demoted, the owner **closes or downgrades their sessions** immediately and rejects further ops. Stale permission caches are a security issue, so treat them like the Dropbox and ticket revocation cases.
- **Roles:** view, comment, edit. A **commenter's connection can send only comment ops.** A **viewer's connection is read-only.** Enforce on the **server**, never trusting the client's UI.
- **Op validation:** the owner validates every op (schema, size limits, bounds, no references to nonexistent nodes), because a malformed or malicious op can corrupt shared state for everyone. In a CRDT, a buggy or hostile client can **inject operations that bloat or corrupt** the structure, so servers validate and rate-limit.
- **Link sharing:** unguessable tokens, optional expiry and password, view-versus-edit, and revocation.
- **Encryption:** TLS in transit, encryption at rest with envelope keys (per-document keys enable crypto-shredding).
- **End-to-end encryption:** the server sees only ciphertext ops, so it **cannot transform (OT), validate, search, index, or generate previews**, and persistence and merge must happen on clients. **CRDTs fit E2EE far better than OT** (the server just relays and stores opaque, commutative updates), which is a real argument for CRDTs when E2EE is a requirement. Costs: no server-side search, no server-side conflict resolution, key distribution for every collaborator, and harder recovery.
- **Abuse and DoS:** limits on document size, op rate, number of collaborators, and cursor spam. Malware/phishing scanning on shared links is async and best-effort.
- **Audit:** log permission changes, shares, exports, and access (needed for enterprise).

---

## 12. Hop by hop: reliability and trade-offs

### 1. Client
- **Local-first:** edits apply instantly, and persist locally (IndexedDB) so a crash doesn't lose unsynced work. Show a clear **sync status** ("Saving…", "Saved", "Offline, changes will sync").
- **Never block typing on the network.** Retry with exponential backoff and jitter to avoid reconnect storms.
- **Trade-off:** local-first needs a robust merge path and more client code, but it is what makes the product feel instant and resilient.

### 2. Gateways and load balancing
- **Stateful connections** at scale (the chat gateway design): L4 balancing, heartbeats, rate limits, **gradual drain on deploys with jittered reconnects** (a rolling restart that drops millions of editors simultaneously causes a reconnect herd).
- Gateways are **thin**: auth, framing, and forwarding to the owner. They hold no document logic.

### 3. Session owner (the stateful core)
- **Failure of an owner** is the key scenario: the lease expires, another node acquires it with a **higher epoch**, **loads the snapshot and replays the log tail**, and clients reconnect and **resync from their last `rev` and resend pending ops** (deduped by `opId`). The user sees a brief "reconnecting…" blip and loses nothing, because acked ops were already durable and unacked ops were retained by the client.
- **Memory limits:** cap document size and concurrent editors per owner, and **shed** by refusing new editors on pathological documents (a 50 MB doc or 500 concurrent editors) rather than letting one document degrade a node. Large documents can be pinned to dedicated nodes.
- **Hot-document isolation:** bulkhead per document (CPU/memory caps) so one runaway doc can't starve neighbors.
- **Trade-off:** single-writer ownership gives trivial ordering and fast in-memory transforms, at the cost of **a short failover pause per owner failure** and the need for leases and fencing.

### 4. Op log store
- Quorum writes (W=2 of 3 across AZs), partitioned by `docId`, with **epoch fencing** on append. **Durability before ACK.**
- **If the log store is unavailable:** the owner **stops ACKing**, clients keep editing locally and buffering, and the UI shows "Offline/Reconnecting." No data is lost, and sync resumes on recovery. **Availability degrades into offline mode, not failure.** That's a major benefit of the local-first design.

### 5. Coordination store (leases)
- Small, consensus-backed (etcd/ZooKeeper-style), **off the per-keystroke path** (touched at owner acquisition and renewal only). If it is briefly unavailable, existing owners keep serving until their lease expires (renew early and with a safety margin), and **no new documents can be opened or failed over** until it recovers. This is static stability.

### 6. Async consumers (snapshots, search, notifications, history)
- Kafka consumers with at-least-once delivery and idempotent handlers (keyed by `(docId, rev)`). **A lag here never affects editing**, only the freshness of derived features.

### 7. Presence tier
- Best-effort, in-memory, TTL-based, and **the first thing to degrade** under load.

---

## 13. Failure scenarios

| Failure | Impact | Mitigation |
|---|---|---|
| Client disconnects mid-edit | Pending ops unacknowledged | Local buffer + IndexedDB, reconnect with `rev` and pending ops, **dedupe by `opId`**, rebase |
| Session owner node crashes | Doc unavailable briefly | Lease expiry, new owner loads snapshot + log tail with higher epoch, clients resync, **no acked data lost** |
| **Zombie owner** (paused/partitioned, then wakes up) | Two writers → divergent order | **Epoch fencing** at the log store rejects stale writes, old owner steps down |
| Op log store down/slow | Can't commit ops | Stop ACKing, clients keep editing locally (offline mode), resume and resend on recovery |
| Coordination store down | Can't acquire/transfer ownership | Existing owners continue, safety-margin lease renewal, alerts, new opens retry |
| Duplicate op delivery (retry) | Double-applied edit | Idempotent `opId` dedupe (and CRDT idempotence by construction) |
| Out-of-order or concurrent ops | Divergence | OT transform against committed ops / CRDT commutativity, deterministic ordering |
| **Transform/merge bug** (OT) or **CRDT bug** | **Permanent divergence between replicas** | Property-based and fuzz testing of convergence, periodic **state hash checks** across clients and server, auto-resync from snapshot on mismatch |
| Very large document / pathological history | Slow load, memory pressure | Snapshots, size caps, tombstone GC, dedicated capacity, paginated rendering |
| Reconnect storm (deploy/outage) | Herd on gateways, owners, log store | Jittered backoff, connection-rate caps, gradual drain, staged recovery, shed presence |
| Hot document (thousands of viewers) | Owner overwhelmed by sockets | Broadcast tier for viewers, cap editors, sample presence |
| Permission revoked while editing | Unauthorized edits continue | Owner closes/downgrades sessions on ACL change, short permission cache TTL, server-side enforcement |
| Malicious or buggy client ops | Corrupt shared state | Server-side op validation, rate limits, size limits, quarantine and rollback via history |
| Client far behind / clock or version mismatch | Can't rebase | Fall back to snapshot resync, protocol version negotiation, force-refresh |
| Snapshot corruption | Can't load doc | Snapshots are derived: rebuild from the op log, checksum, keep multiple recent snapshots |
| Accidental mass deletion or vandalism | Content lost | Op log history, version restore (as a new op), per-user revert, anomaly detection on bulk deletes |
| Compaction bug | History or state lost | Write-verify-delete, keep pre-compaction copy for a grace period, validate replay equivalence |
| Region outage | Docs in region unavailable | Fail over ownership to another region (replicated log), clients keep local state and resync |
| Bad deploy of the collaboration service | Mass editing failures | Canary by doc cohort, protocol compatibility, kill switch, rollback, **never corrupt the log** |

**Principle (never lose a user's keystrokes, and degrade into offline mode):** under any backend failure the editor **keeps accepting local edits and syncs later.** The server **refuses to ACK anything it hasn't made durable**, **fences stale owners**, and **can always rebuild state from the op log.** The worst realistic outcome is a short "reconnecting" state, never lost or diverged text.

---

## 14. Consistency decisions

- **Convergence:** **strong eventual consistency.** All replicas that have seen the same set of operations are in the same state, guaranteed by OT correctness or CRDT mathematics.
- **Ordering:** a **total order per document** by `rev` (server-sequenced), with no ordering across documents (not needed).
- **Durability:** an acknowledged op is **persisted at quorum before the ACK** and before broadcast.
- **Local view:** each client is **immediately consistent with its own edits** (read-your-writes by construction) and **eventually consistent with others'** (sub-second).
- **Intention preservation:** a best-effort property defined by the algorithm's semantics (e.g., concurrent bold and insert resolve predictably). No algorithm can resolve every semantic conflict.
- **Permissions and revocation:** enforced promptly, a security-sensitive exception to eventual consistency.
- **Presence:** ephemeral, best-effort.
- **Derived data (search, previews, comment indexes, version groupings):** eventually consistent, rebuildable from the log.
- **History:** append-only and immutable, with restores recorded as new operations.

---

## 15. Multi-region

- **Home region per document** (typically where the creator or majority of editors are), which hosts its session owner and primary log, with **synchronous replication across AZs** and **asynchronous replication to a DR region.** One writer region avoids split-brain ordering.
- **Edge termination:** users connect to the **nearest edge**, which proxies over the backbone to the home region. Cross-region RTT (~50-150 ms) adds to **remote collaborators' perceived latency, but not to the typist's own latency** (local-first), which is the key point that makes a single-home design acceptable.
- **Migration of ownership:** if most active editors move to another region (a team across the globe), **hand off ownership** to the region that minimizes collaborator latency, using a lease transfer with a fencing epoch.
- **Active-active** is natural **only with CRDTs** (any region can accept edits and merge later), at the cost of metadata overhead and loss of a single authoritative order (history and permission enforcement become harder).
- **Failover:** promote the DR replica (accepting replication lag as RPO, e.g., seconds). Clients retain unacked ops locally and **resend after reconnect**, so most or all of the lag window is recovered by client replay, since ops are idempotent.
- **Data residency:** pin documents to allowed regions, which constrains ownership migration and DR choices.

---

## 16. Observability and operations

- **Editing SLIs:** **op commit latency** (receive to durable ACK), **remote propagation latency** (author to other clients), reconnect rate, resync-from-snapshot rate, pending-op queue depth on clients, session-owner failover time, and active documents per owner.
- **Correctness SLIs:** **convergence checks** (clients periodically send a hash of their document state at a given `rev`, and the server compares them), transform/merge error counts, and invalid-op rejection rate. A hash mismatch triggers an automatic resync and an alert, because it means a convergence bug in production.
- **Capacity SLIs:** memory per owner, document size distribution, concurrent editors per doc, connection counts per gateway, log append latency, snapshot lag.
- **Alerts:** rising commit latency, owner failover spikes, convergence mismatches, reconnect storms, op-log write errors, and snapshot or compaction failures.
- **Tooling:** a document-level debugger (replay ops, diff replicas), **time-travel from the op log**, admin repair (rebuild from log, force snapshot), kill switches (disable realtime, force read-only for a doc or tenant), and a "why did this text change?" attribution view.
- **Testing:** **property-based and fuzz tests** with random concurrent ops, random delivery order, duplication, and delay (the single most valuable testing investment for OT/CRDT), deterministic simulation of many clients, chaos tests (kill owners, partition networks, delay the log store), and long-offline-merge tests.

---

## 17. Scaling summary

| Component | Strategy |
|---|---|
| Convergence | Server-sequenced OT or a sequence CRDT (Yjs-style), property-based tested, state-hash verification |
| Sessions | One lease-owned, epoch-fenced owner per active doc, consistent hashing by `docId`, in-memory state, idle eviction |
| Connections | Stateful thin gateways, L4 balancing, jittered reconnect, gradual drain, broadcast tier for viewers |
| Persistence | Op log (quorum, RF=3, by `docId`) + periodic snapshots in object storage, replay for recovery |
| History | Derived from the log, grouped into revisions, restore as a new op, compaction with write-verify-delete |
| Presence | In-memory, TTL, throttled, droppable first |
| Offline | Local replica in IndexedDB, rebase on reconnect, snapshot fallback for stale clients |
| Async features | Kafka consumers (search, notifications, previews, comment indexing), idempotent |
| Security | Server-side ACL and op validation, prompt revocation, envelope encryption, optional E2EE (favors CRDT) |
| Isolation | Per-document bulkheads, size and editor caps, dedicated nodes for huge docs |
| Regions | Home-region ownership, edge proxying, async DR replication, ownership migration, client replay to cover RPO |

---

## 18. Likely follow-up questions

- **"Why can't you just use last-write-wins or locks?"** LWW destroys one user's work, and locks kill real-time collaboration and break when a lock-holder disconnects. You need a mechanism that merges concurrent edits and preserves both.
- **"Explain OT in one example."** Two users edit `"abc"` concurrently: one inserts at position 0, the other deletes position 2. Applying the second op unchanged after the first would delete the wrong character, so the server **transforms** the delete to position 3, and both replicas converge.
- **"Why is server-centric OT easier than peer-to-peer OT?"** A central server linearizes operations, so you only need pairwise transformation (TP1) against a single history, and you avoid the hard TP2 property required for decentralized OT.
- **"OT or CRDT?"** OT is compact and proven but needs a central sequencer and correct, complex transform functions. CRDTs converge by construction, suit offline and P2P and E2EE, but carry per-character metadata and need tombstone GC. I'd pick by constraints: offline or E2EE favors CRDT, and tight memory with a server-authoritative design favors OT.
- **"What are tombstones and why do they matter?"** Deleted characters kept as markers so concurrent inserts anchored to them still resolve consistently. They bloat state until garbage collected once all replicas have seen the deletion.
- **"How do you scale this?"** Per-document independence. Partition by `docId`, one owner per active document, and add nodes. Per-document load is small, and aggregate scale is just many independent documents plus a stateful connection tier.
- **"What if the session owner dies mid-edit?"** Its lease expires, a new owner loads the latest snapshot and replays the log with a higher epoch, clients reconnect, resync from their last `rev`, and resend unacked ops, which are deduped by `opId`. Acked ops were durable already, so nothing is lost.
- **"How do you prevent two servers both owning a document?"** Leases plus **fencing epochs** enforced by the op log store, so a stale owner's writes are rejected.
- **"How does offline editing work?"** The client keeps a local replica and queue (persisted locally), and on reconnect rebases or merges. CRDTs merge naturally, and OT transforms the queue over server history. Very long divergences fall back to snapshot resync and a review UI.
- **"How does undo work?"** Per-user undo stacks. Compute the inverse of your own op, transform it over the changes since, and apply it as a new op. Global undo would revert other people's work.
- **"How do you handle cursors and comments when text changes?"** Transform cursor and comment anchors through each operation (OT), or anchor them to stable character IDs (CRDT), so they move with the content.
- **"How do you keep version history?"** The op log is the history. Group ops into revisions asynchronously, rebuild any state by replaying from the nearest snapshot, and implement restore as a new operation.
- **"How do you handle a document with 5,000 viewers?"** Separate viewers from editors. A broadcast/pub-sub tier fans out ops to viewers, presence is sampled, and the owner only handles editors.
- **"How would E2EE change this?"** The server can't transform, validate, or search ciphertext, so merging moves to clients, which favors CRDTs. You lose server-side search and previews and take on key-distribution complexity.
- **"How do you test correctness?"** Property-based fuzzing of concurrent operations under random delivery order, duplication, and delay, asserting convergence, plus runtime state-hash comparisons across clients.
- **"How is a spreadsheet or Figma different?"** Cells or objects with stable IDs and last-writer-wins per property cover most conflicts, so the hard text-sequence merging is mostly avoided, but structural operations (row insert, reparenting) and formula dependencies add their own difficulties.
- **"Where is the latency budget spent?"** The typist sees zero network latency (local apply). Collaborators see client batch (~50-100 ms) + network + owner transform/append (~5-10 ms) + broadcast + network, so ~150-400 ms end to end.

---

## 19. How to narrate it

1. **Name the dominant problems:** convergence of concurrent edits without locks, a stateful low-latency session tier, and keeping the editor usable through failures. Storage and per-document throughput are small.
2. **Start naive:** save-the-whole-file with last-write-wins, then locks. Show the lost-edit and blocking problems. Introduce **operations** instead of snapshots, show why absolute positions break under concurrency, then bring in **OT** (transform) or **CRDT** (commutative structure), compare them, and pick one with reasons.
3. **Build the architecture around the choice:** one leased, fenced owner per document, a durable op log with snapshots, local-first clients with a pending buffer, then presence, offline, undo, and history.
4. **State the contracts:** persist before ACK, total order per document by `rev`, idempotent ops by `opId`, strong eventual convergence, local read-your-writes, and server-enforced permissions with prompt revocation.
5. **Call out the knobs:** op batching interval, snapshot frequency, idle-eviction timeout, max editors and document size, presence throttle, compaction and retention windows, lease duration and renewal margin, and offline-merge limits.
6. **For each box, say what happens when it fails,** and end with "any server can die or the whole backend can go down, and the user keeps typing locally. Acknowledged edits are durable, unacknowledged ones are replayed idempotently, and every replica converges."

**Patterns reused from our toolbox:** single-writer-per-partition with leases and fencing epochs (chat sequencing, Uber dispatch, ticket booking, KV store), a per-entity monotonic sequence as the sync backbone (chat `seq`, Dropbox namespace journal), persist-before-ack and idempotency keys (payments, chat, notifications), sync-from-cursor on reconnect with real-time as a hint (chat, Dropbox), stateful WebSocket gateways with jittered reconnect and gradual drain (chat, Uber), append-only log plus periodic snapshots and write-verify-delete compaction (Dropbox, metrics TSDB, Kafka), optimistic concurrency and local-first with rebase (Dropbox commits, `baseRev`), ephemeral last-write-wins state for presence (Uber location), fan-out tiering for hot read-heavy objects (feed celebrities, video CDN), per-document bulkheads and quotas (rate limiter, Kafka), async derived consumers via Kafka with idempotent handlers (feed, video, notifications), immutable history and restore as a new write (Dropbox versions), server-side validation and prompt revocation (Dropbox sharing, ticket booking), consensus only for small metadata (leases) off the hot path (KV store, Uber), and graceful degradation into offline mode with static stability.

---

That completes the series: the applied systems (URL shortener, feed, Top-K, chat, rate limiter, crawler, Uber, Dropbox, video, notifications, payments, ticket booking, typeahead, metrics, collaborative editing) and the building blocks (cache/KV store, message queue).

Next, I can play interviewer on this (for example, "two clients' documents have silently diverged for the past hour and a user reports missing text, walk me through detection, recovery, and prevention"), or we can do something different. A **distributed job scheduler / workflow engine** is the one remaining classic that reuses leases, fencing, and exactly-once-ish execution. I can also put together a **cross-cutting review**: a one-page pattern cheat sheet, a "which pattern for which problem" decision guide, and a mixed mock-interview round that pulls questions from several of these designs.
