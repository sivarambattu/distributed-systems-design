# Payment System Design

**What to say early:** Almost every earlier design chose availability and eventual consistency. Payments reverse that. **QPS is low (hundreds to a few thousand per second), and correctness is everything.** The cardinal sins are charging twice, losing a payment, and creating or destroying money. The design philosophy is: **be correct first, then available, then fast.**

Four ideas organize the whole answer:

1. **You don't hold the money. External systems do** (card networks, issuing banks, PSPs). Every call to them can time out in an *unknown* state, and the system is mostly about handling that uncertainty.
2. **Idempotency everywhere,** so retries are always safe.
3. **An immutable double-entry ledger** as the internal source of truth for money movement.
4. **Reconciliation** against external records, because you can't trust that your view matches theirs.

Also state the scope. Two common interpretations are:
- **Payment gateway / PSP (Stripe-like):** serve many merchants, route to acquirers, and be the platform others build on.
- **A company's payment service (Amazon/Uber-like):** take payments from customers via an existing PSP, record them, and pay out sellers or drivers.

I'll design the second, with a PSP integration, and note where the first differs.

---

## 1. Requirements

**Functional**
- **Pay-in:** charge a customer (card, bank transfer, wallet) for an order
- **Authorize, then capture** (hold funds, then take them on shipment) or one-step sale
- **Refunds** (full and partial), voids, and cancellations
- **Pay-out:** disburse money to sellers/drivers (marketplace case)
- Payment methods vault (saved cards) via tokens
- Payment status, history, receipts, and webhooks to merchants
- Multi-currency and fees/taxes
- Handle **3-D Secure/SCA** authentication challenges
- Fraud checks, dispute and chargeback handling

**Non-functional**
- **No double charges and no lost payments.** Exactly-once *effect*.
- **Strong consistency** for money state and the ledger
- **Durability:** every accepted request and every financial event is permanently recorded and auditable
- **Availability:** checkout is revenue, so high availability matters, but never at the cost of correctness
- **Latency:** p99 of a few seconds is acceptable, since the PSP and issuer dominate it
- **Security and compliance:** PCI DSS, PSD2/SCA, encryption, and audit trails
- **Auditability and reconcilability** against banks and PSPs

**Clarify with the interviewer:** Pay-in only, or payouts too? Single PSP or multiple? Which payment methods and currencies? Do we store card data (a huge compliance decision)? Marketplace (multi-party split)? Expected scale and peak (Black Friday)?

---

## 2. Estimation (state assumptions)

- 10M payments/day ≈ **~120/sec average, ~1-3K/sec peak** (flash sales)
- Each payment produces ~5-10 ledger entries, state transitions, and events → **~100M ledger rows/day**
- Ledger row ~200 B → **~20 GB/day**, ~7 TB/year. This is easily a sharded relational DB, with older partitions archived.
- Payment records ~2 KB × 10M = **20 GB/day**
- PSP latency: authorization takes ~0.5-3 s (more with 3DS). At 3K/sec peak and ~2 s each, that's **~6K concurrent in-flight PSP calls**, a connection-pool problem.
- Reconciliation files: millions of rows per day per PSP, a batch job.

**Takeaway to say aloud:** This is a **low-volume, high-stakes** system. You can afford relational transactions, synchronous replication, and conservative design. The difficulty is not scale. It's **correctness under partial failure with third parties.** Resist reaching for exotic distributed tech. A boring, strongly consistent relational DB with careful protocols is the right answer.

---

## 3. API and data model

```
POST /v1/payments
  Headers: Idempotency-Key: <uuid>
  { orderId, amount: 4999, currency: "USD", paymentMethodToken, captureMode: auto|manual,
    metadata, returnUrl }
  → 201/202 { paymentId, status: REQUIRES_ACTION | PROCESSING | AUTHORIZED | SUCCEEDED | FAILED, nextAction? }

POST /v1/payments/{id}/capture   { amount? }
POST /v1/payments/{id}/refunds   { amount, reason }      (Idempotency-Key)
POST /v1/payments/{id}/cancel
GET  /v1/payments/{id}
POST /v1/payment-methods         (client → vault directly; we get a token)
POST /v1/payouts                 { recipientId, amount, currency }  (Idempotency-Key)
POST /v1/webhooks/psp/{name}     (inbound events from PSP; signature-verified)
```

**Money rules (interviewers listen for these):**
- **Amounts are integers in minor units** (cents), with an explicit **currency** code. **Never floats.** Mind currencies with 0 or 3 decimals (JPY, KWD).
- Every request carries an **idempotency key**.
- Return `202`/`PROCESSING` honestly when the outcome isn't known yet, rather than guessing.

| Entity | Shape | Store |
|---|---|---|
| **Payment** | `paymentId → orderId, customerId, amount, currency, status, version, pspName, pspRef, createdAt` | Strongly consistent relational DB, sharded by `paymentId` or `merchant/customer` |
| **Payment attempt** | `(paymentId, attempt#) → provider, providerRequestId, result, errorCode, ts` | Same DB, append-only |
| **Idempotency record** | `(clientId, idempotencyKey) → requestHash, paymentId, responseSnapshot, state, expiresAt` | Same DB (same transaction as the payment) |
| **Ledger entries** | `(entryId, txnId, accountId, direction D/C, amount, currency, ts)`, immutable, append-only | Relational, partitioned by time and account |
| **Ledger accounts** | `accountId → type (customer, merchant, fees, PSP-clearing, revenue...), currency, balance (derived/cached)` | Relational |
| **Payment methods** | `methodId → customerId, tokenRef, brand, last4, expiry, fingerprint` | DB (no raw PAN). Raw card data lives in the **vault**. |
| **Outbox** | `(eventId, aggregateId, type, payload, publishedAt)` | Same DB as the payment |
| **Webhook inbox** | `(provider, eventId) → payload, processedAt` | Same DB, unique key for dedupe |
| **Reconciliation records** | `(provider, settlementId, line) → matched/unmatched` | DB/data lake |

**Key design points:**
- **Payment state and its idempotency record, and the outbox entry, commit in the same DB transaction** (the transactional outbox, again). That kills the dual-write problem.
- **Raw card data is isolated** in a PCI-scoped vault. The rest of the system only ever sees tokens.

---

## 4. High-level architecture

```
Client/Checkout (card entry via PSP-hosted fields / iframe → card data NEVER touches our servers)
      │ token
      ▼
API Gateway (authn, rate limit, schema validation, idempotency-key required)
      ▼
Payment Service (stateless; the orchestrator)
   1. idempotency check (conditional insert)           ┐ one DB
   2. create payment (state = CREATED), fraud pre-check │ transaction
   3. risk/fraud decision (sync, tight timeout)         ┘
   4. call PSP adapter (auth/capture) w/ our request-id as PSP idempotency key
   5. persist result + ledger entries + outbox event (one transaction)
   6. respond
      │
      ├──► PSP Adapters (Stripe/Adyen/etc. + fallback PSP) ──► Card networks ──► Issuer banks
      │            ▲ webhooks (async truth)
      ▼            │
Payments DB (strong, sync-replicated, sharded) ◄── Webhook Receiver (verify sig, dedupe, apply)
      │ outbox relay
      ▼
Kafka: payment.events ─► Order Service · Notifications · Merchant webhooks · Fraud/ML ·
                          Analytics/Data lake · Ledger projections

Ledger Service (double-entry, append-only) ◄── posts entries from payment events/transactions
Recovery/Sweeper: finds payments stuck in PROCESSING/UNKNOWN → queries PSP → resolves
Reconciliation: PSP settlement files + bank statements ⇄ internal ledger → exceptions queue
Vault/Token service (PCI scope) · Payout Service · Dispute Service · Reporting
```

---

## 5. Core deep dive 1: idempotency (the heart of the problem)

**The scenario:** the customer clicks "Pay," the request reaches your server, the server calls the PSP, the PSP charges the card, and then **the response is lost** (timeout, crash, mobile network). The client retries. Without protection, **the customer is charged twice.**

### Idempotency key protocol
- The client generates a unique **`Idempotency-Key`** per *logical* operation (not per HTTP attempt) and reuses it on every retry.
- On receipt, the server does a **conditional insert** of `(clientId, key)` with a hash of the request body **in the same transaction that creates the payment:**
  - **Key not seen:** proceed.
  - **Key seen, same request hash, completed:** **return the stored response** without re-executing.
  - **Key seen, in progress:** return `409` or `202` (client waits and retries), since you can't start a second execution.
  - **Key seen, different request body:** reject with `422`. Reusing a key for a different request is a client bug and must not silently succeed.
- Keys are retained for a window (24 h to days) and then expire. Document this.

### Idempotency at every hop (not just the API)
It's a chain, and each link needs its own protection:
1. **Client → us:** the idempotency key above.
2. **Us → PSP:** pass **our own deterministic request ID** as the PSP's idempotency key (derived from `paymentId + attempt`), so *our* retry of the PSP call can't double-charge.
3. **PSP → us (webhooks):** at-least-once, duplicated, and reordered. Dedupe on `(provider, eventId)` and make state transitions monotonic.
4. **Internal consumers (Kafka):** at-least-once, so every consumer dedupes on `eventId` or applies idempotent writes.
5. **Ledger posting:** keyed by `(paymentId, entryType)` with a unique constraint, so a replay can't post twice.

### Why a unique constraint beats application logic
"Check if exists, then insert" races under concurrency (two retries arrive simultaneously). Use the **database's unique constraint** or `INSERT ... ON CONFLICT` as the arbiter. It's the one primitive that is correct under concurrency.

**Trade-offs:**
- Storing the full response for replay gives exact repeat semantics, but costs storage and must avoid keeping sensitive data longer than needed.
- Short key retention saves space, but a retry after expiry would create a new payment. For payments, retention of at least 24 hours, ideally longer, is standard.
- Exactly-once *delivery* is impossible. **At-least-once + idempotency = exactly-once *effect*.** Say this sentence.

---

## 6. Core deep dive 2: the payment state machine and the "unknown" outcome

### State machine
```
CREATED → PROCESSING → REQUIRES_ACTION (3DS) → AUTHORIZED → CAPTURED/SUCCEEDED → (REFUNDED / PARTIALLY_REFUNDED)
              │                                     │
              ├─► FAILED (declined)                 └─► VOIDED/CANCELLED
              └─► UNKNOWN / PENDING_VERIFICATION  (timeout, ambiguous)
                                             └─► DISPUTED (chargeback)
```
- Every transition is a **conditional update on `version`** (compare-and-set), so concurrent or stale writers can't regress state or apply two terminal outcomes.
- **Illegal transitions are rejected** (you can't capture a failed payment). Terminal states are final, except for refund and dispute flows.
- **Every transition is recorded** in an append-only history for audit.

### The hard part: timeouts mean "I don't know"
When the PSP call times out, **you don't know whether the card was charged.** Both obvious answers are wrong:
- Marking it **FAILED** may mean you charged the customer but told them it failed. They retry (if the idempotency key is lost) or the order is canceled, and now **you've taken money for nothing.**
- Marking it **SUCCEEDED** may mean you shipped goods that were never paid for.

**Correct handling:**
1. Record the attempt **before** calling the PSP (`PROCESSING`, with `providerRequestId`). Durable intent first.
2. On timeout, set state **`UNKNOWN`/pending verification.** Do not guess.
3. **Resolve by asking the PSP:** query by our idempotency key or `providerRequestId` (retrying the *same* idempotent call also returns the original result), and/or wait for the **webhook.**
4. A **recovery sweeper** scans for payments stuck in `PROCESSING/UNKNOWN` beyond a threshold and drives them to a terminal state. This makes the orchestrator **crash-safe,** like the dispatch sweeper in the Uber design: the DB, not process memory, holds the truth.
5. If it's truly unresolvable (PSP lost it), the final backstop is **reconciliation** (section 8), and a **void/refund** if a charge turns up that the order doesn't want.

**Respond to the client honestly:** `PROCESSING` with a way to poll or receive a webhook ("we're confirming your payment"), instead of an optimistic success or failure. UX matters, but correctness wins.

### Webhooks are the asynchronous truth
Many payment methods are inherently async (bank transfers, wallets, 3DS, delayed card confirmations), so the synchronous response is not the whole story.
- **Verify signatures** and reject unauthenticated callbacks.
- **Persist first, then process** (inbox table with a unique `(provider, eventId)`), and ack the PSP fast. Process asynchronously and idempotently.
- Handle **out-of-order events** (`refund.succeeded` before `charge.succeeded`) by treating them as facts to merge into monotonic state, not commands to execute in arrival order. For critical transitions, **re-fetch the object from the PSP API** instead of trusting the payload alone.
- **Never rely on webhooks alone:** a lost webhook means a stuck payment, so the sweeper polls as backup. This is the same **hint vs. truth** principle as chat and Dropbox.

### Authorize vs. capture
- **Auth + capture:** hold funds at order time, capture on shipment (auth holds expire, typically ~7 days for cards). Good for physical goods and "pay on delivery of service." **Cost:** you carry expiry and re-authorization logic, and partial captures.
- **Single-step sale:** simpler, for digital goods. **Trade-off:** refunds are the only remedy if you can't fulfill.

### 3-D Secure / SCA
The PSP may return `REQUIRES_ACTION` with a redirect or challenge. The payment parks in that state, the customer authenticates, and the result returns via redirect and **webhook.** Handle abandonment with a **timeout that voids** the pending payment. **Trade-off:** SCA reduces fraud and shifts liability, but adds friction and drop-off.

---

## 7. Core deep dive 3: the ledger (double-entry bookkeeping)

### Why not just a `balance` column?
`UPDATE accounts SET balance = balance - 50` is **lossy and unauditable.** You can't answer "why is this balance wrong?", concurrent updates race, there's no history, and bugs destroy evidence. Money systems use **double-entry accounting.**

### Double-entry ledger
- Every financial event is a **transaction** composed of **entries** that debit one account and credit another, and **the sum of debits equals the sum of credits** for every transaction. Money never appears or disappears, it only moves.
- **Entries are immutable and append-only.** Mistakes are fixed by **new compensating entries** (reversals), never by editing history.
- **Balances are derived** (sum of entries), with cached or materialized balances as an optimization, periodically verified against the entries.

**Example: customer pays $100 (platform fee 10%, via PSP):**
```
Txn 1 (charge succeeded):
   DR  PSP-clearing (asset, money owed to us by the PSP)   100.00
   CR  Merchant-payable (liability)                          90.00
   CR  Platform-revenue (fees)                               10.00
Txn 2 (PSP settles to bank, 3 days later; minus PSP fee 3.20):
   DR  Bank (asset)                                          96.80
   DR  PSP-fees (expense)                                     3.20
   CR  PSP-clearing                                         100.00
Txn 3 (payout to merchant):
   DR  Merchant-payable                                      90.00
   CR  Bank                                                  90.00
```
**Invariants** to state aloud: each transaction balances to zero, the sum of all accounts is zero, and the **clearing accounts converge to zero** once everything settles. Any non-zero clearing balance after settlement is an exception to investigate.

### Design details
- **Write the ledger in the same DB transaction as the payment state change** (or via the outbox into a ledger service with idempotent posting keyed by `(paymentId, eventType)`). **Trade-off:** same-DB posting is simplest and atomic, but couples the ledger's scaling and ownership to the payment service. A **separate ledger service** gives cleaner ownership and reuse (payouts, refunds, billing) at the cost of eventual consistency between payment state and ledger, so you need idempotent posting and a reconciliation check between the two. At this volume, **start with one DB and split when ownership demands it.**
- **Concurrency on hot accounts:** a platform-revenue account receives an entry from every payment, creating a **hot row** if you maintain a running balance with `UPDATE ... SET balance = balance + x`. Fixes: don't maintain balances on the write path (derive or batch-update them), use **sharded sub-accounts** (the sharded counter trick), or aggregate asynchronously. **Trade-off:** instantly accurate balances versus write throughput. For merchant-facing "available balance" with overdraft protection, you need a **consistent check** on that one merchant's account (row-level lock or conditional update) and can accept per-merchant serialization, since a single merchant's rate is modest.
- **Multi-currency:** accounts are per-currency, and a conversion is a transaction that debits one currency account and credits another, plus an FX gain/loss entry. **Record the FX rate used** with the transaction. Never silently mix currencies.
- **Time and immutability:** store both event time and posting time. Close accounting periods and forbid back-dated edits (corrections go in the current period with references).
- **Rounding:** define rounding rules and where the remainder goes (e.g., splitting $0.01 three ways), and test them.

**Trade-off summary:** a double-entry ledger costs more rows and design discipline than a balance column, but it gives **auditability, error detection (the books must balance), reproducibility, and the ability to rebuild state.** In finance this is non-negotiable.

---

## 8. Core deep dive 4: reconciliation (trust, but verify)

**Why:** your records and the PSP's/bank's records will *disagree* sometimes (lost webhooks, timeouts that actually succeeded, PSP bugs, disputes, fee changes, manual adjustments). Reconciliation is how you **detect and repair** that, and it's the final safety net behind every other mechanism.

**Process (a nightly or continuous batch job):**
1. Ingest **PSP settlement/transaction reports** and **bank statements.**
2. **Match** each external line to an internal payment/ledger transaction by provider reference ID, amount, and currency.
3. Classify results:
   - **Matched:** fine.
   - **In PSP, not in our system:** we charged someone and lost the record (or a crash window), so create the record or **refund/void.**
   - **In our system, not in PSP:** we think it succeeded, but it didn't, so fix our state and notify the order system.
   - **Amount mismatch:** fees, FX, partial captures, or a bug.
4. Push exceptions to an **investigation queue** with tooling for ops, and auto-fix well-understood classes.
5. **Three-way check:** internal ledger ⇄ PSP report ⇄ bank deposit.

**Properties:** reconciliation is **idempotent and re-runnable,** and operates on **immutable inputs.** It also feeds **monitoring** of unexplained drift. **Trade-off:** reconciliation is lagging (hours to days) and heavy, so it's a safety net, not a replacement for the real-time protocol. Real-time correctness reduces exceptions, and reconciliation catches whatever escapes.

---

## 9. Hop by hop: reliability and trade-offs

### 1. Client and card entry (security first)
- **Card data should never touch your servers.** Use **PSP-hosted fields/iframes or SDKs** that tokenize the card client-side and hand you a **token.** This dramatically shrinks **PCI DSS scope** (SAQ A versus the full burden).
- **Trade-off:** hosted fields limit UI control, but the compliance and breach-risk reduction is overwhelming. Storing raw PANs yourself is justified only at massive scale, with a dedicated, isolated, audited vault (tokenization service, HSM-backed encryption, strict access controls, no PAN in logs, ever).

### 2. API gateway
- Authn/authz, rate limiting (also anti-**card-testing**: attackers run stolen card numbers through your checkout to see which work, so limit per IP, device, and card fingerprint), schema validation, **require the idempotency key.**
- TLS everywhere, request signing for server-to-server callers.

### 3. Payment Service
- **Stateless, with all truth in the DB.** Idempotency check and payment creation in **one transaction.**
- **Never hold a DB transaction open across the PSP call** (it would pin connections and locks for seconds). The pattern is: **transaction 1** (record intent: `PROCESSING`) → **call PSP outside any transaction** → **transaction 2** (record the result + ledger + outbox). A crash between them is exactly the `PROCESSING` case the sweeper recovers.
- **Timeouts and retries:** tight, bounded timeouts on PSP calls. Retry **only idempotent calls** with the same PSP idempotency key, with exponential backoff and jitter, and a **total deadline** (don't retry for minutes while the customer waits).
- **Bulkheads and circuit breakers per PSP,** so one slow provider can't exhaust all threads.

### 4. Fraud/risk check
- Sync rules plus an ML score: velocity, device and IP reputation, mismatch signals, amount anomalies, AVS/CVV results. Decision: **approve, reject, or step-up (3DS/manual review).**
- **Latency-bounded** (tens of ms). If the fraud service is down, a **per-merchant/risk-tier fail mode** (fail open with tighter limits for low-value payments, fail closed for high-risk), the same per-category stance as the rate limiter.
- **Trade-off:** aggressive fraud rules prevent losses but **reject legitimate customers** (false positives cost revenue), so tune the threshold on measured loss versus conversion.

### 5. PSP adapters and routing
- A uniform interface (`authorize`, `capture`, `refund`, `void`, `getStatus`) hides provider differences, and responses are normalized to `APPROVED | DECLINED(reason) | RETRYABLE | UNKNOWN`.
- **Multi-PSP:** route by cost, success rate, currency/region support, and payment method. **Failover to a second PSP on provider outage** or on soft-decline retry.
- **Danger in failover:** if PSP-A *timed out*, it may have *already charged*. **Never blindly retry on PSP-B** without first resolving the unknown on PSP-A (or voiding it), or you double-charge. Failover is safe on *clear failures* (connection refused, explicit rejection) and dangerous on *timeouts.* This is a favorite interview probe.
- **Smart retries / cascading:** on soft declines (issuer unavailable), retry via another route to improve **authorization rate**, as a business optimization with a strict attempt cap.
- **Trade-off:** multi-PSP improves availability and negotiating leverage, but multiplies integration cost, reconciliation formats, and compliance surface.

### 6. Webhook receiver
Covered above: authenticated, persist-first, dedupe, idempotent, monotonic, with polling backup. It must be **highly available** (PSPs retry for limited periods) and decoupled from processing, so a slow downstream doesn't cause the PSP to give up.

### 7. Outbox, Kafka, and downstream consumers
- The **transactional outbox** publishes `payment.succeeded`, `payment.failed`, `refund.created`, and so on, at-least-once. **Order, notification, merchant-webhook, analytics, and fraud-feedback consumers** are all idempotent.
- **Merchant webhooks (if a platform):** sign payloads, retry with backoff over days, expose delivery logs, and let merchants replay. The notification-system patterns apply.
- **Trade-off:** event-driven decoupling keeps the payment path small and fast, but means the order system learns the outcome *asynchronously*. The order must handle "payment pending" as a first-class state.

### 8. The database
- **Relational, strongly consistent, synchronously replicated** (at least across AZs, with automatic failover), serializable or at least snapshot-with-constraints for critical paths. **Sharded** by `paymentId` or customer/merchant when needed, but at ~3K/sec peak, a **single well-run primary with replicas can go very far**, so defer sharding.
- **Backups and PITR** are mandatory, with regularly tested restores. **Never delete financial records.** Use retention and legal-hold policies, and archive by partition.
- **Failover must not lose acknowledged commits:** synchronous replication (RPO = 0) for the payments DB, at some write-latency cost, which is entirely acceptable here.
- **Trade-off:** stronger consistency costs latency and some availability during failover, in exchange for never losing or duplicating a financial fact.

---

## 10. Refunds, payouts, disputes, and the marketplace case

### Refunds
- Idempotent (`Idempotency-Key` + a unique `(paymentId, refundRequestId)`), validate **refundable amount** (`captured - alreadyRefunded`) in a **conditional update** so concurrent refunds can't exceed the capture (**over-refund race**).
- Refunds are **async** at the PSP (and take days to appear for the customer), with their own state machine and reconciliation. Post reversing ledger entries, **never delete the originals.**

### Payouts (money out)
- **More dangerous than pay-in,** since sending money is hard to undo. Add: a **holding/delay period** (to catch fraud and chargebacks), **balance and limit checks** (available balance, daily caps), **verification of the destination** (KYC, bank-account validation), and **approval flows or anomaly checks** on large or unusual payouts.
- **Batching:** aggregate small payables into scheduled payouts to cut fees. **Idempotent payout requests** and a payout state machine with the same **unknown-outcome** handling (bank rails confirm asynchronously, sometimes days later, and can **return** a payment after "success").
- **Trade-off:** faster payouts delight sellers and increase risk, while slower payouts reduce fraud exposure but upset sellers.

### Disputes/chargebacks
- The issuer reverses the charge via the network. You receive a webhook, **freeze/debit the merchant balance** (ledger entries into a dispute-reserve account), gather evidence, and respond within a deadline. Outcomes post further ledger entries. **Track dispute rates**, because networks penalize merchants above thresholds.

### Marketplace splits
Model each party's share as ledger accounts (merchant payable, platform fee, tax). **A single charge funds multiple obligations,** and rounding and refunds must **allocate proportionally** across parties.

---

## 11. Security and compliance

- **PCI DSS:** minimize scope via tokenization and hosted fields. Isolate any cardholder-data environment (network segmentation, HSMs, key rotation, restricted access, and **never log PANs/CVV**).
- **Encryption** in transit (TLS) and at rest (envelope encryption with KMS), **field-level encryption** for sensitive data.
- **Access control and audit:** least privilege, **immutable audit logs** of every state change and every human access, dual approval for manual money movements.
- **Secrets:** PSP API keys in a secrets manager with rotation, scoped per environment.
- **Regulation:** **PSD2/SCA** (Europe), **KYC/AML** for payouts and wallets, sanctions screening, data residency (GDPR), and tax requirements. Name them without diving deep.
- **Abuse:** card-testing defenses, rate limits, velocity rules, bot detection, and account takeover protections.
- **Trade-off:** compliance adds friction and process, but non-compliance risks fines and losing the ability to process cards.

---

## 12. Failure scenarios

| Failure | Impact | Mitigation |
|---|---|---|
| Client retries after a lost response | Double-charge risk | **Idempotency key** with a unique constraint, return the stored result |
| PSP call times out | **Unknown outcome** | Record `PROCESSING` first, mark `UNKNOWN`, **query PSP with the same idempotency key**, webhooks, sweeper |
| Server crashes after PSP charged, before recording | Charged but not recorded | Intent recorded first, sweeper resolves via PSP status, **reconciliation** as the backstop |
| Duplicate/out-of-order webhooks | Double application or regression | Unique `(provider, eventId)`, monotonic state machine, re-fetch from the PSP for critical transitions |
| Lost webhook | Payment stuck | Polling sweeper, reconciliation |
| PSP outage | Can't take payments | Circuit breaker, **failover to a second PSP on clear failures only**, queue or degrade gracefully ("try another method") |
| Failover retry on a timed-out charge | **Double-charge across PSPs** | Resolve or void the first attempt before routing elsewhere |
| Concurrent refund/capture | Over-refund or double capture | Conditional updates on `version` and on refundable amount |
| Ledger imbalance (bug) | Books don't balance | Invariant checks (debits = credits), continuous verification jobs, alerts, compensating entries (never edit history) |
| Hot ledger account | Write contention | Derived balances, sharded sub-accounts, async aggregation |
| DB primary fails | Writes blocked briefly | Synchronous replica promotion (RPO 0), retries with idempotency |
| Kafka/outbox relay down | Downstream not notified | Outbox rows persist, the relay catches up, consumers idempotent |
| Fraud service down | Can't score | Per-risk-tier fail mode, cached rules, tighter limits |
| Card-testing attack | Fees and reputation damage | Rate limits per IP/device/card, CAPTCHA, velocity rules, PSP-side fraud tools |
| Stuck 3DS / abandoned checkout | Dangling authorizations | Timeouts that void pending auths, sweeper |
| Auth hold expires before capture | Can't capture | Track auth age, re-authorize or notify, avoid long delays |
| Reconciliation finds mismatches | Silent drift | Exception queue, auto-fix known classes, ops tooling, trend alerts |
| Bad deploy | Mass incorrect charges | Canary, **per-merchant/percentage rollouts,** kill switch, strict **idempotent replays**, amount sanity limits |
| Credential leak (PSP keys) | Fraudulent refunds/charges | Secrets manager, rotation, scoped keys, anomaly alerts on refund/payout spikes |
| Region outage | Payments unavailable | Fail over to the standby region with **synchronous or tightly bounded replication** and idempotency to avoid duplicates (see multi-region) |

**Principle (correctness before availability, and fail toward not moving money):** when uncertain, **don't charge, don't guess, and don't mark terminal.** Park the payment in a well-defined pending state and resolve it with authoritative information. A delayed or pending payment is a minor annoyance, while a double charge or lost payment is a trust and legal event. **A slow checkout beats a wrong checkout.**

---

## 13. Consistency decisions

- **Payment state and ledger:** **strong consistency** (ACID transactions, compare-and-set on `version`, unique constraints). This is the one area of the whole series where eventual consistency is not an option.
- **Idempotency records:** written transactionally with the payment.
- **Cross-service workflows** (order ↔ payment ↔ fulfillment): **eventual consistency via sagas and the outbox,** with explicit pending states and compensating actions (refund/void) instead of distributed transactions (2PC across a PSP is impossible anyway).
- **External truth:** the PSP and bank are the source of truth for *whether money moved*, and our ledger is the source of truth for *what we owe and are owed.* **Reconciliation aligns them.**
- **Webhooks and notifications:** at-least-once, idempotent.
- **Reporting/analytics:** eventually consistent, derived from the ledger and events, never used for money decisions.
- **Balances shown to users:** may be slightly stale, but **balance checks that authorize spending or payouts** read the authoritative store.

---

## 14. Multi-region

- **Cleanest approach: single-writer per account/merchant (home region)** with synchronous replication to a nearby standby, which avoids split-brain double-spends. Reads and non-critical services can be served from other regions.
- **Active-active writes** need a globally consistent DB (Spanner-style, Raft/consensus-backed) with higher commit latency, or careful **partitioning so each payment has exactly one owning region.** Without that, two regions could both accept a charge for the same idempotency key.
- **Idempotency keys must be globally consistent** (or scoped by home region), or a failover retry in region B won't see region A's record, and you double-charge.
- **Failover:** promote the standby (with synchronous replication, RPO ≈ 0), and treat in-flight `PROCESSING` payments via the sweeper and PSP queries. **Accept a short period of unavailability over risking duplicates.**
- **Data residency and PCI:** pin PII and card tokens to allowed regions, and keep the vault regional.
- **Trade-off:** synchronous cross-region replication gives zero data loss but adds latency (tens of ms) to every commit, which is fine at this QPS and for these stakes. Async replication is cheaper and faster, but risks losing acknowledged payments on failover, which is unacceptable unless you can reconstruct from PSP records.

---

## 15. Observability and operations

- **Business SLIs:** payment **success/authorization rate** (by PSP, method, country, issuer), decline-reason breakdown, p99 latency per hop, PSP error and timeout rates, time in `PROCESSING/UNKNOWN`, **count of stuck payments,** duplicate-charge rate (should be ~0).
- **Financial integrity monitors:** ledger balance checks (sum of all entries = 0), clearing accounts trending to zero, reconciliation exception rate and age, refund and payout anomaly alerts.
- **Alert on stuck states and unreconciled money,** not just errors. A silent drift is the worst failure.
- **Tracing** with `paymentId` across all hops, and an **ops console** to see a payment's full timeline (attempts, webhooks, ledger entries) to answer "why was I charged?" quickly.
- **Safety tooling:** kill switches per PSP/method/merchant, amount caps, dry-run and sandbox modes, replay tools with safeguards, and **runbooks** for PSP outages and reconciliation breaks.
- **Testing:** simulate PSP timeouts, duplicate webhooks, crashes between transactions, and out-of-order events. **Chaos and fault injection** around the unknown-outcome paths is where bugs live.

---

## 16. Scaling summary

| Component | Strategy |
|---|---|
| API/Payment Service | Stateless, multi-AZ, idempotency required, bounded timeouts, bulkheads per PSP |
| Payments DB | Relational, sync-replicated, single primary until needed, then shard by paymentId/merchant, PITR backups |
| Idempotency | Unique constraint in the same transaction as the payment, retained ≥ 24h |
| Ledger | Immutable double-entry, derived/sharded balances, partitioned by time, invariant verification |
| PSP layer | Adapters, health-based routing, failover only on clear failures, per-PSP circuit breakers and rate limits |
| Webhooks | Authenticated inbox with dedupe, async processing, polling backup |
| Events | Transactional outbox → Kafka, idempotent consumers |
| Recovery | Sweeper for stuck states, reconciliation for residual drift |
| Vault/security | Isolated PCI scope, tokenization, HSM/KMS, immutable audit logs |
| Regions | Home-region ownership, synchronous replication, globally consistent idempotency |

---

## 17. Likely follow-up questions

- **"How do you prevent double charges?"** Idempotency keys enforced by a database unique constraint in the same transaction as the payment, passed down to the PSP as its own idempotency key, and idempotent webhook and consumer handling. At-least-once plus idempotency gives exactly-once effect.
- **"The PSP call timed out. What now?"** The outcome is unknown. Mark it pending, query the PSP with the same idempotency key (or wait for the webhook), and let the sweeper resolve it. Never mark failed or succeeded by guessing, and never retry on a different PSP before resolving the first.
- **"Why a double-entry ledger?"** Auditability, error detection (debits equal credits), immutability, and the ability to rebuild balances. A mutable balance column is lossy and can't explain itself.
- **"How do you handle concurrent refunds?"** A conditional update on the refundable amount (or a row lock on the payment), idempotent refund requests, and reversing ledger entries.
- **"2PC across order, payment, and inventory?"** No. You can't enroll a PSP in 2PC. Use a **saga** with the outbox and compensations (void/refund, release inventory), and explicit pending states.
- **"How do you scale this?"** The volume is modest. Scale reads with replicas, add shards by `paymentId` or merchant, avoid hot ledger rows with derived or sharded balances, and **mostly scale the PSP concurrency (connection pools) and reconciliation batch jobs.**
- **"How do you reduce PCI scope?"** Hosted fields or SDK tokenization so raw card data never reaches our servers, with the vault isolated if we must store anything.
- **"What is reconciliation and why do you need it?"** The periodic comparison of our ledger with PSP and bank records. It catches lost webhooks, crash windows, and PSP errors that real-time protocols can't fully eliminate.
- **"How do you handle a PSP outage?"** Circuit breaker, route new payments to a secondary PSP, but only fail over on definitive failures, and resolve any ambiguous in-flight attempts first. Offer alternate payment methods and degrade with clear messaging.
- **"How do you design payouts differently?"** They're higher risk and irreversible, so add holding periods, limits, verification, approvals, anomaly detection, and the same unknown-outcome handling, since bank rails confirm late and can reverse.
- **"How do you handle FX?"** Per-currency accounts, conversion as a ledger transaction with the recorded rate and FX gain/loss entries, and never mixing currencies in a single balance.
- **"How do you detect fraud without hurting conversion?"** Layered rules plus ML scoring with step-up authentication (3DS) for the gray zone, tuned on the trade-off between fraud loss and false-decline revenue loss.
- **"What happens during a DB failover?"** Synchronous replication means no acknowledged commit is lost. In-flight requests retry safely via idempotency, and stuck `PROCESSING` payments are picked up by the sweeper.
- **"How do you test a payment system?"** Property tests on ledger invariants, fault injection around the crash windows and timeouts, duplicate and reordered webhook replays, PSP sandboxes, and shadow reconciliation in staging.

---

## 18. How to narrate it

1. **Name the dominant problem:** correctness under partial failure with third parties. Scale is a non-issue. The enemies are double-charges, lost payments, and unknown outcomes.
2. **Start naive:** one request handler that calls the PSP and updates a balance column. Show what breaks (a timeout means you don't know whether the charge happened, a client retry causes a double charge, a crash between the PSP call and the DB write loses the record, and a balance column can't be audited). Then add the idempotency key, a state machine with record-intent-first, the sweeper and webhooks, the double-entry ledger, the outbox, and finally reconciliation.
3. **State the contracts:** exactly-once effect via idempotency, durable intent before any external call, never guess on timeouts, immutable ledger with balanced entries, and reconciliation as the final safety net.
4. **Separate security scope:** tokenization, so the card data never enters your systems.
5. **Call out the knobs:** idempotency retention, PSP timeout and retry budgets, sweeper thresholds, auth-hold duration, fraud thresholds, payout holding periods, reconciliation cadence, and kill switches.
6. **For each box, say what happens when it fails,** and end with "any component can fail or time out, and the worst case is a payment that stays pending until we resolve it with authoritative data, never a double charge or a lost one."

**Patterns reused from our toolbox:** idempotency keys and unique constraints (URL shortener, Uber, Dropbox, notifications), persist-before-ack and record-intent-first, the transactional outbox and sagas (feed, Uber), state machines with compare-and-set (Uber trips, Dropbox commits), the recovery sweeper for stuck work (Uber dispatch), at-least-once plus idempotent consumers (queues, crawler), circuit breakers, bulkheads and timeouts (reliability patterns), per-category fail-open versus fail-closed (rate limiter), hot-row avoidance via sharded counters (feed counters), webhooks as hints versus polling as truth (chat, Dropbox), immutable append-only records with compensating corrections (Dropbox versions, Kafka logs), batch reconciliation as the safety net (Top-K Lambda), and static stability: the system fails toward doing nothing rather than doing the wrong thing.

Next, I can play interviewer on this (for example, "your reconciliation job reports 4,000 charges at the PSP that have no matching payment in your database, walk me through triage, customer impact, and root-cause prevention"), or we can move on. **Ticket booking** is the natural next step: it adds **inventory contention** (many buyers for few seats), holds with expiry, and the interplay with payment. **Distributed cache / KV store** and **typeahead/search autocomplete** are good picks if you want to open up core infrastructure pieces we've used as black boxes.
