# Distributed Job Scheduler Design (Airflow / Temporal / Quartz / Kubernetes CronJob style)

**What to say early:** "Run this job at 9:00" sounds trivial, and on one machine it is. Distributed, it becomes a precise problem: **many machines, any of which can die or pause at any moment, must collectively run each job at the right time, the right number of times, and never lose it.** Five ideas organize the answer:

1. **Three separate concerns hide in one word.** *Triggering* (deciding a job is due), *dispatching* (handing it to capacity), and *executing* (running it on a worker). Each has different scaling and failure properties, so design them as separate stages joined by durable state.
2. **"Exactly once" is a trap.** You get **at-least-once execution plus idempotency**, with **exactly-once *triggering*** achievable through unique constraints. State this early, as in the payment and queue designs.
3. **Leases and fencing are the core primitive.** Every ownership question (who fires this shard, who runs this task) is answered by a **time-bounded lease with a fencing token**, so a paused or partitioned zombie can't cause a double effect.
4. **Durable state is the truth, and memory is a cache.** Schedulers, dispatchers, and workers are all crash-safe because a **sweeper** can rebuild progress from the database.
5. **Time is spiky and adversarial.** Millions of jobs are scheduled for `:00`, so the real load problem is the **thundering herd at round timestamps**, plus fairness between tenants.

Scope first. I'll design a **multi-tenant scheduler and task-execution platform** with delayed, recurring (cron), and ad-hoc jobs, retries, priorities, and a **DAG/workflow layer** on top. Distinguish three product shapes:

| | **Cron/timer service** (K8s CronJob, EventBridge Scheduler) | **Task queue with delays** (Celery, Sidekiq, SQS) | **Workflow engine** (Airflow, Temporal, Step Functions) |
|---|---|---|---|
| Core | Fire at time T | Run this task soon, with retries | Run a graph/long process with state, dependencies, timers |
| State | Tiny | Per task | Per workflow, durable history |
| Hard part | Timer scale, no missed/double fire | Retry, visibility, fairness | Durable state, recovery, long waits |

The platform contains all three layers, so I'll build them bottom-up.

---

## 1. Requirements

**Functional**
- Schedule **one-off** jobs (at a time or after a delay), **recurring** jobs (cron or fixed interval, with time zones), and **immediate** jobs
- Execute on a worker fleet with **timeouts, retries with backoff, and a dead-letter state**
- **Priorities** and **per-tenant concurrency and rate limits**
- Overlap policy for recurring jobs (allow, skip, or replace if the previous run is still going)
- **Dependencies:** run B after A succeeds, with fan-out and fan-in (DAGs)
- Cancel, pause, resume, update schedules, and **manual trigger and backfill**
- Run history, logs, and status, plus alerting on failures and misses

**Non-functional**
- **Trigger accuracy:** fire within ~1 s of the scheduled time (tighter for some tiers)
- **No lost triggers** (a registered job always fires, even through crashes and outages)
- **No duplicate triggers.** Execution is at-least-once, and duplicates are controlled and detectable.
- **High availability** of triggering, with no single scheduler as a point of failure
- **Scale:** hundreds of millions of schedules and tens of thousands of fires per second at peaks
- **Fairness and isolation** between tenants
- **Durability** of job definitions and run history, and **auditability**

**Clarify with the interviewer:** Are jobs short tasks (seconds) or long-running (hours)? Are they idempotent? Is the platform executing user code (sandboxing!) or calling webhooks and queues? Precision needed: seconds or milliseconds? Are DAGs and long workflows in scope? What should happen to fires missed during an outage (catch up or skip)?

---

## 2. Estimation (state assumptions)

- **100M registered schedules** (mostly recurring, plus a large number of one-off delayed jobs, e.g. "send reminder in 2 hours")
- Average interval maybe hourly → **~30K fires/sec average.** But **cron alignment** means a large share lands on `:00` of the hour and minute, so **peaks of 500K-1M fires within a few seconds** if unmanaged.
- **State:** 100M × ~1 KB per definition ≈ **100 GB**, and run history at 1 KB × ~3B runs/day ≈ **3 TB/day** (keep 30 days hot, archive the rest)
- **Concurrent running tasks:** 1M with an average duration of 30 s at 30K/sec. Long-running jobs (hours) push this higher.
- **Workers:** at ~50 concurrent tasks per worker node, 1M concurrent tasks needs ~20K nodes, or far fewer if tasks are tiny or run as lightweight invocations (state your assumption about task shape).
- **Heartbeats:** 1M running tasks × a heartbeat every 10-30 s ≈ **~50-100K heartbeat writes/sec**, which must be cheap (batched, in-memory/lease store, not a durable row update each time).

**Takeaway to say aloud:** Storage is modest. The system is sized by **fires per second at peak, heartbeat volume, and the thundering herd at round timestamps.** The scheduler itself is lightweight, since it mostly moves tiny records. The expensive part is the worker fleet, which is a capacity-planning and fairness problem.

---

## 3. API and data model

```
POST /v1/jobs
  { tenantId, name, schedule: {cron:"0 9 * * MON-FRI", tz:"America/New_York"} | {at: ts} | {every: "15m"} | {delay: "2h"},
    target: {type: http|queue|container|function, spec...},
    retry: {maxAttempts: 5, backoff: exp, initialMs: 1000, maxMs: 300000},
    timeoutSec: 300, priority: high|normal|low,
    overlap: allow|skip|replace|queue, misfire: fire_once|skip|catch_up,
    concurrencyKey?, idempotencyKey, payload }          → { jobId }
PUT/DELETE /v1/jobs/{id}     POST /v1/jobs/{id}/pause|resume|trigger
GET  /v1/jobs/{id}/runs?cursor=                          → run history
POST /v1/jobs/{id}/backfill  { from, to }

Worker protocol
  poll/lease(queue, n)      → tasks with { taskId, attempt, leaseId, fencingToken, leaseExpiresAt }
  heartbeat(taskId, leaseId) → extends lease, returns cancel flag
  complete(taskId, leaseId, result) | fail(taskId, leaseId, error, retryable)
```

| Entity | Shape | Store |
|---|---|---|
| **Job definition** | `jobId → tenant, schedule spec, target, retry policy, overlap/misfire policy, nextFireTime, state (ACTIVE/PAUSED), version` | Sharded relational/KV by `jobId` |
| **Timer index** | `(shardId, fireTimeBucket, jobId) → ` pointer, ordered by time | Same store, indexed (or a dedicated timer store) |
| **Run (instance)** | `(jobId, scheduledTime) → runId, state, attempt, startedAt, finishedAt, result` with **unique key `(jobId, scheduledTime)`** | Sharded relational/wide-column |
| **Task / attempt** | `(runId, attempt) → workerId, leaseId, leaseExpiresAt, fencingToken, state, error` | Same, hot subset in lease store |
| **Dispatch queue** | Ready tasks by `(priority, tenant)` lane | Kafka/SQS-style queue or a DB-backed queue |
| **Lease/ownership registry** | `shardId → ownerNode, epoch, leaseExpiresAt` | Coordination store (etcd/ZooKeeper) |
| **DAG / workflow state** | `workflowId → node states, dependency counts, event history` | Strongly consistent store, event-sourced |
| **Tenant config** | Quotas, concurrency limits, rate limits | Config DB, cached |
| **Audit/event log** | Immutable record of every state transition | Kafka → object store |

**Key design points:**
- **A job and a run are different things.** The *job* is the definition ("every hour"). The *run* is one occurrence, created at fire time, with its own state machine and retries.
- **The unique key `(jobId, scheduledTime)` on runs is the exactly-once-*triggering* mechanism.** Whoever inserts first wins, and any duplicate attempt to fire the same occurrence fails harmlessly.
- **`scheduledTime` is not "now."** It is the *logical* time the run was supposed to happen, which makes catch-up, backfill, and idempotent re-fire well-defined.

---

## 4. High-level architecture

```
Clients/API ──► API Service (stateless: validate, authz, quotas, idempotency) ──► Job Store (sharded, replicated)
                                                                                      │ change stream
                                                                                      ▼
                  ┌──────────────── TRIGGER TIER (stateful, sharded, leased) ──────────────────┐
                  │ Timer shards 0..N-1, each owned by ONE scheduler node via lease + epoch     │
                  │   • loads upcoming fires (next ~1-5 min) into an in-memory timer heap/wheel │
                  │   • at fire time: TX{ insert Run (unique jobId,scheduledTime);              │
                  │                       compute+store nextFireTime }  → enqueue READY task    │
                  └──────────────────────────────────┬──────────────────────────────────────────┘
                                                     ▼
                  ┌──────────────── DISPATCH TIER ──────────────────────────────────────────────┐
                  │ Admission: tenant quotas, concurrency keys, overlap policy, rate limits     │
                  │ Queues: lanes by priority × tenant-weight (weighted fair queuing)            │
                  └──────────────────────────────────┬──────────────────────────────────────────┘
                                                     ▼
Worker fleet (stateless, autoscaled on queue depth)  ◄── PULL tasks with a LEASE (+ fencing token)
   run task (sandboxed) · heartbeat to extend lease · complete/fail with leaseId
        │                                                  ▲
        ▼ state transitions (conditional writes)           │ expired leases
   Run/Task Store ◄───────────── Reaper / Sweeper ─────────┘  (requeue timed-out, resolve stuck runs)
        │ outbox
        ▼
   Kafka "run.events" ──► DAG Orchestrator (dependency resolution) · Notifications/alerts
                          · History/audit · Metrics · Billing/usage

Control plane: coordination store (leases, shard map) · config · rebalancer · admin tools
```

**Separate the stages.** The trigger tier cares about *time precision*, the dispatch tier about *fairness and admission*, and the workers about *execution isolation*. A slow worker fleet must not delay triggering, and a trigger storm must not overload workers. Queues between stages give you backpressure at each boundary.

---

## 5. Core deep dive 1: knowing what's due (the timer problem)

### Naive: one cron daemon, or `SELECT * FROM jobs WHERE next_fire <= now()`
- **One daemon** is a single point of failure, and nothing scales beyond one machine.
- **A polling query over 100M rows** every second is a hot scan. Even with an index, many pollers produce **duplicate fires** (two instances both see the same due row) and lock contention.

### Option A: DB polling with row claiming
```sql
SELECT jobId FROM jobs
WHERE shardId = :s AND next_fire <= now() AND state='ACTIVE'
ORDER BY next_fire LIMIT 500
FOR UPDATE SKIP LOCKED;
```
- **Pros:** simple, correct (the DB arbitrates), and works well up to modest scale. `SKIP LOCKED` lets several pollers cooperate without blocking.
- **Cons:** latency is bounded by the poll interval, steady DB load, and a hot index on `next_fire` (all writes hit the "now" end of the index). Round timestamps cause a spike of locked rows.
- **Verdict:** a legitimate starting point (many production schedulers begin here). It stops scaling when the timer index becomes the bottleneck.

### Option B: Partitioned timers with lookahead and an in-memory heap (my recommended answer)
- **Partition the timer space into N shards** by `hash(jobId) mod N` (N in the thousands, far more than nodes, so shards are the unit of ownership and rebalancing).
- Each shard is owned by **one scheduler node at a time** (lease + epoch). The owner periodically **loads fires due in the next window (e.g., 1-5 minutes)** from the store into an **in-memory min-heap or hierarchical timing wheel**, and sleeps until the earliest fire.
- **At fire time**, it executes the firing transaction (below), then enqueues the task.
- **Why lookahead:** the DB is queried once per window per shard, instead of continuously, and the actual firing is precise to the millisecond from memory.
- **Why one owner per shard:** it removes contention, because only one node ever fires a shard's jobs. The **unique key on the run** is the safety net if ownership briefly overlaps.
- **Trade-off:** in-memory timers are lost on crash, but they're **derived from the durable store**, so a new owner simply reloads the window. The cost is a short gap during failover (bounded by lease timeout), covered by catch-up logic.

### Option C: Delay queues / timing wheels as the primitive
- **Hierarchical timing wheel** (Kafka's delayed-operation design, Netty): O(1) insert and tick for huge numbers of timers, with levels for seconds, minutes, hours, and days. Excellent *inside* a shard for the in-memory window.
- **Redis sorted set** (`ZADD score=fireTime`, `ZRANGEBYSCORE` to pop due): simple and fast, but needs durability care (async replication can lose timers), so treat it as an accelerator over the durable store.
- **Managed delay features** (SQS delay up to 15 min, Kafka delay topics per interval, DynamoDB TTL-style tricks): fine for bounded delays, awkward for long ones (months), which is why long timers live in the durable store and only the near-term window is in memory.

**State your decision:** "Durable timer index in the job store, partitioned into many shards. Each shard is owned by one scheduler via a fenced lease, which loads a lookahead window into an in-memory timing wheel and fires from memory. The store is the source of truth, and the in-memory state is rebuilt on failover."

### Cron semantics and time (a classic trap)
- **Store only the next fire time**, never all future occurrences. After each fire, compute the next from the cron expression and persist it **in the same transaction.**
- **Time zones and DST:** "9:00 America/New_York" is a *wall-clock* rule. On spring-forward, 2:30 AM doesn't exist (decide: skip or shift to 3:30). On fall-back, 1:30 AM happens twice (decide: fire once, on the first). Compute with a real tz database, store the **IANA zone** (not a UTC offset), and document the policy.
- **Leap seconds, clock skew:** use **a single authoritative time source per decision** (the DB or coordination store clock for ordering, NTP-disciplined hosts for firing) and tolerate small skew.
- **Fixed-rate vs. fixed-delay recurrence:** "every 15 min" can mean from the *scheduled* start (drift-free) or from the *completion* of the last run. Offer both, since they behave very differently when runs are slow.
- **Misfire policy (what to do about fires missed during an outage):**
  - **Fire once** (run once now to catch up): the common default
  - **Skip** (wait for the next occurrence): right for "refresh a cache"
  - **Catch up all** (run every missed occurrence, in order): right for "process each hour's data," but dangerous after a long outage (a burst of thousands of runs), so cap it and rate-limit it
  - Since runs are keyed by `scheduledTime`, catch-up is just inserting the missing keys.

### Taming the thundering herd at round timestamps
Everyone schedules `0 * * * *`. At `:00:00` a million jobs are due.
- **Per-job jitter/spread:** a deterministic offset per job (`hash(jobId) mod window`) within a tolerance the user accepts (e.g., fire anywhere in `:00:00-:00:30`), unless exact time is requested.
- **Admission control at the dispatch tier** (below), so the *trigger* can record "due" quickly while *execution* is paced.
- **Pre-materialization:** the lookahead window lets the scheduler pre-load and pre-sort the herd, so the fire itself is cheap.
- **Trade-off:** jitter flattens load but means "9:00" really means "9:00-9:00:30." Offer a precise tier with reserved capacity, priced accordingly.

---

## 6. Core deep dive 2: exactly-once-*ish* triggering

**The danger:** two scheduler nodes both believe they own a shard (after a GC pause, a network partition, or a lease that expired while the node was frozen). Both fire the same job.

### Layered defenses
1. **Lease + epoch (fencing token) per shard.** The coordination store grants shard ownership with an incrementing epoch. Every write the scheduler makes carries its epoch, and the job/run store **rejects writes from a stale epoch.** A zombie scheduler that wakes up can't commit anything.
2. **Unique key on `(jobId, scheduledTime)`.** Even if fencing somehow fails, the second insert of the same occurrence **fails the unique constraint.** The loser simply skips the enqueue. This is the idempotency key, the same pattern as payments.
3. **Atomic fire transaction.** In one DB transaction: insert the run row (state `READY`), advance `nextFireTime`, and write an **outbox** record to enqueue the task. Either all happen or none. A crash between steps can't lose the trigger (the transaction rolled back, so it will be re-found) or fire it twice (the unique key).
4. **Outbox, not dual write.** Never "insert run, then publish to the queue" as two separate steps. A crash in between loses the task. The outbox relay publishes at-least-once, and the task ID is the dedupe key downstream.

### Scheduler failover
1. Coordination store notices the owner's lease expired (missed renewals).
2. Another node acquires the shard with a **higher epoch.**
3. It reloads the lookahead window from the store, **including fire times that already passed** (anything with `nextFireTime <= now` and no run record), and applies the misfire policy.
4. The old owner, if it returns, sees the higher epoch, drops its in-memory state, and stops.

**Trade-offs:**
- **Lease duration:** short leases fail over fast but cause false failovers on GC pauses or network blips. Long leases are stable but leave a longer gap with no owner. Typical: 10-30 s with renewals every third of that, and a **safety margin** so a node stops acting *before* its lease expires by its own clock.
- **Centralized coordination (ZooKeeper/etcd) vs. DB-based leases:** a coordination store gives clean semantics and fast watch notifications, while a DB-row lease (`UPDATE ... WHERE owner IS NULL OR expires < now()`) avoids a new dependency. Keep the coordination store **off the per-fire path** (it's touched at acquisition and renewal only).
- **Gap during failover:** jobs due inside the lease-expiry window fire late by up to the failover time. That is the *bounded lateness* you advertise, and the misfire policy handles the rest.

---

## 7. Core deep dive 3: dispatching and execution (leases for tasks)

### Pull, not push
Workers **pull** tasks from the queue when they have capacity.
- **Pros:** natural backpressure (a busy worker doesn't pull), heterogeneous workers (big and small) just pull what they can handle, and no scheduler-to-worker connection tracking.
- **Cons:** poll latency, mitigated with **long polling.**
- **Push** (scheduler calls workers) gives lower latency but requires tracking worker capacity and health, and overloads slow workers. Use push only for webhook-style targets, with the scheduler acting as the caller.

### The task lease (visibility timeout + heartbeat)
1. A worker **claims** a task: an atomic transition `READY → RUNNING` that records `workerId`, `leaseId`, `leaseExpiresAt`, and a **fencing token** (the attempt number or a monotonic counter).
2. The worker **heartbeats** to extend the lease while running. The response carries a **cancel flag** and the current lease state.
3. On completion, `complete(taskId, leaseId, result)` is a **conditional update** (`WHERE leaseId = :l AND state='RUNNING'`). If the lease was already reassigned, the update **fails**, and the stale worker knows its result was discarded.
4. If heartbeats stop, the **reaper** sees `leaseExpiresAt < now`, flips the task to `READY` (attempt+1), and re-enqueues it.

This is the **same lease-with-fencing pattern** as Uber offers, ticket holds, and SQS visibility timeouts.

### The unavoidable truth: at-least-once
A worker can finish the side effect (send the email, charge the card) and **crash before reporting completion.** The reaper requeues, and a second worker does it again. Or a paused worker resumes after its lease expired while a second worker is already running the same task.

**You cannot prevent that. You contain it:**
- **Tell users plainly: jobs must be idempotent**, and hand them the tools: a stable **`runId`/`idempotencyKey`** and an **attempt number** in every invocation, so they can dedupe in their own systems (conditional writes, unique constraints, idempotency keys on downstream APIs).
- **Fencing tokens for side effects:** pass the monotonic token to the downstream system so **it** can reject writes from a stale attempt (the classic fix for the paused-worker problem, same as distributed locks with fencing).
- **At-most-once mode** (opt-in, for non-idempotent jobs): mark the task started **before** running and **never auto-retry** a task whose outcome is unknown, so a crash results in a failed/unknown run for a human or policy to resolve. **Trade-off:** you give up automatic recovery to avoid duplicates, the same loss-versus-duplicate decision as notifications.
- **Say it explicitly:** *at-least-once execution + idempotent jobs = exactly-once effect.*

### Heartbeat design and cost
- 1M running tasks × heartbeat every 15 s is ~70K writes/sec. Writing each to the durable DB would be wasteful.
- **Keep leases in a fast, replicated store** (Redis/in-memory with replication, or a dedicated lease service) and **persist only state transitions** (`RUNNING`, `COMPLETED`, `FAILED`) durably. On a lease-store loss, tasks are conservatively treated as *possibly running*, and the reaper waits the full timeout before requeueing.
- **Lease duration vs. heartbeat interval:** lease ≈ 3× heartbeat interval, so one or two missed beats don't cause a false requeue.
- **Trade-off:** short leases detect dead workers quickly but risk duplicate runs from transient slowness. Long leases are calm but delay recovery. Make it per-job-class configurable.

### Timeouts and cancellation
- **Execution timeout** (max runtime) enforced two ways: the worker kills its own task, and the **reaper enforces a hard deadline** server-side in case the worker is gone.
- **Cancellation** is cooperative: set a flag the next heartbeat returns, escalate to a forced kill after a grace period. Be honest in the API: cancel is *best-effort* for already-running work.
- **Long-running jobs (hours):** require heartbeats and ideally **checkpointing** (the job records progress, so a retry resumes rather than restarts). Without checkpoints, a long job on spot capacity may never finish.

### Retries, backoff, and poison jobs
- **Retry only retryable failures** (timeouts, 5xx, worker loss). A validation error or `4xx` is permanent: fail immediately.
- **Exponential backoff with jitter** (`min(max, base × 2^attempt) × random(0.5, 1.5)`), a **max attempts** cap, and a **retry budget** per tenant, so a broken dependency doesn't create a retry storm that amplifies the outage.
- Implement the delay as a **scheduled re-enqueue** (a timer, not a sleeping worker), which reuses the same timer machinery.
- **Dead-letter state** after max attempts: the run is marked `FAILED/DEAD` with the error history, an alert fires, and there is **replay tooling** with guards. A poison job must never block others.
- **Circuit breaker per target:** if a downstream is down, pause dispatch to it instead of burning every job's retries.

---

## 8. Core deep dive 4: fairness, priorities, and admission

Without isolation, one tenant's 10M-job backfill starves everyone, and a flood of low-priority work delays critical jobs.

### Lanes and weighted fair queuing
- **Separate queues per priority lane**, with **reserved worker capacity** for high priority that low priority can't borrow (the same physical isolation as the notification system).
- **Within a lane, fair-share across tenants:** weighted round-robin or **deficit round robin** over per-tenant sub-queues, so each tenant gets its share and bursts only use *idle* capacity.
- **Aging:** raise the effective priority of long-waiting low-priority tasks so they don't starve forever.

### Admission control
- **Per-tenant limits:** max concurrent running tasks, max dispatch rate, max queued tasks. Over the limit means tasks wait in a per-tenant backlog (or are rejected at API time with `429` for new submissions).
- **Concurrency keys:** "only one task with key `customer-42` at a time," enforced by a counter or lock (conditional update on a small row). Useful to serialize work per entity.
- **Overlap policy for recurring jobs** (evaluated at fire time): **Allow** (run in parallel), **Skip** (drop this fire if the previous run is still active), **Replace** (cancel the old, start the new), or **Queue** (run after the previous finishes). Skip is the safest default for most periodic jobs. The decision must be **atomic** with run creation (check-and-insert in one transaction), or two fires race past the check.
- **Global load shedding:** under extreme backlog, shed lowest-priority and lowest-value (e.g., stale, skippable) jobs first, never critical ones.
- **Trade-off:** strict isolation and quotas protect everyone but lower utilization (reserved capacity idles) and require tuning. Work-conserving schedulers (borrow idle capacity, reclaim on demand) recover efficiency, and preemption adds complexity.

### Worker autoscaling and placement
- **Scale workers on queue depth and oldest-task age** (the latency SLI), not CPU alone.
- **Capability matching:** tasks declare resource needs (CPU, memory, GPU, region), and workers advertise capabilities. Queue per capability class to avoid head-of-line blocking from a task nobody can run.
- **Bin-packing vs. spreading:** pack for cost, spread for fault tolerance, and use spot/preemptible capacity for retryable, checkpointed work.
- **Isolation:** user code runs sandboxed (containers, gVisor/Firecracker, resource limits, no shared credentials), because **a scheduler that runs arbitrary code is a multi-tenant security boundary.** Network egress policy and secret scoping per tenant.

---

## 9. Core deep dive 5: dependencies and workflows (the DAG layer)

Many jobs are graphs: extract → transform (×N in parallel) → load → notify.

### Model
- A **workflow definition** is a DAG of tasks with edges (`B after A`), plus conditions (on success, on failure, always), retries per node, and a workflow-level timeout.
- A **workflow run** is one instantiation, with per-node state: `PENDING → READY → RUNNING → SUCCEEDED/FAILED/SKIPPED`.

### Orchestrator mechanics
- On each **task-completion event** (from the `run.events` stream), the orchestrator finds the node's downstream edges, **decrements a dependency counter** on each child, and when a child's counter hits zero (and its conditions hold), marks it `READY` and dispatches it. Fan-in is a counter, and fan-out is dispatching all children.
- **State lives in a strongly consistent store**, and every transition is a **conditional write** (compare-and-set on node state/version), so duplicate or reordered completion events are harmless. Event processing is **idempotent**.
- **Orchestrator crash recovery:** all state is durable, so a new instance resumes from the stored state, and a **sweeper** re-evaluates workflows with no recent progress (a lost event can't strand a workflow).
- **Cycle detection and validation** at definition time. **Partial failure policy:** fail-fast, continue-independent-branches, or retry-from-failed-node.

### Durable execution (the Temporal/Cadence approach)
An alternative to explicit DAGs: let users write **workflow code** (`result = await activity(...); await sleep(30 days); ...`) and make it **durable**.
- Every step's input/result is appended to an **event-sourced history** per workflow. On a crash, a worker **replays the history** to rebuild the code's state, **skipping already-completed activities** (their results are recorded), and continues from the last point.
- **Timers** (`sleep(30 days)`) are durable timer records in the same timer machinery as above, costing nothing while waiting.
- **Determinism requirement:** workflow code must be deterministic (no raw clock, randomness, or I/O in the workflow function), so replay reproduces the same decisions. Side effects go in **activities**, which are at-least-once and idempotent.
- **Pros:** arbitrary control flow, long waits, signals and queries, and no hand-rolled state machine. **Cons:** more complex runtime, the determinism discipline, and history growth (mitigated by "continue-as-new" and snapshots).
- **Trade-off:** declarative DAGs are simpler, easier to visualize, and fine for batch pipelines. Durable execution wins for long-lived, branching business processes (sagas, onboarding, order fulfillment), the same sagas we've used for payments and ticket booking.

### Scheduling and data awareness (Airflow-style)
- **Data intervals / logical dates:** a daily run for `2026-10-16` processes that day's data, regardless of when it actually runs. Pairing the run with its **logical `scheduledTime`** makes backfills and reruns deterministic and idempotent.
- **Backfill:** generate runs for a historical range (the unique key `(jobId, scheduledTime)` makes it safe to re-run), rate-limited and low priority so it doesn't crowd production runs.
- **Sensors/event triggers:** start a run when a file lands or a message arrives (an event-driven trigger instead of time), implemented as a consumer that creates runs with a unique key derived from the event.
- **Trade-off:** event-driven triggering reduces latency and polling waste, but time-based schedules remain essential for predictability and as a fallback if events are lost.

---

## 10. Hop by hop: reliability and trade-offs

### 1. API service
- Stateless and multi-AZ. **Idempotency key on job creation** (a retried "create job" must not make two). Validate cron expressions, quotas, and targets (SSRF protection on webhook targets: block internal ranges).
- Writes the definition and the first `nextFireTime` **in one transaction.** A job that exists without a timer, or a timer without a job, is an invariant violation.

### 2. Job and run store
- **Strongly consistent**, partitioned by `jobId`/shard, with synchronous replication across AZs. Fire and state transitions are low-volume relative to a typical DB, so favor correctness. Conditional writes everywhere (`WHERE state=... AND version=...`).
- **Hot-index mitigation:** partition the timer index by shard and time bucket so "now" writes spread across partitions.
- **History retention:** archive old runs to cheap storage, and keep a bounded hot window.

### 3. Trigger tier (scheduler nodes)
- Stateful but **disposable**: in-memory timers derived from durable state, owned via fenced leases. **Many small shards per node** so failover moves small pieces and rebalancing is smooth.
- **Rebalancing:** consistent hashing or a shard map in the coordination store. When nodes join or leave, shards move via lease handoff. Avoid moving shards during the herd at `:00`.
- **Self-protection:** the scheduler fires into the queue **without waiting on workers**, with a bounded outbox. If the dispatch tier is backed up, runs sit `READY` (durable) rather than blocking the timer.
- **Clock safety:** a scheduler that detects its own clock is skewed or its lease about to expire **stops firing** (fail safe).

### 4. Dispatch queue
- A durable queue (Kafka/SQS-like, or DB-backed at modest scale) with **priority and tenant lanes.** At-least-once, so **tasks carry `taskId` and `attempt`** and claiming is a conditional transition, making duplicate queue messages harmless.
- **Queue as buffer:** it decouples trigger spikes from worker capacity. Monitor **oldest-ready-task age** as the master latency SLI.

### 5. Workers
- Pull with long-poll, heartbeat, sandbox, and report via conditional updates. **Graceful drain on scale-in:** stop pulling, finish or checkpoint in-flight work, release leases. A worker killed abruptly relies on lease expiry.
- **Capacity limits per worker** to avoid overload, and **spot-friendly** behavior for retryable work.

### 6. Reaper/sweeper
- The universal recovery mechanism (as in Uber dispatch and payments): periodically finds **expired leases, tasks stuck in `READY`/`RUNNING` past deadlines, runs with no completion, workflows with no progress,** and drives them to a terminal or retry state. Idempotent and itself sharded and leased. **Correctness never depends on a happy-path event arriving.**

### 7. DAG orchestrator
- Event-driven with durable state and idempotent handlers, as above. **Backpressure:** limit concurrent runnable nodes per workflow and per tenant, so a 100K-node fan-out doesn't flood the dispatcher.

### 8. Coordination store
- A small consensus-backed cluster (etcd/ZooKeeper), **off the hot path**: leases and shard maps only. If it's briefly down, running schedulers keep firing within their lease margin, and no new ownership changes occur. This is **static stability**, as in the KV store and collaborative editing designs.

---

## 11. Failure scenarios

| Failure | Impact | Mitigation |
|---|---|---|
| Scheduler node crashes | Its shards stop firing | Lease expiry, new owner loads the window with a higher epoch, **catch-up per misfire policy** |
| **Zombie scheduler** (GC pause, partition) | Duplicate fires | **Epoch fencing** on writes + **unique `(jobId, scheduledTime)`** |
| Crash between "insert run" and "enqueue" | Lost or stuck trigger | Single DB transaction with **outbox**, sweeper re-drives `READY` runs with no queue message |
| Duplicate queue delivery | Task runs twice | Conditional claim (`READY→RUNNING`), `attempt`/`leaseId` checks, idempotent jobs |
| Worker crashes mid-task | Task incomplete | Lease expires, reaper requeues with attempt+1, at-least-once |
| **Worker paused, then resumes** after lease loss | Two concurrent executions | Conditional `complete` rejects stale lease, **fencing token to downstream**, idempotent side effects |
| Completion lost (worker succeeded, report failed) | Re-execution | Idempotency keys and fencing tokens for the job's effects |
| Thundering herd at `:00` | Queue and DB spike, late jobs | Jitter, lookahead pre-load, admission control, per-tenant pacing, autoscale |
| Backlog grows (workers slow/down) | Late execution | Autoscale on queue age, priority lanes, shed lowest priority, alert on lag |
| One tenant floods the system | Others starved | Per-tenant quotas, weighted fair queuing, concurrency limits, reserved critical capacity |
| Retry storm on a broken dependency | Amplified outage | Backoff + jitter, retry budgets, per-target circuit breaker, DLQ |
| Poison job crashes workers | Repeated failure | Sandboxing, max attempts, DLQ, quarantine by job version |
| Long outage, many missed fires | Burst on recovery | Misfire policy (fire-once/skip), catch-up cap and rate limit, gradual ramp |
| Job store primary fails | Trigger and state writes blocked briefly | Sync replica promotion, retries, schedulers keep in-memory window, fire on recovery |
| Coordination store down | No ownership changes | Schedulers continue within lease margins, alert, new shards wait |
| Lost DAG completion event | Workflow stalls | Durable state + sweeper re-evaluation, idempotent handlers |
| Clock skew on scheduler | Early/late fires | Authoritative time for decisions, skew detection and self-fencing, NTP monitoring |
| DST/timezone bug | Missed or double fire twice a year | IANA tz database, explicit DST policy, tests around transitions |
| Bad deploy changing schedule logic | Mass wrong fires | Canary shards, shadow mode comparing computed fire times, kill switch, **dry-run** |
| Runaway recurring job (fires every second by mistake) | Resource burn | Minimum interval limits, per-job rate limit, anomaly alerts |
| Region outage | Region's jobs stop | Replicated state, ownership failover to another region, catch-up on recovery |

**Principle (late is acceptable, lost or duplicated is not):** the scheduler fails toward **running a job late, once-or-slightly-more, and visibly**, never toward silently skipping it. Durable state is the truth, every component is restartable from it, every ownership claim is fenced, and every effect is made idempotent by contract.

---

## 12. Consistency decisions

- **Triggering:** **exactly-once per `(jobId, scheduledTime)`**, enforced by a unique constraint and fenced shard ownership.
- **Execution:** **at-least-once**, with attempt IDs, lease/fencing tokens, and idempotent jobs for an exactly-once *effect*. An optional at-most-once mode trades recovery for no duplicates.
- **Job definitions and run state:** **strongly consistent**, with conditional writes (compare-and-set) on every state transition and a monotonic state machine.
- **Ordering:** within a job, runs are ordered by `scheduledTime` where the overlap policy requires it, and across jobs there is **no ordering guarantee.** Dependencies are enforced explicitly via the DAG.
- **Timers:** *bounded lateness*, not perfect precision, and the SLA says so (e.g., 99.9% within 1 s of scheduled time).
- **Leases and ownership:** consensus-backed (coordination store), strongly consistent, off the hot path.
- **Schedule updates:** take effect on the next fire computation. Changes racing with an in-flight fire are resolved by versioned definitions, so a fire uses the version it was computed from.
- **History, metrics, audit:** eventually consistent, derived from the event stream.
- **Cancellation/pause:** strongly consistent at the state level, *best-effort* for already-running work.

---

## 13. Multi-region

- **Home region per job (or per tenant)** owns its timers and runs, with **synchronous replication across AZs** and **async replication to a DR region.** One owner region avoids two regions firing the same schedule, the same single-writer logic as payments and ticket booking.
- **Workers can run anywhere** (data locality, cost), pulling from the owning region's queue or from regional queues the dispatcher feeds, since execution is the cheap part to distribute.
- **Failover:** promote the DR region, whose **replicated store holds `nextFireTime` and run records.** Replication lag means a few recent fires may be uncertain, resolved by the **unique key** (a re-fire of an already-fired occurrence fails harmlessly or is detected) plus the misfire policy, and runs that were `RUNNING` are resolved by the sweeper via lease expiry. Accept a brief gap over the risk of duplicate triggers.
- **Active-active** across regions needs **partitioned ownership** (each job belongs to exactly one region at a time, moved by fenced handoff), or a globally consistent store at higher latency.
- **Data residency:** pin job definitions, payloads, and execution to allowed regions.
- **Trade-off:** single-owner is simple and safe, but a region outage delays its jobs until failover. Multi-owner would shorten the gap and add duplicate-trigger risk.

---

## 14. Observability and operations

- **Master SLIs:**
  - **Fire lag:** `actualFireTime − scheduledTime`, p50/p99/max, per tier
  - **Dispatch lag / queue age:** time from `READY` to `RUNNING`
  - **Missed-fire count** (a due job with no run), which should be zero
  - **Duplicate-trigger count** (unique-constraint rejections), tracked as a signal of ownership overlap
  - **Run success rate, retry rate, DLQ depth, run duration vs. timeout**
  - **Lease-expiry rate** (high rates mean worker or network trouble), heartbeat latency
  - **Worker utilization and autoscaling lag**, per-tenant usage versus quota
- **Alerts:** fire-lag SLO burn, any missed fires, queue age beyond threshold, DLQ growth, scheduler failover spikes, runs stuck in `RUNNING` past deadline, a tenant hitting quotas, and **jobs that silently stopped running** (alert on *absence*, such as "this daily job has no successful run in 26 hours," the dead-man's-switch idea from the metrics design).
- **A watchdog:** run a canary job every minute, and alert (from a separate system) if it doesn't complete. This measures the whole pipeline end to end.
- **Tooling:** a per-job timeline (scheduled, fired, claimed, heartbeats, retries, outcome), "why didn't this run?" diagnostics (paused? quota-blocked? overlap-skipped? misfire-skipped?), dry-run showing the next N fire times, manual trigger/requeue/cancel with audit logs, and kill switches (pause a tenant, a job, or all dispatch).
- **Testing:** deterministic simulation of time (a fake clock, to test DST and years of schedules in seconds), **chaos** (kill schedulers and workers, partition the coordination store, skew clocks, pause processes), load tests of the `:00` herd at 2× peak, and invariant checks (no job without a timer, no two active runs where overlap forbids it).

---

## 15. Scaling summary

| Component | Strategy |
|---|---|
| Timer index | Durable, partitioned into many shards by `jobId` hash and time bucket, lookahead window in memory |
| Trigger tier | One fenced-lease owner per shard, timing wheel/heap, atomic fire transaction with outbox, small shards for smooth rebalancing |
| Cron | Store only the next fire, IANA time zones, explicit DST and misfire policies |
| Herd control | Per-job jitter, lookahead pre-load, admission control, per-tenant pacing |
| Dispatch | Durable queues in priority × tenant lanes, weighted fair queuing, reserved critical capacity |
| Workers | Stateless pull, lease + heartbeat + fencing token, sandboxed, autoscaled on queue age |
| Leases | Fast replicated lease store for heartbeats, durable store only for state transitions |
| Recovery | Reaper/sweeper for expired leases and stuck runs, idempotent and sharded |
| Workflows | Event-driven DAG orchestrator with dependency counters, or event-sourced durable execution |
| Storage | Strongly consistent job/run store with conditional writes, history archived to cheap storage |
| Isolation | Per-tenant quotas, concurrency keys, overlap policies, sandboxing, per-target circuit breakers |
| Regions | Home-region ownership, sync AZ replication, async DR, unique-key-protected failover |

---

## 16. Likely follow-up questions

- **"How do you make sure a job runs exactly once?"** Triggering is exactly-once through a unique `(jobId, scheduledTime)` run key plus fenced shard ownership. Execution is at-least-once with leases and heartbeats, so jobs must be idempotent, and the platform supplies run IDs, attempt numbers, and fencing tokens to make that easy.
- **"What if two scheduler nodes both think they own a shard?"** The coordination store issues an epoch with each lease, and the job store rejects writes from stale epochs. The unique constraint on runs is the second safety net.
- **"How do you scale to millions of jobs due at the same second?"** Shard timers into many partitions, pre-load a lookahead window so the fire is a memory operation, write runs in batches, add per-job jitter, and let admission control and queues pace execution. Record "due" fast and execute at a sustainable rate.
- **"How do you detect a dead worker?"** Heartbeats extend a lease, and when the lease expires, the reaper requeues the task. The lease should be about three heartbeat intervals so transient delays don't cause false requeues.
- **"What if a worker finishes after its lease expired and the task was reassigned?"** Its completion is a conditional update that fails because the lease ID changed. Downstream side effects are protected by idempotency keys and fencing tokens.
- **"How do you handle cron and daylight saving time?"** Store the IANA time zone and compute with a tz database. Define the policy for skipped (spring-forward) and repeated (fall-back) wall-clock times, and test around transitions with a fake clock.
- **"What happens to jobs due while the scheduler was down?"** On recovery, the new owner finds fires with `scheduledTime <= now` and no run record, then applies the misfire policy: fire once, skip, or catch up (rate-limited and capped).
- **"How do you prevent one tenant from starving others?"** Per-tenant quotas and concurrency limits, weighted fair queuing within priority lanes, reserved capacity for critical work, and backlog caps with `429` at submission.
- **"What do you do about overlapping runs of a periodic job?"** A per-job overlap policy (allow, skip, replace, queue), evaluated atomically with run creation so concurrent fires can't both pass the check.
- **"How would you implement retries?"** Classify failures as retryable or permanent, use exponential backoff with jitter as a scheduled re-enqueue (reusing the timer machinery), cap attempts and per-tenant retry budgets, and move exhausted tasks to a dead-letter state with replay tooling.
- **"How do you implement dependencies between jobs?"** A DAG orchestrator that consumes completion events and decrements dependency counters with idempotent, compare-and-set state transitions, backed by durable state and a sweeper. For long, branching processes, durable execution with event-sourced replay.
- **"How would you build 'run in 30 days' timers cheaply?"** Keep long timers in the durable timer index only, and load them into memory when they enter the lookahead window, so waiting costs storage but not memory or CPU.
- **"Polling the DB vs. an in-memory timer wheel?"** Polling with `SKIP LOCKED` is simple and correct, and fine until the timer index gets hot. A sharded, leased, lookahead design with an in-memory timing wheel gives precision and scale, with the database still as the source of truth.
- **"How do you run untrusted user code safely?"** Sandboxed containers or microVMs with resource limits, scoped credentials, network egress controls, and per-tenant isolation, because the scheduler is a multi-tenant execution boundary.
- **"How do you know it's healthy?"** Fire lag, queue age, missed-fire count, duplicate-trigger count, lease-expiry rate, DLQ depth, and a canary job monitored from an independent system, plus absence alerts for jobs that silently stop running.
- **"Design the difference between a cron service and Airflow."** A cron service fires independent timers at scale. Airflow adds a DAG model, logical dates and data intervals, backfills, sensors, and rich run history, with the scheduler creating runs and an executor dispatching tasks to workers.
- **"What would you change for sub-second precision?"** Smaller lookahead windows and dedicated, pre-warmed schedulers and workers with reserved capacity, no jitter, kernel-level timer tuning, and a clear price tier, since precision trades away efficiency and herd protection.

---

## 17. How to narrate it

1. **Name the dominant problems:** reliable time-based triggering with no lost or duplicate fires under failures, safe at-least-once execution, fair multi-tenant dispatch, and the `:00` thundering herd. Storage is modest.
2. **Start naive:** one cron process on one box, then a DB table polled by several instances. Show the failures (single point of failure, duplicate fires, hot scans, lost jobs on crash, no retries, no isolation). Then introduce sharded timers with fenced leases, the atomic fire transaction with the unique run key and outbox, the queue and worker leases with heartbeats, the reaper, retries and DLQ, fairness and admission, and finally the DAG layer.
3. **Separate the stages:** trigger (time precision), dispatch (fairness and backpressure), execute (isolation), recover (sweeper), each joined by durable state.
4. **State the contracts:** exactly-once triggering via a unique key, at-least-once execution with idempotency expected from jobs, bounded lateness, fenced ownership everywhere, and a defined misfire policy.
5. **Call out the knobs:** lookahead window, shard count, lease and heartbeat durations, jitter window, misfire and overlap policies, retry and backoff parameters, tenant quotas, lane capacities, and catch-up caps.
6. **For each box, say what happens when it fails,** and end with "any scheduler, worker, or queue node can die, and the job still runs, possibly late and possibly twice on rare paths, but never silently missed, and the effects stay correct because jobs are idempotent."

**Patterns reused from our toolbox:** leases with fencing tokens and epochs (chat sequencing, Uber offers, ticket holds, KV store failover, collaborative editing owners), unique-key idempotent creation (payments, URL shortener, notifications), transactional outbox (feed, payments, ticket booking), at-least-once plus idempotent consumers (Kafka, notifications, crawler), reaper/sweeper recovery of stuck state (Uber dispatch, payments, ticket booking), state machines with compare-and-set transitions (trips, orders, file commits), visibility-timeout queues and DLQs (message queue), consistent hashing and many-small-shards rebalancing (KV store), priority lanes, reserved capacity, and weighted fair queuing (notifications, rate limiter), per-tenant quotas and noisy-neighbor isolation (metrics, Kafka), exponential backoff with jitter and retry budgets (reliability patterns, notifications), circuit breakers per dependency, jittered thundering-herd control (chat reconnects, cache stampedes), timing wheels and delay queues (notification scheduling), event sourcing and replay (collaborative editing op log, Kafka), dead-man's-switch and absence alerting (metrics), and static stability with consensus kept off the hot path.

---

That completes the full series: the applied systems (URL shortener, feed, Top-K, chat, rate limiter, crawler, Uber, Dropbox, video, notifications, payments, ticket booking, typeahead, metrics, collaborative editing, job scheduler) and the building blocks (cache/KV store, message queue).

Next, I can play interviewer on this (for example, "a deploy bug causes every hourly job to fire twice, and some jobs charge customers, walk me through containment, remediation, and prevention"), or we can wrap up with the **cross-cutting review**: a one-page pattern cheat sheet, a "which pattern for which problem" decision guide, and a mixed mock-interview round that pulls questions from several of these designs.
