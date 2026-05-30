# Distributed Job Queue — Design Document

A horizontally-scalable, at-least-once job queue built in Go on Redis, designed to
sustain 1M jobs/day with crash-safe delivery, exponential-backoff retries, and a
migration path to Redis Cluster.

---

## 1. Goals and Non-Goals

### Goals
- **At-least-once delivery.** No job is silently lost, even if a worker dies mid-execution.
- **Crash recovery.** A job claimed by a worker that then crashes is automatically requeued.
- **Retry with exponential backoff + jitter**, a bounded attempt count, and a dead-letter queue.
- **Scheduled / delayed jobs** ("run at T").
- **I/O-bound throughput.** Sustain 1M jobs/day (~12/s average, with bursts well above).
- **Observability.** Queue depth, throughput, failure rate, retry rate, claim latency.
- **A status API + CLI** for submitting, inspecting, and managing jobs (demoability).
- **Horizontal scale** via Redis Cluster, sharded by queue.

### Non-Goals (explicitly out of scope, and why)
- **Exactly-once delivery.** Genuinely impossible without distributed transactions across the
  worker's side effects; we choose at-least-once + idempotent handlers instead, which is what
  production systems actually do.
- **Strict global ordering.** With concurrent workers, FIFO is best-effort per queue, not a guarantee.
- **A general workflow/DAG engine** (job dependencies, fan-out/fan-in). This is a queue, not Airflow.

---

## 2. Delivery Semantics — and the justification

We choose **at-least-once + idempotent handlers**.

The alternatives and why they were rejected:

- **At-most-once** (pop and run; if the worker dies, the job is gone) is simpler but loses work
  on every crash. Unacceptable for the reliability goal. *This is what most tutorial queues
  silently implement.*
- **Exactly-once** would require atomically committing both the job's removal from the queue
  AND the handler's external side effect (the email sent, the row written) in one transaction.
  Redis cannot transact across an external system, so true exactly-once is unachievable here.

At-least-once means **a job may run more than once** (e.g. a worker finishes the work, then dies
before acking — the reaper requeues it and it runs again). The cost of this is pushed onto the
handler: **handlers must be idempotent.** We support this with an idempotency key per job so
handlers can dedupe their own side effects.

---

## 3. System Architecture

```
                    ┌──────────────┐
   producers ─────► │  ENQUEUE     │  (Lua: write payload hash + push ID)
                    └──────┬───────┘
                           │
                    ┌──────▼───────────────────────────────────────┐
                    │              REDIS (per queue)                │
                    │                                               │
                    │  pending      LIST   [id, id, id, ...]        │
                    │  processing   ZSET   {id: lease_expiry}       │◄── reaper scans here
                    │  delayed      ZSET   {id: run_at_timestamp}   │◄── poller scans here
                    │  job:<id>     HASH   {payload, state, attempts,│
                    │                       last_error, ...}        │
                    │  dlq          LIST   [id, id, ...]            │
                    └──────┬─────────────────────────────▲──────────┘
                           │ claim (Lua)                  │ ack / retry (Lua)
                    ┌──────▼──────┐                       │
                    │   WORKER    │───── run handler ─────┘
                    │  (goroutine │       success → ack
                    │   pool, N)  │       failure → retry (back into `delayed`)
                    └─────────────┘

   ┌─────────┐   promote due jobs        ┌─────────┐  requeue expired leases
   │ POLLER  │   delayed → pending       │ REAPER  │  processing → pending
   └─────────┘   (Lua, periodic)         └─────────┘  (Lua, periodic)
```

### Components
- **Producer** — validates and enqueues a job: writes the payload hash and pushes the job ID
  onto `pending` (or onto `delayed` if scheduled). One atomic Lua op.
- **Worker** — runs a bounded pool of goroutines. Each loops: claim → run handler → ack or retry.
- **Poller** — periodically promotes jobs in `delayed` whose `run_at` has passed into `pending`.
  Also runs the on-start recovery sweep.
- **Reaper** — periodically scans `processing` for jobs whose lease has expired (worker died/hung)
  and requeues them into `pending`.
- **Status API / CLI** — submit, status, list, cancel, logs.

### Design choices adapted from the reference diagrams
The reference (WebDevSimplified) design contributed three good ideas we keep:
1. **ID/payload split** — the sorted sets and lists hold only job *IDs*; payloads live in a
   separate `job:<id>` hash. This keeps the ZSETs small and fast.
2. **Delayed ZSET scored by run-at timestamp**, with a poller promoting due jobs.
3. **On-start recovery sweep** to promote already-due jobs after a restart.

What the reference lacked — and what this design adds as its core contribution:
- An **in-flight `processing` ZSET + a reaper** for crash recovery (the missing "job done" /
  "worker died" path).
- **Retry with backoff + DLQ**, which elegantly *reuses* the `delayed` ZSET — a job to retry is
  simply a job scheduled to run again later. Backoff and scheduling are the same mechanism.

---

## 4. Why Lua (atomicity)

The critical operations — claim, ack, retry, reap, promote — each touch **multiple keys** and
must happen as one indivisible unit. A naive two-step Go implementation (`RPOP` then `ZADD`) has
a gap: if the process dies between the steps, or another worker interleaves, jobs are lost or
double-processed.

Redis executes a Lua script **to completion without interleaving any other command**. So we ship
each multi-key operation as one Lua script — effectively a custom atomic command. The dangerous
multi-step logic runs inside the atomic boundary; the Go code just calls the script.

**Cluster note:** in Redis Cluster, every key a script touches must hash to the same slot. We
force this with **hash tags** — wrapping the queue name in braces so only that part is hashed:
`queue:{emails}:pending`, `queue:{emails}:processing`, `job:{emails}:<id>`. All keys for one queue
route to one node. Consequence: we **shard by queue**; one queue lives on one node and scales
horizontally across many queues.

---

## 5. Redis Key Schema (Cluster-ready)

For a queue named `Q`, with hash tag `{Q}`:

| Key                      | Type | Contents                                              |
|--------------------------|------|-------------------------------------------------------|
| `queue:{Q}:pending`      | LIST | Job IDs ready to run (FIFO)                            |
| `queue:{Q}:processing`   | ZSET | In-flight job IDs, scored by **lease-expiry** unix ts |
| `queue:{Q}:delayed`      | ZSET | Scheduled/retry job IDs, scored by **run-at** unix ts |
| `queue:{Q}:dlq`          | LIST | Dead-lettered job IDs (retries exhausted)             |
| `job:{Q}:<id>`           | HASH | `payload`, `state`, `attempts`, `max_attempts`, `last_error`, `idempotency_key`, timestamps |

Job `state` ∈ `{pending, processing, delayed, succeeded, failed, dead}`.

---

## 6. The Lua Scripts

> Annotated below in pseudocode-faithful Lua. `KEYS` are the keys; `ARGV` are the arguments.

### 6.1 Enqueue (immediate)
*Race prevented: payload and queue entry must both exist, or neither.*
```lua
-- KEYS[1]=pending list, KEYS[2]=job hash
-- ARGV[1]=job_id, ARGV[2]=payload, ARGV[3]=max_attempts, ARGV[4]=now, ARGV[5]=idem_key
redis.call('HSET', KEYS[2],
  'payload', ARGV[2], 'state', 'pending', 'attempts', 0,
  'max_attempts', ARGV[3], 'enqueued_at', ARGV[4], 'idempotency_key', ARGV[5])
redis.call('LPUSH', KEYS[1], ARGV[1])
return ARGV[1]
```

### 6.2 Claim
*Race prevented: a popped job is ALWAYS recorded as in-flight with a lease; no gap for a crash
or a competing worker.*
```lua
-- KEYS[1]=pending list, KEYS[2]=processing zset, KEYS[3..]=resolved per popped job hash
-- ARGV[1]=now, ARGV[2]=visibility_timeout_seconds
local job_id = redis.call('RPOP', KEYS[1])
if not job_id then return nil end
local expiry = tonumber(ARGV[1]) + tonumber(ARGV[2])
redis.call('ZADD', KEYS[2], expiry, job_id)
-- job hash key derived from job_id by the caller and passed in; update state
redis.call('HSET', 'job:' .. job_id, 'state', 'processing', 'claimed_at', ARGV[1])
return job_id
```
*(In practice the job-hash key is passed in `KEYS` to satisfy Cluster slot rules; shown inline
here for readability.)*

### 6.3 Ack (success)
*Race prevented: removal from `processing` and the state write are atomic, so the reaper can never
requeue a job that actually succeeded.*
```lua
-- KEYS[1]=processing zset, KEYS[2]=job hash
-- ARGV[1]=job_id, ARGV[2]=now
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed == 0 then return 0 end          -- lease already expired & reaped; do nothing
redis.call('HSET', KEYS[2], 'state', 'succeeded', 'finished_at', ARGV[2])
return 1
```

### 6.4 Retry (failure, with backoff)
*Race prevented: the job moves from `processing` to `delayed` in one step; it can never be in both
or neither. Reuses the delayed ZSET — a retry is just a scheduled job.*
```lua
-- KEYS[1]=processing zset, KEYS[2]=delayed zset, KEYS[3]=dlq list, KEYS[4]=job hash
-- ARGV[1]=job_id, ARGV[2]=now, ARGV[3]=next_run_at, ARGV[4]=error
redis.call('ZREM', KEYS[1], ARGV[1])
local attempts = tonumber(redis.call('HGET', KEYS[4], 'attempts')) + 1
local max = tonumber(redis.call('HGET', KEYS[4], 'max_attempts'))
redis.call('HSET', KEYS[4], 'attempts', attempts, 'last_error', ARGV[4])
if attempts >= max then
  redis.call('LPUSH', KEYS[3], ARGV[1])     -- dead-letter
  redis.call('HSET', KEYS[4], 'state', 'dead')
  return 'dead'
else
  redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])   -- schedule retry
  redis.call('HSET', KEYS[4], 'state', 'delayed')
  return 'retry'
end
```

### 6.5 Promote (poller: delayed → pending)
*Race prevented: each due job is moved exactly once even if multiple pollers run.*
```lua
-- KEYS[1]=delayed zset, KEYS[2]=pending list
-- ARGV[1]=now, ARGV[2]=batch_limit
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, job_id in ipairs(due) do
  redis.call('ZREM', KEYS[1], job_id)
  redis.call('LPUSH', KEYS[2], job_id)
end
return #due
```

### 6.6 Reap (reaper: expired leases → pending)
*Race prevented: a job whose lease expired is requeued exactly once; a slow-but-alive worker that
later acks will find it already ZREM'd (see Ack returning 0).*
```lua
-- KEYS[1]=processing zset, KEYS[2]=pending list
-- ARGV[1]=now, ARGV[2]=batch_limit
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, job_id in ipairs(expired) do
  redis.call('ZREM', KEYS[1], job_id)
  redis.call('LPUSH', KEYS[2], job_id)
end
return #expired
```

---

## 7. Exponential Backoff

On failure number `n` (1-indexed), schedule the next attempt at:

```
delay  = min(base * 2^(n-1), cap)
jitter = random in [0.5, 1.0]
next_run_at = now + delay * jitter
```

- **`base`** ≈ 1s, **`cap`** ≈ a few minutes, **`max_attempts`** ≈ 5 (all configurable).
- **Why jitter:** without it, a batch of jobs that all failed at the same instant (e.g. a downstream
  API blipped) would all retry at the *exact same future instant*, hammering the recovering service
  — the "thundering herd." Jitter spreads the retries out.
- **Why a cap:** prevents attempt 20 from being scheduled years out.
- After `max_attempts`, the job is dead-lettered for inspection/manual replay.

---

## 8. Worker Concurrency (I/O-bound)

Jobs are I/O-bound (API calls, DB writes, emails), so goroutines spend most of their time parked
on network I/O, not burning CPU. Each worker process runs a **bounded pool** — a semaphore /
buffered channel of size `N` (start 50–100) — capping concurrent in-flight jobs.

The cap protects **downstream dependencies** more than local CPU: it stops the queue from becoming
a DDoS against the API you're calling. Tune `N` against the downstream's rate limits.

---

## 9. Graceful Shutdown

On `SIGTERM`:
1. Stop claiming new jobs.
2. Let in-flight goroutines finish within a deadline (e.g. 30s).
3. Anything not finished is **left to the reaper** — its lease will expire and it'll be requeued.

We deliberately do **not** hand-requeue on shutdown. The lease + reaper already handle it correctly;
adding a second path just creates a race. This is the payoff of the lease design.

---

## 10. Observability

Export via Prometheus:
- **Queue depth** per queue (`LLEN pending`, `ZCARD delayed`, `ZCARD processing`, `LLEN dlq`).
- **Throughput** (jobs acked/sec), **failure rate**, **retry rate**, **dead-letter rate**.
- **Claim latency** and **end-to-end latency** (enqueued_at → finished_at) histograms (p50/p95/p99).
- Worker pool utilization.

Run-by-dashboards: these metrics are how a production queue is operated, and the load-test graphs
come straight from them.

---

## 11. Redis Cluster & Sharding

- **Shard by queue** using hash tags (Section 4). Each queue's keys live on one node.
- Many queues spread across the cluster → horizontal scale.
- A single queue is bounded by one node's capacity — acceptable, and a trade-off worth stating.
- Build and debug all logic on **single-node Redis first**; migrate to Cluster only in Phase 6,
  so Cluster's constraints aren't fighting you while the core logic is still being proven.

---

## 12. Go Package Layout

```
job-queue/
├── cmd/
│   ├── worker/        main() for a worker process
│   ├── server/        main() for the status API
│   └── cli/           submit/status/list/cancel/logs
├── internal/
│   ├── queue/         enqueue, claim, ack, retry — wraps the Lua scripts
│   ├── scripts/       *.lua files, embedded via //go:embed
│   ├── worker/        goroutine pool, handler dispatch, graceful shutdown
│   ├── reaper/        lease-expiry sweep
│   ├── poller/        delayed→pending promotion + on-start recovery
│   ├── job/           Job struct, state enum, serialization
│   └── metrics/       Prometheus collectors
├── pkg/redisx/        Redis client setup (single + Cluster modes)
├── test/load/         load generator for the 1M/day test
└── DESIGN.md
```

---

## 13. Build Plan (phased)

Each phase ends with a **test that proves its property** — that's the difference between
"I built X" and "I built X and here's proof it survives Y."

| Phase | Deliverable | Proof / exit test |
|-------|-------------|-------------------|
| **0** | This DESIGN.md, repo skeleton, docker-compose with single Redis | Doc reviewed; `go build` passes |
| **1** | Enqueue + claim + ack; Lua scripts; single Redis | Push 1k jobs, all processed exactly once on happy path |
| **2** | Lease/visibility timeout + reaper | **`kill -9` a worker mid-job → job is reclaimed and completes.** (Headline demo.) |
| **3** | Retry + exponential backoff + jitter + DLQ | Deterministically failing handler → job retries with growing delays, lands in DLQ after max attempts |
| **4** | Delayed-job poller + on-start recovery + graceful shutdown | Schedule a job for T+30s → runs at T+30s; SIGTERM drains in-flight jobs |
| **5** | Prometheus metrics + status API + CLI | Dashboard shows live depth/throughput; CLI submit/status/list/cancel/logs work |
| **6** | Redis Cluster + hash tags + load test | Sustained 1M jobs/day across a multi-node cluster; capture p99 claim latency + throughput graph |
| **7** | *(stretch)* priority queues, per-queue rate limiting, idempotency-key dedup, web dashboard | Each is a talking point; none is load-bearing |

---

## 14. Resume Framing

Describe it by its hard parts, not the generic category:

> *Built an at-least-once distributed job queue in Go on Redis Cluster with lease-based crash
> recovery, exponential-backoff retries, and a dead-letter queue; sustained 1M jobs/day at p99
> claim latency of X ms. Atomic claim/ack/retry/reap implemented as Redis Lua scripts; sharded by
> queue via hash tags.*

The differentiators a senior reviewer looks for, all present here: a justified delivery semantic,
crash recovery you can demo live, atomicity isolated in auditable scripts, a real load-tested
number, dashboards, and a design doc that documents the trade-offs rejected.
