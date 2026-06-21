# Job Scheduler with Priority Queues

A distributed job scheduling system written in Go and Python, with Redis-backed
priority queues, lease-based at-least-once delivery, exponential-backoff retries,
a dead-letter queue, and full Prometheus + Grafana observability.

> **Attribution.** This project is built on top of
> [soum-sr/distributed_job_scheduler](https://github.com/soum-sr/distributed_job_scheduler),
> which provides the base FIFO architecture (coordinator, workers, lease monitor,
> heartbeat verifier, retry/DLQ pipeline). My contributions are listed below; the
> rest is upstream code I am studying, extending, and operating.

---

## My Contributions

- **Priority-based dispatch** — replaces the upstream FIFO Redis list with
a sorted set keyed by `priority * 1e13 + unix_millis`. 10 priority
levels (1 = highest, 10 = lowest), FIFO tiebreaking within each band.
Submitter migrated to `ZADD`, coordinator migrated to `BZPOPMIN`, and
every requeue path (failed jobs, lease timeouts, worker errors) now
preserves priority via a shared `priorityScore()` helper.
- **Schema migration** — `priority INT DEFAULT 5 CHECK (priority BETWEEN 1 AND 10)` on `jobs`, with a `(priority, status)` btree index for
inspection queries.
- **Submitter API** — accepts an optional `priority` field, validates the
range, returns HTTP 400 on out-of-bounds, falls back to the column
default of 5 when omitted.
- **Metrics migration** — `jobs_in_queue` Prometheus gauge switched from
`LLEN` to `ZCARD` so Grafana stays accurate after the queue type
change.
- **Demo script** — `scripts/demo_priority_queue.sh` runs two scenarios
(saturated queue + late VIP, mixed priority bands under load) and
prints completion-rank tables. Reproducible proof, ~3 minutes runtime.

---

## Architecture

Seven services orchestrated via Docker Compose:


| Service     | Tech                         | Role                                                     |
| ----------- | ---------------------------- | -------------------------------------------------------- |
| Submitter   | Go (`:8000`)                 | REST API; persists job to Postgres, enqueues to Redis    |
| Coordinator | Go (`:9000`)                 | LRU worker selection, lease management, retry/DLQ logic  |
| Worker × 3  | Python + FastAPI (`:7001-3`) | Pulls and processes jobs; sends heartbeats; reports back |
| Postgres    | `:5432`                      | Durable store for `jobs` and `workers` tables            |
| Redis       | `:6379`                      | Queues (ready / results / DLQ) and worker heartbeats     |
| Prometheus  | `:9090`                      | Scrapes metrics from coordinator and workers             |
| Grafana     | `:3000`                      | Pre-provisioned monitoring dashboard                     |


Job lifecycle (happy path):

```
client ──POST /submit_job──► Submitter ──INSERT──► Postgres
                                  │
                                  └──ZADD job_queue (priority, ts)──► Redis
                                                                        │
                                Coordinator ◄──BZPOPMIN job_queue───────┘
                                  │
                                  ├── pick LRU worker (FOR UPDATE)
                                  ├── UPDATE jobs SET status='leased', lease_start=NOW()
                                  └──POST /run_job──► Worker
                                                        │
                                                        ├── execute simulator
                                                        └──LPUSH job_results──► Redis
                                                                                  │
                                Coordinator ◄──BRPOP job_results──────────────────┘
                                  │
                                  └── UPDATE jobs SET status='completed' / retry / DLQ
```

Resilience:

- **Lease timeouts** — if a worker dies mid-job, the lease monitor reclaims the
job after `lease_timeout` seconds and requeues with exponential backoff + jitter.
- **Heartbeats** — workers `SET worker:<url> alive EX 30` every 10s; the
heartbeat verifier flips missing workers to `unavailable`, removing them
from LRU selection.
- **Dead Letter Queue** — jobs that exhaust `MAX_RETRIES` land in
`dead_letter_queue` for inspection and replay.

---

## Quick Start

### Prerequisites

- Docker + Docker Compose
- Go 1.21+ (only for local dev outside containers)
- Python 3.9+ (only for local dev outside containers)

### Boot the stack

```bash
make up
```

### Service URLs


| What          | URL                                                            |
| ------------- | -------------------------------------------------------------- |
| Submitter API | [http://localhost:8000](http://localhost:8000)                 |
| Coordinator   | [http://localhost:9000](http://localhost:9000)                 |
| Grafana       | [http://localhost:3000](http://localhost:3000) (admin / admin) |
| Prometheus    | [http://localhost:9090](http://localhost:9090)                 |
| Postgres      | `localhost:5432` (scheduler_user/scheduler_password)           |
| Redis         | `localhost:6379`                                               |


### Submit a job

Without priority (defaults to 5):

```bash
curl -X POST http://localhost:8000/submit_job \
  -H "Content-Type: application/json" \
  -d '{"name": "cpu_intensive", "payload": "task1"}'
```

With priority (1 = highest, 10 = lowest, range enforced server-side):

```bash
curl -X POST http://localhost:8000/submit_job \
  -H "Content-Type: application/json" \
  -d '{"name": "cpu_intensive", "payload": "urgent", "priority": 1}'
```

A high-priority job submitted while the queue has lower-priority jobs waiting
will be dispatched **next**, regardless of submission order. Jobs already
running on a worker are not preempted (see [Design Choices](#design-choices)).

### Sample workloads

```bash
make submit-test-jobs                    # 3 mixed jobs
./scripts/send_cpu_intensive_jobs.sh     # 10 CPU-bound
./scripts/send_io_intensive_jobs.sh      # 10 IO-bound
./scripts/send_failing_jobs.sh           # exercise retry + DLQ
./scripts/high_volume_stress_test.sh     # throughput stress
```

### Inspect state live

```bash
# Postgres
docker exec -it postgres psql -U scheduler_user -d scheduler_db \
  -c "SELECT id, name, status, priority, retries, leased_to_worker
      FROM jobs ORDER BY id DESC LIMIT 10;"

# Redis — note: job_queue is a sorted set, dead_letter_queue is a list
docker exec -it redis redis-cli ZCARD job_queue
docker exec -it redis redis-cli ZRANGE job_queue 0 4 WITHSCORES   # peek at priority head
docker exec -it redis redis-cli LLEN dead_letter_queue
docker exec -it redis redis-cli KEYS 'worker:*'
```

### Run the priority-queue demo

```bash
./scripts/demo_priority_queue.sh
```

Two scenarios, ~3 minutes total. See [Demo & Sample Run Results](#demo--sample-run-results)
for what it produces.

### Tear down

```bash
make down       # stop containers
make clean      # remove containers + volumes + images
```

---

## Job Types (Worker Simulators)

The worker (`worker/main.py`) ships with five simulators selected by `name`:


| `name`           | Workload                                          |
| ---------------- | ------------------------------------------------- |
| `cpu_intensive`  | SHA-256 hashing, 5k–15k iterations in thread pool |
| `io_intensive`   | Write/read/delete a 1k–10k line file              |
| `mixed_workload` | CPU + IO chained                                  |
| `network_task`   | 1–5 concurrent HTTP calls to httpbin.org/delay    |
| *anything else*  | Random sleep ("variable work")                    |


Sending `payload: "invalid_job"` forces a deterministic failure — useful for
exercising the retry + DLQ path.

---

## Demo & Sample Run Results

Run the bundled demo to see priority dispatch end-to-end:

```bash
./scripts/demo_priority_queue.sh
```

The script runs two scenarios in ~3 minutes and prints completion-rank tables.

### Scenario 1 — Saturated queue + late VIP

Submits 30 LOW-priority jobs in parallel (`xargs -P` so the queue actually
backs up), then injects a single HIGH-priority VIP and measures where it
lands in the completion order.

Sample run on this repo (3 workers, `mixed_workload` jobs):


| Metric                         | Value                                  |
| ------------------------------ | -------------------------------------- |
| VIP submission position        | 31st                                   |
| VIP completion rank            | **5th**                                |
| LOW jobs ahead of VIP at start | 4 in flight (3 workers + 1 in handoff) |


The 4 jobs that finished before VIP were already running on workers when
VIP entered the queue. Every dispatch decision after that picks VIP. Non-
preemptive scheduling cannot do better than this without breaking
at-least-once delivery.

### Scenario 2 — Mixed priorities under load (P=1, P=5, P=10)

Submits 35 jobs across three priority bands (5 HIGH, 10 MED, 20 LOW) and
aggregates average completion rank per priority.

Sample run on this repo (3 workers, `mixed_workload` jobs):


| Priority | Jobs | Avg completion rank | First | Last |
| -------- | ---- | ------------------- | ----- | ---- |
| 1 (HIGH) | 5    | 3.4                 | 1     | 6    |
| 5 (MED)  | 10   | 10.3                | 4     | 15   |
| 10 (LOW) | 20   | 25.5                | 16    | 35   |


Average completion rank grows monotonically with priority value. HIGH jobs
finish in the first 6 ranks, LOW jobs in the last 20. The bands overlap by
just one rank because `mixed_workload` has variable per-job duration; a
fixed-duration test would show clean separation.

The Grafana **Jobs in Queue Over Time** panel shows two distinct peaks
during a demo run (one per scenario), each spiking to ~30 and draining to
zero — a useful visual signature of priority dispatch at work.

---

## Design Choices

### Score encoding

The sorted-set score is `priority * 1e13 + unix_millis`, computed identically
in submitter and coordinator (`PriorityScoreMultiplier` constant in both).
Properties:

- Each priority band occupies a clean `1e13`-wide slot — bands cannot
overlap.
- Within a band, scores are ordered by submission timestamp, so jobs at the
same priority are dispatched FIFO.
- Stays within `float64` integer precision (2^53 ≈ 9 × 10^15) for ~317 years
before priority bands could collide.
- Trivially observable: `redis-cli ZRANGE job_queue 0 -1 WITHSCORES` shows
the priority band in the leading digits of the score.

### Retry timestamp policy

Requeued jobs (failed jobs and lease-timeout jobs) use the **current**
timestamp in their score, not their original submission time. This sends
them to the back of their priority band on retry. Trade-off:


|         |                                                                     |
| ------- | ------------------------------------------------------------------- |
| **Pro** | A flapping job cannot starve fresh submissions of the same priority |
| **Con** | A long-pending job that fails once gets bumped back slightly        |


Exponential backoff already imposes a delay (1s → 2s → 4s + jitter, capped
at 60s), so the practical cost is negligible. This matches Sidekiq and
Celery semantics.

### Non-preemptive scheduling

A high-priority job jumps the wait queue but **cannot displace jobs already
in flight on a worker**. Preemption would require either cooperative
cancellation (workers periodically polling for cancel signals) or process
kills, both of which break the at-least-once delivery semantics this system
guarantees. So the head-of-line latency for a P=1 job is bounded by:

> at most `N - 1` running jobs ahead of it, where `N` = number of workers

For our demo with 3 workers and ~1.5s `mixed_workload`, that's ~3 seconds
worst-case head-of-line latency — visible in the Scenario 1 result above
(VIP completed 5th out of 31, four jobs ahead all P=10).

### Why a sorted set, not multiple per-priority FIFO lists?

Two designs were considered:

1. **Single sorted set** (chosen) — `O(log n)` insert and pop.
2. **One Redis list per priority level + weighted polling** — `O(1)` ops
  but you have to either (a) loop through priorities each pop (starves
   low priority unless you add aging) or (b) maintain a weighting policy.

The sorted-set design is simpler to reason about, simpler to inspect
(`ZRANGE` is human-readable), and the `O(log n)` factor is irrelevant at
realistic queue sizes.

