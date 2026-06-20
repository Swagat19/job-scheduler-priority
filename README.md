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

> _(Filled in as the project evolves — kept honest and specific so each item maps
> to a real commit. WIP — see roadmap at the bottom.)_

- [ ] **Priority-based dispatch** — replace Redis FIFO list with a sorted set
      (`ZADD` / `BZPOPMIN`); 10 priority levels with FIFO tiebreaking via
      timestamp suffix on the score.
- [ ] **Schema migration** — add `priority` column to `jobs` (indexed) and
      surface it on the submitter API.
- [ ] **Benchmark suite** — reproducible load tests measuring throughput,
      p50 / p95 / p99 latency, and high-priority head-of-line latency.
- [ ] **Grafana panels** — jobs-by-priority counter, priority-aware queue depth.

---

## Architecture

Seven services orchestrated via Docker Compose:

| Service       | Tech                          | Role                                                      |
| ------------- | ----------------------------- | --------------------------------------------------------- |
| Submitter     | Go (`:8000`)                  | REST API; persists job to Postgres, enqueues to Redis     |
| Coordinator   | Go (`:9000`)                  | LRU worker selection, lease management, retry/DLQ logic   |
| Worker × 3    | Python + FastAPI (`:7001-3`)  | Pulls and processes jobs; sends heartbeats; reports back  |
| Postgres      | `:5432`                       | Durable store for `jobs` and `workers` tables             |
| Redis         | `:6379`                       | Queues (ready / results / DLQ) and worker heartbeats      |
| Prometheus    | `:9090`                       | Scrapes metrics from coordinator and workers              |
| Grafana       | `:3000`                       | Pre-provisioned monitoring dashboard                      |

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
  `dead_letter_queue` for inspection (placeholder for alerting).

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

| What             | URL                                           |
| ---------------- | --------------------------------------------- |
| Submitter API    | http://localhost:8000                         |
| Coordinator      | http://localhost:9000                         |
| Grafana          | http://localhost:3000  (admin / admin)        |
| Prometheus       | http://localhost:9090                         |
| Postgres         | `localhost:5432`  (scheduler_user/scheduler_password) |
| Redis            | `localhost:6379`                              |

### Submit a job

```bash
curl -X POST http://localhost:8000/submit_job \
  -H "Content-Type: application/json" \
  -d '{"name": "cpu_intensive", "payload": "task1"}'
```

Once priority dispatch lands, jobs accept a `priority` field (1 = highest, 10 = lowest):

```bash
curl -X POST http://localhost:8000/submit_job \
  -H "Content-Type: application/json" \
  -d '{"name": "cpu_intensive", "payload": "urgent", "priority": 1}'
```

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
  -c "SELECT id, name, status, retries, leased_to_worker FROM jobs ORDER BY id DESC LIMIT 10;"

# Redis
docker exec -it redis redis-cli LLEN job_queue
docker exec -it redis redis-cli LLEN dead_letter_queue
docker exec -it redis redis-cli KEYS 'worker:*'
```

### Tear down

```bash
make down       # stop containers
make clean      # remove containers + volumes + images
```

---

## Job Types (Worker Simulators)

The worker (`worker/main.py`) ships with five simulators selected by `name`:

| `name`              | Workload                                          |
| ------------------- | ------------------------------------------------- |
| `cpu_intensive`     | SHA-256 hashing, 5k–15k iterations in thread pool |
| `io_intensive`      | Write/read/delete a 1k–10k line file              |
| `mixed_workload`    | CPU + IO chained                                  |
| `network_task`      | 1–5 concurrent HTTP calls to httpbin.org/delay    |
| _anything else_     | Random sleep ("variable work")                    |

Sending `payload: "invalid_job"` forces a deterministic failure — useful for
exercising the retry + DLQ path.

---

## Roadmap

- [x] Initial import + attribution
- [ ] Priority queue (Redis sorted set)
- [ ] Benchmark scripts + result tables
- [ ] Coordinator HA via Redis leader election
- [ ] Real (non-simulated) job execution: register Python callables by name
- [ ] gRPC between coordinator and workers
- [ ] Web UI for job/worker inspection

---

## License

Same as the upstream project. See
[soum-sr/distributed_job_scheduler](https://github.com/soum-sr/distributed_job_scheduler)
for original license terms.
