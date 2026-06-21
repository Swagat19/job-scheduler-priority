#!/bin/bash
#
# demo_priority_queue.sh
#
# Demonstrates priority-based dispatch in two scenarios:
#
#   1. Saturated queue + late VIP
#      Spam 30 LOW-priority jobs to build a backlog, then inject a single
#      HIGH-priority VIP and measure where it lands in the completion order.
#
#   2. Mixed priorities under load
#      Submit a batch with three priority bands (P=10, P=5, P=1) and check
#      that average completion rank correlates with priority.
#
# Both scenarios use parallel submission so the queue actually backs up
# (and we don't drain faster than we submit). They use the mixed_workload
# job type so each job takes ~1-2s on the worker, giving the priority
# queue time to do its job.
#
# Usage:
#   ./scripts/demo_priority_queue.sh

set -e

SUBMITTER=${SUBMITTER:-http://localhost:8000}
PARALLEL=${PARALLEL:-30}

banner() {
    echo
    echo "============================================================"
    echo "  $1"
    echo "============================================================"
}

step() {
    echo
    echo "-- $1"
}

post_job() {
    local name=$1 payload=$2 priority=$3
    curl -s -X POST "$SUBMITTER/submit_job" \
        -H "Content-Type: application/json" \
        -d "{\"name\":\"$name\",\"payload\":\"$payload\",\"priority\":$priority}" > /dev/null
}
export -f post_job
export SUBMITTER

post_many() {
    local count=$1 name=$2 prefix=$3 priority=$4
    seq 1 "$count" | xargs -P "$PARALLEL" -I {} \
        bash -c "post_job '$name' '${prefix}_{}' $priority"
}

queue_size() {
    docker exec redis redis-cli ZCARD job_queue
}

queue_head() {
    echo ">> Queue head (lowest score = highest dispatch priority):"
    docker exec redis redis-cli ZRANGE job_queue 0 4 WITHSCORES
}

prereqs() {
    if ! docker ps --format '{{.Names}}' | grep -q '^submitter$'; then
        echo "ERROR: stack not running. Start it with 'make up' first." >&2
        exit 1
    fi
    # Verify reachability by sending a sentinel job and checking HTTP 201.
    local code
    code=$(curl -s -o /dev/null -w '%{http_code}' \
        -X POST "$SUBMITTER/submit_job" \
        -H 'Content-Type: application/json' \
        -d '{"name":"_prereq_check","payload":"_prereq","priority":5}')
    if [ "$code" != "201" ]; then
        echo "ERROR: submitter at $SUBMITTER returned HTTP $code (expected 201)." >&2
        echo "Container status:" >&2
        docker ps --format 'table {{.Names}}\t{{.Status}}' >&2
        exit 1
    fi
}

prereqs

# ============================================================
# SCENARIO 1: Saturated queue + late VIP
# ============================================================
banner "SCENARIO 1 :: Saturated queue + late VIP"

RUN_TAG="demo1_$(date +%s)"
echo "Run tag: $RUN_TAG"

step "Submitting 30 LOW-priority jobs (priority=10) in parallel"
post_many 30 mixed_workload "${RUN_TAG}_low" 10

# Brief pause so all 30 land in the queue before VIP
sleep 0.5

step "Queue state after the spam"
echo ">> Queue size: $(queue_size)"
queue_head

step "Submitting 1 VIP (priority=1)"
post_job mixed_workload "${RUN_TAG}_VIP" 1

step "Queue state immediately after VIP arrives"
echo ">> Queue size: $(queue_size)"
queue_head
echo ">> Notice: VIP should be at position 0 (lowest score, ~1.7e13 band)."
echo "   The 27+ remaining lows should have scores in the ~1.7e14 band."

step "Draining queue (waiting up to 60s)..."
for _ in $(seq 1 60); do
    [ "$(queue_size)" = "0" ] && break
    sleep 1
done
sleep 2  # let last results post-process

step "Completion order (first 10 jobs to finish)"
docker exec postgres psql -U scheduler_user -d scheduler_db -c "
SELECT
  ROW_NUMBER() OVER (ORDER BY completed_at) AS rank,
  payload,
  priority,
  to_char(completed_at, 'HH24:MI:SS.MS') AS completed
FROM jobs
WHERE payload LIKE '${RUN_TAG}_%' AND status = 'completed'
ORDER BY completed_at
LIMIT 10;"

step "VIP's rank in the completion order"
docker exec postgres psql -U scheduler_user -d scheduler_db -c "
WITH ordered AS (
  SELECT payload, priority,
         ROW_NUMBER() OVER (ORDER BY completed_at) AS rank
  FROM jobs
  WHERE payload LIKE '${RUN_TAG}_%' AND status = 'completed'
)
SELECT rank, payload, priority FROM ordered WHERE payload = '${RUN_TAG}_VIP';"

echo
echo "EXPECTED: VIP rank should be small (typically 1-5)."
echo "  At most 3 LOW jobs were already in flight when VIP arrived"
echo "  (one per worker). Every dispatch after that picks VIP."
echo "  This is non-preemptive scheduling working as designed."


# ============================================================
# SCENARIO 2: Mixed priorities under load
# ============================================================
banner "SCENARIO 2 :: Mixed priorities under load (P=1, P=5, P=10)"

RUN_TAG="demo2_$(date +%s)"
echo "Run tag: $RUN_TAG"

step "Submitting 20 LOW (P=10), 10 MEDIUM (P=5), 5 HIGH (P=1) in parallel"
post_many 20 mixed_workload "${RUN_TAG}_low"  10
post_many 10 mixed_workload "${RUN_TAG}_med"   5
post_many  5 mixed_workload "${RUN_TAG}_high"  1

sleep 0.5

step "Queue state after submission"
echo ">> Queue size: $(queue_size)"
queue_head

step "Draining queue (waiting up to 90s)..."
for _ in $(seq 1 90); do
    [ "$(queue_size)" = "0" ] && break
    sleep 1
done
sleep 2

step "First 12 completions (should be dominated by HIGH then MED)"
docker exec postgres psql -U scheduler_user -d scheduler_db -c "
SELECT
  ROW_NUMBER() OVER (ORDER BY completed_at) AS rank,
  payload,
  priority,
  to_char(completed_at, 'HH24:MI:SS.MS') AS completed
FROM jobs
WHERE payload LIKE '${RUN_TAG}_%' AND status = 'completed'
ORDER BY completed_at
LIMIT 12;"

step "Average completion rank by priority"
docker exec postgres psql -U scheduler_user -d scheduler_db -c "
WITH ordered AS (
  SELECT priority,
         ROW_NUMBER() OVER (ORDER BY completed_at) AS rank
  FROM jobs
  WHERE payload LIKE '${RUN_TAG}_%' AND status = 'completed'
)
SELECT
  priority,
  COUNT(*) AS jobs,
  ROUND(AVG(rank)::numeric, 1) AS avg_completion_rank,
  MIN(rank) AS first_rank,
  MAX(rank) AS last_rank
FROM ordered
GROUP BY priority
ORDER BY priority;"

echo
echo "EXPECTED: avg_completion_rank should grow monotonically with priority."
echo "  P=1  (HIGH)  : earliest ranks  (avg ~3-5)"
echo "  P=5  (MED)   : middle  ranks   (avg ~10-15)"
echo "  P=10 (LOW)   : latest  ranks   (avg ~25)"


# ============================================================
banner "DEMO COMPLETE"
echo
echo "Cross-check the visualization in Grafana:"
echo "  http://localhost:3000  (admin / admin)"
echo
echo "Useful Redis inspection commands:"
echo "  docker exec redis redis-cli ZCARD job_queue"
echo "  docker exec redis redis-cli ZRANGE job_queue 0 -1 WITHSCORES"
echo
