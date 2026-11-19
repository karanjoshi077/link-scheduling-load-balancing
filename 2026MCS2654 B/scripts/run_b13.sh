#!/usr/bin/env bash
# B13: one run of the load client through the LB with one deliberately slow backend.
#   usage: scripts/run_b13.sh <rr|queue|inflight> [requests]
#   env:   SCHED (backend policy, default fcfs), QUANTUM, SLOW (default 4), OUT (default results/b13_<sched>_<algo>)
set -euo pipefail
cd "$(dirname "$0")/.."
ALGO=${1:?usage: run_b13.sh <rr|queue|inflight> [requests]}; REQ=${2:-2000}
export SCHED=${SCHED:-fcfs} SLOW=${SLOW:-4}
OUT=${OUT:-results/b13_${SCHED}_${ALGO}}
[ -x ./lb ] && [ -x ./server ] && [ -x ./client ] || make >/dev/null
[ -d workload ] || python3 scripts/gen_workload.py workload 4096 >/dev/null
rm -rf "$OUT" run store; mkdir -p "$OUT"
export RUN=./run STORE=./store

python3 - <<'PY'
import json
c = json.load(open("config.json")); c["server"]["port"] = c["load_balancer"]["port"]
json.dump(c, open("client_lb.json", "w"), indent=2)
PY

scripts/start_backends.sh
./lb --algo "$ALGO" --config config.json --health-log "$OUT/health_log.csv" \
     --dispatch-log "$OUT/lb_dispatch.csv" >"$OUT/lb_stdout.txt" 2>&1 &
LB=$!
sleep 1.5
( mkdir -p run/cwd && cd run/cwd && ../../client load ../../workload --requests "$REQ" --config ../../client_lb.json ) >"$OUT/client_stdout.txt" 2>&1
kill -INT $LB; wait $LB || true
scripts/stop_backends.sh >/dev/null
cp run/metrics_b*.csv "$OUT"/ 2>/dev/null || true
echo "== $ALGO (backends: $SCHED, slow=backend$SLOW) =="; sed -n '/summary/,$p' "$OUT/lb_stdout.txt"
