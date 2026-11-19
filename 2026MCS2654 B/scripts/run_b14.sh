#!/usr/bin/env bash
# B14: fault injection. Load in flight -> kill -9 one backend -> restart it.
#   usage: scripts/run_b14.sh [algo]        env: VICTIM (1-4, default 2), RETRY=1 to run the LB with --retry,
#                                                SCHED, QUANTUM, OUT (default results/b14_<algo>[_retry])
# Timeline: warm-up 5 s -> kill -9 -> down 6 s -> restart -> run until the backend is back in rotation + 4 s.
set -euo pipefail
cd "$(dirname "$0")/.."
ALGO=${1:-inflight}; VICTIM=${VICTIM:-2}
export SCHED=${SCHED:-fcfs} SLOW=${SLOW:-0} RUN=./run STORE=./store
SUFFIX=""; LBARGS=(); [ "${RETRY:-0}" = 1 ] && SUFFIX="_retry" && LBARGS=(--retry)
OUT=${OUT:-results/b14_${ALGO}${SUFFIX}}
[ -x ./lb ] && [ -x ./server ] && [ -x ./client ] || make >/dev/null
[ -d workload ] || python3 scripts/gen_workload.py workload 4096 >/dev/null
rm -rf "$OUT" run store; mkdir -p "$OUT"
iso() { date -u +%Y-%m-%dT%H:%M:%S.%3NZ; }

python3 - <<'PY'
import json
c = json.load(open("config.json")); c["server"]["port"] = c["load_balancer"]["port"]
json.dump(c, open("client_lb.json", "w"), indent=2)
PY
scripts/start_backends.sh
./lb --algo "$ALGO" "${LBARGS[@]}" --config config.json --health-log "$OUT/health_log.csv" \
     --dispatch-log "$OUT/lb_dispatch.csv" >"$OUT/lb_stdout.txt" 2>&1 &
LB=$!
sleep 1.5
VPORT=$(python3 -c "import json;print(json.load(open('config.json'))['load_balancer']['backends'][$VICTIM-1]['port'])")

mkdir -p run/cwd
( cd run/cwd && exec ../../client load ../../workload --requests 1000000 --config ../../client_lb.json ) >"$OUT/client_stdout.txt" 2>&1 &
CL=$!
sleep 5
VPID=$(cat run/backend$VICTIM.pid)
echo "kill_ts=$(iso)" >"$OUT/events.txt"; kill -9 "$VPID"
echo "killed backend$VICTIM (port $VPORT)"
sleep 6
echo "restart_ts=$(iso)" >>"$OUT/events.txt"
ONLY=$VICTIM scripts/start_backends.sh >/dev/null
echo "restarted backend$VICTIM"
# wait (max 20 s) until the LB has dispatched to it again, then keep running a little
for _ in $(seq 1 200); do
  grep -q "127.0.0.1:$VPORT,.*" <(tail -n 200 "$OUT/lb_dispatch.csv" | awk -F, -v t="$(sed -n 's/restart_ts=//p' "$OUT/events.txt")" '$1>t') && break
  sleep 0.1
done
sleep 4
echo "end_ts=$(iso)" >>"$OUT/events.txt"
kill -TERM $CL 2>/dev/null || true; wait $CL 2>/dev/null || true
kill -INT $LB; wait $LB || true
scripts/stop_backends.sh >/dev/null
echo "== B14 $ALGO${SUFFIX} =="; cat "$OUT/events.txt"; sed -n '/summary/,$p' "$OUT/lb_stdout.txt"
