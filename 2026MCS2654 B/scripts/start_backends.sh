#!/usr/bin/env bash
# Start four Part A servers, each with its own config file (own port / thread count).
#
#   SCHED     fcfs|sjf|rr|drr  scheduling policy of ALL four backends   (default fcfs)
#   QUANTUM   bytes, rr/drr only                                       (default 4096)
#   SLOW      1-4: index of the backend run with server_threads=1; 0 = none   (default 4)
#   STORE     directory all four backends serve from (shared)          (default ./store)
#   CONFIG    base config (ports come from load_balancer.backends[])   (default ./config.json)
#   RUN       output dir for generated configs, pids, logs, metrics    (default ./run)
#   ONLY      space-separated backend indices to (re)start             (default "1 2 3 4")
set -euo pipefail
cd "$(dirname "$0")/.."
SCHED=${SCHED:-fcfs}; QUANTUM=${QUANTUM:-4096}; SLOW=${SLOW:-4}
ONLY=${ONLY:-"1 2 3 4"}; STORE=${STORE:-./store}; CONFIG=${CONFIG:-./config.json}; RUN=${RUN:-./run}
[ -x ./server ] || make server >/dev/null
mkdir -p "$STORE" "$RUN"

python3 - "$CONFIG" "$RUN" "$SLOW" <<'PY'
import json, sys
cfg, run, slow = json.load(open(sys.argv[1])), sys.argv[2], int(sys.argv[3])
for i, b in enumerate(cfg["load_balancer"]["backends"], 1):
    c = json.loads(json.dumps(cfg))
    c["server"]["ip"], c["server"]["port"] = b["ip"], b["port"]
    if i == slow:
        c["server"]["server_threads"] = 1
    json.dump(c, open(f"{run}/backend{i}.json", "w"), indent=2)
PY

QARG=()
[ "$SCHED" = rr ] || [ "$SCHED" = drr ] && QARG=(--quantum "$QUANTUM")
for i in $ONLY; do
  ./server --sched "$SCHED" "${QARG[@]}" --file "$STORE" --config "$RUN/backend$i.json" \
           --metrics-out "$RUN/metrics_b$i.csv" >"$RUN/backend$i.log" 2>&1 &
  echo $! >"$RUN/backend$i.pid"
done
sleep 0.5
for i in $ONLY; do
  kill -0 "$(cat "$RUN/backend$i.pid")" 2>/dev/null || { echo "backend $i failed to start:"; cat "$RUN/backend$i.log"; exit 1; }
done
echo "started backends [$ONLY]: sched=$SCHED${QARG:+ quantum=$QUANTUM} slow=backend$SLOW store=$STORE"
