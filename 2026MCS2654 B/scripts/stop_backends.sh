#!/usr/bin/env bash
# Stop the backends started by start_backends.sh (SIGTERM: graceful, writes their metrics CSVs).
cd "$(dirname "$0")/.."
RUN=${RUN:-./run}
for i in 1 2 3 4; do
  [ -f "$RUN/backend$i.pid" ] && kill -TERM "$(cat "$RUN/backend$i.pid")" 2>/dev/null
done
for i in 1 2 3 4; do
  [ -f "$RUN/backend$i.pid" ] && while kill -0 "$(cat "$RUN/backend$i.pid")" 2>/dev/null; do sleep 0.05; done
  rm -f "$RUN/backend$i.pid"
done
echo "backends stopped"
