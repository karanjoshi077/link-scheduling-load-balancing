# TCP Load Balancer



A TCP load balancer (`lb`) in front of four unmodified file-server file servers. It speaks the
file-server wire protocol (`GET` / `PUT`, one request per connection), health-checks the backends
with `HEALTH` probes, and picks a backend with one of three selectable algorithms.

## Layout

```
lb.cpp                  the load balancer (load-balancer deliverable)
Makefile                builds lb, and the unmodified file-server server/client
config.json             shared config (server + load_balancer sections)
include/nlohmann/       vendored single-header JSON parser
partA/server.cpp        file-server server, byte-identical to the file-server repository
partA/client.cpp        file-server client, byte-identical to the file-server repository
scripts/                start/stop backends, experiment drivers, analysis, plots, report
results_new/            fresh B13/B14 health logs, dispatch logs, metrics, analysis, and figures
report.pdf              load-balancer report (3 pages)
```

## Build

```
make            # g++ -std=c++17 -pthread  ->  ./lb  ./server  ./client
```

## Run by hand

```
# 1. four backends, each with its own config (own port; backend 4 gets server_threads=1)
SCHED=fcfs SLOW=4 scripts/start_backends.sh

# 2. the load balancer
./lb --algo inflight --config config.json

# 3. a client config that points at the LB: copy config.json, set server.port to load_balancer.port (8000)
python3 -c "import json;c=json.load(open('config.json'));c['server']['port']=c['load_balancer']['port'];json.dump(c,open('client_lb.json','w'),indent=2)"
./client put workload/medium_30k.txt --config client_lb.json
./client get medium_30k.txt          --config client_lb.json
./client load workload --requests 2000 --config client_lb.json

# 4. stop (SIGINT/SIGTERM): the LB drains, prints per-backend counts, closes its logs
kill -INT <lb pid>;  scripts/stop_backends.sh
```

The file-server client is used unchanged (B2): only the config file differs.

### `lb` flags

| flag | meaning |
|---|---|
| `--algo rr\|queue\|inflight` | **required.** Selection algorithm (below). |
| `--config <path>` | config file (default `config.json`). |
| `--health-log <path>` | health log CSV (default `health_log.csv`). |
| `--dispatch-log <path>` | per-request dispatch log CSV (default `lb_dispatch.csv`). |
| `--retry` | optional: if a backend fails *before any response byte reached the client*, retry on another backend (up to 4 attempts) instead of replying `ERR`. Off by default, which is exactly the B6 behaviour. |
| `--io-timeout-ms <ms>` | longest a single recv/send may stall on either socket (default 30000). |

### Config

`config.json` is the file from the configuration. **Every field is required** (including the file-server
`server` section); a missing field, wrong type, out-of-range value or malformed JSON exits non-zero
with a message naming the field, e.g. `error: missing required field 'load_balancer.backends[2].port'`.
`load_balancer.backends` must have exactly four entries. `load_balancer.ip/port` is where the LB
listens; `health_interval_ms` is the probe period *and* the probe timeout.

## Selection algorithms (B11, B12)

| `--algo` | state it reads | class |
|---|---|---|
| `rr` | none (baseline) | — |
| `queue` | queue depth each backend reported in its **last probe** | probe-cadence: refreshed once per health interval |
| `inflight` | number of requests **currently in flight** from this LB to each backend | real-time: updated at every dispatch and completion |

`queue` and `inflight` choose the backend with the minimum signal among healthy backends; ties are
broken by rotating the starting point, so equal backends share load rather than the lowest index
always winning. For `inflight` the pick and the increment happen under one lock, so concurrent
dispatches see each other.

## Health checking (B7–B10)

* One thread per backend, so a slow probe never delays the others. Probes start on a fixed cadence
  (steady clock) of `health_interval_ms`.
* A probe = connect, send `HEALTH\n`, read one complete `OK <depth>` line, all within one
  health interval. Connection error, malformed reply or timeout ⇒ failure.
* Success marks the backend healthy and stores its queue depth; failure marks it unhealthy and
  removes it from selection. The next successful probe returns it to service.
* There is deliberately **no passive marking** from the data path, so detection time in B14 is
  attributable to the probes alone.
* Before accepting clients the LB waits for one round of probes (at most one interval + 0.5 s).
* `health_log.csv`: `timestamp,backend,status,rtt_ms,queue_depth` — every probe, including failed
  ones (`rtt_ms` is the time observed before the failure; `queue_depth` is `-1` when down).
  Timestamps are UTC ISO-8601 with milliseconds.

## Forwarding (B1, B4, B5, B6)

One thread per accepted client connection (so clients never serialize behind one another). The LB
parses the request line, picks a backend, connects, and relays; bytes the client sent behind the
header are retained for the body.

* **GET**: forward the request, read the backend's response header line *completely*, forward it,
  then relay exactly the declared number of bytes.
* **PUT**: forward the request, read the backend's `OK 0` ack and forward it, relay exactly the
  declared number of body bytes client→backend, then forward the backend's final line.
* **B6**: until the first response byte has been forwarded to the client, a backend failure
  produces `ERR <reason>` (`backend unavailable`, `backend failed`, `no healthy backend available`).
  After that the LB closes the client connection (short read = failure signal). A client that goes
  silent is bounded by a 5 s header timeout and the I/O stall timeout; nothing hangs.
* Backend `ERR` replies (e.g. file not found) are passed through unchanged.
* Malformed requests get an `ERR` from the LB itself. `HEALTH` sent *to the LB* is answered
  locally with `OK <requests currently being handled>` (my choice; the LB has no scheduler queue).
* **B5 / shutdown**: on SIGINT/SIGTERM the LB stops accepting, still accepts connections the kernel had
  already queued, lets all exchanges finish, then prints per-backend counts (forwarded, fraction,
  failed). `SO_REUSEADDR` is set on the listening socket.
* `lb_dispatch.csv`: `timestamp,conn_id,op,filename,backend,outcome,bytes,duration_ms`, one row per
  request (written when it finishes). `outcome` is one of `ok`, `backend_err`,
  `lb_err_backend_down`, `lb_err_backend_failed`, `lb_err_no_backend`, `closed_midstream`,
  `client_gone`. "Forwarded to backend X" means the request line was sent to X.

## Design choices worth knowing

* **Shared storage.** The four backends are independent processes but all serve the same `--file`
  directory (`./store`). Otherwise a PUT stored by backend 1 could never be GET from backend 3 and the
  load client's mixed workload would mostly return `ERR File not found`. Consequence: file-server's per-filename reservation is per process, so concurrent operations on the same file across backends require care. In the fresh B13 runs included here, all 6000 workload requests completed successfully according to the load-balancer outcomes.
* **Seeding** requests (the load client's 5 initial PUTs) go through the LB too; the analysis scripts
  drop them (`conn_id <= 5`).
* All four backends run the same `--sched` (**fcfs** in every experiment), as B8 requires.

## Reproducing the experiments

```
scripts/gen_workload.py workload 4096          # A27 workload: 1 KB / 30 KB / 150 KB, short- and long-line files
scripts/run_b13.sh rr 6000                     # B13, one run per algorithm: rr | queue | inflight
scripts/run_b13.sh queue 6000
scripts/run_b13.sh inflight 6000
python3 scripts/analyze_b13.py                 # distribution, latencies, between-probe behaviour
scripts/run_b14.sh rr                          # B14 fault injection (also: queue, inflight)
scripts/run_b14.sh inflight
RETRY=1 scripts/run_b14.sh inflight            # same, LB started with --retry
python3 scripts/analyze_b14.py results/b14_inflight
python3 scripts/plot_all.py                    # figures into results/
python3 scripts/make_report.py                 # report.pdf
```

Each driver builds fresh state (`run/`, `store/`), starts the four backends and the LB, drives load
with the file-server `client load`, stops everything with SIGINT/SIGTERM and leaves its logs under
`results/` by default. The fresh experiment evidence packaged with this repository is under `results_new/`. `run_b13.sh` makes backend 4 slow (`server_threads=1`, the others 4); override with
`SLOW=<1-4|0>`, `SCHED=<policy>`. `run_b14.sh` runs load, `kill -9`s backend 2 after 5 s, restarts it
6 s later, and stops once it has been dispatched to again.
