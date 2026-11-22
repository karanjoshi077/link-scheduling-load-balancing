# Link Scheduling and Load Balancing

A C++17 networking project implementing a concurrent TCP file server and a health-aware TCP load balancer. It explores how request scheduling, backend selection, and failures affect throughput, waiting time, and tail latency.

## Components

- **Concurrent file server:** non-blocking sockets and `poll()` for connection handling, worker threads for file operations, and per-filename reservations to coordinate concurrent GET/PUT requests.
- **Scheduling policies:** First-Come-First-Served (FCFS), Shortest Job First (SJF), Round Robin (RR), and Deficit Round Robin (DRR).
- **TCP load balancer:** request forwarding to four backend servers, periodic health probes, and selection by round robin, reported queue depth, or in-flight request count.
- **Fault tolerance:** backend failure detection, recovery after restart, and an optional retry mode when no response bytes have reached the client.

## Repository layout

```text
file-server/                 Standalone concurrent server, client, and scheduling experiments
load-balancer/               Load balancer, backend copies, workload, scripts, and results
docs/                        Design and performance notes
.github/workflows/           Automated compilation checks
```

## Build

Requires Linux, `g++`, `make`, and Python 3 for the analysis scripts.

```bash
make -C file-server
make -C load-balancer
```

## Running

```bash
cd load-balancer
SCHED=fcfs SLOW=4 bash scripts/start_backends.sh
./lb --algo inflight --config config.json
```

In a separate terminal, point the client's server port to the load-balancer port from `config.json`, then run `./client` with GET, PUT, or LOAD. Stop the backends using `bash scripts/stop_backends.sh`.

## Experiments and results

The repository contains recorded results for backend-selection comparisons and failure/recovery scenarios. Analysis scripts reproduce plots and summaries. See `docs/performance-analysis.md` for findings and `load-balancer/README.md` for detailed commands.

**Limitations:** Each backend maintains its own per-filename reservation state; a shared directory across backend processes does not provide cross-process filename locking. Recorded measurements depend on the original test environment and workload.

## Implementation

C++17, POSIX sockets, `poll()`, pthreads, JSON configuration, CSV metrics, Python analysis.
