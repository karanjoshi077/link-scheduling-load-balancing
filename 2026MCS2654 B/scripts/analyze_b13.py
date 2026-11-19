#!/usr/bin/env python3
"""B13 analysis. usage: analyze_b13.py [results_dir=results] [sched=fcfs] [n_seed=5]
Per algorithm: fraction of requests per backend, LB-observed latency per backend, probe depth stats and
how requests were spread *between* probes (a 'probe round' = the interval between consecutive probe batches)."""
import sys
import numpy as np, pandas as pd

root = sys.argv[1] if len(sys.argv) > 1 else "results"
sched = sys.argv[2] if len(sys.argv) > 2 else "fcfs"
n_seed = int(sys.argv[3]) if len(sys.argv) > 3 else 5          # the load client's seeding PUTs (workload files)
algos = ["rr", "queue", "inflight"]
rows = []
for a in algos:
    d = f"{root}/b13_{sched}_{a}"
    x = pd.read_csv(f"{d}/lb_dispatch.csv", parse_dates=["timestamp"])
    h = pd.read_csv(f"{d}/health_log.csv", parse_dates=["timestamp"])
    x = x.sort_values("conn_id")
    x = x[x.conn_id > n_seed]                       # drop seeding requests
    backends = sorted(h.backend.unique())
    frac = x.backend.value_counts(normalize=True).reindex(backends).fillna(0)
    ok = x[x.outcome == "ok"]
    lat = ok.groupby("backend").duration_ms.agg(median="median", p99=lambda s: np.percentile(s, 99)).reindex(backends)
    dep = h[h.status == "healthy"].groupby("backend").queue_depth.agg(["mean", "max"]).reindex(backends)

    # probe rounds: assign every dispatch to the most recent probe batch (batch = all four backends within 100 ms)
    t0 = h.timestamp.min()
    h = h.assign(rnd=((h.timestamp - t0).dt.total_seconds() / 0.5).round().astype(int))
    starts = h.groupby("rnd").timestamp.min().sort_index()
    x = x.assign(round_id=np.searchsorted(starts.values, x.timestamp.values, side="right") - 1)
    per = x[x.round_id >= 0].groupby(["round_id", "backend"]).size().unstack(fill_value=0).reindex(columns=backends, fill_value=0)
    per = per[per.sum(axis=1) >= 20]
    top_share = (per.max(axis=1) / per.sum(axis=1))
    slow = backends[-1]
    print(f"\n=== {a}  ({len(x)} requests, {len(per)} probe rounds, backends run '{sched}') ===")
    t = pd.DataFrame({"fraction_%": (frac * 100).round(1), "lat_med_ms": lat["median"].round(2), "lat_p99_ms": lat["p99"].round(2),
                      "probe_depth_mean": dep["mean"].round(2), "probe_depth_max": dep["max"]})
    print(t.to_string())
    print(f"outcomes: {x.outcome.value_counts().to_dict()}")
    print(f"between-probe spread: mean max-single-backend share per round = {top_share.mean()*100:.1f}%   (even split = 25%)")
    print(f"                      mean requests/round to slow backend {slow}: {per[slow].mean():.1f} (sd {per[slow].std():.1f}) vs others {per[backends[:-1]].values.mean():.1f}")
    if a == "queue":
        # how does the slow backend's share in a probe interval depend on the depth the snapshot reported for it?
        hs = h[h.backend == slow].sort_values("timestamp").reset_index(drop=True)
        tab = []
        for i in range(len(hs) - 1):
            w = x[(x.timestamp >= hs.timestamp[i]) & (x.timestamp < hs.timestamp[i + 1])]
            if len(w) >= 20:
                tab.append((int(hs.queue_depth[i]), 100.0 * (w.backend == slow).mean()))
        tb = pd.DataFrame(tab, columns=["slow_depth_at_probe", "share_%"]).groupby("slow_depth_at_probe")["share_%"].agg(["mean", "count"]).round(1)
        print("slow backend's share of the NEXT interval, by the depth its probe reported (queue algo):")
        print(tb.to_string())
        tb.to_csv(f"{root}/b13_queue_depth_vs_share.csv")
    rows.append(dict(algo=a, **{f"frac_{b}": round(frac[b] * 100, 2) for b in backends}, top_share_pct=round(top_share.mean() * 100, 1),
                     slow_sd=round(per[slow].std(), 1), slow_lat_med=round(lat["median"][slow], 2), fast_lat_med=round(lat["median"][backends[:-1]].mean(), 2)))
pd.DataFrame(rows).to_csv(f"{root}/b13_summary.csv", index=False)
print(f"\nwrote {root}/b13_summary.csv")
