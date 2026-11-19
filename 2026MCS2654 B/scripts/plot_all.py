#!/usr/bin/env python3
"""Plots for the Part B report. usage: plot_all.py [results_dir=results] [sched=fcfs]
Writes fig_b13_distribution.png, fig_b13_between_probes.png, fig_b14_timeline.png into results_dir."""
import sys
import numpy as np, pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

root = sys.argv[1] if len(sys.argv) > 1 else "results"
sched = sys.argv[2] if len(sys.argv) > 2 else "fcfs"
algos = ["rr", "queue", "inflight"]
labels = {"rr": "round robin (baseline)", "queue": "queue depth (probe)", "inflight": "in-flight (real-time)"}
colors = ["#4c78a8", "#f58518", "#54a24b", "#e45756"]
plt.rcParams.update({"font.size": 9, "axes.spines.top": False, "axes.spines.right": False})

def load(a):
    x = pd.read_csv(f"{root}/b13_{sched}_{a}/lb_dispatch.csv", parse_dates=["timestamp"])
    h = pd.read_csv(f"{root}/b13_{sched}_{a}/health_log.csv", parse_dates=["timestamp"])
    return x[x.conn_id > 5], h

# ---- Fig 1: per-backend distribution ----
fig, ax = plt.subplots(figsize=(6.4, 2.7))
w = 0.26
for k, a in enumerate(algos):
    x, h = load(a)
    bs = sorted(h.backend.unique())
    f = x.backend.value_counts(normalize=True).reindex(bs).fillna(0) * 100
    bars = ax.bar(np.arange(4) + (k - 1) * w, f.values, w, label=labels[a], color=colors[k])
    for b, v in zip(bars, f.values):
        ax.text(b.get_x() + b.get_width() / 2, v + 0.4, f"{v:.0f}", ha="center", fontsize=7)
ax.axhline(25, color="grey", lw=0.8, ls="--")
ax.set_xticks(range(4))
ax.set_xticklabels([f"b{i+1} (:{9001+i})" + ("\nSLOW: 1 thread" if i == 3 else "\n4 threads") for i in range(4)])
ax.set_ylabel("% of requests"); ax.set_ylim(0, 36)
ax.set_title("B13: share of requests per backend (6000 requests, backends run fcfs)")
ax.legend(frameon=False, fontsize=7.5, ncol=3, loc="upper center", bbox_to_anchor=(0.5, -0.28))
fig.tight_layout(); fig.savefig(f"{root}/fig_b13_distribution.png", dpi=170); plt.close(fig)

# ---- Fig 2: between probes: slow backend's request rate vs the depth the probes reported ----
fig, axs = plt.subplots(2, 1, figsize=(6.4, 3.6), sharex=True)
slow = "127.0.0.1:9004"
for ax, a in zip(axs, ["queue", "inflight"]):
    x, h = load(a)
    t0 = h.timestamp.min() + pd.Timedelta("2s")
    t1 = t0 + pd.Timedelta("9s")
    xs = x[(x.timestamp >= t0) & (x.timestamp < t1)]
    bins = np.arange(0, 9.0001, 0.25)
    sec = (xs.timestamp - t0).dt.total_seconds()
    cnt_slow, _ = np.histogram(sec[xs.backend == slow], bins)
    cnt_all, _ = np.histogram(sec, bins)
    ax.step(bins[:-1], 100.0 * cnt_slow / np.maximum(cnt_all, 1), where="post", color=colors[3], lw=1.4)
    ax.set_ylabel("% to slow b4\n(per 250 ms)"); ax.set_ylim(0, 60)
    hs = h[(h.backend == slow) & (h.timestamp >= t0) & (h.timestamp < t1)]
    for t, d in zip((hs.timestamp - t0).dt.total_seconds(), hs.queue_depth):
        ax.axvline(t, color="grey", lw=0.6, ls=":")
        ax.text(t + 0.05, 52, f"d={d}", fontsize=7, color="#444")
    ax.set_title({"queue": "queue-depth algorithm (reads probe snapshot)", "inflight": "in-flight algorithm (reads live LB state)"}[a], fontsize=8.5, loc="left")
axs[1].set_xlabel("seconds   (dotted lines: probe of b4; d = queue depth it reported)")
fig.tight_layout(); fig.savefig(f"{root}/fig_b13_between_probes.png", dpi=170); plt.close(fig)

# ---- Fig 3: B14 timeline (inflight, no retry) ----
d = f"{root}/b14_inflight"
x = pd.read_csv(f"{d}/lb_dispatch.csv", parse_dates=["timestamp"])
h = pd.read_csv(f"{d}/health_log.csv", parse_dates=["timestamp"])
ev = dict(l.strip().split("=", 1) for l in open(f"{d}/events.txt") if "=" in l)
kill, restart = pd.Timestamp(ev["kill_ts"]), pd.Timestamp(ev["restart_ts"])
victim = "127.0.0.1:9002"
t0 = kill - pd.Timedelta("3s"); t1 = restart + pd.Timedelta("3s")
x = x[(x.timestamp >= t0) & (x.timestamp < t1)]
tt = (x.timestamp - kill).dt.total_seconds()
bins = np.arange(-3, (t1 - kill).total_seconds(), 0.25)
fig, ax = plt.subplots(figsize=(6.4, 3.1))
okv, _ = np.histogram(tt[(x.backend == victim) & (x.outcome == "ok")], bins)
oko, _ = np.histogram(tt[(x.backend != victim) & (x.outcome == "ok")], bins)
bad, _ = np.histogram(tt[x.outcome.isin(["lb_err_backend_down", "lb_err_backend_failed", "lb_err_no_backend", "closed_midstream"])], bins)
ax.step(bins[:-1], oko / 3.0 / 0.25, where="post", color=colors[0], label="ok, per healthy backend (avg of 3)")
ax.step(bins[:-1], okv / 0.25, where="post", color=colors[2], label="ok, killed backend (b2)")
ax.step(bins[:-1], bad / 0.25, where="post", color=colors[3], label="failed (ERR / closed)")
hv = h[h.backend == victim]
fd = hv[(hv.timestamp >= kill) & (hv.status == "down")].timestamp.min()
fh = hv[(hv.timestamp >= restart) & (hv.status == "healthy")].timestamp.min()
ax.set_yscale("symlog", linthresh=10)
ax.set_ylim(0, 2e4)
for t, lab in [(0, "kill -9"), ((fd - kill).total_seconds(), "first down probe"), ((restart - kill).total_seconds(), "restart"), ((fh - kill).total_seconds(), "first healthy probe")]:
    ax.axvline(t, color="k", lw=0.7, ls="--"); ax.text(t + 0.04, 1.5e4, lab, rotation=90, va="top", fontsize=7)
ax.set_xlabel("seconds relative to kill"); ax.set_ylabel("requests / s (symlog)")
ax.set_title("B14: in-flight algorithm, one backend killed then restarted (no retry)", fontsize=9)
ax.legend(frameon=False, fontsize=7, ncol=3, loc="upper center", bbox_to_anchor=(0.5, -0.22))
fig.tight_layout(); fig.savefig(f"{root}/fig_b14_timeline.png", dpi=170); plt.close(fig)
print("wrote figures to", root)
