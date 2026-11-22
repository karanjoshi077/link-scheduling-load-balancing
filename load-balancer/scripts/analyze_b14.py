#!/usr/bin/env python3
"""B14 analysis: detection time, failed requests (and why), re-entry time.
usage: analyze_b14.py <results/b14_dir> [health_interval_ms]"""
import sys, json
import pandas as pd

d = sys.argv[1]
interval_ms = float(sys.argv[2]) if len(sys.argv) > 2 else 1000.0
ev = dict(l.strip().split("=", 1) for l in open(f"{d}/events.txt") if "=" in l)
kill, restart, end = (pd.Timestamp(ev[k]) for k in ("kill_ts", "restart_ts", "end_ts"))
h = pd.read_csv(f"{d}/health_log.csv", parse_dates=["timestamp"])
x = pd.read_csv(f"{d}/lb_dispatch.csv", parse_dates=["timestamp"])
cfg = json.load(open("config.json"))
victim = None
# the victim is the backend whose probes first go down after the kill
down_after = h[(h.timestamp >= kill) & (h.status == "down")]
victim = down_after.backend.iloc[0]
hv = h[h.backend == victim]

print(f"victim backend           : {victim}")
print(f"health interval          : {interval_ms:.0f} ms")
last_ok = hv[(hv.timestamp < kill) & (hv.status == "healthy")].timestamp.max()
first_down = hv[(hv.timestamp >= kill) & (hv.status == "down")].timestamp.min()
det = (first_down - kill).total_seconds() * 1000
print(f"last healthy probe       : {(kill-last_ok).total_seconds()*1000:.0f} ms before the kill")
print(f"first 'down' probe       : {det:.0f} ms after the kill  ({det/interval_ms:.2f} x interval)")

# ---- failures ----
w = x[(x.timestamp >= kill) & (x.timestamp <= end)]
tot_all = len(x[x.timestamp <= end])
bad = {"lb_err_backend_down": "ERR to client: connect to dead backend refused (backend still marked healthy)",
       "lb_err_backend_failed": "ERR to client: backend died before any response byte",
       "lb_err_no_backend": "ERR to client: no healthy backend",
       "closed_midstream": "connection closed after response had started (backend killed mid-transfer)"}
print()
print(f"requests dispatched (whole run, excl. client_gone) : {len(x[(x.timestamp<=end)&(x.outcome!='client_gone')])}")
tot_fail = 0
for k, why in bad.items():
    n = int((w.outcome == k).sum()); tot_fail += n
    print(f"  failed {k:24s}: {n:5d}   {why}")
print(f"  TOTAL failed requests                     : {tot_fail}")
print(f"  backend_err (ERR from a healthy backend)  : {int((w.outcome=='backend_err').sum())}")
if tot_fail:
    f = w[w.outcome.isin(bad)]
    print(f"  failures span {(f.timestamp.min()-kill).total_seconds()*1000:.0f} .. {(f.timestamp.max()-kill).total_seconds()*1000:.0f} ms after the kill")
    print(f"  failures after the first 'down' probe     : {int((f.timestamp > first_down).sum())}")

# ---- re-entry ----
rs = hv[(hv.timestamp >= restart) & (hv.status == "healthy")].timestamp.min()
disp = x[(x.timestamp >= restart) & (x.backend == victim) & (x.outcome == "ok")].timestamp.min()
print()
print(f"restart issued at        : {restart}")
print(f"first healthy probe      : {(rs-restart).total_seconds()*1000:.0f} ms after restart  ({(rs-restart).total_seconds()*1000/interval_ms:.2f} x interval)")
print(f"first completed request  : {(disp-restart).total_seconds()*1000:.0f} ms after restart")
