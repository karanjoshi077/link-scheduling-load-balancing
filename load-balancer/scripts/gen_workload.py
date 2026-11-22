#!/usr/bin/env python3
"""Generate the A27 workload (deterministic): sizes ~1 KB / ~30 KB / ~150 KB and
both short-line (60-80 B) and long-line (> Q) files.   usage: gen_workload.py [outdir] [Q]"""
import os, random, sys

out = sys.argv[1] if len(sys.argv) > 1 else "workload"
Q = int(sys.argv[2]) if len(sys.argv) > 2 else 4096
rng = random.Random(724)
os.makedirs(out, exist_ok=True)
alphabet = "abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

def line(n):                      # n bytes including the trailing \n
    return "".join(rng.choice(alphabet) for _ in range(n - 1)) + "\n"

def make(name, target, lo, hi):
    data, total = [], 0
    while total < target:
        n = min(rng.randint(lo, hi), target - total) if target - total > 1 else 1
        data.append(line(max(n, 1)))
        total += max(n, 1)
    with open(os.path.join(out, name), "w", newline="") as f:
        f.write("".join(data))

make("small_1k.txt",        1_000,   60, 80)
make("medium_30k.txt",     30_000,   60, 80)
make("large_150k.txt",    150_000,   60, 80)
make("longline_30k.txt",   30_000, Q + 500, Q * 2)     # every line longer than Q
make("longline_150k.txt", 150_000, Q + 500, Q * 2)
for n in sorted(os.listdir(out)):
    print(f"{n:20s} {os.path.getsize(os.path.join(out, n)):8d} bytes")
