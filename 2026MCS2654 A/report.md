# Name: Karan Joshi | Entry No: 2026MCS2654

# Assignment 2 - Performance Evaluation Report

## 1. Waiting Time & SJF Starvation (A11)
Our diagnostic benchmarks clearly demonstrate the responsiveness advantages of preemptive scheduling. Preemptive policies (`rr`, `drr`) dominate in responsiveness, with median waiting times (~0.35ms) severely outperforming non-preemptive policies like `fcfs` (2.45ms) and `sjf` (1.34ms). 

Furthermore, SJF starvation is visibly proven by the data. While `sjf` improves median wait times over `fcfs` by pushing smaller requests forward, it aggressively starves large files. The p99 slowdown for large files under `sjf` was **548.82 ns/byte**, which is significantly worse than `fcfs` (280.45 ns/byte). Because `sjf` always prioritizes smaller pending requests, a large file is repeatedly delayed as new small requests arrive.

## 2. Single-Threaded FCFS vs RR (A28)
When running on a single worker thread (`server_threads=1`), `fcfs` heavily penalizes small requests. If a small file is queued behind a large one, it suffers an extreme p50 slowdown (35,572 ns/byte compared to the medium file's 1496 ns/byte). 

Conversely, `rr` with a single thread confirms the design intent: it successfully separates scheduling from parallelism. Even with only one thread, `rr` effectively interleaves file chunks, maintaining an exceptionally low p50 wait time (1.38ms) compared to `fcfs` (37ms). 

## 3. RR vs DRR Deficit Recovery (A15 / A16 / A29)
The standard Round Robin (`rr`) implementation routinely wastes its quantum allowance when a chunk finishes processing slightly under the strict quantum limit. In our benchmark, `rr` threw away **132,626 bytes** of unused allowance (with 108,338 bytes coming from large files alone).

Deficit Round Robin (`drr`) completely mitigates this flaw. It forfeited **0 bytes**, successfully accumulating and carrying over deficits across rounds. This directly translates to better tail latency for large files: `drr` achieved a meaningfully tighter p99 slowdown (3942 ns/byte) than `rr` (5095 ns/byte) on long-line files. 

## 4. Quantum Escape Hatch (A14)
During the `rr` benchmark, the A14 escape hatch—triggered when a single contiguous line exceeds the configured 4096-byte quantum and must be processed entirely to prevent an infinite loop—fired exactly **11 times**.
