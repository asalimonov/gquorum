# Sequential transport performance experiments

Started 2026-10-19 from c16c28f. Implement and measure each candidate before the
next; keep improvements and discard candidates that do not justify their cost.
The five pre-existing staged credential/profiler files are preserved separately.
No public API, persisted schema or application codec changes are intended.

## Method

- Host: Apple M4 Pro, Go 1.26.5; Docker Desktop linux/arm64, 14 CPUs and
  39,878,758,400 bytes RAM. Earlier Colima captures are not a comparison baseline.
- Each phase: three serial fresh three-node cache captures, one worker, 615 keys,
  80% reads/20% writes, GOMAXPROCS=1, disabled load fsync, ten-second warmup and
  30-second CPU profile. No concurrent test/benchmark workload during profiles.
- Timestamped Docker logs and raw per-node CPU/heap profiles, binaries, build
  provenance and interactive reports are retained under
  [transport-followups](../../test-reports/performance/transport-followups/).
- Throughput and operation-weighted mean latency use the six five-second intervals ending at
  loader elapsed 15–40 seconds. CPU profiling starts about one second later;
  CPU/op is a nearby-window estimate, not exact synchronized accounting. The
  rounded log means are weighted by completed reads/writes, not by interval duration;
  they are not latency percentiles. Baseline throughput varies substantially;
  component benchmarks and sampled subtree costs supplement workload medians.
- No syscall tracing overlaps these CPU windows. Syscall samples are CPU cost,
  not exact invocation counts or blocking durations.
- `compare.py`, `comparison.json`, complete candidate patches and command logs
  retain analysis inputs. The capture tool's source patch excludes untracked
  files, so the separately retained candidate patch includes those new sources.

## 1. Exact routing lookup — kept

Added an immutable exact `(cluster, node, group, replica)` set to metadata's
existing derived indexes. Graph changes, pruning and snapshot restore rebuild
and publish it with the validated state; session-only changes retain it.
ValidateLoadPeer checks the current process incarnation and service lifetime on
every batch before membership lookup. The set preserves every existing
LoadCatalog source: Target, proven checkpoints, retained operation documents,
authority grants and cleanup debt. Latest alone grants no route.

Five 500 ms microbenchmark repetitions (`BenchmarkLoadPeerLookup`, one CPU):

| Catalog size | Old lookup median | Indexed median | Old/new allocations |
| --- | ---: | ---: | ---: |
| 1 group, 3 replicas | 227.5 ns | 23.15 ns | 176 B / 0 B; 1 / 0 allocs |
| 64 groups, 3 replicas each | 330.6 ns | 24.63 ns | 176 B / 0 B; 1 / 0 allocs |

These measure reads, excluding metadata update/index construction. The workload
profiles include update cost. [Raw benchmark](../../test-reports/performance/transport-followups/index-bench.txt).

Checks so far: 1,967 all-module units passed; focused metaengine/metaservice units
and functional suites passed under race; all-module lint passed after extracting
index construction helpers and wrapping one line. Differential tests compare the
new set with the original catalog across retry, retained grants/debt, admission,
pruning and restore. Additional tests exercise staged intent, refused/empty
restore, cancellation, process revocation/replacement and quiescence.

Three-node result (three repetitions per phase):

| Phase | Operations/s | Estimated cluster CPU µs/op | Write/read mean µs |
| --- | ---: | ---: | ---: |
| Baseline 1 | 3,092 | 412.7 | 395 / 307 |
| Baseline 2 | 3,893 | 361.0 | 316 / 242 |
| Baseline 3 | 2,366 | 486.2 | 505 / 400 |
| Indexed 1 | 4,331 | 322.5 | 281 / 218 |
| Indexed 2 | 4,484 | 313.5 | 270 / 210 |
| Indexed 3 | 4,268 | 331.3 | 288 / 219 |

All comparison windows have zero read/write/stale/corrupt errors. No
scopedLoadCatalog samples remain under StreamRaft in the indexed profiles.
Example node3 peer-validation sampled CPU fell from 0.29 s to 0.13 s between
baseline-1 and index-1 (node3 is a follower in both). Actual leaders vary and are
recorded in comparison.json; cluster totals avoid assuming node3 always leads.

Decision: keep. The allocation-free lookup is repeatably faster, the intended
profile hotspot disappears, and every indexed workload repetition has lower
estimated CPU/op and latency than the baseline repetitions. Host variation and
the nearby measurement windows prevent claiming an exact causal percentage.

## 2. Stream lifecycle allocation — rolled back

The candidate made incoming StreamRaft install one shutdown link per stream. Every frame still
reserves the shared bounded admission slot, has a fresh CallTimeout context,
rechecks identities/authentication and remains joined until callbacks return.
Close joins the shutdown callback and every admitted frame. Idle gRPC Recv stays
owned by the borrowed server, as before; it does not retain an active-call slot.
Unary and outgoing operations retain their individual shutdown linkage.

Five isolated 500 ms repetitions of BenchmarkIngressLifecycle, one CPU:

| Variant | Median | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Per-frame shutdown link | 481.4 ns | 584 | 9 |
| Shared stream shutdown link | 311.9 ns | 296 | 5 |

[Isolated benchmark](../../test-reports/performance/transport-followups/lifecycle-bench-isolated.txt).
An earlier preliminary benchmark overlapped a functional race run; only the
isolated rerun above supports these numbers. Units, all-module lint and repeated
functional race checks pass. Added real gRPC regressions for ignored callback
cancellation and Close retry, shared admission bounds, fresh deadlines after an
idle interval, authentication timeout, and closing idle streams.


Additional alternating-order runs used a detached checkout of 0b015d7 for
controls and the candidate worktree for the optimization, with the same five
prerequisite files. Run order: index-4, lifecycle-4, lifecycle-5, index-5. The
temporary checkout was removed after capture.

| Run | Operations/s | Estimated CPU µs/op | Weighted write/read µs |
| --- | ---: | ---: | ---: |
| index-4 | 4,316 | 324.6 | 285 / 218 |
| index-5 | 4,391 | 319.7 | 280 / 213 |
| lifecycle-1 | 4,427 | 315.9 | 275 / 210 |
| lifecycle-2 | 2,740 | 405.4 | 436 / 345 |
| lifecycle-3 | 4,174 | 333.5 | 292 / 227 |
| lifecycle-4 | 4,234 | 333.5 | 287 / 222 |
| lifecycle-5 | 4,285 | 329.3 | 284 / 220 |

Decision: roll back. Five-run median estimated CPU/op is 333.5 µs for the
candidate versus 322.5 µs for the indexed baseline; median throughput is 4,234
versus 4,331 operations/s. The large outlier is retained. The exact cause of
workload variation/regression is unproven, but the component allocation gain did
not establish a workload gain, including the alternating controls. All windows
had zero workload errors. Restored both production files and removed the three
candidate-only source/test files. The complete rejected patch and all evidence
remain under test-reports/performance/transport-followups/lifecycle-rejected.patch.
The third capture's syscall trace ran after CPU profiling and is diagnostic only.

Latency analysis was refined to weight each interval's logged mean by its
completed operation count; the initial routing table above uses that corrected
calculation. This does not change the routing-index decision.

## 3. Queued-message coalescing — pending

The existing destination worker already drains queued batches without a delay.
The proposed experiment will test earlier coalescing before destination dispatch;
any retained change must preserve order, bounds, shutdown and one-worker latency.
