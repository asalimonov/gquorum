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
- Throughput/mean latency use the six five-second progress intervals ending at
  loader elapsed 15–40 seconds. CPU profiling starts about one second later;
  CPU/op is a nearby-window estimate, not exact synchronized accounting. The
  log means are not latency percentiles. Baseline throughput varies substantially;
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
| Baseline 1 | 3,092 | 412.7 | 507 / 402 |
| Baseline 2 | 3,893 | 361.0 | 318 / 243 |
| Baseline 3 | 2,367 | 486.2 | 582 / 467 |
| Indexed 1 | 4,331 | 322.5 | 282 / 218 |
| Indexed 2 | 4,484 | 313.5 | 270 / 210 |
| Indexed 3 | 4,268 | 331.3 | 288 / 220 |

All comparison windows have zero read/write/stale/corrupt errors. No
scopedLoadCatalog samples remain under StreamRaft in the indexed profiles.
Example node3 peer-validation sampled CPU fell from 0.29 s to 0.13 s between
baseline-1 and index-1 (node3 is a follower in both). Actual leaders vary and are
recorded in comparison.json; cluster totals avoid assuming node3 always leads.

Decision: keep. The allocation-free lookup is repeatably faster, the intended
profile hotspot disappears, and every indexed workload repetition has lower
estimated CPU/op and latency than the baseline repetitions. Host variation and
the nearby measurement windows prevent claiming an exact causal percentage.

## 2. Stream lifecycle allocation — pending

## 3. Queued-message coalescing — pending

The existing destination worker already drains queued batches without a delay.
The proposed experiment will test earlier coalescing before destination dispatch;
any retained change must preserve order, bounds, shutdown and one-worker latency.
