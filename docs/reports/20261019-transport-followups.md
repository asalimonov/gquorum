# Sequential transport performance experiments

Started 2026-10-19 from c16c28f. Implement and measure each candidate before the
next; keep improvements and discard candidates that do not justify their cost.
The five pre-existing staged credential/profiler files are preserved separately.
No public API, persisted schema or application codec changed.

Interactive CPU heatmaps and call-stack flamegraphs: open
[baseline-2](../../test-reports/performance/transport-followups/baseline-2/report.html)
and [index-5](../../test-reports/performance/transport-followups/index-5/report.html)
for a pair where node3 leads in both captures. Rejected candidates also retain
interactive reports, for example
[lifecycle-3](../../test-reports/performance/transport-followups/lifecycle-3/report.html)
and [batching-1](../../test-reports/performance/transport-followups/batching-1/report.html).

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

## 3. Queued-message coalescing — rolled back

The existing destination worker already drains queued batches without a delay.
The candidate additionally combined already queued Ready batches before splitting
by destination, only while all destination workers were idle. The sole dispatcher
drained a fixed queue-length snapshot, bounded to 64 Ready batches, with no timer
or wait for arrivals. Backlogged workers retained their existing bounded queues.

Five isolated 500 ms repetitions of BenchmarkReadyDispatchBatching, one CPU:

| Ready batches | Individual/coalesced median ns | Individual/coalesced bytes | Individual/coalesced allocations |
| --- | ---: | ---: | ---: |
| 1 | 205.9 / 206.4 | 320 / 320 | 2 / 2 |
| 8 | 1,453 / 1,920 | 2,560 / 9,984 | 16 / 11 |
| 64 | 11,020 / 11,971 | 20,480 / 85,888 | 128 / 20 |

This benchmark includes splitting/enqueuing and draining destination queues,
without network workers. It demonstrates fewer allocations but more slice-copy
bytes and longer dispatch time for queued bursts; network capture determines
whether fewer frames compensate. [Raw benchmark](../../test-reports/performance/transport-followups/batching-bench.txt).

| Run | Operations/s | Estimated CPU µs/op | Weighted write/read µs |
| --- | ---: | ---: | ---: |
| batching-1 | 4,298 | 326.6 | 283 / 220 |
| batching-2 | 4,370 | 321.7 | 280 / 217 |
| batching-3 | 4,264 | 330.8 | 288 / 221 |

All three windows had zero workload errors. Candidate checks passed: 1,969
all-module units, focused Raft unit race checks, three functional race repetitions
and all-module lint (after replacing reassignment of a parameter with a local).
Ordering, snapshot payload preservation, backlog refusal, drain bounds and the
empty-queue fast path were explicitly tested.

Decision: roll back. Median throughput was 4,298 ops/s versus the five indexed
controls' 4,331; estimated CPU/op was 326.6 versus 322.5 µs. Results overlap the
baseline range and do not establish improvement. Earlier batching adds code and
copying cost without a demonstrated one-worker benefit. No claim is made that
this experiment proves batching cannot help other workloads. Restored
outbound.go and removed the candidate-only test file; the existing destination
coalescer remains. The full candidate is retained in batching-candidate.patch
alongside its profiles and interactive reports.

## Final retained change and verification

Only the exact routing index is retained in production (0b015d7). Current
incarnation checks still run per frame; transport lifecycle and batching remain
as they were before these experiments. Final all-module, race, functional,
protocol-generation and Docker scenario checks are recorded below.

The first full functional run overlapped Docker example work and had two
five-second deadline failures: TestOperationServicesUseScopedPersistence and
TestOperationAndProgressReadsWorkOnScopedComposition. Both passed five isolated
repetitions, and all 1,055 functional checks subsequently passed with Docker work
stopped, without code changes. Contention is a plausible explanation, not a
proven cause. The failed run is retained at
[functional initial run](../../test-reports/function/20261018T224007.976373Z-s_l_25fx/report.md).

The first scheduler example run separately timed out waiting for task reads from
the former leader after quorum restoration. Other replicas continued executing
tasks, while node1 repeatedly pre-voted; its logs do not establish a root cause.
This failure is retained at
[scheduler initial run](../../test-reports/example/20261018T224241.890260Z-vavee3jx/report.md).
The unchanged scheduler rerun passed all phases, including quorum restoration
and live executor partition/reconnection. The first failure is intermittent;
its cause remains unconfirmed. No deadline or assertion has been relaxed.

Final retained-code checks (fresh runs):

| Command | Result | Evidence |
| --- | --- | --- |
| `make test` | 1,967 passed | [report](../../test-reports/unit/20261018T223937.738979Z-i3b88kio/report.md) |
| `make test-race` | 1,967 passed | [report](../../test-reports/unit-race/20261018T223944.625146Z-xy62z8ws/report.md) |
| `make test-function` (isolated rerun) | 1,055 passed | [report](../../test-reports/function/20261018T224535.303773Z-umhq9tfh/report.md) |
| `make test-function-race` | 1,055 passed | [report](../../test-reports/function-race/20261018T224615.458492Z-3hxs2zfq/report.md) |
| `make lint` | Passed | [log](../../test-reports/performance/transport-followups/final-lint.log) |
| `make proto-check` | Passed; no generated diff | [log](../../test-reports/performance/transport-followups/final-proto-check.log) |
| Python scripts unittest discovery | 38 passed | [log](../../test-reports/performance/transport-followups/final-python.log) |
| `make test-example` | Passed | [report](../../test-reports/example/20261018T223942.055533Z-m4f3kw_g/report.md) |
| `make test-cache-example` | Passed, including 1/10/256 workers and restarts | [report](../../test-reports/example/20261018T224017.489290Z-imhxncgz/report.md) |
| `make test-scheduler-example` (rerun) | Passed | [report](../../test-reports/example/20261018T224736.210591Z-yruxtmqp/report.md) |

The full process suite uses the private image
`gquorum-test:transport-followups-0b015d7`, built from the retained runtime,
and `CLUSTER_PARALLEL=4` after all other workloads have finished.


The first full process run had 89 passes and two failures
([report](../../test-reports/cluster/20261018T224928.039283Z-eefhnkkq/report.md)):

- Forced retirement asserted the superseded whole-operation wait for an offline
  host. Current `topology/plan_partition.go` detaches ordinary removal's stop and
  retirement work; `TestScopedPartitionsDetachRemovalButKeepDeletionStops` and
  `TestFailureIsolation_RemovedHostCleanup` explicitly cover this contract. The
  assertion raced native convergence, explaining why it sometimes passed.
  Updated the process test to wait for native convergence, then require exact
  unconfirmed debt and intact offline storage. After host return it waits for
  matching stop/retirement proof before proceeding with retained membership,
  values, readiness fencing and fresh-identity assertions. Clarified the
  corresponding architecture paragraph. No production behavior changed.
  Three consecutive repetitions passed
  ([report](../../test-reports/cluster/20261018T225507.433733Z-9klrp1rz/report.md)),
  and all-module lint passed again.
- Runtime learner growth timed out before reaching its exact promotion barrier.
  Its failed logs are retained. Three unchanged isolated repetitions passed
  ([report](../../test-reports/cluster/20261018T225606.207233Z-31i2rrr2/report.md));
  the cause of the initial timeout remains unconfirmed. No timeout or barrier
  assertion has been relaxed.


The next full process run passed 90/91 checks, including corrected forced
retirement and unchanged learner growth. The remaining failure occurred before
TestLoadIngressAndExactIndependentCommandsThreeNodes entered its test body:
Docker's container-inspection request timed out during mapped-port startup
([report](../../test-reports/cluster/20261018T225633.667431Z-pvp7eai7/report.md)).
Serializing fixture startup with `GQUORUM_TEST_COOK_PARALLEL=1` while retaining
`CLUSTER_PARALLEL=4` did not eliminate this infrastructure failure. That run also
passed 90/91; the snapshot/WAL recovery case hit the same Docker inspection
startup timeout before its body
([report](../../test-reports/cluster/20261018T230211.282027Z-4n7lcp54/report.md)).
The final run uses `CLUSTER_PARALLEL=1` and `GQUORUM_TEST_COOK_PARALLEL=1` to
remove overlap between scenarios. `CLUSTER_TIMEOUT=30m` accommodates serial
suite execution; individual case deadlines and application assertions remain
unchanged. All earlier process runs remain available as evidence.


Final full process suite: **91 passed, zero failures**, 1,144.255 seconds
([report](../../test-reports/cluster/20261018T230834.265657Z-gyqa1ryn/report.md),
[timeline](../../test-reports/cluster/20261018T230834.265657Z-gyqa1ryn/timeline.md)).
Command, after the private image build:

```sh
DOCKER_HOST=unix:///var/run/docker.sock \
TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock \
GQUORUM_TEST_COOK_PARALLEL=1 \
make test-cluster-run CLUSTER_IMAGE=gquorum-test:transport-followups-0b015d7 \
  CLUSTER_PARALLEL=1 CLUSTER_TIMEOUT=30m
```

This passing serial run does not establish that Docker's concurrent-startup
problem or the earlier intermittent scheduler/growth timeouts have been fixed.
Those failures and their successful reruns are documented above. The only
validation-driven code change was the corrected forced-retirement process test;
its production semantics were already established by the planner and existing
cleanup tests.

All 16 capture manifests completed, with raw evidence and interactive reports
retained. Removed the captures' private image tags and the final private test
image. No containers, networks or volumes with the process-test ownership label
remain. The five original staged files and their worktree content remain
unchanged. Every commit is below the 5,000-line added-plus-deleted limit.
