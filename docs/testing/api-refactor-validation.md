# Public API refactor validation

This is the historical API-refactor validation record. The later
[authority/read implementation](../plans/20261012-authority-contract-consistent-reads.md)
deliberately changes snapshot/log formats and rejects immutable legacy meta/load
fixtures without rewriting them; the results below remain those actually run at
the recorded source revision.

Status: **complete**, validated October 11, 2026. The corrected-image complete
process run and all other required gates pass. The
[requirement audit](api-refactor-audit.md) covers the accepted target, algorithm
inventory and failure matrix. This record separates current results from the
[pre-refactor baseline](validation.md).

Validated source: `355b728`. Library changes end at `e135c7e`, including the
scheduler revalidation and canceled recovery/shutdown fixes; `355b728` contains
the fixture-only corrections described below. Environment: Go 1.26.5, macOS
arm64, Apple M4 Pro, Docker 29.6.1. Compilation caches were warm; short units,
fixture, Compose and Docker scenarios were uncached. Timings include concurrent
validation work.

The corrected process image is `gquorum-test:api-final-gates`:

- Image index: `sha256:65a37e9bcce329bfa4805504237706257d8d755d8e2213d74caee826d55e5336`.
- Application image config: `sha256:12e02c9fb3babe270c77529ad37ad22f3203d7322deee526eb47c5cc45dcd47f`.
- Built through `make test-cluster-build`; the run targets below reuse that image
  without replacing its tag while scenarios are active.

| Gate | Current result |
| --- | --- |
| `make test` | PASS, including the corrected complete caller fixture (84.784s). |
| `make test-unit` | PASS, uncached elapsed **15.61s**, below the 30-second budget. |
| `make test-unit-race` | PASS in both modules. |
| `make lint` and cluster-tagged vet | PASS. |
| `make test-fixture` | PASS, uncached caller fixture 95.173s; the corrected fixture also passes in `make test` and the final complete process run (28 tests, 80.056s). |
| `make test-example` | PASS, actual Compose application 32.272s. |
| `make test-cluster-run CLUSTER_IMAGE=gquorum-test:api-final-gates CLUSTER_ARGS=-json` | PASS, all **108** process scenarios (1417.516s) and **28** caller fixture tests (80.056s). Exit status 0; no skipped test cases. |
| `make test-perf-run CLUSTER_IMAGE=gquorum-test:api-final-validation` | PASS, all nine process cases, 148.917s total. All nine also pass against `api-final-gates` within the complete process run; see the matrix below. |
| `make bench` | PASS, planner and complete FSM snapshot matrices at 3/5/7 nodes; those implementations did not change in the final audit fixes. |
| `make proto-check` | PASS, pinned protocol regeneration; no protocol changes in the final audit fixes. |
| Physical baseline fixture hashes | All seven match the captured baseline manifest. |
| Baseline test migration | All 550 names resolve; 64 renamed/split entries have explicit mappings and transferred assertion responsibilities. |
| Public API / `git diff --check` | PASS; no public Runtime/OpenMeta/OpenLoad or engine aliases, and no private library imports in either application. |

The build plus `test-cluster-run` and `test-perf-run` are the same stages invoked
by `make test-cluster` and `make test-perf`. Earlier complete runs using
`gquorum-test:api-refactor-validation` are supplementary evidence; they do not
contain the final scheduler/shutdown fixes.

The final complete run includes the three/five/seven-node fault and recovery
matrices, all nine performance cases, fifteen independent load groups and
removal of every local store. After command completion, no containers, networks
or volumes remained for run `9cc003fea2a5af0716099606`; the image digest still
matched the recorded build. The generated `testapp/loadpb` package has no tests;
every actual test case ran.

The earlier publication-fix run completed with one seven-node pending-pressure
setup failure: connection establishment consumed the engine's one-second
WAL-stall admission window before 80% of its waiter slots were filled. The
harness now acquires the initial HTTP connections behind a barrier before
releasing proposals; pending bounds, excess counts, duration and correctness
assertions remain unchanged. The subsequent audit-fix image run finished with
107 process cases passing and one failure: the fixture's initial founding HTTP
message omitted all names until its first lifecycle observation.
It now names configured peers until an observation supplies the exact missing
set, with a deterministic HTTP unit regression. These fixture corrections are
checked using `gquorum-test:api-final-gates`. Founding timeout cases passed twice
at each of three/five/seven nodes (62.302s). The seven-node pressure case passed
twice (97.821s): pending peak 256/256, 147/148 samples, more than 15,000 excess
attempts per run, overload p95 below 1.4ms, and exact cancellation/durability
checks. The first focused pressure command matched no tests because of Make's
handling of the regex end marker; that invocation is excluded from evidence.

The complete corrected-image run also passes pressure cases at three, five and
seven nodes. Each reaches 256 pending requests; overload p95 is respectively
3.098ms, 2.000ms and 1.766ms, with 146–147 continuous samples per case. Exact
cancellation and durability assertions pass at every size.

## Performance matrix

These results use `gquorum-test:api-final-gates` in the complete process run.
Each case includes deployment, workload and generation release, with its own
two-minute deadline and correctness/resource assertions.

| Nodes | Concurrent health | Concurrent writes | Fifteen groups |
| --- | ---: | ---: | ---: |
| 3 | 2.12s | 4.49s | 41.01s |
| 5 | 2.33s | 5.95s | 41.52s |
| 7 | 2.79s | 5.34s | 46.20s |

## Targeted regression evidence

The complete managed race suite passes (4.599s). The public application
Close-error/explicit-retry scenario passes fifty consecutive race runs
(96.654s). Deterministic tests separately reproduce and guard the exact
startup-publication and canceled-recovery release boundaries.

Scheduler race tests cover a replica recovering during caller code, staged
intent, unknown health, lost quorum, transferred leadership, local holds and
callbacks that ignore cancellation. Public multi-node tests verify independent
caller policy choices across leadership transfer without policy agreement
metadata. No existing Close-error or membership assertion was weakened.

Physical compatibility tests open baseline-produced meta bbolt, load WAL,
snapshots and event journal files. They verify exact pins and bucket records,
full application snapshot plus committed-tail recovery, retained request IDs,
and append/reopen without rewriting legacy journal rows. Provenance and hashes
are recorded in [physical compatibility](api-baseline-physical.md).
