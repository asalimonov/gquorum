# Public API refactor validation

Status: targeted fixture/setup corrections pass; corrected-image complete
process validation is pending. The
[requirement audit](api-refactor-audit.md) covers the accepted target, algorithm
inventory and failure matrix. This record separates current results from the
[pre-refactor baseline](validation.md).

Library source: `e135c7e`, including the scheduler revalidation and canceled
recovery/shutdown fixes; subsequent fixture-only corrections are described
below. Environment: Go 1.26.5, macOS arm64, Apple M4 Pro,
Docker 29.6.1. Compilation caches were warm; short units, fixture, Compose and
Docker scenarios were uncached. Timings include concurrent validation work.

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
| `make test-fixture` | PASS, uncached caller fixture 95.173s. |
| `make test-example` | PASS, actual Compose application 32.272s. |
| `make test-cluster-run CLUSTER_IMAGE=gquorum-test:api-final-gates CLUSTER_ARGS=-json` | Complete run pending; focused corrected-image regressions pass. |
| `make test-perf-run CLUSTER_IMAGE=gquorum-test:api-final-validation` | PASS, all nine process cases, 148.917s total. |
| `make bench` | PASS, planner and complete FSM snapshot matrices at 3/5/7 nodes; those implementations did not change in the final audit fixes. |
| `make proto-check` | PASS, pinned protocol regeneration; no protocol changes in the final audit fixes. |
| Physical baseline fixture hashes | All seven match the captured baseline manifest. |
| Baseline test migration | All 550 names resolve; 64 renamed/split entries have explicit mappings and transferred assertion responsibilities. |
| Public API / `git diff --check` | PASS; no public Runtime/OpenMeta/OpenLoad or engine aliases, and no private library imports in either application. |

The build plus `test-cluster-run` and `test-perf-run` are the same stages invoked
by `make test-cluster` and `make test-perf`. Earlier complete runs using
`gquorum-test:api-refactor-validation` are supplementary evidence; they do not
contain the final scheduler/shutdown fixes.

The earlier publication-fix run completed with one seven-node pending-pressure
setup failure: connection establishment consumed the engine's one-second
WAL-stall admission window before 80% of its waiter slots were filled. The
harness now acquires the initial HTTP connections behind a barrier before
releasing proposals; pending bounds, excess counts, duration and correctness
assertions remain unchanged. The other failure was the fixture's initial
founding HTTP message omitting all names until its first lifecycle observation.
It now names configured peers until an observation supplies the exact missing
set, with a deterministic HTTP unit regression. These fixture corrections are
checked using `gquorum-test:api-final-gates`. Founding timeout cases passed twice
at each of three/five/seven nodes (62.302s). The seven-node pressure case passed
twice (97.821s): pending peak 256/256, 147/148 samples, more than 15,000 excess
attempts per run, overload p95 below 1.4ms, and exact cancellation/durability
checks. The first focused pressure command matched no tests because of Make's
handling of the regex end marker; that invocation is excluded from evidence.

## Performance matrix

Each case includes deployment, workload and generation release, with its own
two-minute deadline and correctness/resource assertions.

| Nodes | Concurrent health | Concurrent writes | Fifteen groups |
| --- | ---: | ---: | ---: |
| 3 | 2.45s | 5.36s | 37.84s |
| 5 | 2.90s | 5.37s | 42.88s |
| 7 | 3.33s | 6.60s | 40.65s |

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
