# Public API refactor validation

This is the historical API-refactor validation record. The later
authority/read implementation
deliberately changes snapshot/log formats and rejects immutable legacy meta/load
fixtures without rewriting them; the results below remain those actually run at
the recorded source revision.

## Scoped operations update — 2026-10-17

Current contracts supersede the historical Active/global-operator model below:
Latest/Target intent, independent group/node convergence, Operation/WaitOperation/
RetryOperation, exact CAS/rebase and detached cleanup. See the
[current architecture](../architecture/library.md) and
[implementation log](../plans/20261015-failure-isolation-and-operational-api.md#progress-log).
Metadata store/FSM/envelopes are v4; the independent authority journal is v2.
Changed older formats refuse unchanged; there is no migration or mixed-version
support. Frozen fixtures and immutable hashes are indexed in
[physical provenance](api-baseline-physical.md).

Post-audit evidence for implementation commit `10b680b` (`e4c5392` plus Task88
corrections): all required test gates pass. The user waived the remaining Claude
review on 2026-10-17 with “Continue w/o claude”. Completion rechecked the source
fingerprint, fixture hashes and report results; the closure changes documentation
only and does not claim a fresh test run or final peer consensus.
The tested source/build set contains 1,096 files; its SHA-256 is
`e7b9b2a60cd4c4d42df52020ef843c2bee325e797c011a9646897436902ed7d3`.
The fingerprint hashes sorted tracked Go/proto/go.mod/go.sum/Makefile/Dockerfile
paths and bytes, each followed by NUL. The only correction after non-Docker
suites/benchmark was clustertest-only stop/readiness barriers; library, fixture and
benchmark sources are unchanged. Final units/lint and complete cluster rerun
followed that correction and passed.

| Check | Actual evidence under `test-reports/` unless otherwise stated |
| --- | --- |
| Fast units | PASS 1,884/1,884; `unit/20261017T031345.867812Z-wbi1bvet/report.md`. |
| Unit race | PASS 1,884/1,884; `unit-race/20261017T030141.240620Z-vdcfzk_u/report.md`. |
| Functional | PASS 1,043/1,043; `function/20261017T030207.714644Z-62nx4vsx/report.md`. |
| Functional race | PASS 1,043/1,043; `function-race/20261017T030252.395699Z-jca4sx81/report.md`. |
| Lint | PASS; `architecture/20261017-task88/lint.log`, all configured module/build-tag analyzers. |
| Physical formats | All seven legacy baseline files and five current frozen fixtures match their recorded SHA-256 hashes. No protocol sources changed. |
| Debt scale/bounds | PASS count=3; `bench/20261017-task88/benchmark.log`. At 10k debts: ~51.3 MB persisted per Apply; median encoding 228.7 ms, bbolt 253.1 ms, restore 1.56 s, progress page 5.37 ms. Separate cleanup selection stays bounded (~344 ns); ordinary four-block selection costs 35.2 ms. |
| Focused process | PASS 6/6 isolation cases (`cluster/20261017T030540.982912Z-11m_z9_l`); exact replay setup regression PASS 3/3 (`cluster/20261017T031315.217037Z-qp98zmp2`). |
| Complete process suite | PASS 91/91, 260.691s; `cluster/20261017T031352.388287Z-yggkc_uc/report.md`, no failures/skips. |
| Compose basic | PASS, 32.226s; `example/20261017T031813.126290Z-4bpecc1h/report.md`. |
| Compose scheduler | PASS, 131.121s; `example/20261017T031845.408057Z-1ejzrshw/report.md`. |
| Compose cache | PASS, 40.816s; `example/20261017T032056.589394Z-ok6sjmbf/report.md`. |
| Resource/format checks | No run-owned containers/networks/volumes or demo containers remain. Unrelated pre-existing volumes are preserved. Image digest is unchanged; local documentation links and git diff --check pass. |
| Peer review | Final thread42 review waived by the user on 2026-10-17 (“Continue w/o claude”). Local architecture/dead-code review is complete; later fixes have no claimed peer consensus. |

Final application image: `gquorum-test:fi-e4c5392-task88a`, OCI index
`sha256:035a0fc5e46a51cc200f28154480d4a222bed6bc13c81ebd6418119c2b660947`,
config `sha256:eb3a1f6c6f9344a502f7614e1139024182a15ca7f4873d4fee2410744e78716e`.
The first complete post-audit run was 90/91 (`cluster/20261017T030558.058983Z-2gm3okjf`):
a replay test raced detached Stop between plan capture and approval. Its explicit
predecessor-stop barrier and subsequent serving-readiness barrier retain all
exact grant, membership, storage, value and consistent-read assertions. These
changes affect only the host-side clustertest harness, so the application image
is identical for the focused and complete reruns.

The historical October11 gates below remain evidence only for their recorded
revision. They do not validate the current scoped cutover or its process image.

## Historical API refactor — October 11, 2026

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

| Historical gate | Result at recorded revision |
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
