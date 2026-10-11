# Baseline validation record

These results precede the public Node/Cluster API refactor. Current refactor
stages and measured checks are recorded in the
[implementation progress](../plans/20261011-api-refactor.md); full refactor
acceptance remains pending. This baseline remains comparison evidence.

Validated October 11, 2026, with Go 1.26.5 on macOS arm64, Apple M4 Pro
(14 logical CPUs), Docker 29.6.1 linux arm64 and Compose 5.3.
Compilation and Docker caches were warm; unit and Docker scenario results
were uncached. The Docker application uses a normal compiled SDK, while its
harness and the complete in-process library suite run under the race detector.

| Gate | Result |
| --- | --- |
| `make test` | PASS, including the separate caller fixture; latest meta package 127.133s |
| `make test-race` | PASS, including the fixture; latest meta package 137.578s |
| `make test-unit` | PASS, latest uncached run 9.54s; 13.23s during concurrent validation, both below the 30-second budget |
| `make lint` and cluster-tagged vet | PASS |
| `make proto-check` | PASS; pinned regeneration produces no protocol diff |
| Complete cluster harness: `go -C tests/cluster test -race -tags cluster -count=1 -timeout=45m -json ./...` | PASS: all 108 top-level Docker tests, plus 15 fixture tests; Docker package 1489.158s |
| Strengthened drained-voter restart matrix | PASS at three/five/seven nodes, 26.957s combined |
| Watch fanout for all three views | PASS at three/five/seven nodes; three repeated race runs, 51.532s combined |
| Admission client seed/refusal ports | PASS; five repeated race runs, 2.026s combined |
| `make test-example` | PASS on the final small-demo manifest, 9.003s |
| Actual `make run-example` / `make stop-example` | PASS: all three documented endpoints ready, version 1, two workers/one observer and only `_meta`; joined stop preserves volumes |
| `make bench` | PASS; planner and complete FSM snapshot matrices at three/five/seven nodes |
| `git diff --check` | PASS |

The complete Docker run used `gquorum-test:admission-barrier-final`, containing
the final library admission and approval fixes. Subsequent changes strengthened
test assertions and isolated the small demo's volumes. The complete run's
namespace was `f716f7c87f624acd67f39b47`; direct label-filtered inspection found
zero remaining containers, volumes or networks after pool shutdown.

Every performance test includes its deployment, workload and generation release:

| Nodes | Health case | Concurrent-write case | Fifteen-group resource case |
| --- | ---: | ---: | ---: |
| 3 | 1.98s | 4.31s | 42.88s |
| 5 | 2.47s | 5.71s | 44.78s |
| 7 | 2.88s | 5.28s | 43.66s |

All nine cases passed their individual two-minute bounds and their exact-state,
rate/latency or process-resource assertions. The fifteen-group cases verify 165
acknowledged commands, then remove every usable store and runtime registration.
Detailed bounds and prior benchmark/resource measurements are in
[performance checks](performance.md).

The source process inventory contains all 150 scenarios exactly once: 60
applicable adaptations, 89 application cases and one deferred force-new case.
All 156 expanded target symbol references resolve. Assertions preserve native
membership, quorum behavior, learner-first admission, staged intent, exact
authorized identities, durable recovery, ownership and cleanup. The
[source inventory](../brainstorms/20261010-raft-library-test-port.md) records the
application-helper and protocol adaptations; counts alone are not evidence of
behavioral equivalence.

The public API/schema audit covers Runtime ownership, explicit settings/paths,
caller node types, Latest/Target/Active and document history/watches, all twelve
meta FSM commands, sessions/admission/locks, approval and exact effect scopes,
load engine/storage/transport contracts, local events and every demo route.
Native ConfState/cursors replace the source application's runtime-counter mirror;
application configuration, discovery providers, serving policy, load protocol,
SQL/domain FSMs and external transactions remain caller-owned. Force-new recovery,
old-format migration, witness/joint operation and linearizable reads retain their
documented exclusions. Normal restart, readmission and replica reseating are
implemented and tested.

Intermediate failures remain recorded in the implementation log. In particular,
the first watch-matrix repeat exceeded the shared five-second fixture startup
budget under concurrent race suites; its separate fifteen-second startup budget
now passes repeats and complete runs while operation deadlines remain unchanged.
The earlier manual demo startup recovered old load-group topology from retained
volumes; dedicated meta volumes fix that without deleting the old stores.
