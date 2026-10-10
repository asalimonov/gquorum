# Implementation: gquorum

Objective: deliver github.com/asalimonov/gquorum for Go 1.26, preserving the agreed meta FSM, application-defined load groups, explicit settings/paths, safe authorized effects, durable stores and full test coverage.

Design: ../brainstorms/20261010-raft-library-target.md. Test obligations: ../brainstorms/20261010-raft-library-test-port.md. Source baseline: fc982c0. The user's implementation instruction supersedes the earlier read-only brainstorm stage.

The process-cluster harness must use testcontainers with isolated, reusable deployment units and a bounded pool, adapted from the supplied source. The three-node Compose demo needs REST e2e coverage. Required performance checks run in the cluster suite and each completes in less than two minutes. Only Limits carries maximum node/replica capacity. The Go module path is github.com/asalimonov/gquorum.

## Implementation stages

- [x] 1. Public topology, limits, identities, faults and contracts (target §§3–5); deterministic validation and source document tests.
- [x] 2. Fixed membership policy/coordinator (target §§3–4); port safety/promotion/serialization tests with cancellation and lifetime regressions.
- [x] 3. Explicit-path synchronous meta bbolt persistence and local identity ledger (target §6); port atomic snapshot, replay, applied ConfState and identity tests.
- [x] 4. Generic etcd WAL store and complete snapshot recovery (target §§5–6, 8.5, 11); conformance, tail overwrite, pins, quarantine, fallback and refiller tests.
- [ ] 5. Caller-defined load runtime (target §§5, 8.4–8.5, 11); Ready ordering, cancellation, membership, snapshots, retry/backpressure and multi-group tests.
- [ ] 6. Full replicated meta FSM (target §§3–6, 8, 11); documents/history, approvals/executions, admission, sessions/locks, deterministic replay and retirement tests.
- [ ] 7. Meta runtime/protocol/bootstrap/join (target §§5, 7–8); real meta groups, authorization, restore, peer isolation and failover functional tests.
- [ ] 8. Topology mutation/planning/convergence and caller effects (target §§4–5, 8, 11); preserve all applicable source contracts and idempotent exact-identity verification.
- [ ] 9. Event subscriptions/local bbolt journal and public runtime lifecycle (target §§5–6, 8.7); ordering, loss semantics, ownership and shutdown tests.
- [ ] 10. Three-node Compose demo with caller load protocol and REST API (target §9); make run-example/stop-example, documented demonstrations and REST e2e tests.
- [ ] 11. Testcontainers pool and complete source-test ports (target §§9.1–9.2 plus inventory); isolated reusable units, three/five/seven-node correctness and fifteen groups.
- [ ] 12. Cluster performance checks and planner/snapshot benchmarks (target §9.2); bounded two-minute cases, concrete throughput/latency/resource targets and recorded results.
- [ ] 13. Architecture and completion audit: public APIs, schemas, every FSM command, transport, caller effect, example route and source-test obligation match the design; all components are wired into real entry points. Fix gaps and run all validation before declaring completion.

## Validation commands

After each stage run make test. For runtime/concurrency changes also run scoped race tests. Final validation includes make test, make test-race, make lint, protocol regeneration diff, make test-cluster, cluster performance cases, make bench, Compose startup and REST e2e checks, and git diff --check. No gate is satisfied by an unexecuted plan or a skipped scenario.

## Progress

- Initial repository/source and Docker inspection completed; Go 1.26.5 toolchain resolved.
- Stages 1–3 implemented: typed faults/topology validation and explicit Limits, fixed membership policy/coordinator, synchronous versioned bbolt meta store and exact identity ledger. Source document, 19 membership and 14 bbolt tests adapted, with additional cancellation, atomicity, overflow and identity regressions. `make test`, scoped `go test -race` and `make lint` pass.
- Stage 4 verified: exact versioned pins and WAL metadata, full snapshots/real ConfState, contiguous tail replay, two-snapshot fallback retention, scoped quarantine/sweep, process-wide segment registration and joined refillers. Ten reusable conformance scenarios and source WAL/refiller scenarios pass, with added committed-write protection, concurrent idempotent writes, corrupt-newest-after-compaction fallback and caller ownership regressions. `make test`, storage race tests and `make lint` pass. Recovery owns read descriptors while using the upstream WAL decoder, avoiding early-corruption descriptor leaks in etcd 3.6 read-only handles.

- Stage 5 dependency implemented ahead of runtime wiring: local event journal and bounded bus, with source retention/sequence/pagination assertions adapted and added filtering, journal-before-delivery, live-only failure, owned-copy, saturation/gap and shutdown tests. `make test`, events race tests and `make lint` pass. Stage 9 remains open for runtime recorders/facade lifecycle.

## Outstanding implementation gaps

All unchecked stages above remain required. Force-new, old source-format migration, application SQL/configuration/licensing, witness mode, joint membership and linearizable read APIs remain the previously specified exclusions.
