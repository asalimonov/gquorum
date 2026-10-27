# gquorum Go library review profile

## Priorities

- Main priority: maintainable Go code and reliable clustering. Prefer explicit contracts, small cohesive packages, clear names, local ownership and predictable failure behavior. Flag abstractions or duplicated state only when they create a concrete maintenance or correctness burden.
- Reliability: consensus safety, durable recovery, exact replica identity, membership authorization, shutdown ownership and bounded resource use take precedence over shorter code or speculative speedups. Never trade durability, sender authentication or identity fencing for lower latency.
- Architectural constraints and limitations are intentional. Recommend the smallest change within the current contracts; do not require new frameworks, generic engines, distributed transactions, provisioning controllers or broad API rewrites without an observed need. Deferred features are not defects merely because they are absent.
- Raft performance and minimal latency: examine proposal-to-commit/apply paths, batching, persistence, snapshots, peer send queues, topology/status collection and lock contention. Seek avoidable serialized work, redundant durable writes, excessive copying, blocking I/O under locks, head-of-line blocking, unbounded queues and synchronous control-plane work on application consensus paths.
- Performance findings must identify a reachable hot path, workload assumption and mechanism affecting throughput or tail latency. Cite code/measurements or label the hypothesis as an open question with a focused benchmark proposal; do not invent latency budgets or treat micro-optimizations as established improvements.
- Favor idiomatic Go, context-aware interfaces, explicit errors, bounded goroutines/queues and understandable concurrency. Cancellation/timeout semantics, idempotency and ownership must be clear to library integrators. Additional abstraction is justified only by demonstrated reuse or a simpler invariant.
- Report tradeoffs: expected reliability/maintenance benefit, latency or resource cost, compatibility impact and a targeted verification method. Avoid recommendations whose complexity exceeds the demonstrated problem.

## Architectural contracts

- Go library (`github.com/asalimonov/gquorum`) for durable clustered applications using etcd Raft, bbolt, WAL, gRPC and protobuf. Applications supply business state machines, load transport, identities and deployment configuration.
- Read `AGENTS.md`, `README.md` and the relevant current contracts in `docs/architecture/library.md` and `docs/architecture/raft.md`.
- Latest is staged intent, Target approved intent, Active converged topology. Staged intent cannot authorize startup or cleanup. Status/document reads are observations, not linearizable barriers. HostNode denotes the reporting node.
- Preserve exact replica identity, authorized retirement/readmission proofs, foreign or contradictory storage evidence, process fencing and monotonically fresh replacement IDs. Deleted group IDs cannot be reused.
- Preserve persisted encodings, protobuf fields and immutable compatibility fixtures. Public API renames are not authorization for changing storage/wire formats. The HTTP example is a separate application boundary; distinguish its JSON contracts from durable metadata encodings.
- Constructor validation does no I/O or callbacks. Successful factories transfer Application.Close to Node; failed factories release partial resources. Borrowed dependencies remain borrowed. Join engine/replay/callback work before releasing owned resources; failed release retains ownership and blocks successors.
- Keep callback inputs owned, calls serialized and queues bounded. Overdue callbacks remain owned until return. A callback may inspect status or RequestStop, but must not join teardown that waits for itself. Proposal cancellation cannot undo committed commands.
- Library enforces authorization and membership safety; replaceable scheduler/lifecycle policies only choose supported decisions. Autoscaling, infrastructure provisioning, cross-group transactions and external side effects belong to applications.
- Root, examples/cluster and tests/cluster are separate Go modules. Root `make test-unit` and `make lint-full` cover all three; Docker/process tests are separate. Generated protobuf, immutable fixtures and historical validation records are not evidence of new checks.

## Reporting bar

- Critical: plausible durable data loss, unauthorized participation/retirement or violation of consensus/identity safety with broad impact.
- Major: a concrete caller-visible correctness defect, lifecycle leak/deadlock, unsafe ownership contract, API ambiguity that realistically causes wrong identity/authorization or unusable integration, or evidenced hot-path behavior that materially damages quorum throughput or tail latency.
- Minor: a demonstrated naming, JSON/documentation or contract inconsistency that materially confuses callers, or localized maintainability/performance cost with a reachable consequence; cite exact conflicting declarations/call sites and a proportionate fix.
- Do not invent naming rules, demand blanket renames or report subjective style preferences. Distinct semantic layers may use distinct names. Explain the caller confusion or failure, affected surfaces, compatibility implications and smallest useful remedy.
- In a whole-current-API audit, existing defects within the specified scope are actionable findings regardless of introduction date. Reserve pre_existing for defects explicitly outside that scope. In a diff review, follow that round's narrower scope.
- Finding nothing is valid. Uncertain design choices belong in open_questions. Verify against live code and current architecture; preserve intentional compatibility names and distinguish them from accidental inconsistencies.
- Review only: do not modify source, stage files, commit, launch Docker workloads or regenerate fixtures. Read nearby implementation/tests as needed to establish claims.
