# gquorum management model

This is the executable specification being built from the [target design](../../docs/brainstorms/20261027-quint-cluster-management-target.md) and [revised 18-task plan](../../docs/plans/20261027-quint-cluster-management.md). The implemented checkpoint and omissions are explicit in [coverage.md](coverage.md). It is not yet a complete cluster-management model and does not prove the Go implementation correct.

## Initial scope revision

The user approved a lean first version after Task 4. The [coverage ledger](coverage.md), target and plan now defer labels/selectors/domain placement, live policy tuning, EnsureGroup/RollbackTo, application locks/manual session workflows, serving override, storage-loss/memory-store suites and offline Recovery. Public metrics/getter/watch/history plumbing is omitted; derived operator visibility and minimum internal session/availability safety remain. The user further fixed performance/stability/metric-freshness evidence to favorable values: no lag/backlog, no-progress, stale-metric or stability/cooldown scenarios in the initial iteration. Quorum/crash/partition behavior and the successful join catch-up milestone remain.

Implementation resumed at Task 5. Executable scope checks constrain invocation and every safety check; `coreChecks` selects retained tests explicitly. Broader Task 4 helpers/tests remain separate historical work. Metrics are fixed favorable; availability, quorum, exact identity, captured topology and local apply still matter.

## Run the current checkpoint

Use Quint **0.33.0**. The vendored libraries are pinned independently in [vendor/provenance.json](vendor/provenance.json). An existing installation of the pinned CLI is required; these commands do not install dependencies.

```sh
quint typecheck scripts/quint/instances/foundation.qnt
quint test scripts/quint/instances/foundation.qnt --main foundation --seed 1 --max-samples 10
quint run scripts/quint/instances/foundation.qnt --main foundation --invariant safety --witnesses deliveredRequest --seed 1 --max-samples 50 --max-steps 20
```

The foundation has one newly constructed, unadmitted actor and bounds of two calls and eight delivery identities. It models queuing, delivery and Choreo handling of a local phase observation and an invalid request. Neither operation creates consensus or authority. The request/delivery bounds and dropped calls can terminate exploration; that is a finite harness boundary, not evidence of a protocol deadlock. Successful `run` means no violation in sampled traces, not exhaustive checking.

## Profile runner

```sh
python3 scripts/quint/run.py --profile foundation --mode test
python3 scripts/quint/run.py --profile foundation --mode simulate --seed 1
python3 -m unittest discover -s scripts/quint -p '*_test.py'
```

Profiles pin concrete modules, tests, properties, bounds, seeds/sample counts and backends. Before the requested check, the runner executes a zero-step invariant comparing the instance's `modelBounds` to the manifest; a stale profile cannot claim different bounds from those executed. Every declared test name must appear as passed; empty/missing/skipped selections fail.

Artifacts are retained under `test-reports/quint/<run>/`: `report.md`, `manifest.json`, separate child logs, machine-readable Quint output and available ITF traces. `--reports-dir` overrides `TEST_REPORTS_DIR`; either specifies the report root, with `quint/` appended. `latest` changes only after reporting a finished non-interrupted run. Failures keep their artifacts and nonzero status. A timeout or signal is incomplete, not a verified pass.

The default profile uses the bundled TypeScript evaluator. Rust profiles require the preinstalled evaluator pinned by Quint 0.33.0 (v0.7.0); verification needs explicitly provisioned pinned Apalache/JVM files. No backend is downloaded by this runner. Verification is refused until a profile is marked qualified following revised Task 13's backend controls. Its implementation is not a current claim of exhaustive/liveness qualification.

Quint 0.33.0's machine-readable simulation output omits witness counts. The manifest records that omission instead of inventing counts; deterministic witness tests are separate evidence. Direct `quint run` above prints sampled witness counts. Model hashes and original child exit statuses are retained for reproduction.

## Composition

`types.qnt` contains data records. `handlers.qnt` uses the unmodified Choreo `cue`, `convert_context` and `apply_effect` functions. `cluster.qnt` owns the single `world` state variable; Choreo's unused own state/display actions are not invoked. Its `GlobalContext` groups actors and inboxes, with separately typed metadata, consensus, network and recovery records in `extensions`.

Enqueue and delivery are explicit transitions before `main_listener` can consume a request. Built-in Choreo Send/Broadcast are not used to skip transport delivery. `cluster_test.qnt` explicitly re-exports core actions; concrete instances bind the core once and expose the tests and state-reading properties. `properties.qnt` reads records through pure predicates, with no second cluster instantiation.

The target's record field `action` is spelled `kind` in Quint because `action` is a reserved keyword. Proof forms remain distinct: action/readmission receipts carry exact optional fields, while fences retain separate evidence. StorageDefault/StorageMemory is the replicated group class; OwnedWAL/BorrowedCustom is a node setting.

## Pending implementation

Task 5 now wires shared reads, staging, approval, exact retry and atomic reseat/rebase. Execution grants/fences/receipts, ownership, membership, creation/deletion effects, cleanup and scheduler work remain in Tasks 6–11. Minimum internal sessions belong to Task 7 and shared fresh reads to Task 5; no separate public session/lock or offline Recovery workflow is required. Tasks 12–18 cover core visibility/coverage, progress/verification, tooling, audit, review and documentation. No pending path returns an invented successful result.

Do not infer completed coverage from the presence of a record or enum. Add each transition with an executable witness and invariants, retain counterexamples, and update the coverage matrix before extending claims.

## Scope boundaries

- TODO(not planned): concrete Raft replication/elections; the eventual membership service abstracts safe consensus while exposing management boundaries.
- TODO(not planned): application business semantics, external transactions and outboxes.
- TODO(not planned): byte-level and OS/cryptographic modeling; failure evidence and authenticated logical messages are the boundary.
- TODO(not planned): automatic Go trace replay; counterexamples will have manual regression recipes.
- TODO(not planned): strict global operation gate; the agreed baseline is implemented scoped concurrency.
- TODO(not planned): universal five-node exhaustive guarantee; all results require named finite bounds and completion evidence.

No production Go, storage format, public API or default test target is changed by this checkpoint.

## Three-node consensus/network checkpoint

```sh
python3 scripts/quint/run.py --profile smoke3 --mode test
python3 scripts/quint/run.py --profile smoke3 --mode simulate --seed 1
```

`smoke3` initializes three established nodes, `_meta`, and load groups `a` and `b`. Its initializer assumes consistent approved/proven declarations, pins, valid stores and running replicas; it is not a shortcut for the later create/admit workflows. The profile binds three calls per process, position 8, incarnation 2 and delivery ID 40 limits and uses the preinstalled pinned Rust evaluator.

The environment abstracts consensus commitment/election/catch-up. A commit requires an exact running leader plus a bidirectionally reachable majority of native voters with valid retained identity/storage; learners and healthy observations cannot supply votes. Host membership is distinct from connectivity. `commitProgress` advances a bounded consensus position, not an application result. Metadata commit and each node's durable/local application are separate actions. Native leader authority may change while local leader observations stay stale.

Crashes discard process-owned calls/observations/owners and retain durable pin/storage evidence. Restart supplies a new incarnation and restores only retained metadata into Starting; it does not restore published load handles. Delayed old-incarnation replies are consumed without completing a current call. Loads stop contributing votes after their local engine is gone. `HostState.metaImage/metaIndex` represent retained metadata state, and `OwnerState.engineRunning` distinguishes native participation from public publication. Those implementation fields refine the target records without changing production storage.

Management inboxes and in-flight messages are distinct, and link changes, loss and duplicate delivery are explicit actions. Requests include cross-node metadata proposals and leader-side approval work; native membership and effect dispatch arrive in later tasks. Tests exercise both directions of metadata/load isolation, asymmetric links, stale observations, delayed application, majority loss, restart/catch-up, old messages, duplicates and loss. Progress/liveness qualification remains pending in revised Tasks 13–14; no automatic readiness after restart is claimed yet.

## Historical Task 4 checkpoint (broader than initial scope)

Task 4 implements pure validation, edits, placement and immutable plan construction in `topology.qnt`, with `placement.qnt` and `planning.qnt` helpers. The original concrete `topologyChecks` suite remains available separately. Initial acceptance uses `coreChecks`, which excludes label/policy/rollback extension tests and imports only named tests to avoid helper-name collisions.

Placement has distinct minimum/maximum voter bounds and total host capacity. Configured hosting differs from legacy placement; adoption pins intentional learners, explicit host lists retain duplicate validation before normalization, and reserved nodes retain only exact approved identities. Defaults and configuration validation preserve voter surge, lag and stability/cooldown controls. Full-document Stage, typed mutations, preview, forced retirement, readmission, reseating and rollback have separate guarded edit paths.

The planner returns `Planned(plan)` or `PlanRejected(errors)`, never an empty successful plan for a refusal. Required steps and detached cleanup remain separate, with exact identities, voter-set guards, source versions, step sequences and dependencies. Replacements respect host ownership, learner-first admission, catch-up revalidation and transfer before leader removal. Already-proven stops are retained. Deleted-group plans require no native load observation/quorum and carry host cleanup authority separately from logical deletion.

`ReseatReplica` uses atomic repair-and-approval against approved intent and retained allocation floors; Task 5 wires that commitment and independent-intent rebase. Rollback has a new version and historical parent independently of the original CAS. The retained identity inventory supplements unapproved removals without reusing retired or overwritten allocations.

The 40 topology tests cover semantic input classes and plan sequencing. Symbolic strings represent valid names/labels except explicit empty/reserved cases; byte-level encoding/UTF-8/JSON validation is outside this model. Malformed role encodings are represented by the generic InvalidRequest path before constructing typed ReplicaRole values. No exhaustive verification or Go conformance claim follows from these unit/scenario checks.

## Metadata transaction checkpoint

`reads.qnt`, `transactions.qnt`, `operations.qnt`, `authority.qnt` and `repair.qnt` keep pure rules separate from the single Choreo composition. A request captures its original CAS/audit, queues work, commits through native `_meta` quorum, waits for the relevant local application and replies separately. Raw Stage preserves literal CAS; a convenience zero CAS is captured once. Reseat first confirms/applies a read before capturing zero CAS, then commits its repair, approval and any independent-intent rebase atomically.

Approval work runs on the receiving leader after a shared quorum-read/local-apply barrier. Quorum-confirmed native reads and availability observations are separate steps. A second metadata confirmation then recaptures the affected scopes; distinct read rounds reject delayed earlier acknowledgements. Metadata commitment rechecks the captured scopes and immutable plan content. Replaying approval returns the retained first attempt; RetryOperation derives one exact successor of a failed attempt. Retry scenarios currently include a retained failed-before-effect fixture; runtime execution failure and consumed-effect reconciliation belong to Task 6.

Logical deletion retains exact stop/retirement actions and cleanup debt without inventing receipts or issuing execution grants. Existing native replicas remain until later execution/lifecycle tasks implement their obligations. A staged version survives approval failure. Cancellation stops a waiter without undoing a submitted mutation. It returns an indeterminate outcome for a pending submission, or preserves a known staged version with cancellation; old-incarnation acknowledgements cannot complete its replacement's call.

`managementStep` combines new requests with `protocolStep`. The latter runs an existing request through ordinary processing without retry flooding. The full `step` also includes fault/retry actions and abstract native activity; retries remain reachable there. These subsets share the same state and transitions. Bound exhaustion and unconstrained permanent failures can end traces; neither sampled safety success nor a deterministic witness proves fair progress. Backend qualification remains pending.

Batch `quint test` runs after significant implementation changes; use cheap typechecks during editing and targeted diagnostics for failures. Source-faithful failing library scenarios belong in the [findings report](../../docs/reports/20261027-quint-cluster-management.md), retaining their properties, traces and reproduction commands.
