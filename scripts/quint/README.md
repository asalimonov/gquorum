# gquorum management model

This is the executable specification being built from the [target design](../../docs/brainstorms/20261027-quint-cluster-management-target.md) and [revised 18-task plan](../../docs/plans/20261027-quint-cluster-management.md). The implemented checkpoint and omissions are explicit in [coverage.md](coverage.md). It is not yet a complete cluster-management model and does not prove the Go implementation correct.

## Initial scope revision

The user approved a lean first version after Task 4. The [coverage ledger](coverage.md), target and plan now defer labels/selectors/domain placement, live policy tuning, EnsureGroup/RollbackTo, application locks/manual session workflows, serving override, storage-loss/memory-store suites and offline Recovery. Public metrics/getter/watch/history plumbing is omitted; derived operator visibility and minimum internal session/availability safety remain. The user further fixed performance/stability/metric-freshness evidence to favorable values: no lag/backlog, no-progress, stale-metric or stability/cooldown scenarios in the initial iteration. Quorum/crash/partition behavior and the successful join catch-up milestone remain.

Protocol implementation has completed Tasks 1–9; cleanup, collection and readmission are next in Task 10. The separate module refactoring is complete. Executable scope checks constrain invocation and every safety check; `coreChecks` selects retained tests explicitly. Broader Task 4 helpers/tests remain separate historical work. Metrics are fixed favorable; availability, quorum, exact identity, captured topology and local apply still matter.

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

## Composition and module ownership

`cluster.qnt` owns the only mutable `world`, stable named action adapters and exploration selectors. `types.qnt` contains the unchanged data catalogue; `runtime/world.qnt` provides Choreo aliases without another state instance. Pure subsystem functions receive World explicitly. Choreo's unused state/display actions are not invoked.

| Location | Responsibility |
| --- | --- |
| `topology/` | Document edits, placement and immutable planning |
| `metaengine/` | Approval, scoped ownership, grants, repair and session rules |
| `metaservice/` | Reads, caller/leader coordination, listeners, authority and session steps |
| `managed/` | Replica ownership, local lifetime, node shutdown and effect consumption |
| `consensus/` | Abstract quorum, election, commit/apply and catch-up |
| `runtime/` | World aliases, canonical Queue/Consume effects and state framing |
| `harness/` | Initial states, scope, properties and environment faults |
| `tests/fixtures/`, `tests/unit/`, `tests/scenarios/` | Stateless setup, pure rule checks and asynchronous traces |
| `instances/`, `profiles/` | Concrete bounds, entry points and exact test inventories |

A deterministic phase uses `phaseEnabled(w, ...)` and `phaseNext(w, ...)` in its subsystem. Its facade action uses `guarded(condition, before => phaseNext(before, ...))`; a false guard disables the action without evaluating the callback. `withNode`, `withProcess` and `withOwner` preserve the surrounding state frame. They do not authorize changes or supply missing map entries: guards and exact identity checks stay in domain code. Nondeterministic choices remain in the facade. Enqueue, delivery, handling, commitment, application and reply stay separate transitions.

Runtime suites deliberately form one chain: cluster → membership → reads → transactions → operations → effects → lifetime. A concrete instance binds constants once at its leaf. Distinct adjacent-stage tests check shared state through effect typing; the root check is its baseline. Do not compose independently instantiated sibling suites. Pure fixtures have no `var` or `run` declarations and do not import test suites.

For a new protocol phase, extend the owning rule/step module and its named facade action, wire the appropriate selector, then add an interruptible scenario at the relevant chain stage and list it in affected profile inventories. Reuse stateless fixtures and frame helpers. Production modules cannot import tests or cluster; runtime cannot import domain rules. The detailed dependency table is in the [refactoring plan](../../docs/plans/20261028-quint-module-refactoring.md).

The target's record field `action` is spelled `kind` because Quint reserves `action`. Action/readmission receipts and fences retain distinct proof forms. StorageDefault/StorageMemory is the group class; OwnedWAL/BorrowedCustom is a node setting. Deferred records do not imply active coverage.

## Pending implementation

Tasks 1–7 implement shared reads, staging/approval/retry/reseat, authority execution/fencing, local ownership/lifetime and minimum internal sessions. Tasks 8–9 add membership/scaling and group creation/detached deletion. Tasks 10–11 retain retirement/collection/readmission and scheduler repair. Tasks 12–18 retain coverage/visibility, progress/verification, tooling, audit, review and documentation work. Refactoring does not complete those protocol tasks or introduce successful placeholders.

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

Task 4 implements pure validation, edits, placement and immutable plan construction in `topology/topology.qnt`, with `topology/placement.qnt` and `topology/planning.qnt` helpers. The original concrete `topologyChecks` suite remains available separately. Initial acceptance uses `coreChecks`, which excludes label/policy/rollback extension tests and imports only named tests to avoid helper-name collisions.

Placement has distinct minimum/maximum voter bounds and total host capacity. Configured hosting differs from legacy placement; adoption pins intentional learners, explicit host lists retain duplicate validation before normalization, and reserved nodes retain only exact approved identities. Defaults and configuration validation preserve voter surge, lag and stability/cooldown controls. Full-document Stage, typed mutations, preview, forced retirement, readmission, reseating and rollback have separate guarded edit paths.

The planner returns `Planned(plan)` or `PlanRejected(errors)`, never an empty successful plan for a refusal. Required steps and detached cleanup remain separate, with exact identities, voter-set guards, source versions, step sequences and dependencies. Replacements respect host ownership, learner-first admission, catch-up revalidation and transfer before leader removal. Already-proven stops are retained. Deleted-group plans require no native load observation/quorum and carry host cleanup authority separately from logical deletion.

`ReseatReplica` uses atomic repair-and-approval against approved intent and retained allocation floors; Task 5 wires that commitment and independent-intent rebase. Rollback has a new version and historical parent independently of the original CAS. The retained identity inventory supplements unapproved removals without reusing retired or overwritten allocations.

The 40 topology tests cover semantic input classes and plan sequencing. Symbolic strings represent valid names/labels except explicit empty/reserved cases; byte-level encoding/UTF-8/JSON validation is outside this model. Malformed role encodings are represented by the generic InvalidRequest path before constructing typed ReplicaRole values. No exhaustive verification or Go conformance claim follows from these unit/scenario checks.

## Metadata transaction checkpoint

`metaservice/reads.qnt`, `metaservice/transactions.qnt`, `metaservice/operations.qnt`, `metaengine/authority.qnt` and `metaengine/repair.qnt` keep pure rules separate from the single Choreo composition. A request captures its original CAS/audit, queues work, commits through native `_meta` quorum, waits for the relevant local application and replies separately. Raw Stage preserves literal CAS; a convenience zero CAS is captured once. Reseat first confirms/applies a read before capturing zero CAS, then commits its repair, approval and any independent-intent rebase atomically.

Approval work runs on the receiving leader after a shared quorum-read/local-apply barrier. Quorum-confirmed native reads and availability observations are separate steps. A second metadata confirmation then recaptures the affected scopes; distinct read rounds reject delayed earlier acknowledgements. Metadata commitment rechecks the captured scopes and immutable plan content. Replaying approval returns the retained first attempt; RetryOperation derives one exact successor of a failed attempt. Retry scenarios include retained failure fixtures before and after consumption. `metaservice/remaining.qnt` validates the immutable suffix against exact receipt proof, native predecessor/result membership, transfer prerequisites and capacity; later lifecycle/membership tasks supply their effect-specific failure transitions.

Logical deletion retains exact stop/retirement actions and cleanup debt without inventing receipts or issuing execution grants. Existing native replicas remain until later execution/lifecycle tasks implement their obligations. A staged version survives approval failure. Cancellation stops a waiter without undoing a submitted mutation. It returns an indeterminate outcome for a pending submission, or preserves a known staged version with cancellation; old-incarnation acknowledgements cannot complete its replacement's call.

`managementStep` combines new requests with `protocolStep`. The latter runs an existing request through ordinary processing without retry flooding. The full `step` also includes fault/retry actions and abstract native activity; retries remain reachable there. These subsets share the same state and transitions. Bound exhaustion and unconstrained permanent failures can end traces; neither sampled safety success nor a deterministic witness proves fair progress. Backend qualification remains pending.

Batch `quint test` runs after significant implementation changes; use cheap typechecks during editing and targeted diagnostics for failures. Source-faithful failing library scenarios belong in the [findings report](../../docs/reports/20261027-quint-cluster-management.md), retaining their properties, traces and reproduction commands.

## Authority execution checkpoint

`metaengine/grants.qnt`, `metaengine/controls.qnt`, `managed/execution.qnt` and `metaservice/remaining.qnt` implement immutable grant issuance, exact replay, closure, resource fencing, durable receipts, reporting, operator checkpointing and receipt-aware retries. Authority commands share the existing metadata proposal/commit/apply path. Receivers confirm a metadata authority proof that binds the courier's process and member identity, then recheck their own exact identity and physical resource floor after waiting. A bounded consumption-history monitor preserves the identity actually used at that boundary.

The physical path currently executes transfer requests and host/native fences. Transfer-start is durable command consumption, not observed transfer completion. Metadata receipt reporting and checkpointing remain separate. Native fences need native quorum; offline host obligations retain their exact resource/fence keys. Local lifetime is implemented in Task 7; native membership and cleanup effect bodies remain in Tasks 8–10; no success receipt is fabricated for those unfinished paths. Task 7 now separates host-floor installation from completion and waits for older startup/Close work to join.

```sh
python3 scripts/quint/run.py --profile authority3 --mode test --seed 1
python3 scripts/quint/run.py --profile authority3 --mode simulate --seed 1
```

`authority3` uses three nodes, two loads, position bound 16, delivery bound 80 and the same request/incarnation bounds as smoke3. Its simulation starts with a manual transfer already issued and authenticated at the receiver; this is a focused authority fixture, not a fresh-cluster reachability claim. Its test list includes the normal smoke cases, authority regressions and the shared core checks. The internal session lifecycle updates replicated courier bindings. Full public TransferLeader waiter/routing coverage remains part of the API coverage task.

## Local lifetime and internal sessions

`managed/lifecycle.qnt` and `metaengine/sessions.qnt` share the same world and metadata commit path. Local startup separates one exact reservation, shared/cancelable waiters, factory ownership transfer, replay, local Target/floor revalidation and publication. Stop separates admission blocking, engine/callback join, serialized application Close and final release. A timeout does not discard an outstanding factory or Close; a late successful factory still transfers ownership. Holds name exact replicas. Node shutdown keeps metadata participation until managed owners release.

New-owner recovery confirms current host floors; sharing an accepted owner does not require a new quorum query. Scope recapture uses locally applied approved Target, as the implementation does. Established fixtures retain an exact `seedAuthority` set for already-consumed founding history at floor 1. This baseline cannot authorize new identities/effects and is superseded by explicit host authority. Interrupted host fencing retains its durable position and finishes only after older work joins.

Internal session grant and reserved node binding commit separately. Renewal is leader-local and does not prove quorum. Expiry observation precedes an exact-process probe and serialized revoke decision. Renewal replies bind process, member, session and round; delayed expired replies cannot ready a restarted process. Node readiness is an observed local lease/eligibility predicate. A load handle can publish after its own startup checks before the node's session becomes ready. No public session controls or application lock model is enabled.

```sh
python3 scripts/quint/run.py --profile lifetime3 --mode test --seed 1
python3 scripts/quint/run.py --profile lifetime3 --mode simulate --seed 1
```

`lifetime3` uses authority3's bounds and starts simulation with a retained replica reserved for restart. Its 85 runtime scenarios include the earlier management/authority cases and composition/frame checks; 94 shared core checks are selected explicitly. Coverage remains bounded/sampled. Membership/scaling, complete group/cleanup execution and scheduling are later tasks.

## Refactoring evidence

The [execution log](../../docs/progress/20261028-quint-module-refactoring.md) records commits, source-hashed reports and peer review. All original actions and tests remain; current inventories are foundation 10, smoke3 50 + core 94, authority3 65 + core 94, and lifetime3 85 + core 94. These are overlapping suites. New checks validate composition/framing, not additional public API coverage.

`python3 scripts/quint/refactor_audit.py` defaults to the historical mechanical extraction commit `c5ece71`. It checks 95 action mappings against the organization checkpoint; it does not compare current helpers or prove semantic equivalence. Later helper evidence consists of frame tests, preserved scenarios, sampled simulations and source review. Temporary differential artifacts are local-only under gitignored test-reports. See the [delta audit](../../docs/progress/20261028-quint-refactoring-audit.md) for limits. No faster exploration or exhaustive equivalence is claimed.

## Membership, admission and scaling (Task 8)

Native changes use separate authority installation, precheck, revalidated proposal and current-quorum commit actions. Exact receipts precede metadata reporting/checkpointing. Learners cannot vote; promotion needs catch-up; removal/demotion preserve the last voter and require safe post-change membership. A failure after proposal can leave the resulting group unavailable without undoing its committed change. Returned receipts are process-bound cached evidence and can be reported after native quorum is lost; transfer completion still needs observed leadership.

Fresh admission binds an approved member to a process, returns immutable proof, pins the identity and starts the metadata engine. Host preparation, learner addition, catch-up, promotion and internal session activation remain separate. Retained restart cannot reuse an old reply for a new process. Load preparation reuses reservation/factory/replay/publication phases, including retry after a failed factory. No missing established identity is silently refounded.

Node progress waits for relevant load dependencies. `_meta` removal also waits for exact load-stop receipts; native eviction alone is insufficient. Stop authorization, callback/engine joins, application release, durable receipt and metadata reporting remain separate. Native removal creates cleanup debt when its receipt reaches metadata; whole-group deletion retains debt at approval. Applied approved self-removal persists a fence across restart. Retirement, physical collection and complete returning-owner readmission remain Task 10 work.

```sh
python3 scripts/quint/run.py --profile admission4 --mode test --seed 1
python3 scripts/quint/run.py --profile scale5 --mode test --seed 1
```

`admission4` uses four potential nodes, three established founders and two loads, with request/position/incarnation/delivery bounds 3/128/2/512. `scale5` uses five potential nodes with bounds 8/256/2/1024. Its 3→4→5→4→3 witness preserves the original two load groups; the four-node evacuation witness also moves both loads before exclusion. A separate `admission4Meta` case isolates metadata-only host replacement. Provisioning is external: the potential node pool stays fixed while admitted membership changes.

Large profiles declare a 16 GiB Node frontend heap limit; it is a tooling resource setting, not model state. Quint 0.33.0 exports huge full traces for long `quint test` cases, exceeding its Node wrapper's string/heap limits. The profile runner routes explicitly named long witnesses through the same pinned Rust evaluator's REPL. Every listed expression must return exactly `true`; a zero process exit by itself is insufficient. These witnesses execute once at the recorded seed, without full trace export. Ordinary suites retain their configured sample counts, exact test inventories and ITF artifacts; exclusions require matching REPL entries. See the [tooling report](../../docs/reports/20261028-quint-task8-tooling.md).

Rules live in `consensus/membership.qnt`, `metaengine/admission.qnt` and `metaengine/completion.qnt`; `managed/{membership,prepare,stop,removal}_steps.qnt` and `metaservice/admission_steps.qnt` expose the asynchronous phases. `cluster.qnt` wires them into the shared step relation. This checkpoint is scenario/sampled evidence, not exhaustive verification or Go conformance.

Task 8 also preserves live-admission courier identity before session activation and carries same-member admissions into superseding topology operations with their original provenance. `_meta` learner addition requires the admission's Prepared/Ready evidence. The separate desired-success check in `instances/admissionRestartExpected.qnt` intentionally fails for the crash-after-preparation/before-Ready case; [QCM-001](../../docs/reports/20261028-quint-admission-prepared-restart.md) records the source-grounded model reproduction and pending Go regression. Positive profiles include a refusal-state witness and do not claim that this automatic recovery succeeded.

## Group creation and detached deletion (Task 9)

Founding uses the immutable approved peer set. Each required host acquires and publishes its exact replica, then records a host receipt. The native group separately confirms the complete voter set through quorum; host readiness alone is not group completion. Explicit placement on healthy hosts can succeed in a degraded cluster, while an offline required founder must return. A late founder never resets the established native state.

Deletion commits through `_meta`, retains exact stop/retirement obligations and permanently retires the group ID. It does not require load quorum or instantly stop replicas. Pending cleanup and revocation remain separate from logical completion, and unrelated groups may progress. Retirement/collection continue in Task 10. A verification-only obligation monitor detects omitted debt and is not an input to actor decisions.

```sh
python3 scripts/quint/run.py --profile deletion3 --mode test --seed 1
python3 scripts/quint/run.py --profile deletion3 --mode simulate --seed 1
```

The profile has three nodes, two initial loads and one bounded creatable ID `c`; request/position/incarnation/delivery bounds are 6/64/2/256. Initial voter policy remains fixed, with explicit/all-eligible hosting. EnsureGroup and historical required-stop deletion semantics remain excluded.
