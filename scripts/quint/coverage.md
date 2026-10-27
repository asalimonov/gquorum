# Initial-model scope and coverage

Scope revision approved on 2026-10-27: **lean cluster management**. Task 5 is complete. Active invocation/safety checks enforce lean scope; smoke3 selects retained core tests through coreChecks. Shared reads, staging, approval, exact retry and atomic reseat/rebase have asynchronous scenario coverage. Native effects, lifecycle and scheduler work remain; this is not completed v1 coverage.

The original 82-entry ledger was an API inventory, not a requirement to implement 82 state machines. Initial disposition: **17 Core**, **15 Primitive**, **19 Observe**, **8 Omit**, **23 Deferred**. Every entry remains accounted for.

| Disposition | Meaning | Initial acceptance |
| --- | --- | --- |
| Core | Distinct administrative input/transition that changes management state. | Reachable success/refusal/wait/uncertainty cases as applicable. |
| Primitive | Reuse a shared protocol mechanism; do not duplicate it for each public wrapper. | Verify the mechanism and meaningful differences in authorization/cancellation. |
| Observe | Derived output or already-required local observation. | Check freshness boundaries needed by actors; expose useful trace state. No getter/polling machine. |
| Omit | Accessor, telemetry or presentation mechanics with no relevant protocol effect. | No implementation backlog item; keep the reason here. |
| Deferred | A real independent feature excluded from v1. | Do not generate it or claim correctness for it. Not a successful stub or a simulated library refusal. |

## Functional groups and decisions

| Group | Keep in the initial model | Remove from initial work |
| --- | --- | --- |
| Topology transaction | Stage, immutable approval, exact retry, scope ownership, supersession, uncertain replies. | EnsureGroup and RollbackTo convenience/history workflows. |
| Cluster and group administration | Admit/readmit, drain/activate/exclude/force-retire, create/delete, exact hosting changes and replica roles/reseating. | Live placement/voter-policy configuration and standalone pin toggles. |
| Membership and authority | Native quorum, learners/catch-up, transfer, exact grants/fences/receipts, predecessor ownership. | Detailed Raft algorithm and network framing. |
| Runtime/faults | Retained-storage crash/restart, independent network partitions, stale messages, holds, asynchronous startup/stop/release. | Disk-loss/corruption fault suites, memory-storage semantics and offline disaster recovery. |
| Automatic work | Supported repair choices, outdated-decision revalidation, independent intent protection, scheduler enablement; metrics always good. | Custom workload-metric policy design, dynamic tuning and custom policy implementation internals. |
| Sessions | Minimum exact-process session activation/renewal/expiry/revocation required by admission/readiness. | Public manual session workflows and application locks. |
| Operator visibility | Derived Latest/Target, operations, blocked reasons, native membership, owner/debt/fence state. | Metrics, metrics history, watch delivery, query pagination and getter plumbing. |
| Labels and placement dimensions | One node type; fixed voter/capacity presets; explicit or all-eligible host sets. | All labels/selectors, AZ domains, reserved/learner-only labels and descriptive label edits. |

Labels are deferred **as logic**, including paths through Stage, AddNode, CreateGroup and voter config—not just by omitting setters. Empty label/selector maps, no domain policy and one node type are v1 assumptions. Numeric quorum/host limits, exact identity and internal learner/pin safety remain essential.

Deferring offline Recovery does not defer ReadmitNode, ReseatReplica, retained replay, predecessor stop/retirement or cleanup debt. A permanently missing quorum can remain an expected blocker; v1 makes no claim that it can rebuild the cluster.

## Method dispositions

### Core

| Public API | Source | Current evidence | Initial treatment / reason |
| --- | --- | --- | --- |
| `Cluster.Stage` | `cluster.go` | Task 5: asynchronous CAS/apply/reply and exact replay checks | Bounded complete-document edits for core operations only; labels/policy changes are outside v1. |
| `Cluster.Approve` | `cluster.go` | Task 5: leader reads/recapture, immutable scopes, replay and deletion debt | Immutable approved plan, exact captured scopes and detached deletion debt. |
| `Cluster.RetryOperation` | `cluster.go` | Task 5: routed retained failed-attempt scenarios; consumed effects in Task 6 | Exact failed predecessor and at most one successor; preserve uncertain replay. |
| `Cluster.AddNode` | `cluster.go` | Pure edit + shared staging; operation-specific execution remains pending | Fresh identity and mandatory learner-first _meta admission. |
| `Cluster.ReadmitNode` | `cluster.go` | Pure edit + shared staging; operation-specific execution remains pending | Exact returning predecessor, fencing and retirement batch; not offline Rebuild. |
| `Cluster.DrainNode` | `cluster.go` | Pure edit + shared staging; operation-specific execution remains pending | Evacuate loads while retaining _meta membership. |
| `Cluster.ActivateNode` | `cluster.go` | Pure edit + shared staging; operation-specific execution remains pending | Undo drain eligibility; needed to recover a valid management workflow. |
| `Cluster.ExcludeNode` | `cluster.go` | Pure edit + shared staging; operation-specific execution remains pending | Remove a proven drained member without bypassing quorum. |
| `Cluster.ForceRetireNode` | `cluster.go` | Pure edit + shared staging; operation-specific execution remains pending | Explicit degraded-cluster path; retain audit, quorum and cleanup obligations. |
| `Cluster.SetGroupHosting` | `cluster.go` | Pure edit + shared staging; operation-specific execution remains pending | Keep explicit/all-eligible host changes; core relocation without labels. |
| `Cluster.CreateGroup` | `cluster.go` | Pure edit + shared staging; operation-specific execution remains pending | Create on fixed simple hosting/voter presets, including unavailable hosts. |
| `Cluster.DeleteGroup` | `cluster.go` | Pure edit + shared staging; operation-specific execution remains pending | Logical deletion through _meta plus exact detached cleanup debt. |
| `Cluster.ChangeReplica` | `cluster.go` | Pure edit + shared staging; operation-specific execution remains pending | Manual voter/learner role changes through guarded membership. |
| `Cluster.ReseatReplica` | `cluster.go` | Task 5: atomic approved repair/rebase and fresh-read CAS scenarios | Atomic approved fresh-ID repair; protect old owner and independent staged intent. |
| `Node.StartLoadGroup` | `node.go` | No method-specific implementation | Exact local start/hold clearance; reuse shared startup phases. |
| `Node.StopLoadGroup` | `node.go` | No method-specific implementation | Exact hold distinct from failure or retirement; must not be undone by repair. |
| `Node.SetSchedulingEnabled` | `node.go` | No method-specific implementation | Scheduler enable/disable as a control input and progress premise. |

### Primitive

| Public API | Source | Current evidence | Initial treatment / reason |
| --- | --- | --- | --- |
| `Cluster.ReadBarrier` | `cluster.go` | Task 5: shared quorum-confirmed read and local-apply checks | One shared quorum-confirmation plus local-apply primitive. |
| `Cluster.ReadTopology` | `cluster.go` | Task 5: confirmed-view projection with exact read ticket | Reuse confirmed-read primitive and local topology snapshot. |
| `Cluster.Plan` | `cluster.go` | Task 4 pure helpers; dispatch not implemented | Reuse pure makePlan for a staged version; no separate RPC machine. |
| `Cluster.PlanFor` | `cluster.go` | Task 4 pure helpers; dispatch not implemented | Reuse pure makePlan for a candidate; preview cannot authorize effects. |
| `Cluster.TransferLeader` | `cluster.go` | No method-specific implementation | Reuse transfer request/proof phases; preserve manual transfer grant versus approved-plan authority. |
| `Node.Start` | `node.go` | No method-specific implementation | Shared Starting → replay/authorization/session → Ready phases. |
| `Node.WaitReady` | `node.go` | No method-specific implementation | Observe shared readiness; cancellation stops waiting only. |
| `Node.Stop` | `node.go` | No method-specific implementation | Join shared teardown; timeout/failed release retains ownership. |
| `Node.RequestStop` | `node.go` | No method-specific implementation | Begin shared teardown separately from successful join. |
| `Node.LoadSnapshots` | `node.go` | No method-specific implementation | Abstract complete catch-up; omit service accessor, chunks and transfer tuning. |
| `Node.ReceiveLoadRaft` | `node.go` | No method-specific implementation | Abstract native consensus/network boundary; no per-Raft-message implementation. |
| `Node.ReadLoadReplica` | `node.go` | No method-specific implementation | Exact native observation/capture used by planner and scheduler. |
| `Node.ApplyLoadAction` | `node.go` | No method-specific implementation | Core message receiver and exact authorized-effect consumption. |
| `Node.InstallLoadFence` | `node.go` | No method-specific implementation | Core durable resource fence and acknowledgement. |
| `Node.ReportLoadDelivery` | `node.go` | No method-specific implementation | Identity/delivery-bound feedback; no transport counters or per-packet model. |

### Observe

| Public API | Source | Current evidence | Initial treatment / reason |
| --- | --- | --- | --- |
| `Cluster.ReadProgress` | `cluster.go` | No method-specific implementation | Derived progress output; no pages, cursor, attachment or formatting workflow. |
| `Cluster.ReadAuthority` | `cluster.go` | No method-specific implementation | Derived exact revocation obligations; use confirmed-read primitive where an actor needs freshness. |
| `Cluster.WaitRevoked` | `cluster.go` | No method-specific implementation | Observe revocation proof; no polling loop or extra state machine. |
| `Cluster.Rules` | `cluster.go` | No method-specific implementation | Fixed replicated capacity/configuration parameters. |
| `Cluster.IsLeader` | `cluster.go` | No method-specific implementation | Stale leadership observation, never quorum proof. |
| `Cluster.Status` | `cluster.go` | No method-specific implementation | Compact derived operator summary of topology, group state and blockers. |
| `Cluster.HealthObservation` | `cluster.go` | No method-specific implementation | Current modeled availability for repair; performance/stability metric evidence is fixed good. |
| `Cluster.ParkedJoins` | `cluster.go` | No method-specific implementation | Derived admission blockers for trace diagnosis. |
| `Cluster.Latest` | `cluster.go` | No method-specific implementation | Existing per-node staged view; no separate getter action. |
| `Cluster.Target` | `cluster.go` | No method-specific implementation | Existing per-node approved view; no separate getter action. |
| `Cluster.Operation` | `cluster.go` | No method-specific implementation | Derived immutable attempt/block progress. |
| `Cluster.WaitOperation` | `cluster.go` | No method-specific implementation | Observe terminal state; caller cancellation cannot undo work. |
| `Node.Identity` | `node.go` | No method-specific implementation | Existing durable identity plus incarnation fields. |
| `Node.Status` | `node.go` | Task 1 phase projection | Derived process phase/readiness; phase projection already exercised. |
| `Node.LoadGroup` | `node.go` | No method-specific implementation | Derived exact published handle identity; no accessor model. |
| `Node.LoadGroupIDs` | `node.go` | No method-specific implementation | Derived set of published groups. |
| `Node.ParkedLoads` | `node.go` | No method-specific implementation | Derived exact parked/held-owner reasons. |
| `Node.OwnSessionLive` | `node.go` | No method-specific implementation | Observe minimum internal session state needed by readiness. |
| `Node.SchedulingStatus` | `node.go` | No method-specific implementation | Derived scheduler phase/reason and affected group. |

### Omit

| Public API | Source | Current evidence | Initial treatment / reason |
| --- | --- | --- | --- |
| `Cluster.Metrics` | `cluster.go` | No method-specific implementation | No topology or authority effect; metrics API is not a model feature. |
| `Cluster.MetricsHistory` | `cluster.go` | No method-specific implementation | Historical telemetry/pagination adds no core protocol transition. |
| `Cluster.Get` | `cluster.go` | No method-specific implementation | Omit history lookup API; keep internal history/proof needed for retries and cleanup. |
| `Cluster.History` | `cluster.go` | No method-specific implementation | Omit history pagination/export; protected evidence remains core state. |
| `Cluster.Watch` | `cluster.go` | No method-specific implementation | Omit subscriptions/coalescing/backpressure; stale observations remain modeled. |
| `Node.Watch` | `node.go` | No method-specific implementation | Omit event-stream wrapper; process observations remain available. |
| `Node.Cluster` | `node.go` | No method-specific implementation | Pure handle accessor with no independent lifetime or transition. |
| `NewNode` | `node.go` | No method-specific implementation | Configuration/initial-state construction; no separate constructor/DI/I/O test machine. |

### Deferred

| Public API | Source | Current evidence | Initial treatment / reason |
| --- | --- | --- | --- |
| `Cluster.ReadLockHolder` | `cluster.go` | No method-specific implementation | Application lock semantics deferred, including linearizable lock queries. |
| `Cluster.SetPlacement` | `cluster.go` | Task 4 pure helpers; dispatch not implemented; extension work, not v1 obligation | Live placement-rule changes deferred; numeric capacity/voter presets stay fixed. |
| `Cluster.SetGroupPlacement` | `cluster.go` | Task 4 pure helpers; dispatch not implemented; extension work, not v1 obligation | Selectors, node-type policy and live bound changes deferred. |
| `Cluster.SetGroupVoterConfig` | `cluster.go` | Task 4 pure helpers; dispatch not implemented; extension work, not v1 obligation | Live policy tuning deferred; fixed target/surge presets and guarded role transitions retained. |
| `Cluster.SetReplicaPinnedLearner` | `cluster.go` | Task 4 pure helpers; dispatch not implemented; extension work, not v1 obligation | Standalone pin/unpin API deferred; existing/role-derived pins still constrain promotion. |
| `Cluster.EnsureGroup` | `cluster.go` | No method-specific implementation | Compound convenience/retry-recognition workflow; use CreateGroup/Stage/Approve in v1. |
| `Cluster.RollbackTo` | `cluster.go` | Task 4 pure helpers; dispatch not implemented; extension work, not v1 obligation | Historical desired-topology workflow deferred; no identity reuse remains a core invariant. |
| `Cluster.AcquireLock` | `cluster.go` | No method-specific implementation | Application lock ownership is outside the management protocol focus. |
| `Cluster.LockHolder` | `cluster.go` | No method-specific implementation | Application lock reads deferred with acquisition. |
| `Node.Recovery` | `node.go` | No method-specific implementation | Offline recovery handle/workflow deferred; ordinary retained restart remains. |
| `Node.ActivateSession` | `node.go` | No method-specific implementation | Manual API deferred; internal exact-process activation retained. |
| `Node.KeepAlive` | `node.go` | No method-specific implementation | Manual API/retry behavior deferred; internal renewal/expiry retained abstractly. |
| `Node.DeactivateSession` | `node.go` | No method-specific implementation | Manual session-pausing/lock-release workflow deferred. |
| `Node.ServeOverride` | `node.go` | No method-specific implementation | Deferred with serving override controls; v1 uses normal/default serving policy. |
| `Node.SetServeOverride` | `node.go` | No method-specific implementation | Advanced local override deferred; core authority never bypassed. |
| `Recovery.Inspect` | `node_recovery.go` | No method-specific implementation | Offline Inspect/accept/archive/import/cancel/history workflow deferred as a unit. |
| `Recovery.Approve` | `node_recovery.go` | No method-specific implementation | Offline Inspect/accept/archive/import/cancel/history workflow deferred as a unit. |
| `Recovery.Run` | `node_recovery.go` | No method-specific implementation | Offline Inspect/accept/archive/import/cancel/history workflow deferred as a unit. |
| `Recovery.Cancel` | `node_recovery.go` | No method-specific implementation | Offline Inspect/accept/archive/import/cancel/history workflow deferred as a unit. |
| `Recovery.Status` | `node_recovery.go` | No method-specific implementation | Offline Inspect/accept/archive/import/cancel/history workflow deferred as a unit. |
| `Recovery.List` | `node_recovery.go` | No method-specific implementation | Offline Inspect/accept/archive/import/cancel/history workflow deferred as a unit. |
| `NewRecovery` | `node_recovery.go` | No method-specific implementation | Offline recovery constructor deferred. |
| `RecoveryStartOptions` | `node_recovery.go` | No method-specific implementation | Fresh-lineage/reset startup adaptation deferred. |

## State and visibility that must survive simplification

Keep the separation between staged/approved/proven/native/local-observed state, exact member/replica/incarnation, immutable operation attempts, resource fences, partial receipts and per-owner cleanup milestones. Status convenience does not justify dropping these records. Actor decisions still use their own applied/cached views or a modeled fresh-read result; a human trace projection is not permission for an omniscient controller.

Minimum internal sessions remain because `node_lifecycle.go` requires the current identity to be admitted, recovered and SessionLive before readiness. No application lock map or public session API state machine is required for that property.

The default voter scheduler consumes internal metrics in `scheduling/voters.go`/`internal/votercheck`, but the user explicitly chose **metrics always good for this iteration**. Fix replication lag/apply backlog within limits, metric freshness/current collector valid, progress healthy, standby stability satisfied and cooldowns clear. Do not generate lag, no-progress, age/gap or stability-window failures, or implement elapsed-window counters. Crashes/partitions and native quorum remain separate actual conditions. Unavailable-replica repair eligibility is abstracted without detection-window timing; a newly joined replica still needs its successful native catch-up milestone. Decisions captured before a real topology/identity/leadership change must still be revalidated. This does not remove controller deadlock/livelock checks.

Keep sampled and bounded/exhaustive results distinct, explicit unexpected-stuck checks, fair-progress assumptions and negative controls. Removing diagnostics is safe; removing safety evidence would change the protocol under test.

## Existing evidence and migration into the smaller model

Tasks 1–4 and commits through `fa2e8ad` remain completed historical work. The foundation has four tests; smoke3 has 16 core scenarios and the broader topologyChecks suite has 40 tests. Some of the latter cover labels, domain placement, pin setters, policy tuning and rollback, which are now outside v1. Passing them is not evidence that their full workflows were implemented.

Task 5 now restricts invocation/configuration domains and separates extension-only tests from initial acceptance. Original broader helpers and topologyChecks remain available separately. Later task machinery must preserve these scope checks; no deferred path may re-enter through generic Stage or defaults.

The source baseline remains `e8e85dbd7879b1c858252cd2a6126a36920f8a8e`. Exact behavior and earlier validation evidence are recorded in [the execution log](../../docs/progress/20261027-quint-cluster-management.md). Full design history before this scope revision is retained in Git; the [target](../../docs/brainstorms/20261027-quint-cluster-management-target.md) and [plan](../../docs/plans/20261027-quint-cluster-management.md) now define initial acceptance.

## Task 5 validation checkpoint

Foundation: 7 tests. smoke3: 43 asynchronous scenarios plus 60 retained pure/scope checks. Reports: `test-reports/quint/20261027T154905846454Z-d5708b3f`, `20261027T154929137587Z-91c5bd39`; 50 fault samples at depth 60 passed in `20261027T155031955190Z-bb318403`. These are sampled/scenario checks, not exhaustive or Go-conformance proof. A separate healthy protocol-only approval run reached approval and its reply in all 50 samples (seed 1, depth 160).

The full step relation retains retries, loss and partitions. The healthy subset excludes retry flooding, which otherwise can consume finite proposal/message bounds before ordinary work finishes. Initial failed-operation fixtures cover public retry routing; runtime execution failures and consumed-receipt suffix validation are Task 6 work. Source-faithful failing library scenarios are tracked in [the findings report](../../docs/reports/20261027-quint-cluster-management.md).
