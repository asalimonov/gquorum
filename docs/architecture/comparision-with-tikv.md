# Cluster management: gquorum compared with TiKV and PD

Status: source-verified comparison and design recommendations, 2026-10-12.
Recommendations below are proposed work, not implemented contracts.

gquorum and TiKV share the essential normal-operation safety boundary: a
management leader can choose and dispatch work, but changing an established
group's voting membership requires that group's Raft commitment. Management
consensus cannot supply missing votes to an application group. Their management
architectures differ substantially, however. gquorum replicates approved plans
and execution progress through `_meta`; PD usually reconciles independently
reported Region state toward durable policy, without an etcd commit for each
operator step.

This comparison covers cluster formation, authority, admission, placement,
scheduling, topology execution, failure handling, decommissioning, recovery,
upgrades, observability and administration. Learners are one part of that scope.
TiKV's database-specific services are identified separately where they have no
direct equivalent in a general-purpose Raft library.

## Scope and evidence

| Code inspected | Revision and qualification |
| --- | --- |
| gquorum | Working tree based on `84ffa43`, including the current stable-learner implementation. Existing staged and unstaged changes were inspected as implementation, rather than treating the base commit as the complete feature. |
| TiKV | `../../neria/tikv`, commit `137cbb2593decafebf99905b4d570869230565c2`. Inspected source files match that commit; the checkout also contains untracked documentation. |
| TiKV's raft-rs dependency | `/tmp/tikv-quorum-raft-rs-20261012`, commit `1fd05e000fb094015507c5c985849d370100fb72`, matching TiKV's `Cargo.lock`. |
| Placement Driver (PD) | `/tmp/tikv-quorum-pd-20261012`, commit `34ce7eca7eaf9b4fd06061984a1cc35884c49ee4`. This is the independent reference revision used by the supplied report, **not a PD revision pinned by TiKV or evidence of a deployed version**. |

The starting point was the supplied TiKV report,
`../../tikv/docs/reports/raft-quorums-and-cluster-topology.md`.
Its relevant claims were checked against implementation, including the separate
PD and raft-rs checkouts. TiKV-side claims below link to the exact inspected
revision. PD-side claims link to PD, because TiKV's client code alone cannot
establish how PD persists policy or runs scheduling. Both raftstore engines are
used as evidence where relevant; engine-specific optimizations are qualified.

**Parity** means a comparable implemented mechanism for the stated property,
not equal production maturity. **Difference** means a deliberate architectural
choice with a tradeoff. **Gap** means a missing or materially narrower capability.
**Needs proof** identifies a guarantee that this source review does not establish.
Negative findings refer to the public APIs and management paths inspected here;
application code can add capabilities outside the library.

## 1. Map the components before comparing guarantees

| Responsibility | TiKV / PD | gquorum | Assessment and consequence |
| --- | --- | --- | --- |
| Durable management authority | PD stores durable cluster/store metadata and placement policy in etcd. PD service leadership uses a leased leader key; the PD leader and etcd Raft leader are distinct roles. [PD leadership][p-leadership], [metadata writes][p-store], [etcd writes][p-etcd]. | `_meta` combines the replicated metadata state machine with the leader that coordinates management. [Meta state machine][g-sm], [operator coordinator][g-planning]. | **Parity of purpose; different composition.** `_meta` corresponds to durable PD metadata plus its coordination role, not to an individual TiKV Region. |
| Application consensus | Each Region has its own peers, leader, log, configuration and epoch. PD commands are delivered to that Region's leader. [Dispatch][t-pd], [configuration proposal][t-conf]. | Each load group has its own Raft instance and application state machine. `_meta` authorizes management but is not on every application write path. [Engine][g-engine], [load API][g-load]. | **Parity.** Separate management and data quorums; neither system has one quorum composed of every data replica in the cluster. |
| Which machines replicate management state | A TiKV store registers with PD; it does not thereby become a member of PD's etcd group. PD member removal and TiKV store removal are separate operations. [Store bootstrap][t-bootstrap], [PD members][p-members], [store lifecycle][p-offline]. | Every admitted node hosts `_meta`, including nodes that host no load groups. Extra `_meta` replicas can be learners. [Admission][g-join], [document validation][g-validate]. | **Difference retained by requirement.** A small voter set limits quorum size, but all-node `_meta` replication still costs network, disk, catch-up and membership work as the cluster grows. |
| Data placement across machines | Regions have selected peers. Placement rules specify counts, roles and eligible locations; stores need not host every Region. [Rules][p-rules], [checker][p-checker]. | Policy-managed load groups default to all eligible hosts; explicit host sets are also supported. Voter count is independent of hosted replica count. Legacy groups retain their previous behavior. [Hosting][g-hosting], [voter policy][g-voter-policy]. | **Partial parity.** Explicit subsets exist. Automatic partition assignment and movement based on capacity/load do not follow merely from introducing learners. All-host learners still contain full application state. |
| Control-plane failure isolation | Region replication and elections are separate from PD availability. Operations needing PD metadata, scheduling or timestamps can still be affected. [Region command path][t-command], [PD client/worker][t-pd], [TSO][p-tso]. | Existing load consensus is separate from `_meta`; admission, new approvals and coordinated repair need `_meta`. Shared Node lifecycle, session fencing and resources can also affect service. [Engine][g-engine], [session lifecycle][g-session], [scheduler][g-scheduler]. | **Partial parity.** Do not promise that every application operation survives management-plane loss. Define this at the API and lifecycle level, not just from separate Raft logs. |

The intended relationship is:

```mermaid
flowchart LR
    subgraph TiKV_and_PD[TiKV and PD]
        PE[etcd quorum: durable policy and registry] --> PD[PD scheduler]
        RH[Store and Region reports] --> PD
        PD -->|request| RL[Region leader]
        RL -->|configuration entry| RQ[Region quorum]
        RQ --> RH
    end
    subgraph gquorum
        MQ[_meta quorum: intent, approval, progress] --> OP[Operator controller]
        OB[Local and remote observations] --> OP
        OP -->|exact approved action| GL[Load group leader]
        GL -->|configuration entry| GQ[Load group quorum]
        GQ --> OB
        OP -->|record progress| MQ
    end
```

Neither arrow from a scheduler to a group leader is permission to bypass that
group's quorum. Local host actions, such as opening or retiring storage, have
different completion conditions from Raft membership changes.

## 2. Consensus, authority and acknowledgement boundaries

| Concern | TiKV / PD implementation | gquorum implementation | Parity, gap or design implication |
| --- | --- | --- | --- |
| Durable management changes | Store metadata is saved before publishing the updated cache; placement patches are saved before publishing the rule set. The etcd backend waits for transaction commit. [Store][p-store], [rules][p-rule-save], [etcd][p-etcd]. | Staging, approving a plan, recording transitions and activating topology are `_meta` proposals. `Latest`, `Target` and `Active` have different meanings. [Documents][g-documents], [approval][g-operator-state], [activation][g-planning]. | **Parity:** durable shared authority requires management consensus. gquorum additionally offers an explicit staged-approval workflow. PD does not expose an equivalent cluster-wide three-document lifecycle in these paths. |
| Membership commitment | A PD heartbeat response becomes a Region admin request, then a Raft configuration proposal. Applying it updates Region membership/epoch. [PD receiver][t-pd], [proposal/apply][t-conf], [epoch update][t-apply]. | An approved action reaches the affected group's leader; membership coordination submits a configuration entry and waits for application. `_meta` membership changes use `_meta`'s own Raft group. [Actions][g-actions], [membership][g-membership], [loop][g-loop]. | **Parity.** A leader-only API identifies who may submit the change. It does not make the change unilateral. An existing majority must authorize reducing the voter set. |
| What success acknowledges | PD's normal scheduling receiver uses `Callback::None`. Operator steps finish when later Region observations satisfy exact peer/role/leader predicates; adding a learner also checks it is no longer pending. [Receiver][t-pd], [step predicates][p-steps]. | Effects and exact host acknowledgements are checked; progress then commits in `_meta`. `WaitOperator` observes terminal status locally; activation is separately driven. Application proposals return after quorum commitment and application on the submitting local replica. [Dispatch][g-dispatch], [planning][g-planning], [loop][g-loop]. | **Difference.** Dispatch, committed membership, locally applied state, operator completion and cluster convergence must remain distinct. Neither normal membership path waits for every replica to apply the entry. |
| Consensus versus physical persistence | TiKV persists the Raft data required for dependent follower responses before sending them. Its asynchronous integration can apply committed entries before the leader's own log persistence catches up. [Writer][t-writer], [Ready handling][t-ready], [raft-rs progress][r-raft]. | The engine persists Ready data before releasing that batch's messages and applying its entries. [Loop][g-loop], [storage contract][g-storage]. | **Parity in required durable consensus; different I/O pipeline.** Do not infer that TiKV always fsyncs the leader before every commit/apply, or weaken gquorum ordering by copying only part of that optimization. |
| Stale management commands | Region leaders validate store ID, peer ID, term and Region epoch. Membership increments `conf_ver`; range changes use the other epoch dimension. PD also cancels stale operators. [Validation][t-command], [apply][t-apply], [controller][p-controller]. | Receivers verify the exact operator/version/attempt/step/action/replica tuple against locally applied approval and recheck after waits. Native identity and membership checks also apply. [Authorization][g-authorization], [actions][g-actions]. | **Partial parity; needs proof.** A stale local `_meta` view is not a linearizable revocation fence. Unlike a Region epoch checked in the affected group, the approval lives in a different log. Define and test whether an already authorized, delayed action may finish after supersession. This review does not establish an exploitable race or instantaneous revocation. |
| Leadership transfer | Advisory request with target readiness checks, followed by Raft election. Certain in-memory-lock transfers add a replicated marker. PD observes the resulting leader. [Transfer][t-transfer], [steps][p-steps]. | Engine transfer submission returns before election completes; managed load actions wait to observe the target leader. The direct meta transfer path acknowledges initiation. [Actions][g-actions], [meta mutations][g-mutations], [loop][g-loop]. | **Partial parity.** Standardize public accepted-versus-completed semantics. Transfer is not a quorum-free way to appoint a leader; the successor needs election authority. TiKV's lock-specific marker is not a generic requirement for all gquorum applications. |
| Consistent reads | TiKV has guarded lease reads and ReadIndex paths. A valid lease can answer some ReadIndex requests without a fresh heartbeat round; otherwise safe reads require the corresponding leadership/apply checks. [Lease path][t-read], [raft-rs][r-raft]. | Status, topology documents, lock-holder and metrics reads are local observations. No public read barrier exists on `Cluster` or `load.Group`. [Cluster API][g-cluster], [load API][g-load], [documents][g-documents]. | **Gap.** Provide an explicit authoritative read/barrier contract if applications need it. PD's live Region map is also an observation; TiKV's data-read guarantees must not be attributed automatically to all PD status endpoints. |
| Discovery and request routing | PD provides Region/leader lookup; TiKV validates the destination and returns errors such as `NotLeader` and epoch mismatch. [Lookup][t-routing], [validation][t-command]. | Ordinary proposals on a ready local follower/learner forward through Raft. When no local group exists, the application must route the request; there is no library key-to-group router. [Load API][g-load], [forwarding][g-forwarding], [Raft contract][g-raft-doc]. | **Partial parity.** Replica forwarding does not solve discovery or routing for partitioned deployments. Define cache invalidation, exact group identity and retry handling at the application/client boundary. |
| Transactions across groups | PD policy persistence and multiple Region reconfigurations are separate steps. Split/merge have specific protocols, not a generic atomic cluster-wide topology transaction. [PD dispatch][p-controller], [merge apply][t-merge]. | `_meta` approval, load configuration commits and local effects are separate. No transaction atomically commits them together. [Controller][g-controller], [actions][g-actions]. | **Parity of limitation.** Recovery must tolerate partial completion. TiKV's distributed transaction implementation is a separate database layer, not something `_meta` replication supplies. |

For example, with management quorum available but only one surviving voter of a
three-voter application group, both systems can record policy or inspect state.
Neither normal management path can promote its way around the missing current
quorum. TiKV's explicitly unsafe recovery is a separate exception discussed below.

## 3. Formation, admission, identity and lifecycle

| Concern | TiKV / PD implementation | gquorum implementation | Parity, gap or design implication |
| --- | --- | --- | --- |
| Bootstrap and competing founders | TiKV persists cluster/store identity and prepared first-Region state; bootstrap is re-entrant. PD installs initial cluster/store/Region metadata through an etcd transaction guarded by absence of the cluster key. [TiKV bootstrap][t-bootstrap], [PD bootstrap][p-bootstrap]. | Founding configuration, durable pins and retained state are checked before startup. The first topology and approved founding plan are installed through `_meta`; established identities are not silently refounded. [Bootstrap][g-bootstrap], [recovery architecture][g-runtime]. | **Comparable safety goals, different protocols.** Preserve explicit founding-versus-restart evidence. Fewer than three actual voters means reduced fault tolerance even if desired replication is three. |
| Adding a host | Store bootstrap obtains a PD-allocated ID and durable identity. Registering a store and assigning Region peers are distinct. [Bootstrap][t-bootstrap], [ID allocation][p-ids], [store registry][p-store]. | Admission binds node/member/process identity and approved topology; all admitted nodes join `_meta`. A current-term committed/applied barrier precedes admission classification. [Join][g-join], [admission records][g-admission]. | **Difference.** Data-host expansion also expands `_meta` replication and requires its membership workflow. Evaluate this cost separately from voter quorum size. |
| Identity reuse and obsolete replicas | Store/cluster identity is persisted. Peer IDs and Region epochs fence stale commands; durable tombstones and newer peer identities distinguish obsolete peers. [Bootstrap][t-bootstrap], [validation][t-command], [peer lifecycle][t-life]. | Stable node names, fresh process `InstanceID`, monotonic member/replica IDs, retained pins and retirement evidence have separate roles. Explicit readmission is fenced and verifies replacement identity. [Join][g-join], [managed readmission][g-readmission], [runtime][g-runtime]. | **Substantial parity of intent.** Exact identity checks are core safety, not optional policy. Do not equate a name/address with ownership of retained data. |
| Liveness versus authority | Store/Region heartbeats report health, down/pending peers and load. Suspicion drives scheduling; Region quorum decides resulting membership. PD's own leadership lease is a separate mechanism. [Reports][t-pd], [checker][p-checker], [leadership][p-leadership]. | Probes and metrics are observations. Session grants/revocations and lock acquisition commit in `_meta`; `KeepAlive` refreshes leader-local deadlines. Expiry probes the exact process before proposing revocation. [RPC][g-rpc], [session][g-session], [expiry][g-expiry]. | **Difference requiring a clear contract.** A session-renewal response is not proof of a fresh quorum or external fencing. Do not equate gquorum's process session with PD leadership or TiKV's transaction locks. |
| Restart and recovery | TiKV checks retained store identity, uses Raft state and peer lifecycle records, and refuses mismatched clusters. [Bootstrap][t-bootstrap], [lifecycle][t-life]. | Snapshot plus committed tail reconstruct the application; pins, assignment and ledgers must agree. Missing/foreign/contradictory evidence is parked rather than overwritten. [Storage][g-storage], [runtime][g-runtime]. | **Comparable mechanism, not a maturity claim.** Custom state machines and stores remain part of gquorum's recovery correctness boundary. |
| Drain and decommission | PD first marks a store offline/removing. Region scheduling evacuates it; observed emptiness permits burial/tombstoning. The offline API does not mean evacuation is complete. [Offline transition and checks][p-offline], [burial checks][p-bury], [rule checker][p-checker]. | Drain changes load eligibility while retaining `_meta`. Exclusion/removal follows an approved plan, membership changes and exact retirement actions. [Mutations][g-mutations], [planner][g-planner], [retirement][g-retirement]. | **Partial parity.** Expose separate requested, evacuated, consensus-removed, fenced and physically-cleaned states. A single success flag cannot describe all of them. |
| Offline removed host and cleanup | A Region can remove an unreachable peer through the remaining quorum. The removed peer need not acknowledge removal. Tombstone delivery and destruction happen later; v2 keeps removal records and retries cleanup. [Lifecycle][t-life], [remove-step completion][p-steps]. | Membership removal also does not require the victim's vote. However, planned host stop/cleanup steps require exact acknowledgements; unavailable required hosts can keep the operator from converging. [Controller][g-controller], [dispatch][g-dispatch], [retirement][g-retirement]. | **Manageability gap/tradeoff.** Separate completed consensus topology from outstanding physical cleanup while preserving durable obligations and fencing. Never turn absence of an acknowledgement into evidence of deletion. |
| Deleting a whole application group | Region removal through merge preserves data in a target and follows its specific protocol. It is not equivalent to arbitrary destruction of a user-owned Raft group. [Merge][t-merge], [lifecycle][t-life]. | Approved whole-group deletion stops exact hosts and retires storage without first obtaining a configuration change from the dying load group; required host acknowledgements still apply. [Controller][g-controller], [dispatch][g-dispatch]. | **Intentional semantic difference.** Authorization comes from `_meta`; data-destruction policy belongs to the application. Do not describe this as a normal membership change bypassing Raft. |

## 4. Scheduling, placement, capacity and topology evolution

| Concern | TiKV / PD implementation | gquorum implementation | Parity, gap or design implication |
| --- | --- | --- | --- |
| Desired replication and insufficient capacity | PD rules express desired peer counts; the checker repairs missing peers using eligible stores. Policy and observed compliance are separate. [Rules][p-rules], [checker][p-checker]. | Per-group voter targets are replicated. Fresh defaults target three; shortfalls do not rewrite the target. All-eligible hosting rejects replica-capacity overflow. `Limits` defaults to three nodes and five total replicas **per load group**. [Policy][g-voter-policy], [hosting][g-hosting], [limits][g-document]. | **Partial parity.** Desired three does not mean three exist. Count limits prevent oversubscription of declarations but do not measure disk or CPU capacity. |
| Failure-domain placement | PD rules carry label constraints, ordered location labels and enforced isolation levels. The checker can move peers to improve location; filters protect placement. [Rules][p-rules], [checker][p-checker], [filters][p-filters]. | Nodes have `Region` and `Labels`, but built-in placement uses allowed node types and hosting sets. Founding voter selection is name-ordered; the voter scheduler has no equivalent location rule. [Model][g-document], [hosting][g-hosting], [scheduler policy][g-voters]. | **Gap with direct availability impact.** Three voters can all land in one failure domain. Add declared failure-domain constraints, validation and explainable unsatisfiable results for both `_meta` and load groups. Labels alone provide no protection. |
| Capacity and healthy-group balancing | PD includes Region and leader balancers, load statistics, low-space filtering, snapshot/pending-peer limits and operator influence when choosing moves. [Region balancer][p-balance], [leader balancer][p-balance-leader], [filters][p-filters], [store statistics][t-stats]. | The built-in policy repairs supported degradation and voter policy; it preserves healthy placements. Metrics expose Raft/proposal/WAL/apply state, not a full store resource model. [Policy][g-voters], [repair policy][g-restore], [metrics DTOs][g-metrics]. | **Functional gap.** Add capacity/load inputs and constrained balancing if automatic distribution is required. Custom scheduling hooks are extension points, not an implemented PD-equivalent balancer. |
| Operator concurrency and unrelated intent | PD tracks operators by Region, with waiting queues, priorities and scheduling limits; independent Regions can progress concurrently. [Controller][p-controller], [limits][p-limits], [balancer limit][p-balance]. | A current approved topology execution advances one exact step at a time. Automatic actions require `Latest = Target = Active` and no conflicting execution; unrelated staged intent can block repair. [Operator state][g-operator-state], [controller][g-controller], [driver][g-scheduler]. | **Logical and scaling gap.** Preserve approval isolation while moving toward per-group execution scopes and versions. Automatically approving unrelated staged edits would be an incorrect shortcut. |
| Management progress persistence | In the inspected ordinary path, PD operators and their step cursor live in controller memory; commands and observed completion do not require a replicated per-step journal. Durable policy plus current Region state enable reconciliation. [Controller fields/dispatch][p-controller], [Region cache][p-regions]. | Immutable plans, attempts, transitions and completion evidence are durable `_meta` state. Recovery can resume the exact approved execution. [Operator state][g-operator-state], [controller][g-controller]. | **Difference, with both benefits and costs.** Preserve auditability and exact authorization. Measure extra `_meta` writes, global contention and recovery delays; more journal commits do not establish greater overall reliability. |
| Metrics transport and temporal evidence | TiKV pushes Store and Region heartbeats containing peer state, size and load. PD updates caches and asynchronously persists/synchronizes Region observations; local Region storage is the configured default, separate from etcd KV. [Statistics][t-stats], [cache][p-regions], [storage selection][p-region-storage], [default][p-region-default]. | The meta leader gathers bounded parallel samples; source generations, terms, exact identities, ages and continuity determine evidence. Histories remain in memory; role-switch records are replicated. [Collector][g-collector], [evidence][g-evidence], [switch records][g-switches]. | **Parity in separating observations from authority; different collection strategy.** Do not replicate every sample just because `_meta` exists. At scale, assess pull fan-out, freshness under overload and whether node-batched or pushed reports are needed. |
| Failure detection and stabilization | PD combines down-peer reports, store state/down time, pending-peer readiness and scheduling filters. Startup preparation checks reported Region coverage, with a timeout escape. [Checker][p-checker], [filters][p-filters], [preparation][p-prepare]. | Defaults require 30 seconds of failure/replication lag and 60 seconds of standby readiness; apply backlog blocks promotion. Freshness, hysteresis and configurable cooldowns suppress churn. A new observer rebuilds evidence and rearms cooldowns from durable switch records. [Policy][g-voter-policy], [evidence][g-evidence], [checks][g-voter-check]. | **Comparable objectives, not identical algorithms or thresholds.** The current defaults are not TiKV-derived operating guarantees. Validate failure/recovery latency and repeated failover behavior under realistic workload and clock/scheduling delays. |
| Learners and replacement sequencing | PD supports learner-first steps and joint configuration changes when feature support allows them. Joint commitment requires both old and new majorities. [Steps][p-steps], [feature-gated dispatch][p-controller], [TiKV conf change][t-conf], [joint quorum][r-joint]. | Stable learners, pinned learners and guarded promotion are implemented. Membership accepts only single-change Auto transitions. Optional surge can temporarily increase voter count; each intermediate configuration must pass safety checks. [Policy][g-voter-policy], [membership guards][g-safety], [voter planning][g-voter-plan]. | **Partial parity; joint-consensus capability gap.** Sequential replacement is not inherently unsafe, but its temporary quorum/fault-tolerance states differ. Joint support requires a full planning, recovery and observation design, not merely exposing a protobuf option. |
| Leadership and transition recovery | TiKV maintains pending-conf-change restrictions and exact Region state; PD advances or discards operators based on fresh observations and epochs. [Conf change][t-conf], [controller][p-controller]. | The controller revalidates immutable scope, observes already-completed effects and retries. Role transitions are recorded only after completion evidence; leadership transfer may require rerouting. [Controller][g-controller], [actions][g-actions], [operator state][g-operator-state]. | **Partial parity.** Fault coverage must include crash after group commit but before `_meta` progress commit, leader changes during transfer, stale effect delivery and concurrent admission. Source structure alone cannot prove every recovery interleaving. |
| Split, merge and automatic partitioning | TiKV commits split on the parent Region and coordinates merge through source preparation, target commit and catch-up/rollback handling. Region epochs protect routing transitions. [Split][t-split], [merge][t-merge], [validation][t-command]. | Explicit groups and host sets exist. There is no key-range catalog, split/merge protocol, atomic data migration or built-in routing layer. [Group model][g-document], [load API][g-load], [hosting][g-hosting]. | **Functional gap if partition management is a library goal.** Define an application integration contract for data movement and routing before adding automatic partition scheduling. Merely creating three groups does not partition their business data. |
| Domain-aware durability modes and witnesses | The inspected v1 code includes DR auto-sync/group-commit wiring and witness handling. These are deployment/engine-specific capabilities beyond ordinary majority voting. [Replication mode][t-replication-mode], [witness apply][t-witness]. | Ordinary full-state voters and learners; no witness or additional cross-domain commit mode in the public topology model. [Model][g-document], [engine][g-engine]. | **Optional capability gap.** First implement correct placement. Do not confuse a witness with a learner, or claim ordinary majority means a durable copy in each zone. |

## 5. Operations, upgrades, recovery and service boundaries

| Concern | TiKV / PD implementation | gquorum implementation | Parity, gap or design implication |
| --- | --- | --- | --- |
| Throttling maintenance work | PD budgets operators and per-store work, accounting for snapshot traffic, pending peers and capacity. TiKV reports the underlying store measurements. [Limits][p-limits], [filters][p-filters], [stats][t-stats]. | Proposal, transport and observation work is bounded; scheduler calls are serialized and cooldowns limit switching. There is no equivalent general per-store migration/snapshot scheduling budget. [Engine][g-engine], [driver][g-scheduler], [collector][g-collector]. | **Gap.** Cooldowns and bounded queues do not substitute for admission budgets across many recovering groups. Introduce priorities and resource reservations before increasing management concurrency. |
| Mixed versions and rollout | PD checks store compatibility, tracks cluster version and gates features such as ConfChangeV2; TiKV learns cluster version through PD responses. [Store version][p-version], [features][p-features], [TiKV client][t-client]. | Persisted/wire compatibility fixtures exist, but the inspected admission and scheduling interfaces do not negotiate per-node feature support or a replicated minimum capability set. [Meta protocol][g-proto], [join][g-join], [compatibility baseline][g-baseline]. | **Operational gap.** Byte compatibility is not behavioral compatibility. Gate new topology roles/actions/policy semantics before any participant depends on them; define upgrade and rollback rules. |
| Policy agreement after failover | PD persists placement rules and uses cluster-version feature gates. These are shared management inputs, although observations and ordinary execution cursors may be local. [Rules][p-rule-save], [features][p-features]. | Voter policy is replicated, but custom scheduler code and some process-local settings remain caller-configured. [Policy][g-voter-policy], [scheduler interface][g-policy], [driver][g-scheduler]. | **Partial parity.** A replicated target does not ensure different binaries/custom policies make compatible decisions. Add capability/policy revision declarations and an admission/activation rule for incompatible participants. |
| Online configuration | PD exposes updates to supported scheduling/replication settings. TiKV validates and dispatches supported local configuration changes and can persist them to its configuration file; this local update is not a Region consensus operation. [PD API][p-config], [TiKV controller][t-config]. | Placement, hosting and voter policy have topology mutation APIs. Many process settings are constructor options; `Cluster.Rules()` exposes capacity rules without an online mutation API. [Cluster API][g-cluster], [options][g-options]. | **Partial parity.** Classify each setting as replicated policy, local tunable or restart-required configuration. Capacity expansion and cluster-wide rollout of local settings need an explicit supported procedure; a topology edit cannot change every process option. |
| Observability and administration | PD exposes stores, Regions, rules, schedulers and operators, with scheduling metrics. TiKV provides detailed metrics and `tikv-ctl` inspection/consistency/recovery operations. [PD router][p-api], [operator metrics][p-metrics], [TiKV tools][t-tools]. | Public administration/status/metrics APIs and local events exist; the caller builds its service interface. The small example is not a full operations product. [Cluster API][g-cluster], [metrics][g-metrics], [example][g-example]. | **Library boundary plus manageability gap.** Supply stable operation IDs, blocking reasons, freshness and cleanup-debt views, exporters and reference runbooks. A UI need not be embedded, but operational evidence must be available. |
| Security boundary | TiKV supports TLS configuration, allowed certificate names and certificate reload; PD has separate management APIs. Configuration still matters. [TiKV security][t-security], [PD API][p-api]. | Meta transport takes TLS configuration; load transport must authenticate senders before dispatch. Exact identity authorization is enforced by the library; application administration authorization remains an integration responsibility. [Meta transport][g-transport], [load contract][g-load-transport], [actions][g-actions]. | **Partial parity, not a security certification.** Provide and test an integration profile for identity binding, certificate rotation and admin authorization. Replicated approval is not user authentication or protection against a malicious authorized participant. |
| Permanent loss of a data-group majority | Normal changes stop. TiKV/PD also offer an explicit unsafe recovery workflow; force-leader code can synthesize votes and disable quorum checking under a recovery plan. [PD controller][p-unsafe], [TiKV force leader][t-unsafe]. | No force-new-group recovery after permanent majority loss. `ForceRetireNode` is not permission to manufacture a data-group quorum. [Mutations][g-mutations], [membership guards][g-safety], [Raft failure contract][g-raft-doc]. | **Recovery capability gap, not a normal-consensus safety gap.** Decide whether to support a separately authorized disaster workflow with explicit data-loss semantics and fencing of old copies. Never put it in the ordinary scheduler. |
| Backup and disaster restoration | TiKV includes snapshot-based backup/export machinery and diagnostic/recovery tools. Those do not by themselves back up PD or prove a coordinated deployment restore. [Backup endpoint][t-backup], [tools][t-tools]. | Raft snapshots and retained-state recovery exist; there is no public coordinated backup/restore protocol spanning `_meta`, application snapshots, identities and external effects. [Load snapshot API][g-load], [storage][g-storage], [runtime][g-runtime]. | **Operational gap.** Define recovery manifests, retention, verification and restore drills. Raft replication and local snapshots are not an independent backup. A deployment-level PD/etcd restore comparison needs additional environment-specific evidence. |
| Database-specific shared services | PD supplies timestamp allocation and GC state/barriers used by database protocols. These have their own persistence and leadership rules. [TSO][p-tso], [GC state][p-gc]. | `_meta` supplies topology, process sessions and locks; applications own transaction semantics, external side effects and retention of business data. [Meta FSM][g-sm], [state-machine contract][g-machine]. | **Intentional boundary.** Do not add TSO/MVCC/GC semantics merely to resemble TiKV. Define extension points if a gquorum application needs those guarantees. |
| Reliability evidence | The checkout contains integration/failpoint suites for membership, heartbeat behavior and recovery, alongside the implementations above. [Configuration tests][t-tests], [heartbeat tests][t-heartbeat-tests]. | Unit, functional, race, process and retained-compatibility coverage is documented. [Testing guide][g-tests], [feature validation][g-validation]. | **Needs proof at the intended scale and failure envelope.** Similar algorithms and passing existing tests cannot establish equal reliability, failover bounds or operational maturity. This comparison did not run either project's tests or failure experiments. |

## 6. Design direction for `_meta`, using TiKV as the reference

The useful reference is PD's separation of durable policy, observations,
per-group consensus and eventual cleanup. We should retain gquorum's explicit
approval and exact ownership model while addressing the availability and
manageability costs of its current global execution boundary.

| Priority | Proposed change | Required outcome / acceptance evidence |
| --- | --- | --- |
| P0: authority contract | Specify the validity interval of an approved effect across meta leadership changes, supersession and delayed delivery. Decide whether strict revocation needs an affected-group fence/epoch or a stronger coordination protocol. | Delayed actions from an old controller cannot violate the selected contract, reuse successor resources or silently become authorized by unrelated intent. Exercise stale local meta state and leadership changes; local checks alone are not a proof. |
| P0: failure isolation | Make approval and execution scopes explicit per group where dependencies allow it. Keep cluster-wide admission/decommission dependencies explicit. | Staging an unrelated edit or blocking one group's operation does not prevent safe repair elsewhere; the repair never approves that staged edit. Preserve CAS, exact action identities and durable history. |
| P0: cleanup separation | Represent consensus removal, runtime fencing and physical collection as separate durable milestones. Retain cleanup obligations until verified. | An unreachable removed host cannot indefinitely block unrelated topology progress. On return, it is fenced before serving, and cleanup cannot delete a successor's data. TiKV's asynchronous peer destruction is the reference, not permission to discard evidence. |
| P0: deployment fault tolerance | Add failure-domain constraints and capacity-aware eligibility for `_meta` voters and application replicas. | Show which host/zone failures each placement tolerates. Reject or report unsatisfiable placement; do not silently claim target redundancy or count learners as votes. |
| P0: rollout safety | Establish replicated feature/policy revisions and participant capability negotiation. | Old nodes cannot accept topology they misinterpret. Test mixed versions, feature activation, retained restarts and allowed rollback boundaries. |
| P1: consistent reads | Add a documented authoritative read/barrier API, separate from diagnostics. | A caller can obtain a safe read after leadership confirmation and required local application. Specify cancellation and partition behavior; do not retrofit a linearizability claim onto existing status reads. |
| P1: scalable reconciliation | Combine independent group execution with global/per-node budgets, priorities and observed resource pressure. Benchmark management metadata and metric fan-out. | Bound recovery storms and user-workload impact while many groups repair. Demonstrate useful throughput without unbounded snapshots, polling or progress writes. |
| P1: operational interface | Expose desired/approved/native/applied state, freshness, blocked reasons, current operation and cleanup obligations. Provide exporters and recovery/upgrade runbooks. | An operator can tell whether a request was accepted, committed, observed complete or physically cleaned, and can safely resume after an interrupted API response. |
| P1: disaster readiness | Design and test supported backup/restore first; decide separately whether explicit unsafe majority-loss recovery belongs in the library. | Verify restoration of `_meta`, application state, IDs and fencing evidence together. State data-loss and unavailable-operation boundaries; never imply a normal scheduler can repair permanent majority loss. |
| P2: optional broader capabilities | Consider joint membership, automatic group placement/partitioning, domain-aware durability modes and witnesses according to application needs. | Each feature has an end-to-end safety/recovery protocol and observability, rather than only new configuration fields. Database-specific range migration remains an explicit integration decision. |

P0 identifies work to settle before claiming TiKV-like management guarantees for
the corresponding deployment requirements. It does not classify every item as
a demonstrated safety bug. For example, global serialization is an implemented
availability limitation; strict cross-log revocation is an unresolved guarantee
requiring a defined contract and failure evidence.

### Reference scenarios for validating the design

These are proposed acceptance scenarios, not test results from this review:

1. **Management quorum lost, load quorum retained:** distinguish surviving
   application consensus from operations blocked by admission, sessions or
   management policy. Restore management without conflicting actions.
2. **Load quorum lost, management quorum retained:** normal repair refuses to
   create voting authority; status explains the missing quorum and any separate
   disaster-recovery option.
3. **Unrelated staged edit during a failure:** repair another group without
   approving or altering the staged request.
4. **Crash between group commit and progress commit:** the next controller
   recognizes the completed effect, preserves its exact identity and resumes
   without duplicating destructive work.
5. **Supersession with a delayed old action:** exercise receivers with stale
   `_meta` state, replaced processes, reused addresses and new group leadership.
6. **Decommission with a permanently offline host:** complete the supported
   consensus milestones while keeping durable cleanup debt and return fencing.
7. **Zone loss and low-space recovery:** placement retains the intended quorum;
   migration budgets prevent remaining nodes from exhausting resources.
8. **Mixed-version rollout and retained restart:** feature gates, approval
   decoding and retirement evidence remain compatible through the supported path.
9. **Many concurrent group failures:** measure detection freshness, scheduling
   fairness, metadata write amplification and time to restore redundancy.
10. **Restore from independent backup:** verify data and identity consistency,
    prevent old copies from rejoining incorrectly, and record achieved recovery
    point/time against explicit objectives.

## 7. Verification record and limits

The comparison follows concrete call paths, rather than relying on names such
as `leader`, `RaftCluster` or `Apply`: PD persistence and dispatch, TiKV command
validation/configuration apply, gquorum approval/authorization/effect execution,
and both systems' observation/completion paths. The references below identify
the source files used for each table row.

The inspected PD and raft-rs checkouts were clean. TiKV's source tree was clean
apart from untracked documentation. gquorum's in-progress implementation was
read without changing its source or index. Documentation links and whitespace
were checked; no compilation, test suite, benchmark or fault experiment was run
for this document.

This is an implementation comparison, not a deployment certification. It does
not establish an RPO/RTO, a maximum supported cluster size, security audit
completion, or equivalence to TiKV's production history. Those claims require
specified workloads, storage/transport implementations, placement, deployment
versions and measured failure/recovery evidence.

Related gquorum documentation: [Raft boundaries](raft.md),
[library architecture](library.md), [ownership and recovery](runtime.md), and
[detailed consensus versus leader-local action report](../reports/20261012-raft-consensus-and-leader-local-actions.md).

<!-- Source references: upstream URLs identify the exact locally inspected revision. -->

[t-bootstrap]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/raftstore-v2/src/bootstrap.rs#L59
[t-pd]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/raftstore/src/store/worker/pd.rs#L1841
[t-stats]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/raftstore/src/store/worker/pd.rs#L1378
[t-conf]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/raftstore-v2/src/operation/command/admin/conf_change.rs#L84
[t-command]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/raftstore-v2/src/operation/command/mod.rs#L169
[t-apply]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/raftstore/src/store/fsm/apply.rs#L2167
[t-writer]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/raftstore/src/store/async_io/write.rs#L1144
[t-ready]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/raftstore/src/store/peer.rs#L2977
[t-transfer]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/raftstore-v2/src/operation/command/admin/transfer_leader.rs#L45
[t-read]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/raftstore-v2/src/operation/query/lease.rs#L34
[t-life]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/raftstore-v2/src/operation/life.rs#L3
[t-split]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/raftstore/src/store/fsm/apply.rs#L2631
[t-merge]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/raftstore/src/store/fsm/apply.rs#L2843
[t-replication-mode]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/raftstore/src/store/replication_mode.rs#L166
[t-witness]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/raftstore/src/store/fsm/apply.rs#L936
[t-client]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/pd_client/src/client.rs#L799
[t-routing]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/pd_client/src/client.rs#L524
[t-config]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/src/config/mod.rs#L5149
[t-tools]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/cmd/tikv-ctl/src/main.rs#L644
[t-security]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/security/src/lib.rs#L172
[t-unsafe]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/raftstore-v2/src/operation/unsafe_recovery/force_leader.rs#L145
[t-backup]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/components/backup/src/endpoint.rs#L337
[t-tests]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/tests/failpoints/cases/test_conf_change.rs
[t-heartbeat-tests]: https://github.com/tikv/tikv/blob/137cbb2593decafebf99905b4d570869230565c2/tests/integrations/raftstore/test_region_heartbeat.rs
[r-raft]: https://github.com/tikv/raft-rs/blob/1fd05e000fb094015507c5c985849d370100fb72/src/raft.rs#L939
[r-joint]: https://github.com/tikv/raft-rs/blob/1fd05e000fb094015507c5c985849d370100fb72/src/quorum/joint.rs#L43
[p-leadership]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/pkg/election/leadership.go#L221
[p-etcd]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/pkg/storage/kv/etcd_kv.go#L97
[p-store]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/server/cluster/cluster.go#L2008
[p-offline]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/server/cluster/cluster.go#L1704
[p-bury]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/server/cluster/cluster.go#L2094
[p-members]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/server/api/member.go#L114
[p-bootstrap]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/server/server.go#L787
[p-ids]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/pkg/id/id.go#L50
[p-controller]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/pkg/schedule/operator/operator_controller.go#L86
[p-limits]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/pkg/schedule/operator/operator_controller.go#L1037
[p-steps]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/pkg/schedule/operator/step.go#L120
[p-regions]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/server/cluster/cluster.go#L1394
[p-region-storage]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/pkg/storage/storage.go#L201
[p-region-default]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/server/config/config.go#L207
[p-rules]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/pkg/schedule/placement/rule.go#L52
[p-rule-save]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/pkg/schedule/placement/rule_manager.go#L549
[p-checker]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/pkg/schedule/checker/rule_checker.go#L144
[p-filters]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/pkg/schedule/filter/filters.go#L202
[p-balance]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/pkg/schedule/schedulers/balance_region.go#L88
[p-balance-leader]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/pkg/schedule/schedulers/balance_leader.go#L330
[p-prepare]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/pkg/schedule/prepare_checker.go#L43
[p-version]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/server/cluster/cluster.go#L1663
[p-features]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/pkg/versioninfo/feature.go#L31
[p-api]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/server/api/router.go
[p-config]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/server/api/config.go#L133
[p-metrics]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/pkg/schedule/operator/metrics.go#L24
[p-unsafe]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/pkg/unsaferecovery/unsafe_recovery_controller.go#L220
[p-tso]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/pkg/tso/tso.go
[p-gc]: https://github.com/tikv/pd/blob/34ce7eca7eaf9b4fd06061984a1cc35884c49ee4/pkg/storage/endpoint/gc_states.go
[g-engine]: ../../internal/raftengine/group.go
[g-loop]: ../../internal/raftengine/loop.go
[g-forwarding]: ../../internal/raftengine/forwarding.go
[g-options]: ../../node_options.go
[g-sm]: ../../internal/metaengine/sm.go
[g-planning]: ../../internal/metaservice/planning.go
[g-documents]: ../../internal/metaservice/documents.go
[g-operator-state]: ../../internal/metaengine/operator_state.go
[g-authorization]: ../../internal/metaengine/authorization.go
[g-controller]: ../../internal/operator/controller.go
[g-actions]: ../../internal/managed/actions.go
[g-dispatch]: ../../internal/managed/dispatch.go
[g-membership]: ../../membership/coordinator.go
[g-safety]: ../../membership/safety.go
[g-cluster]: ../../cluster.go
[g-load]: ../../load/group.go
[g-load-transport]: ../../load/transport.go
[g-machine]: ../../load/machine.go
[g-storage]: ../../storage/storage.go
[g-document]: ../../topology/document.go
[g-validate]: ../../topology/validate.go
[g-hosting]: ../../topology/hosting.go
[g-voter-policy]: ../../topology/voter_policy.go
[g-voter-plan]: ../../topology/voter_plan.go
[g-planner]: ../../topology/planner.go
[g-join]: ../../internal/metaservice/join.go
[g-bootstrap]: ../../internal/metaservice/bootstrap.go
[g-admission]: ../../internal/metaengine/admission.go
[g-readmission]: ../../internal/managed/readmission.go
[g-retirement]: ../../internal/managed/retirement.go
[g-mutations]: ../../internal/metaservice/mutations.go
[g-session]: ../../internal/metaservice/session.go
[g-expiry]: ../../internal/metaservice/expiry.go
[g-rpc]: ../../internal/metaservice/rpc.go
[g-transport]: ../../internal/metatransport/transport.go
[g-scheduler]: ../../internal/scheduling/driver.go
[g-voters]: ../../policy/voters.go
[g-restore]: ../../policy/restore_degraded.go
[g-policy]: ../../policy/scheduler.go
[g-metrics]: ../../metrics/types.go
[g-collector]: ../../internal/observe/metrics.go
[g-evidence]: ../../internal/observe/evidence.go
[g-switches]: ../../internal/metaengine/voter_switch.go
[g-voter-check]: ../../internal/votercheck/check.go
[g-proto]: ../../internal/metapb/meta.proto
[g-runtime]: runtime.md
[g-raft-doc]: raft.md
[g-example]: ../../examples/cluster/cluster.go
[g-baseline]: ../testing/api-baseline-physical.md
[g-tests]: ../testing/tests.md
[g-validation]: ../testing/stable-learner-validation.md
