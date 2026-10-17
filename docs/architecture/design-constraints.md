# Design constraints

## Raft

1. Each node of a cluster should have a replica of `_meta` raft group. For clusters with more than 5 nodes we recommend not more 5 voters, other hosts contain learners for `_meta` Raft group.
2. Removed metadata member IDs and load replica IDs are never reused; deleted group IDs stay retired. Retained restarts keep durable pins and use a fresh process InstanceID. Explicit readmission may retain the node name only with fresh monotonic IDs, exact predecessor fencing/retirement receipts and durable authority rebind. A name or address alone never proves ownership.

## Out of scope

1. Router or forwarder of requests between nodes when number of replicas of "load" raft groups is less than number of nodes in cluster.
2. State Machine for load group, because it is specific for each implementation
3. Placement of raft groups on hosts based on host (node) resources, `_meta` raft group is not a resource scheduler. It use only labels for host processes (nodes) which are specified via configuration file or startup parameters.

## Implemented contracts

P0 authority uses immutable committed grants and affected-resource fencing.
Approval requests revocation; an older delayed action may finish before its
resource fence, and must refuse afterward. Unreachable hosts or missing group
quorum keep revocation pending. Exact receipts distinguish completed effects from
native absence, and retained evidence protects successor resources. See
[authority and recovery](runtime.md#authority-control-after-participation-fencing)
and the [scoped target](../brainstorms/20261015-failure-isolation-and-operational-api-target.md).

P1 safe reads use ReadIndex confirmation plus local application on any ready
replica, including followers and learners. `Group.Read` runs optional Query
serialized with Apply/Restore; Cluster supplies authoritative topology, lock-holder
and authority reads. Existing diagnostics stay observational. Cancellation ends a
wait and partitions prevent fresh confirmation; there are no lease reads or
external/time-valid lock fences. See the [read contract](raft.md).

`FI_SCOPE` / `NO_ACTIVE`: Latest and Target are immutable intent; durable group/node
checkpoints own current convergence. FullyConverged is historical. Per-group
operation ownership and state-based admission/evacuation barriers replace global
execution gates. Disjoint repair atomically rebases staged intent without
approving it; strict CAS and retained exact replay remain mandatory.

`CLEANUP_PROOF`: native removal, joined stop, retirement and collection are
separate facts. Whole-group deletion still requires every former owner's stop
proof. Detached cleanup uses its own bounded workers, durable host journal and
store marker; permanently lost hosts can leave unconfirmed debt. Same-host
successors still wait for actual owner release. No public cleanup manager/API.

The Go operational interface is Operation/WaitOperation/RetryOperation and one
ReadProgress query. Page bounds and cached observation timestamps are explicit;
quorum-confirmed metadata is not fresh native health. Defaults are four executing
groups and two cleanup requests, immutable replicated Limits. Retained proof has
no global count cap. Full-state persistence remains a scale limit: 10,000 debts
measured ~51 MB per changed Apply; see the [measurement](../plans/20261015-failure-isolation-and-operational-api.md#progress-log).

## Backlog

| Priority | Proposed change | Required outcome / acceptance evidence |
| --- | --- | --- |
| P0: deployment fault tolerance | Validate deployment AZ labels against implemented strict voter-domain placement; applications supply resource-aware eligibility. | Show which host/zone failures each placement tolerates. Reject or report unsatisfiable placement; do not silently claim target redundancy or count learners as votes. |
| P0: rollout safety | Establish replicated feature/policy revisions and participant capability negotiation. | Old nodes cannot accept topology they misinterpret. Test mixed versions, feature activation, retained restarts and allowed rollback boundaries. |
| P1: scalable reconciliation | Combine independent group execution with global/per-node budgets, priorities and observed resource pressure. Benchmark management metadata and metric fan-out. | Bound recovery storms and user-workload impact while many groups repair. Demonstrate useful throughput without unbounded snapshots, polling or progress writes. |
| P1: disaster readiness | Supported backup/restore first for explicit unsafe majority-loss recovery. | Verify restoration of `_meta`, application state, IDs and fencing evidence together. State data-loss and unavailable-operation boundaries; never imply a normal scheduler can repair permanent majority loss. |
