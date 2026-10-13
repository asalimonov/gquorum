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
and the [implementation plan](../plans/20261012-authority-contract-consistent-reads.md).

P1 safe reads use ReadIndex confirmation plus local application on any ready
replica, including followers and learners. `Group.Read` runs optional Query
serialized with Apply/Restore; Cluster supplies authoritative topology, lock-holder
and authority reads. Existing diagnostics stay observational. Cancellation ends a
wait and partitions prevent fresh confirmation; there are no lease reads or
external/time-valid lock fences. See the [read contract](raft.md).

## Backlog

| Priority | Proposed change | Required outcome / acceptance evidence |
| --- | --- | --- |
| P0: failure isolation | Make approval and execution scopes explicit per group where dependencies allow it. Keep cluster-wide admission/decommission dependencies explicit. | Staging an unrelated edit or blocking one group's operation does not prevent safe repair elsewhere; the repair never approves that staged edit. Preserve CAS, exact action identities and durable history. |
| P0: cleanup separation | Represent consensus removal, runtime fencing and physical collection as separate durable milestones. Retain cleanup obligations until verified. | An unreachable removed host cannot indefinitely block unrelated topology progress. On return, it is fenced before serving, and cleanup cannot delete a successor's data. TiKV's asynchronous peer destruction is the reference, not permission to discard evidence. |
| P0: deployment fault tolerance | Add failure-domain constraints and capacity-aware eligibility for `_meta` voters and application replicas. | Show which host/zone failures each placement tolerates. Reject or report unsatisfiable placement; do not silently claim target redundancy or count learners as votes. |
| P0: rollout safety | Establish replicated feature/policy revisions and participant capability negotiation. | Old nodes cannot accept topology they misinterpret. Test mixed versions, feature activation, retained restarts and allowed rollback boundaries. |
| P1: scalable reconciliation | Combine independent group execution with global/per-node budgets, priorities and observed resource pressure. Benchmark management metadata and metric fan-out. | Bound recovery storms and user-workload impact while many groups repair. Demonstrate useful throughput without unbounded snapshots, polling or progress writes. |
| P1: operational interface | Expose desired/approved/native/applied state, freshness, blocked reasons, current operation and cleanup obligations. Provide exporters and recovery/upgrade runbooks. | An operator can tell whether a request was accepted, committed, observed complete or physically cleaned, and can safely resume after an interrupted API response. |
| P1: disaster readiness | Supported backup/restore first for explicit unsafe majority-loss recovery. | Verify restoration of `_meta`, application state, IDs and fencing evidence together. State data-loss and unavailable-operation boundaries; never imply a normal scheduler can repair permanent majority loss. |
