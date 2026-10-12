# Design constraints

## Raft

1. Each node of a cluster should have a replica of `_meta` raft group. For clusters with more than 5 nodes we recommend not more 5 voters, other hosts contain learners for `_meta` Raft group.
2. Cluster cannot reuse Raft ID or name of a node which were removed from a cluster before. Admistrator or some automation should add this node with a new name or id to avoid collisions and issues with WALs and metadata.

## Out of scope

1. Router or forwarder of requests between nodes when number of replicas of "load" raft groups is less than number of nodes in cluster.
2. State Machine for load group, because it is specific for each implementation
3. Placement of raft groups on hosts based on host (node) resources, `_meta` raft group is not a resource scheduler. It use only labels for host processes (nodes) which are specified via configuration file or startup parameters.

## Backlog

| Priority | Proposed change | Required outcome / acceptance evidence |
| --- | --- | --- |
| P0: authority contract | Ability to specify the validity interval of an approved effect across meta leadership changes, supersession and delayed delivery. Decide whether strict revocation needs an affected-group fence/epoch or a stronger coordination protocol. | Delayed actions from an old controller cannot violate the selected contract, reuse successor resources or silently become authorized by unrelated intent. Exercise stale local meta state and leadership changes; local checks alone are not a proof. |
| P0: failure isolation | Make approval and execution scopes explicit per group where dependencies allow it. Keep cluster-wide admission/decommission dependencies explicit. | Staging an unrelated edit or blocking one group's operation does not prevent safe repair elsewhere; the repair never approves that staged edit. Preserve CAS, exact action identities and durable history. |
| P0: cleanup separation | Represent consensus removal, runtime fencing and physical collection as separate durable milestones. Retain cleanup obligations until verified. | An unreachable removed host cannot indefinitely block unrelated topology progress. On return, it is fenced before serving, and cleanup cannot delete a successor's data. TiKV's asynchronous peer destruction is the reference, not permission to discard evidence. |
| P0: deployment fault tolerance | Add failure-domain constraints and capacity-aware eligibility for `_meta` voters and application replicas. | Show which host/zone failures each placement tolerates. Reject or report unsatisfiable placement; do not silently claim target redundancy or count learners as votes. |
| P0: rollout safety | Establish replicated feature/policy revisions and participant capability negotiation. | Old nodes cannot accept topology they misinterpret. Test mixed versions, feature activation, retained restarts and allowed rollback boundaries. |
| P1: consistent reads | Add an authoritative read/barrier API, separate from diagnostics. | A caller can obtain a safe read after leadership confirmation and required local application. Specify cancellation and partition behavior; do not retrofit a linearizability claim onto existing status reads. |
| P1: scalable reconciliation | Combine independent group execution with global/per-node budgets, priorities and observed resource pressure. Benchmark management metadata and metric fan-out. | Bound recovery storms and user-workload impact while many groups repair. Demonstrate useful throughput without unbounded snapshots, polling or progress writes. |
| P1: operational interface | Expose desired/approved/native/applied state, freshness, blocked reasons, current operation and cleanup obligations. Provide exporters and recovery/upgrade runbooks. | An operator can tell whether a request was accepted, committed, observed complete or physically cleaned, and can safely resume after an interrupted API response. |
| P1: disaster readiness | Supported backup/restore first for explicit unsafe majority-loss recovery. | Verify restoration of `_meta`, application state, IDs and fencing evidence together. State data-loss and unavailable-operation boundaries; never imply a normal scheduler can repair permanent majority loss. |
