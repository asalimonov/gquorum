# Design constraints

## Raft

1. Each node of a cluster should have a replica of `_meta` raft group. For clusters with more than 5 nodes we recommend not more 5 voters, other hosts contain learners for `_meta` Raft group.
2. Removed metadata member IDs and load replica IDs are never reused; deleted group IDs stay retired. Retained restarts keep durable pins and use a fresh process InstanceID. Explicit readmission may retain the node name only with fresh monotonic IDs, exact predecessor fencing/retirement receipts and durable authority rebind. A name or address alone never proves ownership.

## Out of scope

1. Router or forwarder of requests between nodes when number of replicas of "load" raft groups is less than number of nodes in cluster.
2. State Machine for load group, because it is specific for each implementation
3. Placement of raft groups on hosts based on host (node) resources, `_meta` raft group is not a resource scheduler. It use only labels for host processes (nodes) which are specified via configuration file or startup parameters.
4. Service discovery, it should be in caller's code.
5. On disk persistance of a receiving shapshots, only in memory to avoid management of its files

## Backlog

| Priority | Proposed change | Required outcome / acceptance evidence |
| --- | --- | --- |
| P0: deployment fault tolerance | Validate deployment AZ labels against implemented strict voter-domain placement; applications supply resource-aware eligibility. | Show which host/zone failures each placement tolerates. Reject or report unsatisfiable placement; do not silently claim target redundancy or count learners as votes. |
| P0: rollout safety | Establish replicated feature/policy revisions and participant capability negotiation. | Old nodes cannot accept topology they misinterpret. Test mixed versions, feature activation, retained restarts and allowed rollback boundaries. |
| P1: scalable reconciliation | Combine independent group execution with global/per-node budgets, priorities and observed resource pressure. Benchmark management metadata and metric fan-out. | Bound recovery storms and user-workload impact while many groups repair. Demonstrate useful throughput without unbounded snapshots, polling or progress writes. |
| P1: disaster readiness | Supported backup/restore first for explicit unsafe majority-loss recovery. | Verify restoration of `_meta`, application state, IDs and fencing evidence together. State data-loss and unavailable-operation boundaries; never imply a normal scheduler can repair permanent majority loss. |
