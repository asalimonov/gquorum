# Design constraints

## Raft

1. Each node of a cluster should have a replica of `_meta` raft group. For clusters with more than 5 nodes we recommend not more 5 voters, other hosts contain learners for `_meta` Raft group.
2. Cluster cannot reuse Raft ID or name of a node which were removed from a cluster before. Admistrator or some automation should add this node with a new name or id to avoid collisions and issues with WALs and metadata.

## Out of scope

1. Router or forwarder of requests between nodes when number of replicas of "load" raft groups is less than number of nodes in cluster.
2. State Machine for load group, because it is specific for each implementation