# Building a cluster with gquorum

`gquorum` owns a node's topology quorum and its local application replicas. Applications supply settings, listeners, identities, paths, load state machines and load transport. The library contains the `_meta` protocol, its bbolt store, the Raft engines, recovery, an etcd WAL backend for load groups and an in-memory store for groups that do not need to survive a full restart.

The public handles separate participant operation, administration, consensus and offline recovery:

| Handle | Responsibility |
| --- | --- |
| `gquorum.Node` | One participant: start, readiness, shutdown, local replica ownership, sessions and load protocol receivers. |
| `gquorum.Cluster` | A stable administration view obtained from `node.Cluster()`: topology, membership, approval and progress. It has no independent lifetime. |
| `*load.Group` | One local replica: application proposals, leadership/status, snapshots and proposal-pressure diagnostics. A stopped handle never redirects into its replacement. |
| `gquorum.Recovery` | Configuration for inspecting, accepting and archiving a stopped survivor. Each call owns and releases its resources; the handle has no Close lifetime. |

```text
Application
    |
    v
gquorum.Node
    |-- Cluster administration --> _meta Raft group
    |                               |-- approved operations --------+
    |                               `-- scheduler and repair policy |
    |-- managed local replicas <------------------------------------+
    |      |-- load.Group and Raft engine
    |      `-- application factory, transport and retirement
    `-- lifecycle hooks
```

## Design constraints

### Raft

1. Each node of a cluster should have a replica of `_meta` raft group. `_meta` is used for management of cluster topology and reporting status of nodes.
2. Removed metadata member IDs and load replica IDs are never reused; deleted group IDs stay retired. Retained restarts keep durable pins and use a fresh process InstanceID. Explicit readmission may retain the node name only with fresh monotonic IDs, exact predecessor fencing/retirement receipts and durable authority rebind. A name or address alone never proves ownership.
3. The library uses only 2 TCP ports to reduce complexity of management of connections and firewall rules.
4. No external message brokers and cache services to reduce external dependencies.
5. Library should hide complexity of management of cluster and try not extend the current public APIs of the library without significant reason. Library should manage statuses, retries, policies and scheduling itself.

### Out of scope

1. Router or forwarder of requests between nodes when number of replicas of "load" raft groups is less than number of nodes in cluster.
2. State Machine for load group, because it is specific for each implementation
3. Placement of raft groups on hosts based on host (node) resources, `_meta` raft group is not a resource scheduler. It supports only `labels`.
4. No affinity and anti-affinity scopes between groups.
5. Service discovery, it should be in caller's code.
6. On disk persistance of a receiving shapshots, only in memory to avoid management of its files and state.
7. Additional storages, only etcd/bolt storage because it is very reliable and has good level of maintenance instead of BadgerDB and Pebble. etcd/bolt has lower performance due to using BTree instead of LSTM but reliability is more important. `storage/memstore` is for particular and test cases.

## Construct and start a node

`NewNode(NodeOptions)` validates and copies settings without opening storage or calling application code. `Start(ctx)` acquires local resources, starts `_meta` and begins admission/recovery. It returns before quorum readiness, so callers can start several nodes sequentially. `WaitReady(ctx)` waits for local admission, recovery, session activation and the configured startup gate; canceling that wait does not stop the node. The context passed to `Start` owns its lifetime.

The application creates these settings directly; there is no configuration-file loader or built-in storage root:

| Setting | Meaning |
| --- | --- |
| `Identity topology.Identity` | Node name, cluster ID, durable meta member ID and fresh process `InstanceID`. Member ID can be resolved from bootstrap/admission or a retained pin. |
| `Node topology.Node` | The caller-defined node type and admission description. Every admitted type joins `_meta`; type controls load placement. |
| `Bootstrap BootstrapOptions` | Founders for a new cluster, or `Join` with `JoinSeeds` for admission. Retained nodes recover their durable identity. |
| `Limits topology.Limits` | Replicated capacity: defaults are three nodes, five replicas per load group, four executing groups and two cleanup requests. Zero selects defaults; negative values refuse. |
| `Placement topology.PlacementRule` | Default load replica count and eligible node types. Individual groups can override it. |
| `StorageDir string` | Explicit system-storage directory containing fixed `_meta.bolt` and `_authority.bolt` files; outside all load retirement roots. |
| `Meta MetaOptions` | Network, timing, reads, session, observation, snapshot and watcher settings. |
| `Loads LoadOptions` | Optional managed load integration; leave empty for a meta-only participant. |
| `Scheduling scheduling.Options` | Repair policy selection and local reconciliation timing. |
| `Lifecycle lifecycle.Options` | Local readiness, retirement-retention and removal hooks. |
| `CallbackTimeout time.Duration` | Shared deadline for finite application and decision callbacks; zero selects ten seconds. |
| `Health`, `Events`, `Logger` | Optional application health observations, borrowed local event bus and logger. |

`MetaNetworkOptions` takes a caller-created listener, advertised address and cluster credential, plus TLS/client TLS or explicit `AllowPlaintext`. A caller address resolver supports address-free topology documents. The listener transfers to Node when startup acquires the meta service. Dependencies and callbacks must remain usable until their owner joins them.

Each founder supplies `FoundingMember{Node, MemberID, Address}` with a unique positive member ID. `FoundingIDs(clusterID, names)` derives a stable founding map. For a fixed roster, `StaticPeers` (node name to `StaticPeer{MetaAddress, LoadAddress}`) implements the meta `Resolver`, provides `ResolveLoad` for `loadgrpc.Options.Resolve`, and builds sorted founders with `Founders(clusterID, nodeType)` using `FoundingIDs`. A route is never proof of admission. Dynamic service discovery is not provided; it can implement the same resolver shapes.

`LoadStoredIdentity(storageDir)` reads the fixed `_meta.bolt` file's validated durable pin without creating storage; it returns `topology.IdentityLookup`. On success, `Found` reports pin presence and `Identity` contains the durable pin without a process incarnation.

Use `util.NewInstanceID()` once per process start for `Identity.InstanceID`, including retained restarts. This helper is only for process identities; request IDs and durable member/replica IDs have separate contracts. See the executable [node construction](../../examples/cluster/cluster.go) and [process wiring](../../examples/cluster/main.go) for founding, retained restart, listener creation and shutdown.

## Topology and cluster administration

The cluster has one distributed `_meta` Raft group. Every admitted node hosts a replica of it, so the nodes share an approved description of cluster membership, placement and application groups. The application groups, called **load groups**, each have their own replicas, leader and quorum. A load command uses its load group's quorum; creating, moving or deleting that group is authorized through `_meta`.

This five-node cluster illustrates one possible placement. A name repeated in different boxes is one distributed Raft group with a replica on each listed node. A dash means that node has no replica of that group.

```text
+----------------------+  +----------------------+  +----------------------+
| node1                |  | node2                |  | node3                |
| _meta     voter      |  | _meta     voter      |  | _meta     voter      |
| orders    voter      |  | orders    voter      |  | orders    voter      |
| jobs      voter      |  | jobs      -          |  | jobs      voter      |
| accounts  -          |  | accounts  voter      |  | accounts  learner    |
+----------------------+  +----------------------+  +----------------------+

+----------------------+  +----------------------+
| node4                |  | node5                |
| _meta     learner    |  | _meta     learner    |
| orders    -          |  | orders    -          |
| jobs      voter      |  | jobs      learner    |
| accounts  voter      |  | accounts  voter      |
+----------------------+  +----------------------+
```

Each node keeps its own local files. For example, if node3 uses the library's persistent load store, its configured roots have this shape:

```text
node3
|-- <StorageDir>/
|   |-- _meta.bolt
|   `-- _authority.bolt
`-- <load WAL root>/
    |-- orders/
    |-- jobs/
    `-- accounts/
```

`_meta.bolt` contains this node's `_meta` log, snapshot and topology state. `_authority.bolt` holds local proof for authorized effects. Each load directory contains that group's local Raft state and snapshots. The application chooses both roots and owns any separate business files. A group using memory storage has no load WAL directory; a custom store can use a different layout.

Cluster administration changes a versioned topology document. These are views of its progress, rather than one global execution lock:

```text
Proposed document vN
        |
        | Stage (version check)
        v
Latest: staged vN -------- Approve --------> Target: approved vN
        |                                              |
        | Stage vN+1                                   | Apply approved work
        v                                              v
Latest: staged vN+1                      Group / node progress for vN
                                                       |
                                                       | Required topology work is proven
                                                       v
                                               FullyConverged: vN
```

The views in the diagram mean:

| View                  | Meaning                                                                  |
| --------------------- | ------------------------------------------------------------------------ |
| `Latest`              | Staged document; does not authorize replica changes.                     |
| `Target`              | Approved document; authorizes topology work.                             |
| Group / node progress | What each part has reached; groups progress independently.               |
| `FullyConverged`      | Historical topology completion; cleanup and current quorum are separate. |

## Nodes life-cycle

### Approved node state

A node's approved state controls where load replicas may be placed. It is different from whether the node's process is currently running or reachable. Each change below is a topology operation approved through `_meta`.

```text
not admitted --AddNode--> active --DrainNode--> draining --loads leave--> drained --ExcludeNode--> excluded
```

| State      | Cluster behavior                                             | Available administration                       |
| ---------- | ------------------------------------------------------------ | ---------------------------------------------- |
| `active`   | Hosts `_meta`; eligible for new load replicas.               | `DrainNode`; `ReadmitNode` if confirmed dead.   |
| `draining` | Evacuates loads; keeps `_meta`; no new load placement.        | `ActivateNode`, or wait for evacuation.         |
| `drained`  | No assigned loads; keeps `_meta`.                            | `ActivateNode` or `ExcludeNode`.                |
| `excluded` | Outside approved membership.                                 | `AddNode` with fresh identities.                |

Normal exclusion requires a completed drain and no remaining load assignments. `ForceRetireNode` is an explicit, audited option for a non-excluded node when that path cannot complete; it still follows quorum and membership safety checks. Exclusion is not proof that an unreachable host's files were erased.

### Local process state

`Node.Status().State` describes this process, not the approved placement state. A node can be `active` in the topology while its process is stopped or unavailable.

| Process state | Caller action                                                         |
| ------------- | --------------------------------------------------------------------- |
| `new`         | Call `Start`.                                                         |
| `starting`    | Use `WaitReady`, `RequestStop` or `Stop`.                              |
| `ready`       | Use assigned replicas and cluster APIs; `RequestStop` or `Stop`.       |
| `fenced`      | Call `Stop` to join shutdown; this participant no longer serves.      |
| `stopping`    | Call `Stop` to wait for owned work.                                    |
| `stopped`     | Construct a new Node to restart from retained state.                  |
| `failed`      | Inspect the error and call `Stop`; construct a new Node after release. |

## Scheduling and policies

The default scheduler runs on the `_meta` leader. It observes approved placement, group membership and replica health, then selects at most one guarded repair per evaluation through the same topology approval path as an administrator. Different groups can keep serving and progressing while one repair waits. `Node.SchedulingStatus()` shows whether the scheduler is working, waiting or blocked and why.

| Observed condition                               | Automatic cluster behavior                                       |
| ------------------------------------------------ | ---------------------------------------------------------------- |
| Fewer voters than the configured target          | Promote a stable, caught-up learner when safe.                   |
| More voters than the target                      | Demote a voter, preferring a follower.                           |
| A voter stays unavailable or far behind          | Replace its voter seat with a stable eligible learner.           |
| Legacy group without voter config misses a voter | Restore the seat with a fresh replica ID after stability checks. |

Adding or activating a node updates placement for groups whose approved hosting rules include it. A new replica may start as a learner and later become a voter when the target and safety checks permit. Draining a node moves its load assignments through an approved topology change.

These voter repairs apply to configured load groups and `_meta`; every admitted node still hosts an `_meta` replica, even when it is a learner. For example, if one voter stays unavailable and another host has a stable learner, the scheduler may promote that learner and demote the unavailable voter. The scheduler respects approved hosting and placement, voter domains, learner pins and live quorum. It does not move healthy groups simply to balance nodes. If it lacks a safe candidate or reliable observations, it waits rather than making an unsafe membership change.

Applications can disable local scheduling or supply a custom repair policy. The library still checks its proposed action before approval and does not approve unrelated staged edits. Local `lifecycle.Hooks` govern readiness, retention and removal decisions; they do not replace the replicated placement and voter rules.

More details see in [placement](./placement.md).
