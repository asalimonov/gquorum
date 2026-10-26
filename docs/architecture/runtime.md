# Internal runtime of gquorum

Explain only the most important and high level parts of runtime of the library.

## Key public entities and interfaces

| Entity | Package | Role |
| --- | --- | --- |
| `Node` | root | One participant: `Start`/`WaitReady`/`Stop`, local load replicas, sessions, authenticated load ingress. |
| `Cluster` | root | Administration view from `node.Cluster()`: reads, staging/approval, node/group mutations, locks. |
| `Recovery` | root | Permanent-majority-loss controller: `Inspect`, `Approve`, `Run`, `Cancel`, `Status`, `List`. |
| `NodeOptions`, `GroupFactory`, `RetireReplicaFunc` | root | Settings and application callbacks that create and retire load replicas. |
| `Document`, `Group`, `Node`, `Replica` | `topology` | Versioned cluster declaration; `Latest` is staged intent, `Target` is approved. |
| `Plan`, `Operation`, `AuthorityGrant`, `EffectReceipt` | `topology` | Immutable approved work, grants for exact effects and proof of their completion. |
| `Identity` | `topology` | Participant identity: cluster, node name, `_meta` member ID, process instance. |
| `StateMachine`, `QueryMachine`, `Application` | `load` | Application `Apply`/`Snapshot`/`Restore`, optional `Query`, `Run` and `Close`. |
| `Group`, `Leadership` | `load` | Handle to one exact local replica; term-scoped proposals for leader work. |
| `Transport` | `load` | Load Raft delivery, replica reads, authorized actions and fences. |
| `Standalone` | `load` | Independent engine without `_meta`; caller owns membership and recovery. |
| `Store`, `RetirementStore`, `Identity` | `storage` | Raft persistence, retired-state collection, exact durable replica identity. |
| `RepairPolicy` | `scheduling` | Chooses repair decisions; the core revalidates them. |
| `Hooks` | `lifecycle` | Local readiness, retention and removal decisions. |
| `Coordinator`, `SafetyRules` | `membership` | Serialized membership changes with quorum safety checks. |
| `Error`, `Code` | `fault` | Classified errors shared across APIs and wire protocols. |

## Internal components

| Component | Package | Role |
| --- | --- | --- |
| `metaservice.Group` | `internal/metaservice` | Runs `_meta`: bootstrap, join, document APIs, sessions, authority, operator and scheduler wiring. |
| `metaengine.Machine` | `internal/metaengine` | Deterministic metadata FSM: documents, operations, checkpoints, grants, cleanup debt, sessions. |
| `operator.Controller` | `internal/operator` | Leader-owned executor of approved plan blocks; records exact receipts before advancing. |
| `scheduling.Scheduler` | `internal/scheduling` | Serialized repair loop around `RepairPolicy` with revalidation and CAS approval. |
| `managed.Host` | `internal/managed` | Owns local load replicas: reservations, holds, effects, fences, retirement, readmission. |
| `authoritystore.Store` | `internal/authoritystore` | Node-local durable host fences, effect receipts and collection journal. |
| `raftengine.Group` | `internal/raftengine` | One Raft replica: run loop, persistence ordering, apply, reads, snapshots, send workers. |

## Identities

| Identity | Scope |
| --- | --- |
| `NodeName` | Stable logical node name. |
| `MemberID` | Numeric `_meta` Raft ID; never reused. |
| `ReplicaID` | Numeric load-group Raft ID within one group; never reused. |
| `InstanceID` | One process run; fresh on every start. |
| `RunID` | One engine run of a local replica. |
| Document version, authority revision, Raft index | Separate counters; never interchangeable. |

## Durable state ownership

| State | Owner |
| --- | --- |
| `StorageDir/_meta.bolt` | `_meta` Raft log, snapshot, metadata FSM, identity pin, load-replica ledger. |
| `StorageDir/_authority.bolt` | Host fences, effect receipts, collection journal; survives `_meta` replacement. |
| WAL root `<GroupID>/` | Load replica identity, Raft log, snapshots and applied hints. |
| Memory store | Volatile Raft state of `StorageMemory` groups. |
| Application resources | Application-owned; acquired by `GroupFactory`, released by `Close`, removed by `Retire`. |

## Main flows

| Flow | Path |
| --- | --- |
| Start a node | `Node.Start` → metadata and authority checks → `_meta` founding, join or recovery → `managed.Host` recovers assigned replicas → publish handles. |
| Change topology | `Cluster` mutation → staged `Latest` (CAS) → `Approve` → immutable plan and grants → `operator` → host/group effects → receipts → per-group convergence. |
| Write a command | `load.Group.Propose` → engine admission → leader (or forwarded) proposal → durable commit → local `Apply` → receipt and result. |
| Read | `Group.Read` / `Cluster.ReadTopology` → ReadIndex → wait for local apply → serialized query. |
| Repair | `observe` evidence → `RepairPolicy` → scheduler revalidation → CAS approval → normal operator execution. |
| Retire or readmit | Fence and join the exact owner → application `Retire` → quarantine Raft state → ledger cleanup; readmission installs fresh IDs. |
