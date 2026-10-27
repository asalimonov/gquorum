# Model coverage ledger

Implemented checkpoint: Tasks 1–3 (foundation, runner, consensus/network). Source baseline: `e8e85dbd7879b1c858252cd2a6126a36920f8a8e`.

The table enumerates public methods, not completed behavior. `Planned` means no modeled implementation yet. Task 16 must resolve every entry to actions, observation projection or a justified exclusion and executable witness. A generic invalid request currently tests transport/refusal wiring; it does not count as each mutation's validation coverage.

| Public API | Source | Status | Action / evidence |
| --- | --- | --- | --- |
| `Cluster.ReadBarrier` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.ReadTopology` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.ReadProgress` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.ReadAuthority` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.WaitRevoked` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.ReadLockHolder` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.Rules` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.IsLeader` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.Status` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.HealthObservation` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.Metrics` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.MetricsHistory` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.ParkedJoins` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.Latest` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.Target` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.Get` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.History` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.Watch` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.Stage` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.Plan` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.PlanFor` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.Approve` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.Operation` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.WaitOperation` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.RetryOperation` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.AddNode` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.ReadmitNode` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.DrainNode` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.ActivateNode` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.ExcludeNode` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.ForceRetireNode` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.SetPlacement` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.SetGroupPlacement` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.SetGroupVoterConfig` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.SetGroupHosting` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.SetReplicaPinnedLearner` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.CreateGroup` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.EnsureGroup` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.DeleteGroup` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.ChangeReplica` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.ReseatReplica` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.RollbackTo` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.TransferLeader` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.AcquireLock` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Cluster.LockHolder` | `cluster.go` | Planned | Pending owning plan task; no success stub |
| `Node.Start` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.WaitReady` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.Stop` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.RequestStop` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.Identity` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.Status` | `node.go` | Partial: phase projection only | ObserveLocal → invoke/deliver/handle; deliveredObservationTest |
| `Node.Watch` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.Cluster` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.Recovery` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.StartLoadGroup` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.LoadGroup` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.LoadGroupIDs` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.LoadSnapshots` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.StopLoadGroup` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.ParkedLoads` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.ReceiveLoadRaft` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.ReadLoadReplica` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.ApplyLoadAction` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.InstallLoadFence` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.ReportLoadDelivery` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.ActivateSession` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.KeepAlive` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.DeactivateSession` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.OwnSessionLive` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.ServeOverride` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.SetServeOverride` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.SchedulingStatus` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Node.SetSchedulingEnabled` | `node.go` | Planned | Pending owning plan task; no success stub |
| `Recovery.Inspect` | `node_recovery.go` | Planned | Pending owning plan task; no success stub |
| `Recovery.Approve` | `node_recovery.go` | Planned | Pending owning plan task; no success stub |
| `Recovery.Run` | `node_recovery.go` | Planned | Pending owning plan task; no success stub |
| `Recovery.Cancel` | `node_recovery.go` | Planned | Pending owning plan task; no success stub |
| `Recovery.Status` | `node_recovery.go` | Planned | Pending owning plan task; no success stub |
| `Recovery.List` | `node_recovery.go` | Planned | Pending owning plan task; no success stub |
| `NewNode` | `node.go` | Planned | Pending lifecycle/recovery tasks |
| `NewRecovery` | `node_recovery.go` | Planned | Pending lifecycle/recovery tasks |
| `RecoveryStartOptions` | `node_recovery.go` | Planned | Pending lifecycle/recovery tasks |

## State correspondence

| Record family | Source | Current treatment |
| --- | --- | --- |
| Durable/process identities and topology declarations | `topology/identity.go`, `topology/document.go`, `topology/nodes.go` | Typed vocabulary; foundation is unadmitted. |
| Storage class and hosting | `topology/storage_class.go`, `topology/hosting.go` | Default/memory and optional hosting; backend is node configuration. |
| Operations / planned steps | `topology/operation.go`, `topology/operator_steps.go` | Typed vocabulary; later tasks supply guards/transitions. |
| Grants / receipts / fences | `topology/authority_grant.go`, `topology/authority_receipt.go`, `topology/authority_fence.go` | Distinct typed records; no evidence produced yet. |
| Owner lifecycle | `node_lifecycle.go`, `node_load.go`, `internal/managed` | New/Ready fixture and crash/Starting restart; complete startup/teardown remains Task 7. |
| Native consensus / observations | `internal/raftengine`, `topology/status.go` | Exact native commit/election/catch-up and per-node application; membership changes arrive in Task 8. |
| Scheduler | `scheduling/scheduler.go`, `internal/scheduling` | Typed capture/decision; handlers absent (Task 11). |
| Sessions and locks | `topology/sessions.go`, `node_session.go` | Typed records; transitions absent (Task 12). |
| Recovery | `recovery/types.go`, `internal/recovery/model/types.go` | Typed phases/evidence; execution absent (Tasks 14–15). |

## Reachability and checks

`foundation::deliveredObservationTest` queues and delivers a local call through the shared Choreo handler. `invalidRequestRefusedTest` proves the call is unanswered before handling and returns Invalid without domain effects. `requestBoundTest` verifies that calls stop at the declared harness limit without reusing request identity. `assumptionsTest` executes domain premises. `safety` checks call structure and absence of authority/domain effects at this checkpoint; `deliveredRequest` counts sampled reachable replies.

## Naming and abstraction notes

The target's `action` record field is `kind` because Quint reserves `action`. Process phases follow NodeStatus; operation phases use an `Op` prefix where a constructor would otherwise collide. `PrepareReplica` corresponds to `StepPrepareReplica`, `WaitCatchUp` to `StepWaitCaughtUp`, `PromoteLearner` to `StepPromoteVoter`, `TransferLeadership` to `StepTransferLeader`, `RemoveMember` to `StepRemoveReplica`, and `RetireReplica` to `StepCleanupReplicaState`. These aliases do not yet imply that the effect protocol is implemented.

No claims are made about deadlock freedom, eventual progress, exhaustive exploration or Go conformance at this checkpoint. The exclusions and future handler tasks are listed in README.md.

## Consensus and environment evidence (Task 3)

The smoke3 instance runs the four common tests plus twelve membership/environment tests from `membership_test.qnt`. The initializer represents an established approved/proven cluster, not a successful create/admit API. `commitProgress`, `elect`, `catchUp` and `applyMeta` model the trusted Raft boundary; `crash`, `restart`, `setLink`, `drop` and `duplicate` model the fault environment. No application payloads or Raft wire internals are implemented.

`metaFailureDoesNotStopLoadTest`, `loadFailureDoesNotStopMetaTest`, `commitBeforeLocalApplyTest`, `majorityLossDespiteHealthyCacheTest`, `loadMajorityLossTest`, `staleLeaderObservationTest`, `retainedRestartCatchUpTest`, `oldIncarnationMessageTest`, `duplicateDeliveryTest`, `droppedRequestIsNotReplyTest` and `learnerDoesNotVoteTest` demonstrate their named conditions. `membershipFixtureTest` checks the concrete scenario assumptions. Pure learner-certificate input is explicitly adversarial; ordinary scenarios use composed actions.

The native-service helpers may access global consensus facts as the abstract environment; Choreo handlers only inspect delivered messages and their own local state. No handler consults shared current topology to simulate a fresh read. The target's durable host schema is refined with metaImage/metaIndex for crash-recovered local application and engineRunning for participation independently of handle publication.
