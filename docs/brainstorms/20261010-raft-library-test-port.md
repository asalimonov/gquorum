# Required source test port inventory

Date: 2026-10-10. Source baseline: `fc982c0`. Status: implementation in progress; stages 1–6 have verified unit/function ports. Process-cluster ports remain outstanding.

## Scope and acceptance

Port every applicable source cluster scenario that proves Raft correctness, including supporting unit tests and all table-driven cases/subtests within retained source functions. Larger-cluster validation is mandatory main scope. This inventory supplements the target specification; it does not narrow the user requirement to a smoke suite.

Enumerated all 42 Go test files recursively under `tests/cluster`, with 150 test functions excluding TestMain. 60 scenarios require adaptation to the new library/test application, 89 exercise application-only or old-store-migration behavior, and 1 exercises the explicitly deferred force-new feature. Mixed scenarios retain their applicable Raft portions. TestMain is harness wiring and must be replaced, not counted as a test.

Also enumerated supporting source test symbols under topology/meta, membership, load runtime/storage, transport, local events, discovery/identity and relevant governance integration seams. Source paths and symbols below are traceability identifiers; they must not become imports or product references in the extracted library.

Implementation must record a target test name and result for every required scenario. Do not silently omit cases, weaken assertions to successful HTTP responses, copy application-only features into the library, or preserve skip gates that leave the required scale/recovery cases unexecuted. Source old-format migrations and force-new exceptions do not excuse ordinary disk recovery or fresh-ID readmission.

## Complete process-cluster inventory

Paths in the following tables are relative to the source checkout. Target paths are in the new library. ADAPT means required port of the applicable behavior; APPLICATION means the stated domain feature is not part of the library; FORCE-NEW means the user previously deferred that specific recovery mechanism.

### `tests/cluster/admission_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestShardAdmission_OverloadShedsWithoutOrphans` | ADAPT → `tests/cluster/load_test.go` | Replace reservation holders/admission policy with generic pending proposals and an injected writer stall. Prove bounded overload, no leaked waiters, continued apply and recovery. Reservation-specific admission exemptions are removed. |
| `TestShardAdmission_LargePendingSetDoesNotSlowApply` | ADAPT → `tests/cluster/load_test.go` | Replace reservation holders/admission policy with generic pending proposals and an injected writer stall. Prove bounded overload, no leaked waiters, continued apply and recovery. Reservation-specific admission exemptions are removed. |

### `tests/cluster/authz_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestAuthzGrantEnforceRevoke` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestAuthzBudgetMode` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestAuthzCustomPrices` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestAuthzAuditLog` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/autorepair_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestAutoRepair_DataLostReturn` | ADAPT → `tests/cluster/{identity,topology}_test.go` | Preserve fresh identity after leader data loss, operator-stage protection and address-free documents; inject discovery through caller resolver. |
| `TestAutoRepair_StagedRepairRequiresOperatorApply` | ADAPT → `tests/cluster/{identity,topology}_test.go` | Preserve fresh identity after leader data loss, operator-stage protection and address-free documents; inject discovery through caller resolver. |
| `TestTopology_AddressFreeRouting` | ADAPT → `tests/cluster/{identity,topology}_test.go` | Preserve fresh identity after leader data loss, operator-stage protection and address-free documents; inject discovery through caller resolver. |

### `tests/cluster/budget_disabledreservation_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestReservation_Disabled_PreCheckEffectivelyNever` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestReservation_Disabled_RejectionOnlyAfterSettlement` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestReservation_Disabled_FinalizeSettlesActualCost` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/budget_flatreservation_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestReservation_Flat_AcceptsManyBeforeSettlement` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestReservation_Flat_Resolver402AfterCap` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestReservation_Flat_ConcurrentCallsAllAccepted` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestReservation_Flat_ModelOverrideWorstCase` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/budget_sentinel_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestBudget_Sentinel_UnknownModel` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestBudget_Sentinel_NoBudgetSilent` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/budget_spend_matrix_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestBudgetSpendMatrix` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/budget_structural_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestBudget_Structural` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/budget_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestBudget_EnforcesVKLimit` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestBudget_EnforcesTeamLimit` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/cluster_api_forward_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestCluster_APICrossNodeForwarding` | ADAPT → `tests/cluster/meta_test.go` | Port one-hop meta leader forwarding and cross-node result/refusal propagation; use caller test ingress for load routing. |
| `TestAuthzForward_RequiresGlobalClusterRead` | APPLICATION | Tests the application permission model. Meta credentials/cluster-ID checks and exact effect authorization remain required. |

### `tests/cluster/cluster_api_task18_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestClusterAPI_StagedTopologyIsInert` | ADAPT → `tests/cluster/{membership,execution,scale}_test.go` | Retain the exact membership/execution assertions; replace application traffic and SQL rows with acknowledged Machine commands and controller-owned cleanup records. |
| `TestClusterAPI_RuntimeGrowLearnerFirst` | ADAPT → `tests/cluster/{membership,execution,scale}_test.go` | Retain the exact membership/execution assertions; replace application traffic and SQL rows with acknowledged Machine commands and controller-owned cleanup records. |
| `TestClusterAPI_MetaJoinLearnerFirst` | ADAPT → `tests/cluster/{membership,execution,scale}_test.go` | Retain the exact membership/execution assertions; replace application traffic and SQL rows with acknowledged Machine commands and controller-owned cleanup records. |
| `TestClusterAPI_RemoteApplyFromNonHostMetaLeader` | ADAPT → `tests/cluster/{membership,execution,scale}_test.go` | Retain the exact membership/execution assertions; replace application traffic and SQL rows with acknowledged Machine commands and controller-owned cleanup records. |
| `TestClusterAPI_DrainChecksActualMetaQuorum` | ADAPT → `tests/cluster/{membership,execution,scale}_test.go` | Retain the exact membership/execution assertions; replace application traffic and SQL rows with acknowledged Machine commands and controller-owned cleanup records. |
| `TestClusterAPI_ForceRetireDeadNode` | ADAPT → `tests/cluster/{membership,execution,scale}_test.go` | Retain the exact membership/execution assertions; replace application traffic and SQL rows with acknowledged Machine commands and controller-owned cleanup records. |
| `TestClusterAPI_ExecutionResumesAfterMetaLeaderChange` | ADAPT → `tests/cluster/{membership,execution,scale}_test.go` | Retain the exact membership/execution assertions; replace application traffic and SQL rows with acknowledged Machine commands and controller-owned cleanup records. |
| `TestClusterAPI_CompactedLearnerCatchUp` | ADAPT → `tests/cluster/{membership,execution,scale}_test.go` | Retain the exact membership/execution assertions; replace application traffic and SQL rows with acknowledged Machine commands and controller-owned cleanup records. |
| `TestClusterAPI_IntentionalLearnerNotPromoted` | ADAPT → `tests/cluster/{membership,execution,scale}_test.go` | Retain the exact membership/execution assertions; replace application traffic and SQL rows with acknowledged Machine commands and controller-owned cleanup records. |
| `TestClusterAPI_DeleteShardWaitsForAllHosts` | ADAPT → `tests/cluster/{membership,execution,scale}_test.go` | Retain the exact membership/execution assertions; replace application traffic and SQL rows with acknowledged Machine commands and controller-owned cleanup records. |
| `TestClusterAPI_FiveNodeMultiShardControllerShutdown` | ADAPT → `tests/cluster/{membership,execution,scale}_test.go` | Retain the exact membership/execution assertions; replace application traffic and SQL rows with acknowledged Machine commands and controller-owned cleanup records. |

### `tests/cluster/cluster_api_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestCluster_APIStatusReadsAndGuards` | ADAPT → `tests/cluster/topology_test.go` | Keep view/status/version guards, node lifecycle, placement/leader transfer and forwarding from a non-leader. HTTP and permissions are supplied by the test application; assert typed library errors and exact execution state. |
| `TestCluster_APINodeLifecycle` | ADAPT → `tests/cluster/topology_test.go` | Keep view/status/version guards, node lifecycle, placement/leader transfer and forwarding from a non-leader. HTTP and permissions are supplied by the test application; assert typed library errors and exact execution state. |
| `TestCluster_APIPlacementAndTransferLeader` | ADAPT → `tests/cluster/topology_test.go` | Keep view/status/version guards, node lifecycle, placement/leader transfer and forwarding from a non-leader. HTTP and permissions are supplied by the test application; assert typed library errors and exact execution state. |
| `TestCluster_APIShardCountersCountOrphanVKs` | APPLICATION | Tests application-specific entity counters/SQL views. |
| `TestCluster_DrainViaNonLeader` | ADAPT → `tests/cluster/topology_test.go` | Keep view/status/version guards, node lifecycle, placement/leader transfer and forwarding from a non-leader. HTTP and permissions are supplied by the test application; assert typed library errors and exact execution state. |

### `tests/cluster/cluster_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestCluster_HealthEndpoint` | ADAPT → `tests/cluster/meta_test.go` | Keep real node liveness/status, persisted restart and converged local observations. Assert each node identity, applied cursor and group state instead of application payloads. |
| `TestCluster_ConfigPropagation` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestCluster_VirtualKeySync` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestCluster_NodeRestart` | ADAPT → `tests/cluster/meta_test.go` | Keep real node liveness/status, persisted restart and converged local observations. Assert each node identity, applied cursor and group state instead of application payloads. |
| `TestCluster_ConcurrentConfigWrites` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestCluster_StatusEndpoint` | ADAPT → `tests/cluster/meta_test.go` | Keep real node liveness/status, persisted restart and converged local observations. Assert each node identity, applied cursor and group state instead of application payloads. |
| `TestCluster_StatusEndpoint_AllNodesConsistent` | ADAPT → `tests/cluster/meta_test.go` | Keep real node liveness/status, persisted restart and converged local observations. Assert each node identity, applied cursor and group state instead of application payloads. |

### `tests/cluster/configmerge/configmerge_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestMergeReservationConfig_Unit` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/dbview_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestDBView_OrphanVK_InShardBudgetsView` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestDBView_OrphanVK_InRateLimitsView` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestDBView_LegacyTeamRootedVK` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/durability_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestDurability_RecoveryFromPG` | ADAPT → `tests/cluster/recovery_test.go` | Use exact full Machine state and local WAL recovery, not PG projections. Replace PG freeze/blip with injected durable writer stall and unrelated caller-store outage; verify meta independence, typed availability, no halt from transient stall and continued commits after recovery. |
| `TestDurability_WALPostgresStall` | ADAPT → `tests/cluster/recovery_test.go` | Use exact full Machine state and local WAL recovery, not PG projections. Replace PG freeze/blip with injected durable writer stall and unrelated caller-store outage; verify meta independence, typed availability, no halt from transient stall and continued commits after recovery. |
| `TestDurability_PostgresBlipDoesNotShed` | ADAPT → `tests/cluster/recovery_test.go` | Use exact full Machine state and local WAL recovery, not PG projections. Replace PG freeze/blip with injected durable writer stall and unrelated caller-store outage; verify meta independence, typed availability, no halt from transient stall and continued commits after recovery. |

### `tests/cluster/follower_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestFollower_ProcessesLocallyWithoutForwarding` | APPLICATION | Tests local inference processing with application-specific budget prechecks; generic follower proposal and leader-routing scenarios are ported from routing/runtime tests. |
| `TestFollower_BudgetAccountingCorrect` | ADAPT → `tests/cluster/load_test.go` | Replace budget totals with exact generic command-ID/value state observed on every replica. Verify results converge and state does not grow without additional commands. |
| `TestFollower_BudgetUsageMatchesCostAcrossNodes` | ADAPT → `tests/cluster/load_test.go` | Replace budget totals with exact generic command-ID/value state observed on every replica. Verify results converge and state does not grow without additional commands. |
| `TestFollower_PreCheckOnFollowerRejectsBudgetExceeded` | APPLICATION | Tests application budget precheck policy, not Raft membership/commit behavior. |

### `tests/cluster/founding_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestCluster_GreenfieldFoundingFullTopology` | ADAPT → `tests/cluster/founding_test.go` | Retain full founding, delayed member, bounded founding timeout and existing-cluster startup with an absent configured peer. Replace the SQL legacy-import fixture with persisted library state; old-format import is not added. |
| `TestCluster_GreenfieldFoundingDelayedMember` | ADAPT → `tests/cluster/founding_test.go` | Retain full founding, delayed member, bounded founding timeout and existing-cluster startup with an absent configured peer. Replace the SQL legacy-import fixture with persisted library state; old-format import is not added. |
| `TestCluster_GreenfieldFoundingTimeoutExit` | ADAPT → `tests/cluster/founding_test.go` | Retain full founding, delayed member, bounded founding timeout and existing-cluster startup with an absent configured peer. Replace the SQL legacy-import fixture with persisted library state; old-format import is not added. |
| `TestCluster_GreenfieldLegacyImportNotGated` | ADAPT → `tests/cluster/founding_test.go` | Retain full founding, delayed member, bounded founding timeout and existing-cluster startup with an absent configured peer. Replace the SQL legacy-import fixture with persisted library state; old-format import is not added. |

### `tests/cluster/httpplugin_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestHTTPPlugin_RegisteredOnceReachableFromEveryNode` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/identity_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestIdentity_HealthAndStatusSurfaces` | ADAPT → `tests/cluster/identity_test.go` | Check exact runtime identity/session health and minority serving observation; test caller-defined serving policy and identity-checked local override. Override must not create commit quorum. |
| `TestIdentity_ServingGate_MinorityAndOverride` | ADAPT → `tests/cluster/identity_test.go` | Check exact runtime identity/session health and minority serving observation; test caller-defined serving policy and identity-checked local override. Override must not create commit quorum. |

### `tests/cluster/main_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestMain` | Harness replacement | Build/start the local test application, run required cluster tests, collect diagnostics and clean all isolated resources. |

### `tests/cluster/migration_atomicity_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestMigration_ConcurrentReadDuringViewRecreate` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/migration_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestShard_MigrateCustomer_SpendLogging` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestShard_MigrateCustomer_InFlightInference` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestShard_MigrateCustomer_NoOverspendRetriable` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestShard_MigrateCustomer_CustomerBudgetMoves` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestShard_MigrateCustomer_DirectVKs` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestShard_MigrateCustomer_CrossCustomerNotFrozen` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestShard_MigrateCustomer_LeavesNoStagedTopology` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestShard_MigrateComm01_Rejected` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestShard_MigrateAbort` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestShard_MutationDuringMigration` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/passthrough_billing_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestPassthrough_StreamingBillsNonZeroAcrossCluster` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/passthrough_cancel_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestPassthrough_CancelChargesReserved_Default` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestPassthrough_CancelRelease_NoCharge` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/performance_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestPerf_GossipConvergenceTiming` | APPLICATION | Tests memberlist gossip convergence rather than Raft; no gossip implementation is requested. |
| `TestPerf_ConfigPropagationLatency` | APPLICATION | Tests SQL-backed gossip invalidation, rather than Raft commit latency. Meta/load convergence measurements are required separately. |
| `TestPerf_ConcurrentHealthChecks` | ADAPT → `tests/cluster/scale_test.go` | Adapt to meta CAS and unique load commands, concurrent status reads and repeated isolated process restarts. Verify all acknowledged writes after convergence, nonempty workload, configured deadlines and resource cleanup; do not copy SQL/gossip performance thresholds. |
| `TestPerf_ConcurrentConfigWrites` | ADAPT → `tests/cluster/scale_test.go` | Adapt to meta CAS and unique load commands, concurrent status reads and repeated isolated process restarts. Verify all acknowledged writes after convergence, nonempty workload, configured deadlines and resource cleanup; do not copy SQL/gossip performance thresholds. |
| `TestStability_NodeFailureRecovery` | ADAPT → `tests/cluster/scale_test.go` | Adapt to meta CAS and unique load commands, concurrent status reads and repeated isolated process restarts. Verify all acknowledged writes after convergence, nonempty workload, configured deadlines and resource cleanup; do not copy SQL/gossip performance thresholds. |
| `TestStability_DataConsistencyUnderLoad` | ADAPT → `tests/cluster/scale_test.go` | Adapt to meta CAS and unique load commands, concurrent status reads and repeated isolated process restarts. Verify all acknowledged writes after convergence, nonempty workload, configured deadlines and resource cleanup; do not copy SQL/gossip performance thresholds. |
| `TestStability_HealthEndpointReliability` | ADAPT → `tests/cluster/scale_test.go` | Adapt to meta CAS and unique load commands, concurrent status reads and repeated isolated process restarts. Verify all acknowledged writes after convergence, nonempty workload, configured deadlines and resource cleanup; do not copy SQL/gossip performance thresholds. |

### `tests/cluster/promote_rejoin_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestPromoteSingleSurvivorAndRejoin` | FORCE-NEW | Force-new after permanent majority loss was explicitly deferred by the user. Normal minority refusal and reconnect recovery remain mandatory separate tests. |

### `tests/cluster/pubsub_budget_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestPubSub_VKBudgetVisibleInRaftQuickly` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestPubSub_VKBudgetDeleteRemovesFromRaft` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestPubSub_VKBudgetSwapDeletesOldAndAddsNew` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestPubSub_TeamBudgetVisibleInRaftQuickly` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestPubSub_CustomerBudgetVisibleInRaftQuickly` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestPubSub_ProviderBudgetVisibleInRaftQuickly` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestPubSub_ModelConfigBudgetVisibleInRaftQuickly` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestPubSub_BudgetSyncerBackfillsWhenEventLost` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/ratelimit_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestRateLimit_JointEnforcementAcrossNodes` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestRateLimit_DumperPersistsUsage` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/recon_stage2_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestRecon2_PluginConfigOnPeers` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestRecon2_RoutingRuleDeleteOnPeers` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestRecon2_PricingOverrideOnPeers` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestRecon2_WebhookEndpointOnPeers` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/recon_stage3_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestRecon3_VKDeleteOnPeers` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestRecon3_CustomerDeleteOnPeers` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestRecon3_TeamBudgetOnPeers` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestRecon3_ModelConfigOnPeers` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestRecon3_FromMemoryReadsOnPeers` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestRecon3_ReservationUsesOverride` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestRecon3_MultiShardPlacement` | ADAPT → `tests/cluster/topology_test.go` | Port the multi-group hosting/routing portion using the test Machine and typed eligibility. Remove application cache/configuration propagation assertions. |
| `TestRecon3_ReloadAllDropsMissedDelete` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/recon_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestRecon_ProviderKeyVisibleOnPeers` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestRecon_ClientConfigOnPeers` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestRecon_PricingAttributesOnPeers` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestRecon_MetricsExposed` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/rejoin_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestRejoin_ExcludedSameDiskRestartParksAndReadmits` | ADAPT → `tests/cluster/identity_test.go` | Preserve disk/lineage checks, parking, fresh member IDs, load replica retirement, current-voter recovery and duplicate-name rejection through caller readmission hooks. |
| `TestRejoin_ReaddedWhileDownRejoinsUnderTheFreshIdentity` | ADAPT → `tests/cluster/identity_test.go` | Preserve disk/lineage checks, parking, fresh member IDs, load replica retirement, current-voter recovery and duplicate-name rejection through caller readmission hooks. |
| `TestRejoin_CurrentVoterRestartKeepsItsStore` | ADAPT → `tests/cluster/identity_test.go` | Preserve disk/lineage checks, parking, fresh member IDs, load replica retirement, current-voter recovery and duplicate-name rejection through caller readmission hooks. |
| `TestRejoin_DuplicateNameIntruderIsRefused` | ADAPT → `tests/cluster/identity_test.go` | Preserve disk/lineage checks, parking, fresh member IDs, load replica retirement, current-voter recovery and duplicate-name rejection through caller readmission hooks. |

### `tests/cluster/reservation_reconfigure_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestCluster_ReservationRollingReconfigure` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/routing_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestRouting_ForwardsToLeader` | ADAPT → `tests/cluster/load_test.go` | Use the caller test load ingress to submit opaque commands from every node/follower and verify leader routing, acknowledgements and replica convergence. This does not introduce a library load protocol. |
| `TestRouting_AllNodesAcceptRequests` | ADAPT → `tests/cluster/load_test.go` | Use the caller test load ingress to submit opaque commands from every node/follower and verify leader routing, acknowledgements and replica convergence. This does not introduce a library load protocol. |

### `tests/cluster/shard_ops_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestShardOps_MigratedShardStillDeletable` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestShardOps_DeleteBlockedByActiveMigration` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestShardOps_CustomerDeleteFreesShard` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestShardOps_StuckMigrationRetry` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestShardOps_ForceAbortReleasesFence` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestShardOps_StuckVisibleBetweenSweeps` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestShardOps_OrphanBudgetsReclaimed` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestShardOps_GCSkippedDuringMigration` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestShardOps_GCNeverTouchesProviderModel` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestShardOps_MigrationRetentionPrunes` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestShardOps_OrphanVKCounted` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestShardOps_OrphanedShardVisible` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/shard_provider_model_budget_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestShard_ProviderBudget_EnforcedOnShard0` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |
| `TestShard_ProviderModelBudget_MultiShardIndependence` | APPLICATION | Application budget/reservation/rate-limit/billing, customer migration, SQL view, cache/pub-sub configuration propagation, plugin or permission behavior; no generic Raft contract is asserted by this case. Applicable consensus, durability and topology portions are covered by the required ports listed separately. |

### `tests/cluster/shard_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestShard_CRUD_ListCreateDelete` | ADAPT → `tests/cluster/scale_test.go` | Port group declaration/approval/runtime lifecycle and create/delete propagation, including fifteen groups. Reserved group guard is for _meta; remove source default workload-group special treatment. Verify load commits and acknowledged cleanup before absence. |
| `TestShard_DeleteNonEmpty_Rejected` | ADAPT → `tests/cluster/execution_test.go` | Replace customer occupancy with a caller cleanup refusal. Verify durable execution reports the error and does not complete deletion before authorized caller effects succeed; the library does not define application occupancy. |
| `TestShard_MultiShard_CostLogging` | ADAPT → `tests/cluster/load_test.go` | Replace billing/SQL with exact commands in distinct load groups, proving routing, independent state and successful commits in a nondefault group. |
| `TestShard_OnlyCustomersBindToShards` | APPLICATION | Tests customer/team/key SQL assignment schema; caller application owns workload assignment. |
| `TestShard_DeleteAfterCustomerDeleted` | APPLICATION | Tests application entity deletion and SQL occupancy cleanup; group cleanup ordering and refusal are ported independently. |
| `TestShard_ScaleTo15` | ADAPT → `tests/cluster/scale_test.go` | Port group declaration/approval/runtime lifecycle and create/delete propagation, including fifteen groups. Reserved group guard is for _meta; remove source default workload-group special treatment. Verify load commits and acknowledged cleanup before absence. |
| `TestShard_PeerPropagation` | ADAPT → `tests/cluster/scale_test.go` | Port group declaration/approval/runtime lifecycle and create/delete propagation, including fifteen groups. Reserved group guard is for _meta; remove source default workload-group special treatment. Verify load commits and acknowledged cleanup before absence. |

### `tests/cluster/topology_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestTopology_FoundingAndLegacyImport` | ADAPT → `tests/cluster/topology_test.go` | Retain founding, drain/restore, actual quorum/readiness and token checks. Supply initial groups directly rather than importing SQL; meta membership survives drain. |
| `TestTopology_DrainPlanApplyRestore` | ADAPT → `tests/cluster/topology_test.go` | Retain founding, drain/restore, actual quorum/readiness and token checks. Supply initial groups directly rather than importing SQL; meta membership survives drain. |
| `TestTopology_StoppedFollowerHealthAndReadiness` | ADAPT → `tests/cluster/topology_test.go` | Retain founding, drain/restore, actual quorum/readiness and token checks. Supply initial groups directly rather than importing SQL; meta membership survives drain. |
| `TestTopology_ClusterTokenEnabled` | ADAPT → `tests/cluster/topology_test.go` | Retain founding, drain/restore, actual quorum/readiness and token checks. Supply initial groups directly rather than importing SQL; meta membership survives drain. |

### `tests/cluster/transport_deadpeer_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestTransport_DeadPeerDoesNotDisturbLeadership` | ADAPT → `tests/cluster/transport_test.go` | Prove dead peer isolation, continued healthy-peer traffic and unchanged meta leadership using actual meta protocol and test caller load transport. |

### `tests/cluster/wal_coalescing_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestCluster_BudgetSpendExactUnderWALCoalescing` | ADAPT → `tests/cluster/load_test.go` | Run identical unique-command workloads with coalescing off/on. Compare exact replicated results and per-replica entries; show larger persistence batches without changing command count/order or losing acknowledgements. |
| `TestCluster_SharedTablesSeedContinuity` | APPLICATION | Tests migration from deprecated shared PG WAL tables; in-place source data migration is outside the requested new-store extraction. |

### `tests/cluster/wal_store_test.go`

| Source test | Disposition / target | Required adaptation or reason |
| --- | --- | --- |
| `TestShard_SharedTablesSeedToWAL` | APPLICATION | Tests SQL legacy-to-WAL migration. Current-format WAL restart, boot identity ledger and full snapshot recovery remain required. |
| `TestShard_ParkedReplicaReseat` | ADAPT → `tests/cluster/recovery_test.go` | Prove a previously voted replica with missing local WAL parks, receives a new monotonic replica ID through explicit reseat, catches up and resumes commits; inspect the recovering node itself. |

## Supporting tests required by the extraction

The source symbols below make the smaller correctness tests discoverable alongside the process suite. File-level rules identify how application coupling is removed; applicable safety assertions remain required even when test bodies are rewritten.

### Topology and exact execution — `framework/cluster/topology`

Target: `topology/*_test.go and tests/cluster/topology_test.go`. Port generic document, guardrail, health, plan, CAS, drain, execution, reconciliation, readmission and watcher assertions. Remove source license and application counter expectations; adapt bound checks to Limits and node eligibility to caller types. Pause/force-new features remain unsupported as already specified.

- `cluster_health_test.go` (3 cases): `TestClusterStatus_HealthSerializationUnknown`, `TestClusterStatus_HealthSerializationKnown`, `TestClusterStatus_NoHealthKnownField`
- `contracts_test.go` (5 cases): `TestExecutionErrorsSupportClassification`, `TestExecutionStateClassification`, `TestAuthorizedStepMatchesExactScope`, `TestStepJSONUsesGroupAndNodeNames`, `TestExecutionIDAttemptRoundTrip`
- `convergence_test.go` (21 cases): `TestConvergence_LearnerFirstSteps`, `TestConvergence_FinalValidatorFailureLeavesActiveUnchanged`, `TestConvergence_ForcedRetirementWaivesOnlyDeadNodeTeardown`, `TestConvergence_DestructiveAndLifecycleSteps`, `TestConvergence_DeleteShardStopsEveryHostBeforeSharedDelete`, `TestConvergence_DeleteShardWaitsForUnreachableHost`, `TestConvergence_DeleteShardCompletesAfterGroupLosesLeader`, `TestConvergence_DeleteShardContinuesAfterGroupBecomesAbsent`, `TestConvergence_FoundingDemotionTransferAndDelete`, `TestConvergence_WaitFailureOwnershipAndCancellation`, `TestConvergence_EmptyPlanAndActivationFailure`, `TestApply_BeginsSignalsAndRetriesFailedAttempt`, `TestConvergence_MetricsAreBounded`, `TestConvergence_FailoverResumesExactStep`, `TestConvergence_SupersedesOnlyAtStepBoundary`, `TestConvergence_ResumePreservesDurableWaiting`, `TestConvergence_ResumeMixedPlaneOwnership`, `TestConvergence_ShutdownCancelsBlockedWorkAndIsIdempotent`, `TestConvergence_ShutdownReusesBoundedRetryTimer`, `TestConvergence_ResumeSerializesConcurrentTicks`, `TestConvergence_TickSurvivesLostProposal`
- `discovered_test.go` (12 cases): `TestDiscoveredNodes_GossipOnlyNew`, `TestDiscoveredNodes_GossipOnlyExcludedReturning`, `TestDiscoveredNodes_ParkedOnly`, `TestDiscoveredNodes_GossipAndParkedMerge`, `TestDiscoveredNodes_PlacedNodesNeverDiscovered`, `TestDiscoveredNodes_ExcludedAndDeadIsAbsent`, `TestDiscoveredNodes_ParkExcludedVersionIsAuthoritative`, `TestDiscoveredNodes_SortedByName`, `TestDiscoveredNodes_EmptyInputs`, `TestDiscoveredNodes_DuplicateNameKeepsNewestPark`, `TestDiscoveredNodes_ParkedNilInfoTolerated`, `TestClusterStatus_DiscoveredSerialization`
- `document_test.go` (15 cases): `TestDocumentLookupHelpers`, `TestDocumentCloneIsolatesForcedChangeAudit`, `TestDocumentValidateRejectsIncompleteForcedAudit`, `TestVotersOf`, `TestQuorumOf`, `TestDocumentCloneIsolation`, `TestAllocateMemberID`, `TestAllocateMemberIDBackfillsFromNodes`, `TestDocumentCloneCopiesNextMemberID`, `TestGroupBounds`, `TestDocumentCloneIsolatesReferenceFields`, `TestDocumentCloneNilSlices`, `TestEffectiveNodeState`, `TestEffectiveNodeStateDoesNotMutate`, `TestOriginInternal_IsNotTheDefault`
- `drain_safety_test.go` (4 cases): `TestMetaDrainSafetyActualQuorumSizes`, `TestMetaDrainSafetyFailsClosed`, `TestValidateFinalTargetRechecksLeaderAndHealth`, `TestDrainApplyRetryAfterHealthRecoversCreatesNextAttempt`
- `executor_forward_test.go` (15 cases): `TestApply_ForwardsToMetaLeader`, `TestApply_ForwardLeaderUnknown`, `TestApply_ForwardLeaderUnmapped`, `TestApply_ForwardAddressUnresolved`, `TestApply_ForwardTransportError`, `TestApply_ForwardPreservesLeaderRefusalClass`, `TestRefusalKindRoundTrip`, `TestApply_ForwardEmptyExecutionID`, `TestApply_NoForwarderKeepsLegacy421`, `TestApply_NoResolverCannotForward`, `TestApplyLocal_NeverReForwards`, `TestApplyLocal_LeaderRunsLocally`, `TestApply_LeaderDoesNotForward`, `TestOwnerOfMetaMember`, `TestOwnerOfMetaMember_AmbiguousRefuses`
- `executor_test.go` (15 cases): `TestPlan_DryRunGrow`, `TestPlan_EmptyWhenMatched`, `TestPlan_UsesLiveMetaConfStateAheadOfActive`, `TestPlan_RequiresCommander`, `TestApply_FullExecution`, `TestApply_SupersededVersionRefused`, `TestApply_NotMetaLeader`, `TestApply_NoCoordinator`, `TestApply_ReturnsBeforeConvergenceFailure`, `TestApply_NonterminalAttemptIsIdempotent`, `TestApply_AlreadySucceeded`, `TestApply_CreateAndDeleteShard`, `TestExecution_Unknown`, `TestExecution_NoCoordinator`, `TestExecution_MalformedIDsAreRejected`
- `list_shards_test.go` (1 cases): `TestListShards`
- `manager_confver_test.go` (11 cases): `TestBumpGroupConfVer_MirrorsAndSkips`, `TestBumpGroupConfVer_NoOpCommitsNothing`, `TestBumpGroupConfVer_StampsInternalOriginAndApplies`, `TestBumpGroupConfVer_ApplyFailureReturnsCommittedDoc`, `TestBumpGroupConfVer_NonEmptyPlanDefersApply`, `TestBumpGroupConfVer_LaunderedOperatorIntentDefersApply`, `TestBumpGroupConfVer_StagedNonMembershipIntentDefersApply`, `TestBumpGroupConfVer_DryRunSkipsApply`, `TestBumpGroupConfVer_NoCoordinatorSkipsApply`, `TestBumpGroupConfVer_ApplySurvivesCallerCancellation`, `TestBumpGroupConfVer_ConcurrentMirrorCommitsNothing`
- `manager_reseat_test.go` (7 cases): `TestReseatReplica_AllocatesFreshIDAndCommitsPlan`, `TestReseatReplica_RefusedWhileViewsDiffer`, `TestReseatReplica_RefusedWhileExecutionRuns`, `TestReseatReplica_UndeclaredReplica`, `TestReseatReplica_MetaGroupRefused`, `TestReseatReplica_NoCoordinator`, `TestBuildReplicaRepair_SameMutationForBothOrigins`
- `manager_test.go` (103 cases): `TestAddNodeHappyPath`, `TestAddNodeSeedsRegionFromGossip`, `TestAddNodeRegionUnknown`, `TestAddNodeReseedsRegionOnReadmission`, `TestAddNodeClampedByMaxReplicas`, `TestAddNodeLicenseRefused`, `TestAddNodeRefusals`, `TestAddNodeReplacesExcludedNodeWithFreshIdentity`, `TestAddNodeMemberIDUniqueAcrossHistory`, `TestDrainNodeHappyPath`, `TestActivateNodeReversesDrain`, `TestActivateNodeIdempotent`, `TestActivateNodeRefusals`, `TestActivateNodeLicense`, `TestNodeLifecycleStamps`, `TestDrainNodeBelowMinRefused`, `TestDrainNodeQuorumSafety`, `TestDrainNodeStateRefusals`, `TestDrainNodeRefusalIsActionable`, `TestDrainNodeRefusalSurvivesActiveReadFailure`, `TestRemoveNodeHappyPath`, `TestRemoveNodeCancelsStagedOnlyAdmission`, `TestChangeReplica_Promote`, `TestChangeReplicaLeavesRuntimeRepairToPlan`, `TestChangeReplica_Demote`, `TestPlacementInvariant_ClampsOnVotersNotReplicas`, `TestPlacementInvariant_StopsAtMaxVoters`, `TestGroupUpdatedAt_StampedOnReplicaSetChange`, `TestGroupConfigChanged_CloneIsIdentity`, `TestChangeReplica_DemoteQuorumSafety`, `TestChangeReplica_Errors`, `TestChangeReplica_MetaDemotionUsesSafePlan`, `TestChangeReplica_IntentionalLearnerStaysLearnerWhenLiveMatches`, `TestRemoveNode_RemovesMetaVoterDeclaratively`, `TestRemoveNode_MetaQuorumSafety`, `TestDrainNode_StagesWithoutTransferringMetaLeader`, `TestDrainNode_NonMetaLeaderNoTransfer`, `TestDrainNode_MetaLeaderCanBeStagedOffLeader`, `TestDrainNode_MetaLeaderDryRunStagesOffLeader`, `TestDrainNode_RefusalMovesNoMetaLeadership`, `TestDryRunDrainNode_StagesButPlanRefusesUnevacuableMetaLeader`, `TestRemoveNodeRefusals`, `TestForceRetireNodeRequiresReasonAndActor`, `TestForceRetireNodeAuditsAndAllocatesFreshReplacement`, `TestForceRetireNodeImpossibleBoundsDoesNotCommit`, `TestSetPlacementGrowsShardsTowardMax`, `TestSetPlacementLicense`, `TestSetPlacementRefusals`, `TestSetPlacement_QuorumSafety`, `TestSetPlacement_QuorumSafetyAllowsHealthyGrow`, `TestAddNode_AllowsDeclareBeforeBoot`, `TestAssociateNodeHappyPath`, `TestAssociateNodeRestoresDrainedNode`, `TestAssociateNodeLearnerNotImplemented`, `TestAssociateNodeLicenseRefused`, `TestAssociateNodeRefusals`, `TestAssociateNode_QuorumSafety`, `TestAssociateNode_QuorumSafetyAllowsHealthyGrow`, `TestDisassociateNodeHappyPath`, `TestDisassociateNodeRefusals`, `TestDisassociateNodeQuorumSafety`, `TestCreateShardHappyPath`, `TestCreateShardLicense`, `TestCreateShardRefusals`, `TestCreateShard_RefusesUnaddressableIDs`, `TestCreateShard_QuorumSafety`, `TestCreateShard_QuorumSafetyAllowsHealthyCluster`, `TestDeleteShard`, `TestRollbackLineage`, `TestRollbackRefusals`, `TestRollbackHorizonPruned`, `TestRollbackDoesNotRegressIDCounters`, `TestRollbackRefusesResurrectedIdentity`, `TestRollbackAllowsMembershipRewind`, `TestRollbackRefusesQuorumUnsafeRestore`, `TestRollbackAllowsQuorumSafeRestore`, `TestProposeErrorPropagation`, `TestCASConflictFromConcurrentCommit`, `TestTransferLeaderDelegates`, `TestTransferLeaderUsesGlobalControllerWithActiveScope`, `TestTransferLeaderRejectsIneligibleTarget`, `TestTransferLeaderHealthUnknownAdmits`, `TestPlanDrainMetaLeaderSelectsKnownAliveTransferTarget`, `TestPlanDrainMetaLeaderRefusesWhenAllPeersDead`, `TestTransferLeaderMetaUsesCoordinator`, `TestTransferLeaderMetaNoCoordinator`, `TestTransferLeaderRefusals`, `TestClusterStatusWithHealth`, `TestClusterStatusHealthUnknown`, `TestClusterStatusEffectiveState`, `TestReadsDelegateToStore`, `TestClusterStatus_ReportsAllViewVersionsAndUsesActiveGroups`, `TestTopologyHistoryPaging`, `TestClusterStatusReportsLeaders`, `TestClusterStatusLeaderUnknown`, `TestClusterStatusMetaLocalLeader`, `TestClusterStatusNoRuntimeDeps`, `TestClusterStatusNoMetaGroup`, `TestShardCounters`, `TestAddNode_ReaddsExcludedWithFreshMemberID`, `TestRemoveNode_DoesNotSynchronouslyCleanHardState`, `TestRemoveNode_IgnoresCommanderCleanupCapability`, `TestAddNode_RefusesReservedTargetToken`
- `plan_test.go` (1 cases): `TestPlanIsEmpty`
- `planner_test.go` (17 cases): `TestBuildPlanFreshFounding`, `TestBuildPlanFreshFoundingAddsIntentionalLearnerAfterFounders`, `TestBuildPlanEstablishedVoterGrowAddsLearnerThenVoter`, `TestBuildPlanExistingLearnerPromotion`, `TestBuildPlanIntentionalLearnerStopsAfterAdd`, `TestBuildPlanDemotesVoterAndTransfersLeader`, `TestBuildPlanIncludesMetaLearnerThenVoterChange`, `TestBuildPlanReplacementAtMaxShrinksFirst`, `TestBuildPlanWholeGroupDeletionStopsEveryFormerHost`, `TestBuildPlanRefusesUnsafeDisjointReplacement`, `TestBuildPlanEstablishedGroupMissingGloballyIsUnavailable`, `TestBuildPlanRefusesObservedGroupWithoutActiveIdentities`, `TestBuildPlanDeterministic`, `TestValidatePlanShapeRejectsMissingAcknowledgementAdjacency`, `TestPlanForAllowsUnavailableGroupDeletion`, `TestPlanForRejectsUnavailableRetainedGroup`, `TestPlanForUsesActiveAndRemoteGlobalStatus`
- `preview_test.go` (18 cases): `TestDryRunMutationThenPlanForPreviewsWithoutCommitting`, `TestDryRunRefusesInvalidResultWithoutCommitting`, `TestDryRunAddNodeComputesWithoutCommitting`, `TestDryRunAddNodeStillEnforcesGuardrails`, `TestDryRunRemoveNodeDrivesNoSideEffects`, `TestDryRunDrainNodeDoesNotMoveMetaLeadership`, `TestDryRunChangeReplicaProposesNoConfChange`, `TestDryRunTransferLeaderDoesNotTransfer`, `TestExpectedVersionMatchingCommits`, `TestExpectedVersionRefusesAConcurrentCommit`, `TestExpectedVersionRefusesStaleDryRun`, `TestExpectedVersionRefusesStaleRollback`, `TestUnpinnedContextIsUnrestricted`, `TestCommitStampsActorAndOriginFromContext`, `TestCommitDefaultsProvenanceWithoutContext`, `TestEmptyActorAndOriginFallBackToDefaults`, `TestRollbackStampsProvenanceAndRestoredParent`, `TestDryRunRollbackDoesNotCommit`
- `promotion_refusal_grpc_test.go` (6 cases): `TestClassifyStepError_RetypedRefusalIsRetriable`, `TestClassifyStepError_RefusalIsTerminalOffPromotionSteps`, `TestClassifyStepError_PermanentPromoteRefusalStillParks`, `TestClassifyStepError_GenuineErrorStaysTerminal`, `TestCaughtUpStatus_NamesTheReason`, `TestClassifyStepError_LivenessRefusalParksOnEveryStepKind`
- `readmit_test.go` (6 cases): `TestAddNode_ReplaceExistingSwapsIdentity`, `TestAddNode_ReplaceExistingRemovesTheOldMetaMember`, `TestAddNode_ReplaceExistingIsFreshOnEveryCall`, `TestAddNode_ReplaceExistingRefusesLiveNode`, `TestAddNode_ReplaceExistingAllowedWhenHealthUnknown`, `TestReplaceNodeReplicaIDs_SeedsUnsetCounterPastLiveIDs`
- `reconcile_heal_test.go` (8 cases): `TestHealable_Matrix`, `TestReconciler_HealsStagedInternal`, `TestReconciler_DoesNotHealOperatorStage`, `TestReconciler_DoesNotHealLaunderedVersion`, `TestReconciler_HealableErrorConsumesTheTick`, `TestReconciler_FollowerNeverHeals`, `TestReconciler_HealApplyFailureReportsBlocked`, `TestReconcileOnce_BoundsTheTick`
- `reconcile_policy_test.go` (2 cases): `TestRestoreDegradedPolicyAllowList`, `TestRestoreDegradedPolicyNeverRepairsMeta`
- `reconcile_status_test.go` (10 cases): `TestReconcilerStatusStatesAndReasons`, `TestReconcilerStatusFollowerNeverActs`, `TestReconcilerFollowerResetsStabilityObservations`, `TestReconcilerStatusUnavailableOnReadFailure`, `TestReconcilerStatusSinceStableAcrossIdenticalTicks`, `TestReconcilerStatusTransitionLogsAreMeaningfulOnly`, `TestReconcilerStatusErrorWarnsOnceNoPerTickRepeat`, `TestReconcilerStatusMetricKeepsSinglePairActive`, `TestReconcilerStatusSnapshotDeepCopy`, `TestReconcilerStatusConcurrentAccess`
- `reconcile_test.go` (8 cases): `TestReconcilerStartStopLifecycle`, `TestReconcilerGatesLeaderHealthAndKillSwitch`, `TestReconcilerResumesApprovedExecutionBeforeHealthGate`, `TestReconcilerDoesNotMergeStagedOrFailedOperatorIntent`, `TestReconcilerCommitsExactLearnerFirstRepair`, `TestReconcilerCASRaceLeavesOperatorVersionStaged`, `TestReconcilerRejectsPolicyActionsOutsideAllowList`, `TestValidateReplicaRepairCandidateRejectsTopologyEdits`
- `serving_test.go` (5 cases): `TestServingGate_Auto_SessionLive`, `TestServingGate_Auto_ShardQuorumLost`, `TestServingGate_LatestStagingDoesNotChangeActiveHealth`, `TestServingGate_Override`, `TestServingGate_Auto_MinorityFreezeNotReady`
- `validate_test.go` (4 cases): `TestDocumentValidate`, `TestValidateExcludedTombstonesDoNotConsumeNodeCap`, `TestValidateLiveNodesStillCapped`, `TestGuardrailViolationf`
- `watch_test.go` (13 cases): `TestWatch_StartStopLifecycle`, `TestEvaluateGroupHealth`, `TestEvaluateGroupHealth_BelowMinIndependent`, `TestEvaluateGroupHealth_Unknown`, `TestEvaluateDataAndAllGroupHealth`, `TestWatch_ReflectsDegradationIntoGauges`, `TestWatch_PublishesMetaAndDataUniformly`, `TestWatch_BelowMinReplicasSeparate`, `TestWatch_UnknownDeletesNumericSeries`, `TestWatch_RemovedGroupCleared`, `TestWatch_TransitionLogsOncePerChange`, `TestWatch_OnTransitionFiresOncePerEdge`, `TestWatch_ObservationLossWarnsOnce`

### Meta FSM, bbolt and real-meta clusters — `framework/cluster/topology/metaraft`

Target: `internal/metaengine/*_test.go and internal/testcluster`. Port actual Raft election/follower propose/concurrent CAS/watch fanout/snapshot catch-up and executor failover harnesses, plus FSM/durable-state/join/session/activation tests. Replace implicit-directory and legacy-format fixtures. Quarantine tests become explicit caller readmission-hook contract tests. forcenew_test.go is the explicitly deferred feature, not a normal recovery exception.

- `activation_test.go` (9 cases): `TestWaitForSelfVoter_EmptyDocumentAcceptsMatchingFounder`, `TestWaitForSelfVoter_EmptyDocumentStillRejectsUnadmittedIdentity`, `TestWaitForSelfVoter_JoinedFounderSeedDoesNotBypassPromotion`, `TestWaitForSelfVoter_BlocksLearnerUntilPromotion`, `TestActivate_MultiFounderQuorumCommits`, `TestActivate_MultiFounderWithoutQuorumFails`, `TestActivate_SingleNodeSucceeds`, `TestActivate_DuplicateNameRejected`, `TestActivate_Deactivate_ReleasesLock`
- `admission_test.go` (6 cases): `TestAdmission_SameInstanceRetryAcrossPhasesAndSnapshot`, `TestAdmission_SupersededInstanceCannotBeResurrected`, `TestAdmission_DifferentInstanceCannotReusePreparedMemberID`, `TestBuildReadmissionPlan_ShrinksBeforeLearnerFirstReplacementAtMaximum`, `TestJoinAndJoinReady_PrimeResolverWithoutReplicatingAddress`, `TestJoinReady_MetaConfStateIsLearnerBeforeVoter`
- `bolt_test.go` (17 cases): `TestStoreIsFresh`, `TestBoltStore_Roundtrip`, `TestBoltStore_LoadAllWithoutSnapshot`, `TestBoltStore_EmptyHardStateNotClobbered`, `TestBoltStore_LeaderOverwriteTruncatesTail`, `TestBoltStore_Compaction`, `TestBoltStore_StaleSnapshotIgnored`, `TestBoltStore_AppliedMonotone`, `TestBoltStore_SetAppliedPersistsConfState`, `TestBoltStore_ReopenAfterPartialWrite`, `TestBoltStore_IdentityPinning`, `TestResolveDataDir_Precedence`, `TestOpenBoltStore_CreatesFileAndBuckets`, `TestInstallSnapshot_AtomicAndCommitClamped`, `TestInstallSnapshot_IgnoresStale`, `TestBoltStore_LegacyCurrentStateRestoresNamedViews`, `TestPeekLocalLineage`
- `bootstrap_test.go` (28 cases): `TestDeriveMemberID_Deterministic`, `TestBootstrap_RecoversLeaderAssignedIdentity`, `TestDeriveClusterID_OrderIndependent`, `TestBuildFoundingMembership_TwoNodesAgree`, `TestBuildFoundingMembership_EmptyPeers`, `TestBuildFoundingMembership_TooMany`, `TestBuildFoundingMembership_RejectsReservedTargetToken`, `TestBuildLegacyDocument`, `TestBuildLegacyDocument_FoundingStamps`, `TestBuildLegacyDocument_UnknownNode`, `TestBootstrap_EmptyPeers_SingleFounder`, `TestEnsureFoundingTopology_LegacyImport`, `TestFoundingActivationNeeded`, `TestClassifyLegacyImport`, `TestFoundingSettled_RequiresAnAppliedActiveDocument`, `TestEnsureFoundingTopology_RecoversInterruptedFounding`, `TestEnsureFoundingTopology_SkipsNonFoundingLatest`, `TestBuildGreenfieldDocument_Deterministic`, `TestBuildGreenfieldDocument_MatchesLegacyShape`, `TestWaitFoundingMembersActivated`, `TestWaitFoundingMembersActivated_ReleasesWhenAnotherFounderWins`, `TestEnsureFoundingTopology_GreenfieldDispatch`, `TestEnsureFoundingTopology_MetaOnlyRowIsGreenfield`, `TestBuildFoundingDocument_NoPeers`, `TestIsTransientFoundingErr`, `TestProposeFoundingDocument_Bounded`, `TestWaitFoundingRetry_HonoursCancellation`, `TestNextFoundingRetryDelay_BacksOffToACap`
- `executor_test.go` (3 cases): `TestExecutor_RealMeta_FullExecution`, `TestExecutor_RealMeta_EmptyPlanSucceeds`, `TestExecutor_RealMeta_FailoverCompletesExecutionExactlyOnce`
- `forcenew_test.go` (13 cases): `TestParseForceNewClusterEnv`, `TestMaybeForceNewCluster_RewritesAndRecovers`, `TestMaybeForceNewCluster_DropsStaleLogTail`, `TestMaybeForceNewCluster_IdentityMismatch`, `TestMaybeForceNewCluster_Unarmed`, `TestForceNewArm_SaveIsCompareAndSet`, `TestForceNewArm_SaveAndPending`, `TestMaybeForceNewCluster_ArmMarkerPrecedence`, `TestMaybeForceNewCluster_ArmMatchesPinnedClusterID`, `TestForceNew_OrphanCheckMustReadLatest`, `TestMaybeForceNewCluster_UsesPinnedMemberID`, `TestMaybeForceNewCluster_DeclinesWhenLatestChangedAfterArm`, `TestMaybeForceNewCluster_SecondAPIArmReapplies`
- `founding_test.go` (1 cases): `TestFoundingMetaSnapshot_SeedsSortedConfState`
- `group_controller_test.go` (1 cases): `TestPrepareReplica_UnacknowledgedJoinReadyNamesNodeAndAction`
- `group_selfremove_test.go` (13 cases): `TestSelfRemoval_FiresOnceWhenExcluded`, `TestSelfRemoval_OtherMemberRemovedDoesNotFire`, `TestSelfRemoval_DemotionToLearnerDoesNotFire`, `TestSelfRemoval_NotExcludedDoesNotFire`, `TestSelfRemoval_SupersededMemberIDFires`, `TestSelfRemoval_JointDemotionDoesNotFire`, `TestSelfRemoval_JointRemovalFiresOnLeavingJoint`, `TestSelfRemoval_NoMembershipDeltaIsInert`, `TestSelfRemoval_ApplyEntryDoesNotDeliverBeforeDurable`, `TestSelfRemoval_FiresThroughApplyEntry`, `TestSelfRemoval_UnknownMemberIDDoesNotFire`, `TestSelfRemoval_NilCallbackIsInert`, `TestConfStateContains`
- `group_test.go` (13 cases): `TestMetaGroup_LeadershipObserverFiresOnChange`, `TestHandleInboundDropsMessageForRetiredMemberID`, `TestMetaGroup_Hydrate_RecoversConfStateFromAppliedIndex`, `TestMetaGroup_Hydrate_FallsBackToSnapshotConfState`, `TestMetaGroup_HaltsOnDurableWriteFailure`, `TestMetaGroup_HaltedGroupDoesNotBlockCallers`, `TestMetaGroup_SingleNode_ProposeAndRecover`, `TestMetaGroup_CASConflict`, `TestMetaGroup_IdentityMismatchOnReopen`, `TestMetaGroup_Resolver`, `TestMetaGroup_ResolverUsesAddressResolver`, `TestMetaGroup_GroupInterface`, `TestMetaGroup_RegistersUnderMetaShardID`
- `join_reason_test.go` (2 cases): `TestIsNotLeaderReason_ClassifiesDroppedProposal`, `TestProposeJoinCommandDroppedReason_IsRetryable`
- `join_resolver_test.go` (3 cases): `TestBootstrapJoined_ResolvesFoundingMembersBeforeCatchup`, `TestBootstrapJoined_ResolvesNonFounderLeaderFromMembers`, `TestJoinModeWithoutSeed_CannotResolvePeers`
- `join_test.go` (32 cases): `TestHandleJoin_NotLeader`, `TestHandleJoin_RefusesToClassifyWhileApplyIsBehind`, `TestHandleJoin_RefusesToClassifyBeforeCommittingItsOwnTerm`, `TestHandleJoin_RejectsMalformedIdentity`, `TestHandleJoin_RejectsClusterIDMismatch`, `TestHandleJoin_StillCurrentIsReadOnly`, `TestHandleJoin_StillCurrentCoversDrainStates`, `TestHandleJoin_AllowlistReject`, `TestHandleJoin_ConfiguredFounderBeforeDocumentIsNotReady`, `TestHandleJoin_ReadmitsKnownNode`, `TestHandleJoin_EchoesNeverActivatedNode`, `TestHandleJoin_RejectsLatestOnlyStaging`, `TestHandleJoin_ActivatedMarkerSurvivesRestart`, `TestHandleJoin_ReturnsCurrentMembership`, `TestHandleJoin_RejectsWhenNameActive`, `TestHandleJoin_RejectsExcludedNode`, `TestJoinCluster_SkipsUnreachableThenAccepts`, `TestJoinCluster_SurfacesReadmitted`, `TestJoinCluster_SurfacesIdentitySuperseded`, `TestJoinCluster_NoSupersededByDefault`, `TestJoinCluster_NoPeerAnswered`, `TestJoinCluster_NotReady`, `TestJoinCluster_NotSupported`, `TestJoinCluster_EstablishedReplyTakesPrecedenceOverColdPeer`, `TestJoinCluster_FollowsLeaderHint`, `TestJoinCluster_DefinitiveReject`, `TestJoinCluster_RefusalKindClassifiesWithoutProse`, `TestJoinCluster_PermissionDeniedDefinitive`, `TestHandleJoin_ReadmitsThroughStaleLock`, `TestHandleJoin_MachineryFailureAnswersTransient`, `TestHandleJoin_ReadmissionRetryIsEstablished`, `TestJoinCluster_RedirectExhaustionIsRetryable`
- `parked_test.go` (9 cases): `TestParkedRegistry_UpsertAttemptsExpiryRemoval`, `TestParkedRegistry_KeyedByNameAndInstance`, `TestParkedRegistry_SweepExpiresWithoutAReader`, `TestParkedRegistry_CapEvictsOldest`, `TestHandleParkedRead_LeaderAnswersFollowerRefuses`, `TestParkedJoins_NilReceiver`, `TestHandleJoin_RefusalRecordsPark`, `TestHandleJoin_AdmissionRemovesPark`, `TestHandleJoin_IdentitySupersededOnReplacement`
- `quarantine_test.go` (7 cases): `TestQuarantineSupersededState_MovesRaftDirAsOneArtifactKeepsNodeStore`, `TestQuarantineSupersededState_MissingRaftDirIsNoError`, `TestQuarantineSupersededState_TouchesNothingButRaftDir`, `TestQuarantineSupersededState_ErrorPropagates`, `TestQuarantineSupersededState_SymlinkedRaftDirIsRefused`, `TestQuarantineSupersededState_RepeatedCallsNeverOverwrite`, `TestFreeQuarantinePath_DisambiguatesCollision`
- `replica_ledger_test.go` (3 cases): `TestReplicaLedger_RecordLookupForget_IdentityCrossCheck`, `TestReplicaLedger_RefusesMetaGroupAndZeroReplica`, `TestMetaGroup_ReplicaLedger_NilSafe`
- `serve_override_test.go` (4 cases): `TestSetServeOverride_IdentityConfirmed`, `TestSetServeOverride_Clear`, `TestMaybeRatchet_RaisesAndClears`, `TestMaybeRatchet_QuorumLostKeepsOverride`
- `session_manager_test.go` (8 cases): `TestSessionManager_RefreshAndExpiry`, `TestSessionManager_SeedMissing`, `TestSessionManager_SeedMissingPreservesExistingDeadlines`, `TestSessionManager_ResetFrom`, `TestMetaGroup_IdentityPing_ReturnsIdentity`, `TestMetaGroup_KeepAlive_LeaderRefreshesSession`, `TestMetaGroup_KeepAlive_NonLeaderRedirects`, `TestMetaGroup_StartKeepAlive_MarksOwnSessionLive`
- `sessions_test.go` (6 cases): `TestSM_GrantSession_AndIdempotent`, `TestSM_AcquireLock_RequiresSession`, `TestSM_AcquireLock_Grant_Reacquire_Conflict`, `TestSM_RevokeSession_ReleasesLocks`, `TestSM_AcquireLock_ReclaimsAfterHolderGone`, `TestSM_Sessions_SnapshotRestore_NoVersionBump`
- `sm_test.go` (28 cases): `TestCodec_Roundtrip`, `TestCodec_EncodeUnknownType`, `TestSM_ProposeDocument_AssignsVersionAndCommits`, `TestSM_ProposeDocument_CASReject`, `TestSM_ProposeDocument_ValidationReject`, `TestSM_AdmitNode_AddAndIdempotent`, `TestSM_Apply_RefusesReplayedEntry`, `TestSM_AdmitNode_StampsAuditFieldsFromProposal`, `TestSM_AdmitNode_StampIsDeterministicAcrossReplicas`, `TestSM_AdmitNode_ReplaceExisting_FreshMemberID`, `TestSM_History_Ordering`, `TestSM_ExclusionVersion`, `TestSM_ExclusionVersion_AgedOutOfRing`, `TestSM_History_Paging`, `TestSM_ExactExecutionTransitions`, `TestSM_ForcedRetirementRecordsApplyingActor`, `TestSM_ExecutionSupersededAtExactBoundary`, `TestSM_ExecutionBeginRetryAndSupersession`, `TestSM_RejectsMalformedPlanAndPrematureActivation`, `TestSM_CommitRepairExecutionGuardrails`, `TestSM_RestoreLegacyCurrent`, `TestSM_PinsTargetActiveAndNonterminalBeyondHistory`, `TestSM_ApplyDeterminism`, `TestSM_SnapshotRestoreEquivalence`, `TestSM_PersistsToBoltStateBucket`, `TestBoltStore_LoadStateEmpty`, `TestSM_HistoryRingBounded`, `TestSM_ExecutionsBounded`
- `store_test.go` (11 cases): `TestStore_SingleNode_ProposeAndReads`, `TestStore_SingleNode_CASConflict`, `TestStore_GuardrailRejectionIsNotAVersionConflict`, `TestStore_CommitRepairExecutionAtomic`, `TestStore_SingleNode_NoQuorum`, `TestStore_SingleNode_Watch`, `TestStore_Watch_SeedIsNotRepeatedByALateApply`, `TestStore_Watch_RegressionIsDeliveredNotSuppressed`, `TestStore_SingleNode_WatchDropOldest`, `TestStore_Close`, `TestStore_WatchCancel`
- `testharness_test.go` (5 cases): `TestStore_Cluster_LeaderElection`, `TestStore_Cluster_ProposeFromFollower`, `TestStore_Cluster_ConcurrentCAS`, `TestStore_Cluster_WatchFanout`, `TestStore_Cluster_SnapshotCatchup`

### Fixed membership safety — `framework/cluster/membership`

Target: `membership/*_test.go`. Port every safety/coordinator case, including unknown versus known outage, actual retained majority, leader transfer, learner-first growth/catch-up and serialization; unsupported joint/multiple changes remain refusal tests.

- `membership_test.go` (19 cases): `TestChangeConstructorsUseLearnerThenVoterGrowth`, `TestSafetyAllowsLearnerAddWithUnknownHealth`, `TestSafetyAllowsSafeRemoval`, `TestSafetyUnknownHealthRefusesRemovalAndDemotion`, `TestSafetyKnownTotalOutageRefusesRemoval`, `TestSafetyRefusesQuorumBreakingRemoval`, `TestSafetyRequiresLeaderTransfer`, `TestSafetyRefusesFinalVoterRemoval`, `TestSafetyRefusesDirectVoterGrow`, `TestCaughtUpRequiresExactActiveLearner`, `TestCaughtUpRejectsLagMissingProgressZeroCommitAndFollower`, `TestSafetyPromotesOnlyCaughtUpActiveLearner`, `TestSafetyRefusalIncludesOperationalContext`, `TestSafetyRejectsMultiChange`, `TestSafetyRejectsJointTransition`, `TestCoordinatorAppliesOneExactChange`, `TestCoordinatorStopsBeforeUnsafeProposal`, `TestCoordinatorWrapsProposalFailure`, `TestCoordinatorSerializesPerGroup`

### Load Raft engine and proposal lifecycle — `framework/cluster/raftbudget/raft`

Target: `load/*_test.go and internal/testcluster`. Port generic founding, proposal correlation/cancellation, registry, events, conf changes, transfer, writer retry/off-loop/coalescing, snapshots, shutdown and halted-state behavior. Replace concrete reserve/finalize commands with the test Machine. Reservation rescue/compensation/redirect-GC business behavior is removed; retain any generic waiter-cleanup assertion from mixed cases. No budget-specific admission exemption or separate settlement timer is copied.

- `export_test.go` (0 cases): Test seam/helper file; port only helpers required by the retained cases.
- `founding_storage_test.go` (8 cases): `TestNewRaftGroup_FoundsViaSeedSnapshot`, `TestNewPreparedRaftGroup_LeavesStorageAndConfStateEmpty`, `TestNewPreparedRaftGroup_RejectsFoundingPeers`, `TestNewRaftGroupWithState_EmptyStorageSeedsFounding`, `TestNewRaftGroupWithState_RecoveredStorageNotReseeded`, `TestNewRaftGroupWithState_SeedsConfStateWithLearners`, `TestNewRaftGroupWithState_FreshClusterElectsLeader`, `TestStorage_EntryCountSnapshotCompact`
- `raftgroup_admission_test.go` (7 cases): `TestRaftGroup_InflightMatchesPendingReserves`, `TestRaftGroup_ExpiryFeedsTimeoutSample`, `TestRaftGroup_LogBacklogShedsReserveOnly`, `TestRaftGroup_ShedErrorIsGroupOverloaded`, `TestRaftGroup_ReadyAppliesWholeCommittedBacklog`, `TestNewRaftGroup_RejectsBacklogLimitBelowCompaction`, `TestWireGroupDeps_ShardIDFallsBackToShardConfig`
- `raftgroup_compensation_test.go` (2 cases): `TestProposeRG_CompensatingFinalizeGetsItsOwnResult`, `TestProposeRG_AbandonedWaiterIsDeregisteredImmediately`
- `raftgroup_confchange_test.go` (8 cases): `TestRaftGroup_ProposeConfChange_AddVoter`, `TestRaftGroup_ProposeConfChange_AddLearner`, `TestRaftGroup_LeaderEmptyEntryAdvancesApplied`, `TestRaftGroup_ProposeConfChange_PromotesPresentLearner`, `TestRaftGroup_ProposeConfChange_DemotesPresentVoter`, `TestRaftGroup_ProposeConfChange_RemoveVoter`, `TestRaftGroup_ProposeConfChange_IdempotentNoOps`, `TestRaftGroup_ProposeConfChange_SingleFlight`
- `raftgroup_errors_ext_test.go` (2 cases): `TestProposeRG_HaltedReturnsExportedError`, `TestDeadWAL_NeverHaltsFromOutside`
- `raftgroup_gauge_lifecycle_test.go` (2 cases): `TestGroupGauges_HaveALifecycle`, `TestStartRG_ResetsHaltedGaugeForARecreatedShard`
- `raftgroup_halt_test.go` (2 cases): `TestStepRG_HaltedGroupRefusesInsteadOfBlocking`, `TestStoreSnapshotRG_HaltedGroupRefusesInsteadOfBlocking`
- `raftgroup_integration_test.go` (4 cases): `TestRaftGroup_ReserveAndFinalizeEndToEnd`, `TestRaftGroup_BudgetExhaustion`, `TestRaftGroup_ConcurrentReserves`, `TestStateMachine_ExpiredBudgetReserve_Integration`
- `raftgroup_pending_sweep_test.go` (4 cases): `TestExpirePendingProposals_DoesNotBlockOnAbandonedWaiter`, `TestApplyCommittedEntry_DoesNotBlockOnAbandonedWaiter`, `TestHandleProposalRG_RejectionAfterRegistrationDeregisters`, `TestDeletePendingIfOwner_KeepsLaterAttemptsRegistration`
- `raftgroup_settlement_test.go` (7 cases): `TestHandleProposal_SettlementTimeoutLongerThanReserve`, `TestHandleProposal_StaleTimerDoesNotAnswerLaterAttempt`, `TestProposeRG_EnqueueBoundDoesNotCapResultWait`, `TestProposeRG_EnqueueBoundFiresOnFullQueue`, `TestStoreSnapshotRG_BoundedWhileWALPending`, `TestStoreSnapshotRG_CallerCancelBeatsTheTimeout`, `TestHandleProposal_ResolvedProposalLeavesNoWatchdog`
- `raftgroup_test.go` (14 cases): `TestRaftGroup_SingleNodePropose`, `TestRaftGroup_ProposeAndFinalize`, `TestRaftGroup_NonLeaderForwardsProposal`, `TestRaftGroup_MultipleShards`, `TestRaftRegistry_DuplicateRegister`, `TestRaftRegistry_Unregister`, `TestRaftRegistry_ProposeToUnknownShard`, `TestRaftGroup_IsLeader`, `TestRaftRegistry_IsLeaderUnknownShard`, `TestRaftGroup_FinalizeOnNonLeader`, `TestRaftGroup_LeadershipEvent_OnStart`, `TestRaftGroup_LeadershipObserverFiresOnElection`, `TestRaftGroup_LeadershipChannel_NonBlocking`, `TestRaftGroup_TransferLeaderSingleFlight`
- `raftgroup_wal_coalescing_test.go` (5 cases): `TestWALWriter_AppliedParkAndFlush`, `TestWALWriter_WaitGrowsNextBatch`, `TestWALWriter_IdleGroupSkipsWait`, `TestWALWriter_MessageOnlyReadyBypassesWriter`, `TestWALWriter_ZeroAndDegenerateConfigNoPanic`
- `raftgroup_wal_hardstate_test.go` (4 cases): `TestWAL_HardStateSkippedOnCommitOnlyAdvance`, `TestWAL_HardStateWrittenOnTermOrVoteChange`, `TestWAL_HardStateNotMarkedPersistedOnFailedWrite`, `TestRecover_LastPersistedHSSeeded`
- `raftgroup_wal_offloop_test.go` (9 cases): `TestWAL_PersistentFailureNeverHalts`, `TestWAL_StepRGNeverBlocksDuringStall`, `TestWAL_LoopKeepsTickingDuringStall`, `TestWAL_LeaderStepsDownDuringStall`, `TestWAL_CompactionDeferredWhilePending`, `TestWAL_AppliedPhaseRetries`, `TestWAL_ShutdownDuringStallIsQuiet`, `TestWAL_HaltsOnDecodeFailure`, `TestWAL_NoGoroutineHopOnEmptyReady`
- `raftgroup_wal_retry_test.go` (7 cases): `TestWAL_TransientFailureRecoversWithoutHalt`, `TestProposeRG_OverloadedWhileDegraded`, `TestWAL_ShedsOnlyReserve`, `TestWAL_DegradedIsLatencyBased`, `TestWAL_DegradedGaugesPublishedForSlowSuccess`, `TestPublishWALStallMetrics_MapsStallToGauges`, `TestWALStallSeconds_ZeroWhenIdle`
- `raftgroup_wal_snapshot_test.go` (1 cases): `TestWAL_ReceivedSnapshotIsPersistedBeforeAck`
- `redirect_gc_test.go` (3 cases): `TestRedirectGC_ExpiredRedirectIsRemoved`, `TestRedirectGC_ActiveRedirectSurvives`, `TestRedirectGC_SkipsNonLeaderShards`
- `registry_test.go` (11 cases): `TestRaftRegistry_RoutesToNonBudgetGroup`, `TestRaftRegistry_HandleJoinRoutesToMetaGroup`, `TestRaftRegistry_MetaGroupMissingIsTransientNotDefinitive`, `TestRaftRegistry_HandleJoinReadyRoutesToMetaGroup`, `TestRaftRegistry_UnknownShardRouting`, `TestRaftRegistry_TypedAccessorSkipsNonBudgetGroups`, `TestRaftRegistry_BudgetGroupThroughTypedAccessor`, `TestRaftRegistry_ShardIDsExcludesReservedGroups`, `TestRaftRegistry_GroupControlRegistrationTeardown`, `TestRaftRegistry_ApplyVersionRegistrationTeardown`, `TestRaftRegistry_ApplyVersionStaleUnregisterKeepsNewer`
- `reservation_tracker_test.go` (5 cases): `TestReservationTracker_ReleasesExpired`, `TestReservationTracker_RescueNotShedWhileDegraded`, `TestReservationTracker_IgnoresNonLeaderShards`, `TestReservationTracker_IdempotentRelease`, `TestReservationTracker_StopIsIdempotent`
- `shard_timing_test.go` (3 cases): `TestNewRaftGroup_RejectsInvertedTicks`, `TestNewRaftGroup_UnsetTimingFallsBackToDefaults`, `TestWireGroupDeps_UnsetTimingFallsBackToDefaults`
- `snapshot_test.go` (6 cases): `TestCreateRaftSnapshot_UsesLiveConfState`, `TestCreateRaftSnapshot_FallsBackToBootstrapPeers`, `TestApplySnapshot_RejectsZeroApplicationData`, `TestAutoCompact_StoresUsableSnapshotBeforeCompacting`, `TestAutoCompact_RefusesWhenSnapshotCannotBeStored`, `TestAutoCompact_NewerSnapshotIsIdempotent`
- `walstub_test.go` (0 cases): Test seam/helper file; port only helpers required by the retained cases.

### WAL durability and conformance — `framework/cluster/raftbudget/storage/walstore`

Target: `storage/walstore/*_test.go`. Port all WAL conformance, segmented reopen, marker, identity, corruption, refill-race, concurrent snapshot, monotone HardState, floor filtering, stats and close assertions. Replace placeholder ConfState/application payloads with complete generic snapshots and actual membership; add corrupt-newest-after-compaction fallback coverage.

- `conformance_test.go` (1 cases): `TestWalStore_Conformance`
- `refill_internal_test.go` (1 cases): `TestRefillRecreatesPipelineFiles`
- `walstore_test.go` (13 cases): `TestWalStore_ReopenAcrossSegments`, `TestWalStore_BaseMarkerSurvivesReopen`, `TestWalStore_IdentityMismatch`, `TestWalStore_ProbeInputs`, `TestWalStore_QuarantineSweep`, `TestWalStore_RefillerRace`, `TestWalStore_LoadShardWhileOpenSeesLastWrite`, `TestWalStore_SnapshotReadWriteConcurrent`, `TestWalStore_HardStateNoRegress`, `TestWalStore_FloorFiltering`, `TestWalStore_CorruptOnlySnapshotIsUnreadable`, `TestWalStore_Stats`, `TestWalStore_CloseIdempotent`

### Meta networking and caller control contracts — `framework/cluster/raftbudget/transport`

Target: `internal/metatransport/*_test.go, load transport-contract tests and tests/cluster/testapp`. Port meta join/apply/parked/session/token/cluster-ID/resolver/peer queue and snapshot-envelope semantics. Group-control exact-scope, cancellation, redirect/error and remote-host behavior become interface/test-application contract tests. Load streaming/snapshot protocol tests exercise the caller test protocol only; they do not export a library load wire protocol. Remove budget-request forwarding RPCs.

- `applyversion_test.go` (6 cases): `TestTransport_ApplyVersion_RoundTrip`, `TestTransport_ApplyVersion_MarksForwarded`, `TestTransport_ApplyVersion_DomainRefusalSurfaces`, `TestTransport_ApplyVersion_UncapableHandlerDegrades`, `TestTransport_ApplyVersion_RefusalClassSurvivesTheWire`, `TestTransport_ApplyVersion_UncapableHandlerIsNotMisdirected`
- `clusterid_test.go` (3 cases): `TestClusterIDHolder`, `TestClusterID_StreamInterceptor`, `TestClusterID_JoinExempt`
- `group_control_test.go` (12 cases): `TestGroupControl_AllOperationsPreserveExactScope`, `TestGroupStatus_DeterministicRoundTrip`, `TestGroupControl_UnsupportedHandlerUsesTypedStatus`, `TestGroupControl_FollowsOnlyOneLeaderHint`, `TestJoinReady_IdempotentDelegationAndRedirect`, `TestGroupControl_ErrorCodeMapping`, `TestGroupControl_NewRPCsRejectWrongToken`, `TestGroupControl_PreservesContextCancellation`, `TestExecutionScopeRejectsIdentityMismatch`, `TestGroupControlRejectsDiscriminatorScopeMismatch`, `TestNoOpTransport_GroupControlIsTypedUnsupported`, `TestGroupRPCReadyTimeouts_StatusFitsACallerBudget`
- `group_rpc_budget_test.go` (1 cases): `TestBoundGroupRPCContext_WalkFitsCallerBudget`
- `parkedread_test.go` (7 cases): `TestTransport_ParkedRead_RoundTrip`, `TestTransport_ParkedRead_EmptyRegistry`, `TestTransport_ParkedRead_StaleLeaderBeliefRefuses`, `TestTransport_ParkedRead_HandlerErrorSurfaces`, `TestTransport_ParkedRead_UncapableHandlerDegrades`, `TestTransport_ParkedRead_AfterCloseDoesNotRedial`, `TestTransport_AfterCloseDoesNotStartPeerQueue`
- `peer_queue_dial_test.go` (7 cases): `TestPeerQueue_DeadPeerDoesNotBlockHealthyPeer`, `TestPeerQueue_DialHonoursTimeout`, `TestPeerQueue_ConnMetricsTrackLifecycle`, `TestPeerQueue_StopReleasesConnectionAndDeletesMetrics`, `TestPeerQueue_DialAfterStopRefusesToPublish`, `TestPeerQueue_StopIsIdempotentAndSafeBeforeStart`, `TestPeerQueue_CloseConnCancelsStreamLifetime`
- `peer_queue_seams_test.go` (0 cases): Test seam/helper file; port only helpers required by the retained cases.
- `peer_queue_test.go` (18 cases): `TestPeerQueue_FirstFailureFlushesAndReportsUnique`, `TestPeerQueue_BackoffProgression`, `TestPeerQueue_BackoffFollowsConfiguredTiming`, `TestPeerQueue_JitterClampedToMax`, `TestPeerQueue_BackoffDiscardsAndCoalesces`, `TestPeerQueue_ProbeFailureThenSuccessResets`, `TestPeerQueue_LoopProbeRecoveryNoReplay`, `TestPeerQueue_FIFOOnHealthyStream`, `TestPeerQueue_SharedGroupFlushBoundary`, `TestPeerQueue_QueueFullDropsNonBlocking`, `TestPeerQueue_StopDuringBackoff`, `TestPeerQueue_ConcurrentEnqueueAndStop`, `TestPeerQueue_Metrics`, `TestPeerQueue_OutageLogging`, `TestPeerQueue_RecoveryLogging`, `TestPeerQueue_StopCleanup`, `TestTransportMetrics_PeerQueueTransitions`, `TestTransportMetrics_DropReasonsBounded`
- `promotion_refusal_grpc_test.go` (4 cases): `TestGroupControlStatusError_SafetyRefusalIsAborted`, `TestGroupControlStatusError_GenericStaysInternal`, `TestGroupControlClientError_AbortedRetypesToRefusalSentinel`, `TestGroupControlClientError_OtherErrorsKeepAddressedWrap`
- `registry_test.go` (8 cases): `TestDocumentNodeRegistry_ResolvesViaResolver`, `TestDocumentNodeRegistry_ResolvesNodeAbsentFromDocument`, `TestDocumentNodeRegistry_FallbackWhenResolverMisses`, `TestDocumentNodeRegistry_NoFallbackErrors`, `TestDocumentNodeRegistry_WatchUpdatesAndClose`, `TestDocumentNodeRegistry_UsesActiveTargetUnionAndRejectsLatestOnly`, `TestDocumentNodeRegistry_RejectsActiveTargetIdentityCollision`, `TestDocumentNodeRegistry_StartWatchErrorPropagates`
- `session_test.go` (3 cases): `TestTransport_SessionKeepAlive_Delegates`, `TestTransport_SessionKeepAlive_UnsupportedHandler`, `TestTransport_IdentityPing_Delegates`
- `token_test.go` (3 cases): `TestClusterToken_Hash`, `TestClusterToken_UnaryInterceptor`, `TestClusterToken_StreamInterceptor`
- `transport_test.go` (10 cases): `TestTransport_Join_DelegatesToJoinHandler`, `TestTransport_Join_RoundTripsLineageAndSuperseded`, `TestTransport_Join_RefusalKindSurvivesTheWire`, `TestTransport_Join_UnsupportedHandler`, `TestTransport_SendSnapshot_PreservesMessageEnvelope`, `TestTransport_StreamMessages`, `TestTransport_SendSnapshot_NoLiveConnectionFails`, `TestTransport_ForwardRequestHandler_Retained`, `TestTransport_MultipleConnections`, `TestTransport_ConnectionReuse`

### Local cluster events — `framework/cluster/events`

Target: `events/*_test.go`. Port journal ordering/paging/retention/live-byte accounting/reopen/close and leadership/liveness/quorum-edge recording. Paths are explicit, subscriptions are nonblocking, event data stays out of replicated snapshots. Replace source filename-layout conventions with the constructor path contract.

- `journal_test.go` (16 cases): `TestOpen_CreatesNodeStoreAndSurvivesReopen`, `TestJournal_AppendListOrdering`, `TestJournal_KeysetPagination`, `TestJournal_AgeRotation`, `TestJournal_AmortizedRotation`, `TestJournal_SizeCapRotation`, `TestRotate_IgnoresFileSizeAndCoTenantBuckets`, `TestJournal_NilSafe`, `TestJournal_AppendAfterCloseDrops`, `TestRecorders_NilJournalNoOp`, `TestHealthRecorder_QuorumCrossingsOnly`, `TestHealthRecorder_QuorumRegainedWhileDegraded`, `TestLeadershipAndLivenessRecorders`, `TestHealthRecorder_RecordsQuorumLostFromUnknown`, `TestRotate_SizePruneConvergesOnLiveBytes`, `TestRotate_StatsWalkIsGatedButStillPrunes`
- `storenames_test.go` (2 cases): `TestStoreNames_BoltFilesUseBoltExtension`, `TestStoreNames_NoRelationalStorePointsAtABoltFile`

### Explicit identity and resolver contracts — `framework/cluster/identity`

Target: `meta identity tests`. Port identity validation, instance identity and reserved-name/mode errors. Source environment/config/hostname precedence is removed; caller supplies identity.

- `identity_test.go` (6 cases): `TestResolve_Precedence`, `TestResolve_InstanceID`, `TestValidateFor`, `TestValidateFor_ReservedTargetTokens`, `TestValidateFor_UnknownMode`, `TestModeString`

### Explicit identity and resolver contracts — `framework/cluster/discovery`

Target: `meta resolver tests`. Port supplied resolver/map-copy/priority/error/cache/prime/concurrency semantics where retained by meta options; discovery remains caller-injected and no gossip runtime is copied.

- `discovery_test.go` (15 cases): `TestStaticProvider_HitAndMiss`, `TestStaticProvider_Instances_SortedNoEmpty`, `TestStaticProvider_MapCopy`, `TestChain_Priority`, `TestChain_LateAdd`, `TestChain_RealErrorPropagates`, `TestChain_Instances_MergeByName`, `TestChain_Instances_ErrorPropagates`, `TestCachingResolver_WriteThroughAndFallback`, `TestCachingResolver_MissWithoutCache`, `TestCachingResolver_StaleValueOverwrite`, `TestCachingResolver_Prime`, `TestCachingResolver_RealErrorNoFallback`, `TestCachingResolver_InstancesPassThrough`, `TestChain_ConcurrentResolveAdd`

### Application-integration seams with generic Raft assertions

- `framework/cluster/raftbudget/service_test.go` (9 cases): Split meta service lifecycle/routing/token/cluster-ID cases into meta transport tests. Generic caller load dispatch/unreachable reporting stays in test-application adapters. Symbols: `TestRaftTransportServiceInboundRouting`, `TestRaftTransportServiceUnreachableDispatch`, `TestRaftTransportServiceStopIdempotent`, `TestRaftTransportServiceStopWithoutStart`, `TestCompositeResolverShardRouting`, `TestCompositeResolverMetaRouting`, `TestCompositeResolverNoResolvers`, `TestRaftTransportServiceEnforcesClusterToken`, `TestRaftTransportServiceEnforcesClusterID`.
- `framework/cluster/raftbudget/config/shard_registry_topology_test.go` (4 cases): Port Active-driven attachment, Latest staging inertness and replica identity-change notifications into runtime registry/controller integration tests. Symbols: `TestShardRegistry_TopologyStore_LatestStagingIsInertUntilActive`, `TestShardRegistry_TopologyStore_FeedsFromDocument`, `TestShardRegistry_TopologyStore_ReplicaIdentityChangeIsModified`, `TestShardRegistry_TopologyStore_IgnoresV0Document`.
- `plugins/governance/cluster_apply_receiver_test.go` (3 cases): Port one-hop forwarded Apply behavior and typed domain refusals into meta transport tests. Symbols: `TestApplyReceiver_ForwardedRunsLocalOnly`, `TestApplyReceiver_UnforwardedMayForward`, `TestApplyReceiver_RefusalIsDomainNotTransport`.
- `plugins/governance/cluster_config_runtime_test.go` (5 cases): Port empty prepared runtime, exact identity conflict/cluster mismatch, partial-start cleanup and first-snapshot reception into load/controller tests. Symbols: `TestPrepareJoiningReplica_IsEmptyIdempotentAndNonServing`, `TestPrepareJoiningReplica_ConflictingIdentityIsQuarantined`, `TestPrepareJoiningReplica_RejectsWrongClusterBeforeConstruction`, `TestPrepareJoiningReplica_PartialStartFailureStopsThenUnregisters`, `TestPrepareJoiningReplica_ReceivesFirstSnapshot`.
- `plugins/governance/cluster_config_topology_test.go` (12 cases): Port stable ID ordering, declared identity/Target authorization, Latest-only startup refusal and fresh-ID replacement into runtime/controller tests. Remove SQL/bootstrap entity assumptions. Symbols: `TestReplicasFromNodeNames_SortedStableIDs`, `TestReplicasFromNodeNames_MatchesRetiredScheme`, `TestVoterIDsFromReplicas_SortedAndSkipsLearners`, `TestCreateShardAtRuntime_RefusesNonDeclaredReplica`, `TestLocalReplicaIDFromReplicas_UsesStableID`, `TestLoadStartupShards_PrefersDocument`, `TestLoadStartupShards_IgnoresLatestOnlyStagedShard`, `TestLoadStartupShards_EmptyDocumentNilDBIsEmpty`, `TestLoadStartupShards_NilStoreNilDBIsEmpty`, `TestReadmissionReplacesLocalReplicaDefersRetiredActiveIdentity`, `TestTargetAuthorizesJoiningReplica`, `TestTopologyManagedCreateShardDeleteShardOnlyDeclareIntent`.
- `plugins/governance/group_controller_test.go` (16 cases): Port every exact-scope/idempotence/safety/remote status/registration retry/close-drain contract into test controller integration and cluster cases. Receiver-global absence requires all known host observations. Symbols: `TestGroupControlStatusUsesRemoteHostWhenControllerHostsNoShard`, `TestGroupControlStatusLocalFollowerRedirectsToLeader`, `TestGroupControlLocalStatusIncludesLiveConfStateAndProgress`, `TestGroupControlStatusAllKnownHostsNotFoundIsGloballyAbsent`, `TestGroupControlExactAuthorizationAndIdempotentReplay`, `TestGroupControlAuthorizationWaitsForLocalMetaCursor`, `TestGroupControlImmediateTransferUsesExactActiveScopeRemotely`, `TestGroupControlRemoteUnavailableRemainsRetryable`, `TestGroupControlPrepareWaitsForRemoteHandlerRegistration`, `TestGroupControlStopWaitsForRemoteHandlerRegistration`, `TestGroupControlMembershipUsesFixedSafetyCoordinator`, `TestGroupControllerIdempotentObservationStillRequiresAuthorization`, `TestGroupControlRejectsSupersededAndDifferentOperation`, `TestGroupControlHandlerCloseDrainsAdmittedCalls`, `TestGroupControlPrepareRegistersEmptyNonServingRuntime`, `TestCleanupReplicaState_DispatchedToOwner`.
- `plugins/governance/shard_boot_test.go` (13 cases): Port current-format boot decision matrix, pin/ledger/store ordering, parking/restoration and caller recovery hooks. SQL legacy seeding is not retained. Symbols: `TestDecideShardBoot_SevenCases`, `TestApplyShardBoot_SeedWritesRecordAfterStore`, `TestApplyShardBoot_FreshCreatesStoreBeforeRecord`, `TestApplyShardBoot_MoveAsideDropsStoreAndRecord`, `TestApplyShardBoot_ParkSetsReadinessAndGauge`, `TestApplyShardBoot_IdentityMismatchParkSurvivesRestart`, `TestApplyShardBoot_RestoredStoreClearsPark`, `TestBootShardStorage_UnreadableStoreParks`, `TestParkedRegistry_SameReplicaStaysParked`, `TestReadinessCheck_ReplicaParked`, `TestLocalShardStates_ReportsParked`, `TestLocalShardParkedState_EveryReason`, `TestPublishStoreBytes_PublishesFootprint`.
- `plugins/governance/shard_lifecycle_test.go` (6 cases): Port prepared-to-Active attachment and exact authorized retry/stop checks. Only _meta is reserved; remove source default workload special treatment. Symbols: `TestShardLifecycle_RemovedSkipsReservedAndDefaultShard`, `TestShardLifecycle_AttachesPreparedRuntimeOnlyAfterActive`, `TestShardLifecycle_DetachedShard0ClosesAndReopensDataPlane`, `TestShardLifecycle_VisibilityAloneDoesNotCreateRuntime`, `TestShardLifecycle_RetriesOnlyExactAuthorizedPrepare`, `TestShardLifecycle_RetriesOnlyExactAuthorizedStop`.
- `plugins/governance/shard_teardown_test.go` (5 cases): Port exact-replica stop/drop/ledger cleanup followed by caller shared cleanup. Remove PG table/per-backend SQL assertions. Symbols: `TestDeleteShardLifecycleCleansExactReplicaThenSharedState`, `TestRaftStateTables_PerBackend`, `TestDeleteShardRuntime_DropsStoreAndForgetsLedger`, `TestCleanupReplicaState_NodeLocalBackendLeavesDeprecatedRows`, `TestCleanupReplicaState_PostgresLeavesSQLToDropShard`.
- `plugins/governance/legacy_seed_test.go` (3 cases): Port durable identity-ledger refusal of reused old IDs and readmission record behavior; legacy SQL seeding assertions remain excluded. Symbols: `TestLegacySeedMarks_WALSeedsOnceAndRefusesReadd`, `TestLegacySeedMarks_ReadmitMarksWithoutDeleting`, `TestLegacySeedMarks_NilPreservesLegacySeed`.
- `transports/server/pendingprobes_test.go` (1 cases): Port bounded probe connection lifetime through controller test fixtures. Symbols: `TestPendingProbeConnectionsDoNotOutliveRelease`.

- `framework/cluster/raftbudget/storage/memstore/memstore_test.go` (3 cases): Port generic conformance, immutable stored-state and concurrent-writer assertions into WAL/test-adapter conformance coverage; no public memory-store backend is required. Symbols: `TestConformance_MemStore`, `TestMemStore_CallerCannotMutateStoredState`, `TestMemStore_ConcurrentWriters`.

This supporting inventory contains 881 test/benchmark symbols in 102 files. Counts enumerate source symbols, not a claim that removed application business rules will be implemented or that any test has passed.

### Other searched suites and harness resources

- `framework/cluster/{cluster,integration,config,configstore,delegate,events,messages,discovery_gossip,conflict}_test.go`: gossip/cache/runtime-config tests do not implement the extracted Raft layer. Resolver and nonblocking peer/health semantics are covered above; no memberlist runtime is introduced.
- `framework/cluster/topology/staticstore/store_test.go`: application static-store implementation is not a required public backend; retain immutable document-copy/watch expectations in meta tests where applicable.
- `framework/cluster/raftbudget/storage/compat_test.go`: tests source compatibility environment-variable parsing, which is excluded by the caller-supplied-settings requirement.
- `framework/cluster/raftbudget/raft/statemachine/*_test.go`, `persistence/*_test.go`, `storage/pgstore/*_test.go`, `types/*_test.go`, `rebalancer/*_test.go`, `admission/controller_test.go` and `config/shard_registry_test.go`: concrete budget FSM, SQL projections/migrations, customer assignment, reservation admission and business rebalancing are caller domain. Generic engine queue/cancellation/recovery and Active lifecycle contracts are inventoried above.
- `tests/governance`: Raft mentions in estimation/entity tests exercise budget seeding and reservation expiry rather than generic Raft correctness. The runtime and controller ports replace the needed consensus coverage without provider fixtures.
- `transports/handlers/cluster_v1_guard_test.go`: application permission guard, not library authentication or exact execution authorization. The latter are required meta/controller tests.
- `tests/cluster/{setup,pool,metrics_helpers}.go`, `README.md` and `docs/architecture/cluster-integration-tests.md`: preserve exclusive fixture ownership, parallel isolation, readiness polling, real local recovery observations and failure diagnostics. Replace PG/template/config/license/provider setup and the fixed three-node pool.

## Required larger-cluster and failure matrix

| Scenario family | Required matrix / assertions |
| --- | --- |
| Meta consensus | Single-node units; three/five/seven-node process clusters; exactly one stable leader per healthy group, follower proposals, concurrent CAS and per-node watch/state convergence. |
| Growth and capacity | Three-to-four/five and five-to-seven transitions with explicit Limits; learner first, caught-up promotion, concurrent admission and refusal above Limits.MaxNodes. Replacement shrinks first; excluded tombstones do not consume capacity. |
| Quorum behavior | Three/five/seven: keep writes available with retained majority; minority cannot acknowledge new commits; refuse destructive changes without live retained majority; reconnect and compare complete committed state. |
| Node types and group hosting | Every admitted node joins meta; load groups use unrestricted/restricted types and subset hosts. Meta leader may be a non-host. Intentional learners stay learners until approved promotion. |
| Multi-group lifecycle | Fifteen groups: declare/approve/replicate/list/delete; retained IDs/tombstones. Five- and seven-node concurrent writer/controller shutdown must complete without leaked resources. |
| Approved effects | Lost completion after successful callback, leadership change, exact authorized retry, stale/conflicting identity refusal, superseded execution and unreachable-host deletion barrier. Active changes only after verified convergence. |
| Recovery | Same-disk restart, delayed founding, full snapshot plus committed WAL tail, compaction catch-up, corrupt-newest fallback, missing voted store parking, fresh-ID readmission/reseat and old-identity rejection. |
| Transport and storage pressure | Dead peer, partition, bounded queue saturation, writer stall/retry, pending waiter cancellation and WAL coalescing. Healthy meta remains responsive and acknowledged commands are not lost/duplicated. |
| Planner/snapshot operation | Measure time/allocations/serialized size/restore time at three/five/seven nodes and fifteen groups with retained history/executions/tombstones. Deterministic safe plans complete within test deadlines; full restored state is equivalent. |

Only Limits sets maximum node and replica capacity. Node lists, partition sets and test matrix sizes specify scenarios, not additional capacity constants or configuration. Keep the runnable example at the default three nodes.

## Validation and recorded results

When implemented, `go test ./...` runs unit/in-process correctness tests; scoped `go test -race` covers engine/controller/watch/journal lifetime; `make test-cluster` runs every required process-cluster case, including scale and normally slow recovery cases; `make bench` records planner/snapshot measurements. `make test-cluster-run RUN=<test expression>` is for targeted iterations. Record source-to-target mapping, tested capacities, command results and diagnostics. A successful smoke run or explicit short run does not satisfy the full port requirement.

Design-stage verification only: verify every discovered source cluster symbol occurs exactly once in the process inventory and every supporting source symbol/file exists; verify larger-cluster testing is absent from deferred lists and required in the main target scope. No source or destination Go/Docker test run is claimed.

## Verified implementation traceability (in progress)

- `framework/cluster/topology/metaraft/sm_test.go`: applicable codec, document CAS/validation/admission, history/exclusion, execution transitions/retry/supersession/repair/retention, determinism, snapshot equivalence and bbolt materialization cases retain their source test names in `internal/metaengine/sm_test.go`. `LastStep` assertions use the equivalent `CurrentStep` field. Node types and meta membership are explicit inputs. Legacy-current restoration is replaced by schema refusal in `TestRestoreRejectsCorruptionWithoutChangingLiveState`; empty bbolt state is covered by metastore tests.
- `framework/cluster/topology/metaraft/sessions_test.go`: all six scenarios ported to `internal/metaengine/sessions_test.go`, supplying a positive TTL. Exact identity conflict is additionally covered by `TestSessionIdentityCannotBeRebound`.
- `framework/cluster/topology/metaraft/admission_test.go`: the three FSM admission scenarios ported to `internal/metaengine/admission_test.go`; the fixture supplies the same exact learner-first sequence directly. Readmission planner and network join cases remain required in stages 7–8.
- Validation for these ports: `make test`, `go test -race ./internal/metaengine ./topology`, `make lint`; all passed. This entry does not satisfy the outstanding process-cluster inventory.

- `framework/cluster/topology/metaraft/session_manager_test.go`: four pure lease-table cases ported with their source names to `meta/leases_test.go`; exact runtime identity ping, leader/follower keepalive and own-session readiness cases remain outstanding in stage 7.
- Shared runtime tests now live in `internal/raftengine/group_test.go` behind the public `load` API. New meta gRPC/bbolt tests in `internal/metaengine/runtime_test.go` verify real consensus, full committed-tail restart and leader replacement; they do not replace the required public bootstrap/join or process-cluster scenarios.

- `framework/cluster/topology/planner_test.go`: first fourteen BuildPlan/ValidatePlanShape scenarios ported with retained names to `topology/planner_test.go`, supplying pinned Rules explicitly. Manager/PlanFor status-collection cases remain required in stage 8.
- `framework/cluster/topology/metaraft/admission_test.go::TestBuildReadmissionPlan_ShrinksBeforeLearnerFirstReplacementAtMaximum`: ported to `internal/metaengine/admission_test.go` at Limits sizes 3, 5 and 7. The earlier direct learner-sequence fixture now calls the real BuildPlan. Remaining network join cases remain required.

Public runtime progress: `meta/group_test.go`, `session_runtime_test.go`, and `restart_test.go` now adapt the source founding/session-manager runtime contracts (identity ping, leader keepalive, nonleader refusal and own-session activation), with exact instance/member identity, expiry/unlock, follower CAS/history, persisted Rules without a snapshot and cancellation regressions. These use real gRPC/bbolt replicas rather than mock public methods. Dynamic join/readmission, remaining founding/bootstrap/activation cases and cluster inventory ports are still outstanding. `internal/metaengine/codec_transport_test.go` verifies every command accepted by the meta proposal RPC and refuses unknown/schema/null/malformed bodies.

Approval/convergence progress: `internal/convergence/engine_test.go` ports nineteen applicable source convergence tests, including learner-first/destructive steps, founding recovery, per-host deletion before shared cleanup, unavailable/deleted groups, final validation, cancellation, failover, exact-step supersession, mixed-plane ownership, retry timer reuse, concurrent ticks and lost transition proposals. The source Apply/retry test is still pending its complete public mutation/runtime port; source Prometheus assertions will be adapted to runtime observations/events rather than importing an application metrics stack. `meta/planning_test.go` exercises real three-node follower approval and meta-leader evacuation. `meta/load_effects_test.go` verifies a caller-owned effect surviving meta leader replacement before its completion record, with one physical creation and rejection of stale scope. Fresh-observation acknowledgement and exact typed refusal tests extend the port. These functional tests do not satisfy the outstanding process-cluster/testcontainers inventory.
