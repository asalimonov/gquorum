# Labels and placement validation

Date: 2026-10-13. Contract: [labels and placement](../architecture/placement.md).
The test names below identify current-format behavior; historical compatibility
fixtures remain immutable and are expected to be rejected as unsupported.

| Acceptance behavior | Direct evidence |
| --- | --- |
| Owned full-map `GetLabels`/`SetLabels`, bounds, reserved keys and descriptive group labels | `topology.TestLabelReplacementOwnsInputsAndOutputs`, `TestSetLabelsFailureIsAtomic`, `TestGroupLabelsAreIndependentOfPlacement`, `TestLabelValidationAppliesToDirectMapWrites` |
| Exact selector matching, inherited versus complete group override, approved-only reservation retention and release | `topology.TestPlacementSelectorMissingAndEmptyAreDistinct`, `TestPlacementReservationRequiresApprovedExactIdentity`, `TestRequiredLoadHostsForPreservesOnlyApprovedReservation`, `TestReservationReleaseAddsLearnerWithoutMovingVoters` |
| Strict voter domains and learner-only in `_meta` and loads | `topology.TestPlacementLearnerOnlyAppliesToMetaAndStrictVoters`, `TestStrictFoundingRequiresCompleteDistinctVoterTarget`, `TestPreparePlacementRejectsStrictShortageWithoutAllocation`; process `TestPlacementLearnerOnlyIncludesMetaThreeNodes` |
| Safe selector relocation, strict creation refusal, reserved restart and refusal of a new identity | process `TestPlacementSelectorReservationAndStrictShortageThreeNodes`; `internal/metaservice.TestReservedIdentityCannotBeReseatedOrReadmittedInPlace` |
| Guarded same-AZ standby switch, loss of a survivor, committed native effect before receipt and recovery | process `TestPlacementStrictStandbySwitchAndSurvivorLossFourNodes` and strict `surge_0` branch of `TestVoterSwapResumesAfterCommittedEffectAndMetaLeaderCrashFourNodes`; `topology.TestBuildPlanReplacementAtMaxUsesSafeDemotionFirst` |
| Replicated CAS validation, approval refusal, report transport and status explanations | `internal/metaengine.TestStagePlacementUsesReplicatedApprovedViewAtomically`, `TestFailedApprovalRetainsStagedPlacementVersion`; `internal/metaservice.TestTypedGroupPlacementPreviewMatchesPurePreparation`; placement status and transport codec tests in their owning packages |
| Scheduler respects AZ, learner-only and reservation, including custom policies | `policy.TestVoterPolicySkipsSameAZAndLearnerOnlyCandidates`, `TestLegacyRepairSkipsReservedSuccessor`; `internal/scheduling.TestDriverSkipsSameAZCandidateAndRejectsCustomBypass`, `TestCustomLegacyRepairCannotAllocateOnReservedNode` |
| Current snapshot, tail replay, exact readmission, cleanup evidence, unsupported old formats | `internal/metaengine.TestCurrentPlacementSnapshotRetainsReplicatedIntentAndReservation`, `TestBuildReadmissionPlanReleasesOldSeatAtNativeLimit`, `TestInterruptedLoadCleanupSurvivesHistoryAndSnapshot`, `TestBaselineSnapshotIsRejectedWithoutMutation`, `TestBaselineProposalsRejectedAndResultBytesPreserved`; `internal/metaservice.TestPublicMetaRestartUsesPinReplaysTailAndRefusesChangedRules`, `TestControlStoreRejectsOldMetadataFormatWithoutRewrite`; process `TestLostMetaLeaderAutomaticallyReadmitsFreshIdentityThreeNodes`, `TestDownNodeRotatedIdentityQuarantinesAndRejoinsThreeNodes` |

The larger strict-layout regression uses four processes because three AZ voters
and a same-AZ standby cannot coexist on three nodes. Delayed stale-action
fencing and exact leader transfer are also exercised by the existing process
authority and voter-recovery regressions. Static placement reports describe
declared constraints, not physical AZ independence, live quorum, disk capacity
or resource pressure. The caller owns resource measurement and any policy that
uses it. This feature adds no mixed-version migration or old-format replay.

Validation commands and results from this implementation:

| Command | Result |
| --- | --- |
| `make test` | Passed, 768/768 unit checks after the at-capacity readmission fix. |
| `make lint` | Passed after the same fix. |
| `make test-race` | Passed, 768/768 unit race checks. |
| `make test-function` | Passed, 838/838 functional checks. |
| `make test-function-race` | Passed, 838/838 functional race checks. |
| Focused topology/metaengine race checks | Passed for safe at-capacity replacement and readmission. |
| `make test-cluster-build` | Passed after the final planner change. |
| Focused process run for two readmission cases and selector/reservation case | Passed, 3/3. |
| Focused strict interrupted voter-swap case | Passed. |
| `make test-cluster CLUSTER_PARALLEL=4` before readiness-wait test fixes | Passed 84/85; `TestLoadIngressAndExactIndependentCommandsThreeNodes` received a transient initial `unavailable` response. The same case passed alone. |
| `make test-cluster CLUSTER_PARALLEL=3` after the final fixes | Passed, 85/85 process checks. |

An earlier eight-cluster run also exposed transient leader-transfer startup
refusals, and a lower-concurrency run exposed the actual full-capacity
readmission planner gap. Both readmission process regressions passed after the
planner fix; the final full run above passed all cases.
