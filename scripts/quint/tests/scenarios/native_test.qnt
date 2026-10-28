module native_test {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import lifetime_test.* from "./lifetime_test"
  import grants as GrantRules from "../../metaengine/grants"
  import managed_membership_steps as NativeEffects from "../../managed/membership_steps"
  import membership as NativeRules from "../../consensus/membership"
  export lifetime_test.*

  pure val demotionRequest: ApiRequest = Mutate({ edit: ChangeReplica({ group: "a", node: "n3", role: Learner }),
    options: { ...options, approve: true } })
  pure val nativeAttempt: AttemptKey = { version: 2, attempt: 1 }
  val nativeApproved: ApprovedAction = { operation: nativeAttempt,
    step: world.extensions.metadata.operations.get(nativeAttempt).plan.blocks.get("a")
      .select(s => s.kind == DemoteVoter).head() }
  val nativeGrant = GrantRules::actionGrants(world.extensions.metadata, nativeApproved).getOnlyElement()
  val nativeDelivery = world.system.get("n1").volatile.waitingEffects.filter(d => NativeEffects::isNative(d)).getOnlyElement()

  // Execute the real public mutation/approval path, then dispatch its exact
  // granted effect. No ready-made native change or receipt is injected here.
  action demotionWaiting = init
    .then(beginMutation(mutationKey, demotionRequest)).then(commitProposal(mutationKey))
    .then(receiveMutationAck(mutationKey)).then(applyMeta("n2")).then(finishMutation(mutationKey))
    .then(operationRead(approvalKey)).then(applyMeta("n1")).then(captureOperation("n1", approvalKey))
    .then(observeOperation("n1", approvalKey, "a")).then(observeOperationHealth("n1", approvalKey, "a"))
    .then(commitWork(approvalKey)).then(receiveWorkResult(approvalKey))
    .then(commitAndApplyControl(BeginBlock({ operation: nativeAttempt, group: "a" })))
    .then(commitAndApplyControl(IssueEffect(nativeApproved)))
    .then(dispatchGrant("n1", nativeGrant.key, GroupResource("a"))).then(receiveQueuedGrant)
    .then(installNativeAuthority(nativeDelivery))

  run nativeProposalIsNotCompletionTest = demotionWaiting.then(precheckNative(nativeDelivery))
    .then(proposeNative(nativeDelivery))
    .expect(world.extensions.consensus.get("a").voters.size() == 3)
    .expect(world.extensions.consensus.get("a").authorityReceipts.size() == 0)
    .expect(world.extensions.metadata.effectReceipts.size() == 0)
    .then(commitNative("a"))
    .expect(world.extensions.consensus.get("a").voters.size() == 2)
    .expect(world.extensions.consensus.get("a").learners.exists(r => r.node == "n3"))
    .expect(world.extensions.metadata.effectReceipts.size() == 0)
    .then({ nondet receipt = world.extensions.consensus.get("a").authorityReceipts.oneOf()
      reportNative("n1", "a", RecordEffect(receipt)).then(controlCommit(RecordEffect(receipt))) })
    .then(applyMeta("n1"))
    .then(checkpointEffect("n1", nativeApproved))
    .then(controlCommit(CompleteStep({ approved: nativeApproved,
      observedLeader: world.extensions.consensus.get("a").leaderAuthority })))
    .expect(world.extensions.metadata.operations.get(nativeAttempt).blocks.get("a").phase == OpSucceeded)
    .expect(safety)

  run nativeProposalRechecksAfterPrecheckTest = demotionWaiting.then(precheckNative(nativeDelivery))
    .then(crash("n2"))
    .expect(world.extensions.consensus.get("a").pendingChange == None).expect(safety)
    .then(proposeNative(nativeDelivery).fail())

  run nativeCommitNeedsCurrentQuorumTest = demotionWaiting.then(precheckNative(nativeDelivery))
    .then(proposeNative(nativeDelivery)).then(crash("n2")).then(crash("n3"))
    .expect(world.extensions.consensus.get("a").voters.size() == 3)
    .expect(world.extensions.consensus.get("a").authorityReceipts.size() == 0).expect(safety)
    .then(commitNative("a").fail())

  // Failure after a valid proposal can make the resulting config unavailable.
  // The library does not rerun a health policy while applying committed Raft.
  run postProposalFailureDoesNotUndoCommitTest = demotionWaiting.then(precheckNative(nativeDelivery))
    .then(proposeNative(nativeDelivery)).then(crash("n2")).then(commitNative("a"))
    .expect(world.extensions.consensus.get("a").voters.size() == 2)
    .expect(not(NativeRules::hasCommitPath(world.extensions.consensus.get("a"), world.system,
      world.extensions.network.loadLinks)))
    .expect(safety)

  run nativeReceiverRestartInvalidatesPrecheckTest = demotionWaiting.then(precheckNative(nativeDelivery))
    .then(crash("n1")).then(restart("n1"))
    .expect(world.system.get("n1").volatile.nativeChecks == Map())
    .expect(world.extensions.consensus.get("a").pendingChange == None).expect(safety)

  run nativeReplayDoesNotChangeConfigurationTest = demotionWaiting.then(precheckNative(nativeDelivery))
    .then(proposeNative(nativeDelivery)).then(commitNative("a"))
    .then(dispatchGrant("n1", nativeGrant.key, GroupResource("a"))).then(receiveQueuedGrant)
    .then(replayNative(nativeDelivery))
    .expect(world.extensions.consensus.get("a").configRevision == 2)
    .expect(world.extensions.consensus.get("a").authorityReceipts.size() == 1).expect(safety)

  run uncommittedNativeProposalCanBeRetriedTest = demotionWaiting.then(precheckNative(nativeDelivery))
    .then(proposeNative(nativeDelivery)).then(crash("n1")).then(discardNative("a"))
    .expect(world.extensions.consensus.get("a").pendingChange == None)
    .expect(world.extensions.consensus.get("a").voters.size() == 3)
    .expect(world.extensions.consensus.get("a").authorityFloor == nativeGrant.key.revision)
    .expect(world.extensions.consensus.get("a").authorityReceipts.size() == 0).expect(safety)

  run nativeLivenessWaitKeepsScopedOwnerTest = demotionWaiting
    .then(setLink(LoadChannel, "n1", "n2", false))
    .then(reportNativeStatus("n1", nativeDelivery))
    .then(controlCommit(RecordStepStatus({ approved: nativeApproved, phase: WaitingReplica, reason: Some(NoQuorum) })))
    .then(applyMeta("n1"))
    .expect(world.extensions.metadata.operations.get(nativeAttempt).blocks.get("a").phase == WaitingReplica)
    .expect(world.extensions.metadata.groups.get("a").owner == Some(nativeAttempt))
    .then(setLink(LoadChannel, "n1", "n2", true)).then(precheckNative(nativeDelivery))
    .then(proposeNative(nativeDelivery)).then(commitNative("a"))
    .expect(world.extensions.consensus.get("a").voters.size() == 2).expect(safety)

  run returnedReceiptSurvivesPostChangeQuorumLossTest = demotionWaiting
    .then(precheckNative(nativeDelivery)).then(proposeNative(nativeDelivery))
    .then(crash("n2")).then(commitNative("a"))
    .then({
      nondet msg = world.extensions.network.pending.filter(m => match m.payload {
        | EffectResult(r) => r.receipt.grant == nativeGrant.key | _ => false }).oneOf()
      deliver(msg).then(receiveEffectResult(msg))
    }).then({
      nondet receipt = world.system.get("n1").volatile.effectResults.oneOf()
      reportCachedEffect("n1", receipt).then(controlCommit(RecordEffect(receipt)))
    }).then(applyMeta("n1")).then(checkpointEffect("n1", nativeApproved))
    .then(controlCommit(CompleteStep({ approved: nativeApproved,
      observedLeader: world.extensions.consensus.get("a").leaderAuthority })))
    .expect(world.extensions.metadata.operations.get(nativeAttempt).blocks.get("a").phase == OpSucceeded)
    .expect(not(NativeRules::hasCommitPath(world.extensions.consensus.get("a"), world.system,
      world.extensions.network.loadLinks))).expect(safety)

  action nativeStageProbe = world' = world
  run nativeSharesWorldTest = clusterStageInit.then(lifetimeStageProbe).then(nativeStageProbe)
    .expect(world.system.keys() == NODE_POOL)
}
