module effects_test {
  import types.* from "./types"
  import basicSpells.* from "./vendor/spells/basicSpells"
  import operations_test.* from "./operations_test"
  import membership as ProofMembership from "./membership"
  import sessions as SessionRules from "./sessions"
  import initial as AuthorityInitial from "./initial"
  import grants as GrantRules from "./grants"
  import execution as ExecRules from "./execution"
  export operations_test.*

  pure val transferTarget: ReplicaKey = { cluster: "c1", group: "a", node: "n2", member: 2, replica: 2 }
  pure val transferRequest: RequestKey = { caller: { node: "n1", incarnation: 1 }, sequence: 1 }
  pure val transferCommand = IssueTransfer({ target: transferTarget, request: transferRequest, sourceVersion: 1 })
  val issuedAuthority = world.extensions.metadata.grants.keys().size() > 0
  val consumedAuthority = world.extensions.consumptions.size() > 0
  val reportedAuthority = world.extensions.metadata.effectReceipts.size() > 0
  pure val transferGrantKey: GrantKey = { cluster: "c1", group: "a", revision: 2 }

  action controlCommit(command: ControlCommand): bool = {
    nondet queued = world.extensions.network.pending.filter(m => match m.payload {
      | MetaProposal(p) => p.command == ControlProposal(command) | _ => false }).oneOf()
    deliver(queued).then(commitMutation(queued))
  }
  action commitAndApplyControl(command: ControlCommand): bool = submitControl("n1", command)
    .then(controlCommit(command)).then(applyMeta("n1"))
  action receiveQueuedGrant: bool = {
    nondet queued = world.extensions.network.pending.filter(m => match m.payload {
      | ExecuteGrant(_) => true | _ => false }).oneOf()
    deliver(queued).then(confirmGrant(queued)).then(receiveGrant(queued))
  }
  action consumeWaitingGrant: bool = {
    nondet delivery = world.system.get("n1").volatile.waitingEffects.oneOf()
    consumeGrant(delivery)
  }
  action transferIssued: bool = init.then(commitAndApplyControl(transferCommand))
  action transferWaiting: bool = transferIssued
    .then(dispatchGrant("n1", transferGrantKey, GroupResource("a"))).then(receiveQueuedGrant)

  run transferPersistsBeforeReportingTest = transferWaiting.then(consumeWaitingGrant).expect(and {
    (world.extensions.metadata.effectReceipts.size() == 0),
    (world.extensions.consensus.get("a").authorityReceipts.size() == 1),
    (world.extensions.consensus.get("a").leaderAuthority != Some(transferTarget)),
    (ExecRules::physicalEvidenceValid(world)),
  }).then({
    nondet receipt = world.extensions.consensus.get("a").authorityReceipts.oneOf()
    reportNative("n1", "a", RecordEffect(receipt)).then(controlCommit(RecordEffect(receipt)))
  }).expect(and {
    (world.extensions.metadata.effectReceipts.size() == 1),
    (world.extensions.consensus.get("a").leaderAuthority != Some(transferTarget)),
  }).then(finishTransfer("a", transferGrantKey)).expect((
    world.extensions.consensus.get("a").leaderAuthority == Some(transferTarget)))

  run duplicateGrantDoesNotRepeatEffectTest = transferIssued
    .then(dispatchGrant("n1", transferGrantKey, GroupResource("a")))
    .then({ nondet queued = world.extensions.network.pending.filter(m => match m.payload {
      | ExecuteGrant(_) => true | _ => false }).oneOf()
      duplicate(queued) })
    .then(receiveQueuedGrant).then(consumeWaitingGrant)
    .then(receiveQueuedGrant).then(consumeWaitingGrant)
    .expect(and {
      (world.extensions.consensus.get("a").authorityReceipts.size() == 1),
      (world.extensions.consensus.get("a").committedPosition == 2),
      (ExecRules::physicalEvidenceValid(world)),
    })

  run receiverRestartRejectsOldDeliveryTest = transferIssued
    .then(dispatchGrant("n1", transferGrantKey, GroupResource("a")))
    .then(crash("n1")).then(restart("n1")).then(receiveQueuedGrant)
    .expect(and {
      (world.system.get("n1").volatile.waitingEffects.size() == 0),
      (world.extensions.consensus.get("a").authorityReceipts.size() == 0),
    })

  // Retained metadata fixture for an already committed closure. Physical
  // installation below uses the production transport/wait/consume actions.
  action fixtureMetadata(meta: MetadataState, position: int): bool = {
    val c = world.extensions.consensus.get("_meta")
    val nodes = ProofMembership::persistCertificate(world.system, c.voters, position)
    world' = { ...world, extensions: { ...world.extensions, metadata: meta,
      metaLog: world.extensions.metaLog.put(position, meta),
      consensus: world.extensions.consensus.put("_meta", ProofMembership::committed(c, c.voters, position)) },
      system: nodes.setBy("n1", n => { ...n,
        volatile: { ...n.volatile, appliedMetaIndex: position,
          appliedMetaVersion: meta.latest.version, localView: { latest: meta.latest, target: meta.target } },
        durable: { ...n.durable, metaIndex: position, metaImage: Some({ latest: meta.latest, target: meta.target }) } }) }
  }
  pure val nativeFenceKey: GrantKey = { cluster: "c1", group: "a", revision: 3 }
  action retainedFenceInit: bool = transferWaiting.then(
    fixtureMetadata(GrantRules::closeGrants(world.extensions.metadata, Set(transferGrantKey), 3), 3))
  action consumeFence: bool = {
    nondet delivery = world.system.get("n1").volatile.waitingEffects.filter(d => d.grant.purpose == Fence).oneOf()
    consumeGrant(delivery)
  }
  action installNativeFence: bool = dispatchGrant("n1", nativeFenceKey, GroupResource("a"))
    .then(receiveQueuedGrant).then(consumeFence)

  run fenceRecheckedAfterNativeWaitTest = retainedFenceInit.then(installNativeFence)
    .expect(world.system.get("n1").volatile.waitingEffects.forall(old => not(ExecRules::mayConsume(world, old))))
    .expect(ExecRules::physicalEvidenceValid(world))
  run omittedFenceRecheckHasCounterexampleTest = retainedFenceInit.then(installNativeFence).then({
    nondet old = world.system.get("n1").volatile.waitingEffects.oneOf()
    world' = ExecRules::consume(world, old)
  }).expect(not(ExecRules::physicalEvidenceValid(world)))

  run nativeQuorumLossRetainsRevocationTest = retainedFenceInit
    .then(dispatchGrant("n1", nativeFenceKey, GroupResource("a"))).then(receiveQueuedGrant)
    .then(crash("n2")).then(crash("n3"))
    .expect(world.extensions.metadata.revocations.get(transferGrantKey).provenResources == Set())
    .expect(world.extensions.metadata.revocations.get(transferGrantKey).fences == Map(GroupResource("a") -> nativeFenceKey))
    .expect(world.extensions.consensus.get("a").authorityFences.size() == 0)
    .then(consumeFence.fail())

  // A lost sender does not erase already authenticated durable authority. Its
  // delayed work is bounded by physical fences, not a global process lookup.
  run oldSenderCannotCrossSuccessorFenceTest = transferIssued
    .then(applyMeta("n2")).then(dispatchGrant("n2", transferGrantKey, GroupResource("a")))
    .then(crash("n2")).then(receiveQueuedGrant)
    .then(fixtureMetadata(GrantRules::closeGrants(world.extensions.metadata, Set(transferGrantKey), 3), 3))
    .then(installNativeFence)
    .expect(world.system.get("n1").volatile.waitingEffects.forall(d => not(ExecRules::mayConsume(world, d))))
    .expect(ExecRules::physicalEvidenceValid(world))

  action replaceReceiverPinFixture: bool = {
    world' = { ...world, system: world.system.setBy("n1", node => { ...node,
      durable: { ...node.durable, pin: Some({ cluster: "c1", node: "n1", member: 4 }) } }) }
  }
  run receiverIdentityRecheckedAfterWaitTest = transferWaiting.then(replaceReceiverPinFixture)
    .expect(world.system.get("n1").volatile.waitingEffects.forall(d => not(ExecRules::mayConsume(world, d))))
    .then(consumeWaitingGrant.fail())
  run omittedIdentityCheckHasCounterexampleTest = transferWaiting.then(replaceReceiverPinFixture).then({
    nondet old = world.system.get("n1").volatile.waitingEffects.oneOf()
    world' = ExecRules::consume(world, old)
  }).expect(not(ExecRules::physicalEvidenceValid(world)))

  // Authority-catalog fixture for one retained readmission host obligation.
  // Admission workflow/replacement mechanics are tested in Task 10.
  pure val oldHostKey: GrantKey = { cluster: "c1", group: "a", revision: 2 }
  pure val hostFenceKey: GrantKey = { cluster: "c1", group: "a", revision: 3 }
  pure val hostResource = HostResource({ group: "a", node: "n3" })
  action retainHostFence: bool = {
    val replica: ReplicaKey = { cluster: "c1", group: "a", node: "n3", member: 3, replica: 3 }
    val old: Grant = { key: oldHostKey, purpose: Readmission, effect: None,
      admission: Some({ node: "n3", predecessor: 3, successor: 4 }),
      resources: Set(hostResource), targets: Set(replica), planStep: None, request: None }
    fixtureMetadata(GrantRules::closeGrants(GrantRules::install(world.extensions.metadata, old), Set(oldHostKey), 3), 3)
  }
  action retainedHostFenceInit: bool = init.then(retainHostFence)
  run offlineHostRetainsExactFenceDebtTest = retainedHostFenceInit.then(crash("n3"))
    .then(dispatchGrant("n1", hostFenceKey, hostResource))
    .expect(world.extensions.metadata.revocations.get(oldHostKey).requiredResources == Set(hostResource))
    .expect(world.extensions.metadata.revocations.get(oldHostKey).provenResources == Set())
    .expect(world.extensions.metadata.revocations.get(oldHostKey).fences == Map(hostResource -> hostFenceKey))
    .expect(world.system.get("n3").durable.fences.size() == 0)
    .then({ nondet queued = world.extensions.network.pending.filter(m => match m.payload {
      | ExecuteGrant(_) => true | _ => false }).oneOf()
      deliver(queued).fail() })

  run hostFenceNeedsDurableReportTest = retainedHostFenceInit.then(applyMeta("n3"))
    .then(dispatchGrant("n1", hostFenceKey, hostResource)).then(receiveQueuedGrant)
    .then({ nondet delivery = world.system.get("n3").volatile.waitingEffects.oneOf()
      consumeGrant(delivery) })
    .expect(world.system.get("n3").durable.fences.size() == 0)
    .then(completeHostFence("n3", hostResource))
    .expect(world.system.get("n3").durable.fences.size() == 1)
    .expect(world.extensions.metadata.revocations.get(oldHostKey).provenResources == Set())
    .then({ nondet proof = world.system.get("n3").durable.fences.oneOf()
      reportHostFence("n3", proof).then(controlCommit(RecordFence(proof))) })
    .expect(world.extensions.metadata.revocations.get(oldHostKey).provenResources == Set(hostResource))
    .expect(ExecRules::physicalEvidenceValid(world))

  pure val leaderDemotion: ApiRequest = Mutate({ edit: ChangeReplica({ group: "a", node: "n1", role: Learner }),
    options: { ...options, approve: true } })
  val approvedTransfer: ApprovedAction = {
    operation: { version: 2, attempt: 1 },
    step: world.extensions.metadata.operations.get({ version: 2, attempt: 1 }).plan.blocks.get("a")
      .select(s => s.kind == TransferLeadership).head(),
  }
  val approvedTransferGrant: Grant = GrantRules::actionGrants(world.extensions.metadata, approvedTransfer).getOnlyElement()

  run approvedTransferRunsThroughCheckpointTest = init
    .then(beginMutation(mutationKey, leaderDemotion)).then(commitProposal(mutationKey))
    .then(receiveMutationAck(mutationKey)).then(applyMeta("n2")).then(finishMutation(mutationKey))
    .then(operationRead(approvalKey)).then(applyMeta("n1")).then(captureOperation("n1", approvalKey))
    .then(observeOperation("n1", approvalKey, "a")).then(observeOperationHealth("n1", approvalKey, "a"))
    .then(commitWork(approvalKey)).then(receiveWorkResult(approvalKey))
    .then(commitAndApplyControl(BeginBlock({ operation: { version: 2, attempt: 1 }, group: "a" })))
    .then(commitAndApplyControl(IssueEffect(approvedTransfer)))
    .then(dispatchGrant("n1", approvedTransferGrant.key, GroupResource("a")))
    .then(receiveQueuedGrant).then(consumeWaitingGrant)
    .expect(world.extensions.metadata.operations.get(approvedTransfer.operation).blocks.get("a").step == approvedTransfer.step.seq)
    .then({ nondet receipt = world.extensions.consensus.get("a").authorityReceipts.oneOf()
      reportNative("n1", "a", RecordEffect(receipt)).then(controlCommit(RecordEffect(receipt))) })
    .then(applyMeta("n1"))
    .expect(world.extensions.metadata.operations.get(approvedTransfer.operation).blocks.get("a").step == approvedTransfer.step.seq)
    .then(finishTransfer("a", approvedTransferGrant.key))
    .then(checkpointTransfer("n1", approvedTransfer))
    .then(controlCommit(CompleteStep({ approved: approvedTransfer,
      observedLeader: world.extensions.consensus.get("a").leaderAuthority })))
    .expect(world.extensions.metadata.operations.get(approvedTransfer.operation).blocks.get("a").step > approvedTransfer.step.seq)
    .expect(safety)

  run newCourierBindingRejectsOldSenderTest = transferIssued.then(applyMeta("n2"))
    .then(dispatchGrant("n2", transferGrantKey, GroupResource("a")))
    .then(crash("n2")).then(restart("n2"))
    .then(fixtureMetadata(SessionRules::apply(SessionRules::apply(SessionRules::apply(world.extensions.metadata,
      RevokeInternalSession({ owner: { node: "n2", incarnation: 1 }, generation: 1 })),
      GrantInternalSession({ key: { owner: { node: "n2", incarnation: 2 }, generation: 1 }, member: 2 })),
      BindInternalSession({ owner: { node: "n2", incarnation: 2 }, generation: 1 })), 3))
    .then(receiveQueuedGrant)
    .expect(world.system.get("n1").volatile.waitingEffects.size() == 0)
    .expect(world.extensions.consensus.get("a").authorityReceipts.size() == 0)
    .expect(safety)

  run courierProofRequiresMetaQuorumTest = transferIssued
    .then(dispatchGrant("n1", transferGrantKey, GroupResource("a")))
    .then({ nondet msg = world.extensions.network.pending.filter(m => match m.payload {
      | ExecuteGrant(_) => true | _ => false }).oneOf()
      deliver(msg) })
    .then(setLink(MetaChannel, "n1", "n2", false)).then(setLink(MetaChannel, "n1", "n3", false))
    .expect(world.system.get("n1").volatile.authorityReads.keys().size() == 0)
    .then({ nondet msg = world.messages.get("n1").filter(m => match m.payload {
      | ExecuteGrant(_) => true | _ => false }).oneOf()
      confirmGrant(msg).fail() })
}
