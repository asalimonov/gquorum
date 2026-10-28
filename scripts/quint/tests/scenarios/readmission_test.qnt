module readmission_test {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import scaling_helpers.* from "./scaling_helpers"
  import readmission_base as ReadmitRules from "../../metaengine/readmission_base"
  import controls as ReadmitControls from "../../metaengine/controls"
  import metaservice_admission_steps as ReadmitJoin from "../../metaservice/admission_steps"
  import metaservice_readmission_steps as ReadmitService from "../../metaservice/readmission_steps"
  import managed_readmission_steps as ReadmitHost from "../../managed/readmission_steps"
  import managed_retirement_steps as ReadmitRetirement from "../../managed/retirement_steps"
  import cleanup_properties as ReadmitChecks from "../../harness/cleanup_properties"
  export scaling_helpers.*

  pure val predecessor: ReplicaKey = { cluster: "c1", node: "n3", member: 3, group: "_meta", replica: 3 }
  pure val formerSession: SessionKey = { owner: { node: "n3", incarnation: 1 }, generation: 1 }
  val readmissionKey: AdmissionKey = { node: "n3", predecessor: 3, successor: world.extensions.metadata.target.nodes.get("n3").member }

  action revokeReadmissionPredecessor = seedLeases("n1").then(expireLease("n1", formerSession))
    .then(probeSession("n1", formerSession)).then(timeoutSessionProbe("n1", formerSession))
    .then(revokeExpiredSession("n1", formerSession))
    .then(controlCommit(SessionControl(RevokeInternalSession(formerSession)))).then(applyMeta("n1"))
    .then({
      val gate = world.system.get("n1").volatile.revokeGate.unwrap()
      nondet ack = world.extensions.network.pending.filter(m => match m.payload {
        | MetaAck(a) => a.key == gate.proposal | _ => false }).oneOf()
      deliver(ack).then(handleSession(ack))
    })

  action manualReadmissionApproved = init.then(crash("n3"))
    .then(revokeReadmissionPredecessor)
    .then(approveScaleEdit(ReadmitNode({ name: "n3", expectedMember: 3 }), 1, 2))
    .expect(world.extensions.metadata.target.nodes.get("n3").member == 4)
    .expect(world.system.get("n3").durable.pin == Some({ cluster: "c1", node: "n3", member: 3 }))

  action bindRetainedReadmission = restart("n3").then(requestJoin("n3", "n1"))
    .then({
      nondet msg = world.extensions.network.pending.filter(m => match m.payload {
        | JoinRequest(j) => j.owner.node == "n3" | _ => false }).oneOf()
      deliver(msg).then(confirmJoin(msg)).then({
        val command = AdmissionControl(ReadmitJoin::bindCommand(world, msg).unwrap())
        bindJoin(msg).then(controlCommit(command))
      }).then(applyMeta("n1")).then({
        val command = ReadmitService::command(world, msg).unwrap()
        issueReadmission(msg).then(controlCommit(command))
      }).then(applyMeta("n1")).then(replyReadmission(msg))
    }).then({
      nondet msg = world.extensions.network.pending.filter(m => match m.payload {
        | ReadmissionAccepted(_) => true | _ => false }).oneOf()
      deliver(msg).then(receiveReadmission(msg))
    }).expect(world.system.get("n3").volatile.readmissionOffer != None).expect(safety)

  action retainedReadmissionOffer = manualReadmissionApproved.then(bindRetainedReadmission)

  action joinRetainedReadmission = fenceReadmissionScope("n3", "_meta")
    .then(fenceReadmissionScope("n3", "a")).then(fenceReadmissionScope("n3", "b"))
    .then(stopReadmissionOwners("n3")).then(joinReadmissionOwners("n3"))
    .expect(not(world.system.get("n3").volatile.metaRunning)).expect(safety)

  action readmissionJoined = retainedReadmissionOffer.then(joinRetainedReadmission)

  action retireReadmissionLoad(group: GroupId): bool = {
    val host = world.system.get("n3")
    val key = host.volatile.readmissionOffer.unwrap().grants.filter(g => g.key.group == group).getOnlyElement().targets.getOnlyElement()
    beginReadmissionRetirement("n3", key).then({
      val d = world.system.get("n3").volatile.retirementWork.get(key).delivery
      returnRetirement("n3", key, ReadmitRetirement::resultFor(d))
    }).then(quarantineRetirement("n3", key)).then(finishRetirement("n3", key)).expect(safety)
  }

  action finishRetainedReadmission = retireReadmissionLoad("a")
    .then(retireReadmissionLoad("b")).then(backupReadmissionMeta("n3")).then(finishReadmissionMeta("n3"))
    .expect(world.system.get("n3").durable.readmissionBackup.contains(predecessor)).expect(safety)

  action readmissionLocallyComplete = readmissionJoined.then(finishRetainedReadmission)

  action recordReadmissionProofs(receipts: Set[Receipt]): bool = {
    val command = RecordReadmission({ admission: world.extensions.metadata.admissions.get("n3"), receipts: receipts })
    reportReadmissionBatch("n1", "n3", receipts).then(controlCommit(command)).then(applyMeta("n1"))
  }

  run manualReadmissionKeepsPredecessorUntilHandoverTest = manualReadmissionApproved.expect(safety)

  run readmissionNeedsEveryFloorBeforeJoiningTest = retainedReadmissionOffer.then(fenceReadmissionScope("n3", "a"))
    .expect(not(ReadmitHost::stopEnabled(world, "n3")))
    .expect(not(ReadmitHost::joinEnabled(world, "n3")))
    .expect(not(ReadmitHost::installPinEnabled(world, "n3"))).expect(safety)

  run readmissionPartialBatchCannotSettleDebtTest = readmissionJoined.then(retireReadmissionLoad("a"))
    .then(recordReadmissionProofs(world.system.get("n3").durable.receipts.filter(r => r.admission != None)))
    .expect(not(ReadmitRules::consumed(world.extensions.metadata, readmissionKey)))
    .expect(world.extensions.metadata.cleanup.values().forall(d => d.retired == None))
    .expect(ReadmitControls::errors(world.extensions.metadata,
      AdmissionControl(ReadyAdmission(world.extensions.metadata.admissions.get("n3")))).contains(Unavailable)).expect(safety)

  run readmissionLocalCompletionPrecedesMetadataAcceptanceTest = readmissionLocallyComplete
    .expect(not(ReadmitRules::consumed(world.extensions.metadata, readmissionKey)))
    .then(installReadmissionPin("n3")).then(startJoinedMeta("n3"))
    .expect(world.system.get("n3").volatile.phase == Starting)
    .expect(ReadmitControls::errors(world.extensions.metadata,
      AdmissionControl(ReadyAdmission(world.extensions.metadata.admissions.get("n3")))).contains(Unavailable)).expect(safety)

  run readmissionCompleteBatchRetainsAllFormerOwnersTest = readmissionLocallyComplete
    .then(recordReadmissionProofs(world.system.get("n3").durable.receipts.filter(r => r.admission != None)))
    .expect(ReadmitRules::consumed(world.extensions.metadata, readmissionKey))
    .expect(world.extensions.metadata.cleanup.keys().filter(r => r.node == "n3" and r.member == 3).size() == 3)
    .expect(world.extensions.metadata.cleanup.values().forall(d => d.stopped != None and d.retired != None)).expect(safety)

  action readyReadmittedProcess = announcePrepared("n3", "n1").then({
    nondet msg = world.extensions.network.pending.filter(m => match m.payload {
      | JoinPrepared(a) => a.owner.node == "n3" | _ => false }).oneOf()
    val command = AdmissionControl(ReadyAdmission(world.extensions.metadata.admissions.get("n3")))
    deliver(msg).then(acceptPrepared(msg)).then(controlCommit(command))
  }).then(applyMeta("n1"))

  action readmissionStarted = readmissionLocallyComplete.then(installReadmissionPin("n3"))
    .then(startJoinedMeta("n3"))
    .then(recordReadmissionProofs(world.system.get("n3").durable.receipts.filter(r => r.admission != None)))
    .then(readyReadmittedProcess)

  run readmissionConvergesTwoLoadsAndFreshSessionTest = readmissionStarted.then(executeMetaBlock(2))
    .then(activateJoinedNode("n3")).then(executeGroupBlock(2, "a")).then(executeGroupBlock(2, "b"))
    .expect(world.system.get("n3").volatile.phase == Ready)
    .expect(world.system.get("n3").volatile.owners.values().forall(owner => owner.key.member == 4 and owner.phase == OwnerReady))
    .expect(world.extensions.metadata.groups.values().forall(g => g.owner == None))
    .expect(world.extensions.metadata.activeSessions.get("n3").owner == { node: "n3", incarnation: 2 }).expect(safety)

  run readmissionRejectsIncompleteOfferAndForeignPinTest = retainedReadmissionOffer.expect({
    val offer = world.system.get("n3").volatile.readmissionOffer.unwrap()
    val msg: Envelope = { delivery: offer.delivery, source: offer.sender, destination: "n3", channel: MetaChannel,
      payload: ReadmissionAccepted(offer) }
    val partial = { ...offer, grants: offer.grants.filter(g => g.key.group != "a") }
    val foreign = { ...world, system: world.system.setBy("n3", host => { ...host,
      durable: { ...host.durable, pin: Some({ cluster: "c1", node: "n3", member: 99 }) } }) }
    and { not(ReadmitHost::validOffer(world, { ...msg, payload: ReadmissionAccepted(partial) }, partial)),
      not(ReadmitHost::validOffer(foreign, msg, offer)) }
  }).expect(safety)

  run readmissionPartialBatchBlocksMetadataPreparationTest = readmissionJoined.then(retireReadmissionLoad("a"))
    .then(recordReadmissionProofs(world.system.get("n3").durable.receipts.filter(r => r.admission != None)))
    .then(commitAndApplyControl(BeginBlock({ operation: scaleAttempt(2), group: "_meta" })))
    .expect(ReadmitControls::errors(world.extensions.metadata, IssueEffect(scaleStep(2, PrepareReplica))).size() > 0)
    .expect(safety)

  run readmissionOriginProofCannotBePrunedTest = readmissionLocallyComplete
    .then(recordReadmissionProofs(world.system.get("n3").durable.receipts.filter(r => r.admission != None)))
    .expect({
      val missingProof = { ...world, extensions: { ...world.extensions, metadata: { ...world.extensions.metadata,
        readmissionOrigins: world.extensions.metadata.readmissionOrigins.mapRemove(readmissionKey) } } }
      not(ReadmitChecks::safety(missingProof))
    }).expect(safety)

  run fencedReturningHostMustObtainNewApprovalTest = init.then(growNode("n4", 1, 2))
    .then(removeNode("n4", 2, 3)).then(crash("n4")).then(restart("n4"))
    .then(requestReadmissionJoin("n4", "n1")).then({
      nondet msg = world.extensions.network.pending.filter(m => match m.payload {
        | JoinRequest(j) => j.owner.node == "n4" | _ => false }).oneOf()
      deliver(msg).then(confirmJoin(msg)).expect(ReadmitJoin::bindCommand(world, msg) == None)
    }).expect(world.system.get("n4").volatile.phase == Fenced)
    .expect(not(world.system.get("n4").volatile.metaRunning))
    .expect(world.system.get("n4").durable.pin == Some({ cluster: "c1", node: "n4", member: 4 })).expect(safety)

  run deletedGroupReadmissionSettlesDebtAndCollectsViaSuccessorTest = init.then(crash("n3"))
    .then(revokeReadmissionPredecessor)
    .then(approveScaleEdit(DeleteGroup("a"), 1, 2))
    .then(approveScaleEdit(ReadmitNode({ name: "n3", expectedMember: 3 }), 2, 3))
    .then(bindRetainedReadmission).then(joinRetainedReadmission).then(finishRetainedReadmission)
    .then(installReadmissionPin("n3")).then(startJoinedMeta("n3"))
    .then(recordReadmissionProofs(world.system.get("n3").durable.receipts.filter(r => r.admission != None)))
    .then(readyReadmittedProcess).then(executeMetaBlock(3)).then(activateJoinedNode("n3"))
    .then({
      val key: ReplicaKey = { ...predecessor, group: "a" }
      val receipt = world.extensions.metadata.cleanup.get(key).retired.unwrap()
      retentionDecision("n3", key, true).then(prepareCollection("n3", key, receipt))
        .then(collectRetired("n3", key)).then(completeCollection("n3", key))
        .then(reportCollection("n3", key)).then({
          nondet msg = world.extensions.network.pending.filter(m => match m.payload {
            | MetaProposal(p) => match p.command { | ControlProposal(c) => match c {
              | RecordCollection(r) => r.receipt.identity == key | _ => false } | _ => false } | _ => false }).oneOf()
          deliver(msg).then(commitMutation(msg))
        }).then({
          nondet ack = world.extensions.network.pending.filter(m => match m.payload {
            | MetaAck(a) => world.system.get("n3").volatile.collectionRequests.keys().contains(a.key) | _ => false }).oneOf()
          deliver(ack).then(receiveCollectionAck(ack))
        }).then(releaseCollectionMarker("n3", key)).then(removeCollectionJournal("n3", key))
        .expect(world.extensions.metadata.cleanup.get(key).collected != None)
        .expect(world.extensions.metadata.cleanup.get(key).sourceOperation == scaleAttempt(2))
    }).expect(world.extensions.metadata.operations.get(scaleAttempt(2)).blocks.get("a").phase == OpSucceeded)
    .expect(world.extensions.metadata.cleanup.values().filter(d => d.identity.group == "a" and d.identity.node != "n3")
      .forall(d => d.stopped == None and d.retired == None)).expect(safety)

  action readmissionStageProbe = world' = world
  run readmissionSharesWorldTest = clusterStageInit.then(capacityStageProbe).then(readmissionStageProbe)
}
