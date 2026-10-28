module cleanup_test {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import groups_test.* from "./groups_test"
  import grants as CleanupGrants from "../../metaengine/grants"
  import managed_retirement_steps as RetirementRules from "../../managed/retirement_steps"
  import managed_collection_steps as CollectionRules from "../../managed/collection_steps"
  import controls as CleanupControls from "../../metaengine/controls"
  import cleanup_properties as CleanupChecks from "../../harness/cleanup_properties"
  import managed_node_steps as CleanupNodeSteps from "../../managed/node_steps"
  export groups_test.*

  def debtKey(node: NodeName): ReplicaKey = world.extensions.metadata.cleanup.keys().filter(r => r.node == node and r.group == "a").getOnlyElement()
  def retiringGrant(node: NodeName): Grant = world.extensions.metadata.grants.values().filter(g =>
    g.targets == Set(debtKey(node)) and match g.effect {
      | Some(e) => Set(RetireReplica, DeleteGroupState).contains(e.kind) | None => false }).getOnlyElement()

  action stopDeletedOwner(node: NodeName): bool = {
    val key = debtKey(node)
    val approved = world.extensions.metadata.cleanup.get(key).stopAction.unwrap()
    commitAndApplyControl(IssueCleanupStop(approved)).then({
      val grant = CleanupGrants::actionGrants(world.extensions.metadata, approved).getOnlyElement()
      dispatchGrant("n1", grant.key, HostResource({ group: "a", node: node })).then(receiveQueuedGrant)
        .then({ nondet d = world.system.get(node).volatile.waitingEffects.filter(d => d.grant == grant).oneOf()
          beginGrantedStop(d).then(closeLocal(node, "a")).then(finishGrantedStop(d)) })
        .then({ nondet receipt = world.system.get(node).durable.receipts.filter(r => r.grant == grant.key).oneOf()
          reportHostEffect("n1", receipt).then(controlCommit(RecordEffect(receipt))) }).then(applyMeta("n1"))
    }).expect(world.extensions.metadata.cleanup.get(key).stopped != None).expect(safety)
  }

  action requestRetirement(node: NodeName): bool = {
    val key = debtKey(node)
    val approved = world.extensions.metadata.cleanup.get(key).retireAction.unwrap()
    commitAndApplyControl(IssueRetirement({ identity: key, approved: approved }))
      .then(dispatchGrant("n1", retiringGrant(node).key, HostResource({ group: "a", node: node })))
      .then(receiveQueuedGrant).then({
        nondet d = world.system.get(node).volatile.waitingEffects.filter(d => d.grant == retiringGrant(node)).oneOf()
        beginRetirement(d)
      })
  }

  action finishExactRetirement(node: NodeName): bool = {
    val key = debtKey(node)
    val d = world.system.get(node).volatile.retirementWork.get(key).delivery
    returnRetirement(node, key, RetirementRules::resultFor(d))
      .then(quarantineRetirement(node, key))
      .expect(world.extensions.metadata.cleanup.get(key).retired == None)
      .then(finishRetirement(node, key)).then({
        nondet receipt = world.system.get(node).durable.receipts.filter(r => r.grant == d.grant.key).oneOf()
        reportHostEffect("n1", receipt).then(controlCommit(RecordEffect(receipt)))
      }).then(applyMeta("n1")).expect(safety)
  }

  action retiredFirst = deletedWithOfflineOwner.then(stopDeletedOwner("n1"))
    .then(requestRetirement("n1")).then(finishExactRetirement("n1"))

  action prepareFirstCollection = {
    val key = debtKey("n1")
    val receipt = world.extensions.metadata.cleanup.get(key).retired.unwrap()
    retentionDecision("n1", key, true).then(prepareCollection("n1", key, receipt))
  }

  action reportAndAcknowledge(node: NodeName, key: ReplicaKey): bool = reportCollection(node, key)
    .then({
      nondet msg = world.extensions.network.pending.filter(m => match m.payload {
        | MetaProposal(p) => match p.command { | ControlProposal(c) => match c {
          | RecordCollection(r) => r.receipt.identity == key | _ => false } | _ => false } | _ => false }).oneOf()
      deliver(msg).then(commitMutation(msg))
    }).then({
      nondet ack = world.extensions.network.pending.filter(m => match m.payload {
        | MetaAck(a) => world.system.get(node).volatile.collectionRequests.keys().contains(a.key) | _ => false }).oneOf()
      deliver(ack).then(receiveCollectionAck(ack))
    }).expect(world.extensions.metadata.cleanup.get(key).collected != None).expect(safety)

  run deletionRetiresOnlineOwnerWithoutOfflineOwnerTest = retiredFirst
    .expect(world.extensions.metadata.cleanup.get(debtKey("n1")).retired != None)
    .expect(world.extensions.metadata.cleanup.get(debtKey("n3")).stopped == None)
    .expect(world.extensions.metadata.operations.get(groupAttempt(2)).blocks.get("a").phase == OpSucceeded)

  run retirementBareVerifiedFlagIsInsufficientTest = deletedWithOfflineOwner.then(stopDeletedOwner("n1"))
    .then(requestRetirement("n1")).then({
      val key = debtKey("n1")
      val d = world.system.get("n1").volatile.retirementWork.get(key).delivery
      returnRetirement("n1", key, { ...RetirementRules::resultFor(d), identity: { ...key, replica: 99 } })
    }).expect(world.system.get("n1").durable.retiredStores == Map())
    .expect(world.extensions.metadata.cleanup.get(debtKey("n1")).retired == None).expect(safety)

  run retirementMissingDataDoesNotSkipAuthorityTest = deletedWithOfflineOwner.then(stopDeletedOwner("n1"))
    .then({ world' = { ...world, system: world.system.setBy("n1", n => { ...n, durable: { ...n.durable,
      replicaEvidence: n.durable.replicaEvidence.mapRemove(debtKey("n1")) } }) } })
    .then(requestRetirement("n1")).then({
      val key = debtKey("n1")
      val d = world.system.get("n1").volatile.retirementWork.get(key).delivery
      returnRetirement("n1", key, { ...RetirementRules::resultFor(d), verified: false })
    }).expect(world.extensions.metadata.cleanup.get(debtKey("n1")).retired == None)
    .expect(not(world.system.get("n1").durable.replicaEvidence.keys().contains(debtKey("n1")))).expect(safety)

  run collectionRequiresRetentionDecisionTest = retiredFirst.expect(not(CollectionRules::prepareEnabled(world, "n1", debtKey("n1"),
    world.extensions.metadata.cleanup.get(debtKey("n1")).retired.unwrap(), MAX_POSITION)))

  run collectionPhysicalEffectDoesNotReportCompletionTest = retiredFirst.then(prepareFirstCollection)
    .then(collectRetired("n1", debtKey("n1")))
    .expect(world.extensions.metadata.cleanup.get(debtKey("n1")).collected == None)
    .expect(not(CollectionRules::reportEnabled(world, "n1", debtKey("n1"), MAX_DELIVERIES))).expect(safety)

  run collectionReportsBeforeMarkerReleaseTest = retiredFirst.then(prepareFirstCollection)
    .then(collectRetired("n1", debtKey("n1"))).then(completeCollection("n1", debtKey("n1")))
    .expect(not(CollectionRules::releaseEnabled(world, "n1", debtKey("n1"))))
    .then(reportAndAcknowledge("n1", debtKey("n1")))
    .expect(not(CollectionRules::removeEnabled(world, "n1", debtKey("n1"))))
    .then(releaseCollectionMarker("n1", debtKey("n1"))).then(removeCollectionJournal("n1", debtKey("n1")))
    .expect(world.system.get("n1").durable.collectionMarkers == Map())
    .expect(world.extensions.metadata.cleanup.get(debtKey("n3")).retired == None).expect(safety)

  run collectionResumesAfterPhysicalCrashTest = retiredFirst.then(prepareFirstCollection)
    .then(collectRetired("n1", debtKey("n1"))).then(crash("n1")).then(restart("n1"))
    .expect(world.system.get("n1").durable.collectionMarkers.get(debtKey("n1")).phase == Prepared)
    .expect(world.system.get("n1").volatile.collectionAcks == Set())
    .then(completeCollection("n1", debtKey("n1"))).expect(safety)

  run collectionTokenMismatchIsRejectedTest = retiredFirst.then(prepareFirstCollection).then({
    val key = debtKey("n1")
    action corruptToken = { world' = { ...world, system: world.system.setBy("n1", n => { ...n,
      durable: { ...n.durable, collectionMarkers: n.durable.collectionMarkers.setBy(key, row => { ...row, token: row.token + 1 }) } }) } }
    corruptToken.expect(not(CollectionRules::collectEnabled(world, "n1", key))).expect(not(CleanupChecks::safety(world)))
  })

  run collectionMarkerReleaseSurvivesCrashTest = retiredFirst.then(prepareFirstCollection)
    .then(collectRetired("n1", debtKey("n1"))).then(completeCollection("n1", debtKey("n1")))
    .then(reportAndAcknowledge("n1", debtKey("n1"))).then(releaseCollectionMarker("n1", debtKey("n1")))
    .then(crash("n1")).then(restart("n1"))
    .expect(world.system.get("n1").volatile.collectionAcks == Set())
    .expect(not(CollectionRules::collectEnabled(world, "n1", debtKey("n1"))))
    .then(observeAuthorityLeader("n1", "_meta"))
    .then(reportAndAcknowledge("n1", debtKey("n1"))).then(removeCollectionJournal("n1", debtKey("n1")))
    .expect(world.system.get("n1").durable.collectionMarkers == Map()).expect(safety)

  run nodeStopJoinsRetirementCallbackTest = deletedWithOfflineOwner.then(stopDeletedOwner("n1"))
    .then(requestRetirement("n1")).then(requestNodeStop("n1"))
    .expect(not(CleanupNodeSteps::joinAuthorityWorkersEnabled(world, "n1")))
    .expect(not(CleanupNodeSteps::finishNodeStopEnabled(world, "n1")))
    .then({
      val key = debtKey("n1")
      val work = world.system.get("n1").volatile.retirementWork.get(key)
      returnRetirement("n1", key, RetirementRules::resultFor(work.delivery))
    }).then(joinAuthorityWorkers("n1"))
    .expect(world.system.get("n1").volatile.retirementWork == Map())
    .expect(world.system.get("n1").durable.retiredStores == Map()).expect(safety)

  run collectionPreservesSuccessorResourcesTest = retiredFirst.then(prepareFirstCollection).then({
    // Adversarial local frame: collection uses the old exact token while a
    // different replica occupies the live path. No old cleanup may remove it.
    val old = debtKey("n1")
    val successor = { ...old, replica: 99 }
    action installSuccessor = { world' = { ...world, system: world.system.setBy("n1", host => { ...host,
      durable: { ...host.durable, replicaLedger: host.durable.replicaLedger.union(Set(successor)),
        replicaEvidence: host.durable.replicaEvidence.put(successor, ValidStore({ snapshot: 0, commit: 1, tail: 1, appliedHint: 1 })) } }) } }
    installSuccessor.then(collectRetired("n1", old)).then(completeCollection("n1", old))
      .expect(world.system.get("n1").durable.replicaLedger.contains(successor))
      .expect(world.system.get("n1").durable.replicaEvidence.keys().contains(successor))
      .expect(CleanupChecks::safety(world))
  })

  run collectionWaitsForRecordedRetirementTest = deletedWithOfflineOwner.then(stopDeletedOwner("n1"))
    .then(requestRetirement("n1")).then({
      val key = debtKey("n1")
      val d = world.system.get("n1").volatile.retirementWork.get(key).delivery
      returnRetirement("n1", key, RetirementRules::resultFor(d)).then(quarantineRetirement("n1", key))
        .then(finishRetirement("n1", key)).then({
          nondet proof = world.system.get("n1").durable.receipts.filter(r => r.grant == d.grant.key).oneOf()
          retentionDecision("n1", key, true).then(prepareCollection("n1", key, proof))
            .then(collectRetired("n1", key)).then(completeCollection("n1", key))
            .expect(world.extensions.metadata.cleanup.get(key).retired == None)
            .expect(CleanupControls::errors(world.extensions.metadata, RecordCollection({
              receipt: CollectionRules::collectionReceipt(world.system.get("n1").durable.collectionMarkers.get(key)),
              courier: { node: "n1", incarnation: 1 }, member: key.member })).contains(Unauthorized))
            .then(reportHostEffect("n1", proof)).then(controlCommit(RecordEffect(proof)))
            .then(reportAndAcknowledge("n1", key)).expect(safety)
        })
    })

  action cleanupStageProbe = world' = world
  run cleanupSharesWorldTest = clusterStageInit.then(groupsStageProbe).then(cleanupStageProbe)
}
