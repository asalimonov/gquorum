module groups_test {
  import properties as GroupChecks from "../../harness/properties"
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import lifetime_test.* from "./lifetime_test"
  import operations as Work from "../../metaservice/operations"
  import grants as GroupGrants from "../../metaengine/grants"
  import membership as GroupMembership from "../../consensus/membership"
  export lifetime_test.*

  pure def groupKey(sequence: int): RequestKey = { caller: { node: "n2", incarnation: 1 }, sequence: sequence }
  pure def groupAttempt(version: int): AttemptKey = { version: version, attempt: 1 }
  pure def createSpec(hosts: List[NodeName]): GroupSpec = {
    id: "c", labels: Map(), storage: StorageDefault, placement: None, nodes: None, voterConfig: None,
    hosting: Some({ mode: ExplicitHosts, selectedNodes: hosts }),
  }
  def founder(node: NodeName): ReplicaKey = world.extensions.metadata.target.groups.get("c").replicas.filter(r => r.key.node == node).getOnlyElement().key
  val foundingAction: ApprovedAction = { operation: groupAttempt(2),
    step: world.extensions.metadata.operations.get(groupAttempt(2)).plan.blocks.get("c").head() }
  val foundingGrant = GroupGrants::actionGrants(world.extensions.metadata, foundingAction).getOnlyElement()

  action syncGroupMeta(node: NodeName): bool = {
    val key = world.extensions.consensus.get("_meta").voters.union(world.extensions.consensus.get("_meta").learners)
      .filter(r => r.node == node).getOnlyElement()
    val stored = match world.system.get(node).durable.replicaEvidence.get(key) { | ValidStore(p) => p.commit | _ => 0 }
    (if (stored < world.extensions.consensus.get("_meta").committedPosition) catchUp("_meta", key) else { world' = world })
      .then(if (world.system.get(node).volatile.appliedMetaIndex < world.extensions.consensus.get("_meta").committedPosition)
        applyMeta(node) else { world' = world })
  }

  action approveGroupEdit(edit: DocumentEdit, sequence: int, version: int): bool = {
    val key = groupKey(sequence)
    val work = { request: key, sequence: 1 }
    syncGroupMeta("n2")
      .then(beginMutation(key, Mutate({ edit: edit, options: { ...options, expectedVersion: 0, approve: true } })))
      .then(commitProposal(key)).then(receiveMutationAck(key)).then(applyMeta("n2")).then(finishMutation(key))
      .then(operationRead(work)).then(applyMeta("n1")).then(captureOperation("n1", work))
      .then(4.reps(_ => {
        val current = world.system.get("n1").volatile.operationWork.get(work)
        val missing = Work::requiredReads(current).exclude(current.queried)
        if (missing.size() == 0) { world' = world } else {
          nondet group = missing.oneOf()
          observeOperation("n1", work, group).then(observeOperationHealth("n1", work, group))
        }
      })).then(commitWork(work)).then(receiveWorkResult(work))
      .expect(world.extensions.metadata.target.version == version)
      .expect(world.extensions.metadata.operations.keys().contains(groupAttempt(version))).expect(safety)
  }

  action issueFounding = commitAndApplyControl(BeginBlock({ operation: groupAttempt(2), group: "c" }))
    .then(commitAndApplyControl(IssueEffect(foundingAction)))

  action startFounder(node: NodeName): bool = syncGroupMeta(node)
    .then(dispatchGrant("n1", foundingGrant.key, HostResource({ group: "c", node: node })))
    .then(receiveQueuedGrant).then({
      nondet d = world.system.get(node).volatile.waitingEffects.filter(d => d.grant == foundingGrant).oneOf()
      beginLoadPreparation(d).then({
        nondet request = world.system.get(node).volatile.starts.keys().filter(k =>
          world.system.get(node).volatile.starts.get(k).key == d.replica).oneOf()
        readStartScope(request).then(reserveOwner(request)).then(startupBody(node, "c"))
          .then(replyStart(request)).then(finishLoadPreparation(d))
      })
    }).then({
      nondet receipt = world.system.get(node).durable.receipts.filter(r => r.grant == foundingGrant.key).oneOf()
      reportHostEffect("n1", receipt).then(controlCommit(RecordEffect(receipt)))
    }).then(applyMeta("n1")).expect(safety)

  action completeFounding = elect("c", founder("n1")).then(observeAuthorityLeader("n1", "c"))
    .then(dispatchGrant("n1", foundingGrant.key, GroupResource("c"))).then(receiveQueuedGrant)
    .then({ nondet d = world.system.get("n1").volatile.waitingEffects.filter(d => d.resource == GroupResource("c")).oneOf()
      confirmFounding(d) })
    .expect(world.extensions.metadata.operations.get(groupAttempt(2)).blocks.get("c").phase != OpSucceeded)
    .then({ nondet receipt = world.extensions.consensus.get("c").authorityReceipts.oneOf()
      reportNative("n1", "c", RecordEffect(receipt)).then(controlCommit(RecordEffect(receipt))) })
    .then(applyMeta("n1")).then(checkpointEffect("n1", foundingAction))
    .then(controlCommit(CompleteStep({ approved: foundingAction, observedLeader: world.extensions.consensus.get("c").leaderAuthority })))
    .then(applyMeta("n1"))
    .expect(world.extensions.metadata.operations.get(groupAttempt(2)).blocks.get("c").phase == OpSucceeded).expect(safety)

  action healthyCreated = init.then(approveGroupEdit(CreateGroup(createSpec(List("n1", "n2", "n3"))), 1, 2))
    .then(issueFounding).then(startFounder("n1")).then(startFounder("n2")).then(startFounder("n3")).then(completeFounding)

  run healthyGroupCreationReachesNativeProofTest = healthyCreated
    .expect(world.extensions.consensus.get("c").voters.size() == 3)
    .expect(world.extensions.metadata.groups.get("c").proven == Some(world.extensions.metadata.target.groups.get("c")))

  run degradedCreationUsesExplicitHealthyHostsTest = init.then(crash("n3"))
    .then(approveGroupEdit(CreateGroup(createSpec(List("n1", "n2"))), 1, 2))
    .then(issueFounding).then(startFounder("n1")).then(startFounder("n2")).then(completeFounding)
    .expect(world.extensions.consensus.get("c").voters.map(r => r.node) == Set("n1", "n2"))
    .expect(world.system.get("n3").volatile.owners.keys().size() == 0).expect(safety)

  run requiredOfflineFounderPreventsCompletionTest = init.then(crash("n3"))
    .then(approveGroupEdit(CreateGroup(createSpec(List("n1", "n2", "n3"))), 1, 2))
    .then(issueFounding).then(startFounder("n1")).then(startFounder("n2"))
    .then(elect("c", founder("n1"))).then(commitProgress("c")).then(observeAuthorityLeader("n1", "c"))
    .expect(world.extensions.metadata.operations.get(groupAttempt(2)).blocks.get("c").phase == OpRunning)
    .then(dispatchGrant("n1", foundingGrant.key, GroupResource("c")).fail())

  run returningRequiredFounderJoinsSameGenesisTest = init.then(crash("n3"))
    .then(approveGroupEdit(CreateGroup(createSpec(List("n1", "n2", "n3"))), 1, 2))
    .then(issueFounding).then(startFounder("n1")).then(startFounder("n2"))
    .then(elect("c", founder("n1"))).then(commitProgress("c"))
    .then(restart("n3")).then(startFounder("n3"))
    .expect(world.extensions.consensus.get("c").committedPosition == 1)
    .then(observeAuthorityLeader("n1", "c"))
    .then(dispatchGrant("n1", foundingGrant.key, GroupResource("c"))).then(receiveQueuedGrant)
    .then({ nondet d = world.system.get("n1").volatile.waitingEffects.filter(d => d.resource == GroupResource("c")).oneOf()
      confirmFounding(d) }).expect(safety)

  run deletionWithoutLoadQuorumRetainsEveryOwnerTest = init
    .then(stopOwner("n2", { cluster: "c1", group: "a", node: "n2", member: 2, replica: 2 }, true)).then(closeLocal("n2", "a"))
    .then(stopOwner("n3", { cluster: "c1", group: "a", node: "n3", member: 3, replica: 3 }, true)).then(closeLocal("n3", "a"))
    .expect(not(GroupMembership::hasCommitPath(world.extensions.consensus.get("a"), world.system, world.extensions.network.loadLinks)))
    .then(approveGroupEdit(DeleteGroup("a"), 1, 2))
    .expect(world.extensions.metadata.operations.get(groupAttempt(2)).blocks.get("a").phase == OpSucceeded)
    .expect(world.extensions.metadata.cleanup.keys().filter(r => r.group == "a").size() == 3)
    .expect(world.extensions.metadata.cleanup.values().forall(d => d.stopped == None))
    .expect(world.system.get("n1").volatile.owners.get("a").engineRunning)
    .then(commitProgress("b")).expect(world.extensions.consensus.get("b").committedPosition == 2).expect(safety)

  action deletedWithOfflineOwner = init.then(crash("n3")).then(approveGroupEdit(DeleteGroup("a"), 1, 2))

  run offlineDeletionDoesNotFabricateStopProofTest = deletedWithOfflineOwner
    .expect(world.extensions.metadata.cleanup.keys().filter(r => r.node == "n3" and r.group == "a").size() == 1)
    .expect(world.extensions.metadata.cleanup.values().forall(d => d.stopped == None and d.retired == None))
    .expect(world.extensions.metadata.retiredGroupIds.contains("a"))
    .then(commitProgress("b")).expect(safety)

  run missingDeletionDebtIsDetectedTest = deletedWithOfflineOwner.then({
    nondet missing = world.extensions.metadata.cleanup.keys().filter(r => r.node == "n3").oneOf()
    world' = { ...world, extensions: { ...world.extensions, metadata: { ...world.extensions.metadata,
      cleanup: world.extensions.metadata.cleanup.mapRemove(missing) } } }
  }).expect(not(GroupChecks::deletionDebtComplete(world)))

  run committedDeletionRejectsGroupIdReuseTest = deletedWithOfflineOwner.then(syncGroupMeta("n2"))
    .then(invoke("n2", Mutate({ edit: CreateGroup({ ...createSpec(List("n1", "n2")), id: "a" }), options: options })))
    .then(deliverApi(groupKey(2))).then(handle("n2")).then(prepareMutation(groupKey(2)))
    .expect(match world.system.get("n2").volatile.pendingCalls.get(groupKey(2)).result {
      | Some(result) => match result { | MutationResult(outcome) => outcome.errors == Set(Invalid) | _ => false }
      | None => false })
    .expect(not(world.extensions.metadata.target.groups.keys().contains("a"))).expect(safety)

  run deletionSupersedesIncompleteFoundingWithExactDebtTest = init
    .then(approveGroupEdit(CreateGroup(createSpec(List("n1", "n2", "n3"))), 1, 2)).then(issueFounding)
    .then(startFounder("n1")).then(crash("n3"))
    .then(approveGroupEdit(DeleteGroup("c"), 2, 3))
    .expect(world.extensions.metadata.operations.get(groupAttempt(3)).blocks.get("c").phase == OpSucceeded)
    .expect(world.extensions.metadata.operations.get(groupAttempt(2)).blocks.get("c").phase == Superseded)
    .expect(world.extensions.metadata.cleanup.keys().filter(r => r.group == "c").size() == 3)
    .expect(world.extensions.metadata.closedGrants.contains(foundingGrant.key))
    .expect(world.extensions.metadata.revocations.get(foundingGrant.key).provenResources.size() <
      world.extensions.metadata.revocations.get(foundingGrant.key).requiredResources.size())
    .expect(world.system.get("n1").volatile.owners.get("c").published).expect(safety)

  run stagedCreationCannotStartAReplicaTest = init
    .then(beginMutation(groupKey(1), Mutate({ edit: CreateGroup(createSpec(List("n1", "n2"))), options: options })))
    .then(commitProposal(groupKey(1))).then(applyMeta("n1"))
    .then({
      val replica = world.extensions.metadata.latest.groups.get("c").replicas.filter(r => r.key.node == "n1").getOnlyElement().key
      val request: RequestKey = { caller: { node: "n1", incarnation: 1 }, sequence: 1 }
      requestStart("n1", replica, true).then(readStartScope(request)).then(reserveOwner(request))
    }).expect(not(world.extensions.consensus.keys().contains("c")))
    .expect(not(world.system.get("n1").volatile.owners.keys().contains("c")))
    .expect(world.extensions.metadata.grants.keys().size() == 0).expect(safety)

  run healthyCreationThenDeletionKeepsPhysicalDebtTest = healthyCreated
    .then(approveGroupEdit(DeleteGroup("c"), 2, 3))
    .expect(world.extensions.metadata.operations.get(groupAttempt(3)).blocks.get("c").phase == OpSucceeded)
    .expect(world.extensions.metadata.cleanup.keys().filter(r => r.group == "c").size() == 3)
    .expect(world.system.values().forall(n => n.volatile.owners.get("c").published))
    .expect(world.extensions.metadata.retiredGroupIds.contains("c")).expect(safety)

  run deletionKeepsUnavailableNativeRevocationIndependentTest = init
    .then(approveGroupEdit(ChangeReplica({ group: "a", node: "n1", role: Learner }), 1, 2))
    .then(commitAndApplyControl(BeginBlock({ operation: groupAttempt(2), group: "a" })))
    .then(commitAndApplyControl(IssueEffect(approvedTransfer)))
    .then(stopOwner("n2", { cluster: "c1", group: "a", node: "n2", member: 2, replica: 2 }, true)).then(closeLocal("n2", "a"))
    .then(stopOwner("n3", { cluster: "c1", group: "a", node: "n3", member: 3, replica: 3 }, true)).then(closeLocal("n3", "a"))
    .then(approveGroupEdit(DeleteGroup("a"), 2, 3))
    .expect(world.extensions.metadata.operations.get(groupAttempt(3)).blocks.get("a").phase == OpSucceeded)
    .expect(world.extensions.metadata.revocations.get(approvedTransferGrant.key).requiredResources == Set(GroupResource("a")))
    .expect(world.extensions.metadata.revocations.get(approvedTransferGrant.key).provenResources == Set())
    .then(approveGroupEdit(ChangeReplica({ group: "b", node: "n3", role: Learner }), 3, 4))
    .expect(world.extensions.metadata.groups.get("b").owner == Some(groupAttempt(4)))
    .then(commitProgress("b")).expect(safety)

  action groupsStageProbe = world' = world
  run groupsShareWorldTest = clusterStageInit.then(lifetimeStageProbe).then(groupsStageProbe)
    .expect(world.system.keys() == NODE_POOL)
}
