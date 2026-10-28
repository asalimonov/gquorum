module scaling_helpers {
  import operations as PlanningWork from "../../metaservice/operations"
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import admission_test.* from "./admission_test"
  import grants as SG from "../../metaengine/grants"
  import metaservice_admission_steps as JS from "../../metaservice/admission_steps"
  export admission_test.*

  pure def scaleKey(sequence: int): RequestKey = { caller: { node: "n2", incarnation: 1 }, sequence: sequence }
  pure def scaleAttempt(version: int): AttemptKey = { version: version, attempt: 1 }
  def scaleStep(version: int, kind: ActionKind): ApprovedAction = { operation: scaleAttempt(version),
    step: world.extensions.metadata.operations.get(scaleAttempt(version)).plan.blocks.get("_meta").select(s => s.kind == kind).head() }
  def metaReplica(node: NodeName): ReplicaKey = world.extensions.metadata.target.groups.get("_meta").replicas.filter(r => r.key.node == node).getOnlyElement().key

  // Scenario orchestration only: all production phases remain separately
  // selectable and every state update is through the shared model facade.
  action syncMeta(node: NodeName): bool = if (world.system.get(node).volatile.appliedMetaIndex <
    world.extensions.consensus.get("_meta").committedPosition) applyMeta(node) else { world' = world }

  action approveScaleEdit(edit: DocumentEdit, sequence: int, version: int): bool = {
    val key = scaleKey(sequence)
    val work = { request: key, sequence: 1 }
    syncMeta("n2")
      .then(beginMutation(key, Mutate({ edit: edit, options: { ...options, expectedVersion: 0, approve: true } })))
      .then(commitProposal(key)).then(receiveMutationAck(key)).then(applyMeta("n2")).then(finishMutation(key))
      .then(operationRead(work)).then(applyMeta("n1")).then(captureOperation("n1", work))
      .then(3.reps(_ => {
        val current = world.system.get("n1").volatile.operationWork.get(work)
        val missing = PlanningWork::requiredReads(current).exclude(current.queried)
        if (missing.size() == 0) { world' = world } else {
          nondet group = missing.oneOf()
          observeOperation("n1", work, group).then(observeOperationHealth("n1", work, group))
        }
      }))
      .then(commitWork(work)).then(receiveWorkResult(work))
      .expect(world.extensions.metadata.target.version == version)
      .expect(world.extensions.metadata.operations.keys().contains(scaleAttempt(version)))
      .expect(safety)
  }

  action bindScaleNode(node: NodeName): bool = beginNodeStart(node).then(requestJoin(node, "n1")).then({
    nondet msg = world.extensions.network.pending.filter(m => match m.payload {
      | JoinRequest(j) => j.owner.node == node | _ => false }).oneOf()
    deliver(msg).then(confirmJoin(msg)).then({
      val command = AdmissionControl(JS::bindCommand(world, msg).unwrap())
      bindJoin(msg).then(controlCommit(command))
    }).then(applyMeta("n1")).then(replyJoin(msg))
  }).then({
    nondet msg = world.extensions.network.pending.filter(m => match m.payload {
      | JoinAccepted(a) => a.admission.owner.node == node | _ => false }).oneOf()
    deliver(msg).then(receiveJoin(msg))
  }).then(pinJoin(node)).then(startJoinedMeta(node)).then(announcePrepared(node, "n1")).then({
    nondet msg = world.extensions.network.pending.filter(m => match m.payload {
      | JoinPrepared(a) => a.owner.node == node | _ => false }).oneOf()
    val command = match msg.payload { | JoinPrepared(a) => AdmissionControl(ReadyAdmission(a))
      | _ => AdmissionControl(ReadyAdmission(world.extensions.metadata.admissions.get(node))) }
    deliver(msg).then(acceptPrepared(msg)).then(controlCommit(command))
  }).then(applyMeta("n1"))

  action runNativeApproved(approved: ApprovedAction): bool = {
    val group = SG::groupOf(approved.step.target)
    commitAndApplyControl(IssueEffect(approved)).then({
      val grant = SG::actionGrants(world.extensions.metadata, approved).getOnlyElement()
      dispatchGrant("n1", grant.key, GroupResource(group))
    }).then(receiveQueuedGrant).then({
      nondet delivery = world.system.get("n1").volatile.waitingEffects.filter(d => d.grant.planStep == Some(approved.step)).oneOf()
      installNativeAuthority(delivery).then(if (approved.step.kind == PromoteLearner)
        catchUp(group, (match approved.step.target { | ReplicaTarget(r) => r | _ => metaReplica("n1") })) else { world' = world })
        .then(precheckNative(delivery)).then(proposeNative(delivery))
    }).then(commitNative(group)).then(reportAndCheckpoint(approved)).expect(safety)
  }

  action runMetaStep(approved: ApprovedAction): bool = match approved.step.kind {
    | PrepareReplica => commitAndApplyControl(IssueEffect(approved)).then({
        val grant = SG::actionGrants(world.extensions.metadata, approved).getOnlyElement()
        nondet resource = grant.resources.oneOf()
        dispatchGrant("n1", grant.key, resource)
      }).then(receiveQueuedGrant).then({
        val target = match approved.step.target { | ReplicaTarget(r) => r.node | _ => "" }
        nondet delivery = world.system.get(target).volatile.waitingEffects.filter(d => d.grant.planStep == Some(approved.step)).oneOf()
        if (delivery.grant.key.group == "_meta") prepareMeta(delivery) else
          syncMeta(target).then(beginLoadPreparation(delivery)).then({
            nondet work = world.system.get(target).volatile.starts.keys().filter(k =>
              world.system.get(target).volatile.starts.get(k).key == delivery.replica).oneOf()
            readStartScope(work).then(reserveOwner(work)).then(startupBody(target, delivery.grant.key.group))
              .then(replyStart(work)).then(finishLoadPreparation(delivery))
          })
      }).then(reportAndCheckpoint(approved))
    | WaitCatchUp => {
        val replica = match approved.step.target { | ReplicaTarget(r) => r | _ => metaReplica("n1") }
        catchUp(replica.group, replica).then(checkpointCatchUp("n1", approved))
          .then(controlCommit(CompleteCatchUp(approved))).then(applyMeta("n1"))
      }
    | AddLearner => runNativeApproved(approved)
    | PromoteLearner => runNativeApproved(approved)
    | DemoteVoter => runNativeApproved(approved)
    | RemoveMember => runNativeApproved(approved)
    | _ => all { false, world' = world }
  }

  action executeGroupBlock(version: int, group: GroupId): bool = {
    val key = scaleAttempt(version)
    val steps = world.extensions.metadata.operations.get(key).plan.blocks.get(group)
    (if (steps.length() == 0) { world' = world } else
      (if (world.extensions.metadata.operations.get(key).blocks.get(group).phase == OpPending)
        commitAndApplyControl(BeginBlock({ operation: key, group: group })) else { world' = world })
        .then(8.reps(i => if (i < steps.length()) runMetaStep({ operation: key, step: steps.nth(i) }) else { world' = world })))
      .expect(steps.length() <= 8)
      .expect(world.extensions.metadata.operations.get(key).blocks.get(group).phase == OpSucceeded)
  }

  action executeMetaBlock(version: int): bool = executeGroupBlock(version, "_meta")

  action activateJoinedNode(node: NodeName): bool = {
    val session: SessionKey = { owner: { node: node, incarnation: world.system.get(node).volatile.incarnation }, generation: 1 }
    syncMeta(node).then(observeMetaLeader(node)).then(beginSession(node)).then(proposeSession(node))
      .then(controlCommit(SessionControl(GrantInternalSession({ key: session, member: metaReplica(node).member }))))
      .then(applyMeta(node)).then(proposeSession(node))
      .then(controlCommit(SessionControl(BindInternalSession(session)))).then(applyMeta(node)).then(applyMeta("n1"))
      .then(renewSession(node)).then(sessionMessage("renew", "n1")).then(sessionMessage("renewed", node))
      .expect(world.system.get(node).volatile.phase == Ready)
  }

  action growNode(node: NodeName, sequence: int, version: int): bool =
    approveScaleEdit(AddNode({ name: node, member: 0, state: Active, nodeType: "worker", labels: Map() }), sequence, version)
      .then(bindScaleNode(node)).then(executeMetaBlock(version)).then(activateJoinedNode(node))
      .expect(world.extensions.metadata.operations.get(scaleAttempt(version)).blocks.get("_meta").phase == OpSucceeded)
      .expect(world.extensions.metadata.nodes.get(node).proven == Some(world.extensions.metadata.target.nodes.get(node)))
      .expect(safety)

  action removeNode(node: NodeName, sequence: int, version: int): bool =
    approveScaleEdit(DrainNode(node), sequence, version)
      .expect(world.extensions.metadata.nodes.get(node).proven == Some(world.extensions.metadata.target.nodes.get(node)))
      .then(approveScaleEdit(ExcludeNode(node), sequence + 1, version + 1))
      .then(executeMetaBlock(version + 1)).then({
        nondet receipt = world.extensions.consensus.get("_meta").authorityReceipts.filter(r =>
          (match r.target { | Some(key) => key.node == node | _ => false }) and
          (match r.effect { | Some(e) => e.kind == RemoveMember | _ => false })).oneOf()
        applyRemovedMeta(node, receipt)
      }).expect(world.system.get(node).volatile.phase == Fenced).expect(safety)

  run removedMemberStaysFencedAfterRestartTest = init.then(growNode("n4", 1, 2))
    .then(removeNode("n4", 2, 3)).then(crash("n4")).then(restart("n4"))
    .expect(world.system.get("n4").volatile.phase == Fenced)
    .expect(not(world.system.get("n4").volatile.metaRunning))
    .expect(world.system.get("n4").durable.pin == Some({ cluster: "c1", node: "n4", member: 4 })).expect(safety)
    .then(requestNodeStop("n4")).then(finishNodeStop("n4")).expect(safety)

  run drainedMetadataMemberCanActivateTest = init.then(growNode("n4", 1, 2))
    .then(approveScaleEdit(DrainNode("n4"), 2, 3))
    .expect(world.extensions.metadata.nodes.get("n4").proven == Some(world.extensions.metadata.target.nodes.get("n4")))
    .then(approveScaleEdit(ActivateNode("n4"), 3, 4))
    .expect(world.extensions.metadata.target.nodes.get("n4").state == Active)
    .expect(world.extensions.consensus.get("_meta").voters.size() == 4)
    .expect(world.system.get("n4").durable.metaRemoved == None).expect(safety)

  action stopEvacuatedLoad(node: NodeName, group: GroupId): bool = {
    nondet key = world.extensions.metadata.cleanup.keys().filter(r => r.node == node and r.group == group).oneOf()
    val approved = world.extensions.metadata.cleanup.get(key).stopAction.unwrap()
    commitAndApplyControl(IssueCleanupStop(approved)).then({
      val grant = SG::actionGrants(world.extensions.metadata, approved).filter(g => SG::currentForIssuance(world.extensions.metadata, g)).getOnlyElement()
      dispatchGrant("n1", grant.key, HostResource({ group: group, node: node }))
    }).then(receiveQueuedGrant).then({
      nondet delivery = world.system.get(node).volatile.waitingEffects.filter(d => d.grant.planStep == Some(approved.step)).oneOf()
      beginGrantedStop(delivery).then(closeLocal(node, group)).then(finishGrantedStop(delivery))
    }).then({
      nondet receipt = world.system.get(node).durable.receipts.filter(r => SG::matchesAction(r, Some(approved))).oneOf()
      reportHostEffect("n1", receipt).then(controlCommit(RecordEffect(receipt)))
    }).then(applyMeta("n1"))
      .expect(world.extensions.metadata.cleanup.get(key).stopped != None)
      .expect(world.extensions.metadata.cleanup.get(key).retired == None).expect(safety)
  }

  run drainEvacuatesTwoLoadsBeforeExclusionTest = init.then(growNode("n4", 1, 2))
    .then(approveScaleEdit(DrainNode("n3"), 2, 3))
    .expect(world.extensions.metadata.nodes.get("n3").proven != Some(world.extensions.metadata.target.nodes.get("n3")))
    .then(executeGroupBlock(3, "a"))
    .expect(world.extensions.metadata.nodes.get("n3").proven != Some(world.extensions.metadata.target.nodes.get("n3")))
    .then(executeGroupBlock(3, "b"))
    .expect(world.extensions.metadata.nodes.get("n3").proven == Some(world.extensions.metadata.target.nodes.get("n3")))
    .then(approveScaleEdit(ExcludeNode("n3"), 3, 4))
    .then(commitAndApplyControl(BeginBlock({ operation: scaleAttempt(4), group: "_meta" })))
    .expect(not(SG::canIssueStep(world.extensions.metadata, scaleStep(4, RemoveMember))))
    .then(stopEvacuatedLoad("n3", "a"))
    .expect(not(SG::canIssueStep(world.extensions.metadata, scaleStep(4, RemoveMember))))
    .then(stopEvacuatedLoad("n3", "b")).then(executeMetaBlock(4))
    .expect(world.extensions.consensus.get("_meta").voters.map(r => r.node) == Set("n1", "n2", "n4"))
    .expect(world.extensions.consensus.get("a").voters.map(r => r.node) == Set("n1", "n2", "n4"))
    .expect(world.extensions.consensus.get("b").voters.map(r => r.node) == Set("n1", "n2", "n4"))
    .expect(world.system.get("n4").volatile.owners.get("a").published)
    .expect(world.system.get("n4").volatile.owners.get("b").published).expect(safety)

  run failedFreshFactoryResumesExactPreparationTest = init.then(growNode("n4", 1, 2))
    .then(approveScaleEdit(DrainNode("n3"), 2, 3))
    .then(commitAndApplyControl(BeginBlock({ operation: scaleAttempt(3), group: "a" })))
    .then({
      val approved: ApprovedAction = { operation: scaleAttempt(3),
        step: world.extensions.metadata.operations.get(scaleAttempt(3)).plan.blocks.get("a").head() }
      commitAndApplyControl(IssueEffect(approved)).then({
        val grant = SG::actionGrants(world.extensions.metadata, approved).getOnlyElement()
        dispatchGrant("n1", grant.key, HostResource({ group: "a", node: "n4" }))
      }).then(receiveQueuedGrant).then(syncMeta("n4")).then({
        nondet delivery = world.system.get("n4").volatile.waitingEffects.filter(d => d.grant.planStep == Some(approved.step)).oneOf()
        val first: RequestKey = { caller: { node: "n4", incarnation: 1 }, sequence: 1 }
        val retry: RequestKey = { caller: { node: "n4", incarnation: 1 }, sequence: 2 }
        beginLoadPreparation(delivery).then(readStartScope(first)).then(reserveOwner(first))
          .then(beginOwnerFactory("n4", "a")).then(returnOwnerFactory("n4", "a", false)).then(releaseOwner("n4", "a"))
          .then(beginLoadPreparation(delivery)).then(readStartScope(retry)).then(reserveOwner(retry))
          .then(startupBody("n4", "a")).then(replyStart(retry)).then(finishLoadPreparation(delivery))
      })
    }).expect(world.system.get("n4").volatile.owners.get("a").published)
    .expect(world.system.get("n4").durable.receipts.exists(r => r.resource == HostResource({ group: "a", node: "n4" })))
    .expect(world.extensions.consensus.get("a").voters.forall(r => r.node != "n4")).expect(safety)

  action capacityStageProbe = world' = world
  run capacitySharesWorldTest = clusterStageInit.then(admissionStageProbe).then(capacityStageProbe)
    .expect(world.system.keys() == NODE_POOL)
}
