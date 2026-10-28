module admission_test {
  import admission as AdmissionRules from "../../metaengine/admission"
  import managed_membership_steps as AdmissionNative from "../../managed/membership_steps"
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import native_test.* from "./native_test"
  import grants as JoinGrants from "../../metaengine/grants"
  import metaservice_admission_steps as JoinSteps from "../../metaservice/admission_steps"
  export native_test.*

  pure val joinNode = "n4"
  pure val joinMutation: ApiRequest = Mutate({ edit: AddNode({ name: joinNode, member: 0, state: Active,
    nodeType: "worker", labels: Map() }), options: { ...options, approve: true } })
  pure val joinAttempt: AttemptKey = { version: 2, attempt: 1 }
  val prepareApproved: ApprovedAction = { operation: joinAttempt,
    step: world.extensions.metadata.operations.get(joinAttempt).plan.blocks.get("_meta")
      .select(s => s.kind == PrepareReplica).head() }
  val prepareGrant = JoinGrants::actionGrants(world.extensions.metadata, prepareApproved).getOnlyElement()
  val joinMessage = world.messages.get("n1").filter(m => match m.payload {
    | JoinRequest(j) => j.owner.node == joinNode | _ => false }).getOnlyElement()

  action additionApproved = init
    .then(beginMutation(mutationKey, joinMutation)).then(commitProposal(mutationKey))
    .then(receiveMutationAck(mutationKey)).then(applyMeta("n2")).then(finishMutation(mutationKey))
    .then(operationRead(approvalKey)).then(applyMeta("n1")).then(captureOperation("n1", approvalKey))
    .then(observeOperation("n1", approvalKey, "_meta")).then(observeOperationHealth("n1", approvalKey, "_meta"))
    .then(commitWork(approvalKey)).then(receiveWorkResult(approvalKey))
    .then(beginNodeStart(joinNode))

  action deliverJoinRequest = {
    nondet msg = world.extensions.network.pending.filter(m => match m.payload {
      | JoinRequest(j) => j.owner.node == joinNode | _ => false }).oneOf()
    deliver(msg)
  }
  action joinedBinding = additionApproved.then(requestJoin(joinNode, "n1")).then(deliverJoinRequest)
    .then(confirmJoin(joinMessage)).then({
      val command = AdmissionControl(JoinSteps::bindCommand(world, joinMessage).unwrap())
      bindJoin(joinMessage).then(controlCommit(command))
    }).then(applyMeta("n1"))
  action receiveJoinReply = {
    nondet msg = world.extensions.network.pending.filter(m => match m.payload {
      | JoinAccepted(a) => a.admission.owner.node == joinNode | _ => false }).oneOf()
    deliver(msg).then(receiveJoin(msg))
  }
  action freshJoinOffer = joinedBinding.then(replyJoin(joinMessage)).then(receiveJoinReply)
  action freshMetaStarted = freshJoinOffer.then(pinJoin(joinNode)).then(startJoinedMeta(joinNode))
  action preparedMeta = freshMetaStarted
    .then(commitAndApplyControl(BeginBlock({ operation: joinAttempt, group: "_meta" })))
    .then(commitAndApplyControl(IssueEffect(prepareApproved)))
    .then(dispatchGrant("n1", prepareGrant.key, HostResource({ group: "_meta", node: joinNode })))
    .then(receiveQueuedGrant).then({
      nondet delivery = world.system.get(joinNode).volatile.waitingEffects.oneOf()
      prepareMeta(delivery)
    })

  run freshJoinNeedsApprovedAdmissionTest = init.then(beginNodeStart(joinNode))
    .then(requestJoin(joinNode, "n1")).then(deliverJoinRequest).then(confirmJoin(joinMessage))
    .expect(world.system.get(joinNode).durable.pin == None)
    .then(bindJoin(joinMessage).fail())

  run joinBindingPrecedesDurablePinTest = freshJoinOffer
    .expect(world.extensions.metadata.admissions.get(joinNode).replica.member == 4)
    .expect(world.system.get(joinNode).durable.pin == None)
    .then(pinJoin(joinNode))
    .expect(world.system.get(joinNode).durable.pin == Some({ cluster: "c1", node: joinNode, member: 4 }))
    .expect(not(world.system.get(joinNode).volatile.metaRunning)).expect(safety)

  run preparationNeedsProcessBindingTest = additionApproved
    .then(commitAndApplyControl(BeginBlock({ operation: joinAttempt, group: "_meta" })))
    .expect(world.extensions.metadata.admissions == Map())
    .then(submitControl("n1", IssueEffect(prepareApproved)).fail())

  run freshMetaPreparationIsNotNativeMembershipTest = preparedMeta
    .expect(world.system.get(joinNode).durable.receipts.size() == 1)
    .expect(world.extensions.metadata.effectReceipts.size() == 0)
    .expect(world.extensions.consensus.get("_meta").voters.union(world.extensions.consensus.get("_meta").learners)
      .forall(r => r.node != joinNode)).expect(safety)

  run oldJoinReplyCannotPinNewProcessTest = joinedBinding.then(replyJoin(joinMessage))
    .then(crash(joinNode)).then(restart(joinNode)).then(receiveJoinReply)
    .expect(world.system.get(joinNode).volatile.joinOffer == None)
    .expect(world.system.get(joinNode).durable.pin == None).expect(safety)

  run pendingUnpreparedAdmissionRebindsExactlyTest = joinedBinding.then(replyJoin(joinMessage))
    .then(crash(joinNode)).then(restart(joinNode)).then(requestJoin(joinNode, "n1"))
    .then(deliverJoinRequest).then(confirmJoin(joinMessage)).then({
      val command = AdmissionControl(JoinSteps::bindCommand(world, joinMessage).unwrap())
      bindJoin(joinMessage).then(controlCommit(command))
    }).then(applyMeta("n1"))
    .expect(world.extensions.metadata.admissions.get(joinNode).owner == { node: joinNode, incarnation: 2 })
    .expect(world.extensions.metadata.admissions.get(joinNode).replica.member == 4).expect(safety)

  def joinStep(kind: ActionKind): ApprovedAction = { operation: joinAttempt,
    step: world.extensions.metadata.operations.get(joinAttempt).plan.blocks.get("_meta").select(s => s.kind == kind).head() }
  val freshMetaReplica = world.extensions.metadata.target.groups.get("_meta").replicas.filter(r => r.key.node == joinNode).getOnlyElement().key

  action readyJoin = announcePrepared(joinNode, "n1").then({
    nondet msg = world.extensions.network.pending.filter(m => match m.payload {
      | JoinPrepared(a) => a.owner.node == joinNode | _ => false }).oneOf()
    val command = match msg.payload { | JoinPrepared(a) => AdmissionControl(ReadyAdmission(a))
      | _ => AdmissionControl(ReadyAdmission(world.extensions.metadata.admissions.get(joinNode))) }
    deliver(msg).then(acceptPrepared(msg)).then(controlCommit(command))
  }).then(applyMeta("n1"))

  action runJoinNative(kind: ActionKind): bool = {
    val approved = joinStep(kind)
    commitAndApplyControl(IssueEffect(approved)).then({
      val grant = JoinGrants::actionGrants(world.extensions.metadata, approved).getOnlyElement()
      dispatchGrant("n1", grant.key, GroupResource("_meta"))
    }).then(receiveQueuedGrant).then({
      nondet delivery = world.system.get("n1").volatile.waitingEffects.filter(d => d.grant.planStep == Some(approved.step)).oneOf()
      installNativeAuthority(delivery)
        .then(if (kind == PromoteLearner) catchUp("_meta", freshMetaReplica) else { world' = world })
        .then(precheckNative(delivery)).then(proposeNative(delivery))
    }).then(commitNative("_meta")).then(reportAndCheckpoint(approved))
  }

  action addedFreshLearner = preparedMeta.then(readyJoin).then(reportAndCheckpoint(prepareApproved))
    .then(runJoinNative(AddLearner))

  run freshAdmissionAddsLearnerBeforeVoterTest = addedFreshLearner
    .expect(world.extensions.consensus.get("_meta").learners.contains(freshMetaReplica))
    .expect(not(world.extensions.consensus.get("_meta").voters.contains(freshMetaReplica)))
    .expect(world.extensions.consensus.get("_meta").voters.size() == 3).expect(safety)

  run freshAdmissionPromotesOnlyAfterCatchUpTest = addedFreshLearner
    .then(catchUp("_meta", freshMetaReplica))
    .then(checkpointCatchUp("n1", joinStep(WaitCatchUp)))
    .then(controlCommit(CompleteCatchUp(joinStep(WaitCatchUp)))).then(applyMeta("n1"))
    .then(runJoinNative(PromoteLearner))
    .expect(world.extensions.consensus.get("_meta").voters.size() == 4)
    .expect(world.extensions.consensus.get("_meta").learners.size() == 0)
    .expect(world.extensions.metadata.groups.get("_meta").owner == None)
    .expect(world.extensions.metadata.operations.get(joinAttempt).blocks.get("_meta").phase == OpSucceeded).expect(safety)

  run catchUpCheckpointCannotUsePreparedHostAloneTest = addedFreshLearner
    .expect(world.extensions.consensus.get("_meta").catchUp.get(freshMetaReplica) == Behind)
    .then(checkpointCatchUp("n1", joinStep(WaitCatchUp)).fail())

  run retainedPinRebindsUnpreparedAdmissionTest = freshMetaStarted
    .then(crash(joinNode)).then(restart(joinNode)).then(requestJoin(joinNode, "n1"))
    .then(deliverJoinRequest).then(confirmJoin(joinMessage)).then({
      val command = AdmissionControl(JoinSteps::bindCommand(world, joinMessage).unwrap())
      bindJoin(joinMessage).then(controlCommit(command))
    }).then(applyMeta("n1")).then(replyJoin(joinMessage)).then(receiveJoinReply)
    .expect(world.extensions.metadata.admissions.get(joinNode).owner.incarnation == 2)
    .expect(world.system.get(joinNode).durable.pin == Some({ cluster: "c1", node: joinNode, member: 4 }))
    .expect(world.system.get(joinNode).volatile.metaRunning).expect(safety)

  run retainedPreparedAdmissionCompletesAfterRestartTest = preparedMeta.then(readyJoin)
    .then(crash(joinNode)).then(restart(joinNode))
    .then(reportAndCheckpoint(prepareApproved)).then(runJoinNative(AddLearner))
    .then(catchUp("_meta", freshMetaReplica))
    .then(checkpointCatchUp("n1", joinStep(WaitCatchUp)))
    .then(controlCommit(CompleteCatchUp(joinStep(WaitCatchUp)))).then(applyMeta("n1"))
    .then(runJoinNative(PromoteLearner))
    .expect(world.extensions.metadata.operations.get(joinAttempt).blocks.get("_meta").phase == OpSucceeded)
    .expect(world.system.get(joinNode).volatile.incarnation == 2).expect(safety)

  // No Ready RPC has been sent. The preparation receipt and pin survive.
  action unacknowledgedPreparedRestart = preparedMeta.then(reportAndCheckpoint(prepareApproved))
    .then(crash(joinNode)).then(restart(joinNode))

  run preparedRestartBeforeReadyExposesBindingBlockTest = unacknowledgedPreparedRestart
    .expect(not(world.extensions.metadata.admissions.get(joinNode).ready))
    .expect(world.extensions.metadata.admissions.get(joinNode).owner.incarnation == 1)
    .then(requestJoin(joinNode, "n1")).then(deliverJoinRequest).then(confirmJoin(joinMessage))
    .expect(AdmissionRules::errors(world.extensions.metadata, JoinSteps::bindCommand(world, joinMessage).unwrap()) == Set(Unauthorized))
    .then(commitAndApplyControl(IssueEffect(joinStep(AddLearner)))).then({
      val grant = JoinGrants::actionGrants(world.extensions.metadata, joinStep(AddLearner)).getOnlyElement()
      dispatchGrant("n1", grant.key, GroupResource("_meta"))
    }).then(receiveQueuedGrant).then({
      nondet delivery = world.system.get("n1").volatile.waitingEffects.filter(d => d.grant.planStep == Some(joinStep(AddLearner).step)).oneOf()
      installNativeAuthority(delivery).expect(not(AdmissionNative::admissionReady(world, delivery)))
    }).expect(world.system.get(joinNode).durable.pin == Some({ cluster: "c1", node: joinNode, member: 4 }))
    .expect(world.system.get(joinNode).volatile.incarnation == 2)
    .expect(world.extensions.consensus.get("_meta").learners.size() == 0).expect(safety)

  action admissionStageProbe = world' = world
  run admissionSharesWorldTest = clusterStageInit.then(nativeStageProbe).then(admissionStageProbe)
    .expect(world.system.keys() == NODE_POOL)
}
