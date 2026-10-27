module operations_test {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import transactions_test.* from "./transactions_test"
  import authority_fixtures as AF from "../fixtures/authority"
  import initial as Seeds from "../../harness/initial"
  export transactions_test.*

  pure val approvalKey: ProposalKey = { request: mutationKey, sequence: 1 }
  pure val applyDelete: ApiRequest = Mutate({ edit: DeleteGroup("a"), options: { ...options, approve: true } })
  action approvalReachabilityInit = init.then(invoke("n2", applyDelete))
  val approvedOperation = world.extensions.metadata.firstAttempts.keys().size() > 0

  action deliverOperationCall(key: ProposalKey): bool = {
    nondet msg = world.extensions.network.pending.filter(m => match m.payload {
      | OperationCall(c) => c.key == key | _ => false }).oneOf()
    deliver(msg)
  }
  action deliverWorkProbe(key: ProposalKey): bool = {
    nondet msg = world.extensions.network.pending.filter(m => match m.payload {
      | ReadProbe(t) => t.request == key.request and t.proposal == Some(key.sequence) | _ => false }).oneOf()
    deliver(msg)
  }
  action confirmWorkProbe(key: ProposalKey): bool = {
    nondet msg = world.messages.get("n1").filter(m => match m.payload {
      | ReadProbe(t) => t.request == key.request and t.proposal == Some(key.sequence) | _ => false }).oneOf()
    confirmRead(msg)
  }
  action deliverWorkReadAck(key: ProposalKey): bool = {
    nondet msg = world.extensions.network.pending.filter(m => match m.payload {
      | ReadAck(ack) => ack.ticket.request == key.request and ack.ticket.proposal == Some(key.sequence) | _ => false }).oneOf()
    deliver(msg)
  }
  action deliverWorkCommitAck(key: ProposalKey): bool = {
    nondet msg = world.extensions.network.pending.filter(m => match m.payload {
      | MetaAck(ack) => ack.forWork and ack.key == key | _ => false }).oneOf()
    deliver(msg)
  }
  action deliverOperationAck(key: ProposalKey): bool = {
    nondet msg = world.extensions.network.pending.filter(m => match m.payload {
      | OperationAck(ack) => ack.key == key | _ => false }).oneOf()
    deliver(msg)
  }
  action stageForApproval: bool = beginMutation(mutationKey, applyDelete).then(commitProposal(mutationKey))
    .then(receiveMutationAck(mutationKey)).then(applyMeta("n2")).then(finishMutation(mutationKey))
  action operationRead(key: ProposalKey): bool = dispatchOperation(key.request).then(deliverOperationCall(key))
    .then(handle("n1")).then(startOperationRead("n1", key)).then(deliverWorkProbe(key))
    .then(confirmWorkProbe(key)).then(deliverWorkReadAck(key)).then(handle("n1"))
  action recaptureWork(key: ProposalKey): bool = beginOperationRecapture("n1", key)
    .then(startOperationRead("n1", key)).then(deliverWorkProbe(key)).then(confirmWorkProbe(key))
    .then(deliverWorkReadAck(key)).then(handle("n1")).then(captureOperation("n1", key))
  action commitWork(key: ProposalKey): bool = recaptureWork(key).then(buildOperation("n1", key)).then(submitOperation("n1", key))
    .then(deliverProposal(key.request)).then(commitProposal(key.request))
    .then(deliverWorkCommitAck(key)).then(handle("n1")).then(applyMeta("n1")).then(finishOperation("n1", key))
  action receiveWorkResult(key: ProposalKey): bool = replyOperation("n1", key).then(deliverOperationAck(key))
    .then(handle(key.request.caller.node))

  run stageThenApproveKeepsSeparateCommitsTest = init.then(stageForApproval)
    .expect(world.extensions.metadata.target.version == 1)
    .expect(world.system.get("n2").volatile.pendingCalls.get(mutationKey).phase == AwaitingApproval)
    .then(operationRead(approvalKey))
    .expect(world.system.get("n1").volatile.appliedMetaIndex == 1)
    .then(applyMeta("n1")).then(captureOperation("n1", approvalKey))
    .then(commitWork(approvalKey)).then(receiveWorkResult(approvalKey))
    .expect(approvedOperation)
    .expect(world.extensions.metadata.target.version == 2)
    .expect(world.extensions.metadata.cleanup.keys().size() == 3)
    .expect(world.extensions.metadata.cleanup.values().forall(d => d.stopped == None and d.retired == None))
    .expect(world.system.get("n2").volatile.pendingCalls.get(mutationKey).result == Some(MutationResult({
      staged: Some(2), operation: Some({ version: 2, attempt: 1 }), rebasedTo: None, errors: Set() })))
    .expect(safety)

  run cancellationAfterStagePreservesVersionTest = init.then(stageForApproval).then(cancelCall(mutationKey))
    .expect(world.extensions.metadata.latest.version == 2 and world.extensions.metadata.target.version == 1)
    .expect(world.system.get("n2").volatile.pendingCalls.get(mutationKey).result == Some(MutationResult({
      staged: Some(2), operation: None, rebasedTo: None, errors: Set(WaitCancelled) })))
    .expect(safety).then(dispatchOperation(mutationKey).fail())

  run failedApprovalReturnsStagedVersionTest = {
    val other: RequestKey = { caller: { node: "n3", incarnation: 1 }, sequence: 1 }
    init.then(stageForApproval).then(operationRead(approvalKey)).then(applyMeta("n1"))
      .then(captureOperation("n1", approvalKey)).then(recaptureWork(approvalKey)).then(buildOperation("n1", approvalKey))
      .then(applyMeta("n3"))
      .then(beginMutation(other, Mutate({ edit: DeleteGroup("b"), options: options }))).then(commitProposal(other))
      .then(submitOperation("n1", approvalKey)).then(deliverProposal(mutationKey)).then(commitProposal(mutationKey))
      .then(deliverWorkCommitAck(approvalKey)).then(handle("n1")).then(applyMeta("n1"))
      .then(finishOperation("n1", approvalKey)).then(receiveWorkResult(approvalKey))
      .expect(world.extensions.metadata.latest.version == 3 and world.extensions.metadata.target.version == 1)
      .expect(world.system.get("n2").volatile.pendingCalls.get(mutationKey).result == Some(MutationResult({
        staged: Some(2), operation: None, rebasedTo: None, errors: Set(Conflict) })))
      .expect(safety)
  }

  run lostApprovalReplyReplaysExactOperationTest = init.then(stageForApproval).then(operationRead(approvalKey))
    .then(applyMeta("n1")).then(captureOperation("n1", approvalKey)).then(commitWork(approvalKey))
    .then(replyOperation("n1", approvalKey))
    .then({ nondet msg = world.extensions.network.pending.filter(m => match m.payload {
        | OperationAck(ack) => ack.key == approvalKey | _ => false }).oneOf()
      drop(msg) })
    .then(dispatchOperation(mutationKey)).then(deliverOperationCall(approvalKey)).then(handle("n1"))
    .then(receiveWorkResult(approvalKey))
    .expect(world.extensions.metadata.operations.keys() == Set({ version: 2, attempt: 1 }))
    .expect(world.system.get("n2").volatile.pendingCalls.get(mutationKey).stagedVersion == Some(2))
    .expect(safety)

  run offlineDeletionNeedsOnlyMetaQuorumTest = init.then(crash("n3")).then(stageForApproval)
    .then(operationRead(approvalKey)).then(applyMeta("n1")).then(captureOperation("n1", approvalKey))
    .then(commitWork(approvalKey)).then(receiveWorkResult(approvalKey))
    .expect(world.extensions.metadata.cleanup.keys().exists(r => r.node == "n3"))
    .expect(world.extensions.metadata.operations.get({ version: 2, attempt: 1 }).blocks.get("a").phase == OpSucceeded)
    .expect(safety)

  pure val reseatKey: ProposalKey = { request: mutationKey, sequence: 0 }
  pure val reseat: ApiRequest = Mutate({ edit: ReseatReplica(Seeds::replica(Map("n3" -> 3), "a", "n3")), options: options })
  action reseatRead: bool = invoke("n2", reseat).then(deliverApi(mutationKey)).then(handle("n2"))
    .then(beginReseatRead(mutationKey)).then(deliverProbe).then(confirmProbe).then(receiveAck)

  run reseatCommitsApprovalAtomicallyTest = init.then(reseatRead).then(finishReseatRead(mutationKey))
    .then(operationRead(reseatKey)).then(captureOperation("n1", reseatKey))
    .then(observeOperation("n1", reseatKey, "a")).then(observeOperationHealth("n1", reseatKey, "a"))
    .then(commitWork(reseatKey)).then(receiveWorkResult(reseatKey))
    .expect(world.extensions.consensus.get("_meta").committedPosition == 2)
    .expect(world.extensions.metadata.target == world.extensions.metadata.latest)
    .expect(world.extensions.metadata.target.version == 2)
    .expect(world.extensions.metadata.firstAttempts.get(2) == { version: 2, attempt: 1 })
    .expect(world.extensions.consensus.get("a").voters.exists(r => r.node == "n3" and r.replica == 3))
    .expect(world.extensions.metadata.target.groups.get("a").replicas.exists(r => r.key.node == "n3" and r.key.replica == 4))
    .expect(safety)

  run reseatCapturesCasAfterReadAndProtectsStagedIntentTest = {
    val other: RequestKey = { caller: { node: "n3", incarnation: 1 }, sequence: 1 }
    init.then(beginMutation(other, Mutate({ edit: DeleteGroup("b"), options: options }))).then(commitProposal(other))
      .then(reseatRead)
      .expect(world.system.get("n2").volatile.localView.latest.version == 1)
      .then(applyMeta("n2")).then(finishReseatRead(mutationKey))
      .expect(match world.system.get("n2").volatile.pendingCalls.get(mutationKey).request {
        | Mutate(m) => m.options.expectedVersion == 2 | _ => false })
      .then(operationRead(reseatKey)).then(applyMeta("n1")).then(captureOperation("n1", reseatKey))
      .then(observeOperation("n1", reseatKey, "a")).then(observeOperationHealth("n1", reseatKey, "a"))
      .then(commitWork(reseatKey)).then(receiveWorkResult(reseatKey))
      .expect(world.extensions.consensus.get("_meta").committedPosition == 3)
      .expect(world.extensions.metadata.target.version == 3 and world.extensions.metadata.latest.version == 4)
      .expect(world.extensions.metadata.latest.rebasedFrom == Some(2))
      .expect(world.extensions.metadata.target.groups.keys().contains("b"))
      .expect(not(world.extensions.metadata.latest.groups.keys().contains("b")))
      .expect(world.extensions.metadata.cleanup.keys().size() == 0)
      .expect(safety)
  }

  // Retained failed-before-effect checkpoint; execution failure/reporting itself
  // is added with Task 6. Retry still enters through the ordinary public call.
  action retainedFailedInit: bool = {
    val seed = Seeds::worldFor(NODE_POOL, FOUNDERS, LOAD_IDS, INITIAL_LEADER)
    val meta = AF::failed
    val view = { latest: meta.latest, target: meta.target }
    world' = { ...seed, system: seed.system.keys().mapBy(name => {
      val n = seed.system.get(name)
      { ...n, volatile: { ...n.volatile, localView: view, appliedMetaVersion: meta.latest.version },
        durable: { ...n.durable, metaImage: Some(view) } }
    }),
      extensions: { ...seed.extensions, metadata: meta, metaLog: Map(1 -> meta) } }
  }

  run retryRoutesExactFailedAttemptTest = retainedFailedInit
    .then(invoke("n2", Operate(RetryAttempt({ version: 2, attempt: 1 }))))
    .then(deliverApi(mutationKey)).then(handle("n2")).then(operationRead(reseatKey))
    .then(captureOperation("n1", reseatKey)).then(observeOperation("n1", reseatKey, "a"))
    .then(observeOperationHealth("n1", reseatKey, "a"))
    .then(commitWork(reseatKey)).then(receiveWorkResult(reseatKey))
    .expect(world.extensions.metadata.successors.get({ version: 2, attempt: 1 }) == { version: 2, attempt: 2 })
    .expect(world.extensions.metadata.operations.get({ version: 2, attempt: 2 }).plan == AF::failed.operations.get(AF::attempt).plan)
    .expect(world.extensions.metadata.firstAttempts.get(2) == { version: 2, attempt: 1 })
    .expect(safety)

  run approveFailedVersionReplaysFirstAttemptTest = retainedFailedInit
    .then(invoke("n2", Operate(ApproveVersion(2))))
    .then(deliverApi(mutationKey)).then(handle("n2")).then(operationRead(reseatKey))
    .then(captureOperation("n1", reseatKey)).then(receiveWorkResult(reseatKey))
    .expect(world.extensions.metadata.operations.keys() == Set(AF::attempt))
    .expect(world.system.get("n2").volatile.pendingCalls.get(mutationKey).result == Some(MutationResult({
      staged: Some(2), operation: Some(AF::attempt), rebasedTo: None, errors: Set() })))
    .expect(safety)

  pure val applyDemotion: ApiRequest = Mutate({ edit: ChangeReplica({ group: "a", node: "n3", role: Learner }),
    options: { ...options, approve: true } })
  action stageDemotionForApproval: bool = beginMutation(mutationKey, applyDemotion).then(commitProposal(mutationKey))
    .then(receiveMutationAck(mutationKey)).then(applyMeta("n2")).then(finishMutation(mutationKey))

  run planningNativeReadNeedsLoadQuorumTest = init.then(stageDemotionForApproval)
    .then(operationRead(approvalKey)).then(applyMeta("n1")).then(captureOperation("n1", approvalKey))
    .then(setLink(LoadChannel, "n1", "n2", false)).then(setLink(LoadChannel, "n1", "n3", false))
    .then(setLink(LoadChannel, "n2", "n3", false))
    .then(observeOperation("n1", approvalKey, "a")).then(receiveWorkResult(approvalKey))
    .expect(world.extensions.metadata.target.version == 1)
    .expect(world.system.get("n2").volatile.pendingCalls.get(mutationKey).result == Some(MutationResult({
      staged: Some(2), operation: None, rebasedTo: None, errors: Set(Unavailable) })))
    .expect(safety)

  run approvalRecaptureNeedsMetaQuorumTest = init.then(stageDemotionForApproval)
    .then(operationRead(approvalKey)).then(applyMeta("n1")).then(captureOperation("n1", approvalKey))
    .then(observeOperation("n1", approvalKey, "a")).then(observeOperationHealth("n1", approvalKey, "a"))
    .then(beginOperationRecapture("n1", approvalKey)).then(startOperationRead("n1", approvalKey))
    .then(deliverWorkProbe(approvalKey))
    .then(setLink(MetaChannel, "n1", "n2", false)).then(setLink(MetaChannel, "n1", "n3", false))
    .expect(world.extensions.metadata.target.version == 1).expect(safety)
    .then(confirmWorkProbe(approvalKey).fail())

  run oldReadAckCannotSatisfyRecaptureTest = init.then(stageForApproval)
    .then(dispatchOperation(mutationKey)).then(deliverOperationCall(approvalKey)).then(handle("n1"))
    .then(startOperationRead("n1", approvalKey)).then(deliverWorkProbe(approvalKey)).then(confirmWorkProbe(approvalKey))
    .then({ nondet msg = world.extensions.network.pending.filter(m => match m.payload {
        | ReadAck(ack) => ack.ticket.request == mutationKey and ack.ticket.proposal == Some(1) | _ => false }).oneOf()
      duplicate(msg) })
    .then(deliverWorkReadAck(approvalKey)).then(handle("n1")).then(applyMeta("n1"))
    .then(captureOperation("n1", approvalKey)).then(beginOperationRecapture("n1", approvalKey))
    .then(startOperationRead("n1", approvalKey))
    .then(deliverWorkReadAck(approvalKey)).then(handle("n1"))
    .expect(world.system.get("n1").volatile.operationWork.get(approvalKey).phase == WorkReadQueued)
    .expect(world.system.get("n1").volatile.operationWork.get(approvalKey).readIndex == None)
    .expect(world.system.get("n1").volatile.operationWork.get(approvalKey).readRound == 1)
    .expect(safety)
    .then(captureOperation("n1", approvalKey).fail())
  action operationsStageProbe = world' = world
  run operationsSharesWorldTest = clusterStageInit.then(transactionsStageProbe).then(operationsStageProbe)
    .expect(world.system.keys() == NODE_POOL)
}
