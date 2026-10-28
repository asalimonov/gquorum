module transactions_test {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import transactions as Tx from "../../metaservice/transactions"
  import initial as I from "../../harness/initial"
  import reads_test.* from "./reads_test"
  export reads_test.*

  pure val mutationKey: RequestKey = { caller: { node: "n2", incarnation: 1 }, sequence: 1 }
  pure val options: MutationOptions = { expectedVersion: 0,
    audit: { actor: "operator", comment: "original", forced: None }, preview: false, approve: false }
  pure val deleteA: ApiRequest = Mutate({ edit: DeleteGroup("a"), options: options })
  val stagedMutation = world.extensions.metadata.latest.version > world.extensions.metadata.target.version
  val repliedMutation = world.system.values().exists(n => n.volatile.pendingCalls.values().exists(c =>
    match c.result { | Some(r) => match r { | MutationResult(o) => o.staged != None | _ => false } | None => false }))

  action deliverApi(key: RequestKey): bool = {
    nondet msg = world.extensions.network.pending.filter(m => match m.payload {
      | ApiCall(c) => c.key == key | _ => false }).oneOf()
    deliver(msg)
  }
  action deliverProposal(key: RequestKey): bool = {
    nondet msg = world.extensions.network.pending.filter(m => match m.payload {
      | MetaProposal(p) => p.key.request == key | _ => false }).oneOf()
    deliver(msg)
  }
  action commitProposal(key: RequestKey): bool = {
    nondet msg = world.messages.get("n1").filter(m => match m.payload {
      | MetaProposal(p) => p.key.request == key | _ => false }).oneOf()
    commitMutation(msg)
  }
  action deliverMutationAck(key: RequestKey): bool = {
    nondet msg = world.extensions.network.pending.filter(m => match m.payload {
      | MetaAck(ack) => ack.key.request == key | _ => false }).oneOf()
    deliver(msg)
  }
  action beginMutation(key: RequestKey, request: ApiRequest): bool = invoke(key.caller.node, request)
    .then(deliverApi(key)).then(handle(key.caller.node))
    .then(match request { | Mutate(m) => match m.edit { | ReadmitNode(r) => observeMutationHealth(key, r.name)
      | _ => { world' = world } } | _ => { world' = world } }).then(prepareMutation(key))
    .then(submitMutation(key)).then(deliverProposal(key))
  action receiveMutationAck(key: RequestKey): bool = deliverMutationAck(key).then(handle(key.caller.node))

  run stageCommitPrecedesLocalApplyAndReplyTest = init.then(beginMutation(mutationKey, deleteA))
    .expect(world.extensions.metadata.latest.version == 1)
    .then(commitProposal(mutationKey))
    .expect(stagedMutation)
    .expect(world.system.get("n2").volatile.localView.latest.version == 1)
    .expect(world.extensions.metadata.target.version == 1)
    .expect(world.extensions.metadata.operations.keys().size() == 0)
    .then(receiveMutationAck(mutationKey))
    .expect(not(Tx::canFinish(world.system.get("n2"), mutationKey)))
    .then(applyMeta("n2")).then(finishMutation(mutationKey))
    .expect(repliedMutation).expect(safety)

  run lostStageReplyReplaysOriginalCasTest = init.then(beginMutation(mutationKey, deleteA))
    .then(commitProposal(mutationKey))
    .then({ nondet msg = world.extensions.network.pending.filter(m => match m.payload {
        | MetaAck(ack) => ack.key.request == mutationKey | _ => false }).oneOf()
      drop(msg) })
    .then(submitMutation(mutationKey)).then(deliverProposal(mutationKey)).then(commitProposal(mutationKey))
    .expect(world.extensions.metadata.latest.version == 2)
    .expect(world.extensions.metadata.stagedIntents.keys() == Set(2))
    .then(receiveMutationAck(mutationKey)).then(applyMeta("n2")).then(finishMutation(mutationKey))
    .expect(world.system.get("n2").volatile.pendingCalls.get(mutationKey).stagedVersion == Some(2))
    .expect(repliedMutation).expect(safety)

  run concurrentMutationKeepsCapturedCasTest = {
    val other: RequestKey = { caller: { node: "n3", incarnation: 1 }, sequence: 1 }
    init.then(beginMutation(mutationKey, deleteA))
      .then(beginMutation(other, Mutate({ edit: DeleteGroup("b"), options: options })))
      .then(commitProposal(mutationKey)).then(commitProposal(other))
      .then(receiveMutationAck(other)).then(applyMeta("n3")).then(finishMutation(other))
      .expect(world.extensions.metadata.latest.version == 2)
      .expect(world.extensions.metadata.latest.groups.keys().contains("b"))
      .expect(world.system.get("n3").volatile.pendingCalls.get(other).result == Some(MutationResult({
        staged: None, operation: None, rebasedTo: None, errors: Set(Conflict) })))
      .expect(safety)
  }

  run cancelledSubmittedMutationCanCommitTest = init.then(beginMutation(mutationKey, deleteA))
    .then(cancelCall(mutationKey)).then(commitProposal(mutationKey))
    .then(receiveMutationAck(mutationKey)).then(applyMeta("n2"))
    .expect(world.extensions.metadata.latest.version == 2)
    .expect(world.system.get("n2").volatile.pendingCalls.get(mutationKey).result == Some(Indeterminate))
    .expect(safety).then(finishMutation(mutationKey).fail())

  run mutationPreviewDoesNotSubmitTest = {
    val preview = Mutate({ edit: DeleteGroup("a"), options: { ...options, preview: true } })
    init.then(invoke("n2", preview)).then(deliverApi(mutationKey)).then(handle("n2"))
      .then(prepareMutation(mutationKey))
      .expect(world.extensions.metadata.latest.version == 1)
      .expect(world.extensions.network.pending.size() == 0)
      .expect(world.system.get("n2").volatile.pendingCalls.get(mutationKey).phase == Replied)
      .expect(safety).then(submitMutation(mutationKey).fail())
  }

  run rawStageFutureCasUsesCommitStateTest = {
    val other: RequestKey = { caller: { node: "n3", incarnation: 1 }, sequence: 1 }
    val base = I::declarations(FOUNDERS, LOAD_IDS)
    val desired = { ...base, version: 99, parent: 2, groups: base.groups.mapRemove("a").mapRemove("b") }
    init.then(beginMutation(mutationKey, deleteA)).then(commitProposal(mutationKey))
      .expect(world.system.get("n3").volatile.localView.latest.version == 1)
      .then(beginMutation(other, Mutate({ edit: StageDocument(desired), options: { ...options, expectedVersion: 2 } })))
      .then(commitProposal(other)).then(receiveMutationAck(other)).then(applyMeta("n3"))
      .then(finishMutation(other))
      .expect(world.extensions.metadata.latest.version == 3)
      .expect(world.system.get("n3").volatile.pendingCalls.get(other).stagedVersion == Some(3))
      .expect(safety)
  }
  action transactionsStageProbe = world' = world
  run transactionsSharesWorldTest = clusterStageInit.then(readsStageProbe).then(transactionsStageProbe)
    .expect(world.system.keys() == NODE_POOL)
}
