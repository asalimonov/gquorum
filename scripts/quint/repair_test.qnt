module repair_test {
  import types.* from "./types"
  import basicSpells.* from "./vendor/spells/basicSpells"
  import authority as A from "./authority"
  import repair as R from "./repair"
  import topology as T from "./topology"
  import topology_test as F from "./topology_test"
  import planning_test as PF from "./planning_test"
  import authority_test as AF from "./authority_test"
  import initial as I from "./initial"

  pure val predecessor = I::replica(Map("n3" -> 3), "a", "n3")
  pure def command(meta: MetadataState): RepairCommand = {
    val request = { edit: ReseatReplica(predecessor), options: { ...F::options, expectedVersion: meta.latest.version } }
    val desired = T::mutateDocument(A::mutationInput(meta, Map(), F::input.rules), request)
    val input = { ...A::planningInput(meta, F::input.rules), observations: PF::input.observations }
    val plan = match T::makePlan(input, desired) { | Planned(p) => p
      | PlanRejected(_) => { version: desired.version, blocks: Map(), cleanup: Map(), sources: Map(), dependencies: Set() } }
    { request: request, expectedTarget: meta.target.version, approval: {
      desired: desired, plan: plan, scopes: T::affectedScopes(meta.target, desired).mapBy(g => A::scopeAt(meta, g)),
      captured: input, origin: Operator, audit: desired.audit } }
  }
  run repairApprovesOneAtomicVersionTest = {
    val cmd = command(F::meta)
    val result = R::applyRepair(F::meta, cmd)
    and { R::canRepair(F::meta, cmd, F::input.rules), result.latest == result.target,
      result.target.version == 2, result.firstAttempts.get(2) == { version: 2, attempt: 1 },
      result.target.groups.get("a").replicas.exists(r => r.key.node == "n3" and r.key.replica == 4),
      result.target.groups.get("b") == F::base.groups.get("b") }
  }
  run repairRebasesIndependentStagedDeletionTest = {
    val staged = AF::stageEdit(F::meta, DeleteGroup("b"))
    val cmd = command(staged)
    val result = R::applyRepair(staged, cmd)
    and { R::canRepair(staged, cmd, F::input.rules),
      result.target.version == 3, result.latest.version == 4, result.latest.rebasedFrom == Some(2),
      result.target.groups.keys().contains("b"), not(result.latest.groups.keys().contains("b")),
      result.operations.keys() == Set({ version: 3, attempt: 1 }),
      A::rebasedSuccessor(result, 2) == Some(4), result.latest.audit == staged.latest.audit,
      R::applyRepair(result, cmd) == result }
  }
  run repairPreservesIndependentAllocationTest = {
    val stagedDoc = { ...F::base, version: 2, parent: 1, groups: F::base.groups.setBy("b", g => {
      ...g, nextReplica: 5, replicas: g.replicas.map(r => if (r.key.node == "n3")
        { ...r, key: { ...r.key, replica: 4 } } else r) }) }
    val staged = A::applyStage(F::meta, { request: F::request(StageDocument(stagedDoc)), desired: stagedDoc })
    val cmd = command(staged)
    val result = R::applyRepair(staged, cmd)
    and { R::canRepair(staged, cmd, F::input.rules),
      result.target.groups.get("b").nextReplica == 4, result.latest.groups.get("b").nextReplica == 5,
      result.replicaAllocationFloors.get("b") == 5, result.latest.groups.get("b") == stagedDoc.groups.get("b") }
  }
  run repairRejectsOverlappingStagedIntentTest = {
    val staged = AF::stageEdit(F::meta, ChangeReplica({ group: "a", node: "n3", role: Learner }))
    R::errors(staged, command(staged), F::input.rules) == Set(Conflict)
  }
  run repairChecksBothVersionCapturesTest = {
    val cmd = command(F::meta)
    and { R::errors(F::meta, { ...cmd, expectedTarget: 0 }, F::input.rules) == Set(Conflict),
      R::errors(F::meta, { ...cmd, request: { ...cmd.request, options: { ...cmd.request.options,
        expectedVersion: 0 } } }, F::input.rules) == Set(Conflict) }
  }
  run rebaseLookupFollowsWholeChainTest = {
    val meta = { ...F::meta, history: F::meta.history.put(2, { ...F::base, version: 2, rebasedFrom: Some(1) })
      .put(3, { ...F::base, version: 3, rebasedFrom: Some(2) }) }
    A::rebasedSuccessor(meta, 1) == Some(3)
  }
}
