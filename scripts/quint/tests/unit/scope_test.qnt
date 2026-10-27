module scope_test {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import scope as S from "../../harness/scope"
  import topology_fixtures as F from "../fixtures/topology"

  run labelsCannotEnterThroughStageTest = {
    val changed = { ...F::base, groups: F::base.groups.setBy("a", g => { ...g, labels: Map("az" -> "x") }) }
    not(S::requestInScope(F::base, Mutate(F::request(StageDocument(changed)))))
  }
  run labelsCannotEnterThroughAdmissionTest = not(S::requestInScope(F::base,
    Mutate(F::request(AddNode({ name: "n4", member: 0, state: Active, nodeType: "worker", labels: Map("az" -> "x") })))))
  run labelsAndMemoryCannotEnterThroughCreateTest = {
    val spec: GroupSpec = { id: "c", labels: Map(), storage: StorageDefault, placement: None,
      nodes: None, hosting: None, voterConfig: None }
    and {
      S::requestInScope(F::base, Mutate(F::request(CreateGroup(spec)))),
      not(S::requestInScope(F::base, Mutate(F::request(CreateGroup({ ...spec, labels: Map("az" -> "x") }))))),
      not(S::requestInScope(F::base, Mutate(F::request(CreateGroup({ ...spec, storage: StorageMemory }))))),
    }
  }
  run deferredPolicyCallsExcludedTest = {
    val requests = Set(SetPlacement(F::base.placement), SetGroupPlacement({ group: "a", rule: F::base.placement }),
      RollbackTo(1))
    requests.forall(edit => not(S::requestInScope(F::base, Mutate(F::request(edit)))))
  }
  run stageCannotTuneVoterPolicyTest = {
    val changed = { ...F::base, groups: F::base.groups.setBy("a", g =>
      { ...g, voterConfig: Some({ target: 2, domain: None, maxSurge: 1, failureWindow: 30,
        standbyWindow: 60, failureLag: 1000, standbyLag: 100, standbyApplyBacklog: 100,
        replaceNoProgress: true, noProgressWindow: 30, maxObservationGap: 15, switchCooldown: 300,
        demotionCooldown: 300, allowNewFailureDuringCooldown: true }) }) }
    not(S::stageInScope(F::base, changed))
  }
  run rawStageHasNoConvenienceOptionsTest = and {
    not(S::requestInScope(F::base, Mutate({ edit: StageDocument(F::base), options: { ...F::options, preview: true } }))),
    not(S::requestInScope(F::base, Mutate({ edit: StageDocument(F::base), options: { ...F::options, approve: true } }))),
  }
  run standalonePinAndVoterSettersExcludedTest = {
    val key = F::base.groups.get("a").replicas.filter(r => r.key.node == "n1").getOnlyElement().key
    and {
      not(S::requestInScope(F::base, Mutate(F::request(SetReplicaPinnedLearner({ key: key, pinned: true }))))),
      not(S::requestInScope(F::base, Mutate(F::request(SetGroupVoterConfig({ group: "a", config: {
        target: 3, domain: None, maxSurge: 1, failureWindow: 30, standbyWindow: 60,
        failureLag: 1000, standbyLag: 100, standbyApplyBacklog: 100, replaceNoProgress: true,
        noProgressWindow: 30, maxObservationGap: 15, switchCooldown: 300, demotionCooldown: 300,
        allowNewFailureDuringCooldown: true } }))))),
    }
  }
}
