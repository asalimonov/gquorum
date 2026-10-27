module authority_fixtures {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import topology as T from "../../topology/topology"
  import authority as A from "../../metaengine/authority"
  import topology_fixtures as F from "./topology"
  import planning_fixtures as PF from "./planning"
  pure val request = F::request(DeleteGroup("a"))
  pure val command: StageCommand = { request: request, desired: T::mutateDocument(F::input, request) }
  pure val staged = A::applyStage(F::meta, command)
  pure def approvalFor(meta: MetadataState): ApprovalCommand = {
    val input = { ...A::planningInput(meta, F::input.rules), observations: PF::input.observations }
    val plan = match T::makePlan(input, meta.latest) {
      | Planned(p) => p
      | PlanRejected(_) => { version: meta.latest.version, blocks: Map(), cleanup: Map(), sources: Map(), dependencies: Set() }
    }
    { desired: meta.latest, plan: plan,
      scopes: T::affectedScopes(meta.target, meta.latest).mapBy(g => A::scopeAt(meta, g)),
      captured: input, origin: Operator, audit: meta.latest.audit }
  }
  pure def stageEdit(meta: MetadataState, edit: DocumentEdit): MetadataState = {
    val request = { edit: edit, options: { ...F::options, expectedVersion: meta.latest.version } }
    val input = A::mutationInput(meta, Map(), F::input.rules)
    A::applyStage(meta, { request: request, desired: T::mutateDocument(input, request) })
  }
  pure val stagedChange = stageEdit(F::meta, ChangeReplica({ group: "a", node: "n3", role: Learner }))
  pure val change = A::applyApproval(stagedChange, approvalFor(stagedChange))
  pure val attempt = { version: 2, attempt: 1 }
  // A failed-before-effect fixture. Runtime execution failure/reporting belongs to Task 6.
  pure val failed = { ...change,
    operations: change.operations.setBy(attempt, op => { ...op,
      blocks: op.blocks.setBy("a", b => { ...b, phase: OpFailed }) }),
    groups: change.groups.setBy("a", g => { ...g, owner: None }),
  }
  pure val retry: RetryCommand = { previous: attempt, scopes: Map("a" -> A::scopeAt(failed, "a")) }
}
