module grants_fixtures {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import grants as G from "../../metaengine/grants"
  import controls as Ctrl from "../../metaengine/controls"
  import authority as A from "../../metaengine/authority"
  import topology_fixtures as F from "./topology"
  import authority_fixtures as AF from "./authority"
  pure val key: AttemptKey = { version: 2, attempt: 1 }
  pure val running = { ...AF::change, operations: AF::change.operations.setBy(key, op => { ...op,
    blocks: op.blocks.setBy("a", b => { ...b, phase: OpRunning }) }) }
  pure val planned = running.operations.get(key).plan.blocks.get("a").head()
  pure val approved: ApprovedAction = { operation: key, step: planned }
  pure val issued = G::issueStep(running, approved, 4)
  pure val grant = G::actionGrants(issued, approved).getOnlyElement()
  pure val replica = grant.targets.filter(r => r.node == "n3").getOnlyElement()
  pure val resource = GroupResource("a")
  pure val receipt: Receipt = { grant: grant.key, effect: grant.effect, admission: None,
    target: Some(replica), resource: resource, outcome: EffectApplied, planStep: grant.planStep,
    durablePosition: 2, durableTerm: 1 }
  pure val closed = G::closeGrants(issued, Set(grant.key), 5)
  pure val fenceGrant = closed.grants.get({ cluster: "c1", group: "a", revision: 5 })
  pure val fence: FenceEvidence = { grant: fenceGrant.key, resource: resource,
    receiver: { cluster: replica.cluster, node: replica.node, member: replica.member },
    replica: Some(replica), durablePosition: 3, durableTerm: 1 }
  pure val request: RequestKey = { caller: { node: "n1", incarnation: 1 }, sequence: 1 }
  pure val manual = Ctrl::manualGrant(F::meta, replica, request, 4)
}
