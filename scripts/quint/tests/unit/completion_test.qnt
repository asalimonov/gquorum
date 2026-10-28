module completion_test {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import initial as I from "../../harness/initial"
  import completion as C from "../../metaengine/completion"
  import planning as P from "../../topology/planning"

  pure val doc = I::declarations(Map("n1" -> 1, "n2" -> 2, "n3" -> 3), Set("a"))
  pure val base = I::metadata(doc)
  pure val third = doc.groups.get("a").replicas.filter(r => r.key.node == "n3").getOnlyElement().key
  pure val desired = { ...doc, version: 2, parent: 1, groups: doc.groups.setBy("a", g => { ...g,
    replicas: g.replicas.map(r => if (r.key == third) { ...r, role: Learner } else r) }) }
  pure val step = { ...P::stepFor(DemoteVoter, third, doc.groups.get("a").replicas.map(r => r.key),
    doc.groups.get("a").replicas.filter(r => r.key != third).map(r => r.key)), seq: 1 }
  pure val granted: Grant = { key: { cluster: "c1", group: "a", revision: 4 }, purpose: Effect,
    effect: Some({ operation: { version: 2, attempt: 1 }, step: 1, kind: DemoteVoter,
      target: ReplicaTarget(third), grantRevision: 4 }), admission: None,
    resources: step.resources, targets: Set(third), planStep: Some(step), request: None }
  pure val recorded: Receipt = { grant: granted.key, effect: granted.effect, admission: None,
    target: Some(third), resource: GroupResource("a"), outcome: EffectApplied, planStep: Some(step),
    durablePosition: 2, durableTerm: 1 }
  pure val issued = { ...base, history: base.history.put(2, desired), grants: Map(granted.key -> granted) }

  run unconsumedMembershipCannotProveEmptyPlanTest = not(C::zeroStepMembership(issued, desired, "a"))
  run exactConsumedMembershipProvesEmptyPlanTest =
    C::zeroStepMembership({ ...issued, effectReceipts: Set(recorded) }, desired, "a")
  run foreignReceiptCannotProveMembershipTest = not(C::zeroStepMembership({ ...issued,
    effectReceipts: Set({ ...recorded, grant: { ...recorded.grant, revision: 5 } }) }, desired, "a"))
  run checkpointSubsumesOldGrantsTest = C::zeroStepMembership({ ...issued,
    groups: issued.groups.setBy("a", row => { ...row, sourceVersion: 2, proven: Some(desired.groups.get("a")) }) }, desired, "a")

  pure val drained = { ...doc, version: 2, parent: 1,
    nodes: doc.nodes.setBy("n3", n => { ...n, state: Draining }),
    groups: doc.groups.setBy("a", g => { ...g, replicas: g.replicas.filter(r => r.key.node != "n3") }) }
  pure val draining = { ...base, target: drained, latest: drained, history: base.history.put(2, drained) }
  run nodeProgressWaitsForLoadEvacuationTest =
    C::advanceNodes(draining).nodes.get("n3").proven == Some(doc.nodes.get("n3"))
  run nativeEvacuationAllowsNodeProgressTest = {
    val proven = { ...draining, groups: draining.groups.setBy("a", row => { ...row,
      sourceVersion: 2, proven: Some(drained.groups.get("a")) }) }
    C::advanceNodes(proven).nodes.get("n3").proven == Some(drained.nodes.get("n3"))
  }
  run unprovenMetaPreventsNodeProgressTest = {
    val pending = { ...draining, groups: draining.groups.setBy("_meta", row => { ...row,
      owner: Some({ version: 2, attempt: 1 }) }).setBy("a", row => { ...row, sourceVersion: 2, proven: Some(drained.groups.get("a")) }) }
    C::advanceNodes(pending).nodes.get("n3").proven == Some(doc.nodes.get("n3"))
  }
}
