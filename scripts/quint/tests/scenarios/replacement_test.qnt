module replacement_test {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import scaling_helpers.* from "./scaling_helpers"
  export scaling_helpers.*

  // This named case uses admission4Meta: no load evacuation is claimed here.
  run metadataReplacementWithFourthHostTest = init.then(growNode("n4", 1, 2))
    .then(approveScaleEdit(ForceRetireNode({ name: "n3", reason: "replace host" }), 2, 3))
    .then(executeMetaBlock(3)).then({
      nondet receipt = world.extensions.consensus.get("_meta").authorityReceipts.filter(r =>
        (match r.target { | Some(key) => key.node == "n3" | _ => false }) and
        (match r.effect { | Some(e) => e.kind == RemoveMember | _ => false })).oneOf()
      applyRemovedMeta("n3", receipt)
    }).expect(world.extensions.consensus.get("_meta").voters.map(r => r.node) == Set("n1", "n2", "n4"))
    .expect(world.extensions.metadata.target.audit.forced == Some({ node: "n3", reason: "replace host" }))
    .expect(world.extensions.metadata.memberAllocationFloor == 5).expect(safety)

}
