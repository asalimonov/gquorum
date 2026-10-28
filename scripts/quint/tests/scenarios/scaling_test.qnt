module scaling_test {
  import admission as PendingAdmission from "../../metaengine/admission"
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import scaling_helpers.* from "./scaling_helpers"
  export scaling_helpers.*

  action scaledToFive = init.then(growNode("n4", 1, 2)).then(growNode("n5", 2, 3))

  run scaleThreeFourFiveFourThreeTest = init
    .then(growNode("n4", 1, 2)).expect(world.extensions.consensus.get("_meta").voters.size() == 4)
    .then(growNode("n5", 2, 3)).expect(world.extensions.consensus.get("_meta").voters.size() == 5)
    .then(removeNode("n5", 3, 4)).expect(world.extensions.consensus.get("_meta").voters.size() == 4)
    .then(removeNode("n4", 5, 6)).expect(world.extensions.consensus.get("_meta").voters.size() == 3)
    .expect(world.extensions.metadata.memberAllocationFloor == 6)
    .expect(world.extensions.consensus.get("a").voters.size() == 3)
    .expect(world.extensions.consensus.get("b").voters.size() == 3).expect(safety)

  run pendingAdmissionFollowsSupersedingOperationTest = preparedMeta.then(readyJoin)
    .then(reportAndCheckpoint(prepareApproved))
    .then(approveScaleEdit(AddNode({ name: "n5", member: 0, state: Active, nodeType: "worker", labels: Map() }), 2, 3))
    .expect(world.extensions.metadata.admissions.get("n4").operation == scaleAttempt(3))
    .expect(world.extensions.metadata.admissions.get("n4").origin == Some(scaleAttempt(2)))
    .then(bindScaleNode("n5")).then(executeMetaBlock(3))
    .expect(world.extensions.consensus.get("_meta").voters.size() == 5)
    .expect(world.extensions.metadata.operations.get(scaleAttempt(3)).blocks.get("_meta").phase == OpSucceeded).expect(safety)

  run admissionCourierPrecedesInternalSessionTest = freshMetaStarted
    .expect(world.extensions.metadata.activeSessions.keys().contains("n4") == false)
    .expect(PendingAdmission::courier(world.extensions.metadata, "n4") == Some({ process: { node: "n4", incarnation: 1 }, member: 4 }))
    .expect(safety)

  action scalingStageProbe = world' = world
  run scalingSharesWorldTest = clusterStageInit.then(capacityStageProbe).then(scalingStageProbe)
    .expect(world.system.keys() == NODE_POOL)
}
