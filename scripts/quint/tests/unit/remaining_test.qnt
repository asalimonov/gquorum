module remaining_test {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import remaining as R from "../../metaservice/remaining"
  import topology as T from "../../topology/topology"
  import grants as G from "../../metaengine/grants"
  import grants_fixtures as GF from "../fixtures/grants"
  import authority_fixtures as AF from "../fixtures/authority"
  import planning_fixtures as PF from "../fixtures/planning"

  run retryValidatesOriginalNativePredecessorTest = {
    val observation = PF::input.observations.get("a")
    val changed = { ...observation, native: { ...observation.native,
      voters: observation.native.voters.exclude(Set(GF::replica)), learners: Set(GF::replica) } }
    and { R::errors(AF::failed, AF::attempt, PF::input) == Set(),
      R::errors(AF::failed, AF::attempt, { ...PF::input,
        observations: PF::input.observations.put("a", changed) }).contains(Conflict) }
  }
  run retryAcceptsConsumedCurrentNativeStepTest = {
    val meta = G::recordReceipt({ ...GF::issued,
      operations: AF::failed.operations, groups: AF::failed.groups }, GF::receipt)
    val observation = PF::input.observations.get("a")
    val changed = { ...observation, native: { ...observation.native,
      voters: observation.native.voters.exclude(Set(GF::replica)), learners: Set(GF::replica) } }
    R::errors(meta, AF::attempt, { ...PF::input,
      observations: PF::input.observations.put("a", changed) }) == Set()
  }
  run retryRejectsReceiptWithoutNativeResultTest = {
    val meta = G::recordReceipt({ ...GF::issued,
      operations: AF::failed.operations, groups: AF::failed.groups }, GF::receipt)
    R::errors(meta, AF::attempt, PF::input).contains(Conflict)
  }
  run retryRejectsMissingCompletedPrefixTest = {
    val failed = { ...AF::failed, operations: AF::failed.operations.setBy(AF::attempt, op => { ...op,
      blocks: op.blocks.setBy("a", b => { ...b, step: b.step + 1 }) }) }
    R::errors(failed, AF::attempt, PF::input).contains(Unauthorized)
  }
  run retryRejectsLostTransferPrerequisiteTest = {
    val r = GF::replica
    val other = { ...r, node: "n4", member: 4, replica: 4 }
    val sim: R::Simulated = { voters: GF::planned.expectedVoters, learners: Set(), errors: Set() }
    val step = { ...GF::planned, transferTarget: Some(other) }
    R::simulate(sim, step, 3, false).errors.contains(UnsafeMembership)
  }
  run retryUsesConfiguredSurgeAndCapacityTest = {
    val doc = AF::failed.target
    val meta = { ...doc.groups.get("_meta"), voterConfig: Some(T::defaultVoterConfig) }
    and { R::voterMaximum(doc, meta, { ...PF::input.rules, maxNodes: 5 }) == 4,
      R::voterMaximum(doc, meta, { ...PF::input.rules, maxNodes: 3 }) == 3 }
  }
}
