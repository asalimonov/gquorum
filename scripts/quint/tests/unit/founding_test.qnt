module founding_test {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import initial as I from "../../harness/initial"
  import founding as F from "../../consensus/founding"
  import planning as P from "../../topology/planning"
  import grants as G from "../../metaengine/grants"

  pure val peers = Set("n1", "n2", "n3").map(n => I::replica(Map("n1" -> 1, "n2" -> 2, "n3" -> 3), "c", n))
  pure val step = { ...P::groupStep(CreateFoundingGroup, "c", peers), seq: 1 }
  pure val permit: Grant = { key: { cluster: "c1", group: "c", revision: 3 }, purpose: Effect,
    effect: Some({ operation: { version: 2, attempt: 1 }, step: 1, kind: CreateFoundingGroup,
      target: GroupTarget("c"), grantRevision: 3 }), admission: None, resources: step.resources,
    targets: peers, planStep: Some(step), request: None }

  run foundingRequiresExactVoterProofTest = and {
    G::wellFormed(permit), F::isFounding(permit),
    not(F::isFounding({ ...permit, planStep: Some({ ...step, expectedVoters: peers }) })),
    not(F::isFounding({ ...permit, targets: peers.filter(r => r.node != "n3") })),
    not(F::isFounding({ ...permit, planStep: Some({ ...step, resultVoters: Set() }) })),
  }
  run foundingConfigurationIsNotCompletionTest = {
    val native = F::initial(permit)
    and { native.voters == peers, native.learners == Set(), native.leaderAuthority == None,
      native.committedPosition == 0, native.authorityReceipts == Set(), native.authorityFloor == permit.key.revision }
  }
  run foundingCannotResetExistingConsensusTest = {
    val seed = I::worldFor(Set("n1", "n2", "n3"), Map("n1" -> 1, "n2" -> 2, "n3" -> 3), Set("c"), "n1")
    val withGrant = { ...seed, system: seed.system.setBy("n1", n => { ...n, durable: { ...n.durable,
      authorityGrants: n.durable.authorityGrants.put(HostResource({ group: "c", node: "n1" }), permit) } }) }
    F::engineStarted(withGrant, "n1", "c") == withGrant
  }
}
