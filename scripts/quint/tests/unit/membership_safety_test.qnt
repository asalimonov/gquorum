module membership_safety_test {
  import topology as T from "../../topology/topology"
  import placement as Placement from "../../topology/placement"
  import placement_fixtures as PF from "../fixtures/placement"
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import initial as I from "../../harness/initial"
  import membership as M from "../../consensus/membership"
  import planning as P from "../../topology/planning"

  pure val seed = I::worldFor(Set("n1", "n2", "n3", "n4"), Map("n1" -> 1, "n2" -> 2, "n3" -> 3), Set("a"), "n1")
  pure val native = seed.extensions.consensus.get("a")
  pure val incoming: ReplicaKey = { cluster: "c1", group: "a", node: "n4", member: 4, replica: 4 }
  pure val learner = { ...native, learners: Set(incoming), catchUp: native.catchUp.put(incoming, Behind) }
  pure val first = native.voters.filter(r => r.node == "n1").getOnlyElement()
  pure val third = native.voters.filter(r => r.node == "n3").getOnlyElement()

  run singleAutomaticChangeRequiredTest = and {
    M::singleAutomatic(List(AddLearner), false, true),
    not(M::singleAutomatic(List(), false, true)),
    not(M::singleAutomatic(List(AddLearner, PromoteLearner), false, true)),
    not(M::singleAutomatic(List(AddLearner), true, true)),
    not(M::singleAutomatic(List(AddLearner), false, false)),
    not(M::singleAutomatic(List(StopReplica), false, true)),
  }
  run nativePromotionRequiresLearnerTest =
    not(M::safeChange(native, PromoteLearner, incoming, native.voters.union(Set(incoming)), true))
  run nativePromotionRequiresCatchUpTest =
    not(M::safeChange(learner, PromoteLearner, incoming, native.voters.union(Set(incoming)), false))
  run nativePromotionRequiresPostQuorumTest = and {
    not(M::safeChange(learner, PromoteLearner, incoming, Set(first, incoming), true)),
    M::safeChange(learner, PromoteLearner, incoming, native.voters.union(Set(incoming)), true),
  }
  run nativeLearnerAdditionIsQuorumNeutralTest = and {
    M::safeChange(native, AddLearner, incoming, Set(), false),
    not(M::safeChange(native, AddLearner, first, native.voters, true)),
  }
  run nativeLeaderMustTransferTest = and {
    not(M::safeChange(native, RemoveMember, first, native.voters, true)),
    not(M::safeChange(native, DemoteVoter, first, native.voters, true)),
    M::safeChange({ ...native, leaderAuthority: Some(third) }, DemoteVoter, first, native.voters, true),
  }
  run nativeFinalVoterCannotLeaveTest = {
    val alone = { ...native, voters: Set(first), leaderAuthority: None }
    and { not(M::safeChange(alone, RemoveMember, first, Set(first), true)),
      not(M::safeChange(alone, DemoteVoter, first, Set(first), true)) }
  }
  run nativeRemovalNeedsLivePostQuorumTest = and {
    not(M::safeChange(native, RemoveMember, third, Set(first, third), true)),
    M::safeChange(native, RemoveMember, third, native.voters, true),
  }
  run nativeExactIdentityCannotAliasTest = and {
    not(M::safeChange(native, AddLearner, { ...incoming, replica: first.replica }, native.voters, true)),
    not(M::safeChange(native, AddLearner, { ...third, member: 4 }, native.voters, true)),
  }
  run nativeReadmissionAllowsDistinctMemberOnSameHostTest = {
    val meta = seed.extensions.consensus.get("_meta")
    val successor: ReplicaKey = { ...incoming, group: "_meta", node: "n3" }
    val prior: ReplicaKey = { ...third, group: "_meta" }
    val rebound = seed.system.setBy("n3", host => { ...host, durable: { ...host.durable,
      pin: Some({ cluster: "c1", node: "n3", member: 4 }),
      replicaEvidence: host.durable.replicaEvidence.put(successor, ValidStore({ snapshot: 0, commit: 0, tail: 0, appliedHint: 0 })) } })
    and { M::safeChange(meta, AddLearner, successor, meta.voters.exclude(Set(prior)), false),
      not(M::liveIdentity(rebound, prior)), M::liveIdentity(rebound, successor) }
  }
  run nativeApprovedVoterProjectionRequiredTest = {
    val step = P::stepFor(DemoteVoter, third, native.voters, native.voters.exclude(Set(third)))
    and { M::expectedChange(native, step),
      not(M::expectedChange(native, { ...step, resultVoters: native.voters })),
      not(M::expectedChange(native, { ...step, expectedVoters: Set(first) })) }
  }
  run nativeDemotionPreservesLearnerIdentityTest = {
    val result = M::applyChange(native, DemoteVoter, third)
    and { result.learners == Set(third), result.voters == native.voters.exclude(Set(third)),
      result.configRevision == native.configRevision + 1,
      result.leaderAuthority == native.leaderAuthority,
      M::nativeSatisfied(result, DemoteVoter, third) }
  }

  run explicitNodeCapacityIsEnforcedTest = {
    val five = I::declarations(Map("n1" -> 1, "n2" -> 2, "n3" -> 3, "n4" -> 4, "n5" -> 5), Set())
    and { Placement::validateDocument(five, PF::rules).size() == 0,
      Placement::validateDocument(five, { ...PF::rules, maxNodes: 4 }).size() > 0 }
  }
  run manualLearnerRoleRemainsPinnedTest = {
    val four = I::declarations(Map("n1" -> 1, "n2" -> 2, "n3" -> 3, "n4" -> 4), Set("a"))
    val configured = { ...four, groups: four.groups.setBy("a", g => { ...g,
      voterConfig: Some(T::defaultVoterConfig), hosting: Some({ mode: AllEligible, selectedNodes: List() }),
      replicas: g.replicas.map(r => if (r.key.node == "n4") { ...r, role: Learner } else r) }) }
    val meta = I::metadata(configured)
    val input: MutationInput = { view: { latest: configured, target: configured }, retired: Set(),
      memberFloor: meta.memberAllocationFloor, replicaFloors: meta.replicaAllocationFloors, history: meta.history,
      groups: meta.groups, nodes: meta.nodes, health: configured.nodes.keys().mapBy(_ => Healthy), rules: PF::rules }
    val changed = T::mutateDocument(input, { edit: ChangeReplica({ group: "a", node: "n3", role: Learner }),
      options: { expectedVersion: 1, audit: { actor: "operator", comment: "pin", forced: None }, preview: false, approve: false } })
    val group = changed.groups.get("a")
    group.replicas.exists(r => r.key.node == "n3" and r.role == Learner and r.pinnedLearner) and
      not(Placement::canVote(configured, changed, group, changed.nodes.get("n3")))
  }
}
