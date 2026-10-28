module scheduler_policy_test {
  import types.* from "../../types"
  import basicSpells.* from "../../vendor/spells/basicSpells"
  import initial as I from "../../harness/initial"
  import topology as T from "../../topology/topology"
  import scheduling_policy as Policy from "../../scheduling/policy"
  import scheduler_repair as Commit from "../../metaengine/scheduler_repair"
  import authority as A from "../../metaengine/authority"
  import repair as R from "../../metaengine/repair"

  pure val nodeOrder = List("n1", "n2", "n3", "n4")
  pure val groupOrder = List("_meta", "a", "b")
  pure val founders = Map("n1" -> 1, "n2" -> 2, "n3" -> 3, "n4" -> 4)
  pure val raw = I::declarations(founders, Set("a", "b"))
  pure val base = { ...raw, groups: raw.groups.transformValues(g => if (g.id == "_meta") g else { ...g,
    replicas: g.replicas.map(r => if (r.key.node == "n4") { ...r, role: Learner } else r) }) }
  pure val configured = { ...base, groups: base.groups.setBy("a", g => { ...g,
    voterConfig: Some(T::defaultVoterConfig), hosting: Some({ mode: AllEligible, selectedNodes: List() }),
    replicas: g.replicas.map(r => if (r.key.node == "n4") { ...r, role: Learner } else r) }) }
  pure val world = I::worldFromDocument(founders.keys(), configured, "n1")
  pure val rules: Rules = { maxNodes: 4, maxReplicas: 5, concurrentGroups: 4, concurrentCleanup: 2,
    nodeOrder: nodeOrder, groupOrder: groupOrder }
  pure val input: SchedulerInput = { target: configured, groups: world.extensions.metadata.groups,
    observations: world.extensions.consensus.keys().mapBy(group => {
      native: world.extensions.consensus.get(group), healthKnown: true,
      live: configured.groups.get(group).replicas.map(r => r.key), ready: configured.groups.get(group).replicas.map(r => r.key) }),
    healthyNodes: founders.keys(), held: Set() }
  pure def key(group: GroupId, node: NodeName): ReplicaKey = configured.groups.get(group).replicas.filter(r => r.key.node == node).getOnlyElement().key
  pure val replacement: RepairDecision = { kind: ReconfigureVoters, group: "a", missing: None,
    promote: Some(key("a", "n4")), demote: Some(key("a", "n3")) }
  pure val unavailable = { ...input, observations: input.observations.setBy("a", o => { ...o,
    live: o.live.exclude(Set(key("a", "n3"))), ready: o.ready.exclude(Set(key("a", "n3"))) }) }
  pure val legacy: RepairDecision = { kind: RestoreMissingReplica, group: "b", missing: Some(key("b", "n3")), promote: None, demote: None }
  pure val missing = { ...input, observations: input.observations.setBy("b", o => { ...o, native: { ...o.native,
    voters: o.native.voters.exclude(Set(key("b", "n3"))) } }) }

  pure def command(meta: MetadataState, selected: RepairDecision, observed: SchedulerInput): SchedulerCommand = {
    val desired = Commit::document(meta, selected)
    val captured = { ...A::planningInput(meta, rules), observations: observed.observations }
    val plan = match T::makePlan(captured, desired) { | Planned(p) => p
      | PlanRejected(_) => { version: desired.version, blocks: Map(), cleanup: Map(), sources: Map(), dependencies: Set() } }
    { decision: selected, expectedVersion: meta.latest.version, expectedTarget: meta.target.version,
      approval: { desired: desired, plan: plan, captured: captured, origin: Reconciler, audit: desired.audit,
        scopes: Map(selected.group -> A::scopeAt(meta, selected.group)) } }
  }

  run schedulerKeepsHealthySeatsTest = Policy::choose(input, None, groupOrder, nodeOrder) == None
  run schedulerPromotesToFixedTargetTest = {
    val group = { ...configured.groups.get("a"), replicas: configured.groups.get("a").replicas.map(r =>
      if (r.key.node == "n3") { ...r, role: Learner } else r) }
    val native = input.observations.get("a").native
    val below = { ...input, target: { ...configured, groups: configured.groups.put("a", group) },
      groups: input.groups.setBy("a", row => { ...row, proven: Some(group) }),
      observations: input.observations.setBy("a", o => { ...o, native: { ...native,
        voters: native.voters.exclude(Set(key("a", "n3"))), learners: native.learners.union(Set(key("a", "n3"))) } }) }
    Policy::choose(below, None, groupOrder, nodeOrder) == Some({ ...replacement, promote: Some(key("a", "n3")), demote: None })
  }
  run schedulerExcessMetadataVotersPreferFollowerTest = {
    val group = { ...configured.groups.get("_meta"), voterConfig: Some(T::defaultVoterConfig) }
    val over = { ...input, target: { ...configured, groups: configured.groups.put("_meta", group) },
      groups: input.groups.setBy("_meta", row => { ...row, proven: Some(group) }) }
    Policy::choose(over, None, groupOrder, nodeOrder) == Some({ kind: ReconfigureVoters, group: "_meta",
      missing: None, promote: None, demote: Some(key("_meta", "n2")) })
  }
  run schedulerReplacesUnavailableVoterTest = Policy::choose(unavailable, None, groupOrder, nodeOrder) == Some(replacement)
  run schedulerDoesNotReplaceHealthyVoterTest = not(Policy::eligible(input, replacement))
  run schedulerPreservesPinnedLearnerTest = {
    val group = { ...configured.groups.get("a"), replicas: configured.groups.get("a").replicas.map(r =>
      if (r.key.node == "n4") { ...r, pinnedLearner: true } else r) }
    val held = { ...unavailable, target: { ...configured, groups: configured.groups.put("a", group) },
      groups: unavailable.groups.setBy("a", row => { ...row, proven: Some(group) }) }
    not(Policy::eligible(held, replacement))
  }
  run schedulerPreservesLocalHoldTest = not(Policy::eligible({ ...unavailable, held: Set(key("a", "n4")) }, replacement))
  run schedulerNeedsNativeQuorumTest = not(Policy::eligible({ ...unavailable, observations: unavailable.observations.setBy("a", o =>
    { ...o, live: Set(key("a", "n1")) }) }, replacement))
  run schedulerNeedsExactNativeConfigurationTest = not(Policy::eligible({ ...unavailable,
    observations: unavailable.observations.setBy("a", o => { ...o, native: { ...o.native, learners: Set() } }) }, replacement))
  run schedulerLegacyMissingIdentityIsExactTest = and {
    Policy::eligible(missing, legacy), not(Policy::eligible(input, legacy)),
    not(Policy::eligible(missing, { ...legacy, missing: Some({ ...key("b", "n3"), replica: 99 }) })),
    not(Policy::eligible({ ...missing, held: Set(key("b", "n3")) }, legacy)),
  }
  run schedulerUnavailableGroupDoesNotBlockAnotherTest = Policy::choose({ ...missing,
    observations: missing.observations.mapRemove("a") }, None, groupOrder, nodeOrder) == Some(legacy)
  run schedulerOwnedGroupCannotOriginateRepairTest = not(Policy::eligible({ ...unavailable,
    groups: unavailable.groups.setBy("a", row => { ...row, owner: Some({ version: 2, attempt: 1 }) }) }, replacement))
  run schedulerDecisionCursorAdvancesBeforeCommitTest = {
    val both = { ...unavailable, observations: unavailable.observations.put("b", missing.observations.get("b")) }
    and { Policy::choose(both, None, groupOrder, nodeOrder) == Some(replacement),
      Policy::choose(both, Some("a"), groupOrder, nodeOrder) == Some(legacy) }
  }
  run schedulerAutomaticDemotionDoesNotPinTest = {
    val desired = Commit::document(world.extensions.metadata, replacement)
    and { desired.groups.get("a").replicas.exists(r => r.key == key("a", "n3") and r.role == Learner and not(r.pinnedLearner)),
      desired.nodes == configured.nodes, desired.groups.get("b") == configured.groups.get("b") }
  }
  run schedulerReplicatedCASRejectsOutdatedCaptureTest = {
    val proposal = command(world.extensions.metadata, replacement, unavailable)
    Commit::errors(world.extensions.metadata, { ...proposal, expectedVersion: 0 }, rules) == Set(Conflict)
  }
  run schedulerVoterPlanCannotOmitRequestedPromotionTest = {
    val fake = { ...unavailable, observations: unavailable.observations.setBy("a", o => { ...o, native: { ...o.native,
      voters: o.native.voters.union(Set(key("a", "n4"))), learners: Set() } }) }
    val proposal = command(world.extensions.metadata, replacement, fake)
    Commit::errors(world.extensions.metadata, proposal, rules).contains(Unauthorized)
  }
  run schedulerExactRoleEditRejectsForeignChangesTest = {
    val proposal = command(world.extensions.metadata, replacement, unavailable)
    val foreign = { ...proposal.approval.desired, nodes: proposal.approval.desired.nodes.setBy("n2", n => { ...n, state: Draining }) }
    Commit::errors(world.extensions.metadata, { ...proposal, approval: { ...proposal.approval, desired: foreign } }, rules) == Set(Unauthorized)
  }
  run schedulerRepairDoesNotApproveStagedIntentTest = {
    val meta = world.extensions.metadata
    val edit: MutationRequest = { edit: ChangeReplica({ group: "b", node: "n2", role: Learner }),
      options: { expectedVersion: 1, audit: { actor: "operator", comment: "pending", forced: None }, preview: false, approve: false } }
    val staged = A::applyStage(meta, { request: edit, desired: T::mutateDocument(A::mutationInput(meta, Map(), rules), edit) })
    val proposal = command(staged, replacement, unavailable)
    val repaired = Commit::apply(staged, proposal)
    and { Commit::errors(staged, proposal, rules) == Set(),
      repaired.target.groups.get("b") == meta.target.groups.get("b"),
      repaired.latest.groups.get("b") == staged.latest.groups.get("b"),
      repaired.latest.rebasedFrom == Some(staged.latest.version),
      not(repaired.firstAttempts.keys().contains(repaired.latest.version)),
      repaired.operations.get({ version: repaired.target.version, attempt: 1 }).origin == Reconciler }
  }
}
