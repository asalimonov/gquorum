module remaining {
  import types.* from "../types"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import grants as G from "../metaengine/grants"
  import placement as P from "../topology/placement"

  type Simulated = { voters: Set[ReplicaKey], learners: Set[ReplicaKey], errors: Set[Refusal] }
  pure def changesMembership(kind: ActionKind): bool =
    Set(CreateFoundingGroup, AddLearner, PromoteLearner, DemoteVoter, RemoveMember).contains(kind)

  // Retried attempts keep the immutable plan and already consumed prefix. The
  // retained attempt chain, not whichever operation is Latest, identifies proof.
  pure def proofGrants(meta: MetadataState, op: Operation, step: PlannedStep): Set[Grant] =
    meta.grants.values().filter(g => g.planStep == Some(step) and match g.effect {
      | Some(e) => e.operation.version == op.key.version and e.operation.attempt <= op.key.attempt
      | None => false })
  pure def proven(meta: MetadataState, op: Operation, step: PlannedStep): bool =
    proofGrants(meta, op, step).exists(g => g.resources.forall(r => G::resourceConsumed(meta, g, r)))

  pure def simulate(sim: Simulated, step: PlannedStep, maximum: int, proof: bool): Simulated = {
    val membership = changesMembership(step.kind)
    val predecessor = if (proof) step.resultVoters else step.expectedVoters
    val errors = if (membership and sim.voters != predecessor) sim.errors.union(Set(Conflict)) else sim.errors
    val before = { ...sim, errors: errors }
    val changed = match step.target {
      | GroupTarget(_) => if (step.kind == CreateFoundingGroup) {
          ...before, voters: step.resultVoters,
          errors: if (sim.voters.size() > 0 and sim.voters != step.resultVoters)
            errors.union(Set(Conflict)) else errors,
        } else before
      | ReplicaTarget(target) => {
          val present = sim.voters.union(sim.learners).contains(target)
          val needsMember = Set(WaitCatchUp, PromoteLearner, DemoteVoter, TransferLeadership).contains(step.kind)
          val unsafeRemoval = sim.voters.contains(target) and
            Set(DemoteVoter, RemoveMember).contains(step.kind) and
            (sim.voters.size() - 1) * 2 <= sim.voters.size()
          val lostTransfer = step.kind == DemoteVoter and sim.voters.contains(target) and
            match step.transferTarget { | Some(t) => not(sim.voters.contains(t)) | None => false }
          val unsafe = (needsMember and not(present)) or unsafeRemoval or lostTransfer or
            (step.kind == TransferLeadership and not(sim.voters.contains(target))) or
            (Set(StopReplica, RetireReplica).contains(step.kind) and present)
          val checked = { ...before, errors: if (unsafe) errors.union(Set(UnsafeMembership)) else errors }
          match step.kind {
            | AddLearner => if (sim.voters.contains(target)) checked
                else { ...checked, learners: sim.learners.union(Set(target)) }
            | PromoteLearner => { ...checked, voters: sim.voters.union(Set(target)), learners: sim.learners.exclude(Set(target)) }
            | DemoteVoter => { ...checked, voters: sim.voters.exclude(Set(target)), learners: sim.learners.union(Set(target)) }
            | RemoveMember => { ...checked, voters: sim.voters.exclude(Set(target)), learners: sim.learners.exclude(Set(target)) }
            | _ => checked
          }
        }
    }
    { ...changed, errors: if ((membership and changed.voters != step.resultVoters) or changed.voters.size() > maximum)
      changed.errors.union(Set(UnsafeMembership)) else changed.errors }
  }

  pure def voterMaximum(doc: Document, group: GroupDecl, rules: Rules): int = {
    val bound = if (group.id == "_meta") rules.maxNodes else P::effective(doc, group).maxVoters
    match group.voterConfig { | Some(c) => if (c.target < bound) c.target + c.maxSurge else bound
      | None => bound }
  }

  pure def groupErrors(meta: MetadataState, op: Operation, group: GroupId, input: PlanningInput): Set[Refusal] =
    if (not(input.observations.keys().contains(group))) Set(Unavailable) else {
      val observation = input.observations.get(group)
      val native = observation.native
      val desired = meta.history.get(op.key.version).groups.get(group)
      val steps = op.plan.blocks.get(group)
      val cursor = op.blocks.get(group).step
      val prefix = steps.select(s => s.seq < cursor)
      val missingProof = prefix.indices().exists(i => {
        val s = prefix.nth(i)
        s.kind != WaitCatchUp and not(proven(meta, op, s))
      })
      val suffix = steps.select(s => s.seq >= cursor)
      val ahead = suffix.indices().exists(i => {
        val s = suffix.nth(i)
        s.seq > cursor and proven(meta, op, s)
      })
      val maximum = voterMaximum(meta.history.get(op.key.version), desired, input.rules)
      val initial: Simulated = { voters: native.voters, learners: native.learners,
        errors: if (missingProof or ahead) Set(Unauthorized) else Set() }
      val capacity = if (group == "_meta") input.rules.maxNodes else input.rules.maxReplicas
      val result = suffix.foldl(initial, (sim, step) => {
        val updated = simulate(sim, step, maximum, proven(meta, op, step))
        if (updated.voters.union(updated.learners).size() > capacity)
          { ...updated, errors: updated.errors.union(Set(CapacityExceeded)) } else updated
      })
      if (native.pendingChange != None) Set(Unavailable)
      else if (result.voters != P::voters(desired) or
          result.learners != desired.replicas.filter(r => r.role == Learner).map(r => r.key))
        result.errors.union(Set(Conflict)) else result.errors
    }

  pure def errors(meta: MetadataState, previous: AttemptKey, input: PlanningInput): Set[Refusal] = {
    val op = meta.operations.get(previous)
    op.blocks.keys().filter(g => op.blocks.get(g).phase == OpFailed).map(g =>
      groupErrors(meta, op, g, input)).flatten()
  }
}
