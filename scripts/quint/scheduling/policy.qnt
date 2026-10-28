module scheduling_policy {
  import repair_edits as Edit from "../topology/repair_edits"
  import types.* from "../types"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import placement as P from "../topology/placement"

  // Metrics, observation freshness, lag/backlog, stability and cooldown inputs
  // are fixed favorable by the harness. Actual readiness, native membership,
  // connectivity, scoped ownership and identity remain independent predicates.
  pure def readyGroup(input: SchedulerInput, group: GroupId): bool =
    input.target.groups.keys().contains(group) and input.groups.keys().contains(group) and {
      val row = input.groups.get(group)
      row.sourceVersion > 0 and row.owner == None and not(row.deleting) and row.proven == Some(input.target.groups.get(group))
    }

  pure def quorum(observed: NativeObservation): bool = observed.healthKnown and
    observed.native.pendingChange == None and observed.native.voters.size() > 0 and
    observed.native.voters.intersect(observed.live).size() * 2 > observed.native.voters.size() and
    (match observed.native.leaderAuthority { | Some(leader) => observed.native.voters.contains(leader) and observed.live.contains(leader)
      | None => false })

  pure def knownGroup(input: SchedulerInput, group: GroupId): bool = readyGroup(input, group) and
    input.observations.keys().contains(group) and quorum(input.observations.get(group))

  pure def configuredGroup(input: SchedulerInput, group: GroupId): bool = knownGroup(input, group) and {
    val desired = input.target.groups.get(group)
    val native = input.observations.get(group).native
    desired.voterConfig != None and
      native.voters == desired.replicas.filter(r => r.role == Voter).map(r => r.key) and
      native.learners == desired.replicas.filter(r => r.role == Learner).map(r => r.key)
  }

  pure def promotable(input: SchedulerInput, group: GroupId, key: ReplicaKey): bool = {
    val desired = input.target.groups.get(group)
    val observed = input.observations.get(group)
    desired.replicas.exists(r => r.key == key and r.role == Learner and not(r.pinnedLearner)) and
      input.target.nodes.keys().contains(key.node) and input.target.nodes.get(key.node).state == Active and
      P::canVote(input.target, input.target, desired, input.target.nodes.get(key.node)) and
      not(input.held.contains(key)) and observed.live.contains(key) and observed.ready.contains(key) and
      observed.native.catchUp.getOrElse(key, Behind) == CaughtUp
  }

  pure def demotable(input: SchedulerInput, group: GroupId, key: ReplicaKey): bool = {
    val desired = input.target.groups.get(group)
    desired.replicas.exists(r => r.key == key and r.role == Voter and not(r.pinnedLearner)) and
      input.target.nodes.keys().contains(key.node) and
      P::canRetain(input.target, input.target, desired, input.target.nodes.get(key.node))
  }

  pure def eligible(input: SchedulerInput, decision: RepairDecision): bool =
    if (not(Edit::shape(decision)) or not(knownGroup(input, decision.group))) false else {
      val group = input.target.groups.get(decision.group)
      val observed = input.observations.get(decision.group)
      match decision.kind {
        | RestoreMissingReplica => {
            val key = decision.missing.unwrap()
            group.voterConfig == None and key.cluster == input.target.cluster and
              group.replicas.exists(r => r.key == key and r.role == Voter) and
              input.target.nodes.get(key.node).state == Active and input.healthyNodes.contains(key.node) and
              not(observed.native.voters.union(observed.native.learners).contains(key)) and not(input.held.contains(key))
          }
        | ReconfigureVoters => if (not(configuredGroup(input, decision.group))) false else {
            val count = observed.native.voters.size()
            val target = group.voterConfig.unwrap().target
            val shape = if (decision.promote != None and decision.demote != None) count == target
              else if (decision.promote != None) count < target else count > target
            shape and
              (match decision.promote { | Some(key) => promotable(input, decision.group, key) | None => true }) and
              (match decision.demote { | Some(key) => demotable(input, decision.group, key) and
                  (decision.promote == None or not(observed.live.contains(key))) | None => true }) and
              Edit::roles(group, decision).replicas.filter(r => r.role == Voter).size() >= P::effective(input.target, group).minVoters and
              Edit::roles(group, decision).replicas.filter(r => r.role == Voter).size() <=
                (if (decision.group == "_meta") input.target.nodes.keys().size() else P::effective(input.target, group).maxVoters)
          }
      }
    }

  pure def ordered(identities: Set[ReplicaKey], order: List[NodeName]): List[ReplicaKey] =
    order.foldl(List(), (out, node) => {
      val matching = identities.filter(key => key.node == node)
      if (matching.size() == 1) out.append(matching.getOnlyElement()) else out
    })

  pure def firstEligible(input: SchedulerInput, choices: List[RepairDecision]): Option[RepairDecision] =
    choices.foldl(None, (chosen, decision) => if (chosen != None) chosen
      else if (eligible(input, decision)) Some(decision) else None)

  pure def chooseGroup(input: SchedulerInput, groupId: GroupId, nodeOrder: List[NodeName]): Option[RepairDecision] =
    if (not(knownGroup(input, groupId))) None else {
      val group = input.target.groups.get(groupId)
      val empty: RepairDecision = { kind: ReconfigureVoters, group: groupId, missing: None, promote: None, demote: None }
      val voters = ordered(group.replicas.filter(r => r.role == Voter).map(r => r.key), nodeOrder)
      val learners = ordered(group.replicas.filter(r => r.role == Learner).map(r => r.key), nodeOrder)
      if (group.voterConfig == None) firstEligible(input, voters.foldl(List(), (out, key) =>
        out.append({ ...empty, kind: RestoreMissingReplica, missing: Some(key) })))
      else {
        val observed = input.observations.get(groupId)
        val count = observed.native.voters.size()
        val target = group.voterConfig.unwrap().target
        val choices = if (count < target) learners.foldl(List(), (out, key) => out.append({ ...empty, promote: Some(key) }))
          else if (count > target) {
            val order = voters.select(key => observed.native.leaderAuthority != Some(key))
              .concat(voters.select(key => observed.native.leaderAuthority == Some(key)))
            order.foldl(List(), (out, key) => out.append({ ...empty, demote: Some(key) }))
          } else voters.foldl(List(), (out, old) => out.concat(learners.foldl(List(), (pairs, fresh) =>
            pairs.append({ ...empty, promote: Some(fresh), demote: Some(old) }))))
        firstEligible(input, choices)
      }
    }

  pure def choose(input: SchedulerInput, cursor: Option[GroupId], order: List[GroupId], nodeOrder: List[NodeName]): Option[RepairDecision] = {
    val groups = order.select(g => input.target.groups.keys().contains(g))
    val rotated = match cursor { | None => groups | Some(last) => {
      val pivot = order.indices().filter(i => order.nth(i) == last).fold(-1, (found, i) => i)
      val later = order.indices().filter(i => i > pivot).map(i => order.nth(i))
      groups.select(g => later.contains(g)).concat(groups.select(g => not(later.contains(g))))
    } }
    rotated.foldl(None, (selected, group) => if (selected != None) selected else chooseGroup(input, group, nodeOrder))
  }
}
