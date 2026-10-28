module scheduler_repair {
  import types.* from "../types"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import repair_edits as Edit from "../topology/repair_edits"
  import placement as P from "../topology/placement"
  import authority as A from "./authority"
  import repair as R from "./repair"

  // The replicated command checks the exact edit, scope ownership and CAS.
  // It cannot consult remote live health; the driver rechecks that before
  // submission and the approved native steps recheck it at execution.
  pure def shapeAllowed(meta: MetadataState, decision: RepairDecision): bool =
    if (not(Edit::shape(decision)) or not(meta.target.groups.keys().contains(decision.group)) or
        not(meta.groups.keys().contains(decision.group))) false else {
      val group = meta.target.groups.get(decision.group)
      val row = meta.groups.get(decision.group)
      row.owner == None and not(row.deleting) and row.sourceVersion > 0 and row.proven == Some(group) and
        match decision.kind {
          | RestoreMissingReplica => group.voterConfig == None and group.replicas.exists(r =>
              decision.missing == Some(r.key) and r.role == Voter and meta.target.nodes.get(r.key.node).state == Active)
          | ReconfigureVoters => group.voterConfig != None and {
              val count = group.replicas.filter(r => r.role == Voter).size()
              val target = group.voterConfig.unwrap().target
              val shape = if (decision.promote != None and decision.demote != None) count == target
                else if (decision.promote != None) count < target else count > target
              shape and
                (match decision.promote { | Some(key) => group.replicas.exists(r => r.key == key and r.role == Learner and not(r.pinnedLearner)) and
                    meta.target.nodes.get(key.node).state == Active | None => true }) and
                (match decision.demote { | Some(key) => group.replicas.exists(r => r.key == key and r.role == Voter and not(r.pinnedLearner)) and
                    P::canRetain(meta.target, meta.target, group, meta.target.nodes.get(key.node)) | None => true })
            }
        }
    }

  pure def document(meta: MetadataState, decision: RepairDecision): Document = {
    val group = meta.target.groups.get(decision.group)
    val updated = match decision.kind {
      | ReconfigureVoters => Edit::roles(group, decision)
      | RestoreMissingReplica => {
          val old = decision.missing.unwrap()
          val latestFloor = if (meta.latest.groups.keys().contains(group.id)) meta.latest.groups.get(group.id).nextReplica else 1
          val fresh = max(max(group.nextReplica, latestFloor), meta.replicaAllocationFloors.getOrElse(group.id, 1))
          { ...group, nextReplica: fresh + 1,
            replicas: group.replicas.map(r => if (r.key == old) { ...r, key: { ...r.key, replica: fresh } } else r) }
        }
    }
    { ...meta.target, version: meta.latest.version + 1, parent: meta.target.version, rebasedFrom: None,
      groups: meta.target.groups.put(group.id, updated),
      audit: { actor: if (decision.kind == ReconfigureVoters) "auto-voter-config" else "auto-repair", comment: "scheduler", forced: None } }
  }

  pure def latestAfter(meta: MetadataState, desired: Document): Document =
    if (R::semantic(meta.latest) == R::semantic(meta.target)) desired else R::rebase(meta.target, meta.latest, desired)

  pure def candidate(meta: MetadataState, desired: Document): MetadataState = {
    val stage = A::installStage(meta, { desired: desired,
      request: { edit: StageDocument(desired), options: { expectedVersion: meta.latest.version,
        audit: desired.audit, preview: false, approve: false } } })
    // Automatic repairs have no public convenience-mutation replay intent.
    { ...stage, stagedIntents: meta.stagedIntents }
  }

  // CommitVoterChange validates its own requested role steps in addition to
  // general plan reconstruction. A fabricated native base cannot omit the
  // requested promotion merely because it already lists that voter.
  pure def voterPlanAllowed(command: SchedulerCommand): bool = {
    val decision = command.decision
    val approval = command.approval
    if (decision.kind != ReconfigureVoters) true
    else if (not(approval.plan.blocks.keys().contains(decision.group)) or
        not(approval.captured.observations.keys().contains(decision.group))) false else {
      val steps = approval.plan.blocks.get(decision.group)
      val policy = approval.desired.groups.get(decision.group).voterConfig.unwrap()
      val bound = if (decision.group == "_meta") approval.captured.rules.maxNodes
        else P::effective(approval.desired, approval.desired.groups.get(decision.group)).maxVoters
      val initial = { valid: approval.plan.cleanup.getOrElse(decision.group, List()).length() == 0,
        voters: approval.captured.observations.get(decision.group).native.voters,
        promoted: false, demoted: false, transferred: false, waits: 0 }
      // Use list order, rather than an unordered set fold, for phase evidence.
      val checked = steps.foldl(initial, (state, step) => {
        val target = match step.target { | ReplicaTarget(key) => Some(key) | _ => None }
        match step.kind {
          | WaitCatchUp => { ...state, waits: state.waits + 1, valid: state.valid and
              target != None and target == decision.promote and not(state.promoted) and
              state.waits < 2 and (state.waits == 0 or state.demoted) }
          | PromoteLearner => { ...state, promoted: true,
              voters: match target { | Some(key) => state.voters.union(Set(key)) | None => state.voters },
              valid: state.valid and target != None and target == decision.promote and
                not(state.promoted) and state.waits > 0 and
                (match target { | Some(key) => not(state.voters.contains(key)) | None => false }) and
                state.voters.size() < policy.target + policy.maxSurge and state.voters.size() < bound }
          | DemoteVoter => { ...state, demoted: true,
              voters: match target { | Some(key) => state.voters.exclude(Set(key)) | None => state.voters },
              valid: state.valid and target != None and target == decision.demote and not(state.demoted) and
                (match target { | Some(key) => state.voters.contains(key) | None => false }) and
                (match step.transferTarget { | Some(key) => state.voters.contains(key) | None => false }) and
                (state.voters.size() - 1) * 2 > state.voters.size() }
          | TransferLeadership => { ...state, transferred: true, valid: state.valid and not(state.transferred) and
              decision.demote != None and not(state.demoted) and target != decision.demote and
              (match target { | Some(key) => state.voters.contains(key) | None => false }) and
              steps.indices().exists(i => steps.nth(i) == step and i + 1 < steps.length() and
                steps.nth(i + 1).kind == DemoteVoter and steps.nth(i + 1).transferTarget == target) }
          | _ => { ...state, valid: false }
        }
      })
      checked.valid and checked.promoted == (decision.promote != None) and checked.demoted == (decision.demote != None)
    }
  }

  pure def errors(meta: MetadataState, command: SchedulerCommand, rules: Rules): Set[Refusal] = {
    val approval = command.approval
    if (command.expectedVersion != meta.latest.version or command.expectedTarget != meta.target.version) Set(Conflict)
    else if (not(shapeAllowed(meta, command.decision))) Set(Unauthorized)
    else {
      val desired = document(meta, command.decision)
      if (approval.desired != desired or approval.origin != Reconciler or approval.audit != desired.audit or
          not(voterPlanAllowed(command)) or
          approval.plan.blocks.keys() != Set(command.decision.group) or
          approval.plan.blocks.values().map(steps => steps.length()).fold(0, (total, count) => total + count) == 0) Set(Unauthorized)
      else if (not(R::independent(meta.target, meta.latest, desired))) Set(Conflict)
      else {
        val latest = latestAfter(meta, desired)
        A::approvalErrors(candidate(meta, desired), approval, rules)
          .union(P::validateTransition(desired, latest, meta.retiredGroupIds, rules))
      }
    }
  }

  pure def apply(meta: MetadataState, command: SchedulerCommand): MetadataState = {
    val desired = command.approval.desired
    val approved = A::installApproval(candidate(meta, desired), command.approval)
    val latest = latestAfter(meta, desired)
    { ...approved, latest: latest, history: approved.history.put(latest.version, latest),
      replicaAllocationFloors: latest.groups.values().fold(approved.replicaAllocationFloors, (floors, group) =>
        floors.put(group.id, max(floors.getOrElse(group.id, 1), group.nextReplica))) }
  }
}
