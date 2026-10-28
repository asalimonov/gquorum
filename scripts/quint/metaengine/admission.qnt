module admission {
  import types.* from "../types"
  import basicSpells.* from "../vendor/spells/basicSpells"

  // Fresh learner-first admission only. Former-owner retirement/readmission
  // is a distinct Task 10 workflow; these rules never rebind a retained pin.
  pure def current(meta: MetadataState, a: ProcessAdmission): bool =
    if (not(meta.target.nodes.keys().contains(a.owner.node)) or not(meta.groups.keys().contains("_meta")) or
        not(meta.operations.keys().contains(a.operation))) false else {
      val node = meta.target.nodes.get(a.owner.node)
      val origin = match a.origin { | Some(key) => key | None => a.operation }
      val op = meta.operations.get(origin)
      a.owner.node == a.replica.node and a.replica.group == "_meta" and
        a.replica.cluster == meta.target.cluster and a.replica.member == node.member and
        a.replica.replica == node.member and node.state != Excluded and
        meta.target.groups.get("_meta").replicas.exists(r => r.key == a.replica) and
        a.owner.incarnation > 0 and meta.groups.get("_meta").owner == Some(a.operation) and
        op.plan.blocks.keys().contains("_meta") and
        op.plan.blocks.get("_meta").select(s => s.kind == PrepareReplica and s.target == ReplicaTarget(a.replica)).length() > 0 and
        op.plan.blocks.get("_meta").select(s => s.kind == AddLearner and s.target == ReplicaTarget(a.replica)).length() > 0
    }

  pure def same(a: ProcessAdmission, b: ProcessAdmission): bool = { ...a, ready: false } == { ...b, ready: false }

  pure def prepared(meta: MetadataState, replica: ReplicaKey): bool =
    (match meta.groups.get("_meta").proven { | Some(g) => g.replicas.exists(r => r.key == replica) | None => false }) or
      meta.effectReceipts.exists(r => r.target == Some(replica) and match r.effect {
        | Some(e) => e.kind == PrepareReplica | None => false })

  pure def errors(meta: MetadataState, command: AdmissionCommand): Set[Refusal] = match command {
    | BindAdmission(b) => {
        val a = b.admission
        if (not(current(meta, a)) or a.ready) Set(Unauthorized)
        else if (meta.admissions.keys().contains(a.owner.node) and same(meta.admissions.get(a.owner.node), a)) Set()
        else if (prepared(meta, a.replica)) Set(Unauthorized)
        else if (meta.activeSessions.keys().contains(a.owner.node)) Set(IdentityMismatch)
        else if (meta.admissions.keys().contains(a.owner.node)) {
          val old = meta.admissions.get(a.owner.node)
          if (b.previous != Some(old.owner) or old.ready or old.replica != a.replica or old.operation != a.operation)
            Set(Unauthorized) else Set()
        } else if (b.previous != None) Set(Conflict) else Set()
      }
    | ReadyAdmission(a) => if (not(current(meta, a)) or not(meta.admissions.keys().contains(a.owner.node))) Set(Unauthorized)
        else if (not(same(meta.admissions.get(a.owner.node), a))) Set(IdentityMismatch) else Set()
  }

  pure def apply(meta: MetadataState, command: AdmissionCommand): MetadataState = match command {
    | BindAdmission(b) => if (meta.admissions.keys().contains(b.admission.owner.node) and
        same(meta.admissions.get(b.admission.owner.node), b.admission)) meta
        else { ...meta, admissions: meta.admissions.put(b.admission.owner.node, { ...b.admission, ready: false }) }
    | ReadyAdmission(a) => { ...meta, admissions: meta.admissions.setBy(a.owner.node, old => { ...old, ready: true }) }
  }

  pure def preparationAllowed(meta: MetadataState, approved: ApprovedAction): bool =
    if (approved.step.kind != PrepareReplica) true else match approved.step.target {
      | ReplicaTarget(replica) => if (replica.group != "_meta")
          meta.cleanup.values().filter(debt => debt.identity.group == replica.group and
            debt.identity.node == replica.node and debt.identity != replica).forall(debt => debt.retired != None) and
          ((match meta.groups.get("_meta").proven { | Some(group) => group.replicas.exists(r =>
            r.key.node == replica.node and r.key.member == replica.member) | None => false }) or
          (meta.admissions.keys().contains(replica.node) and {
            val a = meta.admissions.get(replica.node)
            a.replica.member == replica.member and (a.ready or
              meta.operations.get(a.operation).plan.blocks.get("_meta").select(s => s.kind == AddLearner and
                s.target == ReplicaTarget(a.replica) and s.seq < meta.operations.get(a.operation).blocks.get("_meta").step).length() > 0) and
              prepared(meta, a.replica)
          }))
          else if (not(meta.admissions.keys().contains(replica.node))) false else {
            val a = meta.admissions.get(replica.node)
            a.replica == replica and a.operation == approved.operation and current(meta, a)
          }
      | _ => false
    }

  pure def preparedProcess(meta: MetadataState, a: ProcessAdmission): bool = a.ready or
    meta.effectReceipts.exists(r => r.target == Some(a.replica) and match r.effect {
      | Some(e) => Set(AddLearner, PromoteLearner).contains(e.kind) and
          (e.operation != a.operation or meta.operations.get(a.operation).blocks.get("_meta").step > e.step or
            meta.operations.get(a.operation).blocks.get("_meta").phase == OpSucceeded)
      | None => false })

  pure def completionAllowed(meta: MetadataState, approved: ApprovedAction): bool = {
    val group = match approved.step.target { | ReplicaTarget(r) => r.group | GroupTarget(g) => g }
    if (group != "_meta") true else {
      val later = meta.operations.get(approved.operation).plan.blocks.get(group).select(s => s.seq > approved.step.seq)
      later.length() > 0 or meta.admissions.values().filter(a => a.operation == approved.operation).forall(a =>
        preparedProcess(meta, a) or (approved.step.target == ReplicaTarget(a.replica) and
          Set(AddLearner, PromoteLearner).contains(approved.step.kind)))
    }
  }

  pure def liveAdmission(meta: MetadataState, a: ProcessAdmission): bool =
    meta.operations.keys().contains(a.operation) and meta.operations.get(a.operation).blocks.keys().contains("_meta") and
      not(Set(OpSucceeded, Superseded).contains(meta.operations.get(a.operation).blocks.get("_meta").phase))

  // authority_queries.scopedProcessIdentity prefers a live admission; the
  // reserved session is the fallback only for proven exact membership.
  pure def courier(meta: MetadataState, node: NodeName): Option[{ process: ProcessKey, member: int }] = {
    val wanted = if (meta.target.groups.keys().contains("_meta"))
      meta.target.groups.get("_meta").replicas.filter(r => r.key.node == node).map(r => r.key.member) else Set()
    val member = if (wanted.size() == 1) wanted.getOnlyElement() else if (meta.nodes.keys().contains(node))
      match meta.nodes.get(node).proven { | Some(n) => if (n.state == Excluded) 0 else n.member | None => 0 } else 0
    if (member == 0) None else if (meta.admissions.keys().contains(node) and
        meta.admissions.get(node).replica.member == member and liveAdmission(meta, meta.admissions.get(node)))
      Some({ process: meta.admissions.get(node).owner, member: member })
    else if (not(meta.groups.keys().contains("_meta")) or not(meta.activeSessions.keys().contains(node))) None else {
      val key = meta.activeSessions.get(node)
      val proven = match meta.groups.get("_meta").proven { | Some(g) =>
        g.replicas.exists(r => r.key.node == node and r.key.member == member) | None => false }
      if (proven and meta.sessions.get(key).phase == Live and meta.sessions.get(key).member == member)
        Some({ process: key.owner, member: member }) else None
    }
  }

  pure def validSuccessor(meta: MetadataState, desired: Document, plan: Plan, key: AttemptKey): bool =
    if (not(plan.blocks.keys().contains("_meta"))) true else meta.admissions.values().forall(a => {
      val wanted = desired.groups.get("_meta").replicas.filter(r => r.key == a.replica)
      if (not(liveAdmission(meta, a)) or wanted.size() == 0) true else {
        val previous = meta.operations.get(a.operation)
        val owned = meta.groups.get("_meta").owner == Some(a.operation) or
          (meta.groups.get("_meta").owner == None and previous.blocks.get("_meta").phase == OpFailed)
        if (not(owned)) false else if (key.version == a.operation.version)
          key.attempt == a.operation.attempt + 1 and plan == previous.plan
        else {
          val row = meta.groups.get("_meta")
          val proven = match row.proven { | Some(g) => g.replicas.filter(r => r.key == a.replica) | None => Set() }
          val effects = meta.effectReceipts.filter(r => r.target == Some(a.replica)).filter(r => match r.effect {
            | Some(e) => e.operation.version > row.sourceVersion | None => false })
          val prepared = proven.size() > 0 or effects.exists(r => r.effect.unwrap().kind == PrepareReplica)
          val learner = proven.size() > 0 or effects.exists(r => r.effect.unwrap().kind == AddLearner)
          val promoted = proven.exists(r => r.role == Voter) or effects.exists(r => r.effect.unwrap().kind == PromoteLearner)
          val suffix = plan.blocks.get("_meta").select(s => s.target == ReplicaTarget(a.replica) and s.kind != TransferLeadership).foldl(List(), (kinds, step) => kinds.append(step.kind))
          val role = wanted.getOnlyElement().role
          if (promoted) (if (role == Voter) suffix == List() else suffix == List(DemoteVoter))
          else {
            val hasPrepare = suffix.select(k => k == PrepareReplica).length() > 0
            val hasAdd = suffix.select(k => k == AddLearner).length() > 0
            val prefix = (if (hasPrepare) List() else List(PrepareReplica))
              .concat(if (hasAdd) List() else List(AddLearner))
            (hasPrepare or prepared) and (hasAdd or learner) and prefix.concat(suffix) ==
              (if (role == Voter) List(PrepareReplica, AddLearner, WaitCatchUp, PromoteLearner) else List(PrepareReplica, AddLearner))
          }
        }
      }
    })

  pure def handover(before: MetadataState, after: MetadataState, key: AttemptKey): MetadataState =
    if (not(after.operations.get(key).blocks.keys().contains("_meta")) or
        after.operations.get(key).blocks.get("_meta").phase == OpSucceeded) after else {
      val desired = after.target.groups.get("_meta").replicas.map(r => r.key)
      { ...after, admissions: after.admissions.transformValues(a =>
        if (not(liveAdmission(before, a)) or not(desired.contains(a.replica))) a else
          { ...a, operation: key, origin: match a.origin { | Some(old) => Some(old) | None => Some(a.operation) } }) }
    }

  pure def metadataSafety(meta: MetadataState): bool = meta.admissions.keys().forall(node => {
    val a = meta.admissions.get(node)
    a.owner.node == node and a.owner.incarnation > 0 and a.replica.node == node and
      a.replica.group == "_meta" and a.replica.member == a.replica.replica and
      meta.operations.keys().contains(a.operation) and meta.history.keys().contains(a.operation.version) and
      (match a.origin { | Some(key) => key != a.operation and meta.operations.keys().contains(key) and
          key.version <= a.operation.version | None => true }) and
      meta.history.get(a.operation.version).groups.get("_meta").replicas.exists(r => r.key == a.replica)
  })
}
