module controls {
  import types.* from "./types"
  import basicSpells.* from "./vendor/spells/basicSpells"
  import grants as G from "./grants"
  import authority as A from "./authority"

  pure def manualGrant(meta: MetadataState, target: ReplicaKey, request: RequestKey, revision: int): Grant = {
    key: { cluster: target.cluster, group: target.group, revision: revision }, purpose: Transfer,
    effect: Some({ operation: { version: meta.groups.get(target.group).sourceVersion, attempt: 0 },
      step: 0, kind: TransferLeadership, target: ReplicaTarget(target), grantRevision: revision }),
    admission: None, resources: Set(GroupResource(target.group)),
    targets: match meta.groups.get(target.group).proven { | Some(g) => g.replicas.map(r => r.key) | None => Set() },
    planStep: None, request: Some(request),
  }

  pure def manualAuthorized(meta: MetadataState, target: ReplicaKey): bool =
    if (not(meta.groups.keys().contains(target.group)) or not(meta.target.groups.keys().contains(target.group))) false
    else {
      val row = meta.groups.get(target.group)
      row.owner == None and not(row.deleting) and row.proven == Some(meta.target.groups.get(target.group)) and
        meta.target.groups.get(target.group).replicas.exists(r => r.key == target and r.role == Voter)
    }

  pure def manualReplay(meta: MetadataState, request: RequestKey): Set[Grant] =
    meta.grants.values().filter(g => g.purpose == Transfer and g.request == Some(request))

  pure def manualErrors(meta: MetadataState, target: ReplicaKey, request: RequestKey): Set[Refusal] = {
    val replay = manualReplay(meta, request)
    if (replay.size() > 0) {
      val grant = replay.getOnlyElement()
      if (not(G::currentForIssuance(meta, grant))) Set(Unauthorized)
      else match grant.effect { | Some(e) => if (e.target == ReplicaTarget(target)) Set() else Set(Conflict)
        | None => Set(Invalid) }
    } else if (not(manualAuthorized(meta, target))) Set(Unauthorized)
    else {
      val resource = GroupResource(target.group)
      val pending = meta.grants.values().filter(g => g.resources.contains(resource) and
        not(meta.closedGrants.contains(g.key)) and not(G::resourceConsumed(meta, g, resource)))
      val same = pending.forall(g => g.purpose == Transfer and match g.effect {
        | Some(e) => e.target == ReplicaTarget(target) | None => false })
      if (G::resourcesAvailable(meta, Set(resource)) and same) Set() else Set(Unavailable)
    }
  }

  pure def beginManual(meta: MetadataState, target: ReplicaKey, request: RequestKey, revision: int): MetadataState = {
    val same = meta.grants.values().exists(g => g.purpose == Transfer and G::currentForIssuance(meta, g) and
      (g.request == Some(request) or (not(G::resourceConsumed(meta, g, GroupResource(target.group))) and
        match g.effect { | Some(e) => e.target == ReplicaTarget(target) | None => false })))
    if (same) meta else G::install(meta, manualGrant(meta, target, request, revision))
  }

  pure def stepComplete(meta: MetadataState, approved: ApprovedAction, observedLeader: Option[ReplicaKey]): bool = {
    val grants = G::actionGrants(meta, approved)
    grants.size() == 1 and grants.forall(g => g.resources.forall(r => G::resourceConsumed(meta, g, r))) and
      (approved.step.kind != TransferLeadership or match approved.step.target {
        | ReplicaTarget(r) => observedLeader == Some(r) | _ => false })
  }

  pure def errors(meta: MetadataState, command: ControlCommand): Set[Refusal] = match command {
    | BeginBlock(b) => if (not(meta.operations.keys().contains(b.operation)) or not(meta.groups.keys().contains(b.group))) Set(NotFound)
        else if (meta.groups.get(b.group).owner != Some(b.operation)) Set(Unauthorized)
        else if (meta.operations.get(b.operation).blocks.get(b.group).phase != OpPending) Set(Conflict) else Set()
    | IssueEffect(a) => if (G::canIssueStep(meta, a)) Set() else Set(Unauthorized)
    | IssueTransfer(t) => {
        val replay = manualReplay(meta, t.request)
        val captured = if (replay.size() > 0) replay.forall(g => match g.effect {
          | Some(e) => e.operation.version == t.sourceVersion | None => false })
          else meta.groups.keys().contains(t.target.group) and
            meta.groups.get(t.target.group).sourceVersion == t.sourceVersion
        if (not(captured)) Set(Unauthorized) else manualErrors(meta, t.target, t.request)
      }
    | RecordEffect(r) => if (G::canRecordReceipt(meta, r)) Set() else Set(Unauthorized)
    | RecordFence(f) => if (G::canRecordFence(meta, f)) Set() else Set(Unauthorized)
    | CompleteStep(c) => if (G::exactStep(meta, c.approved) and stepComplete(meta, c.approved, c.observedLeader)) Set()
        else Set(Unauthorized)
  }

  pure def complete(meta: MetadataState, approved: ApprovedAction): MetadataState = {
    val group = G::groupOf(approved.step.target)
    val op = meta.operations.get(approved.operation)
    val remaining = op.plan.blocks.get(group).select(s => s.seq > approved.step.seq)
    val finished = remaining.length() == 0
    { ...meta, operations: meta.operations.setBy(approved.operation, old => { ...old,
      blocks: old.blocks.put(group, { phase: if (finished) OpSucceeded else OpRunning,
        step: if (finished) approved.step.seq else remaining.head().seq, reason: None }) }),
      groups: if (finished) meta.groups.setBy(group, row => { ...row, owner: None,
        sourceVersion: approved.operation.version,
        proven: Some(meta.history.get(approved.operation.version).groups.get(group)) }) else meta.groups }
  }

  pure def apply(meta: MetadataState, command: ControlCommand, revision: int): MetadataState =
    G::settleRevocations(match command {
      | BeginBlock(b) => { ...meta, operations: meta.operations.setBy(b.operation, op => { ...op,
          blocks: op.blocks.setBy(b.group, block => { ...block, phase: OpRunning }) }) }
      | IssueEffect(a) => G::issueStep(meta, a, revision)
      | IssueTransfer(t) => beginManual(meta, t.target, t.request, revision)
      | RecordEffect(r) => G::recordReceipt(meta, r)
      | RecordFence(f) => G::recordFence(meta, f)
      | CompleteStep(c) => complete(meta, c.approved)
    })

  // A successor closes both superseded execution and exact conflicting resources;
  // unrelated groups and already consumed one-shot work retain independence.
  pure def reconcile(before: MetadataState, after: MetadataState, revision: int): MetadataState = {
    val created = after.operations.keys().exclude(before.operations.keys())
    val resources = created.map(k => {
      val op = after.operations.get(k)
      val scopes = match op.predecessor { | Some(previous) => A::failedGroups(before.operations.get(previous))
        | None => op.plan.blocks.keys() }
      scopes.map(g => op.plan.blocks.getOrElse(g, List()).concat(op.plan.cleanup.getOrElse(g, List())))
        .map(steps => steps.indices().map(i => steps.nth(i).resources).flatten()).flatten()
    }).flatten()
    val obsolete = before.grants.keys().filter(k => {
      val grant = before.grants.get(k)
      val superseded = match grant.effect {
        | Some(e) => if (not(before.operations.keys().contains(e.operation))) false else {
            val prior = before.operations.get(e.operation).blocks.get(k.group)
            val now = after.operations.get(e.operation).blocks.get(k.group)
            prior.phase != Superseded and now.phase == Superseded or
              created.exists(successor => after.operations.get(successor).predecessor == Some(e.operation) and
                A::failedGroups(before.operations.get(e.operation)).contains(k.group))
          }
        | None => false
      }
      grant.purpose != Fence and not(before.closedGrants.contains(k)) and
        (superseded or grant.resources.intersect(resources).size() > 0)
    })
    if (obsolete.size() == 0) after else G::settleRevocations(G::closeGrants(after, obsolete, revision))
  }
}
