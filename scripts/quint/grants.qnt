module grants {
  import types.* from "./types"
  import basicSpells.* from "./vendor/spells/basicSpells"

  // metaengine.ValidateGrant checks immutable identity, including closed grants.
  // Closure controls new issuance; native/host resource floors fence execution.
  pure def catalogued(meta: MetadataState, grant: Grant): bool =
    meta.grants.keys().contains(grant.key) and meta.grants.get(grant.key) == grant

  pure def sameReceiptSlot(a: Receipt, b: Receipt): bool = a.grant == b.grant and a.resource == b.resource and
    match a.resource { | GroupResource(_) => true | HostResource(_) => a.target == b.target }

  pure def receiptFitsFence(receipt: Receipt, fence: FenceEvidence): bool =
    if (receipt.resource != fence.resource) true else match receipt.resource {
      | HostResource(_) =>
        if (receipt.grant == fence.grant) receipt.durablePosition == fence.durablePosition
        else if (receipt.grant.revision < fence.grant.revision) receipt.durablePosition < fence.durablePosition
        else receipt.durablePosition > fence.durablePosition
      | GroupResource(_) =>
        if (receipt.grant.revision < fence.grant.revision)
          receipt.durablePosition < fence.durablePosition and receipt.durableTerm <= fence.durableTerm
        else receipt.durablePosition >= fence.durablePosition and receipt.durableTerm >= fence.durableTerm
    }

  pure def orderedFences(a: FenceEvidence, b: FenceEvidence): bool =
    if (a.resource != b.resource or a.grant == b.grant) true else {
      val before = if (a.grant.revision < b.grant.revision) a else b
      val after = if (a.grant.revision < b.grant.revision) b else a
      before.durablePosition < after.durablePosition and match a.resource {
        | GroupResource(_) => before.durableTerm <= after.durableTerm
        | HostResource(_) => true
      }
    }

  pure def resourceMatches(resource: ResourceKey, target: ReplicaKey): bool = match resource {
    | GroupResource(group) => target.group == group
    | HostResource(host) => target.group == host.group and target.node == host.node
  }

  pure def receiptIdentifiesGrant(grant: Grant, receipt: Receipt): bool = and {
    receipt.grant == grant.key, grant.resources.contains(receipt.resource),
    receipt.effect == grant.effect, receipt.admission == grant.admission,
    receipt.planStep == grant.planStep,
    match receipt.target {
      | Some(target) => grant.targets.contains(target) and resourceMatches(receipt.resource, target) and
          match grant.effect { | Some(e) => match e.target { | ReplicaTarget(r) => target == r | GroupTarget(_) => true }
            | None => true }
      | None => match receipt.resource { | GroupResource(_) => match grant.effect {
          | Some(e) => e.kind == CreateFoundingGroup or e.kind == DeleteGroupState | None => false }
          | _ => false }
    },
    receipt.durablePosition > 0,
    match receipt.resource { | HostResource(_) => receipt.durableTerm == 0 | GroupResource(_) => receipt.durableTerm > 0 },
    grant.purpose != Fence,
    if (receipt.outcome == TransferStarted) match receipt.resource {
      | GroupResource(_) => match grant.effect { | Some(e) => e.kind == TransferLeadership | None => false }
      | _ => false
    }
    else true,
  }

  pure def canRecordReceipt(meta: MetadataState, receipt: Receipt): bool =
    if (not(meta.grants.keys().contains(receipt.grant))) false else and {
      receiptIdentifiesGrant(meta.grants.get(receipt.grant), receipt),
      meta.effectReceipts.forall(old => not(sameReceiptSlot(old, receipt)) or old == receipt),
      meta.fenceEvidence.forall(fence => receiptFitsFence(receipt, fence)),
    }

  pure def recordReceipt(meta: MetadataState, receipt: Receipt): MetadataState =
    { ...meta, effectReceipts: meta.effectReceipts.union(Set(receipt)) }

  // Consumption of a transfer command is not completion of the transfer or
  // settlement of resumable intent. Those require separate checks below.
  pure def resourceConsumed(meta: MetadataState, grant: Grant, resource: ResourceKey): bool =
    if (grant.purpose == Fence) meta.fenceEvidence.exists(f => f.grant == grant.key and f.resource == resource)
    else match resource {
      | GroupResource(_) => meta.effectReceipts.exists(r => r.grant == grant.key and r.resource == resource)
      | HostResource(host) => grant.targets.filter(t => t.node == host.node).forall(t =>
          meta.effectReceipts.exists(r => r.grant == grant.key and r.resource == resource and r.target == Some(t)))
    }

  pure def currentForIssuance(meta: MetadataState, grant: Grant): bool = and {
    catalogued(meta, grant), not(meta.closedGrants.contains(grant.key)),
    grant.resources.forall(resource =>
      (meta.authorityFloors.keys().contains(resource) and meta.authorityFloors.get(resource) == grant.key)
      or resourceConsumed(meta, grant, resource)),
  }

  pure def transferIntent(grant: Grant): bool = grant.purpose == Transfer or match grant.effect {
    | Some(effect) => effect.kind == TransferLeadership | None => false
  }

  pure def resourceSettled(meta: MetadataState, grant: Grant, resource: ResourceKey): bool =
    (not(transferIntent(grant)) and resourceConsumed(meta, grant, resource)) or
      meta.fenceEvidence.exists(f => f.resource == resource and f.grant.group == grant.key.group and
        f.grant.revision > grant.key.revision)

  pure def resourcesAvailable(meta: MetadataState, resources: Set[ResourceKey]): bool =
    meta.revocations.values().forall(r =>
      r.requiredResources.exclude(r.provenResources).intersect(resources).size() == 0)

  pure def fenceIdentifiesGrant(grant: Grant, fence: FenceEvidence): bool = and {
    fence.grant == grant.key, fence.receiver.cluster == grant.key.cluster,
    grant.resources.contains(fence.resource), fence.durablePosition > 0,
    match fence.resource {
      | HostResource(host) => and {
          fence.receiver.node == host.node, fence.replica == None, fence.durableTerm == 0,
          grant.targets.exists(t => t.node == host.node and t.member == fence.receiver.member),
        }
      | GroupResource(group) => fence.durableTerm > 0 and match fence.replica {
          | Some(r) => grant.targets.contains(r) and r.group == group and
              fence.receiver == { cluster: r.cluster, node: r.node, member: r.member }
          | None => false
        }
    },
  }

  pure def sameFenceBoundary(a: FenceEvidence, b: FenceEvidence): bool = and {
    a.grant == b.grant, a.resource == b.resource,
    a.durablePosition == b.durablePosition, a.durableTerm == b.durableTerm,
    match a.resource { | HostResource(_) => a.receiver.member == b.receiver.member | GroupResource(_) => true },
  }

  pure def canRecordFence(meta: MetadataState, fence: FenceEvidence): bool =
    if (not(meta.grants.keys().contains(fence.grant))) false else and {
      fenceIdentifiesGrant(meta.grants.get(fence.grant), fence),
      meta.fenceEvidence.forall(old => if (old.grant == fence.grant and old.resource == fence.resource)
        sameFenceBoundary(old, fence) else orderedFences(old, fence)),
      meta.effectReceipts.forall(receipt => receiptFitsFence(receipt, fence)),
    }

  pure def recordFence(meta: MetadataState, fence: FenceEvidence): MetadataState =
    if (meta.fenceEvidence.exists(old => old.grant == fence.grant and old.resource == fence.resource)) meta
    else { ...meta, fenceEvidence: meta.fenceEvidence.union(Set(fence)) }

  pure def groupOf(target: EffectTarget): GroupId = match target {
    | GroupTarget(g) => g | ReplicaTarget(r) => r.group
  }

  // topology.AuthorityGrant.Validate: purpose and resource shape are part of
  // authorization, not merely a convenient serialization of an action.
  pure def wellFormed(grant: Grant): bool = and {
    grant.key.revision > 0, grant.resources.size() > 0, grant.targets.size() > 0,
    grant.targets.forall(t => t.cluster == grant.key.cluster and t.group == grant.key.group),
    grant.resources.forall(r => match r {
      | GroupResource(g) => g == grant.key.group
      | HostResource(h) => h.group == grant.key.group and grant.targets.exists(t => t.node == h.node)
    }),
    match grant.purpose {
      | Fence => grant.effect == None and grant.admission == None and grant.planStep == None and grant.request == None
      | Readmission => grant.effect == None and grant.planStep == None and grant.request == None and
          match grant.admission { | Some(a) => a.predecessor > 0 and a.successor > a.predecessor and
            grant.resources.forall(r => r == HostResource({ group: grant.key.group, node: a.node }))
            | None => false }
      | _ => grant.admission == None and match grant.effect {
          | None => false
          | Some(e) => and {
              e.grantRevision == grant.key.revision, e.operation.version > 0,
              groupOf(e.target) == grant.key.group,
              match e.target { | ReplicaTarget(t) => grant.targets.contains(t)
                | GroupTarget(_) => e.kind == CreateFoundingGroup or e.kind == DeleteGroupState },
              if (grant.purpose == Transfer) and {
                e.operation.attempt == 0, e.step == 0, e.kind == TransferLeadership,
                grant.planStep == None, grant.request != None,
              } else and {
                e.operation.attempt > 0, e.step > 0, grant.request == None,
                match grant.planStep { | Some(s) => s.seq == e.step and s.kind == e.kind and s.target == e.target
                  | None => false },
              },
              if (Set(PrepareReplica, StopReplica, RetireReplica, DeleteGroupState).contains(e.kind))
                grant.resources.forall(r => match r { | HostResource(_) => true | _ => false })
              else if (e.kind == CreateFoundingGroup) grant.resources.contains(GroupResource(grant.key.group)) and
                grant.targets.forall(t => grant.resources.contains(HostResource({ group: t.group, node: t.node })))
              else e.kind != WaitCatchUp and grant.resources == Set(GroupResource(grant.key.group)),
            }
        }
    },
  }

  pure def install(meta: MetadataState, grant: Grant): MetadataState = {
    ...meta, grants: meta.grants.put(grant.key, grant),
    authorityFloors: grant.resources.fold(meta.authorityFloors, (floors, r) => floors.put(r, grant.key)),
  }

  pure def exactStep(meta: MetadataState, approved: ApprovedAction): bool = {
    val group = groupOf(approved.step.target)
    if (not(meta.operations.keys().contains(approved.operation)) or not(meta.groups.keys().contains(group))) false
    else {
      val op = meta.operations.get(approved.operation)
      if (not(op.blocks.keys().contains(group))) false else {
        val block = op.blocks.get(group)
        meta.groups.get(group).owner == Some(approved.operation) and
          not(Set(OpPending, OpSucceeded, OpFailed, Superseded).contains(block.phase)) and
          block.step == approved.step.seq and op.plan.blocks.get(group).indices().exists(i =>
            op.plan.blocks.get(group).nth(i) == approved.step)
      }
    }
  }

  pure def actionGrants(meta: MetadataState, approved: ApprovedAction): Set[Grant] =
    meta.grants.values().filter(g => g.purpose == Effect and g.planStep == Some(approved.step) and
      match g.effect { | Some(e) => e.operation == approved.operation | None => false })

  pure def stepGrant(meta: MetadataState, approved: ApprovedAction, revision: int): Grant = {
    val step = approved.step
    val group = groupOf(step.target)
    val native = step.resources.contains(GroupResource(group)) and step.kind != CreateFoundingGroup
    val historical = meta.history.values().filter(d => (d.version == 1 or meta.firstAttempts.keys().contains(d.version)) and d.groups.keys().contains(group))
      .map(d => d.groups.get(group).replicas.map(r => r.key)).flatten()
    val retained = meta.grants.values().filter(g => g.key.group == group).map(g => g.targets).flatten()
    { key: { cluster: meta.target.cluster, group: group, revision: revision }, purpose: Effect,
      effect: Some({ operation: approved.operation, step: step.seq, kind: step.kind,
        target: step.target, grantRevision: revision }), admission: None,
      resources: step.resources, targets: if (native) step.targets.union(historical).union(retained) else step.targets,
      planStep: Some(step), request: None }
  }

  pure def canIssueStep(meta: MetadataState, approved: ApprovedAction): bool = {
    val existing = actionGrants(meta, approved)
    exactStep(meta, approved) and approved.step.kind != WaitCatchUp and
      (if (existing.size() > 0) existing.size() == 1 and currentForIssuance(meta, existing.getOnlyElement())
       else resourcesAvailable(meta, approved.step.resources))
  }

  pure def issueStep(meta: MetadataState, approved: ApprovedAction, revision: int): MetadataState =
    if (actionGrants(meta, approved).size() > 0) meta else install(meta, stepGrant(meta, approved, revision))

  // Closure does not pretend an offline resource has installed its fence.
  // One fence per affected group is shared by exact resource obligations.
  pure def closeGrants(meta: MetadataState, obsolete: Set[GrantKey], revision: int): MetadataState = {
    val closed = { ...meta, closedGrants: meta.closedGrants.union(obsolete) }
    obsolete.map(k => k.group).fold(closed, (acc, group) => {
      val selected = obsolete.filter(k => k.group == group)
      val unsettled = selected.map(k => acc.grants.get(k).resources.filter(r =>
        not(resourceSettled(acc, acc.grants.get(k), r)))).flatten()
      val reuse = unsettled.filter(r => acc.authorityFloors.keys().contains(r) and
        acc.grants.get(acc.authorityFloors.get(r)).purpose == Fence)
      val fresh = unsettled.exclude(reuse)
      val key = { cluster: acc.target.cluster, group: group, revision: revision }
      val grant: Grant = { key: key, purpose: Fence, effect: None, admission: None,
        resources: fresh, targets: selected.map(k => acc.grants.get(k).targets).flatten(),
        planStep: None, request: None }
      val installed = if (fresh.size() > 0) install(acc, grant) else acc
      { ...installed, revocations: selected.fold(installed.revocations, (rows, k) => {
        val needed = acc.grants.get(k).resources.filter(r => unsettled.contains(r) and
          not(resourceSettled(acc, acc.grants.get(k), r)))
        val prior = rows.getOrElse(k, { requested: true, requiredResources: Set(),
          provenResources: Set(), evidence: Set(), fences: Map() })
        rows.put(k, { ...prior, requested: true,
          requiredResources: prior.requiredResources.union(needed),
          fences: needed.fold(prior.fences, (fs, r) => fs.put(r, installed.authorityFloors.get(r))) })
      }) }
    })
  }

  pure def settleRevocations(meta: MetadataState): MetadataState = {
    ...meta, revocations: meta.revocations.keys().mapBy(k => {
      val obligation = meta.revocations.get(k)
      val settled = obligation.requiredResources.filter(r => resourceSettled(meta, meta.grants.get(k), r))
      { ...obligation, provenResources: obligation.provenResources.union(settled),
        evidence: obligation.evidence.union(meta.fenceEvidence.filter(f =>
          obligation.requiredResources.contains(f.resource) and f.grant.revision > k.revision)) }
    }),
  }
}
