module cleanup {
  import readmission_base as Readmit from "./readmission_base"
  import types.* from "../types"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import grants as G from "./grants"

  pure def assigned(meta: MetadataState, key: ReplicaKey): bool = meta.target.groups.keys().contains(key.group) and
    meta.target.groups.get(key.group).replicas.exists(r => r.key == key)

  // BeginCleanupEffect binds one identity, even for a whole-group plan step.
  pure def retirementGrants(meta: MetadataState, key: ReplicaKey, approved: ApprovedAction): Set[Grant] =
    G::actionGrants(meta, approved).filter(g => g.targets.contains(key))

  pure def latestRetirement(meta: MetadataState, key: ReplicaKey, approved: ApprovedAction): Set[Grant] = {
    val candidates = retirementGrants(meta, key, approved)
    candidates.filter(g => candidates.forall(other => other.key.revision <= g.key.revision))
  }

  pure def mayIssue(meta: MetadataState, key: ReplicaKey, approved: ApprovedAction): bool =
    if (key.group == "_meta" or not(meta.cleanup.keys().contains(key))) false else {
      val debt = meta.cleanup.get(key)
      val latest = latestRetirement(meta, key, approved)
      val host = HostResource({ group: key.group, node: key.node })
      val readmissionPending = not(meta.target.groups.keys().contains(key.group)) and
        meta.target.nodes.keys().contains(key.node) and meta.target.nodes.get(key.node).member > key.member and debt.retired == None
      debt.retireAction == Some(approved) and Set(RetireReplica, DeleteGroupState).contains(approved.step.kind) and
        debt.stopped != None and not(assigned(meta, key)) and not(readmissionPending) and
        (latest.exists(g => G::currentForIssuance(meta, g)) or
          (debt.retired == None and G::resourcesAvailable(meta, Set(host)) and
            latest.forall(g => G::resourceSettled(meta, g, host))))
    }

  pure def issue(meta: MetadataState, key: ReplicaKey, approved: ApprovedAction, revision: int): MetadataState = {
    val existing = latestRetirement(meta, key, approved).filter(g => G::currentForIssuance(meta, g))
    if (existing.size() > 0) meta else {
      val base = G::stepGrant(meta, approved, revision)
      G::install(meta, { ...base, resources: Set(HostResource({ group: key.group, node: key.node })), targets: Set(key) })
    }
  }

  pure def retirementProof(meta: MetadataState, receipt: CollectionReceipt): bool = {
    val key = receipt.identity
    if (not(meta.cleanup.keys().contains(key)) or not(meta.grants.keys().contains(receipt.grant))) false else {
      val debt = meta.cleanup.get(key)
      val grant = meta.grants.get(receipt.grant)
      debt.storage == StorageDefault and debt.stopped != None and debt.retired != None and
        receipt.grant.cluster == key.cluster and receipt.grant.group == key.group and grant.targets.contains(key) and
        receipt.token > 0 and receipt.sequence > 0 and
        meta.effectReceipts.exists(r => r.grant == receipt.grant and r.target == Some(key) and
          (G::matchesAction(r, debt.retireAction) or (grant.purpose == Readmission and match grant.admission {
            | Some(key) => Readmit::consumed(meta, key) | None => false })))
    }
  }

  pure def collectionErrors(meta: MetadataState, report: CollectionReport): Set[Refusal] = {
    val receipt = report.receipt
    if (not(retirementProof(meta, receipt))) Set(Unauthorized)
    else if (report.courier.node != receipt.identity.node or report.courier.incarnation <= 0 or
        report.member != Readmit::courierMember(meta, receipt.identity.node, receipt.identity.member)) Set(IdentityMismatch)
    else if (meta.cleanup.get(receipt.identity).collected != None and
        meta.cleanup.get(receipt.identity).collected != Some(receipt)) Set(Conflict) else Set()
  }

  pure def recordCollection(meta: MetadataState, report: CollectionReport): MetadataState = {
    ...meta, cleanup: meta.cleanup.setBy(report.receipt.identity, debt => { ...debt,
      collected: Some(report.receipt), collectionPhase: Reported })
  }
}
