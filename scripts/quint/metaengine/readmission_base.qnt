module readmission_base {
  import types.* from "../types"
  import basicSpells.* from "../vendor/spells/basicSpells"

  pure def approvedHistory(meta: MetadataState): Set[Document] = meta.history.values().filter(doc =>
    doc.version == 1 or meta.firstAttempts.keys().contains(doc.version))

  pure def previousMember(meta: MetadataState, node: NodeName, successor: int): int =
    approvedHistory(meta).filter(doc => doc.nodes.keys().contains(node)).map(doc => doc.nodes.get(node).member)
      .filter(member => member < successor).fold(0, (old, member) => max(old, member))

  pure def priorTargets(meta: MetadataState, key: AdmissionKey): Set[ReplicaKey] = {
    val history = approvedHistory(meta).map(doc => doc.groups.values().map(g => g.replicas.map(r => r.key)).flatten()).flatten()
    val retained = history.union(meta.grants.values().map(g => g.targets).flatten()).union(meta.cleanup.keys())
    retained.filter(r => r.node == key.node and r.member == key.predecessor and
      (r.group != "_meta" or r.replica == key.predecessor) and
      (not(meta.target.groups.keys().contains(r.group)) or not(meta.target.groups.get(r.group).replicas.exists(x => x.key == r))))
  }

  pure def batch(meta: MetadataState, key: AdmissionKey): Set[Grant] =
    meta.grants.values().filter(g => g.purpose == Readmission and g.admission == Some(key))

  pure def consumed(meta: MetadataState, key: AdmissionKey): bool = {
    val grants = batch(meta, key)
    grants.exists(g => g.key.group == "_meta") and grants.forall(g => g.targets.forall(target =>
      meta.effectReceipts.exists(r => r.grant == g.key and r.target == Some(target) and r.admission == Some(key))))
  }

  pure def ready(meta: MetadataState, a: ProcessAdmission): bool =
    previousMember(meta, a.owner.node, a.replica.member) == 0 or meta.readmissionOrigins.keys().exists(key =>
      key.node == a.owner.node and key.successor == a.replica.member and consumed(meta, key))

  // Recording a partial batch preserves physical evidence but cannot settle
  // any of its debts. The final exact receipt settles the whole retained batch.
  pure def settle(meta: MetadataState): MetadataState = {
    val complete = meta.readmissionOrigins.keys().filter(key => consumed(meta, key))
    complete.fold(meta, (current, key) => {
      val receipts = current.effectReceipts.filter(r => r.admission == Some(key))
      { ...current, cleanup: receipts.fold(current.cleanup, (debts, receipt) => {
          val target = receipt.target.unwrap()
          val row = debts.getOrElse(target, { identity: target, sourceOperation: current.readmissionOrigins.get(key),
            storage: StorageDefault, stopAction: None, retireAction: None, stopGrant: None,
            removed: None, stopped: None, retired: None, collected: None, retention: Retain, collectionPhase: Unprepared })
          if (row.retired != None) debts else debts.put(target, { ...row,
            stopped: if (row.stopped == None) Some(receipt) else row.stopped, retired: Some(receipt) })
        }) }
    })
  }

  // Completed journal rebindings form monotonic, exact member links. A bounded
  // fold follows them in member order without recursion or a fresh Target read.
  pure def courierMember(meta: MetadataState, node: NodeName, former: int): int = {
    val links = meta.readmissionOrigins.keys().filter(key => key.node == node and consumed(meta, key))
    val ordered = links.fold(List(), (out, key) => out.select(k => k.successor < key.successor).append(key)
      .concat(out.select(k => k.successor >= key.successor)))
    val reachable = ordered.foldl(Set(former), (members, key) =>
      if (members.contains(key.predecessor)) members.union(Set(key.successor)) else members)
    if (reachable.exists(member => links.filter(key => key.predecessor == member).size() > 1)) 0
    else ordered.foldl(former, (member, key) => if (key.predecessor == member) key.successor else member)
  }
}
