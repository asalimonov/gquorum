module managed_retirement_steps {
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import frames as F from "../runtime/frames"
  import execution as E from "./execution"
  import lifecycle as Life from "./lifecycle"

  pure def retiring(d: EffectDelivery): bool = (d.grant.purpose == Effect or d.grant.purpose == Readmission) and d.replica.group != "_meta" and
    d.resource == HostResource({ group: d.replica.group, node: d.replica.node }) and
    (match d.grant.effect { | Some(e) => Set(RetireReplica, DeleteGroupState).contains(e.kind) | None => d.grant.purpose == Readmission })

  pure def otherAllowed(host: LocalNode, d: EffectDelivery, other: ReplicaKey): bool =
    other.cluster == d.replica.cluster and other.node == d.replica.node and other.group == d.replica.group and
      (if (d.grant.purpose == Readmission) d.grant.targets.contains(other) else match d.grant.effect { | Some(e) => if (e.kind == DeleteGroupState) d.grant.targets.contains(other)
        else host.volatile.localView.target.groups.keys().contains(other.group) and
          host.volatile.localView.target.groups.get(other.group).replicas.exists(r => r.key == other)
        | None => false })

  pure def batchCurrent(host: LocalNode, d: EffectDelivery): bool = d.grant.purpose != Readmission or
    (host.volatile.handoverJoined and match host.volatile.readmissionOffer {
      | Some(offer) => d.grant.admission == Some(offer.prior) and offer.grants.forall(g => g.resources.forall(r =>
          host.durable.authorityGrants.keys().contains(r) and host.durable.authorityGrants.get(r) == g and
            host.durable.authorityFloors.getOrElse(r, 0) == g.key.revision))
      | None => false })

  // Absence is acceptable only after validating the complete authority. Any
  // successor is checked and preserved, never treated as this retirement's data.
  pure def resourcesSafe(w: World, d: EffectDelivery): bool = {
    val host = w.system.get(d.receiver.node)
    val key: ReplicaKey = d.replica
    and { (not(host.volatile.owners.keys().contains(key.group)) or {
      val owner = host.volatile.owners.get(key.group)
      if (owner.key == key) owner.phase == OwnerStopped and Life::released(owner)
      else otherAllowed(host, d, owner.key)
    }) and host.durable.replicaLedger.filter(r => r.group == key.group).forall(r => r == key or otherAllowed(host, d, r)) and
      host.durable.replicaEvidence.keys().filter(r => r.group == key.group).forall(r =>
        (r == key or otherAllowed(host, d, r)) and match host.durable.replicaEvidence.get(r) {
          | ValidStore(_) => true | MissingStore => true | _ => false }) }
  }

  pure def beginEnabled(w: World, d: EffectDelivery, limit: int): bool = retiring(d) and
    w.system.get(d.receiver.node).volatile.waitingEffects.contains(d) and E::mayConsume(w, d) and
    not(E::consumed(w, d)) and resourcesSafe(w, d) and batchCurrent(w.system.get(d.receiver.node), d) and
    not(w.system.get(d.receiver.node).volatile.retirementWork.keys().contains(d.replica)) and
    w.system.get(d.receiver.node).durable.journalPosition < limit

  pure def beginNext(w: World, d: EffectDelivery): World = F::withNode(w, d.receiver.node, host => { ...host,
    volatile: { ...host.volatile, retirementWork: host.volatile.retirementWork.put(d.replica, { delivery: d, phase: RetireWaiting }) },
    durable: { ...host.durable, journalPosition: host.durable.journalPosition + 1,
      authorityFloors: host.durable.authorityFloors.put(d.resource, d.grant.key.revision),
      authorityGrants: host.durable.authorityGrants.put(d.resource, d.grant) } })

  pure def resultFor(d: EffectDelivery): RetirementResult = { grant: d.grant, identity: d.replica, verified: true }

  pure def returnEnabled(w: World, node: NodeName, key: ReplicaKey): bool =
    (E::hostAvailable(w.system.get(node)) or w.system.get(node).volatile.phase == Stopping) and
    w.system.get(node).volatile.retirementWork.keys().contains(key) and
    w.system.get(node).volatile.retirementWork.get(key).phase == RetireWaiting

  pure def returnNext(w: World, node: NodeName, key: ReplicaKey, result: RetirementResult): World =
    F::withProcess(w, node, process => { ...process, retirementWork:
      if (result == resultFor(process.retirementWork.get(key).delivery))
        process.retirementWork.setBy(key, work => { ...work, phase: RetireChecked })
      else process.retirementWork.mapRemove(key) })

  pure def phaseEnabled(w: World, node: NodeName, key: ReplicaKey, phase: RetirementPhase, limit: int): bool = {
    val host = w.system.get(node)
    host.volatile.retirementWork.keys().contains(key) and {
      val work = host.volatile.retirementWork.get(key)
      work.phase == phase and E::mayConsume(w, work.delivery) and resourcesSafe(w, work.delivery) and batchCurrent(host, work.delivery) and
        host.durable.journalPosition < limit
    }
  }

  pure def quarantineNext(w: World, node: NodeName, key: ReplicaKey): World = F::withNode(w, node, host => {
    val hasData = host.durable.replicaEvidence.keys().contains(key) and
      (match host.durable.replicaEvidence.get(key) { | ValidStore(_) => true | _ => false })
    { ...host, volatile: { ...host.volatile, retirementWork: host.volatile.retirementWork.setBy(key, work =>
        { ...work, phase: RetireQuarantined }) },
      durable: { ...host.durable, journalPosition: host.durable.journalPosition + 1,
        replicaEvidence: host.durable.replicaEvidence.mapRemove(key),
        retiredStores: if (not(hasData) or host.durable.retiredStores.keys().contains(key)) host.durable.retiredStores
          else host.durable.retiredStores.put(key, { token: host.durable.journalPosition + 1, dataPresent: true, markerPresent: true }) } }
  })

  pure def finishNext(w: World, node: NodeName, key: ReplicaKey, deliveries: int): World = {
    val host = w.system.get(node)
    val d = host.volatile.retirementWork.get(key).delivery
    val position = host.durable.journalPosition + 1
    val receiptPosition = if (d.grant.purpose == Readmission) host.durable.fences.filter(f =>
      f.grant == d.grant.key and f.resource == d.resource).getOnlyElement().durablePosition else position
    val receipt = { ...E::receipt(d, receiptPosition), target: Some(key), outcome: EffectApplied, durableTerm: 0 }
    val evidence = { delivery: d, pin: host.durable.pin, incarnation: host.volatile.incarnation,
      owner: if (host.volatile.owners.keys().contains(key.group)) Some(host.volatile.owners.get(key.group).key) else None }
    val base = F::withNode(E::forgetWait(w, d), node, n => { ...n,
      volatile: { ...n.volatile, retirementWork: n.volatile.retirementWork.mapRemove(key) },
      durable: { ...n.durable, journalPosition: position, receipts: n.durable.receipts.union(Set(receipt)),
        replicaLedger: n.durable.replicaLedger.exclude(Set(key)), seedAuthority: n.durable.seedAuthority.exclude(Set(key)) } })
    E::returnReceipt({ ...base, extensions: { ...base.extensions,
      consumptions: base.extensions.consumptions.union(Set(evidence)) } }, d, receipt, deliveries)
  }

  pure def replayEnabled(w: World, d: EffectDelivery): bool = retiring(d) and
    w.system.get(d.receiver.node).volatile.waitingEffects.contains(d) and E::mayConsume(w, d) and E::consumed(w, d)

  pure def replayNext(w: World, d: EffectDelivery, deliveries: int): World = {
    val receipt = w.system.get(d.receiver.node).durable.receipts.filter(r =>
      r.grant == d.grant.key and r.resource == d.resource and r.target == Some(d.replica)).getOnlyElement()
    E::returnReceipt(E::forgetWait(w, d), d, receipt, deliveries)
  }

  pure def abandonEnabled(w: World, node: NodeName, key: ReplicaKey): bool =
    w.system.get(node).volatile.retirementWork.keys().contains(key) and {
      val work = w.system.get(node).volatile.retirementWork.get(key)
      work.phase != RetireWaiting and (not(E::mayConsume(w, work.delivery)) or not(resourcesSafe(w, work.delivery)) or not(batchCurrent(w.system.get(node), work.delivery)))
    }

  pure def abandonNext(w: World, node: NodeName, key: ReplicaKey): World = {
    val d = w.system.get(node).volatile.retirementWork.get(key).delivery
    F::withProcess(E::forgetWait(w, d), node, p => { ...p, retirementWork: p.retirementWork.mapRemove(key) })
  }
}
