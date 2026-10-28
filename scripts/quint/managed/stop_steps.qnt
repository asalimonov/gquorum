module managed_stop_steps {
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import frames as F from "../runtime/frames"
  import execution as E from "./execution"
  import lifecycle as Life from "./lifecycle"

  pure def loadStop(d: EffectDelivery): bool = d.grant.key.group != "_meta" and
    d.resource == HostResource({ group: d.grant.key.group, node: d.receiver.node }) and
    (match d.grant.effect { | Some(e) => e.kind == StopReplica and e.target == ReplicaTarget(d.replica) | None => false })

  pure def beginStopEnabled(w: World, d: EffectDelivery, limit: int): bool =
    if (not(loadStop(d))) false else {
      val host = w.system.get(d.receiver.node)
      host.volatile.waitingEffects.contains(d) and E::mayConsume(w, d) and not(E::consumed(w, d)) and
        host.durable.journalPosition < limit and
        (not(host.durable.authorityGrants.keys().contains(d.resource)) or host.durable.authorityGrants.get(d.resource) != d.grant) and
        (not(host.volatile.owners.keys().contains(d.grant.key.group)) or host.volatile.owners.get(d.grant.key.group).key == d.replica)
    }

  pure def beginStopNext(w: World, d: EffectDelivery): World = F::withNode(w, d.receiver.node, host => { ...host,
    durable: { ...host.durable, journalPosition: host.durable.journalPosition + 1,
      authorityFloors: host.durable.authorityFloors.put(d.resource, d.grant.key.revision),
      authorityGrants: host.durable.authorityGrants.put(d.resource, d.grant) },
    volatile: { ...host.volatile, owners: if (host.volatile.owners.keys().contains(d.grant.key.group))
      host.volatile.owners.setBy(d.grant.key.group, owner => if (owner.phase == OwnerStopped) owner else Life::requestStop(owner))
      else host.volatile.owners } })

  pure def finishStopEnabled(w: World, d: EffectDelivery, limit: int): bool =
    if (not(loadStop(d))) false else {
      val host = w.system.get(d.receiver.node)
      host.volatile.waitingEffects.contains(d) and E::mayConsume(w, d) and
        (E::consumed(w, d) or (host.durable.journalPosition < limit and
          host.durable.authorityGrants.keys().contains(d.resource) and host.durable.authorityGrants.get(d.resource) == d.grant and
          (not(host.volatile.owners.keys().contains(d.grant.key.group)) or {
            val owner = host.volatile.owners.get(d.grant.key.group)
            owner.key == d.replica and owner.phase == OwnerStopped and Life::released(owner) })))
    }

  pure def finishStopNext(w: World, d: EffectDelivery, deliveries: int): World = {
    val host = w.system.get(d.receiver.node)
    val position = host.durable.journalPosition + 1
    val proof = if (E::consumed(w, d)) host.durable.receipts.filter(r => r.grant == d.grant.key and r.resource == d.resource).getOnlyElement()
      else { ...E::receipt(d, position), durableTerm: 0,
        outcome: if (host.volatile.owners.keys().contains(d.grant.key.group)) EffectApplied else AlreadySatisfied }
    val evidence: ConsumptionEvidence = { delivery: d, pin: host.durable.pin, incarnation: host.volatile.incarnation,
      owner: if (host.volatile.owners.keys().contains(d.grant.key.group)) Some(host.volatile.owners.get(d.grant.key.group).key) else None }
    val forgotten = E::forgetWait(w, d)
    val consumed = if (E::consumed(w, d)) forgotten else {
      val base = F::withNode(forgotten, d.receiver.node, node => { ...node,
        durable: { ...node.durable, journalPosition: position, receipts: node.durable.receipts.union(Set(proof)) } })
      { ...base, extensions: { ...base.extensions, consumptions: base.extensions.consumptions.union(Set(evidence)) } }
    E::returnReceipt(consumed, d, proof, deliveries)
  }
}
