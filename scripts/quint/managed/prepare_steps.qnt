module managed_prepare_steps {
  import founding as NativeSeed from "../consensus/founding"
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import frames as F from "../runtime/frames"
  import execution as E from "./execution"
  import lifecycle as Life from "./lifecycle"
  import managed_lifetime_steps as Starts from "./lifetime_steps"

  pure def loadPreparation(delivery: EffectDelivery): bool = delivery.grant.key.group != "_meta" and
    delivery.resource == HostResource({ group: delivery.grant.key.group, node: delivery.receiver.node }) and
    (match delivery.grant.effect {
      | Some(e) => (e.kind == PrepareReplica and e.target == ReplicaTarget(delivery.replica)) or
          (NativeSeed::isFounding(delivery.grant) and delivery.grant.targets.contains(delivery.replica))
      | None => false })

  // Exact fresh authority permits first acquisition, never refounding an
  // established native member or overwriting an unresolved/foreign owner.
  pure def beginPrepareEnabled(w: World, delivery: EffectDelivery, requests: int, position: int): bool =
    if (not(loadPreparation(delivery))) false else {
      val host = w.system.get(delivery.receiver.node)
      val group = delivery.grant.key.group
      host.volatile.waitingEffects.contains(delivery) and E::mayConsume(w, delivery) and not(E::consumed(w, delivery)) and
        host.durable.journalPosition < position and
        not(host.volatile.starts.values().exists(work => work.key == delivery.replica and
          Set(StartPreparing, StartAccepted).contains(work.phase))) and
        Starts::requestStartEnabled(w, delivery.receiver.node, delivery.replica, false, requests) and
        host.durable.replicaEvidence.keys().filter(r => r.group == group).forall(r => r == delivery.replica) and
        (not(host.volatile.owners.keys().contains(group)) or Life::released(host.volatile.owners.get(group))) and
        (host.durable.replicaEvidence.keys().contains(delivery.replica) or
          not(w.extensions.consensus.keys().contains(group)) or
          (match delivery.grant.effect {
            | Some(e) => e.kind == CreateFoundingGroup and not(host.durable.seedAuthority.contains(delivery.replica)) and
                not(host.durable.receipts.exists(r => r.target == Some(delivery.replica))) and
                (not(host.durable.authorityGrants.keys().contains(delivery.resource)) or
                  host.durable.authorityGrants.get(delivery.resource).purpose == Fence) and
                w.extensions.consensus.get(group).voters == delivery.grant.targets
            | None => false }) or
          not(w.extensions.consensus.get(group).voters.union(w.extensions.consensus.get(group).learners).contains(delivery.replica)))
    }

  pure def beginPrepareNext(w: World, delivery: EffectDelivery, requests: int): World = {
    val base = F::withNode(w, delivery.receiver.node, host => { ...host, durable: { ...host.durable,
      journalPosition: if (host.durable.authorityGrants.keys().contains(delivery.resource) and
        host.durable.authorityGrants.get(delivery.resource) == delivery.grant) host.durable.journalPosition else host.durable.journalPosition + 1,
      authorityFloors: host.durable.authorityFloors.put(delivery.resource, delivery.grant.key.revision),
      authorityGrants: host.durable.authorityGrants.put(delivery.resource, delivery.grant),
      replicaEvidence: if (host.durable.replicaEvidence.keys().contains(delivery.replica)) host.durable.replicaEvidence
        else host.durable.replicaEvidence.put(delivery.replica, ValidStore({ snapshot: 0, commit: 0, tail: 0, appliedHint: 0 })) } })
    Starts::requestStartNext(base, delivery.receiver.node, delivery.replica, false, requests)
  }

  pure def finishPrepareEnabled(w: World, delivery: EffectDelivery, position: int): bool =
    if (not(loadPreparation(delivery))) false else {
      val host = w.system.get(delivery.receiver.node)
      val group = delivery.grant.key.group
      host.volatile.waitingEffects.contains(delivery) and E::mayConsume(w, delivery) and
        (E::consumed(w, delivery) or (host.durable.journalPosition < position and
          host.durable.authorityGrants.getOrElse(delivery.resource, delivery.grant) == delivery.grant and
          host.durable.authorityGrants.keys().contains(delivery.resource) and
          host.volatile.owners.keys().contains(group) and {
            val owner = host.volatile.owners.get(group)
            owner.key == delivery.replica and owner.phase == OwnerReady and owner.published and
              owner.replayReady and owner.startupAuthorized and owner.authorityRevision == delivery.grant.key.revision
          }))
    }

  pure def finishPrepareNext(w: World, delivery: EffectDelivery, deliveries: int): World =
    if (E::consumed(w, delivery)) E::forgetWait(w, delivery) else {
      val host = w.system.get(delivery.receiver.node)
      val position = host.durable.journalPosition + 1
      val receipt = { ...E::receipt(delivery, position), target: Some(delivery.replica), outcome: EffectApplied, durableTerm: 0 }
      val evidence: ConsumptionEvidence = { delivery: delivery, pin: host.durable.pin,
        incarnation: host.volatile.incarnation, owner: Some(delivery.replica) }
      val base = F::withNode(E::forgetWait(w, delivery), delivery.receiver.node, n => { ...n,
        durable: { ...n.durable, journalPosition: position, receipts: n.durable.receipts.union(Set(receipt)) } })
      E::returnReceipt({ ...base, extensions: { ...base.extensions,
        consumptions: base.extensions.consumptions.union(Set(evidence)) } }, delivery, receipt, deliveries)
    }
}
