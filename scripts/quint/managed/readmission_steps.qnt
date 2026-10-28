module managed_readmission_steps {
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import frames as F from "../runtime/frames"
  import message_effects.* from "../runtime/messages"
  import readmission_base as B from "../metaengine/readmission_base"
  import admission as A from "../metaengine/admission"
  import grants as G from "../metaengine/grants"
  import execution as E from "./execution"
  import lifecycle as Life from "./lifecycle"
  import managed_retirement_steps as Retire from "./retirement_steps"

  pure def validOffer(w: World, msg: Envelope, offer: ReadmissionOffer): bool = {
    val host = w.system.get(msg.destination)
    if (not(w.extensions.metaLog.keys().contains(offer.proofIndex))) false else {
      val proof = w.extensions.metaLog.get(offer.proofIndex)
      offer.sender == msg.source and offer.delivery == msg.delivery and
        offer.admission.owner == { node: msg.destination, incarnation: host.volatile.incarnation } and
        offer.prior.node == msg.destination and offer.prior.successor == offer.admission.replica.member and
        host.durable.pin == Some({ cluster: offer.admission.replica.cluster, node: msg.destination, member: offer.prior.predecessor }) and
        A::current(proof, offer.admission) and proof.admissions.getOrElse(msg.destination, offer.admission) == offer.admission and
        offer.grants == B::batch(proof, offer.prior) and offer.grants.exists(g => g.key.group == "_meta") and
        offer.grants.forall(g => G::wellFormed(g) and G::currentForIssuance(proof, g)) and
        (match A::courier(proof, offer.sender.node) { | Some(c) => c.process == offer.sender and c.member == offer.senderMember | None => false })
    }
  }

  pure def receiveEnabled(w: World, msg: Envelope): bool = w.messages.get(msg.destination).contains(msg) and
    E::hostAvailable(w.system.get(msg.destination)) and match msg.payload { | ReadmissionAccepted(_) => true | _ => false }

  pure def receiveNext(w: World, msg: Envelope): World = match msg.payload {
    | ReadmissionAccepted(offer) => apply_custom_effect(if (validOffer(w, msg, offer)) F::withProcess(w, msg.destination, p => {
        ...p, readmissionOffer: Some(offer), handoverJoined: false, ownLease: Revoked, renewal: None }) else w, Consume(msg))
    | _ => w
  }

  pure def delivery(host: LocalNode, key: ReplicaKey): EffectDelivery = {
    val offer = host.volatile.readmissionOffer.unwrap()
    val grant = offer.grants.filter(g => g.targets.contains(key)).getOnlyElement()
    { delivery: offer.delivery, sender: offer.sender, senderMember: offer.senderMember,
      receiver: offer.admission.owner, proofIndex: Some(offer.proofIndex), replica: key,
      grant: grant, resource: HostResource({ group: key.group, node: key.node }) }
  }

  pure def oldMeta(host: LocalNode): ReplicaKey = host.volatile.readmissionOffer.unwrap().grants
    .filter(g => g.key.group == "_meta").getOnlyElement().targets.getOnlyElement()

  pure def floorsInstalled(host: LocalNode): bool = match host.volatile.readmissionOffer {
    | None => false
    | Some(offer) => offer.grants.forall(g => g.resources.forall(r =>
        host.durable.authorityFloors.getOrElse(r, 0) == g.key.revision and
        host.durable.authorityGrants.keys().contains(r) and host.durable.authorityGrants.get(r) == g))
  }

  pure def fenceEnabled(w: World, node: NodeName, group: GroupId, limit: int): bool = {
    val host = w.system.get(node)
    match host.volatile.readmissionOffer {
      | None => false
      | Some(offer) => if (not(offer.grants.exists(g => g.key.group == group))) false else {
          val grant = offer.grants.filter(g => g.key.group == group).getOnlyElement()
          val key = grant.targets.fold(oldMeta(host), (found, candidate) => if (found.group != group or candidate.replica < found.replica) candidate else found)
          val d = delivery(host, key)
          val resource = HostResource({ group: group, node: node })
          E::hostAvailable(host) and E::mayReceive(w, d) and host.durable.journalPosition < limit and
            host.durable.authorityFloors.getOrElse(resource, 0) <= grant.key.revision and
            (not(host.durable.authorityGrants.keys().contains(resource)) or host.durable.authorityGrants.get(resource) != grant)
        }
    }
  }

  pure def fenceNext(w: World, node: NodeName, group: GroupId): World = {
    val host = w.system.get(node)
    val grant = host.volatile.readmissionOffer.unwrap().grants.filter(g => g.key.group == group).getOnlyElement()
    val target = grant.targets.fold(oldMeta(host), (found, candidate) => if (found.group != group or candidate.replica < found.replica) candidate else found)
    val d = delivery(host, target)
    F::withNode(w, node, n => { ...n, durable: { ...n.durable,
      journalPosition: n.durable.journalPosition + 1,
      authorityFloors: n.durable.authorityFloors.put(d.resource, grant.key.revision),
      authorityGrants: n.durable.authorityGrants.put(d.resource, grant),
      pendingFences: n.durable.pendingFences.put(d.resource, { delivery: d, position: n.durable.journalPosition + 1 }) } })
  }

  pure def stopEnabled(w: World, node: NodeName): bool = {
    val host = w.system.get(node)
    E::hostAvailable(host) and floorsInstalled(host) and not(host.volatile.handoverJoined) and host.volatile.phase != Fenced
  }

  pure def stopNext(w: World, node: NodeName): World = F::withProcess(w, node, p => { ...p, phase: Fenced, ownLease: Revoked,
    owners: p.owners.transformValues(owner => if (owner.phase == OwnerStopped) owner else Life::requestStop(owner)) })

  pure def joinEnabled(w: World, node: NodeName): bool = {
    val host = w.system.get(node)
    host.volatile.phase == Fenced and floorsInstalled(host) and not(host.volatile.handoverJoined) and
      host.volatile.owners.values().forall(owner => owner.phase == OwnerStopped and Life::released(owner) and
        host.volatile.readmissionOffer.unwrap().grants.exists(g => g.targets.contains(owner.key))) and
      host.volatile.retirementWork.values().forall(work => work.phase != RetireWaiting) and
      (match host.volatile.schedulerCapture { | Some(work) => work.phase != ScheduleCallback | None => true })
  }

  pure def joinNext(w: World, node: NodeName): World = F::withNode(w, node, host => {
    val resources = host.volatile.readmissionOffer.unwrap().grants.map(g => g.resources).flatten()
    val fences = resources.filter(r => host.durable.pendingFences.keys().contains(r)).map(r => {
      val pending = host.durable.pendingFences.get(r)
      E::fence(pending.delivery, pending.position)
    })
    { ...host, volatile: { ...host.volatile, handoverJoined: true, metaRunning: false, schedulerCapture: None,
        retirementWork: Map(), collectionRequests: Map(), collectionAcks: Set(), fenceWorkers: Set(), waitingEffects: Set() },
      durable: { ...host.durable, fences: host.durable.fences.union(fences),
        pendingFences: resources.fold(host.durable.pendingFences, (pending, resource) => pending.mapRemove(resource)) } }
  })

  pure def beginLoadEnabled(w: World, node: NodeName, key: ReplicaKey, limit: int): bool = {
    val host = w.system.get(node)
    if (host.volatile.readmissionOffer == None or not(host.volatile.handoverJoined) or key.group == "_meta" or
        not(host.volatile.readmissionOffer.unwrap().grants.exists(g => g.targets.contains(key)))) false else {
      val d = delivery(host, key)
      val base = F::withProcess(w, node, p => { ...p, waitingEffects: p.waitingEffects.union(Set(d)) })
      floorsInstalled(host) and Retire::beginEnabled(base, d, limit)
    }
  }

  pure def beginLoadNext(w: World, node: NodeName, key: ReplicaKey): World = {
    val d = delivery(w.system.get(node), key)
    Retire::beginNext(F::withProcess(w, node, p => { ...p, waitingEffects: p.waitingEffects.union(Set(d)) }), d)
  }

  pure def loadsRetired(host: LocalNode): bool = match host.volatile.readmissionOffer {
    | None => false
    | Some(offer) => offer.grants.filter(g => g.key.group != "_meta").forall(g => g.targets.forall(key =>
        host.durable.receipts.exists(r => r.grant == g.key and r.target == Some(key))))
  }

  pure def backupEnabled(w: World, node: NodeName): bool = {
    val host = w.system.get(node)
    if (not(host.volatile.handoverJoined) or not(floorsInstalled(host)) or not(loadsRetired(host))) false else {
      val key = oldMeta(host)
      E::mayConsume(w, delivery(host, key)) and not(host.durable.readmissionBackup.contains(key)) and
        host.durable.replicaEvidence.keys().contains(key) and
        (match host.durable.replicaEvidence.get(key) { | ValidStore(_) => true | _ => false }) and
        host.durable.replicaLedger.size() == 0
    }
  }

  pure def backupNext(w: World, node: NodeName): World = F::withNode(w, node, host => {
    val key = oldMeta(host)
    { ...host, durable: { ...host.durable, replicaEvidence: host.durable.replicaEvidence.mapRemove(key),
      readmissionBackup: host.durable.readmissionBackup.union(Set(key)), metaImage: None, metaIndex: 0 } }
  })

  pure def finishMetaEnabled(w: World, node: NodeName, limit: int): bool = {
    val host = w.system.get(node)
    if (not(host.volatile.handoverJoined) or not(floorsInstalled(host)) or not(loadsRetired(host))) false else {
      val key = oldMeta(host)
      E::mayConsume(w, delivery(host, key)) and not(E::consumed(w, delivery(host, key))) and
        host.durable.readmissionBackup.contains(key) and not(host.durable.replicaEvidence.keys().contains(key)) and
        host.durable.journalPosition < limit
    }
  }

  pure def finishMetaNext(w: World, node: NodeName, deliveries: int): World = {
    val host = w.system.get(node)
    val key = oldMeta(host)
    val d = delivery(host, key)
    val fence = host.durable.fences.filter(f => f.grant == d.grant.key and f.resource == d.resource).getOnlyElement()
    val receipt = { ...E::receipt(d, fence.durablePosition), outcome: EffectApplied, durableTerm: 0 }
    val evidence = { delivery: d, pin: host.durable.pin, incarnation: host.volatile.incarnation, owner: None }
    val base = F::withNode(w, node, n => { ...n, durable: { ...n.durable, journalPosition: n.durable.journalPosition + 1,
      receipts: n.durable.receipts.union(Set(receipt)) } })
    E::returnReceipt({ ...base, extensions: { ...base.extensions, consumptions: base.extensions.consumptions.union(Set(evidence)) } }, d, receipt, deliveries)
  }

  pure def localBatchComplete(host: LocalNode): bool = loadsRetired(host) and host.durable.receipts.exists(r =>
    r.admission == Some(host.volatile.readmissionOffer.unwrap().prior) and r.target == Some(oldMeta(host)))

  pure def installPinEnabled(w: World, node: NodeName): bool = {
    val host = w.system.get(node)
    host.volatile.handoverJoined and floorsInstalled(host) and localBatchComplete(host) and
      host.durable.readmissionBackup.contains(oldMeta(host)) and host.durable.pin == Some({
        cluster: oldMeta(host).cluster, node: node, member: oldMeta(host).member })
  }

  pure def installPinNext(w: World, node: NodeName): World = F::withNode(w, node, host => {
    val offer = host.volatile.readmissionOffer.unwrap()
    val key = offer.admission.replica
    val empty = w.extensions.metaLog.get(0)
    { ...host, volatile: { ...host.volatile, phase: Starting, metaRunning: false, owners: Map(), starts: Map(), holds: Set(),
        session: None, ownLease: Revoked, leases: Map(), renewal: None, waitingEffects: Set(),
        appliedMetaIndex: 0, appliedMetaVersion: 0, localView: { latest: empty.latest, target: empty.target },
        joinOffer: Some({ admission: offer.admission, proofIndex: offer.proofIndex }) },
      durable: { ...host.durable, pin: Some({ cluster: key.cluster, node: node, member: key.member }), metaRemoved: None,
        replicaEvidence: host.durable.replicaEvidence.put(key, ValidStore({ snapshot: 0, commit: 0, tail: 0, appliedHint: 0 })) } }
  })
}
