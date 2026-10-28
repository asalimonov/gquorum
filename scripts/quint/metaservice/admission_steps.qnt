module metaservice_admission_steps {
  import readmission_base as Readmit from "../metaengine/readmission_base"
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import frames as F from "../runtime/frames"
  import message_effects.* from "../runtime/messages"
  import admission as A from "../metaengine/admission"
  import membership as M from "../consensus/membership"
  import execution as E from "../managed/execution"

  pure def requestJoinEnabled(w: World, node: NodeName, seed: NodeName, limit: int): bool =
    w.system.get(node).volatile.phase == Starting and w.system.get(node).durable.metaRemoved == None and
      w.extensions.nextDelivery <= limit and node != seed

  pure def requestJoinNext(w: World, node: NodeName, seed: NodeName): World = {
    val owner = { node: node, incarnation: w.system.get(node).volatile.incarnation }
    apply_custom_effect(w, Queue({ delivery: w.extensions.nextDelivery, source: owner, destination: seed,
      channel: MetaChannel, payload: JoinRequest({ owner: owner, member: match w.system.get(node).durable.pin { | Some(pin) => pin.member | None => 0 } }) }))
  }

  pure def confirmJoinEnabled(w: World, msg: Envelope): bool = {
    val node = w.system.get(msg.destination)
    val c = w.extensions.consensus.get("_meta")
    w.messages.get(msg.destination).contains(msg) and M::running(node) and
      not(node.volatile.authorityReads.keys().contains(msg.delivery)) and
      (match msg.payload { | JoinRequest(j) => j.owner == msg.source | _ => false }) and
      (match c.leaderAuthority { | Some(r) => r.node == msg.destination | None => false }) and
      M::hasCommitPath(c, w.system, w.extensions.network.metaLinks)
  }

  pure def confirmJoinNext(w: World, msg: Envelope): World =
    F::withProcess(w, msg.destination, p => { ...p, authorityReads: p.authorityReads.put(msg.delivery,
      w.extensions.consensus.get("_meta").committedPosition) })

  pure def joinApplied(w: World, msg: Envelope): bool = {
    val host = w.system.get(msg.destination)
    w.messages.get(msg.destination).contains(msg) and M::running(host) and
      host.volatile.authorityReads.keys().contains(msg.delivery) and
      host.volatile.appliedMetaIndex >= host.volatile.authorityReads.get(msg.delivery)
  }

  pure def bindCommand(w: World, msg: Envelope): Option[AdmissionCommand] = match msg.payload {
    | JoinRequest(j) => {
        val host = w.system.get(msg.destination)
        val local = w.extensions.metaLog.get(host.volatile.appliedMetaIndex)
        if (j.owner != msg.source or not(local.target.nodes.keys().contains(j.owner.node)) or
            not(local.groups.keys().contains("_meta"))) None
        else if ((j.member == 0 and local.target.nodes.get(j.owner.node).state != Active) or
          (j.member != 0 and j.member != local.target.nodes.get(j.owner.node).member and
            j.member != Readmit::previousMember(local, j.owner.node, local.target.nodes.get(j.owner.node).member)) or
          (j.member == 0 and Readmit::previousMember(local, j.owner.node, local.target.nodes.get(j.owner.node).member) > 0)) None
        else match local.groups.get("_meta").owner {
          | None => None
          | Some(operation) => {
              val replicas = local.target.groups.get("_meta").replicas.filter(r => r.key.node == j.owner.node)
              if (replicas.size() != 1) None else {
                val candidate: ProcessAdmission = { owner: j.owner, replica: replicas.getOnlyElement().key,
                  operation: operation, origin: None, ready: false }
                val old = local.admissions.getOrElse(j.owner.node, candidate)
                Some(BindAdmission({ admission: if (old.owner == candidate.owner and old.replica == candidate.replica and
                    old.operation == operation) { ...old, ready: false } else candidate,
                  previous: if (local.admissions.keys().contains(j.owner.node)) Some(old.owner) else None }))
              }
            }
        }
      }
    | _ => None
  }

  pure def replyJoinEnabled(w: World, msg: Envelope, limit: int): bool =
    if (not(joinApplied(w, msg)) or w.extensions.nextDelivery > limit) false else match msg.payload {
      | JoinRequest(j) => {
          val local = w.extensions.metaLog.get(w.system.get(msg.destination).volatile.appliedMetaIndex)
          if (not(local.admissions.keys().contains(j.owner.node))) false else {
            val a = local.admissions.get(j.owner.node)
            a.owner == j.owner and j.owner == msg.source and A::current(local, a) and
              (j.member == 0 or j.member == a.replica.member)
          }
        }
      | _ => false
    }

  pure def replyJoinNext(w: World, msg: Envelope): World = match msg.payload {
    | JoinRequest(j) => {
        val host = w.system.get(msg.destination)
        val index = host.volatile.appliedMetaIndex
        val a = w.extensions.metaLog.get(index).admissions.get(j.owner.node)
        val base = F::withProcess(w, msg.destination, p => { ...p, authorityReads: p.authorityReads.mapRemove(msg.delivery) })
        apply_custom_effect(apply_custom_effect(base, Consume(msg)), Queue({ delivery: w.extensions.nextDelivery,
          source: { node: msg.destination, incarnation: host.volatile.incarnation }, destination: j.owner.node,
          channel: MetaChannel, payload: JoinAccepted({ admission: a, proofIndex: index }) }))
      }
    | _ => w
  }

  pure def validOffer(w: World, msg: Envelope, offer: JoinOffer): bool = {
    val host = w.system.get(msg.destination)
    val a = offer.admission
    if (not(w.extensions.metaLog.keys().contains(offer.proofIndex))) false else {
      val proof = w.extensions.metaLog.get(offer.proofIndex)
      a.owner == { node: msg.destination, incarnation: host.volatile.incarnation } and
        a.replica.node == msg.destination and a.replica.group == "_meta" and A::current(proof, a) and
        (host.durable.pin == None or host.durable.pin == Some({ cluster: a.replica.cluster, node: msg.destination, member: a.replica.member })) and
        proof.admissions.getOrElse(msg.destination, a) == a and proof.admissions.keys().contains(msg.destination) and
        (match A::courier(proof, msg.source.node) { | Some(c) => c.process == msg.source | None => false })
    }
  }

  pure def receiveJoinEnabled(w: World, msg: Envelope): bool = w.messages.get(msg.destination).contains(msg) and
    M::running(w.system.get(msg.destination)) and (match msg.payload { | JoinAccepted(_) => true | _ => false })

  pure def receiveJoinNext(w: World, msg: Envelope): World = match msg.payload {
    | JoinAccepted(offer) => apply_custom_effect(if (validOffer(w, msg, offer))
        F::withProcess(w, msg.destination, p => { ...p, joinOffer: Some(offer) }) else w, Consume(msg))
    | _ => w
  }

  pure def pinJoinEnabled(w: World, node: NodeName): bool = {
    val host = w.system.get(node)
    match host.volatile.joinOffer {
      | Some(offer) => host.volatile.phase == Starting and host.durable.pin == None and
          host.durable.replicaEvidence.keys().size() == 0 and host.durable.metaImage == None and
          offer.admission.owner == { node: node, incarnation: host.volatile.incarnation }
      | None => false
    }
  }

  pure def pinJoinNext(w: World, node: NodeName): World = {
    val offer = w.system.get(node).volatile.joinOffer.unwrap()
    val key = offer.admission.replica
    F::withNode(w, node, host => { ...host, durable: { ...host.durable,
      pin: Some({ cluster: key.cluster, node: node, member: key.member }),
      replicaEvidence: host.durable.replicaEvidence.put(key, ValidStore({ snapshot: 0, commit: 0, tail: 0, appliedHint: 0 })) } })
  }

  pure def startJoinedMetaEnabled(w: World, node: NodeName): bool = {
    val host = w.system.get(node)
    match host.volatile.joinOffer {
      | Some(offer) => {
          val key = offer.admission.replica
          host.volatile.phase == Starting and not(host.volatile.metaRunning) and host.durable.metaRemoved == None and
            offer.admission.owner == { node: node, incarnation: host.volatile.incarnation } and
            host.durable.pin == Some({ cluster: key.cluster, node: node, member: key.member }) and
            host.durable.replicaEvidence.keys().contains(key) and M::validStore(host.durable.replicaEvidence.get(key))
        }
      | None => false
    }
  }

  pure def startJoinedMetaNext(w: World, node: NodeName): World =
    F::withProcess(w, node, p => { ...p, metaRunning: true })

  pure def prepareMetaEnabled(w: World, delivery: EffectDelivery, limit: int): bool = {
    val host = w.system.get(delivery.receiver.node)
    val kind = match delivery.grant.effect { | Some(e) => e.kind == PrepareReplica | None => false }
    kind and delivery.grant.key.group == "_meta" and
      delivery.resource == HostResource({ group: "_meta", node: delivery.receiver.node }) and
      host.volatile.waitingEffects.contains(delivery) and E::mayConsume(w, delivery) and
      (E::consumed(w, delivery) or (M::liveIdentity(w.system, delivery.replica) and host.durable.journalPosition < limit))
  }

  pure def prepareMetaNext(w: World, delivery: EffectDelivery, deliveries: int): World =
    if (E::consumed(w, delivery)) E::forgetWait(w, delivery) else {
      val host = w.system.get(delivery.receiver.node)
      val position = host.durable.journalPosition + 1
      val receipt = { ...E::receipt(delivery, position), outcome: EffectApplied, durableTerm: 0 }
      val evidence: ConsumptionEvidence = { delivery: delivery, pin: host.durable.pin,
        incarnation: host.volatile.incarnation, owner: None }
      val base = F::withNode(E::forgetWait(w, delivery), delivery.receiver.node, n => { ...n, durable: { ...n.durable,
        journalPosition: position, authorityFloors: n.durable.authorityFloors.put(delivery.resource, delivery.grant.key.revision),
        authorityGrants: n.durable.authorityGrants.put(delivery.resource, delivery.grant),
        receipts: n.durable.receipts.union(Set(receipt)) } })
      E::returnReceipt({ ...base, extensions: { ...base.extensions,
        consumptions: base.extensions.consumptions.union(Set(evidence)) } }, delivery, receipt, deliveries)
    }

  pure def ownAdmission(w: World, node: NodeName): Option[ProcessAdmission] = {
    val host = w.system.get(node)
    val local = w.extensions.metaLog.get(host.volatile.appliedMetaIndex)
    if (local.admissions.keys().contains(node)) Some(local.admissions.get(node))
    else match host.volatile.joinOffer { | Some(offer) => Some(offer.admission) | None => None }
  }

  // Engine WaitReady means local startup/replay, not voter membership or a
  // quorum certificate. The accepted immutable join response supplies identity.
  pure def announcePreparedEnabled(w: World, node: NodeName, seed: NodeName, limit: int): bool = {
    val host = w.system.get(node)
    match ownAdmission(w, node) {
      | Some(a) => M::running(host) and a.owner == { node: node, incarnation: host.volatile.incarnation } and
          not(a.ready) and M::liveIdentity(w.system, a.replica) and w.extensions.nextDelivery <= limit and node != seed
      | None => false
    }
  }

  pure def announcePreparedNext(w: World, node: NodeName, seed: NodeName): World = {
    val a = ownAdmission(w, node).unwrap()
    apply_custom_effect(w, Queue({ delivery: w.extensions.nextDelivery, source: a.owner, destination: seed,
      channel: MetaChannel, payload: JoinPrepared(a) }))
  }

  pure def readyMessage(w: World, msg: Envelope): bool =
    w.messages.get(msg.destination).contains(msg) and match msg.payload {
      | JoinPrepared(a) => a.owner == msg.source
      | _ => false
    }
}
