module execution {
  import admission as Admission from "../metaengine/admission"
  import message_effects.* from "../runtime/messages"
  import types.* from "../types"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import world_types.* from "../runtime/world"
  import grants as G from "../metaengine/grants"
  import membership as M from "../consensus/membership"

  pure def hostAvailable(node: LocalNode): bool = M::running(node) or node.volatile.phase == Fenced

  pure def floorAt(w: World, delivery: EffectDelivery): int = match delivery.resource {
    | GroupResource(group) => w.extensions.consensus.get(group).authorityFloor
    | HostResource(_) => w.system.get(delivery.receiver.node).durable.authorityFloors.getOrElse(delivery.resource, 0)
  }

  pure def receiptsAt(w: World, delivery: EffectDelivery): Set[Receipt] = match delivery.resource {
    | GroupResource(group) => w.extensions.consensus.get(group).authorityReceipts
    | HostResource(_) => w.system.get(delivery.receiver.node).durable.receipts
  }
  pure def fencesAt(w: World, delivery: EffectDelivery): Set[FenceEvidence] = match delivery.resource {
    | GroupResource(group) => w.extensions.consensus.get(group).authorityFences
    | HostResource(_) => w.system.get(delivery.receiver.node).durable.fences
  }
  pure def consumed(w: World, delivery: EffectDelivery): bool =
    receiptsAt(w, delivery).exists(r => r.grant == delivery.grant.key and r.resource == delivery.resource and
      (match delivery.resource { | HostResource(_) => r.target == Some(delivery.replica) | GroupResource(_) => true })) or
      (delivery.grant.purpose == Fence and fencesAt(w, delivery).exists(f => f.grant == delivery.grant.key and f.resource == delivery.resource))

  pure def receiverExact(w: World, delivery: EffectDelivery): bool = {
    val receiver = w.system.get(delivery.receiver.node)
    and {
      (match delivery.resource { | HostResource(_) => hostAvailable(receiver) | GroupResource(_) => M::running(receiver) }), delivery.receiver.incarnation == receiver.volatile.incarnation,
      delivery.replica.node == delivery.receiver.node,
      receiver.durable.pin == Some({ cluster: delivery.replica.cluster, node: delivery.replica.node,
        member: delivery.replica.member }),
      delivery.grant.targets.contains(delivery.replica),
      G::resourceMatches(delivery.resource, delivery.replica),
      match delivery.resource {
        | HostResource(_) => true
        | GroupResource(_) => M::readableIdentity(w.system, delivery.replica)
      },
    }
  }

  pure def courierMatches(meta: MetadataState, delivery: EffectDelivery): bool =
    Admission::courier(meta, delivery.sender.node) == Some({ process: delivery.sender, member: delivery.senderMember })

  pure def mayReceive(w: World, delivery: EffectDelivery): bool = match delivery.proofIndex {
    | None => false
    | Some(index) => if (not(w.extensions.metaLog.keys().contains(index))) false else {
        val proof = w.extensions.metaLog.get(index)
        receiverExact(w, delivery) and G::wellFormed(delivery.grant) and courierMatches(proof, delivery) and
          G::catalogued(proof, delivery.grant) and delivery.grant.resources.contains(delivery.resource)
      }
  }

  // Physical floors are checked again after all transport, metadata and native
  // waits. A closed catalogue entry alone is not a physical fence.
  pure def mayConsume(w: World, delivery: EffectDelivery): bool =
    mayReceive(w, delivery) and
      (consumed(w, delivery) or floorAt(w, delivery) <= delivery.grant.key.revision)

  pure def executable(delivery: EffectDelivery): bool =
    delivery.grant.purpose == Fence or match delivery.grant.effect {
      | Some(e) => e.kind == TransferLeadership and delivery.resource == GroupResource(delivery.grant.key.group)
      | None => false
    }

  // Dispatch only mechanisms with a wired receiver path. Native membership
  // uses its phased install/precheck/propose/commit path, not consume().
  pure def dispatchable(delivery: EffectDelivery): bool = executable(delivery) or
    match delivery.grant.effect { | Some(e) => delivery.grant.planStep != None and (
      (M::nativeKind(e.kind) and delivery.resource == GroupResource(delivery.grant.key.group)) or
      ((e.kind == PrepareReplica or e.kind == CreateFoundingGroup or e.kind == RetireReplica or e.kind == DeleteGroupState or (e.kind == StopReplica and delivery.grant.key.group != "_meta")) and match delivery.resource {
        | HostResource(_) => true | _ => false }) or
      (e.kind == CreateFoundingGroup and delivery.resource == GroupResource(delivery.grant.key.group))) | None => false }

  pure def receipt(delivery: EffectDelivery, position: int): Receipt = {
    grant: delivery.grant.key, effect: delivery.grant.effect, admission: delivery.grant.admission,
    target: match delivery.grant.effect { | Some(e) => match e.target {
      | ReplicaTarget(r) => Some(r) | GroupTarget(_) => None } | None => Some(delivery.replica) },
    resource: delivery.resource, outcome: TransferStarted, planStep: delivery.grant.planStep,
    durablePosition: position, durableTerm: 1,
  }

  pure def fence(delivery: EffectDelivery, position: int): FenceEvidence = {
    grant: delivery.grant.key, resource: delivery.resource,
    receiver: { cluster: delivery.replica.cluster, node: delivery.replica.node, member: delivery.replica.member },
    replica: match delivery.resource { | GroupResource(_) => Some(delivery.replica) | HostResource(_) => None },
    durablePosition: position,
    // Terms are abstracted to one epoch; log positions preserve every authority
    // ordering boundary. Election messages and concrete terms are out of scope.
    durableTerm: match delivery.resource { | GroupResource(_) => 1 | HostResource(_) => 0 },
  }

  pure def forgetWait(w: World, delivery: EffectDelivery): World = {
    ...w, system: w.system.setBy(delivery.receiver.node, n => { ...n,
      volatile: { ...n.volatile, waitingEffects: n.volatile.waitingEffects.exclude(Set(delivery)) } }),
  }

  // Call only after mayConsume/native guards. A deliberate unchecked call is
  // used by negative controls to witness a missing post-wait floor check.
  pure def consume(w: World, delivery: EffectDelivery): World =
    if (consumed(w, delivery)) forgetWait(w, delivery) else {
      val receiver = w.system.get(delivery.receiver.node)
      val evidence: ConsumptionEvidence = { delivery: delivery, pin: receiver.durable.pin,
        incarnation: receiver.volatile.incarnation,
        owner: if (receiver.volatile.owners.keys().contains(delivery.grant.key.group))
          Some(receiver.volatile.owners.get(delivery.grant.key.group).key) else None }
      val waiting = forgetWait(w, delivery)
      val base = { ...waiting, extensions: { ...waiting.extensions,
        consumptions: waiting.extensions.consumptions.union(Set(evidence)) } }
      match delivery.resource {
        | HostResource(_) => {
          val node = base.system.get(delivery.receiver.node)
          val prior = node.durable.pendingFences.getOrElse(delivery.resource,
            { delivery: delivery, position: node.durable.journalPosition + 1 })
          val position = if (prior.delivery.grant.key == delivery.grant.key) prior.position else node.durable.journalPosition + 1
          { ...base, system: base.system.put(delivery.receiver.node, { ...node,
            volatile: { ...node.volatile, fenceWorkers: node.volatile.fenceWorkers.union(Set(delivery.resource)) },
            durable: { ...node.durable,
              authorityFloors: node.durable.authorityFloors.put(delivery.resource, delivery.grant.key.revision),
              authorityGrants: node.durable.authorityGrants.put(delivery.resource, delivery.grant),
              journalPosition: max(position, node.durable.journalPosition),
              pendingFences: node.durable.pendingFences.put(delivery.resource, { delivery: delivery, position: position }) } }) }
        }
        | GroupResource(group) => {
          val c = base.extensions.consensus.get(group)
          val position = c.committedPosition + 1
          val voters = M::certificate(c, base.system, M::linksFor(base.extensions.network, group), delivery.replica)
          val updated = { ...M::committed(c, voters, position), authorityFloor: delivery.grant.key.revision,
            authorityReceipts: if (delivery.grant.purpose == Fence) c.authorityReceipts
              else c.authorityReceipts.union(Set(receipt(delivery, position))),
            authorityFences: if (delivery.grant.purpose == Fence) c.authorityFences.union(Set(fence(delivery, position)))
              else c.authorityFences }
          { ...base, system: M::persistCertificate(base.system, voters, position),
            extensions: { ...base.extensions, consensus: base.extensions.consensus.put(group, updated),
              metaLog: if (group == "_meta") base.extensions.metaLog.put(position, base.extensions.metadata)
                else base.extensions.metaLog } }
        }
      }
    }

  // Returning a durable receipt is distinct from later metadata reporting.
  // A lost result does not undo the effect; a cached result needs no new quorum.
  pure def returnReceipt(w: World, delivery: EffectDelivery, receipt: Receipt, limit: int): World = {
    val receiver = w.system.get(delivery.receiver.node)
    val applied = match delivery.resource {
      | HostResource(_) => receiver.durable.receipts.contains(receipt)
      | GroupResource(_) => match receiver.durable.replicaEvidence.getOrElse(delivery.replica, MissingStore) {
          | ValidStore(p) => p.commit >= receipt.durablePosition | _ => false }
    }
    if (w.extensions.nextDelivery > limit or not(hostAvailable(receiver)) or
        receiver.volatile.incarnation != delivery.receiver.incarnation or not(applied)) w else
      apply_custom_effect(w, Queue({ delivery: w.extensions.nextDelivery, source: delivery.receiver,
        destination: delivery.sender.node, channel: if (delivery.grant.key.group == "_meta") MetaChannel else LoadChannel,
        payload: EffectResult({ delivery: delivery, receipt: receipt }) }))
  }

  pure def physicalEvidenceValid(w: World): bool = {
    val receipts = w.system.values().map(n => n.durable.receipts).flatten().union(
      w.extensions.consensus.values().map(c => c.authorityReceipts).flatten())
    val fences = w.system.values().map(n => n.durable.fences).flatten().union(
      w.extensions.consensus.values().map(c => c.authorityFences).flatten())
    and {
      w.extensions.consumptions.forall(c => {
        val d = c.delivery
        and { c.incarnation == d.receiver.incarnation,
          c.pin == Some({ cluster: d.replica.cluster, node: d.replica.node, member: d.replica.member }),
          d.receiver.node == d.replica.node, d.grant.targets.contains(d.replica),
          match d.proofIndex { | Some(index) => w.extensions.metaLog.keys().contains(index) and
              courierMatches(w.extensions.metaLog.get(index), d) and
              G::catalogued(w.extensions.metaLog.get(index), d.grant) | None => false },
          match d.resource { | GroupResource(g) => g == "_meta" or c.owner == Some(d.replica)
            | HostResource(h) => h.node == d.replica.node },
        }
      }),
      w.extensions.metadata.effectReceipts.subseteq(receipts),
      w.extensions.metadata.fenceEvidence.forall(recorded => fences.exists(actual => G::sameFenceBoundary(recorded, actual))),
      receipts.forall(r => w.extensions.metadata.grants.keys().contains(r.grant) and
        G::receiptIdentifiesGrant(w.extensions.metadata.grants.get(r.grant), r)),
      fences.forall(f => w.extensions.metadata.grants.keys().contains(f.grant) and
        G::fenceIdentifiesGrant(w.extensions.metadata.grants.get(f.grant), f)),
      receipts.forall(r => fences.forall(f => G::receiptFitsFence(r, f))),
      fences.forall(a => fences.forall(b => G::orderedFences(a, b))),
      receipts.forall(a => receipts.forall(b => not(G::sameReceiptSlot(a, b)) or a == b)),
    }
  }
}
