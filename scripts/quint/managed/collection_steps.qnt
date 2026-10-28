module managed_collection_steps {
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import frames as F from "../runtime/frames"
  import message_effects.* from "../runtime/messages"
  import execution as E from "./execution"
  import grants as G from "../metaengine/grants"
  import membership as M from "../consensus/membership"

  pure def collectionReceipt(row: CollectionState): CollectionReceipt = {
    identity: row.identity, grant: row.grant, token: row.token, sequence: row.sequence,
  }

  // Fenced participation still permits authenticated maintenance reporting.
  // Discover an endpoint over the network; this observation grants no effect.
  pure def observeLeaderEnabled(w: World, node: NodeName): bool = {
    val host = w.system.get(node)
    match w.extensions.consensus.get("_meta").leaderAuthority {
      | Some(leader) => E::hostAvailable(host) and M::readableIdentity(w.system, leader) and
          M::communicates(w.extensions.network.metaLinks, node, leader.node) and
          (not(host.volatile.observations.keys().contains("_meta")) or
            host.volatile.observations.get("_meta").observedLeader != Some(leader))
      | None => false
    }
  }

  pure def observeLeaderNext(w: World, node: NodeName): World = {
    val native = w.extensions.consensus.get("_meta")
    val leader = native.leaderAuthority.unwrap()
    F::withProcess(w, node, p => { ...p, observations: p.observations.put("_meta", {
      group: "_meta", replica: leader, incarnation: w.system.get(leader.node).volatile.incarnation,
      observedConfig: native.configRevision, observedLeader: Some(leader), health: Healthy, age: Fresh }) })
  }

  pure def localRetirement(host: LocalNode, key: ReplicaKey, proof: Receipt): bool =
    host.durable.receipts.contains(proof) and proof.target == Some(key) and
    proof.resource == HostResource({ group: key.group, node: key.node }) and proof.outcome != TransferStarted and
    (match proof.effect { | Some(e) => Set(RetireReplica, DeleteGroupState).contains(e.kind) | None => proof.admission != None })

  pure def safeLocal(host: LocalNode, key: ReplicaKey): bool = key.node == host.process_id and
    not(host.durable.replicaLedger.contains(key)) and not(host.durable.replicaEvidence.keys().contains(key)) and
    (not(host.volatile.owners.keys().contains(key.group)) or host.volatile.owners.get(key.group).key != key or
      host.volatile.owners.get(key.group).phase == OwnerStopped)

  pure def retentionEnabled(w: World, node: NodeName, key: ReplicaKey, eligible: bool): bool = {
    val host = w.system.get(node)
    E::hostAvailable(host) and host.durable.retiredStores.keys().contains(key) and
      not(host.durable.collectionMarkers.keys().contains(key)) and
      host.volatile.retentionEligible.contains(key) != eligible
  }

  pure def retentionNext(w: World, node: NodeName, key: ReplicaKey, eligible: bool): World = F::withProcess(w, node, p => { ...p,
    retentionEligible: if (eligible) p.retentionEligible.union(Set(key)) else p.retentionEligible.exclude(Set(key)) })

  pure def prepareEnabled(w: World, node: NodeName, key: ReplicaKey, proof: Receipt, limit: int): bool = {
    val host = w.system.get(node)
    E::hostAvailable(host) and safeLocal(host, key) and localRetirement(host, key, proof) and
      host.volatile.retentionEligible.contains(key) and not(host.durable.collectionMarkers.keys().contains(key)) and
      host.durable.retiredStores.keys().contains(key) and host.durable.retiredStores.get(key).markerPresent and
      host.durable.journalPosition < limit
  }

  pure def prepareNext(w: World, node: NodeName, key: ReplicaKey, proof: Receipt): World = F::withNode(w, node, host => { ...host,
    durable: { ...host.durable, journalPosition: host.durable.journalPosition + 1,
      collectionMarkers: host.durable.collectionMarkers.put(key, { identity: key, grant: proof.grant,
        token: host.durable.retiredStores.get(key).token, sequence: host.durable.journalPosition + 1,
        retirement: proof, phase: Prepared }) } })

  pure def rowValid(host: LocalNode, key: ReplicaKey): bool =
    host.durable.collectionMarkers.keys().contains(key) and host.durable.retiredStores.keys().contains(key) and {
      val row = host.durable.collectionMarkers.get(key)
      row.identity == key and row.grant == row.retirement.grant and row.sequence > 0 and
        row.token > 0 and row.token == host.durable.retiredStores.get(key).token and
        localRetirement(host, key, row.retirement) and safeLocal(host, key)
    }

  pure def collectEnabled(w: World, node: NodeName, key: ReplicaKey): bool = {
    val host = w.system.get(node)
    E::hostAvailable(host) and rowValid(host, key) and host.durable.collectionMarkers.get(key).phase == Prepared and
      host.durable.retiredStores.get(key).dataPresent and host.durable.retiredStores.get(key).markerPresent
  }

  // The physical store's token survives deletion. The journal is still prepared
  // if the process crashes before completeNext, so retries cannot infer success
  // merely from absent files or acquire a successor's resources.
  pure def collectNext(w: World, node: NodeName, key: ReplicaKey): World = F::withNode(w, node, host => { ...host,
    durable: { ...host.durable, retiredStores: host.durable.retiredStores.setBy(key, store => { ...store, dataPresent: false }) } })

  pure def completeEnabled(w: World, node: NodeName, key: ReplicaKey, limit: int): bool = {
    val host = w.system.get(node)
    E::hostAvailable(host) and rowValid(host, key) and host.durable.collectionMarkers.get(key).phase == Prepared and
      not(host.durable.retiredStores.get(key).dataPresent) and host.durable.retiredStores.get(key).markerPresent and
      host.durable.journalPosition < limit
  }

  pure def completeNext(w: World, node: NodeName, key: ReplicaKey): World = F::withNode(w, node, host => { ...host,
    durable: { ...host.durable, journalPosition: host.durable.journalPosition + 1,
      collectionMarkers: host.durable.collectionMarkers.setBy(key, row => { ...row, phase: CompletionDurable }) } })

  pure def reportEnabled(w: World, node: NodeName, key: ReplicaKey, limit: int): bool = {
    val host = w.system.get(node)
    E::hostAvailable(host) and rowValid(host, key) and host.durable.collectionMarkers.get(key).phase == CompletionDurable and
      host.durable.pin != None and host.volatile.observations.keys().contains("_meta") and
      host.volatile.observations.get("_meta").observedLeader != None and w.extensions.nextDelivery <= limit and
      not(host.volatile.collectionAcks.contains(collectionReceipt(host.durable.collectionMarkers.get(key))))
  }

  pure def reportNext(w: World, node: NodeName, key: ReplicaKey): World = {
    val host = w.system.get(node)
    val caller = { node: node, incarnation: host.volatile.incarnation }
    val proposal: ProposalKey = { request: { caller: caller, sequence: -w.extensions.nextDelivery }, sequence: 0 }
    val receipt = collectionReceipt(host.durable.collectionMarkers.get(key))
    val command = RecordCollection({ receipt: receipt, courier: caller, member: host.durable.pin.unwrap().member })
    val base = F::withProcess(w, node, p => { ...p, collectionRequests: p.collectionRequests.put(proposal, receipt) })
    apply_custom_effect(base, Queue({ delivery: w.extensions.nextDelivery, source: caller,
      destination: host.volatile.observations.get("_meta").observedLeader.unwrap().node, channel: MetaChannel,
      payload: MetaProposal({ key: proposal, coordinator: caller, forWork: false, command: ControlProposal(command) }) }))
  }

  pure def ackEnabled(w: World, msg: Envelope): bool = match msg.payload {
    | MetaAck(ack) => w.messages.get(msg.destination).contains(msg) and E::hostAvailable(w.system.get(msg.destination)) and
        ack.coordinator == { node: msg.destination, incarnation: w.system.get(msg.destination).volatile.incarnation } and
        w.system.get(msg.destination).volatile.collectionRequests.keys().contains(ack.key)
    | _ => false
  }

  pure def ackNext(w: World, msg: Envelope): World = match msg.payload {
    | MetaAck(ack) => apply_custom_effect(F::withProcess(w, msg.destination, p => { ...p,
        collectionAcks: if (ack.index > 0 and ack.outcome.errors == Set())
          p.collectionAcks.union(Set(p.collectionRequests.get(ack.key))) else p.collectionAcks,
        collectionRequests: p.collectionRequests.mapRemove(ack.key) }), Consume(msg))
    | _ => w
  }

  pure def releasable(w: World, node: NodeName, key: ReplicaKey): bool = {
    val host = w.system.get(node)
    E::hostAvailable(host) and rowValid(host, key) and host.durable.collectionMarkers.get(key).phase == CompletionDurable and
      host.volatile.collectionAcks.contains(collectionReceipt(host.durable.collectionMarkers.get(key))) and
      not(host.durable.retiredStores.get(key).dataPresent) and
      not(host.durable.pendingFences.keys().exists(r => r == HostResource({ group: key.group, node: node }) or
        r == HostResource({ group: "_meta", node: node }))) and
      host.durable.authorityGrants.values().filter(g => g.key.group == key.group or g.key.group == "_meta")
        .forall(g => g.purpose != Readmission or not(g.targets.exists(r =>
          match host.durable.pin { | Some(pin) => r.member == pin.member | None => true })))
  }

  pure def releaseEnabled(w: World, node: NodeName, key: ReplicaKey): bool = releasable(w, node, key) and
    w.system.get(node).durable.retiredStores.get(key).markerPresent

  pure def releaseNext(w: World, node: NodeName, key: ReplicaKey): World = F::withNode(w, node, host => { ...host,
    durable: { ...host.durable, retiredStores: host.durable.retiredStores.setBy(key, store => { ...store, markerPresent: false }) } })

  pure def removeEnabled(w: World, node: NodeName, key: ReplicaKey): bool = releasable(w, node, key) and
    not(w.system.get(node).durable.retiredStores.get(key).markerPresent)

  pure def removeNext(w: World, node: NodeName, key: ReplicaKey): World = F::withNode(w, node, host => { ...host,
    durable: { ...host.durable, collectionMarkers: host.durable.collectionMarkers.mapRemove(key) } })
}
