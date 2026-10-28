module managed_node_steps {
  import frames as Frames from "../runtime/frames"
  import execution as E from "./execution"
  import lifecycle as Life from "./lifecycle"
  import membership as M from "../consensus/membership"
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import message_effects.* from "../runtime/messages"

  pure def stopOlderForFenceEnabled(w: World, node: NodeName, resource: ResourceKey): bool = {
    val host = w.system.get(node)
    val pending = host.durable.pendingFences.get(resource)
    val group = pending.delivery.grant.key.group
    val owner = host.volatile.owners.get(group)
    and {
      host.volatile.fenceWorkers.contains(resource),
      owner.authorityRevision < pending.delivery.grant.key.revision,
      not(Set(OwnerReady, OwnerStopping, OwnerStopped).contains(owner.phase))
    }
  }

  pure def stopOlderForFenceNext(w: World, node: NodeName, resource: ResourceKey): World = {
    val host = w.system.get(node)
    val pending = host.durable.pendingFences.get(resource)
    val group = pending.delivery.grant.key.group
    val owner = host.volatile.owners.get(group)
    (Frames::withProcess(w, node, updatedProcess => { ...updatedProcess,
        owners: updatedProcess.owners.put(group, Life::requestStop(owner)) }))
  }

  pure def completeHostFenceEnabled(w: World, node: NodeName, resource: ResourceKey): bool = {
    val host = w.system.get(node)
    val pending = host.durable.pendingFences.get(resource)
    val group = pending.delivery.grant.key.group
    val ownerJoined = if (not(host.volatile.owners.keys().contains(group))) true else {
      val owner = host.volatile.owners.get(group)
      owner.phase == OwnerReady or Life::released(owner)
    }
    and {
      E::hostAvailable(host),
      host.volatile.fenceWorkers.contains(resource),
      ownerJoined,
      E::receiverExact(w, pending.delivery),
      host.durable.authorityFloors.get(resource) == pending.delivery.grant.key.revision
    }
  }

  pure def completeHostFenceNext(w: World, node: NodeName, resource: ResourceKey): World = {
    val host = w.system.get(node)
    val pending = host.durable.pendingFences.get(resource)
    val proof = E::fence(pending.delivery, pending.position)
    (Frames::withNode(w, node, _ => { ...host,
        volatile: { ...host.volatile, fenceWorkers: host.volatile.fenceWorkers.exclude(Set(resource)) },
        durable: { ...host.durable, pendingFences: host.durable.pendingFences.mapRemove(resource),
          fences: host.durable.fences.union(Set(proof)) } }))
  }

  pure def requestNodeStopEnabled(w: World, node: NodeName, MAX_DELIVERIES: int): bool = {
    val host = w.system.get(node)
    and {
      M::running(host) or host.volatile.phase == Fenced
    }
  }

  pure def requestNodeStopNext(w: World, node: NodeName, MAX_DELIVERIES: int): World = {
    val host = w.system.get(node)
    val caller = { node: node, incarnation: host.volatile.incarnation }
    val stopping = Frames::withProcess(w, node, updatedProcess => { ...updatedProcess,
      phase: Stopping, ownLease: Revoked, renewal: None,
      owners: updatedProcess.owners.transformValues(owner => if (owner.phase == OwnerStopped) owner else Life::requestStop(owner)) })
    val leader = if (host.volatile.observations.keys().contains("_meta")) host.volatile.observations.get("_meta").observedLeader else None
    val nextWorld = match host.volatile.session {
      | Some(key) => match leader {
          | Some(destination) => if (w.extensions.nextDelivery <= MAX_DELIVERIES) apply_custom_effect(stopping,
              Queue({ delivery: w.extensions.nextDelivery, source: caller, destination: destination.node, channel: MetaChannel,
                payload: MetaProposal({ key: { request: { caller: caller, sequence: -w.extensions.nextDelivery }, sequence: 0 },
                  coordinator: caller, forWork: false, command: ControlProposal(SessionControl(RevokeInternalSession(key))) }) }))
            else stopping
          | None => stopping }
      | None => stopping
    }
    (nextWorld)
  }

  pure def joinAuthorityWorkersEnabled(w: World, node: NodeName): bool = {
    val host = w.system.get(node)
    and {
      host.volatile.phase == Stopping,
      host.volatile.retirementWork.values().forall(work => work.phase != RetireWaiting),
      host.volatile.fenceWorkers.size() > 0 or host.volatile.waitingEffects.size() > 0 or
        host.volatile.retirementWork.keys().size() > 0 or host.volatile.collectionRequests.keys().size() > 0
    }
  }

  pure def joinAuthorityWorkersNext(w: World, node: NodeName): World = {
    val host = w.system.get(node)
    (Frames::withProcess(w, node, updatedProcess => { ...updatedProcess,
        fenceWorkers: Set(), waitingEffects: Set(), retirementWork: Map(), collectionRequests: Map(), collectionAcks: Set() }))
  }

  pure def timeoutNodeStopEnabled(w: World, node: NodeName): bool = {
    val host = w.system.get(node)
    and {
      host.volatile.phase == Stopping,
      not(host.volatile.stopTimedOut)
    }
  }

  pure def timeoutNodeStopNext(w: World, node: NodeName): World = {
    val host = w.system.get(node)
    (Frames::withProcess(w, node, updatedProcess => { ...updatedProcess, stopTimedOut: true }))
  }

  pure def expireOwnerCloseEnabled(w: World, node: NodeName, group: GroupId): bool = {
    val owner = w.system.get(node).volatile.owners.get(group)
    and {
      owner.applicationClose == Closing,
      not(owner.closeExpired)
    }
  }

  pure def expireOwnerCloseNext(w: World, node: NodeName, group: GroupId): World = {
    (Frames::withOwner(w, node, group, slot => { ...slot, closeExpired: true }))
  }

  pure def finishNodeStopEnabled(w: World, node: NodeName): bool = {
    val host = w.system.get(node)
    and {
      host.volatile.phase == Stopping,
      host.volatile.owners.values().forall(owner => owner.phase == OwnerStopped and Life::released(owner)),
      host.volatile.fenceWorkers.size() == 0,
      host.volatile.waitingEffects.size() == 0,
      host.volatile.retirementWork.keys().size() == 0,
      host.volatile.collectionRequests.keys().size() == 0
    }
  }

  pure def finishNodeStopNext(w: World, node: NodeName): World = {
    val host = w.system.get(node)
    (Frames::withProcess(w, node, updatedProcess => { ...updatedProcess,
        phase: Stopped, metaRunning: false, session: None, ownLease: Revoked }))
  }

  pure def beginNodeStartEnabled(w: World, node: NodeName): bool = {
    val host = w.system.get(node)
    and {
      host.volatile.phase == New
    }
  }

  pure def beginNodeStartNext(w: World, node: NodeName): World = {
    val host = w.system.get(node)
    (Frames::withNode(w, node, _ => { ...host,
        volatile: { ...host.volatile, phase: Starting, metaRunning: host.durable.pin != None and host.durable.metaRemoved == None } }))
  }

}
