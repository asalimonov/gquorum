module managed_lifetime_steps {
  import founding as Founding from "../consensus/founding"
  import frames as Frames from "../runtime/frames"
  import lifecycle as Life from "./lifecycle"
  import membership as M from "../consensus/membership"
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"

  pure def requestStartEnabled(w: World, node: NodeName, replica: ReplicaKey, explicit: bool, MAX_REQUESTS: int): bool = {
    val owner = w.system.get(node)
    val key = { caller: { node: node, incarnation: owner.volatile.incarnation },
      sequence: owner.volatile.starts.keys().size() + 1 }
    and {
      M::running(owner),
      replica.node == node,
      replica.group != "_meta",
      key.sequence <= MAX_REQUESTS
    }
  }

  pure def requestStartNext(w: World, node: NodeName, replica: ReplicaKey, explicit: bool, MAX_REQUESTS: int): World = {
    val owner = w.system.get(node)
    val key = { caller: { node: node, incarnation: owner.volatile.incarnation },
      sequence: owner.volatile.starts.keys().size() + 1 }
    (Frames::withProcess(w, node, updatedProcess => { ...updatedProcess,
        starts: updatedProcess.starts.put(key, { key: replica, request: key, explicit: explicit,
          floorIndex: None, phase: StartPreparing, error: None }) }))
  }

  pure def readStartScopeEnabled(w: World, request: RequestKey): bool = {
    val node = w.system.get(request.caller.node)
    val work = node.volatile.starts.get(request)
    and {
      request.caller.incarnation == node.volatile.incarnation,
      work.phase == StartPreparing,
      work.floorIndex == None,
      (Life::canReuse(node, work.key) or M::freshMetaAvailable(w.extensions, w.system, request.caller.node))
    }
  }

  pure def readStartScopeNext(w: World, request: RequestKey): World = {
    val node = w.system.get(request.caller.node)
    val work = node.volatile.starts.get(request)
    (Frames::withNode(w, request.caller.node, _ => { ...node,
        volatile: { ...node.volatile, starts: node.volatile.starts.put(request,
          { ...work, floorIndex: Some(if (Life::canReuse(node, work.key)) node.volatile.appliedMetaIndex
            else w.extensions.consensus.get("_meta").committedPosition) }) } }))
  }

  pure def reserveOwnerEnabled(w: World, request: RequestKey): bool = {
    val node = w.system.get(request.caller.node)
    val work = node.volatile.starts.get(request)
    match work.floorIndex {
      | Some(index) => {
          val floors = w.extensions.metaLog.get(index)
          val meta = w.extensions.metaLog.get(node.volatile.appliedMetaIndex)
          val staleFloor = floors.authorityFloors.keys().contains(Life::resource(work.key)) and
            Life::floorRevision(node, work.key) > floors.authorityFloors.get(Life::resource(work.key)).revision
          val valid = not(staleFloor) and Life::assigned(meta, work.key) and Life::retainedStore(node, work.key) and
            Life::canReserve(node, work.key, request, work.explicit)
          val imported = Life::canReuse(node, work.key) or Life::importedFloor(node, floors, work.key)
          and {
            M::running(node),
            request.caller.incarnation == node.volatile.incarnation,
            work.phase == StartPreparing,
            imported or not(valid)
          }
        }
      | None => and {
        false
      }
    }
  }

  pure def reserveOwnerNext(w: World, request: RequestKey): World = {
    val node = w.system.get(request.caller.node)
    val work = node.volatile.starts.get(request)
    match work.floorIndex {
      | Some(index) => {
          val floors = w.extensions.metaLog.get(index)
          val meta = w.extensions.metaLog.get(node.volatile.appliedMetaIndex)
          val staleFloor = floors.authorityFloors.keys().contains(Life::resource(work.key)) and
            Life::floorRevision(node, work.key) > floors.authorityFloors.get(Life::resource(work.key)).revision
          val valid = not(staleFloor) and Life::assigned(meta, work.key) and Life::retainedStore(node, work.key) and
            Life::canReserve(node, work.key, request, work.explicit)
          val imported = Life::canReuse(node, work.key) or Life::importedFloor(node, floors, work.key)
          val accepted = if (valid and imported) Life::reserve(node, work.key, request, work.explicit,
            meta.target.groups.get(work.key.group), Life::floorRevision(node, work.key)) else node
          (Frames::withNode(w, request.caller.node, _ => { ...accepted,
              volatile: { ...accepted.volatile, starts: accepted.volatile.starts.put(request,
                { ...work, phase: if (valid) StartAccepted else StartRefused,
                  error: if (valid) None else Some(Unavailable) }) } }))
        }
      | None => (w)
    }
  }

  pure def beginOwnerFactoryEnabled(w: World, node: NodeName, group: GroupId): bool = {
    val owner = w.system.get(node).volatile.owners.get(group)
    and {
      M::running(w.system.get(node)),
      owner.phase == Reserved
    }
  }

  pure def beginOwnerFactoryNext(w: World, node: NodeName, group: GroupId): World = {
    (Frames::withOwner(w, node, group, slot => Life::beginFactory(slot)))
  }

  pure def returnOwnerFactoryEnabled(w: World, node: NodeName, group: GroupId, success: bool): bool = {
    val owner = w.system.get(node).volatile.owners.get(group)
    and {
      w.system.get(node).volatile.phase != Stopped,
      owner.factoryOutstanding
    }
  }

  pure def returnOwnerFactoryNext(w: World, node: NodeName, group: GroupId, success: bool): World = {
    Founding::engineStarted(Frames::withOwner(w, node, group, slot => Life::factoryReturned(slot, success)), node, group)
  }

  pure def expireOwnerFactoryEnabled(w: World, node: NodeName, group: GroupId): bool = {
    val owner = w.system.get(node).volatile.owners.get(group)
    and {
      owner.factoryOutstanding,
      not(owner.callbackExpired)
    }
  }

  pure def expireOwnerFactoryNext(w: World, node: NodeName, group: GroupId): World = {
    (Frames::withOwner(w, node, group, slot => Life::expireFactory(slot)))
  }

  pure def finishOwnerReplayEnabled(w: World, node: NodeName, group: GroupId): bool = {
    val owner = w.system.get(node).volatile.owners.get(group)
    and {
      owner.replayOutstanding
    }
  }

  pure def finishOwnerReplayNext(w: World, node: NodeName, group: GroupId): World = {
    (Frames::withOwner(w, node, group, slot => Life::replayFinished(slot)))
  }

  pure def authorizeOwnerEnabled(w: World, node: NodeName, group: GroupId): bool = {
    val owner = w.system.get(node).volatile.owners.get(group)
    and {
      owner.phase == Replaying,
      owner.replayReady,
      not(owner.startupAuthorized),
      M::running(w.system.get(node))
    }
  }

  pure def authorizeOwnerNext(w: World, node: NodeName, group: GroupId): World = {
    val owner = w.system.get(node).volatile.owners.get(group)
    val index = w.system.get(node).volatile.appliedMetaIndex
    val meta = w.extensions.metaLog.get(index)
    val allowed = if (not(Life::assigned(meta, owner.key))) false else true and owner.startDeclaration == Some(meta.target.groups.get(group)) and
      Life::exactPin(w.system.get(node), owner.key) and Life::importedFloor(w.system.get(node), meta, owner.key) and
      owner.authorityRevision == Life::floorRevision(w.system.get(node), owner.key)
    (Frames::withOwner(w, node, group, slot => if (allowed) { ...Life::authorize(slot), scopeIndex: index }
          else Life::requestStop(slot)))
  }

  pure def publishOwnerEnabled(w: World, node: NodeName, group: GroupId): bool = {
    val owner = w.system.get(node).volatile.owners.get(group)
    and {
      Life::publicationAllowed(w.system.get(node), owner)
    }
  }

  pure def publishOwnerNext(w: World, node: NodeName, group: GroupId): World = {
    (Frames::withOwner(w, node, group, slot => Life::publish(slot)))
  }

  pure def replyStartEnabled(w: World, request: RequestKey): bool = {
    val node = w.system.get(request.caller.node)
    val work = node.volatile.starts.get(request)
    val owner = node.volatile.owners.get(work.key.group)
    and {
      work.phase == StartAccepted,
      owner.key == work.key,
      owner.phase == OwnerReady or owner.phase == OwnerStopped
    }
  }

  pure def replyStartNext(w: World, request: RequestKey): World = {
    val node = w.system.get(request.caller.node)
    val work = node.volatile.starts.get(request)
    val owner = node.volatile.owners.get(work.key.group)
    (Frames::withProcess(w, request.caller.node, updatedProcess => { ...updatedProcess, starts: updatedProcess.starts.put(request,
          { ...work, phase: if (owner.phase == OwnerReady) StartSucceeded else StartRefused,
            error: if (owner.phase == OwnerReady) None else Some(Unavailable) }) }))
  }

  pure def cancelStartEnabled(w: World, request: RequestKey): bool = {
    val node = w.system.get(request.caller.node)
    val work = node.volatile.starts.get(request)
    and {
      work.phase == StartPreparing or work.phase == StartAccepted
    }
  }

  pure def cancelStartNext(w: World, request: RequestKey): World = {
    val node = w.system.get(request.caller.node)
    val work = node.volatile.starts.get(request)
    (Frames::withProcess(w, request.caller.node, updatedProcess => { ...updatedProcess,
          starts: updatedProcess.starts.put(request, { ...work, phase: StartRefused, error: Some(WaitCancelled) }),
          owners: if (work.phase == StartAccepted) updatedProcess.owners.setBy(work.key.group,
            owner => Life::cancelWaiter(owner, request)) else updatedProcess.owners }))
  }

  pure def stopOwnerEnabled(w: World, node: NodeName, replica: ReplicaKey, explicit: bool): bool = {
    val host = w.system.get(node)
    and {
      replica.node == node,
      replica.group != "_meta",
      host.volatile.phase != Stopped,
      if (explicit) not(host.volatile.holds.contains(replica)) else
        host.volatile.owners.keys().contains(replica.group) and
          host.volatile.owners.get(replica.group).key == replica and
          not(Set(OwnerStopping, OwnerStopped).contains(host.volatile.owners.get(replica.group).phase))
    }
  }

  pure def stopOwnerNext(w: World, node: NodeName, replica: ReplicaKey, explicit: bool): World = {
    val host = w.system.get(node)
    (Frames::withNode(w, node, _ => if (explicit) Life::hold(host, replica) else { ...host,
        volatile: { ...host.volatile, owners: host.volatile.owners.setBy(replica.group, owner => Life::requestStop(owner)) } }))
  }

  pure def joinOwnerEnabled(w: World, node: NodeName, group: GroupId): bool = {
    val owner = w.system.get(node).volatile.owners.get(group)
    and {
      Life::canJoin(owner)
    }
  }

  pure def joinOwnerNext(w: World, node: NodeName, group: GroupId): World = {
    (Frames::withOwner(w, node, group, slot => Life::joined(slot)))
  }

  pure def beginOwnerCloseEnabled(w: World, node: NodeName, group: GroupId): bool = {
    val owner = w.system.get(node).volatile.owners.get(group)
    and {
      Life::canClose(owner)
    }
  }

  pure def beginOwnerCloseNext(w: World, node: NodeName, group: GroupId): World = {
    (Frames::withOwner(w, node, group, slot => Life::beginClose(slot)))
  }

  pure def returnOwnerCloseEnabled(w: World, node: NodeName, group: GroupId, success: bool): bool = {
    val owner = w.system.get(node).volatile.owners.get(group)
    and {
      owner.applicationClose == Closing
    }
  }

  pure def returnOwnerCloseNext(w: World, node: NodeName, group: GroupId, success: bool): World = {
    (Frames::withOwner(w, node, group, slot => Life::closeReturned(slot, success)))
  }

  pure def releaseOwnerEnabled(w: World, node: NodeName, group: GroupId): bool = {
    val owner = w.system.get(node).volatile.owners.get(group)
    and {
      owner.phase == OwnerStopping,
      Life::released(owner)
    }
  }

  pure def releaseOwnerNext(w: World, node: NodeName, group: GroupId): World = {
    val owner = w.system.get(node).volatile.owners.get(group)
    Frames::withProcess(w, node, updatedProcess => { ...updatedProcess, owners: updatedProcess.owners.put(group, Life::finishRelease(owner)),
        starts: updatedProcess.starts.transformValues(work => if (work.key == owner.key and work.phase == StartAccepted)
          { ...work, phase: StartRefused, error: Some(Unavailable) } else work) })
  }

}
