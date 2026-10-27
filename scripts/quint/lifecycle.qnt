module lifecycle {
  import types.* from "./types"
  import basicSpells.* from "./vendor/spells/basicSpells"
  import membership as M from "./membership"

  // managed.reserveStart/acceptStart: a single exact reservation owns factory,
  // replay, engine and application release. Canceling a waiter is not a stop.
  pure def released(owner: OwnerState): bool = and {
    not(owner.factoryOutstanding), not(owner.replayOutstanding),
    owner.engineJoined, not(owner.engineRunning),
    owner.applicationClose == NotAcquired or owner.applicationClose == Closed,
  }
  pure def acceptsWaiter(owner: OwnerState): bool =
    Set(Reserved, Opening, Replaying, OwnerReady).contains(owner.phase)

  pure def reserved(key: ReplicaKey, request: RequestKey, declaration: GroupDecl, revision: int): OwnerState = {
    key: key, phase: Reserved, held: false, reservation: Some(request),
    replayReady: false, startupAuthorized: false, published: false,
    factoryOutstanding: false, engineJoined: true, applicationClose: NotAcquired, engineRunning: false,
    process: request.caller, waiters: Set(request), callbackExpired: false, closeExpired: false,
    replayOutstanding: false, startDeclaration: Some(declaration), authorityRevision: revision, scopeIndex: 0,
  }

  pure def canReserve(node: LocalNode, key: ReplicaKey, request: RequestKey, explicit: bool): bool = and {
    M::running(node), key.group != "_meta", key.node == node.process_id,
    request.caller == { node: node.process_id, incarnation: node.volatile.incarnation },
    node.durable.pin == Some({ cluster: key.cluster, node: key.node, member: key.member }),
    explicit or not(node.volatile.holds.contains(key)), currentAuthority(node, key),
    if (not(node.volatile.owners.keys().contains(key.group))) true else {
      val old = node.volatile.owners.get(key.group)
      (old.key == key and acceptsWaiter(old) and old.process == request.caller) or
        (old.phase == OwnerStopped and released(old))
    },
  }

  pure def reserve(node: LocalNode, key: ReplicaKey, request: RequestKey, explicit: bool,
                   declaration: GroupDecl, revision: int): LocalNode = {
    val prior = node.volatile.owners.getOrElse(key.group, reserved(key, request, declaration, revision))
    val owner = if (prior.key == key and acceptsWaiter(prior))
        { ...prior, waiters: prior.waiters.union(Set(request)), held: if (explicit) false else prior.held }
      else reserved(key, request, declaration, revision)
    { ...node, volatile: { ...node.volatile, owners: node.volatile.owners.put(key.group, owner),
      holds: if (explicit) node.volatile.holds.exclude(Set(key)) else node.volatile.holds } }
  }

  pure def hold(node: LocalNode, key: ReplicaKey): LocalNode = {
    ...node, volatile: { ...node.volatile, holds: node.volatile.holds.union(Set(key)),
      owners: if (node.volatile.owners.keys().contains(key.group) and
        node.volatile.owners.get(key.group).key == key)
        node.volatile.owners.setBy(key.group, owner => { ...requestStop(owner), held: true })
      else node.volatile.owners },
  }

  pure def cancelWaiter(owner: OwnerState, request: RequestKey): OwnerState =
    { ...owner, waiters: owner.waiters.exclude(Set(request)) }
  pure def beginFactory(owner: OwnerState): OwnerState =
    { ...owner, phase: Opening, factoryOutstanding: true }
  pure def expireFactory(owner: OwnerState): OwnerState =
    { ...owner, callbackExpired: true }

  pure def requestStop(owner: OwnerState): OwnerState =
    { ...owner, phase: OwnerStopping, published: false, startupAuthorized: false }

  // A nil factory error transfers application ownership even after deadline or
  // stop. A factory error leaves partial-resource release with the factory.
  pure def factoryReturned(owner: OwnerState, success: bool): OwnerState = {
    val stopping = owner.phase == OwnerStopping or owner.callbackExpired or not(success)
    { ...owner, factoryOutstanding: false,
      applicationClose: if (success) Acquired else NotAcquired,
      phase: if (stopping) OwnerStopping else Replaying,
      replayOutstanding: success and not(stopping),
      engineJoined: stopping, engineRunning: success and not(stopping),
      startupAuthorized: false, published: false }
  }

  pure def replayFinished(owner: OwnerState): OwnerState =
    { ...owner, replayOutstanding: false,
      replayReady: owner.phase != OwnerStopping }

  pure def authorize(owner: OwnerState): OwnerState = { ...owner, startupAuthorized: true }
  pure def publish(owner: OwnerState): OwnerState = { ...owner, phase: OwnerReady, published: true }
  pure def canPublish(owner: OwnerState): bool = and {
    owner.phase == Replaying, owner.replayReady, owner.startupAuthorized,
    not(owner.factoryOutstanding), not(owner.replayOutstanding),
    owner.applicationClose == Acquired, owner.engineRunning, not(owner.engineJoined),
  }

  pure def canJoin(owner: OwnerState): bool = and {
    owner.phase == OwnerStopping, not(owner.factoryOutstanding), not(owner.replayOutstanding),
    not(owner.engineJoined) or owner.engineRunning,
  }
  pure def joined(owner: OwnerState): OwnerState =
    { ...owner, engineJoined: true, engineRunning: false }
  pure def canClose(owner: OwnerState): bool = and {
    owner.phase == OwnerStopping, owner.engineJoined, not(owner.engineRunning),
    not(owner.factoryOutstanding), not(owner.replayOutstanding),
    owner.applicationClose == Acquired or owner.applicationClose == CloseFailed,
  }
  pure def beginClose(owner: OwnerState): OwnerState = { ...owner, applicationClose: Closing, closeExpired: false }
  pure def closeReturned(owner: OwnerState, success: bool): OwnerState =
    { ...owner, applicationClose: if (success) Closed else CloseFailed }
  pure def finishRelease(owner: OwnerState): OwnerState =
    { ...owner, phase: OwnerStopped, published: false, startupAuthorized: false }

  pure def ownerSafety(owner: OwnerState): bool = and {
    if (owner.published) and {
      owner.phase == OwnerReady, owner.replayReady, owner.startupAuthorized,
      owner.applicationClose == Acquired, owner.engineRunning, not(owner.engineJoined),
      not(owner.factoryOutstanding), not(owner.replayOutstanding),
    } else true,
    if (owner.phase == OwnerStopped) released(owner) else true,
    if (owner.applicationClose == Closing or owner.applicationClose == Closed)
      owner.engineJoined and not(owner.engineRunning) and not(owner.factoryOutstanding) and not(owner.replayOutstanding)
    else true,
    owner.process.node == owner.key.node,
  }

  pure def resource(key: ReplicaKey): ResourceKey = HostResource({ group: key.group, node: key.node })
  pure def exactPin(node: LocalNode, key: ReplicaKey): bool =
    node.durable.pin == Some({ cluster: key.cluster, node: key.node, member: key.member })
  pure def assigned(meta: MetadataState, key: ReplicaKey): bool = and {
    meta.target.cluster == key.cluster, meta.target.nodes.keys().contains(key.node),
    meta.target.nodes.get(key.node).member == key.member,
    meta.target.groups.keys().contains(key.group),
    meta.target.groups.get(key.group).replicas.exists(r => r.key == key),
  }
  pure def floorRevision(node: LocalNode, key: ReplicaKey): int =
    node.durable.authorityFloors.getOrElse(resource(key), if (node.durable.seedAuthority.contains(key)) 1 else 0)
  pure def currentAuthority(node: LocalNode, key: ReplicaKey): bool =
    if (node.durable.pendingFences.keys().contains(resource(key))) false
    else if (node.durable.authorityGrants.keys().contains(resource(key))) {
      val grant = node.durable.authorityGrants.get(resource(key))
      grant.targets.contains(key) and grant.key.revision == floorRevision(node, key)
    } else node.durable.seedAuthority.contains(key) and floorRevision(node, key) == 1

  pure def importedFloor(node: LocalNode, meta: MetadataState, key: ReplicaKey): bool = {
    val host = resource(key)
    if (not(currentAuthority(node, key))) false
    else if (meta.authorityFloors.keys().contains(host))
      floorRevision(node, key) == meta.authorityFloors.get(host).revision
    else node.durable.seedAuthority.contains(key) and floorRevision(node, key) == 1
  }
  pure def canReuse(node: LocalNode, key: ReplicaKey): bool =
    node.volatile.owners.keys().contains(key.group) and
      node.volatile.owners.get(key.group).key == key and acceptsWaiter(node.volatile.owners.get(key.group))
  pure def retainedStore(node: LocalNode, key: ReplicaKey): bool =
    node.durable.replicaEvidence.keys().contains(key) and M::validStore(node.durable.replicaEvidence.get(key))

  // Publication rechecks local process/floor ownership under the local gate.
  // The confirmed scope snapshot is an observation; later unrelated metadata
  // advancement does not invalidate it or redirect this exact owner.
  pure def publicationAllowed(node: LocalNode, owner: OwnerState): bool = and {
    canPublish(owner), M::running(node), exactPin(node, owner.key),
    owner.process == { node: node.process_id, incarnation: node.volatile.incarnation },
    not(node.volatile.holds.contains(owner.key)),
    owner.authorityRevision == floorRevision(node, owner.key),
    not(node.durable.pendingFences.keys().contains(resource(owner.key))),
    owner.scopeIndex > 0,
  }
}
