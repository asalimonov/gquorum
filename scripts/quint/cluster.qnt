module cluster {
  import initial as I from "./initial"
  import membership as M from "./membership"
  import types.* from "./types"
  import handlers.* from "./handlers"
  import basicSpells.* from "./vendor/spells/basicSpells"
  import choreo as C from "./vendor/choreo"

  const NODE_POOL: Set[NodeName]
  const MAX_REQUESTS: int
  const FOUNDERS: NodeName -> int
  const LOAD_IDS: Set[GroupId]
  const INITIAL_LEADER: NodeName
  const MAX_POSITION: int
  const MAX_INCARNATION: int
  const MAX_DELIVERIES: int
  var world: World
  pure val modelBounds = Map("nodeCount" -> NODE_POOL.size(), "maxRequests" -> MAX_REQUESTS,
    "founderCount" -> FOUNDERS.keys().size(), "loadCount" -> LOAD_IDS.size(),
    "maxPosition" -> MAX_POSITION, "maxIncarnation" -> MAX_INCARNATION,
    "maxDeliveries" -> MAX_DELIVERIES)

  action init = world' = I::worldFor(NODE_POOL, FOUNDERS, LOAD_IDS, INITIAL_LEADER)

  action invoke(node: NodeName, request: ApiRequest): bool = {
    val owner = world.system.get(node)
    val sequence = owner.volatile.pendingCalls.keys().size() + 1
    val caller = { node: node, incarnation: owner.volatile.incarnation }
    val key = { caller: caller, sequence: sequence }
    val msg = { delivery: world.extensions.nextDelivery, source: caller,
                destination: node, channel: MetaChannel,
                payload: ApiCall({ key: key, request: request }) }
    all {
      sequence <= MAX_REQUESTS,
      owner.volatile.phase != Stopped and owner.volatile.phase != Failed,
      world.extensions.nextDelivery <= MAX_DELIVERIES,
      world' = { ...world,
        system: world.system.put(node, { ...owner,
          volatile: { ...owner.volatile, pendingCalls: owner.volatile.pendingCalls.put(key,
            { request: request, phase: Captured, result: None, waiterCancelled: false }) } }),
        extensions: { ...world.extensions,
          network: { ...world.extensions.network,
            pending: world.extensions.network.pending.union(Set(msg)) },
          nextDelivery: world.extensions.nextDelivery + 1 },
      },
    }
  }

  action deliver(msg: Envelope): bool = all {
    world.extensions.network.pending.contains(msg),
    (if (msg.channel == MetaChannel) world.extensions.network.metaLinks
     else world.extensions.network.loadLinks).contains((msg.source.node, msg.destination)),
    world.system.get(msg.destination).volatile.phase != Stopped,
    world' = { ...world,
      messages: world.messages.setBy(msg.destination, inbox => inbox.union(Set(msg))),
      extensions: { ...world.extensions,
        network: { ...world.extensions.network,
          pending: world.extensions.network.pending.exclude(Set(msg)),
          delivered: world.extensions.network.delivered.union(Set(msg.delivery)) } },
    },
  }

  action handle(node: NodeName): bool = {
    val ctx = C::convert_context(world, node)
    nondet tr = main_listener(ctx).oneOf()
    all {
      world.system.get(node).volatile.phase != Stopped,
      world' = C::apply_effect(world, node, tr, apply_custom_effect),
    }
  }

  action drop(msg: Envelope): bool = all {
    world.extensions.network.pending.contains(msg),
    world' = { ...world, extensions: { ...world.extensions,
      network: { ...world.extensions.network,
        pending: world.extensions.network.pending.exclude(Set(msg)) } } },
  }

  action duplicate(msg: Envelope): bool = all {
    world.extensions.network.pending.contains(msg),
    world.extensions.nextDelivery <= MAX_DELIVERIES,
    world' = { ...world, extensions: { ...world.extensions,
      nextDelivery: world.extensions.nextDelivery + 1,
      network: { ...world.extensions.network, pending: world.extensions.network.pending.union(
        Set({ ...msg, delivery: world.extensions.nextDelivery })) } } },
  }

  action setLink(channel: Channel, source: NodeName, destination: NodeName, up: bool): bool = {
    val net = world.extensions.network
    val links = if (channel == MetaChannel) net.metaLinks else net.loadLinks
    val edge = (source, destination)
    val changed = if (up) links.union(Set(edge)) else links.exclude(Set(edge))
    all {
      source != destination, links.contains(edge) != up,
      world' = { ...world, extensions: { ...world.extensions,
        network: if (channel == MetaChannel) { ...net, metaLinks: changed }
                 else { ...net, loadLinks: changed } } },
    }
  }

  action crash(node: NodeName): bool = {
    val owner = world.system.get(node)
    all {
      M::running(owner),
      world' = { ...world,
        system: world.system.put(node, { ...owner, volatile: { ...owner.volatile,
          phase: Stopped, pendingCalls: Map(), owners: Map(), observations: Map(),
          session: None, schedulerCapture: None, appliedMetaIndex: 0, appliedMetaVersion: 0,
          localView: { latest: I::emptyDocument, target: I::emptyDocument } } }),
        messages: world.messages.put(node, Set()),
      },
    }
  }

  action restart(node: NodeName): bool = {
    val owner = world.system.get(node)
    val view = match owner.durable.metaImage {
      | Some(image) => image
      | None => { latest: I::emptyDocument, target: I::emptyDocument }
    }
    all {
      owner.volatile.phase == Stopped,
      owner.volatile.incarnation < MAX_INCARNATION,
      world' = { ...world, system: world.system.put(node, { ...owner,
        volatile: { ...owner.volatile, phase: Starting,
          incarnation: owner.volatile.incarnation + 1,
          localView: view,
          appliedMetaIndex: owner.durable.metaIndex,
          appliedMetaVersion: view.latest.version } }) },
    }
  }

  // TODO(not planned): application business semantics. A bounded committed
  // position witnesses independent consensus, not a business command result.
  action commitProgress(group: GroupId): bool = {
    val c = world.extensions.consensus.get(group)
    val links = M::linksFor(world.extensions.network, group)
    val voters = match c.leaderAuthority {
      | Some(leader) => M::certificate(c, world.system, links, leader)
      | None => Set()
    }
    val position = c.committedPosition + 1
    all {
      M::hasCommitPath(c, world.system, links), position <= MAX_POSITION,
      world' = { ...world,
        system: voters.fold(world.system, (nodes, r) => {
          val node = nodes.get(r.node)
          val evidence = node.durable.replicaEvidence.get(r)
          val updated = match evidence {
            | ValidStore(p) => ValidStore({ ...p, commit: position, tail: position })
            | _ => evidence
          }
          nodes.put(r.node, { ...node, durable: { ...node.durable,
            replicaEvidence: node.durable.replicaEvidence.put(r, updated) } })
        }),
        extensions: { ...world.extensions,
          consensus: world.extensions.consensus.put(group, { ...c, committedPosition: position,
            catchUp: c.catchUp.keys().mapBy(r => if (voters.contains(r)) CaughtUp else Behind) }),
          metaLog: if (group == "_meta") world.extensions.metaLog.put(position, world.extensions.metadata)
                   else world.extensions.metaLog },
      },
    }
  }

  action elect(group: GroupId, candidate: ReplicaKey): bool = {
    val c = world.extensions.consensus.get(group)
    val valid = M::liveIdentity(world.system, candidate)
    val retained = if (valid) match world.system.get(candidate.node).durable.replicaEvidence.get(candidate) {
      | ValidStore(p) => p.commit >= c.committedPosition
      | _ => false
    } else false
    all {
      valid, retained, c.leaderAuthority != Some(candidate),
      M::certificate(c, world.system, M::linksFor(world.extensions.network, group), candidate).size() * 2 > c.voters.size(),
      world' = { ...world, extensions: { ...world.extensions,
        consensus: world.extensions.consensus.put(group, { ...c, leaderAuthority: Some(candidate) }) } },
    }
  }

  action catchUp(group: GroupId, replica: ReplicaKey): bool = {
    val c = world.extensions.consensus.get(group)
    val valid = M::liveIdentity(world.system, replica)
    val source = match c.leaderAuthority {
      | Some(leader) => M::liveIdentity(world.system, leader) and
        M::communicates(M::linksFor(world.extensions.network, group), leader.node, replica.node)
      | None => false
    }
    val node = world.system.get(replica.node)
    val evidence = node.durable.replicaEvidence.getOrElse(replica, MissingStore)
    val lagging = match evidence {
      | ValidStore(p) => p.commit < c.committedPosition
      | _ => false
    }
    all {
      valid, source, lagging, c.voters.union(c.learners).contains(replica),
      world' = { ...world, system: world.system.put(replica.node, { ...node,
        durable: { ...node.durable, replicaEvidence: node.durable.replicaEvidence.put(replica,
          ValidStore({ snapshot: 0, commit: c.committedPosition, tail: c.committedPosition,
                       appliedHint: 0 })) } }),
        extensions: { ...world.extensions,
          consensus: world.extensions.consensus.put(group, { ...c,
            catchUp: c.catchUp.put(replica, CaughtUp) }) },
      },
    }
  }

  action applyMeta(node: NodeName): bool = {
    val owner = world.system.get(node)
    val replicas = world.extensions.consensus.get("_meta").voters.union(
      world.extensions.consensus.get("_meta").learners).filter(r => r.node == node)
    nondet replica = replicas.oneOf()
    val position = match owner.durable.replicaEvidence.getOrElse(replica, MissingStore) {
      | ValidStore(p) => p.commit
      | _ => 0
    }
    val image = world.extensions.metaLog.getOrElse(position, world.extensions.metadata)
    val view = { latest: image.latest, target: image.target }
    all {
      M::liveIdentity(world.system, replica), position > owner.volatile.appliedMetaIndex,
      world.extensions.metaLog.keys().contains(position),
      world' = { ...world, system: world.system.put(node, { ...owner,
        volatile: { ...owner.volatile, appliedMetaIndex: position,
          appliedMetaVersion: image.latest.version, localView: view },
        durable: { ...owner.durable, metaIndex: position, metaImage: Some(view) } }),
        extensions: { ...world.extensions, consensus: world.extensions.consensus.setBy("_meta", c =>
          { ...c, appliedPositions: c.appliedPositions.put(replica, position) }) },
      },
    }
  }

  action step = any {
    nondet node = NODE_POOL.oneOf()
    nondet request = Set(ObserveLocal, InvalidRequest).oneOf()
    invoke(node, request),
    nondet msg = world.extensions.network.pending.oneOf()
    deliver(msg),
    nondet node = NODE_POOL.oneOf()
    handle(node),
    nondet msg = world.extensions.network.pending.oneOf()
    drop(msg),
    nondet msg = world.extensions.network.pending.oneOf()
    duplicate(msg),
    nondet source = NODE_POOL.oneOf()
    nondet destination = NODE_POOL.oneOf()
    nondet channel = Set(MetaChannel, LoadChannel).oneOf()
    nondet up = Set(true, false).oneOf()
    setLink(channel, source, destination, up),
    nondet node = NODE_POOL.oneOf()
    crash(node),
    nondet node = NODE_POOL.oneOf()
    restart(node),
    nondet group = world.extensions.consensus.keys().oneOf()
    commitProgress(group),
    nondet group = world.extensions.consensus.keys().oneOf()
    nondet candidate = world.extensions.consensus.get(group).voters.oneOf()
    elect(group, candidate),
    nondet group = world.extensions.consensus.keys().oneOf()
    nondet replica = world.extensions.consensus.get(group).voters.union(
      world.extensions.consensus.get(group).learners).oneOf()
    catchUp(group, replica),
    nondet node = world.extensions.metadata.target.nodes.keys().oneOf()
    applyMeta(node),
  }
}
