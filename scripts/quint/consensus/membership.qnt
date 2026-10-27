module membership {
  import types.* from "../types"
  import basicSpells.* from "../vendor/spells/basicSpells"

  // TODO(not planned): concrete Raft replication/elections. These predicates
  // describe a safe commit certificate, never an observation available to a host.
  pure def running(node: LocalNode): bool =
    node.volatile.phase == Ready or node.volatile.phase == Starting

  pure def validStore(store: StorageEvidence): bool = match store {
    | ValidStore(p) => and {
      p.snapshot >= 0, p.snapshot <= p.commit, p.commit <= p.tail,
      p.appliedHint >= 0, p.appliedHint <= p.commit,
    }
    | _ => false
  }

  pure def liveIdentity(nodes: NodeStates, key: ReplicaKey): bool =
    if (not(nodes.keys().contains(key.node))) false else {
      val node = nodes.get(key.node)
      and {
        node.volatile.phase != Stopped and node.volatile.phase != Failed,
        node.durable.pin == Some({ cluster: key.cluster, node: key.node, member: key.member }),
        if (key.group == "_meta") node.volatile.metaRunning
        else if (node.volatile.owners.keys().contains(key.group)) {
          val owner = node.volatile.owners.get(key.group)
          owner.key == key and owner.engineRunning
        } else false,
        if (node.durable.replicaEvidence.keys().contains(key))
          validStore(node.durable.replicaEvidence.get(key)) else false,
      }
    }

  pure def communicates(links: Set[(NodeName, NodeName)], a: NodeName, b: NodeName): bool =
    a == b or (links.contains((a, b)) and links.contains((b, a)))

  pure def readableIdentity(nodes: NodeStates, key: ReplicaKey): bool =
    if (not(liveIdentity(nodes, key))) false else if (key.group == "_meta") true else {
      val owner = nodes.get(key.node).volatile.owners.get(key.group)
      owner.published and owner.replayReady and owner.startupAuthorized and owner.phase == OwnerReady
    }

  pure def certificate(c: ConsensusState, nodes: NodeStates,
                       links: Set[(NodeName, NodeName)], leader: ReplicaKey): Set[ReplicaKey] =
    if (not(c.voters.contains(leader)) or not(liveIdentity(nodes, leader))) Set()
    else c.voters.filter(r => liveIdentity(nodes, r) and communicates(links, leader.node, r.node))

  pure def hasCommitPath(c: ConsensusState, nodes: NodeStates,
                        links: Set[(NodeName, NodeName)]): bool = match c.leaderAuthority {
    | Some(leader) => certificate(c, nodes, links, leader).size() * 2 > c.voters.size()
    | None => false
  }

  pure def linksFor(net: NetworkState, group: GroupId): Set[(NodeName, NodeName)] =
    if (group == "_meta") net.metaLinks else net.loadLinks

  pure def persistCertificate(nodes: NodeStates, voters: Set[ReplicaKey], position: int): NodeStates =
    voters.fold(nodes, (current, r) => {
      val node = current.get(r.node)
      val evidence = node.durable.replicaEvidence.get(r)
      val updated = match evidence {
        | ValidStore(p) => ValidStore({ ...p, commit: position, tail: position })
        | _ => evidence
      }
      current.put(r.node, { ...node, durable: { ...node.durable,
        replicaEvidence: node.durable.replicaEvidence.put(r, updated) } })
    })

  pure def committed(c: ConsensusState, voters: Set[ReplicaKey], position: int): ConsensusState =
    { ...c, committedPosition: position,
      catchUp: c.catchUp.keys().mapBy(r => if (voters.contains(r)) CaughtUp else Behind) }
  pure def freshMetaAvailable(env: Environment, nodes: NodeStates, node: NodeName): bool = {
    val c = env.consensus.get("_meta")
    running(nodes.get(node)) and hasCommitPath(c, nodes, env.network.metaLinks) and
      match c.leaderAuthority { | Some(leader) => communicates(env.network.metaLinks, node, leader.node)
        | None => false }
  }
}
