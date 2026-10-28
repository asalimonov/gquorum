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
        replicaEvidence: node.durable.replicaEvidence.put(r, updated),
        metaSeen: node.durable.metaSeen or r.group == "_meta" } })
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

  // membership/safety.go: only one automatic non-joint change is supported.
  pure def singleAutomatic(kinds: List[ActionKind], joint: bool, automatic: bool): bool =
    if (kinds.length() != 1) false else not(joint) and automatic and nativeKind(kinds.head())

  pure def nativeKind(kind: ActionKind): bool =
    Set(AddLearner, PromoteLearner, DemoteVoter, RemoveMember).contains(kind)

  pure def postVoters(c: ConsensusState, kind: ActionKind, target: ReplicaKey): Set[ReplicaKey] =
    match kind {
      | PromoteLearner => c.voters.union(Set(target))
      | DemoteVoter => c.voters.exclude(Set(target))
      | RemoveMember => c.voters.exclude(Set(target))
      | _ => c.voters
    }

  pure def postLearners(c: ConsensusState, kind: ActionKind, target: ReplicaKey): Set[ReplicaKey] =
    match kind {
      | AddLearner => c.learners.union(Set(target))
      | DemoteVoter => c.learners.union(Set(target))
      | PromoteLearner => c.learners.exclude(Set(target))
      | RemoveMember => c.learners.exclude(Set(target))
      | _ => c.learners
    }

  pure def nativeSatisfied(c: ConsensusState, kind: ActionKind, target: ReplicaKey): bool =
    match kind {
      | AddLearner => c.learners.contains(target)
      | PromoteLearner => c.voters.contains(target)
      | DemoteVoter => c.learners.contains(target) and not(c.voters.contains(target))
      | RemoveMember => not(c.voters.union(c.learners).contains(target))
      | _ => false
    }

  // Health is supplied by a fresh native observation. Good metric assumptions
  // do not supply a vote or the successful learner catch-up milestone.
  pure def safeChange(c: ConsensusState, kind: ActionKind, target: ReplicaKey,
                      live: Set[ReplicaKey], caughtUp: bool): bool = {
    val after = postVoters(c, kind, target)
    // Native Raft membership uses numeric IDs, not unique host names. During
    // metadata readmission the fenced predecessor and successor may coexist in
    // ConfState; liveIdentity still accepts only the host's exact current pin.
    val knownIdentity = c.voters.union(c.learners).forall(r =>
      r.replica != target.replica or r == target)
    if (not(nativeKind(kind)) or target.replica <= 0 or not(knownIdentity)) false
    else if (nativeSatisfied(c, kind, target)) true
    else if (kind == AddLearner) not(c.voters.contains(target))
    else and {
      kind != PromoteLearner or (c.learners.contains(target) and caughtUp),
      kind != DemoteVoter or c.voters.contains(target),
      after.size() > 0,
      c.leaderAuthority != Some(target) or after.contains(target),
      after.intersect(live).size() * 2 > after.size(),
    }
  }

  pure def observedLive(c: ConsensusState, nodes: NodeStates,
                        links: Set[(NodeName, NodeName)]): Set[ReplicaKey] = match c.leaderAuthority {
    | Some(leader) => c.voters.union(c.learners).filter(r =>
        liveIdentity(nodes, r) and communicates(links, leader.node, r.node))
    | None => Set()
  }

  pure def expectedChange(c: ConsensusState, step: PlannedStep): bool = match step.target {
    | ReplicaTarget(target) => if (nativeSatisfied(c, step.kind, target)) c.voters == step.resultVoters
        else c.voters == step.expectedVoters and postVoters(c, step.kind, target) == step.resultVoters
    | _ => false
  }

  pure def applyChange(c: ConsensusState, kind: ActionKind, target: ReplicaKey): ConsensusState = {
    val voters = postVoters(c, kind, target)
    val learners = postLearners(c, kind, target)
    val members = voters.union(learners)
    { ...c, voters: voters, learners: learners, configRevision: c.configRevision + 1,
      catchUp: members.mapBy(r => c.catchUp.getOrElse(r, Behind)),
      appliedPositions: members.mapBy(r => c.appliedPositions.getOrElse(r, 0)),
      pendingChange: None, leaderAuthority: match c.leaderAuthority {
        | Some(leader) => if (voters.contains(leader)) Some(leader) else None | None => None } }
  }
}
