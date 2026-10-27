module consensus_steps {
  import membership as M from "./membership"
  import sessions as Sessions from "../metaengine/sessions"
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"

  pure def commitProgressEnabled(w: World, group: GroupId, MAX_POSITION: int): bool = {
    val c = w.extensions.consensus.get(group)
    val links = M::linksFor(w.extensions.network, group)
    val position = c.committedPosition + 1
    and {
      M::hasCommitPath(c, w.system, links),
      position <= MAX_POSITION
    }
  }

  pure def commitProgressNext(w: World, group: GroupId, MAX_POSITION: int): World = {
    val c = w.extensions.consensus.get(group)
    val links = M::linksFor(w.extensions.network, group)
    val voters = match c.leaderAuthority {
      | Some(leader) => M::certificate(c, w.system, links, leader)
      | None => Set()
    }
    val position = c.committedPosition + 1
    ({ ...w,
        system: M::persistCertificate(w.system, voters, position),
        extensions: { ...w.extensions,
          consensus: w.extensions.consensus.put(group, M::committed(c, voters, position)),
          metaLog: if (group == "_meta") w.extensions.metaLog.put(position, w.extensions.metadata)
                   else w.extensions.metaLog },
      })
  }

  pure def electEnabled(w: World, group: GroupId, candidate: ReplicaKey): bool = {
    val c = w.extensions.consensus.get(group)
    val valid = M::liveIdentity(w.system, candidate)
    val retained = if (valid) match w.system.get(candidate.node).durable.replicaEvidence.get(candidate) {
      | ValidStore(p) => p.commit >= c.committedPosition
      | _ => false
    } else false
    and {
      valid,
      retained,
      c.leaderAuthority != Some(candidate),
      M::certificate(c, w.system, M::linksFor(w.extensions.network, group), candidate).size() * 2 > c.voters.size()
    }
  }

  pure def electNext(w: World, group: GroupId, candidate: ReplicaKey): World = {
    val c = w.extensions.consensus.get(group)
    ({ ...w,
        system: if (group == "_meta") w.system.setBy(candidate.node, node => { ...node, volatile: { ...node.volatile,
          leases: w.extensions.metaLog.get(node.volatile.appliedMetaIndex).sessions.keys().filter(k =>
            w.extensions.metaLog.get(node.volatile.appliedMetaIndex).sessions.get(k).phase == Live).mapBy(_ => Sessions::freshLease) } })
          else w.system,
        extensions: { ...w.extensions,
          consensus: w.extensions.consensus.put(group, { ...c, leaderAuthority: Some(candidate) }) } })
  }

  pure def catchUpEnabled(w: World, group: GroupId, replica: ReplicaKey): bool = {
    val c = w.extensions.consensus.get(group)
    val valid = M::liveIdentity(w.system, replica)
    val source = match c.leaderAuthority {
      | Some(leader) => M::liveIdentity(w.system, leader) and
        M::communicates(M::linksFor(w.extensions.network, group), leader.node, replica.node)
      | None => false
    }
    val node = w.system.get(replica.node)
    val evidence = node.durable.replicaEvidence.getOrElse(replica, MissingStore)
    val lagging = match evidence {
      | ValidStore(p) => p.commit < c.committedPosition
      | _ => false
    }
    and {
      valid,
      source,
      lagging,
      c.voters.union(c.learners).contains(replica)
    }
  }

  pure def catchUpNext(w: World, group: GroupId, replica: ReplicaKey): World = {
    val c = w.extensions.consensus.get(group)
    val node = w.system.get(replica.node)
    ({ ...w, system: w.system.put(replica.node, { ...node,
        durable: { ...node.durable, replicaEvidence: node.durable.replicaEvidence.put(replica,
          ValidStore({ snapshot: 0, commit: c.committedPosition, tail: c.committedPosition,
                       appliedHint: 0 })) } }),
        extensions: { ...w.extensions,
          consensus: w.extensions.consensus.put(group, { ...c,
            catchUp: c.catchUp.put(replica, CaughtUp) }) },
      })
  }

  pure def applyMetaCandidates(w: World, node: NodeName): Set[ReplicaKey] = {
    val replicas = w.extensions.consensus.get("_meta").voters.union(
      w.extensions.consensus.get("_meta").learners).filter(r => r.node == node)

    replicas
  }

  pure def applyMetaEnabled(w: World, node: NodeName, replica: ReplicaKey): bool = {
    val owner = w.system.get(node)
    val position = match owner.durable.replicaEvidence.getOrElse(replica, MissingStore) {
      | ValidStore(p) => p.commit
      | _ => 0
    }
    and {
      M::liveIdentity(w.system, replica),
      position > owner.volatile.appliedMetaIndex,
      w.extensions.metaLog.keys().contains(position)
    }
  }

  pure def applyMetaNext(w: World, node: NodeName, replica: ReplicaKey): World = {
    val owner = w.system.get(node)
    val position = match owner.durable.replicaEvidence.getOrElse(replica, MissingStore) {
      | ValidStore(p) => p.commit
      | _ => 0
    }
    val image = w.extensions.metaLog.getOrElse(position, w.extensions.metadata)
    val view = { latest: image.latest, target: image.target }
    ({ ...w, system: w.system.put(node, { ...owner,
        volatile: { ...owner.volatile, appliedMetaIndex: position,
          appliedMetaVersion: image.latest.version, localView: view },
        durable: { ...owner.durable, metaIndex: position, metaImage: Some(view) } }),
        extensions: { ...w.extensions, consensus: w.extensions.consensus.setBy("_meta", c =>
          { ...c, appliedPositions: c.appliedPositions.put(replica, position) }) },
      })
  }

}
