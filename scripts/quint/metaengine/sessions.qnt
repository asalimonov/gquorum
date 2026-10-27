module sessions {
  import types.* from "../types"
  import basicSpells.* from "../vendor/spells/basicSpells"

  // Only the reserved per-node activation ownership is represented. There is
  // no public lock API or application-lock state machine in this model.
  pure def eligible(meta: MetadataState, process: ProcessKey, member: int): bool = and {
    meta.target.nodes.keys().contains(process.node), meta.target.nodes.get(process.node).member == member,
    meta.groups.keys().contains("_meta"), match meta.groups.get("_meta").proven {
      | Some(group) => group.replicas.exists(r => r.key.node == process.node and r.key.member == member)
      | None => false },
  }
  pure def errors(meta: MetadataState, command: SessionCommand): Set[Refusal] = match command {
    | GrantInternalSession(g) => if (not(eligible(meta, g.key.owner, g.member))) Set(IdentityMismatch)
        else if (meta.sessions.keys().contains(g.key) and meta.sessions.get(g.key).member != g.member) Set(IdentityMismatch)
        else Set()
    | BindInternalSession(key) => if (not(meta.sessions.keys().contains(key))) Set(NotFound) else {
        val session = meta.sessions.get(key)
        if (session.phase != Live or not(eligible(meta, key.owner, session.member))) Set(IdentityMismatch)
        else if (meta.activeSessions.keys().contains(key.owner.node) and meta.activeSessions.get(key.owner.node) != key)
          Set(Conflict) else Set()
      }
    | RevokeInternalSession(_) => Set()
  }
  pure def apply(meta: MetadataState, command: SessionCommand): MetadataState = match command {
    | GrantInternalSession(g) => { ...meta, sessions: meta.sessions.put(g.key,
        { owner: g.key.owner, member: g.member, phase: Live, renewalGeneration: 0 }) }
    | BindInternalSession(key) => { ...meta,
        activeSessions: meta.activeSessions.put(key.owner.node, key),
        couriers: meta.couriers.put(key.owner.node, { process: key.owner, member: meta.sessions.get(key).member }) }
    | RevokeInternalSession(key) => {
        val owned = meta.activeSessions.getOrElse(key.owner.node, key) == key
        { ...meta, sessions: if (meta.sessions.keys().contains(key)) meta.sessions.setBy(key, s => { ...s, phase: Revoked }) else meta.sessions,
          activeSessions: if (owned) meta.activeSessions.mapRemove(key.owner.node) else meta.activeSessions,
          couriers: if (owned) meta.couriers.mapRemove(key.owner.node) else meta.couriers }
      }
  }
  pure val freshLease: Lease = { phase: Live, epoch: 0, probe: None, failedProbe: false }
  pure def refreshed(lease: Lease): Lease = { phase: Live, epoch: lease.epoch + 1, probe: None, failedProbe: false }
  pure def renewalMatches(meta: MetadataState, renewal: Renewal, source: ProcessKey): bool =
    source == renewal.key.owner and meta.sessions.keys().contains(renewal.key) and
      meta.sessions.get(renewal.key).phase == Live and meta.sessions.get(renewal.key).member == renewal.member and
      eligible(meta, source, renewal.member)

  // Ready is an observed local lease/declaration predicate, not a quorum
  // certificate and not a claim that remote metadata cannot have advanced.
  pure def observedReady(node: LocalNode, local: MetadataState): bool = match node.volatile.session {
    | None => false
    | Some(key) => node.volatile.ownLease == Live and key.owner == {
        node: node.process_id, incarnation: node.volatile.incarnation } and
        match node.durable.pin { | Some(pin) => eligible(local, key.owner, pin.member) | None => false }
  }
  pure def metadataSafety(meta: MetadataState): bool = meta.activeSessions.keys().forall(node => {
    val key = meta.activeSessions.get(node)
    meta.sessions.keys().contains(key) and meta.sessions.get(key).phase == Live and key.owner.node == node and
      meta.couriers.getOrElse(node, { process: { node: "", incarnation: 0 }, member: 0 }) ==
        { process: key.owner, member: meta.sessions.get(key).member }
  })
}
