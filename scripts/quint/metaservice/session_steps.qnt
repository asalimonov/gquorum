module metaservice_session_steps {
  import frames as Frames from "../runtime/frames"
  import membership as M from "../consensus/membership"
  import sessions as Sessions from "../metaengine/sessions"
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import message_effects.* from "../runtime/messages"

  pure def beginSessionEnabled(w: World, node: NodeName): bool = {
    val host = w.system.get(node)
    and {
      M::running(host),
      host.volatile.session == None,
      host.durable.pin != None
    }
  }

  pure def beginSessionNext(w: World, node: NodeName): World = {
    val host = w.system.get(node)
    val key: SessionKey = { owner: { node: node, incarnation: host.volatile.incarnation }, generation: 1 }
    (Frames::withProcess(w, node, updatedProcess => { ...updatedProcess, session: Some(key), ownLease: Revoked }))
  }

  pure def renewSessionEnabled(w: World, node: NodeName, MAX_DELIVERIES: int): bool = {
    val host = w.system.get(node)
    val local = w.extensions.metaLog.get(host.volatile.appliedMetaIndex)
    val leader = host.volatile.observations.get("_meta").observedLeader
    match host.volatile.session {
      | Some(key) => match host.durable.pin {
          | Some(pin) => match leader {
              | Some(destination) => {
                  val renewal: Renewal = { key: key, round: w.extensions.nextDelivery, member: pin.member }
                  and {
                    M::running(host),
                    host.volatile.ownLease != Live,
                    host.volatile.renewal == None,
                    local.activeSessions.getOrElse(node, { owner: { node: "", incarnation: 0 }, generation: 0 }) == key,
                    Sessions::renewalMatches(local, renewal, { node: node, incarnation: host.volatile.incarnation }),
                    w.extensions.nextDelivery <= MAX_DELIVERIES
                  }
                }
              | None => and {
                false
              }
            }
          | None => and {
            false
          }
        }
      | None => and {
        false
      }
    }
  }

  pure def renewSessionNext(w: World, node: NodeName, MAX_DELIVERIES: int): World = {
    val host = w.system.get(node)
    val leader = host.volatile.observations.get("_meta").observedLeader
    match host.volatile.session {
      | Some(key) => match host.durable.pin {
          | Some(pin) => match leader {
              | Some(destination) => {
                  val renewal: Renewal = { key: key, round: w.extensions.nextDelivery, member: pin.member }
                  (apply_custom_effect(Frames::withProcess(w, node, updatedProcess => { ...updatedProcess, renewal: Some(renewal), renewalExpired: false }),
                      Queue({ delivery: w.extensions.nextDelivery, source: key.owner, destination: destination.node,
                        channel: MetaChannel, payload: SessionRenew(renewal) })))
                }
              | None => (w)
            }
          | None => (w)
        }
      | None => (w)
    }
  }

  pure def handleSessionEnabled(w: World, msg: Envelope, MAX_DELIVERIES: int): bool = {
    val host = w.system.get(msg.destination)
    val source = { node: msg.destination, incarnation: host.volatile.incarnation }
    match msg.payload {
      | SessionRenew(renewal) => {
          val lease = host.volatile.leases.getOrElse(renewal.key, Sessions::freshLease)
          and {
            w.messages.get(msg.destination).contains(msg),
            M::running(host),
            w.extensions.nextDelivery <= MAX_DELIVERIES,
            lease.epoch < MAX_DELIVERIES,
            host.volatile.revokeGate == None
          }
        }
      | SessionRenewed(renewal) => {
          val exact = host.volatile.session == Some(renewal.key) and renewal.key.owner == source and
            host.volatile.renewal == Some(renewal)
          and {
            w.messages.get(msg.destination).contains(msg),
            M::running(host)
          }
        }
      | SessionProbe(probe) => match host.durable.pin {
          | Some(pin) => and {
w.messages.get(msg.destination).contains(msg),
M::running(host),
w.extensions.nextDelivery <= MAX_DELIVERIES
}
          | None => and {
            false
          }
        }
      | SessionProbed(probe) => {
          val lease = host.volatile.leases.getOrElse(probe.key, Sessions::freshLease)
          val exact = probe.leader == source and lease.phase == ExpiredObserved and lease.epoch == probe.epoch and
            lease.probe == Some(probe.round) and not(lease.failedProbe)
          and {
            w.messages.get(msg.destination).contains(msg),
            M::running(host),
            lease.epoch < MAX_DELIVERIES
          }
        }
      | MetaAck(ack) => match host.volatile.revokeGate {
          | Some(gate) => and {
w.messages.get(msg.destination).contains(msg),
ack.key == gate.proposal,
ack.coordinator == source,
host.volatile.appliedMetaIndex >= ack.index
}
          | None => and {
            false
          }
        }
      | _ => and {
        false
      }
    }
  }

  pure def handleSessionNext(w: World, msg: Envelope, MAX_DELIVERIES: int): World = {
    val host = w.system.get(msg.destination)
    val local = w.extensions.metaLog.get(host.volatile.appliedMetaIndex)
    val source = { node: msg.destination, incarnation: host.volatile.incarnation }
    match msg.payload {
      | SessionRenew(renewal) => {
          val valid = Sessions::locallyLeader(host) and host.volatile.revokeGate == None and
            Sessions::renewalMatches(local, renewal, msg.source)
          val lease = host.volatile.leases.getOrElse(renewal.key, Sessions::freshLease)
          (if (valid) apply_custom_effect(apply_custom_effect(Frames::withProcess(w, msg.destination, updatedProcess => { ...updatedProcess,
                leases: updatedProcess.leases.put(renewal.key, Sessions::refreshed(lease)) }), Consume(msg)),
              Queue({ delivery: w.extensions.nextDelivery, source: source, destination: renewal.key.owner.node,
                channel: MetaChannel, payload: SessionRenewed(renewal) }))
              else if (host.volatile.revokeGate == None) apply_custom_effect(w, Consume(msg)) else w)
        }
      | SessionRenewed(renewal) => {
          val exact = host.volatile.session == Some(renewal.key) and renewal.key.owner == source and
            host.volatile.renewal == Some(renewal)
          (apply_custom_effect(Frames::withNode(w, msg.destination, _ => if (exact) { ...host, volatile: { ...host.volatile, renewal: None,
                ownLease: if (host.volatile.renewalExpired) ExpiredObserved else Live,
                phase: if (host.volatile.renewalExpired) host.volatile.phase else Ready } } else host), Consume(msg)))
        }
      | SessionProbe(probe) => match host.durable.pin {
          | Some(pin) => (apply_custom_effect(apply_custom_effect(w, Consume(msg)), Queue({ delivery: w.extensions.nextDelivery,
                source: source, destination: probe.leader.node, channel: MetaChannel,
                payload: SessionProbed({ key: probe.key, epoch: probe.epoch, round: probe.round, leader: probe.leader, observed: source, member: pin.member }) })))
          | None => (w)
        }
      | SessionProbed(probe) => {
          val lease = host.volatile.leases.getOrElse(probe.key, Sessions::freshLease)
          val exact = probe.leader == source and lease.phase == ExpiredObserved and lease.epoch == probe.epoch and
            lease.probe == Some(probe.round) and not(lease.failedProbe)
          val same = probe.observed == probe.key.owner and Sessions::eligible(local, probe.observed, probe.member)
          (apply_custom_effect(Frames::withNode(w, msg.destination, _ => if (exact) { ...host,
              volatile: { ...host.volatile, leases: host.volatile.leases.put(probe.key,
                if (same) Sessions::refreshed(lease) else { ...lease, failedProbe: true }) } } else host), Consume(msg)))
        }
      | MetaAck(ack) => match host.volatile.revokeGate {
          | Some(gate) => (apply_custom_effect(Frames::withProcess(w, msg.destination, updatedProcess => { ...updatedProcess, revokeGate: None, leases: updatedProcess.leases.mapRemove(gate.key) }), Consume(msg)))
          | None => (w)
        }
      | _ => (w)
    }
  }

  pure def seedLeasesEnabled(w: World, node: NodeName): bool = {
    val host = w.system.get(node)
    val local = w.extensions.metaLog.get(host.volatile.appliedMetaIndex)
    val missing = local.sessions.keys().filter(k => local.sessions.get(k).phase == Live).exclude(host.volatile.leases.keys())
    and {
      M::running(host),
      Sessions::locallyLeader(host),
      missing.size() > 0
    }
  }

  pure def seedLeasesNext(w: World, node: NodeName): World = {
    val host = w.system.get(node)
    val local = w.extensions.metaLog.get(host.volatile.appliedMetaIndex)
    val missing = local.sessions.keys().filter(k => local.sessions.get(k).phase == Live).exclude(host.volatile.leases.keys())
    (Frames::withProcess(w, node, updatedProcess => { ...updatedProcess,
        leases: missing.fold(updatedProcess.leases, (leases, key) => leases.put(key, Sessions::freshLease)) }))
  }

  pure def forgetRevokedLeaseEnabled(w: World, node: NodeName, key: SessionKey): bool = {
    val host = w.system.get(node)
    val local = w.extensions.metaLog.get(host.volatile.appliedMetaIndex)
    and {
      Sessions::locallyLeader(host),
      host.volatile.leases.keys().contains(key),
      host.volatile.revokeGate == None,
      not(local.sessions.keys().contains(key)) or local.sessions.get(key).phase == Revoked
    }
  }

  pure def forgetRevokedLeaseNext(w: World, node: NodeName, key: SessionKey): World = {
    val host = w.system.get(node)
    (Frames::withProcess(w, node, updatedProcess => { ...updatedProcess, leases: updatedProcess.leases.mapRemove(key) }))
  }

  pure def expireLeaseEnabled(w: World, node: NodeName, key: SessionKey): bool = {
    val host = w.system.get(node)
    val lease = host.volatile.leases.get(key)
    and {
      M::running(host),
      Sessions::locallyLeader(host),
      lease.phase == Live,
      host.volatile.revokeGate == None
    }
  }

  pure def expireLeaseNext(w: World, node: NodeName, key: SessionKey): World = {
    val host = w.system.get(node)
    val lease = host.volatile.leases.get(key)
    (Frames::withProcess(w, node, updatedProcess => { ...updatedProcess,
        leases: updatedProcess.leases.put(key, { ...lease, phase: ExpiredObserved }) }))
  }

  pure def expireOwnLeaseEnabled(w: World, node: NodeName): bool = {
    val host = w.system.get(node)
    and {
      M::running(host),
      host.volatile.ownLease == Live or (host.volatile.renewal != None and not(host.volatile.renewalExpired))
    }
  }

  pure def expireOwnLeaseNext(w: World, node: NodeName): World = {
    val host = w.system.get(node)
    (Frames::withProcess(w, node, updatedProcess => { ...updatedProcess,
        ownLease: ExpiredObserved, renewalExpired: updatedProcess.renewal != None }))
  }

  pure def cancelRenewalEnabled(w: World, node: NodeName): bool = {
    val host = w.system.get(node)
    and {
      host.volatile.renewal != None
    }
  }

  pure def cancelRenewalNext(w: World, node: NodeName): World = {
    val host = w.system.get(node)
    (Frames::withProcess(w, node, updatedProcess => { ...updatedProcess, renewal: None }))
  }

  pure def probeSessionEnabled(w: World, node: NodeName, key: SessionKey, MAX_DELIVERIES: int): bool = {
    val host = w.system.get(node)
    val lease = host.volatile.leases.get(key)
    val round = w.extensions.nextDelivery
    and {
      M::running(host),
      Sessions::locallyLeader(host),
      lease.phase == ExpiredObserved,
      w.extensions.metaLog.get(host.volatile.appliedMetaIndex).sessions.keys().contains(key),
      w.extensions.metaLog.get(host.volatile.appliedMetaIndex).sessions.get(key).phase == Live,
      lease.probe == None,
      not(lease.failedProbe),
      round <= MAX_DELIVERIES
    }
  }

  pure def probeSessionNext(w: World, node: NodeName, key: SessionKey, MAX_DELIVERIES: int): World = {
    val host = w.system.get(node)
    val lease = host.volatile.leases.get(key)
    val source = { node: node, incarnation: host.volatile.incarnation }
    val round = w.extensions.nextDelivery
    (if (key.owner.node == node) Frames::withNode(w, node, _ => { ...host,
        volatile: { ...host.volatile, leases: host.volatile.leases.put(key,
          if (key.owner == source and match host.durable.pin { | Some(pin) => Sessions::eligible(
            w.extensions.metaLog.get(host.volatile.appliedMetaIndex), source, pin.member) | None => false })
            Sessions::refreshed(lease) else { ...lease, failedProbe: true }) } })
      else apply_custom_effect(Frames::withProcess(w, node, updatedProcess => { ...updatedProcess,
        leases: updatedProcess.leases.put(key, { ...lease, probe: Some(round) }) }),
        Queue({ delivery: round, source: source, destination: key.owner.node, channel: MetaChannel,
          payload: SessionProbe({ key: key, epoch: lease.epoch, round: round, leader: source }) })))
  }

  pure def timeoutSessionProbeEnabled(w: World, node: NodeName, key: SessionKey): bool = {
    val host = w.system.get(node)
    val lease = host.volatile.leases.get(key)
    and {
      lease.phase == ExpiredObserved,
      lease.probe != None,
      not(lease.failedProbe)
    }
  }

  pure def timeoutSessionProbeNext(w: World, node: NodeName, key: SessionKey): World = {
    val host = w.system.get(node)
    val lease = host.volatile.leases.get(key)
    (Frames::withProcess(w, node, updatedProcess => { ...updatedProcess,
        leases: updatedProcess.leases.put(key, { ...lease, failedProbe: true }) }))
  }

  pure def abandonSessionRevokeEnabled(w: World, node: NodeName): bool = {
    val host = w.system.get(node)
    and {
      host.volatile.revokeGate != None
    }
  }

  pure def abandonSessionRevokeNext(w: World, node: NodeName): World = {
    val host = w.system.get(node)
    (Frames::withProcess(w, node, updatedProcess => { ...updatedProcess, revokeGate: None }))
  }

}
