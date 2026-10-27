module metaservice_authority_steps {
  import frames as Frames from "../runtime/frames"
  import controls as Ctrl from "../metaengine/controls"
  import execution as E from "../managed/execution"
  import grants as G from "../metaengine/grants"
  import membership as M from "../consensus/membership"
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import message_effects.* from "../runtime/messages"

  pure def submitControlEnabled(w: World, node: NodeName, command: ControlCommand, MAX_DELIVERIES: int): bool = {
    val owner = w.system.get(node)
    val local = w.extensions.metaLog.get(owner.volatile.appliedMetaIndex)
    val leader = owner.volatile.observations.get("_meta").observedLeader
    val caller = { node: node, incarnation: owner.volatile.incarnation }
    match leader {
      | Some(r) => and {
M::running(owner),
Ctrl::errors(local, command).size() == 0,
w.extensions.nextDelivery <= MAX_DELIVERIES,
not(w.extensions.network.pending.union(w.messages.values().flatten()).exists(msg =>
            msg.source == caller and match msg.payload {
              | MetaProposal(p) => p.command == ControlProposal(command) | _ => false }))
}
      | None => and {
        false
      }
    }
  }

  pure def submitControlNext(w: World, node: NodeName, command: ControlCommand, MAX_DELIVERIES: int): World = {
    val owner = w.system.get(node)
    val leader = owner.volatile.observations.get("_meta").observedLeader
    val caller = { node: node, incarnation: owner.volatile.incarnation }
    val proposalKey: ProposalKey = { request: { caller: caller, sequence: -w.extensions.nextDelivery }, sequence: 0 }
    val submitting = match command {
      | SessionControl(sessionCommand) => match sessionCommand {
          | RevokeInternalSession(key) => Frames::withProcess(w, node, updatedProcess => { ...updatedProcess, revokeGate: Some({ key: key, proposal: proposalKey }) })
          | _ => w }
      | _ => w
    }
    match leader {
      | Some(r) => (apply_custom_effect(submitting, Queue({ delivery: w.extensions.nextDelivery,
            source: caller, destination: r.node, channel: MetaChannel,
            payload: MetaProposal({ key: proposalKey,
              coordinator: caller, forWork: false, command: ControlProposal(command) }) })))
      | None => (w)
    }
  }

  pure def observeAuthorityLeaderEnabled(w: World, node: NodeName, group: GroupId): bool = {
    val c = w.extensions.consensus.get(group)
    val owner = w.system.get(node)
    match c.leaderAuthority {
      | Some(leader) => and {
M::running(owner),
M::readableIdentity(w.system, leader),
M::communicates(M::linksFor(w.extensions.network, group), node, leader.node),
not(owner.volatile.observations.keys().contains(group) and
            owner.volatile.observations.get(group).observedLeader == Some(leader))
}
      | None => and {
        false
      }
    }
  }

  pure def observeAuthorityLeaderNext(w: World, node: NodeName, group: GroupId): World = {
    val c = w.extensions.consensus.get(group)
    val owner = w.system.get(node)
    match c.leaderAuthority {
      | Some(leader) => (Frames::withProcess(w, node, updatedProcess => { ...updatedProcess, observations: updatedProcess.observations.put(group,
              { group: group, replica: leader, incarnation: w.system.get(leader.node).volatile.incarnation,
                observedConfig: c.configRevision, observedLeader: Some(leader), health: Healthy, age: Fresh }) }))
      | None => (w)
    }
  }

  pure def dispatchGrantFromCandidates(w: World, node: NodeName, key: GrantKey, resource: ResourceKey, proofIndex: int, MAX_DELIVERIES: int): Set[ReplicaKey] = {
    val owner = w.system.get(node)
    val local = w.extensions.metaLog.get(proofIndex)
    val grant = local.grants.get(key)
    val candidates = match resource {
      | HostResource(host) => grant.targets.filter(r => r.node == host.node)
      | GroupResource(group) => if (owner.volatile.observations.keys().contains(group))
          match owner.volatile.observations.get(group).observedLeader { | Some(r) => Set(r) | None => Set() }
          else Set()
    }

    candidates
  }

  pure def dispatchGrantFromEnabled(w: World, node: NodeName, key: GrantKey, resource: ResourceKey, proofIndex: int, replica: ReplicaKey, MAX_DELIVERIES: int): bool = {
    val owner = w.system.get(node)
    val local = w.extensions.metaLog.get(proofIndex)
    val grant = local.grants.get(key)
    val delivery: EffectDelivery = { delivery: w.extensions.nextDelivery,
      sender: { node: node, incarnation: owner.volatile.incarnation },
      senderMember: match owner.durable.pin { | Some(pin) => pin.member | None => 0 }, proofIndex: None,
      receiver: { node: replica.node, incarnation: w.system.get(replica.node).volatile.incarnation },
      replica: replica, grant: grant, resource: resource }
    and {
      M::running(owner),
      grant.resources.contains(resource),
      G::currentForIssuance(local, grant),
      E::courierMatches(w.extensions.metadata, delivery),
      M::hasCommitPath(w.extensions.consensus.get("_meta"), w.system, w.extensions.network.metaLinks),
      match w.extensions.consensus.get("_meta").leaderAuthority { | Some(leader) =>
        M::communicates(w.extensions.network.metaLinks, node, leader.node) | None => false },
      not(G::resourceConsumed(local, grant, resource)),
      E::executable(delivery),
      w.extensions.nextDelivery <= MAX_DELIVERIES
    }
  }

  pure def dispatchGrantFromNext(w: World, node: NodeName, key: GrantKey, resource: ResourceKey, proofIndex: int, replica: ReplicaKey, MAX_DELIVERIES: int): World = {
    val owner = w.system.get(node)
    val local = w.extensions.metaLog.get(proofIndex)
    val grant = local.grants.get(key)
    val delivery: EffectDelivery = { delivery: w.extensions.nextDelivery,
      sender: { node: node, incarnation: owner.volatile.incarnation },
      senderMember: match owner.durable.pin { | Some(pin) => pin.member | None => 0 }, proofIndex: None,
      receiver: { node: replica.node, incarnation: w.system.get(replica.node).volatile.incarnation },
      replica: replica, grant: grant, resource: resource }
    (apply_custom_effect(w, Queue({ delivery: delivery.delivery, source: delivery.sender,
        destination: replica.node, channel: if (grant.key.group == "_meta") MetaChannel else LoadChannel,
        payload: ExecuteGrant(delivery) })))
  }

}
