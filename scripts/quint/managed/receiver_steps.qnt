module managed_receiver_steps {
  import frames as Frames from "../runtime/frames"
  import execution as E from "./execution"
  import membership as M from "../consensus/membership"
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import message_effects.* from "../runtime/messages"

  pure def confirmGrantEnabled(w: World, msg: Envelope): bool = match msg.payload {
    | ExecuteGrant(_) => {
      val node = w.system.get(msg.destination)
      val c = w.extensions.consensus.get("_meta")
      and {
        w.messages.get(msg.destination).contains(msg),
        M::running(node),
        not(node.volatile.authorityReads.keys().contains(msg.delivery)),
        M::hasCommitPath(c, w.system, w.extensions.network.metaLinks),
        match c.leaderAuthority { | Some(leader) =>
          M::communicates(w.extensions.network.metaLinks, msg.destination, leader.node) | None => false }
      }
    }
    | _ => and {
      false
    }
  }

  pure def confirmGrantNext(w: World, msg: Envelope): World = match msg.payload {
    | ExecuteGrant(_) => {
      val node = w.system.get(msg.destination)
      val c = w.extensions.consensus.get("_meta")
      (Frames::withProcess(w, msg.destination, updatedProcess => { ...updatedProcess, authorityReads: updatedProcess.authorityReads.put(msg.delivery, c.committedPosition) }))
    }
    | _ => (w)
  }

  pure def receiveGrantEnabled(w: World, msg: Envelope): bool = match msg.payload {
    | ExecuteGrant(delivery) => {
      val node = w.system.get(msg.destination)
      and {
        w.messages.get(msg.destination).contains(msg),
        node.volatile.authorityReads.keys().contains(msg.delivery)
      }
    }
    | _ => and {
      false
    }
  }

  pure def receiveGrantNext(w: World, msg: Envelope): World = match msg.payload {
    | ExecuteGrant(delivery) => {
      val node = w.system.get(msg.destination)
      val authenticated = { ...delivery, proofIndex: if (node.volatile.authorityReads.keys().contains(msg.delivery))
        Some(node.volatile.authorityReads.get(msg.delivery)) else None }
      val accepted = delivery.sender == msg.source and delivery.receiver.node == msg.destination and
        E::mayReceive(w, authenticated)
      (apply_custom_effect(Frames::withProcess(w, msg.destination, updatedProcess => { ...updatedProcess, waitingEffects: if (accepted)
            updatedProcess.waitingEffects.union(Set(authenticated)) else updatedProcess.waitingEffects,
            authorityReads: updatedProcess.authorityReads.mapRemove(msg.delivery) }), Consume(msg)))
    }
    | _ => (w)
  }

  pure def consumeGrantEnabled(w: World, delivery: EffectDelivery, MAX_POSITION: int): bool = {
    val node = w.system.get(delivery.receiver.node)
    val nativeReady = match delivery.resource {
      | HostResource(_) => node.durable.journalPosition < MAX_POSITION
      | GroupResource(group) => {
          val c = w.extensions.consensus.get(group)
          c.leaderAuthority == Some(delivery.replica) and c.committedPosition < MAX_POSITION and
            M::hasCommitPath(c, w.system, M::linksFor(w.extensions.network, group)) and
            (delivery.grant.purpose == Fence or match delivery.grant.effect {
              | Some(e) => match e.target { | ReplicaTarget(r) => c.voters.contains(r) and
                  M::readableIdentity(w.system, r) and c.catchUp.getOrElse(r, Behind) == CaughtUp
                  | _ => false }
              | None => false })
        }
    }
    and {
      node.volatile.waitingEffects.contains(delivery),
      E::executable(delivery),
      E::mayConsume(w, delivery),
      E::consumed(w, delivery) or nativeReady
    }
  }

  pure def consumeGrantNext(w: World, delivery: EffectDelivery, MAX_POSITION: int): World = {
    (E::consume(w, delivery))
  }

  pure def refuseGrantEnabled(w: World, delivery: EffectDelivery): bool = and {
w.system.get(delivery.receiver.node).volatile.waitingEffects.contains(delivery),
not(E::mayConsume(w, delivery))
}

  pure def refuseGrantNext(w: World, delivery: EffectDelivery): World = (E::forgetWait(w, delivery))

}
