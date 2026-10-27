module harness_environment_steps {
  import initial as I from "./initial"
  import membership as M from "../consensus/membership"
  import scope as S from "./scope"
  import transactions as Tx from "../metaservice/transactions"
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"

  pure def invokeEnabled(w: World, node: NodeName, request: ApiRequest, MAX_REQUESTS: int, MAX_DELIVERIES: int): bool = {
    val owner = w.system.get(node)
    val sequence = owner.volatile.pendingCalls.keys().size() + 1
    val caller = { node: node, incarnation: owner.volatile.incarnation }
    val key = { caller: caller, sequence: sequence }
    val call = Tx::newCall(request, owner.volatile.localView.latest.version, node)
    val msg = { delivery: w.extensions.nextDelivery, source: caller,
                destination: node, channel: MetaChannel,
                payload: ApiCall({ key: key, request: call.request }) }
    and {
      S::requestInScope(owner.volatile.localView.latest, request),
      sequence <= MAX_REQUESTS,
      owner.volatile.phase != Stopped and owner.volatile.phase != Failed,
      w.extensions.nextDelivery <= MAX_DELIVERIES
    }
  }

  pure def invokeNext(w: World, node: NodeName, request: ApiRequest, MAX_REQUESTS: int, MAX_DELIVERIES: int): World = {
    val owner = w.system.get(node)
    val sequence = owner.volatile.pendingCalls.keys().size() + 1
    val caller = { node: node, incarnation: owner.volatile.incarnation }
    val key = { caller: caller, sequence: sequence }
    val call = Tx::newCall(request, owner.volatile.localView.latest.version, node)
    val msg = { delivery: w.extensions.nextDelivery, source: caller,
                destination: node, channel: MetaChannel,
                payload: ApiCall({ key: key, request: call.request }) }
    ({ ...w,
        system: w.system.put(node, { ...owner,
          volatile: { ...owner.volatile, pendingCalls: owner.volatile.pendingCalls.put(key,
            call) } }),
        extensions: { ...w.extensions,
          network: { ...w.extensions.network,
            pending: w.extensions.network.pending.union(Set(msg)) },
          nextDelivery: w.extensions.nextDelivery + 1 },
      })
  }

  pure def deliverEnabled(w: World, msg: Envelope): bool = and {
w.extensions.network.pending.contains(msg),
(if (msg.channel == MetaChannel) w.extensions.network.metaLinks
     else w.extensions.network.loadLinks).contains((msg.source.node, msg.destination)),
w.system.get(msg.destination).volatile.phase != Stopped
}

  pure def deliverNext(w: World, msg: Envelope): World = ({ ...w,
      messages: w.messages.setBy(msg.destination, inbox => inbox.union(Set(msg))),
      extensions: { ...w.extensions,
        network: { ...w.extensions.network,
          pending: w.extensions.network.pending.exclude(Set(msg)),
          delivered: w.extensions.network.delivered.union(Set(msg.delivery)) } },
    })

  pure def dropEnabled(w: World, msg: Envelope): bool = and {
w.extensions.network.pending.contains(msg)
}

  pure def dropNext(w: World, msg: Envelope): World = ({ ...w, extensions: { ...w.extensions,
      network: { ...w.extensions.network,
        pending: w.extensions.network.pending.exclude(Set(msg)) } } })

  pure def duplicateEnabled(w: World, msg: Envelope, MAX_DELIVERIES: int): bool = and {
w.extensions.network.pending.contains(msg),
w.extensions.nextDelivery <= MAX_DELIVERIES
}

  pure def duplicateNext(w: World, msg: Envelope, MAX_DELIVERIES: int): World = ({ ...w, extensions: { ...w.extensions,
      nextDelivery: w.extensions.nextDelivery + 1,
      network: { ...w.extensions.network, pending: w.extensions.network.pending.union(
        Set({ ...msg, delivery: w.extensions.nextDelivery })) } } })

  pure def setLinkEnabled(w: World, channel: Channel, source: NodeName, destination: NodeName, up: bool): bool = {
    val net = w.extensions.network
    val links = if (channel == MetaChannel) net.metaLinks else net.loadLinks
    val edge = (source, destination)
    val changed = if (up) links.union(Set(edge)) else links.exclude(Set(edge))
    and {
      source != destination,
      links.contains(edge) != up
    }
  }

  pure def setLinkNext(w: World, channel: Channel, source: NodeName, destination: NodeName, up: bool): World = {
    val net = w.extensions.network
    val links = if (channel == MetaChannel) net.metaLinks else net.loadLinks
    val edge = (source, destination)
    val changed = if (up) links.union(Set(edge)) else links.exclude(Set(edge))
    ({ ...w, extensions: { ...w.extensions,
        network: if (channel == MetaChannel) { ...net, metaLinks: changed }
                 else { ...net, loadLinks: changed } } })
  }

  pure def crashEnabled(w: World, node: NodeName): bool = {
    val owner = w.system.get(node)
    and {
      M::running(owner)
    }
  }

  pure def crashNext(w: World, node: NodeName): World = {
    val owner = w.system.get(node)
    ({ ...w,
        system: w.system.put(node, { ...owner, volatile: { ...owner.volatile,
          phase: Stopped, pendingCalls: Map(), owners: Map(), observations: Map(), operationWork: Map(), waitingEffects: Set(), authorityReads: Map(), holds: Set(), starts: Map(), fenceWorkers: Set(), metaRunning: false, stopTimedOut: false, ownLease: Revoked, leases: Map(), renewal: None, renewalExpired: false, revokeGate: None,
          session: None, schedulerCapture: None, appliedMetaIndex: 0, appliedMetaVersion: 0,
          localView: { latest: I::emptyDocument, target: I::emptyDocument } } }),
        messages: w.messages.put(node, Set()),
      })
  }

  pure def restartEnabled(w: World, node: NodeName, MAX_INCARNATION: int): bool = {
    val owner = w.system.get(node)
    val view = match owner.durable.metaImage {
      | Some(image) => image
      | None => { latest: I::emptyDocument, target: I::emptyDocument }
    }
    and {
      owner.volatile.phase == Stopped,
      owner.volatile.incarnation < MAX_INCARNATION
    }
  }

  // TODO(not planned): application business semantics. A bounded committed
  // position witnesses independent consensus, not a business command result.

  pure def restartNext(w: World, node: NodeName, MAX_INCARNATION: int): World = {
    val owner = w.system.get(node)
    val view = match owner.durable.metaImage {
      | Some(image) => image
      | None => { latest: I::emptyDocument, target: I::emptyDocument }
    }
    ({ ...w, system: w.system.put(node, { ...owner,
        volatile: { ...owner.volatile, phase: Starting, metaRunning: owner.durable.pin != None, stopTimedOut: false,
          owners: Map(), starts: Map(), holds: Set(), session: None, ownLease: Revoked,
          renewal: None, renewalExpired: false, leases: Map(), revokeGate: None,
          fenceWorkers: Set(), authorityReads: Map(), waitingEffects: Set(), pendingCalls: Map(),
          observations: Map(), operationWork: Map(),
          incarnation: owner.volatile.incarnation + 1,
          localView: view,
          appliedMetaIndex: owner.durable.metaIndex,
          appliedMetaVersion: view.latest.version } }) })
  }

  // TODO(not planned): application business semantics. A bounded committed
  // position witnesses independent consensus, not a business command result.
}
