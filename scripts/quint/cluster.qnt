module cluster {
  import types.* from "./types"
  import handlers.* from "./handlers"
  import basicSpells.* from "./vendor/spells/basicSpells"
  import choreo as C from "./vendor/choreo"

  const NODE_POOL: Set[NodeName]
  const MAX_REQUESTS: int
  var world: World
  pure val modelBounds = Map("nodeCount" -> NODE_POOL.size(), "maxRequests" -> MAX_REQUESTS)

  pure val emptyDocument: Document = {
    cluster: "c1", version: 0, parent: 0, rebasedFrom: None, nodes: Map(), groups: Map(),
    placement: { replicas: 3, allowedTypes: Set("worker"), selector: Map(), voterDomain: None },
    audit: { actor: "", comment: "" },
  }

  pure val emptyMetadata: MetadataState = {
    latest: emptyDocument, target: emptyDocument, history: Map(), groups: Map(), nodes: Map(),
    operations: Map(), firstAttempts: Map(), successors: Map(), grants: Map(), revocations: Map(),
    cleanup: Map(), memberAllocationFloor: 1, replicaAllocationFloors: Map(),
    retiredGroupIds: Set(), sessions: Map(), locks: Map(),
  }

  action init = world' = {
    system: NODE_POOL.mapBy(n => {
      process_id: n,
      volatile: {
        phase: New, incarnation: 1, pendingCalls: Map(), appliedMetaVersion: 0,
        appliedMetaIndex: 0, localView: { latest: emptyDocument, target: emptyDocument },
        observations: Map(), owners: Map(), session: None, schedulerEnabled: true,
        serveOverride: 0, schedulerCapture: None,
      },
      durable: { pin: None, replicaEvidence: Map(), authorityFloors: Map(), receipts: Set(),
                 fences: Set(), collectionMarkers: Map() },
    }),
    messages: NODE_POOL.mapBy(_ => Set()),
    events: NODE_POOL.mapBy(_ => Set()),
    extensions: {
      network: { metaLinks: NODE_POOL.map(a => NODE_POOL.map(b => (a, b))).flatten(),
                 loadLinks: Set(), pending: Set(), delivered: Set() },
      nextDelivery: 1,
      metadata: emptyMetadata, consensus: Map(), recovery: Map(),
      storageConfig: NODE_POOL.mapBy(_ => { defaultBackend: OwnedWAL }),
    },
  }

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
    world.extensions.network.metaLinks.contains((msg.source.node, msg.destination)),
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
    world' = C::apply_effect(world, node, tr, apply_custom_effect)
  }

  action step = any {
    nondet node = NODE_POOL.oneOf()
    nondet request = Set(ObserveLocal, InvalidRequest).oneOf()
    invoke(node, request),
    nondet msg = world.extensions.network.pending.oneOf()
    deliver(msg),
    nondet node = NODE_POOL.oneOf()
    handle(node),
  }
}
