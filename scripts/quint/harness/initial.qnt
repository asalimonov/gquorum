module initial {
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"

  pure val emptyDocument: Document = {
    cluster: "c1", version: 0, parent: 0, rebasedFrom: None, nodes: Map(), groups: Map(),
    placement: { minVoters: 1, maxVoters: 3, allowedTypes: Set("worker"), selector: Map() },
    audit: { actor: "", comment: "", forced: None },
  }

  pure def replica(founders: NodeName -> int, group: GroupId, node: NodeName): ReplicaKey =
    { cluster: "c1", group: group, node: node, member: founders.get(node), replica: founders.get(node) }

  pure def declarations(founders: NodeName -> int, loads: Set[GroupId]): Document =
    if (founders.keys().size() == 0) emptyDocument else {
      val groups = loads.union(Set("_meta"))
      { ...emptyDocument, version: 1, audit: { actor: "founders", comment: "established fixture", forced: None },
        nodes: founders.keys().mapBy(n => { name: n, member: founders.get(n), state: Active,
                                            nodeType: "worker", labels: Map() }),
        groups: groups.mapBy(g => {
          id: g, storage: StorageDefault, labels: Map(), placement: None,
          hosting: None, voterConfig: None,
          nextReplica: founders.values().fold(1, (acc, id) => max(acc, id + 1)),
          replicas: founders.keys().map(n => {
            key: replica(founders, g, n), role: Voter, pinnedLearner: false }),
        }),
      }
    }

  pure def metadata(doc: Document): MetadataState = {
    latest: doc, target: doc, history: Map(doc.version -> doc),
    groups: doc.groups.keys().mapBy(g => {
      sourceVersion: doc.version, proven: Some(doc.groups.get(g)), owner: None, deleting: false }),
    nodes: doc.nodes.keys().mapBy(n => {
      sourceVersion: doc.version, proven: Some(doc.nodes.get(n)), admission: None }),
    operations: Map(), firstAttempts: Map(), successors: Map(), grants: Map(), revocations: Map(),
    cleanup: Map(), memberAllocationFloor: doc.nodes.values().fold(1, (acc, n) => max(acc, n.member + 1)),
    replicaAllocationFloors: doc.groups.keys().mapBy(g =>
      doc.groups.get(g).replicas.fold(1, (acc, r) => max(acc, r.key.replica + 1))),
    retiredGroupIds: Set(),
    sessions: doc.nodes.keys().map(n => { owner: { node: n, incarnation: 1 }, generation: 1 }).mapBy(key =>
      { owner: key.owner, member: doc.nodes.get(key.owner.node).member, phase: Live, renewalGeneration: 0 }),
    activeSessions: doc.nodes.keys().mapBy(n => { owner: { node: n, incarnation: 1 }, generation: 1 }), locks: Map(), stagedIntents: Map(),
    couriers: doc.nodes.keys().mapBy(n => { process: { node: n, incarnation: 1 }, member: doc.nodes.get(n).member }),
    authorityFloors: Map(), closedGrants: Set(), effectReceipts: Set(), fenceEvidence: Set(),
  }

  pure def consensus(doc: Document, leader: NodeName): GroupId -> ConsensusState =
    doc.groups.keys().mapBy(g => {
      val replicas = doc.groups.get(g).replicas.map(r => r.key)
      {
        lineage: doc.cluster, configRevision: 1, voters: replicas, learners: Set(),
        leaderAuthority: Some(replicas.filter(r => r.node == leader).getOnlyElement()),
        committedPosition: 1, appliedPositions: replicas.mapBy(_ => 1),
        catchUp: replicas.mapBy(_ => CaughtUp), pendingChange: None,
        authorityFloor: 0, authorityReceipts: Set(), authorityFences: Set(),
      }
    })

  pure def initialNode(n: NodeName, doc: Document, native: GroupId -> ConsensusState): LocalNode = {
    val admitted = doc.nodes.keys().contains(n)
    val replicas = doc.groups.values().map(g => g.replicas.map(r => r.key)).flatten().filter(r => r.node == n)
    val loads = replicas.filter(r => r.group != "_meta")
    val view = { latest: doc, target: doc }
    {
      process_id: n,
      volatile: {
        phase: if (admitted) Ready else New, incarnation: 1, pendingCalls: Map(),
        appliedMetaVersion: if (admitted) doc.version else 0, appliedMetaIndex: if (admitted) 1 else 0,
        localView: if (admitted) view else { latest: emptyDocument, target: emptyDocument },
        observations: replicas.map(r => r.group).mapBy(g => {
          val r = replicas.filter(key => key.group == g).getOnlyElement()
          { group: g, replica: r, incarnation: 1, observedConfig: 1,
            observedLeader: native.get(g).leaderAuthority, health: Healthy, age: Fresh }
        }),
        owners: loads.map(r => r.group).mapBy(g => {
          val r = loads.filter(key => key.group == g).getOnlyElement()
          { key: r, phase: OwnerReady, held: false, reservation: None, replayReady: true,
            startupAuthorized: true, published: true, factoryOutstanding: false,
            engineJoined: false, engineRunning: true, applicationClose: Acquired,
            process: { node: n, incarnation: 1 }, waiters: Set(), callbackExpired: false, closeExpired: false,
            replayOutstanding: false, startDeclaration: Some(doc.groups.get(g)), authorityRevision: 1, scopeIndex: 1 }
        }),
        session: if (admitted) Some({ owner: { node: n, incarnation: 1 }, generation: 1 }) else None, schedulerEnabled: true, serveOverride: 0, schedulerCapture: None, operationWork: Map(), waitingEffects: Set(), authorityReads: Map(), holds: Set(), starts: Map(), fenceWorkers: Set(), metaRunning: admitted, stopTimedOut: false,
        ownLease: if (admitted) Live else Revoked, leases: Map(), renewal: None, renewalExpired: false, revokeGate: None,
      },
      durable: {
        pin: if (admitted) Some({ cluster: doc.cluster, node: n, member: doc.nodes.get(n).member }) else None,
        replicaEvidence: replicas.mapBy(_ => ValidStore({ snapshot: 0, commit: 1, tail: 1, appliedHint: 1 })),
        authorityFloors: Map(), authorityGrants: Map(), receipts: Set(), fences: Set(), journalPosition: 0, pendingFences: Map(), seedAuthority: loads, collectionMarkers: Map(),
        metaImage: if (admitted) Some(view) else None, metaIndex: if (admitted) 1 else 0,
      },
    }
  }

  // Established fixtures assume approved/proven declarations and complete replay;
  // fresh startup and group creation use their own transitions in Tasks 7–9.
  pure def worldFor(pool: Set[NodeName], founders: NodeName -> int,
                    loads: Set[GroupId], leader: NodeName): World = {
    val doc = declarations(founders, loads)
    val native = consensus(doc, leader)
    val meta = metadata(doc)
    val links = pool.map(a => pool.map(b => (a, b))).flatten()
    {
      system: pool.mapBy(n => initialNode(n, doc, native)),
      messages: pool.mapBy(_ => Set()), events: pool.mapBy(_ => Set()),
      extensions: {
        network: { metaLinks: links, loadLinks: links, pending: Set(), delivered: Set() },
        nextDelivery: 1, metadata: meta, consensus: native, recovery: Map(),
        storageConfig: pool.mapBy(_ => { defaultBackend: OwnedWAL }),
        consumptions: Set(),
        metaLog: Map(0 -> metadata(emptyDocument)).put((if (founders.keys().size() > 0) 1 else 0), meta),
      },
    }
  }
}
