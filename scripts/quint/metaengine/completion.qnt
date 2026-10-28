module completion {
  import readmission_base as Readmit from "./readmission_base"
  import types.* from "../types"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import topology as T from "../topology/topology"

  type MemberProof = { members: Set[{ key: ReplicaKey, role: ReplicaRole }], prepared: Set[ReplicaKey], valid: bool }
  pure def membershipKind(kind: ActionKind): bool =
    Set(PrepareReplica, CreateFoundingGroup, AddLearner, PromoteLearner, DemoteVoter, RemoveMember).contains(kind)

  pure def foldGrant(proof: MemberProof, grant: Grant, meta: MetadataState, desired: Document): MemberProof = {
    val effect = grant.effect.unwrap()
    val abandoned = effect.kind == PrepareReplica and match effect.target {
      | ReplicaTarget(key) => not(desired.groups.get(key.group).replicas.exists(r => r.key == key))
      | _ => false }
    val consumed = grant.resources.forall(resource => meta.effectReceipts.exists(r => r.grant == grant.key and r.resource == resource))
    if (abandoned) proof else if (not(consumed)) { ...proof, valid: false }
    else match effect.target {
      | GroupTarget(group) => {
          val founders = meta.history.get(effect.operation.version).groups.get(group).replicas.filter(r => r.role == Voter)
          { ...proof, members: proof.members.union(founders.map(r => { key: r.key, role: r.role })),
            prepared: proof.prepared.union(founders.map(r => r.key)) }
        }
      | ReplicaTarget(key) => match effect.kind {
          | PrepareReplica => { ...proof, prepared: proof.prepared.union(Set(key)) }
          | AddLearner => { ...proof, members: proof.members.filter(r => r.key != key).union(Set({ key: key, role: Learner })) }
          | PromoteLearner => { ...proof, members: proof.members.map(r => if (r.key == key) { ...r, role: Voter } else r) }
          | DemoteVoter => { ...proof, members: proof.members.map(r => if (r.key == key) { ...r, role: Learner } else r) }
          | RemoveMember => { ...proof, members: proof.members.filter(r => r.key != key) }
          | _ => proof
        }
    }
  }

  // scoped_approval_completion.go: a prospective checkpoint cannot prove its
  // own omitted work. Replay exact consumed effects after the retained proof.
  pure def zeroStepMembership(meta: MetadataState, desired: Document, group: GroupId): bool = {
    val row = meta.groups.getOrElse(group, { sourceVersion: 0, proven: None, owner: None, deleting: false })
    val replicas = match row.proven { | Some(g) => g.replicas | None => Set() }
    val start: MemberProof = { members: replicas.map(r => { key: r.key, role: r.role }),
      prepared: replicas.map(r => r.key), valid: true }
    val relevant = meta.grants.values().filter(g => g.key.group == group and match g.effect {
      | Some(e) => e.operation.version > row.sourceVersion and membershipKind(e.kind) | None => false })
    val ordered = relevant.fold(List(), (list, grant) =>
      list.select(g => g.key.revision < grant.key.revision).append(grant)
        .concat(list.select(g => g.key.revision >= grant.key.revision)))
    val result = ordered.foldl(start, (proof, grant) => foldGrant(proof, grant, meta, desired))
    val wanted = desired.groups.get(group).replicas.map(r => { key: r.key, role: r.role })
    result.valid and result.members == wanted and wanted.map(r => r.key).subseteq(result.prepared)
  }

  pure def namesNode(group: Option[GroupDecl], node: NodeName): bool = match group {
    | Some(g) => g.replicas.exists(r => r.key.node == node) | None => false }
  pure def targetGroup(meta: MetadataState, group: GroupId): Option[GroupDecl] =
    if (meta.target.groups.keys().contains(group)) Some(meta.target.groups.get(group)) else None
  pure def targetNode(meta: MetadataState, node: NodeName): Option[NodeDecl] =
    if (meta.target.nodes.keys().contains(node)) Some(meta.target.nodes.get(node)) else None

  pure def loadDependency(meta: MetadataState, row: NodeProgress, node: NodeName, group: GroupId): bool =
    match row.proven {
      | None => true
      | Some(beforeNode) => if (not(meta.target.nodes.keys().contains(node))) true else {
          val old = meta.groups.get(group).proven
          val desiredGroup = targetGroup(meta, group)
          val context = match desiredGroup { | Some(g) => g | None => old.unwrap() }
          val previous = match old { | Some(g) => g.replicas.filter(r => r.key.node == node) | None => Set() }
          val augmented = if (not(namesNode(desiredGroup, node))) { ...context, replicas: context.replicas.union(previous) } else context
          val after = { ...meta.target, groups: meta.target.groups.put(group, augmented) }
          val before = { ...after, nodes: after.nodes.put(node, beforeNode) }
          T::affectedScopes(before, after).contains(group)
        }
    }

  pure def nodeGroupReady(meta: MetadataState, group: GroupId, node: NodeName): bool = {
    val row = meta.groups.get(group)
    row.sourceVersion > 0 and row.owner == None and not(row.deleting) and row.proven == targetGroup(meta, group) and
      (not(namesNode(row.proven, node)) or (meta.history.keys().contains(row.sourceVersion) and
        meta.history.get(row.sourceVersion).nodes.get(node).member == meta.target.nodes.get(node).member))
  }

  pure def nodeReady(meta: MetadataState, row: NodeProgress, node: NodeName): bool = {
    val loads = meta.groups.keys().exclude(Set("_meta")).filter(g =>
      namesNode(meta.groups.get(g).proven, node) or namesNode(targetGroup(meta, g), node))
    loads.forall(g => not(loadDependency(meta, row, node, g)) or nodeGroupReady(meta, g, node))
  }

  // scoped_node_progress.go: current _meta proof plus only the node's relevant
  // load dependencies. Detached physical cleanup does not block native exit.
  pure def advanceNodes(meta: MetadataState): MetadataState =
    if (not(meta.groups.keys().contains("_meta"))) meta else {
      val native = meta.groups.get("_meta")
      val metaReady = native.owner == None and native.sourceVersion > 0 and native.proven == targetGroup(meta, "_meta")
      val rows = meta.nodes.keys().union(meta.target.nodes.keys()).mapBy(node => {
        val row = meta.nodes.getOrElse(node, { sourceVersion: 0, proven: None, admission: None })
        if (row.sourceVersion > 0 and row.proven == targetNode(meta, node)) row
        else if (metaReady and nodeReady(meta, row, node)) { ...row, sourceVersion: meta.target.version, proven: targetNode(meta, node) }
        else row
      })
      { ...meta, nodes: rows }
    }

  pure def metaEvacuated(meta: MetadataState, key: ReplicaKey): bool = {
    val loads = meta.groups.keys().exclude(Set("_meta"))
    val retained = meta.history.values().filter(d => d.version == 1 or meta.firstAttempts.keys().contains(d.version))
      .map(d => d.groups.values().map(g => g.replicas.map(r => r.key)).flatten()).flatten()
      .filter(r => r.group != "_meta" and r.node == key.node and r.member == key.member)
    val successors = meta.target.groups.get("_meta").replicas.filter(r => r.key.node == key.node)
    if (successors.size() > 0) successors.forall(r => r.key.member > key.member and
      Readmit::consumed(meta, { node: key.node, predecessor: key.member, successor: r.key.member })) else
      loads.forall(g => not(namesNode(meta.groups.get(g).proven, key.node)) and not(namesNode(targetGroup(meta, g), key.node))) and
      retained.forall(r => meta.effectReceipts.exists(receipt => receipt.target == Some(r) and
        match receipt.effect { | Some(e) => Set(StopReplica, RetireReplica, DeleteGroupState).contains(e.kind)
          | None => match receipt.admission { | Some(a) => Readmit::consumed(meta, a) | None => false } }))
  }
}
