module repair {
  import types.* from "../types"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import topology as T from "../topology/topology"
  import placement as P from "../topology/placement"
  import authority as A from "./authority"

  // Source: scope_independence.go and rebase.go. Labels/domain policies are
  // excluded by the active harness; node identity and placement state still
  // form dependencies even when the repaired and staged group IDs differ.
  type PlacementRead = { node: NodeName, member: int, canAdd: bool, canRetain: bool, canVote: bool }
  pure def placementReads(base: Document, edited: Document, nodes: Document, group: GroupId): Set[PlacementRead] = {
    val doc = { ...edited, nodes: nodes.nodes }
    val declaration = edited.groups.get(group)
    nodes.nodes.values().map(n => {
      node: n.name, member: n.member,
      canAdd: P::canAdd(doc, declaration, n),
      canRetain: P::canRetain(base, doc, declaration, n),
      canVote: P::canVote(base, doc, declaration, n),
    }).filter(r => r.canAdd or r.canVote or declaration.replicas.exists(p => p.key.node == r.node))
  }

  pure def independent(base: Document, staged: Document, repair: Document): bool = {
    val stagedGroups = T::affectedScopes(base, staged)
    val repairGroups = T::affectedScopes(base, repair)
    and {
      base.cluster == staged.cluster and base.cluster == repair.cluster,
      stagedGroups.intersect(repairGroups).size() == 0,
      T::placementScopes(base, repair).intersect(repairGroups).exclude(Set("_meta"))
        .intersect(repair.groups.keys()).forall(g =>
          base.placement == staged.placement and placementReads(base, repair, base, g) == placementReads(base, repair, staged, g)),
      T::placementScopes(base, staged).intersect(stagedGroups).exclude(Set("_meta"))
        .intersect(staged.groups.keys()).forall(g =>
          base.placement == repair.placement and placementReads(base, staged, base, g) == placementReads(base, staged, repair, g)),
    }
  }

  pure def semantic(doc: Document): Document = { ...doc, version: 0, parent: 0, rebasedFrom: None }

  pure def rebase(base: Document, staged: Document, repair: Document): Document = {
    val removedNodes = base.nodes.keys().exclude(staged.nodes.keys())
    val removedGroups = base.groups.keys().exclude(staged.groups.keys())
    val nodes = staged.nodes.keys().fold(removedNodes.fold(repair.nodes, (out, n) => out.mapRemove(n)), (out, n) =>
      if (base.nodes.keys().contains(n) and base.nodes.get(n) == staged.nodes.get(n)) out else out.put(n, staged.nodes.get(n)))
    val groups = staged.groups.keys().fold(removedGroups.fold(repair.groups, (out, g) => out.mapRemove(g)), (out, g) =>
      if (base.groups.keys().contains(g) and base.groups.get(g) == staged.groups.get(g)) out else out.put(g, staged.groups.get(g)))
    { ...repair, version: repair.version + 1, parent: repair.version, rebasedFrom: Some(staged.version),
      audit: staged.audit, nodes: nodes, groups: groups,
      placement: if (base.placement != staged.placement) staged.placement else repair.placement }
  }

  pure def latestAfter(meta: MetadataState, command: RepairCommand): Document =
    if (semantic(meta.target) == semantic(meta.latest)) command.approval.desired
    else rebase(meta.target, meta.latest, command.approval.desired)

  pure def errors(meta: MetadataState, command: RepairCommand, rules: Rules): Set[Refusal] = {
    val request = command.request
    if (request.options.expectedVersion != meta.latest.version) {
      match A::replayVersion(meta, request) { | Some(_) => Set() | None => Set(Conflict) }
    } else if (command.expectedTarget != meta.target.version) Set(Conflict)
    else if (request.options.preview) Set(Invalid)
    else match request.edit {
      | ReseatReplica(_) => {
        val input = A::mutationInput(meta, Map(), rules)
        val editErrors = T::mutationErrors(input, request)
        if (editErrors.size() > 0) editErrors else {
          val desired = T::mutateDocument(input, request)
          if (desired != command.approval.desired or command.approval.origin != Operator) Set(Unauthorized)
          else if (not(independent(meta.target, meta.latest, desired))) Set(Conflict)
          else {
            val staged = A::installStage(meta, { request: request, desired: desired })
            val latest = latestAfter(meta, command)
            val floorErrors = if (latest.groups.values().forall(g =>
              g.nextReplica >= meta.replicaAllocationFloors.getOrElse(g.id, 1))) Set() else Set(Invalid)
            A::approvalErrors(staged, command.approval, rules).union(floorErrors)
              .union(P::validateTransition(desired, latest, meta.retiredGroupIds, rules))
          }
        }
      }
      | _ => Set(Invalid)
    }
  }

  pure def canRepair(meta: MetadataState, command: RepairCommand, rules: Rules): bool =
    errors(meta, command, rules).size() == 0

  // Only the final record is published by one metadata commitment. The local
  // candidate values below do not expose ordinary staging/approval boundaries.
  pure def applyRepair(meta: MetadataState, command: RepairCommand): MetadataState =
    match A::replayVersion(meta, command.request) {
      | Some(_) => meta
      | None => {
        val staged = A::installStage(meta, { request: command.request, desired: command.approval.desired })
        val approved = A::installApproval(staged, command.approval)
        val latest = latestAfter(meta, command)
        { ...approved, latest: latest, history: approved.history.put(latest.version, latest),
          replicaAllocationFloors: latest.groups.values().fold(approved.replicaAllocationFloors, (floors, g) =>
            floors.put(g.id, max(floors.getOrElse(g.id, 1), g.nextReplica))) }
      }
    }
}
