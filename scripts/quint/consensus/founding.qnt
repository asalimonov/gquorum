module founding {
  import types.* from "../types"
  import world_types.* from "../runtime/world"
  import basicSpells.* from "../vendor/spells/basicSpells"

  pure def isFounding(grant: Grant): bool = grant.purpose == Effect and grant.key.group != "_meta" and
    grant.targets.size() > 0 and (match grant.effect {
      | Some(e) => e.kind == CreateFoundingGroup and e.target == GroupTarget(grant.key.group)
      | None => false }) and (match grant.planStep {
      | Some(step) => step.expectedVoters == Set() and step.resultVoters == grant.targets
      | None => false })

  // Initial authority is restricted to exact approved fresh founding peers.
  // This initializes configuration, not a leader, quorum commit or completion.
  pure def initial(grant: Grant): ConsensusState = {
    lineage: grant.key.cluster, configRevision: 1, voters: grant.targets, learners: Set(),
    leaderAuthority: None, committedPosition: 0,
    appliedPositions: grant.targets.mapBy(_ => 0), catchUp: grant.targets.mapBy(_ => Behind), pendingChange: None,
    authorityFloor: grant.key.revision, authorityReceipts: Set(), authorityFences: Set(),
  }

  pure def engineStarted(w: World, node: NodeName, group: GroupId): World = {
    val host = w.system.get(node)
    val resource = HostResource({ group: group, node: node })
    if (w.extensions.consensus.keys().contains(group) or not(host.durable.authorityGrants.keys().contains(resource))) w else {
      val grant = host.durable.authorityGrants.get(resource)
      if (not(isFounding(grant)) or not(host.volatile.owners.get(group).engineRunning)) w else
        { ...w, extensions: { ...w.extensions, consensus: w.extensions.consensus.put(group, initial(grant)) } }
    }
  }
}
