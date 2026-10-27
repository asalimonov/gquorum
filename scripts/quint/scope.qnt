module scope {
  import types.* from "./types"
  import basicSpells.* from "./vendor/spells/basicSpells"
  import topology as T from "./topology"

  // TODO(not planned): labels, policy tuning, performance faults and offline recovery.
  // These are harness assumptions, not errors returned by the real library.
  pure val goodMetrics = {
    lagWithinLimit: true, backlogWithinLimit: true, progressing: true,
    current: true, standbyStable: true, cooldownClear: true,
  }
  pure val placementPreset: PlacementRule = {
    minVoters: 1, maxVoters: 3, allowedTypes: Set("worker"), selector: Map(),
  }

  pure def groupInScope(group: GroupDecl): bool = and {
    group.labels.keys().size() == 0, group.storage == StorageDefault, group.placement == None,
    group.voterConfig == None or group.voterConfig == Some(T::defaultVoterConfig),
    group.replicas.forall(r => not(r.pinnedLearner) or (group.voterConfig != None and r.role == Learner)),
  }

  pure def documentInScope(doc: Document): bool = and {
    doc.cluster == "c1", doc.placement == placementPreset,
    doc.nodes.values().forall(n => n.nodeType == "worker" and n.labels.keys().size() == 0),
    doc.groups.values().forall(g => groupInScope(g)),
  }

  pure def stageInScope(before: Document, desired: Document): bool = documentInScope(desired) and
    desired.groups.keys().exclude(before.groups.keys()).forall(id => {
      val group = desired.groups.get(id)
      group.replicas.forall(r => not(r.pinnedLearner))
    }) and before.groups.keys().intersect(desired.groups.keys()).forall(id => {
      val old = before.groups.get(id)
      val updated = desired.groups.get(id)
      old.voterConfig == updated.voterConfig and old.placement == updated.placement and
        updated.replicas.forall(r => if (not(old.replicas.exists(p => p.key == r.key)))
          not(r.pinnedLearner)
          else old.replicas.forall(p => if (p.key != r.key) true
            else if (p.role == r.role) p.pinnedLearner == r.pinnedLearner
            else r.pinnedLearner == (updated.voterConfig != None and r.role == Learner)))
    })

  pure def requestInScope(before: Document, request: ApiRequest): bool = match request {
    | Operate(op) => match op {
        | RepairRequest(_) => false // Internal continuation, not a public invocation shortcut.
        | _ => true
      }
    | Mutate(m) => match m.edit {
      | StageDocument(doc) => stageInScope(before, doc) and not(m.options.preview) and not(m.options.approve)
      | AddNode(n) => n.labels.keys().size() == 0 and n.nodeType == "worker"
      | CreateGroup(g) => g.labels.keys().size() == 0 and g.storage == StorageDefault and
          g.placement == None and (g.voterConfig == None or g.voterConfig == Some(T::defaultVoterConfig))
      | SetPlacement(_) => false | SetGroupPlacement(_) => false
      | SetGroupVoterConfig(_) => false | SetReplicaPinnedLearner(_) => false | RollbackTo(_) => false
      | _ => true
    }
    | _ => true
  }
}
