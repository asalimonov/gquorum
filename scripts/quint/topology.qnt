module topology {
  import types.* from "./types"
  import basicSpells.* from "./vendor/spells/basicSpells"
  import placement as P from "./placement"
  import planning as Plans from "./planning"

  pure val defaultVoterConfig: VoterConfig = {
    target: 3, domain: None, maxSurge: 1, failureWindow: 30, standbyWindow: 60,
    failureLag: 1000, standbyLag: 100, standbyApplyBacklog: 100, replaceNoProgress: true,
    noProgressWindow: 30, maxObservationGap: 15, switchCooldown: 300, demotionCooldown: 300,
    allowNewFailureDuringCooldown: true,
  }

  pure def validateDocument(doc: Document, rules: Rules): Set[Refusal] = P::validateDocument(doc, rules)
  pure def eligibleHosts(doc: Document, group: GroupDecl): Set[NodeName] = P::eligibleHosts(doc, group)
  pure def makePlan(input: PlanningInput, desired: Document): PlanResult =
    Plans::buildPlan(input, desired, affectedScopes(input.approved, desired))

  pure def affectedScopes(before: Document, after: Document): Set[GroupId] = {
    val groups = before.groups.keys().union(after.groups.keys()).filter(g =>
      if (not(before.groups.keys().contains(g)) or not(after.groups.keys().contains(g))) true
      else if (before.groups.get(g) != after.groups.get(g)) true
      else if (g == "_meta") false else {
        val old = before.groups.get(g)
        val updated = after.groups.get(g)
        P::effective(before, old) != P::effective(after, updated) or old.replicas.exists(r =>
          if (not(before.nodes.keys().contains(r.key.node)) or not(after.nodes.keys().contains(r.key.node))) true else {
            val a = before.nodes.get(r.key.node)
            val b = after.nodes.get(r.key.node)
            or { a.state != b.state, a.member != b.member,
              P::canRetain(before, before, old, a) != P::canRetain(before, after, updated, b),
              P::canAdd(before, old, a) != P::canAdd(after, updated, b),
              P::canVote(before, before, old, a) != P::canVote(before, after, updated, b),
              P::domain(old, a) != P::domain(updated, b) }
          })
      })
    if (before.nodes != after.nodes) groups.union(Set("_meta")) else groups
  }

  pure def hasLoad(doc: Document, name: NodeName): bool = doc.groups.values().exists(g =>
    g.id != "_meta" and g.replicas.exists(r => r.key.node == name))

  pure def unownedProven(input: MutationInput, group: GroupId): bool =
    if (not(input.groups.keys().contains(group)) or not(input.view.target.groups.keys().contains(group))) false
    else { val p = input.groups.get(group)
      p.owner == None and not(p.deleting) and p.sourceVersion > 0 and
        p.proven == Some(input.view.target.groups.get(group)) }

  pure def editErrors(input: MutationInput, edit: DocumentEdit): Set[Refusal] = {
    val doc = input.view.latest
    match edit {
      | StageDocument(_) => Set()
      | AddNode(n) => if (n.name == "" or n.nodeType == "" or
          (if (doc.nodes.keys().contains(n.name)) doc.nodes.get(n.name).state != Excluded else false)) Set(Invalid) else Set()
      | ReadmitNode(r) =>
        if (r.expectedMember <= 0) Set(Invalid)
        else if (not(doc.nodes.keys().contains(r.name))) Set(NotFound)
        else if (doc.nodes.get(r.name).member != r.expectedMember) Set(Conflict)
        else if (doc.nodes.get(r.name).state != Active) Set(Invalid)
        else if (not(doc.groups.values().filter(g => g.replicas.exists(p => p.key.node == r.name))
          .forall(g => unownedProven(input, g.id)))) Set(Unavailable)
        else match input.health.getOrElse(r.name, HealthUnknown) {
          | ConfirmedDead => Set()
          | Healthy => Set(UnsafeMembership)
          | _ => Set(Unavailable)
        }
      | DrainNode(n) => if (not(doc.nodes.keys().contains(n))) Set(NotFound)
          else if (doc.nodes.get(n).state != Active) Set(Invalid) else Set()
      | ActivateNode(n) => if (not(doc.nodes.keys().contains(n))) Set(NotFound)
          else if (doc.nodes.get(n).state == Excluded) Set(Invalid) else Set()
      | ExcludeNode(n) => if (not(doc.nodes.keys().contains(n))) Set(NotFound) else {
          val current = doc.nodes.get(n)
          val proven = if (input.nodes.keys().contains(n)) input.nodes.get(n).proven else None
          val ready = match proven {
            | Some(old) => (old.state == Draining or old.state == Drained) and
                (current.state == Draining or current.state == Drained) and not(hasLoad(doc, n))
            | None => not(input.view.target.nodes.keys().contains(n)) or input.view.target.nodes.get(n).state == Excluded
          }
          if (ready and current.state != Excluded) Set() else Set(Invalid)
        }
      | ForceRetireNode(r) => if (r.reason == "") Set(Invalid)
          else if (not(doc.nodes.keys().contains(r.name))) Set(NotFound)
          else if (doc.nodes.get(r.name).state == Excluded) Set(Invalid) else Set()
      | SetPlacement(rule) => if (doc.groups.values().exists(g => g.id != "_meta" and
          g.placement == None and g.voterConfig == None and P::voters(g).size() > rule.maxVoters)) Set(Invalid) else Set()
      | SetGroupPlacement(r) => if (r.group == "_meta") Set(Invalid)
          else if (doc.groups.keys().contains(r.group)) Set() else Set(NotFound)
      | SetGroupVoterConfig(r) => if (doc.groups.keys().contains(r.group)) Set() else Set(NotFound)
      | SetGroupHosting(r) => if (r.group == "_meta") Set(Invalid)
          else if (not(doc.groups.keys().contains(r.group))) Set(NotFound)
          else if (doc.groups.get(r.group).voterConfig == None) Set(Invalid)
          else if (r.hosting.selectedNodes.length() != P::toSet(r.hosting.selectedNodes).size()) Set(Invalid)
          else if (not(P::toSet(r.hosting.selectedNodes).subseteq(doc.nodes.keys()))) Set(NotFound)
          else {
            val group = doc.groups.get(r.group)
            val oldHosts = match group.hosting { | Some(h) => P::toSet(h.selectedNodes) | None => Set() }
            if (P::toSet(r.hosting.selectedNodes).exclude(oldHosts).forall(n =>
              doc.nodes.get(n).state == Active and P::allows(P::effective(doc, group), doc.nodes.get(n)))) Set()
            else Set(Invalid)
          }
      | SetReplicaPinnedLearner(r) => if (not(doc.groups.keys().contains(r.key.group))) Set(NotFound)
          else if (doc.groups.get(r.key.group).voterConfig == None) Set(Invalid)
          else if (doc.groups.get(r.key.group).replicas.exists(p => p.key == r.key)) Set() else Set(IdentityMismatch)
      | CreateGroup(spec) => and {
          spec.id != "", spec.id != "_meta", not(doc.groups.keys().contains(spec.id)),
          not(input.retired.contains(spec.id)), not(spec.nodes != None and spec.hosting != None),
          match spec.nodes { | Some(ns) => ns.length() == P::toSet(ns).size() | None => true },
          match spec.hosting { | Some(h) => h.selectedNodes.length() == P::toSet(h.selectedNodes).size() | None => true },
          match spec.nodes { | Some(ns) => P::toSet(ns).subseteq(doc.nodes.keys()) | None => true },
          match spec.hosting { | Some(h) => P::toSet(h.selectedNodes).subseteq(doc.nodes.keys()) | None => true },
        }.ite(Set(), Set(Invalid))
      | DeleteGroup(id) => if (id == "_meta") Set(Invalid)
          else if (doc.groups.keys().contains(id)) Set() else Set(NotFound)
      | ChangeReplica(r) => if (not(doc.groups.keys().contains(r.group))) Set(NotFound)
          else if (doc.groups.get(r.group).replicas.exists(p => p.key.node == r.node)) Set() else Set(NotFound)
      | ReseatReplica(key) => if (key.group == "_meta" or key.replica <= 0) Set(Invalid)
          else if (not(unownedProven(input, key.group))) Set(Conflict)
          else if (input.view.target.groups.get(key.group).replicas.exists(r => r.key == key and r.role == Voter)) Set()
          else Set(Unauthorized)
      | RollbackTo(version) => if (version == doc.version) Set(Invalid)
          else if (input.history.keys().contains(version)) Set() else Set(NotFound)
    }
  }

  pure def withNodeState(doc: Document, name: NodeName, state: NodePhase): Document =
    { ...doc, nodes: doc.nodes.setBy(name, n => { ...n, state: state }) }

  pure def removeNodeReplicas(doc: Document, name: NodeName): Document = {
    ...doc, groups: doc.groups.transformValues(g =>
      { ...g, replicas: g.replicas.filter(r => r.key.node != name) }),
  }

  pure def normalizeHosting(h: HostingRule, rules: Rules): HostingRule =
    { ...h, selectedNodes: rules.nodeOrder.select(n => P::toSet(h.selectedNodes).contains(n)) }

  pure def makeGroup(spec: GroupSpec, rules: Rules): GroupDecl = {
    val config = match spec.voterConfig { | Some(c) => c | None => defaultVoterConfig }
    val declared = match spec.hosting {
      | Some(h) => h
      | None => match spec.nodes {
        | Some(ns) => if (ns.length() > 0) { mode: ExplicitHosts, selectedNodes: ns }
            else { mode: AllEligible, selectedNodes: List() }
        | None => { mode: AllEligible, selectedNodes: List() }
      }
    }
    { id: spec.id, storage: spec.storage, labels: spec.labels, replicas: Set(),
      nextReplica: 1, placement: spec.placement, hosting: Some(normalizeHosting(declared, rules)),
      voterConfig: Some(config) }
  }

  pure def rawEdit(input: MutationInput, edit: DocumentEdit): Document = {
    val doc = input.view.latest
    match edit {
      | StageDocument(candidate) => candidate
      | AddNode(n) => {
          val meta = doc.groups.get("_meta")
          val id = max(input.memberFloor, meta.nextReplica)
          val node = { ...n, member: id, state: Active }
          val role = if (meta.voterConfig != None or P::learnerOnly(node)) Learner else Voter
          val updated = { ...meta, nextReplica: id + 1, replicas: meta.replicas.filter(r => r.key.node != n.name).union(
            Set({ key: { cluster: doc.cluster, group: "_meta", node: n.name, member: id, replica: id },
                  role: role, pinnedLearner: false })) }
          { ...doc, nodes: doc.nodes.put(n.name, node), groups: doc.groups.put("_meta", updated) }
        }
      | ReadmitNode(r) => {
          val member = max(input.memberFloor, doc.groups.get("_meta").nextReplica)
          { ...doc, nodes: doc.nodes.setBy(r.name, n => { ...n, member: member }),
            groups: doc.groups.transformValues(g => if (g.replicas.exists(p => p.key.node == r.name)) {
              val id = if (g.id == "_meta") member else max(g.nextReplica, input.replicaFloors.getOrElse(g.id, 1))
              { ...g, nextReplica: id + 1, replicas: g.replicas.map(p => if (p.key.node == r.name)
                { ...p, key: { ...p.key, member: member, replica: id } } else p) }
            } else g) }
        }
      | DrainNode(n) => withNodeState(doc, n, Draining)
      | ActivateNode(n) => withNodeState(doc, n, Active)
      | ExcludeNode(n) => removeNodeReplicas(withNodeState(doc, n, Excluded), n)
      | ForceRetireNode(r) => withNodeState(doc, r.name, Draining)
      | SetPlacement(rule) => { ...doc, placement: rule }
      | SetGroupPlacement(r) => { ...doc, groups: doc.groups.setBy(r.group, g => { ...g, placement: Some(r.rule) }) }
      | SetGroupVoterConfig(r) => { ...doc, groups: doc.groups.setBy(r.group, g => {
          val adopting = g.voterConfig == None
          { ...g, voterConfig: Some(r.config),
            hosting: if (adopting and g.id != "_meta") Some({ mode: ExplicitHosts,
              selectedNodes: input.rules.nodeOrder.select(n => g.replicas.exists(p => p.key.node == n)) }) else g.hosting,
            replicas: g.replicas.map(p => if (adopting and p.role == Learner) { ...p, pinnedLearner: true } else p) }
        }) }
      | SetGroupHosting(r) => { ...doc, groups: doc.groups.setBy(r.group, g =>
          { ...g, hosting: Some(normalizeHosting(r.hosting, input.rules)) }) }
      | SetReplicaPinnedLearner(r) => { ...doc, groups: doc.groups.setBy(r.key.group, g =>
          { ...g, replicas: g.replicas.map(p => if (p.key == r.key)
            { ...p, pinnedLearner: r.pinned, role: if (r.pinned) Learner else p.role } else p) }) }
      | CreateGroup(spec) => { ...doc, groups: doc.groups.put(spec.id, makeGroup(spec, input.rules)) }
      | DeleteGroup(id) => { ...doc, groups: doc.groups.mapRemove(id) }
      | ChangeReplica(r) => { ...doc, groups: doc.groups.setBy(r.group, g => {
          ...g, replicas: g.replicas.map(p => if (p.key.node == r.node)
            { ...p, role: r.role, pinnedLearner: g.voterConfig != None and r.role == Learner } else p),
        }) }
      | ReseatReplica(key) => {
          val approved = input.view.target
          val group = approved.groups.get(key.group)
          val id = max(max(group.nextReplica, input.replicaFloors.getOrElse(key.group, 1)),
            if (doc.groups.keys().contains(key.group)) doc.groups.get(key.group).nextReplica else 1)
          { ...approved, groups: approved.groups.put(key.group, { ...group, nextReplica: id + 1,
            replicas: group.replicas.map(p => if (p.key == key) { ...p, key: { ...p.key, replica: id } } else p) }) }
        }
      | RollbackTo(version) => {
          val old = input.history.get(version)
          { ...old, groups: old.groups.transformValues(g => { ...g,
            nextReplica: max(max(g.nextReplica, input.replicaFloors.getOrElse(g.id, 1)),
              if (doc.groups.keys().contains(g.id)) doc.groups.get(g.id).nextReplica else 1) }) }
        }
    }
  }

  pure def preparesPlacement(edit: DocumentEdit): bool = match edit {
    | AddNode(_) => true | DrainNode(_) => true | ActivateNode(_) => true
    | ForceRetireNode(_) => true | SetPlacement(_) => true | SetGroupPlacement(_) => true
    | SetGroupVoterConfig(_) => true | SetGroupHosting(_) => true | CreateGroup(_) => true
    | _ => false
  }

  pure def preparesRequest(input: MutationInput, request: MutationRequest): bool =
    preparesPlacement(request.edit) or match request.edit {
      | RollbackTo(_) => {
        val before = input.view.latest
        val candidate = rawEdit(input, request.edit)
        before.groups.keys() == candidate.groups.keys() and candidate.groups.keys().forall(id =>
          candidate.groups.get(id).nextReplica == before.groups.get(id).nextReplica and
          candidate.groups.get(id).replicas == before.groups.get(id).replicas)
      }
      | _ => false
    }

  pure def placementScopes(before: Document, after: Document): Set[GroupId] =
    before.groups.keys().union(after.groups.keys()).filter(id =>
      if (not(before.groups.keys().contains(id)) or not(after.groups.keys().contains(id))) true else {
        val a = before.groups.get(id)
        val b = after.groups.get(id)
        or {
          a.replicas != b.replicas, a.hosting != b.hosting, a.voterConfig != b.voterConfig,
          P::effective(before, a) != P::effective(after, b),
          before.nodes.keys() != after.nodes.keys(),
          before.nodes.keys().intersect(after.nodes.keys()).exists(n => {
            val old = before.nodes.get(n)
            val updated = after.nodes.get(n)
            or {
              P::canAdd(before, a, old) != P::canAdd(after, b, updated),
              P::canRetain(before, before, a, old) != P::canRetain(before, after, b, updated),
              P::canVote(before, before, a, old) != P::canVote(before, after, b, updated),
              P::domain(a, old) != P::domain(b, updated),
            }
          }),
        }
      })

  pure def mutateDocument(input: MutationInput, request: MutationRequest): Document = {
    val edited = rawEdit(input, request.edit)
    val scopes = placementScopes(input.view.latest, edited)
    val materialized = if (preparesRequest(input, request)) {
      val raised = { ...edited, groups: edited.groups.transformValues(g =>
        { ...g, nextReplica: max(g.nextReplica, input.replicaFloors.getOrElse(g.id, 1)) }) }
      input.rules.groupOrder.foldl(raised, (doc, id) => if (not(doc.groups.keys().contains(id)) or not(scopes.contains(id))) doc else {
        val prior = doc.groups.get(id)
        val hosted = if (id == "_meta") prior else P::materialize(input.view.target, doc, prior, input.rules)
        val selected = if (P::needsSeats(input.view.latest, doc, hosted))
          P::selectSeats(input.view.latest, doc, hosted, input.rules) else hosted
        { ...doc, groups: doc.groups.put(id, selected) }
      })
    } else edited
    val finalDoc = match request.edit {
      | ForceRetireNode(r) => removeNodeReplicas(withNodeState(materialized, r.name, Excluded), r.name)
      | _ => materialized
    }
    val parent = match request.edit {
      | StageDocument(d) => d.parent
      | RollbackTo(v) => v
      | ReseatReplica(_) => input.view.target.version
      | _ => input.view.latest.version
    }
    val audit = match request.edit {
      | StageDocument(d) => d.audit
      | ForceRetireNode(r) => { ...request.options.audit, forced: Some({ node: r.name, reason: r.reason }) }
      | _ => { ...request.options.audit, forced: None }
    }
    { ...finalDoc, version: input.view.latest.version + 1, parent: parent, rebasedFrom: None, audit: audit }
  }

  pure def mutationErrors(input: MutationInput, request: MutationRequest): Set[Refusal] = {
    val opts = request.options
    if (opts.preview and opts.approve) Set(Invalid)
    else if (opts.expectedVersion != input.view.latest.version) Set(Conflict)
    else if (not(input.view.latest.groups.keys().contains("_meta"))) Set(Unavailable)
    else {
      val errors = editErrors(input, request.edit)
      if (errors.size() > 0) errors else {
        val changed = mutateDocument(input, request)
        val scopes = placementScopes(input.view.latest, rawEdit(input, request.edit))
        val selected = if (preparesRequest(input, request)) changed.groups.values().filter(g => scopes.contains(g.id))
          .fold(Set(), (es, g) => es.union(P::selectionErrors(changed, g))) else Set()
        val placementValid = changed.groups.values().forall(g => g.replicas.forall(r =>
          if (not(changed.nodes.keys().contains(r.key.node))) false else
            P::canRetain(input.view.target, changed, g, changed.nodes.get(r.key.node)) and
              (r.role != Voter or P::canVote(input.view.target, changed, g, changed.nodes.get(r.key.node)))))
        val forcedValid = match changed.audit.forced {
          | Some(reason) => reason.node != "" and reason.reason != "" and changed.audit.actor != ""
          | None => true
        }
        val repairGroup = match request.edit { | ReseatReplica(key) => Some(key.group) | _ => None }
        val inventory = if (repairGroup != None) input.view.target else retainedIdentities(input.view)
        val floorsValid = changed.groups.values().forall(g => {
          val floor = input.replicaFloors.getOrElse(g.id, 1)
          (g.nextReplica >= floor or (repairGroup != None and repairGroup != Some(g.id))) and
            g.replicas.forall(r => r.key.replica >= floor or
            (if (inventory.groups.keys().contains(g.id)) inventory.groups.get(g.id).replicas.exists(old =>
              old.key.node == r.key.node and old.key.replica == r.key.replica) else false))
        })
        P::validateTransition(inventory, changed, input.retired, input.rules).union(selected)
          .union(if (placementValid and forcedValid and floorsValid and changed.parent < changed.version) Set() else Set(Invalid))
      }
    }
  }

  // metaengine.retainedIdentities: Target supplements staged removals, but
  // never replaces a staged node/replica identity that already occupies the slot.
  pure def retainedIdentities(view: ReadView): Document = {
    ...view.latest, groups: view.target.groups.values().fold(view.latest.groups, (groups, old) =>
      if (not(groups.keys().contains(old.id))) groups.put(old.id, old) else {
        val current = groups.get(old.id)
        val kept = old.replicas.fold(current.replicas, (replicas, r) =>
          if (replicas.exists(p => p.key.node == r.key.node or p.key.replica == r.key.replica)) replicas
          else replicas.union(Set(r)))
        groups.put(old.id, { ...current, replicas: kept })
      }),
  }

  pure def mutationGuard(input: MutationInput, request: MutationRequest): bool =
    mutationErrors(input, request).size() == 0

  // A convenience mutation Preview returns placement/document analysis without
  // committing. Plan/PlanFor use makePlan separately (their native reads can fail).
  pure def preview(input: MutationInput, request: MutationRequest): PreviewResult = {
    val errors = mutationErrors(input, request)
    if (errors.size() == 0) { document: Some(mutateDocument(input, request)), plan: None, errors: Set() }
    else { document: None, plan: None, errors: errors }
  }
}
