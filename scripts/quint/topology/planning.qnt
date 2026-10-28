module planning {
  import types.* from "../types"
  import basicSpells.* from "../vendor/spells/basicSpells"
  import placement as P from "./placement"

  type Build = {
    desired: GroupDecl, doc: Document, approved: Document, voters: Set[ReplicaKey], learners: Set[ReplicaKey],
    leader: Option[ReplicaKey], live: Set[ReplicaKey], conditional: Set[ReplicaKey], known: bool,
    capacity: int, maximum: int, required: List[PlannedStep], cleanup: List[PlannedStep], errors: Set[Refusal],
    stopped: Set[ReplicaKey],
  }

  pure def ordered(identities: Set[ReplicaKey]): List[ReplicaKey] = identities.fold(List(), (out, key) =>
    out.select(r => r.replica < key.replica).append(key).concat(out.select(r => r.replica >= key.replica)))

  pure def hostResource(key: ReplicaKey): ResourceKey = HostResource({ group: key.group, node: key.node })
  pure def stepFor(kind: ActionKind, key: ReplicaKey, before: Set[ReplicaKey], after: Set[ReplicaKey]): PlannedStep = {
    val resources = if (kind == WaitCatchUp) Set()
      else if (Set(PrepareReplica, StopReplica, RetireReplica).contains(kind)) Set(hostResource(key))
      else Set(GroupResource(key.group))
    { kind: kind, target: ReplicaTarget(key), resources: resources, targets: Set(key),
      seq: 0, expectedVoters: before, resultVoters: after, transferTarget: None }
  }

  pure def emit(s: Build, kind: ActionKind, key: ReplicaKey, after: Set[ReplicaKey]): Build = {
    val step = { ...stepFor(kind, key, s.voters, after),
      transferTarget: if (kind == DemoteVoter) s.leader else None }
    val requiredStop = s.desired.replicas.exists(r => r.key.node == key.node and r.key != key)
    if (kind == RetireReplica or (kind == StopReplica and not(requiredStop)))
      { ...s, cleanup: s.cleanup.append(step) }
    else { ...s, required: s.required.append(step) }
  }

  pure def retire(s: Build, key: ReplicaKey): Build = {
    val stopped = emit(s, StopReplica, key, s.voters)
    if (s.stopped.contains(key)) s else if (key.group == "_meta") stopped else emit(stopped, RetireReplica, key, s.voters)
  }

  pure def nodeFor(s: Build, name: NodeName): NodeDecl =
    if (s.doc.nodes.keys().contains(name)) s.doc.nodes.get(name) else s.approved.nodes.get(name)

  pure def domainViolations(s: Build, identities: Set[ReplicaKey]): Set[(ReplicaKey, ReplicaKey)] = {
    val domainErrors = match s.desired.voterConfig {
      | Some(c) => match c.domain {
        | None => Set()
        | Some(label) => identities.map(a => identities.filter(b =>
            if (a == b) nodeFor(s, a.node).labels.getOrElse(label, "") == ""
            else nodeFor(s, a.node).labels.getOrElse(label, "") == nodeFor(s, b.node).labels.getOrElse(label, ""))
          .map(b => (a, b))).flatten()
      }
      | None => Set()
    }
    domainErrors.union(identities.filter(r => P::learnerOnly(nodeFor(s, r.node))).map(r => (r, r)))
  }

  pure def uniqueDomains(s: Build, identities: Set[ReplicaKey]): bool = domainViolations(s, identities).size() == 0
  pure def noNewDomainViolations(s: Build, after: Set[ReplicaKey]): bool =
    domainViolations(s, after).subseteq(domainViolations(s, s.voters))

  pure def liveCount(s: Build, identities: Set[ReplicaKey]): int = identities.intersect(s.live.union(s.conditional)).size()
  pure def canDestroy(s: Build, key: ReplicaKey): bool = {
    val after = s.voters.exclude(Set(key))
    and {
      s.known, s.voters.contains(key), after.size() >= 1,
      after.size() >= s.voters.size() / 2 + 1,
      liveCount(s, s.voters) * 2 > s.voters.size(), liveCount(s, after) * 2 > after.size(),
      noNewDomainViolations(s, after),
    }
  }

  pure def ensureLearner(s: Build, key: ReplicaKey): Build =
    if (s.errors.size() > 0 or s.learners.contains(key)) s
    else if (s.voters.contains(key) or s.voters.union(s.learners).size() >= s.capacity or
      (key.group != "_meta" and s.voters.union(s.learners).exists(r => r.node == key.node and r != key)))
      { ...s, errors: s.errors.union(Set(UnsafeMembership)) }
    else {
      val prepared = emit(s, PrepareReplica, key, s.voters)
      val added = emit(prepared, AddLearner, key, s.voters)
      { ...added, learners: added.learners.union(Set(key)) }
    }

  pure def waitFor(s: Build, key: ReplicaKey): Build = {
    val waited = emit(s, WaitCatchUp, key, s.voters)
    { ...waited, conditional: waited.conditional.union(Set(key)) }
  }

  pure def canPromote(s: Build, key: ReplicaKey): bool = {
    val after = s.voters.union(Set(key))
    and {
      after.size() <= s.maximum, noNewDomainViolations(s, after),
      not(s.known) or after.intersect(s.live.union(s.conditional).union(Set(key))).size() * 2 > after.size(),
    }
  }

  pure def promote(s: Build, key: ReplicaKey): Build =
    if (s.errors.size() > 0) s
    else if (not(s.learners.contains(key)) or not(s.conditional.contains(key)) or not(canPromote(s, key)))
      { ...s, errors: s.errors.union(Set(UnsafeMembership)) }
    else {
      val promoted = emit(s, PromoteLearner, key, s.voters.union(Set(key)))
      { ...promoted, voters: s.voters.union(Set(key)), learners: s.learners.exclude(Set(key)) }
    }

  pure def destroy(s: Build, key: ReplicaKey): Build =
    if (s.errors.size() > 0) s
    else if (not(canDestroy(s, key))) { ...s, errors: s.errors.union(Set(UnsafeMembership)) }
    else {
      val candidates = ordered(P::voters(s.desired).intersect(s.voters).exclude(Set(key))
        .intersect(s.live.union(s.conditional)))
      val needsTransfer = s.leader == None or s.leader == Some(key)
      if (needsTransfer and candidates.length() == 0) { ...s, errors: s.errors.union(Set(Unavailable)) }
      else {
        val transferred = if (needsTransfer) {
          val target = candidates.head()
          { ...emit(s, TransferLeadership, target, s.voters), leader: Some(target) }
        } else s
        val after = s.voters.exclude(Set(key))
        val demoting = s.desired.replicas.exists(r => r.key == key)
        val removed = emit(transferred, if (demoting) DemoteVoter else RemoveMember, key, after)
        val changed = { ...removed, voters: after,
          learners: if (demoting) s.learners.union(Set(key)) else s.learners }
        if (demoting) changed else retire(changed, key)
      }
    }

  pure def addVoter(s: Build, key: ReplicaKey): Build = promote(waitFor(ensureLearner(s, key), key), key)

  pure def replacePair(s: Build, outgoing: ReplicaKey, incoming: ReplicaKey): Build = {
    val sameHost = outgoing.group != "_meta" and outgoing.node == incoming.node
    val noSeat = not(s.learners.contains(incoming)) and s.voters.union(s.learners).size() >= s.capacity and
      not(s.desired.replicas.exists(r => r.key == outgoing))
    if (sameHost or noSeat) addVoter(destroy(s, outgoing), incoming) else {
      val learner = ensureLearner(s, incoming)
      val future = { ...learner, voters: learner.voters.union(Set(incoming)),
        conditional: learner.conditional.union(Set(incoming)), learners: learner.learners.exclude(Set(incoming)) }
      if (canPromote(learner, incoming) and canDestroy(future, outgoing))
        destroy(promote(waitFor(learner, incoming), incoming), outgoing)
      else promote(waitFor(destroy(waitFor(learner, incoming), outgoing), incoming), incoming)
    }
  }

  pure def planMoves(input: PlanningInput, doc: Document, group: GroupDecl, observed: NativeObservation): Build = {
    val native = observed.native
    val capacity = if (group.id == "_meta") input.rules.maxNodes else input.rules.maxReplicas
    val bound = if (group.id == "_meta") input.rules.maxNodes else P::effective(doc, group).maxVoters
    val maximum = match group.voterConfig { | Some(c) => min(bound, c.target + c.maxSurge) | None => bound }
    val state: Build = {
      desired: group, doc: doc, approved: input.approved, voters: native.voters, learners: native.learners, leader: native.leaderAuthority,
      live: observed.live, conditional: Set(), known: observed.healthKnown, capacity: capacity, maximum: maximum,
      required: List(), cleanup: List(), errors: if (native.voters.size() == 0 or
        native.voters.union(native.learners).size() > capacity or
        (observed.healthKnown and observed.live.intersect(native.voters).size() * 2 <= native.voters.size()))
        Set(UnsafeMembership) else Set(),
      stopped: input.retainedStates.keys().filter(r => input.retainedStates.get(r) != RetainedHosted),
    }
    val wanted = group.replicas.map(r => r.key)
    val oldLearners = ordered(native.learners.exclude(wanted))
    val withoutLearners = oldLearners.foldl(state, (s, key) => {
      val removed = emit(s, RemoveMember, key, s.voters)
      retire({ ...removed, learners: removed.learners.exclude(Set(key)) }, key)
    })
    val stale = input.retained.filter(r => r.group == group.id).exclude(wanted).exclude(native.voters.union(native.learners))
    val retired = ordered(stale).foldl(withoutLearners, (s, key) => retire(s, key))
    val departures = ordered(native.voters.exclude(P::voters(group)))
    val replacing = departures.select(r => group.id != "_meta" and group.replicas.exists(p => p.key.node == r.node and p.key != r))
    val outgoing = replacing.select(r => native.leaderAuthority != Some(r))
      .concat(replacing.select(r => native.leaderAuthority == Some(r)))
      .concat(departures.select(r => not(P::toSet(replacing).contains(r))))
    val incoming = P::voters(group).exclude(native.voters)
    val paired = outgoing.foldl({ build: retired, pending: incoming }, (acc, old) => {
      val s = acc.build
      val learnerOnSameHost = group.id != "_meta" and group.replicas.exists(r =>
        r.key.node == old.node and r.key != old and r.role == Learner)
      if (acc.pending.size() == 0 or learnerOnSameHost) { ...acc, build: destroy(s, old) }
      else {
        val sameHost = acc.pending.filter(r => group.id != "_meta" and r.node == old.node)
        val sameDomain = acc.pending.filter(r => P::domain(group, nodeFor(s, r.node)) ==
          P::domain(group, nodeFor(s, old.node)))
        val choices = if (sameHost.size() > 0) sameHost else if (sameDomain.size() > 0) sameDomain else acc.pending
        val arrival = ordered(choices).head()
        { build: replacePair(s, old, arrival), pending: acc.pending.exclude(Set(arrival)) }
      }
    })
    val promoted = ordered(paired.pending).foldl(paired.build, (s, key) => addVoter(s, key))
    val finalState = ordered(wanted.exclude(P::voters(group))).foldl(promoted, (s, key) => ensureLearner(s, key))
    val finalValid = finalState.voters == P::voters(group) and finalState.learners == wanted.exclude(P::voters(group)) and
      uniqueDomains(finalState, finalState.voters) and finalState.voters.forall(r =>
        P::canVote(input.approved, doc, group, doc.nodes.get(r.node)))
    { ...finalState, errors: finalState.errors.union(if (finalValid) Set() else Set(UnsafeMembership)) }
  }

  pure def groupStep(kind: ActionKind, group: GroupId, targets: Set[ReplicaKey]): PlannedStep = {
    val hosts = targets.map(r => hostResource(r))
    { kind: kind, target: GroupTarget(group), targets: targets,
      resources: if (kind == CreateFoundingGroup) hosts.union(Set(GroupResource(group))) else hosts,
      seq: 0, expectedVoters: Set(), resultVoters: if (kind == CreateFoundingGroup) targets else Set(), transferTarget: None }
  }

  type BlockPlan = { required: List[PlannedStep], cleanup: List[PlannedStep], errors: Set[Refusal] }

  pure def planBlock(input: PlanningInput, doc: Document, id: GroupId): BlockPlan = {
    val retained = input.retained.filter(r => r.group == id)
    val hosted = retained.filter(r => input.retainedStates.getOrElse(r, RetainedHosted) == RetainedHosted)
    if (not(doc.groups.keys().contains(id))) {
      val stops = ordered(hosted).foldl(List(), (out, r) => out.append(stepFor(StopReplica, r, Set(), Set())))
      { required: List(), cleanup: if (hosted.size() > 0) stops.append(groupStep(DeleteGroupState, id, retained)) else stops,
        errors: Set() }
    } else {
      val group = doc.groups.get(id)
      val count = P::voters(group).size()
      val minimum = if (id == "_meta") 1 else P::effective(doc, group).minVoters
      val maximum = if (id == "_meta") input.rules.maxNodes else P::effective(doc, group).maxVoters
      if (count < minimum or count > maximum) { required: List(), cleanup: List(), errors: Set(UnsafeMembership) }
      else if (not(input.observations.keys().contains(id))) {
        if (input.approved.groups.keys().contains(id) or retained.size() > 0)
          { required: List(), cleanup: List(), errors: Set(Unavailable) }
        else {
          val founding = List(groupStep(CreateFoundingGroup, id, P::voters(group)))
          val steps = ordered(group.replicas.filter(r => r.role == Learner).map(r => r.key)).foldl(founding, (out, key) =>
            out.append(stepFor(PrepareReplica, key, P::voters(group), P::voters(group)))
              .append(stepFor(AddLearner, key, P::voters(group), P::voters(group))))
          { required: steps, cleanup: List(), errors: P::selectionErrors(doc, group).union(
            if (P::voters(group).forall(r => P::canVote(input.approved, doc, group, doc.nodes.get(r.node)))) Set()
            else Set(UnsafeMembership)) }
        }
      } else {
        val built = planMoves(input, doc, group, input.observations.get(id))
        { required: built.required, cleanup: built.cleanup, errors: built.errors }
      }
    }
  }

  pure def number(steps: List[PlannedStep], start: int): List[PlannedStep] =
    steps.foldl(List(), (out, step) => out.append({ ...step, seq: start + out.length() }))

  // topology.bindReadmissionRetirement: fresh metadata admission owns former
  // host retirement. Keep detached whole-group deletion obligations, but never
  // route later per-replica host calls to the already retired member.
  pure def readmissionOwnsHostStep(input: PlanningInput, desired: Document, step: PlannedStep): bool =
    if (not(Set(StopReplica, RetireReplica).contains(step.kind))) false else match step.target {
      | GroupTarget(_) => false
      | ReplicaTarget(key) => desired.groups.keys().contains(key.group) and desired.nodes.keys().contains(key.node) and
          desired.nodes.get(key.node).member > key.member and input.retained.contains(key) and
          desired.groups.get("_meta").replicas.exists(r => r.key.node == key.node and r.key.member == desired.nodes.get(key.node).member)
    }

  pure def buildPlan(input: PlanningInput, desired: Document, scopes: Set[GroupId]): PlanResult = {
    val emptyPlan: Plan = { version: desired.version, blocks: Map(), cleanup: Map(), sources: Map(), dependencies: Set() }
    val valid = P::validateDocument(desired, input.rules)
    val observableIdentities = input.observations.keys().intersect(scopes).intersect(desired.groups.keys()).forall(g => {
      val c = input.observations.get(g).native
      c.voters.union(c.learners).forall(r => and {
        input.retained.contains(r),
        desired.nodes.keys().contains(r.node) or input.approved.nodes.keys().contains(r.node),
        input.retainedStates.getOrElse(r, RetainedHosted) != RetainedCleanup,
      })
    })
    if (not(observableIdentities)) PlanRejected(Set(Unauthorized)) else {
    val built = input.rules.groupOrder.select(g => scopes.contains(g)).foldl(
      { plan: emptyPlan, seq: 1, errors: valid }, (acc, id) => {
        val block = planBlock(input, desired, id)
        val required = number(block.required.select(s => not(readmissionOwnsHostStep(input, desired, s))), acc.seq)
        val cleanup = number(block.cleanup.select(s => not(readmissionOwnsHostStep(input, desired, s))), acc.seq + required.length())
        val chain = required.indices().filter(i => i > 0).map(i => (required.nth(i).seq, required.nth(i - 1).seq))
        val retireDeps = P::toSet(cleanup).filter(s => s.kind == RetireReplica).map(retirement =>
          P::toSet(required.concat(cleanup)).filter(s => s.kind == StopReplica and s.target == retirement.target)
            .map(stop => (retirement.seq, stop.seq))).flatten()
        val source = if (input.groups.keys().contains(id)) input.groups.get(id).sourceVersion else 0
        { plan: { ...acc.plan, blocks: acc.plan.blocks.put(id, required), cleanup: acc.plan.cleanup.put(id, cleanup),
            sources: acc.plan.sources.put(id, source), dependencies: acc.plan.dependencies.union(chain).union(retireDeps) },
          seq: acc.seq + required.length() + cleanup.length(), errors: acc.errors.union(block.errors) }
      })
    if (not(scopes.subseteq(P::toSet(input.rules.groupOrder)))) PlanRejected(Set(Invalid))
    else if (built.errors.size() > 0) PlanRejected(built.errors)
    else Planned(built.plan)
    }
  }
}
