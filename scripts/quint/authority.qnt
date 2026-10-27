module authority {
  import types.* from "./types"
  import basicSpells.* from "./vendor/spells/basicSpells"
  import topology as T from "./topology"
  import placement as P from "./placement"

  // Source: metaservice.mutationBase, documents.Stage, metaengine.scoped_stage
  // and mutation_replay. No transport/caller-local state is changed here.
  pure def capture(request: MutationRequest, localVersion: int, caller: NodeName): MutationRequest = {
    val rawStage = match request.edit { | StageDocument(_) => true | _ => false }
    val explicitActor = match request.edit { | ForceRetireNode(_) => true | _ => false }
    { ...request, options: { ...request.options,
        expectedVersion: if (not(rawStage) and request.options.expectedVersion == 0)
          localVersion else request.options.expectedVersion,
        audit: if (rawStage or explicitActor or request.options.audit.actor != "") request.options.audit
          else { ...request.options.audit, actor: caller },
      } }
  }

  pure def mutationInput(meta: MetadataState, health: NodeName -> HealthClass, rules: Rules): MutationInput = {
    view: { latest: meta.latest, target: meta.target }, retired: meta.retiredGroupIds,
    memberFloor: meta.memberAllocationFloor, replicaFloors: meta.replicaAllocationFloors,
    history: meta.history, groups: meta.groups, nodes: meta.nodes, health: health, rules: rules,
  }

  pure def semanticRequest(request: MutationRequest): MutationRequest = {
    ...request, options: { ...request.options, preview: false, approve: false },
  }
  pure def rawStageEqual(a: Document, b: Document): bool =
    { ...a, version: 0, rebasedFrom: None } == { ...b, version: 0, rebasedFrom: None }

  pure def replayVersion(meta: MetadataState, request: MutationRequest): Option[int] = {
    val version = request.options.expectedVersion + 1
    if (request.options.expectedVersion >= meta.latest.version) None
    else match request.edit {
      | StageDocument(doc) => if (meta.history.keys().contains(version) and
          rawStageEqual(doc, meta.history.get(version))) Some(version) else None
      | _ => {
        val versions = meta.stagedIntents.keys().filter(v =>
          meta.stagedIntents.get(v) == semanticRequest(request))
        if (versions.size() == 1) Some(versions.getOnlyElement()) else None
      }
    }
  }

  pure def rebasedSuccessor(meta: MetadataState, version: int): Option[int] = {
    val ordered = meta.history.keys().fold(List(), (out, v) =>
      out.select(n => n < v).append(v).concat(out.select(n => n > v)))
    val successor = ordered.foldl(version, (current, v) =>
      if (meta.history.get(v).rebasedFrom == Some(current)) v else current)
    if (successor == version) None else Some(successor)
  }
  pure def stageOutcome(meta: MetadataState, version: int): MutationOutcome = {
    staged: Some(version), operation: if (meta.firstAttempts.keys().contains(version))
      Some(meta.firstAttempts.get(version)) else None,
    rebasedTo: rebasedSuccessor(meta, version), errors: Set(),
  }
  pure def refused(errors: Set[Refusal]): MutationOutcome = {
    staged: None, operation: None, rebasedTo: None, errors: errors,
  }

  // The FSM validates the prepared document, not a fresh invocation of a
  // convenience edit against newer health. Raw Stage is submitted with literal
  // CAS even when the caller's applied metadata is behind that expected version.
  pure def stageErrors(meta: MetadataState, command: StageCommand, rules: Rules): Set[Refusal] = {
    val request = command.request
    if (request.options.preview) Set(Invalid)
    else if (request.options.expectedVersion != meta.latest.version) {
      match replayVersion(meta, request) { | Some(_) => Set() | None => Set(Conflict) }
    } else {
      val input = mutationInput(meta, Map(), rules)
      val validation: MutationRequest = { edit: StageDocument(command.desired),
        options: { ...request.options, approve: false } }
      T::mutationErrors(input, validation)
    }
  }

  pure def canStage(meta: MetadataState, command: StageCommand, rules: Rules): bool =
    stageErrors(meta, command, rules).size() == 0

  pure def installStage(meta: MetadataState, command: StageCommand): MetadataState = {
    val doc = { ...command.desired, version: meta.latest.version + 1, rebasedFrom: None }
    { ...meta, latest: doc, history: meta.history.put(doc.version, doc),
      stagedIntents: match command.request.edit {
        | StageDocument(_) => meta.stagedIntents
        | _ => meta.stagedIntents.put(doc.version, semanticRequest(command.request))
      },
      memberAllocationFloor: doc.nodes.values().fold(meta.memberAllocationFloor,
        (floor, node) => max(floor, node.member + 1)),
      replicaAllocationFloors: doc.groups.values().fold(meta.replicaAllocationFloors, (floors, group) =>
        floors.put(group.id, max(floors.getOrElse(group.id, 1), group.nextReplica))),
    }
  }

  pure def applyStage(meta: MetadataState, command: StageCommand): MetadataState =
    match replayVersion(meta, command.request) { | Some(_) => meta | None => installStage(meta, command) }

  pure def acceptedStage(meta: MetadataState, command: StageCommand): MutationOutcome =
    match replayVersion(meta, command.request) {
      | Some(version) => stageOutcome(meta, version)
      | None => stageOutcome(installStage(meta, command), meta.latest.version + 1)
    }

  pure def terminal(phase: OperationPhase): bool = Set(OpSucceeded, OpFailed, Superseded).contains(phase)
  pure def scopeAt(meta: MetadataState, group: GroupId): ScopeExpectation =
    if (meta.groups.keys().contains(group)) {
      val row = meta.groups.get(group)
      { sourceVersion: row.sourceVersion, owner: row.owner }
    } else { sourceVersion: 0, owner: None }

  pure def retainedOwners(meta: MetadataState): Set[ReplicaKey] =
    meta.groups.values().map(g => match g.proven {
      | Some(d) => d.replicas.map(r => r.key) | None => Set()
    }).flatten().union(meta.cleanup.keys()).union(meta.grants.values().map(g => g.targets).flatten())

  pure def planningInput(meta: MetadataState, rules: Rules): PlanningInput = {
    val retained = retainedOwners(meta)
    { approved: meta.target, groups: meta.groups, retained: retained,
      observations: Map(), rules: rules,
      retainedStates: retained.mapBy(r => if (meta.cleanup.keys().contains(r)) {
        val debt = meta.cleanup.get(r)
        if (debt.retired != None) RetainedCleanup else if (debt.stopped != None) RetainedStopped else RetainedHosted
      } else RetainedHosted) }
  }

  pure def deletionObligationsCovered(meta: MetadataState, command: ApprovalCommand): bool = {
    val deleted = command.plan.blocks.keys().exclude(command.desired.groups.keys())
    retainedOwners(meta).filter(r => deleted.contains(r.group)).forall(r => {
      val cleanup = command.plan.cleanup.getOrElse(r.group, List())
      if (meta.cleanup.keys().contains(r)) true else and {
        cleanup.select(s => s.kind == StopReplica and s.target == ReplicaTarget(r)).length() == 1,
        cleanup.select(s => s.kind == DeleteGroupState and s.targets.contains(r)).length() == 1,
      }
    })
  }

  pure def approvalErrors(meta: MetadataState, command: ApprovalCommand, rules: Rules): Set[Refusal] = {
    val version = command.desired.version
    if (version == 0) Set(Invalid)
    else if (meta.firstAttempts.keys().contains(version)) {
      val first = meta.operations.get(meta.firstAttempts.get(version))
      if (first.plan == command.plan) Set() else Set(Conflict)
    } else if (version <= meta.target.version or version != meta.latest.version or command.desired != meta.latest)
      Set(Conflict)
    else {
      val affected = T::affectedScopes(meta.target, command.desired)
      val captures = command.scopes.keys() == affected and affected.forall(g =>
        command.scopes.get(g) == scopeAt(meta, g))
      val shape = command.plan.version == version and command.plan.blocks.keys() == affected and
        command.plan.sources.keys() == affected and affected.forall(g =>
          command.plan.sources.get(g) == scopeAt(meta, g).sourceVersion)
      val retained = retainedOwners(meta).filter(r => affected.contains(r.group))
      val captured = command.captured.retained.filter(r => affected.contains(r.group))
      if (not(captures)) Set(Conflict)
      else if (not(shape) or command.captured.rules != rules or retained != captured) Set(Unauthorized)
      else if (T::makePlan(command.captured, command.desired) != Planned(command.plan)) Set(Unauthorized)
      else if (not(deletionObligationsCovered(meta, command))) Set(Unauthorized)
      else P::validateTransition(T::retainedIdentities({ latest: meta.latest, target: meta.target }),
        command.desired, meta.retiredGroupIds, rules)
    }
  }

  pure def canApprove(meta: MetadataState, command: ApprovalCommand, rules: Rules): bool =
    approvalErrors(meta, command, rules).size() == 0

  pure def zeroStepProven(meta: MetadataState, command: ApprovalCommand, group: GroupId): bool =
    if (not(command.desired.groups.keys().contains(group))) true
    // Issued effects need receipt-based proof (Task 6), not just declarations.
    else if (meta.grants.keys().exists(k => k.group == group)) false
    else if (not(meta.groups.keys().contains(group))) false else match meta.groups.get(group).proven {
      | Some(prior) => prior.replicas == command.desired.groups.get(group).replicas
      | None => false
    }

  pure def approvedBlocks(meta: MetadataState, command: ApprovalCommand): GroupId -> BlockProgress =
    command.plan.blocks.keys().mapBy(g => {
      val steps = command.plan.blocks.get(g)
      val complete = steps.length() == 0 and zeroStepProven(meta, command, g)
      { phase: if (complete) OpSucceeded else if (steps.length() == 0) WaitingReplica else OpPending,
        step: if (steps.length() == 0) 0 else steps.head().seq,
        reason: if (steps.length() == 0 and not(complete)) Some(AuthorityPending) else None }
    })

  pure def deletionDebt(command: ApprovalCommand, key: ReplicaKey): CleanupDebt = {
    val operation = { version: command.desired.version, attempt: 1 }
    val cleanup = command.plan.cleanup.get(key.group)
    val stops = cleanup.select(s => s.kind == StopReplica and s.target == ReplicaTarget(key))
    val retirements = cleanup.select(s => s.kind == DeleteGroupState and s.targets.contains(key))
    { identity: key, sourceOperation: operation, storage: StorageDefault,
      stopAction: if (stops.length() == 1) Some({ operation: operation, step: stops.head() }) else None,
      retireAction: if (retirements.length() == 1) Some({ operation: operation, step: retirements.head() }) else None,
      stopGrant: None, stopped: None, retired: None, collected: None,
      retention: Retain, collectionPhase: Unprepared }
  }

  pure def installApproval(meta: MetadataState, command: ApprovalCommand): MetadataState = {
    val key = { version: command.desired.version, attempt: 1 }
    val affected = command.scopes.keys()
    val blocks = approvedBlocks(meta, command)
    val operation: Operation = { key: key, predecessor: None, origin: command.origin,
      plan: command.plan, blocks: blocks, audit: command.audit }
    val operations = meta.operations.transformValues(old => { ...old,
      blocks: old.blocks.keys().mapBy(g => {
        val block = old.blocks.get(g)
        if (affected.contains(g) and not(terminal(block.phase))) { ...block, phase: Superseded, reason: None }
        else block
      }) }).put(key, operation)
    val deleted = affected.exclude(command.desired.groups.keys())
    val cleanup = retainedOwners(meta).filter(r => deleted.contains(r.group)).fold(meta.cleanup, (debts, r) =>
      if (debts.keys().contains(r)) debts else debts.put(r, deletionDebt(command, r)))
    val groups = affected.fold(meta.groups, (rows, g) => {
      val old = rows.getOrElse(g, { sourceVersion: 0, proven: None, owner: None, deleting: false })
      val finished = blocks.get(g).phase == OpSucceeded
      rows.put(g, if (finished) {
        sourceVersion: key.version, proven: if (deleted.contains(g)) None else Some(command.desired.groups.get(g)),
        owner: None, deleting: false,
      } else { ...old, owner: Some(key), deleting: deleted.contains(g) })
    })
    { ...meta, target: command.desired, operations: operations, groups: groups,
      firstAttempts: meta.firstAttempts.put(key.version, key), cleanup: cleanup,
      retiredGroupIds: meta.retiredGroupIds.union(deleted) }
  }

  pure def applyApproval(meta: MetadataState, command: ApprovalCommand): MetadataState =
    if (meta.firstAttempts.keys().contains(command.desired.version)) meta else installApproval(meta, command)

  pure def failedGroups(op: Operation): Set[GroupId] = op.blocks.keys().filter(g => op.blocks.get(g).phase == OpFailed)
  pure def retryErrors(meta: MetadataState, request: RetryCommand): Set[Refusal] =
    if (meta.successors.keys().contains(request.previous)) Set()
    else if (not(meta.operations.keys().contains(request.previous))) Set(NotFound)
    else {
      val previous = meta.operations.get(request.previous)
      val failed = failedGroups(previous)
      if (failed.size() == 0 or not(previous.blocks.values().forall(b => terminal(b.phase)))) Set(Unauthorized)
      else if (failed != request.scopes.keys()) Set(Invalid)
      else {
        val changed = T::affectedScopes(meta.history.get(previous.key.version), meta.target)
        val valid = failed.forall(g => and {
          request.scopes.get(g) == scopeAt(meta, g), scopeAt(meta, g).owner == None,
          scopeAt(meta, g).sourceVersion <= previous.key.version,
          not(changed.contains(g)),
          not(meta.operations.values().exists(op => op.key.version > previous.key.version and op.plan.blocks.keys().contains(g))),
        })
        if (valid) Set() else Set(Conflict)
      }
    }

  pure def canRetry(meta: MetadataState, request: RetryCommand): bool = retryErrors(meta, request).size() == 0
  pure def retryKey(meta: MetadataState, request: RetryCommand): AttemptKey =
    meta.successors.getOrElse(request.previous,
      { version: request.previous.version, attempt: request.previous.attempt + 1 })

  pure def applyRetry(meta: MetadataState, request: RetryCommand): MetadataState =
    if (meta.successors.keys().contains(request.previous)) meta else {
      val previous = meta.operations.get(request.previous)
      val key = retryKey(meta, request)
      val failed = failedGroups(previous)
      val retry = { ...previous, key: key, predecessor: Some(previous.key),
        // Leader-side remaining-plan validation checks this retained cursor and
        // consumed prefix before proposing the exact immutable successor.
        blocks: previous.blocks.keys().mapBy(g => {
          val block = previous.blocks.get(g)
          if (failed.contains(g)) { ...block, phase: OpPending, reason: None } else block
        }) }
      { ...meta, successors: meta.successors.put(previous.key, key), operations: meta.operations.put(key, retry),
        groups: failed.fold(meta.groups, (rows, g) => rows.setBy(g, row => { ...row, owner: Some(key) })) }
    }
}
