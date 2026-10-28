module types {
  import basicSpells.* from "./vendor/spells/basicSpells"

  type NodeName = str
  type GroupId = str
  type ProcessKey = { node: NodeName, incarnation: int }
  type RequestKey = { caller: ProcessKey, sequence: int }
  type ProposalKey = { request: RequestKey, sequence: int }
  type ReadTicket = { owner: ProcessKey, request: RequestKey, proposal: Option[int], round: int }
  type ReplicaKey = { cluster: str, group: GroupId, node: NodeName, member: int, replica: int }
  type DurableNodeIdentity = { cluster: str, node: NodeName, member: int }
  type AttemptKey = { version: int, attempt: int }
  type Audit = { actor: str, comment: str, forced: Option[{ node: NodeName, reason: str }] }
  type MutationOptions = { expectedVersion: int, audit: Audit, preview: bool, approve: bool }
  type NodePhase = Active | Draining | Drained | Excluded
  type ReplicaRole = Voter | Learner
  type StorageClass = StorageDefault | StorageMemory
  type StorageBackend = OwnedWAL | BorrowedCustom
  type NodeStorageConfig = { defaultBackend: StorageBackend }
  type NodeDecl = {
    name: NodeName, member: int, state: NodePhase, nodeType: str, labels: str -> str,
  }
  type ReplicaDecl = { key: ReplicaKey, role: ReplicaRole, pinnedLearner: bool }
  type PlacementRule = {
    minVoters: int, maxVoters: int, allowedTypes: Set[str], selector: str -> str,
  }
  type HostingMode = AllEligible | ExplicitHosts
  type HostingRule = { mode: HostingMode, selectedNodes: List[NodeName] }
  type VoterConfig = {
    target: int, domain: Option[str], maxSurge: int,
    failureWindow: int, standbyWindow: int, failureLag: int, standbyLag: int,
    standbyApplyBacklog: int, replaceNoProgress: bool, noProgressWindow: int,
    maxObservationGap: int, switchCooldown: int, demotionCooldown: int,
    allowNewFailureDuringCooldown: bool,
  }
  type GroupDecl = {
    id: GroupId, storage: StorageClass, labels: str -> str, replicas: Set[ReplicaDecl],
    placement: Option[PlacementRule], hosting: Option[HostingRule],
    voterConfig: Option[VoterConfig],
    nextReplica: int,
  }
  type Document = {
    cluster: str, version: int, parent: int, rebasedFrom: Option[int],
    nodes: NodeName -> NodeDecl, groups: GroupId -> GroupDecl,
    placement: PlacementRule, audit: Audit,
  }
  type ReadView = { latest: Document, target: Document }

  // Vocabulary grounded in topology/operator_steps.go; execution arrives in Tasks 5–10.
  type ActionKind = CreateFoundingGroup | AddLearner | WaitCatchUp | PromoteLearner
    | TransferLeadership | DemoteVoter | RemoveMember | StopReplica | RetireReplica
    | DeleteGroupState | PrepareReplica
  type EffectTarget = GroupTarget(GroupId) | ReplicaTarget(ReplicaKey)
  type EffectKey = {
    operation: AttemptKey, step: int, kind: ActionKind,
    target: EffectTarget, grantRevision: int,
  }
  type ResourceKey = GroupResource(GroupId) | HostResource({ group: GroupId, node: NodeName })
  type GrantKey = { cluster: str, group: GroupId, revision: int }
  type AdmissionKey = { node: NodeName, predecessor: int, successor: int }
  type GrantPurpose = Effect | Fence | Transfer | Readmission
  type Grant = {
    key: GrantKey, purpose: GrantPurpose, effect: Option[EffectKey],
    admission: Option[AdmissionKey], resources: Set[ResourceKey], targets: Set[ReplicaKey],
    planStep: Option[PlannedStep],
    request: Option[RequestKey],
  }
  type ReceiptOutcome = EffectApplied | AlreadySatisfied | TransferStarted
  type Receipt = {
    grant: GrantKey, effect: Option[EffectKey], admission: Option[AdmissionKey],
    target: Option[ReplicaKey], resource: ResourceKey, outcome: ReceiptOutcome,
    planStep: Option[PlannedStep],
    durablePosition: int, durableTerm: int,
  }
  type FenceEvidence = {
    grant: GrantKey, resource: ResourceKey, receiver: DurableNodeIdentity,
    replica: Option[ReplicaKey], durablePosition: int,
    durableTerm: int,
  }
  type Revocation = {
    requested: bool, requiredResources: Set[ResourceKey], provenResources: Set[ResourceKey],
    evidence: Set[FenceEvidence],
    fences: ResourceKey -> GrantKey,
  }
  type PlannedStep = {
    kind: ActionKind, target: EffectTarget, resources: Set[ResourceKey],
    targets: Set[ReplicaKey],
    seq: int, expectedVoters: Set[ReplicaKey], resultVoters: Set[ReplicaKey],
    transferTarget: Option[ReplicaKey],
  }
  type Plan = {
    version: int, blocks: GroupId -> List[PlannedStep], cleanup: GroupId -> List[PlannedStep],
    sources: GroupId -> int, dependencies: Set[(int, int)],
  }
  type ApprovedAction = { operation: AttemptKey, step: PlannedStep }
  type ScopeExpectation = { sourceVersion: int, owner: Option[AttemptKey] }
  type ApprovalCommand = {
    desired: Document, plan: Plan, scopes: GroupId -> ScopeExpectation,
    captured: PlanningInput, origin: Origin, audit: Audit,
  }
  type RetryCommand = { previous: AttemptKey, scopes: GroupId -> ScopeExpectation }
  type RepairCommand = { request: MutationRequest, expectedTarget: int, approval: ApprovalCommand }
  type OperationPhase = OpPending | OpRunning | WaitingReplica | WaitingCatchUp
    | WaitingLeader | OpSucceeded | OpFailed | Superseded
  type BlockReason = NoQuorum | HostOffline | CatchUpRequired | LeaderRequired
    | AuthorityPending | LocalHold | CallbackPending | RetentionPending | BoundExhausted
  type BlockProgress = { phase: OperationPhase, step: int, reason: Option[BlockReason] }
  type Origin = Operator | Reconciler
  type Operation = {
    key: AttemptKey, predecessor: Option[AttemptKey], origin: Origin,
    plan: Plan, blocks: GroupId -> BlockProgress, audit: Audit,
  }
  type GroupProgress = {
    sourceVersion: int, proven: Option[GroupDecl], owner: Option[AttemptKey], deleting: bool,
  }
  type NodeProgress = {
    sourceVersion: int, proven: Option[NodeDecl], admission: Option[AdmissionKey],
  }
  type CollectionPhase = Unprepared | Prepared | PhysicallyCollected
    | CompletionDurable | Reported | MarkerReleased
  type RetentionClass = Retain | EligibleForCollection
  type CleanupDebt = {
    identity: ReplicaKey, sourceOperation: AttemptKey, storage: StorageClass,
    // Approval retains exact obligations before any execution grant is issued.
    stopAction: Option[ApprovedAction], retireAction: Option[ApprovedAction],
    stopGrant: Option[GrantKey], removed: Option[Receipt], stopped: Option[Receipt], retired: Option[Receipt],
    collected: Option[Receipt], retention: RetentionClass, collectionPhase: CollectionPhase,
  }
  type CollectionState = {
    identity: ReplicaKey, grant: GrantKey, phase: CollectionPhase, token: int,
  }
  type SessionKey = { owner: ProcessKey, generation: int }
  type SessionPhase = Live | ExpiredObserved | Revoked
  type Session = { owner: ProcessKey, member: int, phase: SessionPhase, renewalGeneration: int }
  type ProcessAdmission = { owner: ProcessKey, replica: ReplicaKey, operation: AttemptKey, origin: Option[AttemptKey], ready: bool }
  type JoinOffer = { admission: ProcessAdmission, proofIndex: int }
  type AdmissionCommand = BindAdmission({ admission: ProcessAdmission, previous: Option[ProcessKey] })
    | ReadyAdmission(ProcessAdmission)
  type MetadataState = {
    latest: Document, target: Document, history: int -> Document,
    groups: GroupId -> GroupProgress, nodes: NodeName -> NodeProgress,
    operations: AttemptKey -> Operation, firstAttempts: int -> AttemptKey,
    successors: AttemptKey -> AttemptKey, grants: GrantKey -> Grant,
    revocations: GrantKey -> Revocation, cleanup: ReplicaKey -> CleanupDebt,
    memberAllocationFloor: int, replicaAllocationFloors: GroupId -> int,
    retiredGroupIds: Set[GroupId], sessions: SessionKey -> Session, activeSessions: NodeName -> SessionKey, locks: str -> SessionKey,
    // Exact semantic request provenance, independent of a transport call ID.
    stagedIntents: int -> MutationRequest,
    authorityFloors: ResourceKey -> GrantKey, closedGrants: Set[GrantKey],
    effectReceipts: Set[Receipt], fenceEvidence: Set[FenceEvidence],
    couriers: NodeName -> { process: ProcessKey, member: int },
    admissions: NodeName -> ProcessAdmission,
  }
  type CatchUpClass = Behind | CaughtUp | CatchUpUnknown
  type NativeChange = {
    kind: ActionKind, target: ReplicaKey, sourceConfig: int,
    evidence: ConsumptionEvidence, live: Set[ReplicaKey],
  }
  type ConsensusState = {
    lineage: str, configRevision: int, voters: Set[ReplicaKey], learners: Set[ReplicaKey],
    leaderAuthority: Option[ReplicaKey], committedPosition: int,
    appliedPositions: ReplicaKey -> int, catchUp: ReplicaKey -> CatchUpClass,
    pendingChange: Option[NativeChange],
    authorityFloor: int, authorityReceipts: Set[Receipt], authorityFences: Set[FenceEvidence],
  }
  type HealthClass = Healthy | Unreachable | HealthUnknown | ConfirmedDead
  type Age = Fresh | Stale
  type Observation = {
    group: GroupId, replica: ReplicaKey, incarnation: int, observedConfig: int,
    observedLeader: Option[ReplicaKey], health: HealthClass, age: Age,
  }
  type OwnerPhase = Absent | Reserved | Opening | Replaying | OwnerReady | Parked
    | OwnerStopping | OwnerStopped
  type ClosePhase = NotAcquired | Acquired | Closing | CloseFailed | Closed
  type OwnerState = {
    key: ReplicaKey, phase: OwnerPhase, held: bool, reservation: Option[RequestKey],
    replayReady: bool, startupAuthorized: bool, published: bool,
    factoryOutstanding: bool, engineJoined: bool, applicationClose: ClosePhase,
    engineRunning: bool,
    process: ProcessKey, waiters: Set[RequestKey], callbackExpired: bool, closeExpired: bool,
    replayOutstanding: bool, startDeclaration: Option[GroupDecl], authorityRevision: int, scopeIndex: int,
  }
  type StorageEvidence = ValidStore({ snapshot: int, commit: int, tail: int, appliedHint: int })
    | MissingStore | ForeignStore(ReplicaKey) | UnreadableStore | ContradictoryStore
  type RepairKind = RestoreMissingReplica | ReconfigureVoters
  type RepairDecision = { kind: RepairKind, group: GroupId, target: ReplicaKey, role: ReplicaRole }
  type SchedulerCapture = {
    target: Document, sourceVersions: GroupId -> int, owners: GroupId -> Option[AttemptKey],
    observations: GroupId -> Observation, decision: Option[RepairDecision],
  }

  // Recovery phases match recovery/types.go; reserved rollback phases are not actions.
  type RecoveryMode = Rebuild | ResetForJoin
  type RecoveryPhase = Inspected | Approved | Files | Cancelling | Cancelled | Archiving
    | AwaitingStart | Importing | Validating | Complete | JoinReady | Joined
  type FilesHookPhase = FilesNotStarted | FilesPending | FilesSucceeded | FilesFailed
    | UndoPending | UndoSucceeded
  type RecoveryRequest = {
    id: int, mode: RecoveryMode, desiredCluster: str, audit: Audit, requiredGroups: Set[GroupId],
  }
  type RecoveryPlan = {
    id: int, request: RecoveryRequest, source: DurableNodeIdentity, freshIdentity: DurableNodeIdentity,
    groups: GroupId -> ReplicaKey, blockers: Set[str], mayLoseAcknowledgedWrites: bool,
  }
  type Acceptance = { exactPlan: int, acknowledgeDataLoss: bool, oldDeploymentFenced: bool }
  type EvidenceKey = ReplicaEvidence(ReplicaKey) | GrantEvidence(GrantKey)
  type RecoveryState = {
    source: DurableNodeIdentity, mode: RecoveryMode, request: RecoveryRequest,
    plan: Option[RecoveryPlan], acceptance: Option[Acceptance], phase: RecoveryPhase,
    sourceOwned: bool, filesHook: FilesHookPhase, archivedEvidence: Set[EvidenceKey],
    newLineage: Option[str], groupsImported: Set[GroupId],
  }
  type ProcessPhase = New | Starting | Ready | Fenced | Stopping | Stopped | Failed
  type Refusal = Invalid | Conflict | Unavailable | UnsafeMembership | Unauthorized
    | IdentityMismatch | NotFound | Unsupported | CapacityExceeded | WaitCancelled
  type Rules = {
    maxNodes: int, maxReplicas: int, concurrentGroups: int, concurrentCleanup: int,
    nodeOrder: List[NodeName], groupOrder: List[GroupId],
  }
  type GroupSpec = {
    id: GroupId, labels: str -> str, storage: StorageClass, placement: Option[PlacementRule],
    nodes: Option[List[NodeName]], hosting: Option[HostingRule], voterConfig: Option[VoterConfig],
  }
  type DocumentEdit = StageDocument(Document) | AddNode(NodeDecl)
    | ReadmitNode({ name: NodeName, expectedMember: int })
    | DrainNode(NodeName) | ActivateNode(NodeName) | ExcludeNode(NodeName)
    | ForceRetireNode({ name: NodeName, reason: str })
    | SetPlacement(PlacementRule) | SetGroupPlacement({ group: GroupId, rule: PlacementRule })
    | SetGroupVoterConfig({ group: GroupId, config: VoterConfig })
    | SetGroupHosting({ group: GroupId, hosting: HostingRule })
    | SetReplicaPinnedLearner({ key: ReplicaKey, pinned: bool })
    | CreateGroup(GroupSpec) | DeleteGroup(GroupId)
    | ChangeReplica({ group: GroupId, node: NodeName, role: ReplicaRole })
    | ReseatReplica(ReplicaKey) | RollbackTo(int)
  type MutationRequest = { edit: DocumentEdit, options: MutationOptions }
  type MutationInput = {
    view: ReadView, retired: Set[GroupId], memberFloor: int, replicaFloors: GroupId -> int,
    history: int -> Document, groups: GroupId -> GroupProgress, nodes: NodeName -> NodeProgress,
    health: NodeName -> HealthClass, rules: Rules,
  }
  type NativeObservation = {
    native: ConsensusState, healthKnown: bool, live: Set[ReplicaKey], ready: Set[ReplicaKey],
  }
  type PlanningInput = {
    approved: Document, groups: GroupId -> GroupProgress, retained: Set[ReplicaKey],
    observations: GroupId -> NativeObservation, rules: Rules,
    retainedStates: ReplicaKey -> RetainedPhase,
  }
  type RetainedPhase = RetainedHosted | RetainedStopped | RetainedCleanup
  type PlanResult = Planned(Plan) | PlanRejected(Set[Refusal])
  type PreviewResult = { document: Option[Document], plan: Option[Plan], errors: Set[Refusal] }
  type MutationOutcome = {
    staged: Option[int], operation: Option[AttemptKey], rebasedTo: Option[int], errors: Set[Refusal],
  }
  type StageCommand = { request: MutationRequest, desired: Document }
  type SessionCommand = GrantInternalSession({ key: SessionKey, member: int })
    | BindInternalSession(SessionKey) | RevokeInternalSession(SessionKey)
  type ControlCommand = AdmissionControl(AdmissionCommand) | SessionControl(SessionCommand) | BeginBlock({ operation: AttemptKey, group: GroupId })
    | IssueEffect(ApprovedAction) | IssueCleanupStop(ApprovedAction) | IssueTransfer({ target: ReplicaKey, request: RequestKey, sourceVersion: int })
    | RecordEffect(Receipt) | RecordFence(FenceEvidence)
    | CompleteStep({ approved: ApprovedAction, observedLeader: Option[ReplicaKey] })
    | CompleteCatchUp(ApprovedAction)
    | RecordStepStatus({ approved: ApprovedAction, phase: OperationPhase, reason: Option[BlockReason] })
  type MetaCommand = StageProposal(StageCommand) | ApprovalProposal(ApprovalCommand)
    | RetryProposal(RetryCommand) | RepairProposal(RepairCommand) | ControlProposal(ControlCommand)
  type OperationRequest = ApproveVersion(int) | RetryAttempt(AttemptKey) | RepairRequest(MutationRequest)
  type ApiRequest = ObserveLocal | InvalidRequest | ConfirmedRead | Mutate(MutationRequest) | Operate(OperationRequest)
  type ApiResult = Observed(ProcessPhase) | Refused(Refusal) | Indeterminate
    | ConfirmedView({ index: int, applied: int, view: ReadView })
    | MutationResult(MutationOutcome) | Previewed(PreviewResult)
  type CallPhase = Captured | PreparingCall | PreparedCall | Submitted | AwaitingReadApply
    | Committed | AwaitingApproval | WaitingOperation | Replied
  type CallState = {
    request: ApiRequest, phase: CallPhase, result: Option[ApiResult], waiterCancelled: bool,
    readIndex: Option[int],
    command: Option[MetaCommand], proposalSequence: int, commitIndex: Option[int],
    commitOutcome: Option[MutationOutcome], stagedVersion: Option[int],
    operationRequest: Option[OperationRequest],
  }
  type WorkPhase = WorkReading | WorkReadQueued | WorkApplying | WorkObserving | WorkPrepared | WorkSubmitted | WorkCommitted | WorkFinished
  type OperationWork = {
    request: OperationRequest, phase: WorkPhase, readIndex: Option[int],
    captured: Option[MetadataState], desired: Option[Document], input: Option[PlanningInput],
    queried: Set[GroupId], revalidated: bool, readRound: int,
    scopes: GroupId -> ScopeExpectation, command: Option[MetaCommand],
    commitIndex: Option[int], result: Option[MutationOutcome], replySent: bool,
  }
  type EffectDelivery = {
    delivery: int, sender: ProcessKey, senderMember: int, receiver: ProcessKey,
    proofIndex: Option[int],
    replica: ReplicaKey, grant: Grant, resource: ResourceKey,
  }
  type Lease = { phase: SessionPhase, epoch: int, probe: Option[int], failedProbe: bool }
  type Renewal = { key: SessionKey, round: int, member: int }
  type StartPhase = StartPreparing | StartAccepted | StartSucceeded | StartRefused
  type StartWork = { key: ReplicaKey, request: RequestKey, explicit: bool,
    floorIndex: Option[int], phase: StartPhase, error: Option[Refusal] }
  type HostFenceWork = { delivery: EffectDelivery, position: int }
  type ProcessState = {
    phase: ProcessPhase, incarnation: int, pendingCalls: RequestKey -> CallState,
    appliedMetaVersion: int, appliedMetaIndex: int, localView: ReadView,
    observations: GroupId -> Observation, owners: GroupId -> OwnerState,
    session: Option[SessionKey], schedulerEnabled: bool, serveOverride: int,
    schedulerCapture: Option[SchedulerCapture],
    operationWork: ProposalKey -> OperationWork,
    waitingEffects: Set[EffectDelivery], authorityReads: int -> int,
    nativeChecks: GrantKey -> NativeChange, joinOffer: Option[JoinOffer], effectResults: Set[Receipt],
    holds: Set[ReplicaKey], starts: RequestKey -> StartWork,
    fenceWorkers: Set[ResourceKey], metaRunning: bool, stopTimedOut: bool,
    ownLease: SessionPhase, leases: SessionKey -> Lease,
    renewal: Option[Renewal], renewalExpired: bool,
    revokeGate: Option[{ key: SessionKey, proposal: ProposalKey }],
  }
  type HostState = {
    pin: Option[DurableNodeIdentity], replicaEvidence: ReplicaKey -> StorageEvidence,
    authorityFloors: ResourceKey -> int, authorityGrants: ResourceKey -> Grant, receipts: Set[Receipt], fences: Set[FenceEvidence],
    journalPosition: int, pendingFences: ResourceKey -> HostFenceWork,
    // Established fixture abstracts already-consumed founding grants. Exact
    // retained identities keep their initial floor until an explicit grant wins.
    seedAuthority: Set[ReplicaKey],
    collectionMarkers: ReplicaKey -> CollectionState,
    metaImage: Option[ReadView], metaIndex: int, metaSeen: bool, metaRemoved: Option[ReplicaKey],
  }
  type NodeState = { volatile: ProcessState, durable: HostState }
  type LocalNode = { process_id: NodeName, volatile: ProcessState, durable: HostState }
  type NodeStates = NodeName -> LocalNode
  type ManagementMessage = JoinRequest({ owner: ProcessKey, member: int })
    | JoinAccepted(JoinOffer) | JoinPrepared(ProcessAdmission)
    | ApiCall({ key: RequestKey, request: ApiRequest })
    | ReadProbe(ReadTicket) | ReadAck({ ticket: ReadTicket, index: int })
    | MetaProposal({ key: ProposalKey, coordinator: ProcessKey, forWork: bool, command: MetaCommand })
    | MetaAck({ key: ProposalKey, coordinator: ProcessKey, forWork: bool, index: int, outcome: MutationOutcome })
    | SessionRenew(Renewal) | SessionRenewed(Renewal)
    | SessionProbe({ key: SessionKey, epoch: int, round: int, leader: ProcessKey })
    | SessionProbed({ key: SessionKey, epoch: int, round: int, leader: ProcessKey, observed: ProcessKey, member: int })
    | ExecuteGrant(EffectDelivery)
    | EffectResult({ delivery: EffectDelivery, receipt: Receipt })
    | OperationCall({ key: ProposalKey, request: OperationRequest })
    | OperationAck({ key: ProposalKey, outcome: MutationOutcome })
  type Channel = MetaChannel | LoadChannel
  type Envelope = {
    delivery: int, source: ProcessKey, destination: NodeName,
    channel: Channel, payload: ManagementMessage,
  }
  type NetworkState = {
    metaLinks: Set[(NodeName, NodeName)], loadLinks: Set[(NodeName, NodeName)],
    pending: Set[Envelope], delivered: Set[int],
  }
  // Bounded history monitor: captures actual receiver identity at consumption,
  // so subsequent restart/readmission cannot rewrite the safety witness.
  type ConsumptionEvidence = {
    delivery: EffectDelivery, pin: Option[DurableNodeIdentity], incarnation: int,
    owner: Option[ReplicaKey],
  }
  type Environment = {
    network: NetworkState, nextDelivery: int, metadata: MetadataState,
    consensus: GroupId -> ConsensusState, recovery: NodeName -> RecoveryState,
    storageConfig: NodeName -> NodeStorageConfig,
    metaLog: int -> MetadataState,
    consumptions: Set[ConsumptionEvidence],
  }
}
