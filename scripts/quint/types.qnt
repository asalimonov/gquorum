module types {
  import basicSpells.* from "./vendor/spells/basicSpells"

  type NodeName = str
  type GroupId = str
  type ProcessKey = { node: NodeName, incarnation: int }
  type RequestKey = { caller: ProcessKey, sequence: int }
  type ReplicaKey = { cluster: str, group: GroupId, node: NodeName, member: int, replica: int }
  type DurableNodeIdentity = { cluster: str, node: NodeName, member: int }
  type AttemptKey = { version: int, attempt: int }
  type Audit = { actor: str, comment: str }
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
    replicas: int, allowedTypes: Set[str], selector: str -> str, voterDomain: Option[str],
  }
  type HostingMode = AllEligible | ExplicitHosts
  type HostingRule = { mode: HostingMode, selectedNodes: Set[NodeName] }
  type VoterConfig = { target: int, domain: Option[str] }
  type GroupDecl = {
    id: GroupId, storage: StorageClass, labels: str -> str, replicas: Set[ReplicaDecl],
    placement: Option[PlacementRule], hosting: Option[HostingRule],
    voterConfig: Option[VoterConfig],
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
  }
  type ReceiptOutcome = EffectApplied | AlreadySatisfied | TransferStarted
  type Receipt = {
    grant: GrantKey, effect: Option[EffectKey], admission: Option[AdmissionKey],
    target: ReplicaKey, resource: ResourceKey, outcome: ReceiptOutcome,
  }
  type FenceEvidence = {
    grant: GrantKey, resource: ResourceKey, receiver: DurableNodeIdentity,
    replica: Option[ReplicaKey], durablePosition: int,
  }
  type Revocation = {
    requested: bool, requiredResources: Set[ResourceKey], provenResources: Set[ResourceKey],
    evidence: Set[FenceEvidence],
  }
  type PlannedStep = {
    kind: ActionKind, target: EffectTarget, resources: Set[ResourceKey],
    targets: Set[ReplicaKey],
  }
  type Plan = { version: int, blocks: GroupId -> List[PlannedStep] }
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
    stopGrant: GrantKey, stopped: Option[Receipt], retired: Option[Receipt],
    collected: Option[Receipt], retention: RetentionClass, collectionPhase: CollectionPhase,
  }
  type CollectionState = {
    identity: ReplicaKey, grant: GrantKey, phase: CollectionPhase, token: int,
  }
  type SessionKey = { owner: ProcessKey, generation: int }
  type SessionPhase = Live | ExpiredObserved | Revoked
  type Session = { owner: ProcessKey, phase: SessionPhase, renewalGeneration: int }
  type MetadataState = {
    latest: Document, target: Document, history: int -> Document,
    groups: GroupId -> GroupProgress, nodes: NodeName -> NodeProgress,
    operations: AttemptKey -> Operation, firstAttempts: int -> AttemptKey,
    successors: AttemptKey -> AttemptKey, grants: GrantKey -> Grant,
    revocations: GrantKey -> Revocation, cleanup: ReplicaKey -> CleanupDebt,
    memberAllocationFloor: int, replicaAllocationFloors: GroupId -> int,
    retiredGroupIds: Set[GroupId], sessions: SessionKey -> Session, locks: str -> SessionKey,
  }
  type CatchUpClass = Behind | CaughtUp | CatchUpUnknown
  type NativeChange = { kind: ActionKind, target: ReplicaKey, sourceConfig: int }
  type ConsensusState = {
    lineage: str, configRevision: int, voters: Set[ReplicaKey], learners: Set[ReplicaKey],
    leaderAuthority: Option[ReplicaKey], committedPosition: int,
    appliedPositions: ReplicaKey -> int, catchUp: ReplicaKey -> CatchUpClass,
    pendingChange: Option[NativeChange],
  }
  type HealthClass = Healthy | Unreachable | HealthUnknown
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
  type ApiRequest = ObserveLocal | InvalidRequest
  type ApiResult = Observed(ProcessPhase) | Refused(Refusal) | Indeterminate
  type CallPhase = Captured | Submitted | Committed | Replied
  type CallState = {
    request: ApiRequest, phase: CallPhase, result: Option[ApiResult], waiterCancelled: bool,
  }
  type ProcessState = {
    phase: ProcessPhase, incarnation: int, pendingCalls: RequestKey -> CallState,
    appliedMetaVersion: int, appliedMetaIndex: int, localView: ReadView,
    observations: GroupId -> Observation, owners: GroupId -> OwnerState,
    session: Option[SessionKey], schedulerEnabled: bool, serveOverride: int,
    schedulerCapture: Option[SchedulerCapture],
  }
  type HostState = {
    pin: Option[DurableNodeIdentity], replicaEvidence: ReplicaKey -> StorageEvidence,
    authorityFloors: ResourceKey -> int, receipts: Set[Receipt], fences: Set[FenceEvidence],
    collectionMarkers: ReplicaKey -> CollectionState,
  }
  type NodeState = { volatile: ProcessState, durable: HostState }
  type ManagementMessage = ApiCall({ key: RequestKey, request: ApiRequest })
  type Channel = MetaChannel | LoadChannel
  type Envelope = {
    delivery: int, source: ProcessKey, destination: NodeName,
    channel: Channel, payload: ManagementMessage,
  }
  type NetworkState = {
    metaLinks: Set[(NodeName, NodeName)], loadLinks: Set[(NodeName, NodeName)],
    pending: Set[Envelope], delivered: Set[int],
  }
  type Environment = {
    network: NetworkState, nextDelivery: int, metadata: MetadataState,
    consensus: GroupId -> ConsensusState, recovery: NodeName -> RecoveryState,
    storageConfig: NodeName -> NodeStorageConfig,
  }
}
