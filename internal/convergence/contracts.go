package convergence

import (
	"context"
	"errors"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	"time"
)

type Document = topology.Document
type Rules = topology.Rules
type Limits = topology.Limits
type ExecutionID = topology.ExecutionID
type ExecutionState = topology.ExecutionState
type ExecutionStatus = topology.ExecutionStatus
type ExecutionTransition = topology.ExecutionTransition
type Step = topology.Step
type StepKind = topology.StepKind
type Plan = topology.Plan
type AuthorizedStep = topology.AuthorizedStep
type GroupStatus = topology.GroupStatus
type ReplicaProgress = topology.ReplicaProgress
type GroupController = topology.GroupController
type Group = topology.Group
type Replica = topology.Replica
type PlacementRule = topology.PlacementRule
type Node = topology.Node
type ChangeAudit = topology.ChangeAudit

const (
	MetaGroupID                   = topology.MetaGroupID
	ExecutionPending              = topology.ExecutionPending
	ExecutionRunning              = topology.ExecutionRunning
	ExecutionSucceeded            = topology.ExecutionSucceeded
	ExecutionFailed               = topology.ExecutionFailed
	ExecutionSuperseded           = topology.ExecutionSuperseded
	ExecutionWaitingForReplica    = topology.ExecutionWaitingForReplica
	ExecutionWaitingForCatchUp    = topology.ExecutionWaitingForCatchUp
	ExecutionWaitingForLeader     = topology.ExecutionWaitingForLeader
	StepCreateFoundingGroup       = topology.StepCreateFoundingGroup
	StepPrepareReplica            = topology.StepPrepareReplica
	StepAddLearner                = topology.StepAddLearner
	StepWaitCaughtUp              = topology.StepWaitCaughtUp
	StepPromoteVoter              = topology.StepPromoteVoter
	StepDemoteVoter               = topology.StepDemoteVoter
	StepRemoveReplica             = topology.StepRemoveReplica
	StepTransferLeader            = topology.StepTransferLeader
	StepStopReplica               = topology.StepStopReplica
	StepCleanupReplicaState       = topology.StepCleanupReplicaState
	StepDeleteGroupState          = topology.StepDeleteGroupState
	ReplicaVoter                  = topology.ReplicaVoter
	ReplicaLearner                = topology.ReplicaLearner
	NodeActive                    = topology.NodeActive
	NodeExcluded                  = topology.NodeExcluded
	NodeDraining                  = topology.NodeDraining
	NodeDrained                   = topology.NodeDrained
	AuditOperationForceRetireNode = topology.AuditOperationForceRetireNode
	ExecutionWaitingReplica       = topology.ExecutionWaitingForReplica
	ExecutionWaitingCatchUp       = topology.ExecutionWaitingForCatchUp
	ExecutionWaitingLeader        = topology.ExecutionWaitingForLeader
)

var (
	ErrExecutionAuthorization = fault.ErrExecutionAuthorization
	ErrGroupUnavailable       = fault.ErrUnavailable
	ErrNotMetaLeader          = fault.ErrNotLeader
	ErrReplicaNotReady        = fault.ErrReplicaNotReady
	ErrNoQuorum               = fault.ErrUnavailable
	ErrSafetyRefused          = fault.ErrUnsafeMembership
	ErrExecutionSuperseded    = errors.New("execution superseded")
	ErrVersionUnknown         = fault.ErrHistoryUnavailable
	ErrNotImplemented         = fault.ErrUnsupported
)

type Store interface {
	Target(context.Context) (Document, error)
	Active(context.Context) (Document, error)
}
type ExecutionCoordinator interface {
	IsMetaLeader() bool
	CurrentExecution(context.Context) (ExecutionRecord, bool)
	RecordExecutionTransition(context.Context, ExecutionTransition) error
}
type MembershipCoordinator interface {
	Apply(context.Context, AuthorizedStep) error
	CaughtUp(GroupStatus, uint64) bool
}
type ExecutionRecord struct {
	ID                                      ExecutionID
	Version                                 uint64
	Attempt                                 uint32
	State                                   ExecutionState
	Steps                                   []Step
	CurrentStep                             int
	GroupID, NodeName, WaitingReason, Error string
	UpdatedAt                               time.Time
}

func FromPublic(r topology.ExecutionRecord) ExecutionRecord {
	s := r.Status
	return ExecutionRecord{ID: s.ID, Version: s.Version, Attempt: s.Attempt, State: s.State, Steps: append([]Step(nil), r.Plan.Steps...), CurrentStep: s.CurrentStep, GroupID: s.GroupID, NodeName: s.NodeName, WaitingReason: s.WaitingReason, Error: s.Error, UpdatedAt: r.UpdatedAt}
}
func ExecutionIDFor(version uint64, attempt uint32) ExecutionID {
	return topology.MakeExecutionID(version, attempt)
}
