package operator

import (
	"context"
	"errors"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"time"
)

type Document = topology.Document
type Rules = topology.Rules
type Limits = topology.Limits
type OperatorID = topology.OperatorID
type OperatorState = topology.OperatorState
type OperatorStatus = topology.OperatorStatus
type OperatorTransition = topology.OperatorTransition
type Step = topology.Step
type StepKind = topology.StepKind
type Plan = topology.Plan
type AuthorizedStep = topology.AuthorizedStep
type GroupStatus = topology.GroupStatus
type ReplicaProgress = topology.ReplicaProgress

// GroupEffects implements idempotent effects on the exact authorized load identities.
// Receivers validate the retained grant and return exact durable receipts.
type GroupEffects interface {
	Status(context.Context, string) (GroupStatus, error)
	ReadNative(context.Context, string, ...storage.Identity) (topology.NativeState, error)
	// CreateGroup seeds only the approved founding voters. Intentional learners
	// are prepared and admitted by later exact steps.
	CreateGroup(context.Context, topology.AuthorityGrant) ([]topology.EffectReceipt, error)
	PrepareReplica(context.Context, topology.AuthorityGrant) ([]topology.EffectReceipt, error)
	ApplyMembership(context.Context, topology.AuthorityGrant) ([]topology.EffectReceipt, error)
	TransferLeader(context.Context, topology.AuthorityGrant) ([]topology.EffectReceipt, error)
	StopReplica(context.Context, topology.AuthorityGrant) ([]topology.EffectReceipt, error)
	CleanupReplicaState(context.Context, topology.AuthorityGrant) ([]topology.EffectReceipt, error)
	DeleteGroupState(context.Context, topology.AuthorityGrant) ([]topology.EffectReceipt, error)
}

type Group = topology.Group
type Replica = topology.Replica
type PlacementRule = topology.PlacementRule
type Node = topology.Node
type ChangeAudit = topology.ChangeAudit

const (
	MetaGroupID                   = topology.MetaGroupID
	OperatorPending               = topology.OperatorPending
	OperatorRunning               = topology.OperatorRunning
	OperatorSucceeded             = topology.OperatorSucceeded
	OperatorFailed                = topology.OperatorFailed
	OperatorSuperseded            = topology.OperatorSuperseded
	OperatorWaitingForReplica     = topology.OperatorWaitingForReplica
	OperatorWaitingForCatchUp     = topology.OperatorWaitingForCatchUp
	OperatorWaitingForLeader      = topology.OperatorWaitingForLeader
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
	OperatorWaitingReplica        = topology.OperatorWaitingForReplica
	OperatorWaitingCatchUp        = topology.OperatorWaitingForCatchUp
	OperatorWaitingLeader         = topology.OperatorWaitingForLeader
)

var (
	ErrOperatorAuthorization = fault.ErrOperatorAuthorization
	ErrGroupUnavailable      = fault.ErrUnavailable
	ErrNotMetaLeader         = fault.ErrNotLeader
	ErrReplicaNotReady       = fault.ErrReplicaNotReady
	ErrNoQuorum              = fault.ErrUnavailable
	ErrSafetyRefused         = fault.ErrUnsafeMembership
	ErrOperatorSuperseded    = errors.New("execution superseded")
	ErrVersionUnknown        = fault.ErrHistoryUnavailable
	ErrNotImplemented        = fault.ErrUnsupported
)

type Store interface {
	Target(context.Context) (Document, error)
	Active(context.Context) (Document, error)
}
type OperatorCoordinator interface {
	IsMetaLeader() bool
	CurrentOperator(context.Context) (Record, bool)
	RecordOperatorTransition(context.Context, OperatorTransition) error
}
type AuthorityCoordinator interface {
	BeginEffect(context.Context, topology.AuthorizedStep) (topology.AuthorityGrant, error)
	ReconcileFences(context.Context) error
	EffectReceipts(context.Context, topology.AuthorityGrant) ([]topology.EffectReceipt, error)
	RecordEffect(context.Context, topology.EffectReceipt) error
}
type MembershipCoordinator interface {
	Apply(context.Context, topology.AuthorityGrant) ([]topology.EffectReceipt, error)
	CaughtUp(GroupStatus, uint64) bool
}
type Record struct {
	ID                                      OperatorID
	Version                                 uint64
	Attempt                                 uint32
	State                                   OperatorState
	Steps                                   []Step
	CurrentStep                             int
	GroupID, NodeName, WaitingReason, Error string
	UpdatedAt                               time.Time
}

func FromPublic(r topology.Operator) Record {
	s := r.Status
	return Record{ID: s.ID, Version: s.Version, Attempt: s.Attempt, State: s.State, Steps: append([]Step(nil), r.Plan.Steps...), CurrentStep: s.CurrentStep, GroupID: s.GroupID, NodeName: s.NodeName, WaitingReason: s.WaitingReason, Error: s.Error, UpdatedAt: r.UpdatedAt}
}
func OperatorIDFor(version uint64, attempt uint32) OperatorID {
	return topology.MakeOperatorID(version, attempt)
}
