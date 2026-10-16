package operator

import (
	"context"
	"errors"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// GroupEffects implements idempotent effects on the exact authorized load
// identities.
// Receivers validate the retained grant and return exact durable receipts.
type GroupEffects interface {
	Status(context.Context, string) (GroupStatus, error)
	ReadNative(context.Context, string, ...storage.Identity) (topology.NativeState, error)
	// DispatchEffect delivers one exact resource/target with the full grant.
	// Native targets are observed leader candidates; receivers verify leadership
	// and never reroute behind the controller's host accounting.
	DispatchEffect(context.Context, topology.AuthorityGrant, topology.AuthorityResource,
		topology.AuthorityTarget) (topology.EffectReceipt, error)
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

// Topology constants retain the original plan, role and state values during
// execution.
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

// Execution errors preserve the shared classifications used by topology and
// membership checks.
var (
	ErrOperationAuthorization = fault.ErrOperationAuthorization
	ErrGroupUnavailable       = fault.ErrUnavailable
	ErrNotMetaLeader          = fault.ErrNotLeader
	ErrReplicaNotReady        = fault.ErrReplicaNotReady
	ErrNoQuorum               = fault.ErrUnavailable
	ErrSafetyRefused          = fault.ErrUnsafeMembership
	ErrOperatorSuperseded     = errors.New("execution superseded")
	ErrVersionUnknown         = fault.ErrHistoryUnavailable
	ErrNotImplemented         = fault.ErrUnsupported
)

// Store supplies locally applied approved and converged topology views.
type Store interface {
	Target(context.Context) (Document, error)
	Active(context.Context) (Document, error)
}

// Record captures an exact execution attempt, plan, progress and retained
// authority evidence.
type Record struct {
	ID                                      ID
	Version                                 uint64
	Attempt                                 uint32
	State                                   State
	Steps                                   []Step
	VoterBases                              []topology.PlanVoterBase
	Grants                                  []topology.AuthorityGrant
	Receipts                                []topology.EffectReceipt
	CurrentStep                             int
	GroupID, NodeName, WaitingReason, Error string
	UpdatedAt                               time.Time
}

// FromPublic owns an operator's plan when adapting its public observation to
// execution state.
func FromPublic(r topology.Operator) Record {
	s := r.Status
	plan := r.Plan.Clone()
	return Record{
		ID:            s.ID,
		Version:       s.Version,
		Attempt:       s.Attempt,
		State:         s.State,
		Steps:         plan.Steps,
		VoterBases:    plan.VoterBases,
		CurrentStep:   s.CurrentStep,
		GroupID:       s.GroupID,
		NodeName:      s.NodeName,
		WaitingReason: s.WaitingReason,
		Error:         s.Error,
		UpdatedAt:     r.UpdatedAt,
	}
}

// Plan returns the record's plan view, sharing its step and voter-base slices.
func (r Record) Plan() topology.Plan {
	return topology.Plan{Version: r.Version, Steps: r.Steps, VoterBases: r.VoterBases}
}

// IDFor derives an operator-attempt identifier from the version and attempt.
func IDFor(version uint64, attempt uint32) ID {
	return topology.MakeOperatorID(version, attempt)
}
