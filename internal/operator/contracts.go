package operator

import (
	"context"

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
}

// Topology constants retain the original plan, role and state values during
// execution.
const (
	MetaGroupID                   = topology.MetaGroupID
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
)

// Record identifies the immutable attempt and cursor used by native simulation.
type Record struct {
	ID          topology.OperationID
	Version     uint64
	Attempt     uint32
	Steps       []Step
	VoterBases  []topology.PlanVoterBase
	CurrentStep int
}

// Plan returns the record's plan view, sharing its step and voter-base slices.
func (r Record) Plan() topology.Plan {
	return topology.Plan{Version: r.Version, Steps: r.Steps, VoterBases: r.VoterBases}
}
