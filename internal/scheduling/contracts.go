package scheduling

import (
	"context"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

const (
	ReconcileReasonConfigDisabled    = "config_disabled"
	ReconcileReasonNotLeader         = "not_leader"
	ReconcileReasonStarting          = "starting"
	ReconcileReasonConverged         = "converged"
	ReconcileReasonApprovedOperator  = "approved_execution"
	ReconcileReasonMissingReplica    = "missing_replica"
	ReconcileReasonStagedLatest      = "staged_latest"
	ReconcileReasonStagedInternal    = "staged_internal"
	ReconcileReasonFailedOperator    = "failed_execution"
	ReconcileReasonQuorumLost        = "quorum_lost"
	ReconcileReasonUnknownHealth     = "unknown_health"
	ReconcileReasonStabilityWindow   = "stability_window"
	ReconcileReasonCooldown          = "cooldown"
	ReconcileReasonStatusUnavailable = "status_unavailable"
	ReconcileReasonCASConflict       = "cas_conflict"
	ReconcileReasonPolicyRejected    = "policy_rejected"
)

type Host interface {
	Views(context.Context) (topology.Document, topology.Document, topology.Document, error)
	CurrentOperator(context.Context) (topology.Operator, bool, error)
	IsLeader() bool
	Health(context.Context) (map[string]topology.Health, error)
	GlobalStatus(context.Context, topology.Document) (map[string]topology.GroupStatus, error)
	PlanFor(context.Context, topology.Document) (topology.Plan, error)
	Approve(context.Context, uint64) (topology.OperatorID, error)
	CommitRepair(context.Context, topology.Document, uint64, topology.Plan) (topology.OperatorID, error)
	RefreshObservations(context.Context) error
	// ResetObservations is an internal, in-memory reset invoked under the
	// scheduler gate before another enabled tick can reuse temporal evidence.
	ResetObservations()
	Observations() metrics.ClusterSnapshot
	VoterSwitches() []topology.VoterSwitchState
	CommitVoterChange(context.Context, topology.VoterChange, topology.Document, uint64, topology.Plan) (topology.OperatorID, error)
	WakeOperator()
}
