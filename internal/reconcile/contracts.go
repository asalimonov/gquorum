package reconcile

import (
	"context"
	"github.com/asalimonov/gquorum/topology"
	"time"
)

const (
	ActionRepairReplica              = "repair_replica"
	ReconcileReasonConfigDisabled    = "config_disabled"
	ReconcileReasonNotLeader         = "not_leader"
	ReconcileReasonStarting          = "starting"
	ReconcileReasonConverged         = "converged"
	ReconcileReasonApprovedExecution = "approved_execution"
	ReconcileReasonMissingReplica    = "missing_replica"
	ReconcileReasonStagedLatest      = "staged_latest"
	ReconcileReasonStagedInternal    = "staged_internal"
	ReconcileReasonFailedExecution   = "failed_execution"
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
	CurrentExecution(context.Context) (topology.ExecutionRecord, bool, error)
	IsLeader() bool
	Health(context.Context) (map[string]topology.Health, error)
	GlobalStatus(context.Context, topology.Document) (map[string]topology.GroupStatus, error)
	PlanFor(context.Context, topology.Document) (topology.Plan, error)
	Approve(context.Context, uint64) (topology.ExecutionID, error)
	CommitRepair(context.Context, topology.Document, uint64, topology.Plan) (topology.ExecutionID, error)
	WakeConvergence()
}
type ReconcileInput struct {
	Latest, Target, Active topology.Document
	Status                 map[string]topology.GroupStatus
	Health                 map[string]topology.GroupHealthEvaluation
	Alive                  map[string]time.Duration
}
type ReconcileAction struct {
	Kind, GroupID, NodeName string
	ReplicaID               uint64
	Reason                  string
}
type ReconcileDecision struct {
	Action *ReconcileAction
	Reason string
}
type ReconcilePolicy interface {
	Name() string
	Evaluate(ReconcileInput) ReconcileDecision
}
