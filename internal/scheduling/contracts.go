package scheduling

import (
	"context"

	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

// Repair reason codes identify local scheduling gates and diagnostic decisions.
const (
	ReconcileReasonConfigDisabled    = "config_disabled"
	ReconcileReasonNotLeader         = "not_leader"
	ReconcileReasonStarting          = "starting"
	ReconcileReasonConverged         = "converged"
	ReconcileReasonApprovedOperation = "approved_execution"
	ReconcileReasonMissingReplica    = "missing_replica"
	ReconcileReasonQuorumLost        = "quorum_lost"
	ReconcileReasonStabilityWindow   = "stability_window"
	ReconcileReasonCooldown          = "cooldown"
	ReconcileReasonCASConflict       = "cas_conflict"
	ReconcileReasonPolicyRejected    = "policy_rejected"
)

// RepairCommit binds the desired document and plan to its observed CAS version.
type RepairCommit struct {
	Document        topology.Document
	ExpectedVersion uint64
	ExpectedTarget  uint64
	Scopes          []metaengine.ScopeExpectation
	Plan            topology.Plan
}

// Host supplies coherent metadata captures, scoped native observations and
// replicated repair commits. Empty-group reads capture metadata only; a named
// group reads native state and recaptures it. PlanRepair revalidates its capture.
type Host interface {
	IsLeader() bool
	Health(context.Context) (map[string]topology.Health, error)
	RefreshObservations(context.Context) error
	ResetObservations()
	Observations() metrics.ClusterSnapshot
	VoterSwitches() []topology.VoterSwitchState
	WakeOperations()
	ReadRepair(context.Context, string) (metaengine.PlanRead, error)
	PlanRepair(context.Context, metaengine.PlanRead, topology.Document) (topology.Plan, error)
	CommitRepair(context.Context, RepairCommit) (topology.OperationID, error)
	CommitVoterChange(context.Context, topology.VoterChange, RepairCommit) (topology.OperationID, error)
}

// Enablement controls whether this node may originate automatic repairs.
type Enablement struct{ Enabled bool }
