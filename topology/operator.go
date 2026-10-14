package topology

import (
	"fmt"
	"slices"
)

// OperatorID identifies an execution attempt for an approved topology version.
type OperatorID string

// OperatorState describes the current execution state of an approved plan.
type OperatorState string

// OperatorPending and the following states describe execution, waits and
// terminal outcomes.
const (
	OperatorPending           OperatorState = "pending"
	OperatorRunning           OperatorState = "running"
	OperatorWaitingForReplica OperatorState = "waiting_for_replica"
	OperatorWaitingForCatchUp OperatorState = "waiting_for_catch_up"
	OperatorWaitingForLeader  OperatorState = "waiting_for_leader"
	OperatorSucceeded         OperatorState = "succeeded"
	OperatorFailed            OperatorState = "failed"
	OperatorSuperseded        OperatorState = "superseded"
)

// Terminal reports whether execution succeeded, failed or was superseded.
func (s OperatorState) Terminal() bool {
	return s == OperatorSucceeded || s == OperatorFailed || s == OperatorSuperseded
}

// Waiting reports whether execution awaits a replica, catch-up or leadership.
func (s OperatorState) Waiting() bool {
	return s == OperatorWaitingForReplica || s == OperatorWaitingForCatchUp || s == OperatorWaitingForLeader
}

// StepCreateFoundingGroup and the following kinds identify replica, membership
// and cleanup steps.
const (
	StepCreateFoundingGroup StepKind = "create_founding_group"
	StepPrepareReplica      StepKind = "prepare_replica"
	StepAddLearner          StepKind = "add_learner"
	StepWaitCaughtUp        StepKind = "wait_caught_up"
	StepPromoteVoter        StepKind = "promote_voter"
	StepDemoteVoter         StepKind = "demote_voter"
	StepRemoveReplica       StepKind = "remove_replica"
	StepTransferLeader      StepKind = "transfer_leader"
	StepStopReplica         StepKind = "stop_replica"
	StepCleanupReplicaState StepKind = "cleanup_replica_state"
	StepDeleteGroupState    StepKind = "delete_group_state"
)

// Equal compares every operation and identity field of two steps.
func (s Step) Equal(other Step) bool { return s == other }

// Clone copies plan steps, voter bases and their nested voter slices.
func (p Plan) Clone() Plan {
	p.Steps = slices.Clone(p.Steps)
	p.VoterBases = slices.Clone(p.VoterBases)
	for i := range p.VoterBases {
		p.VoterBases[i].Voters = slices.Clone(p.VoterBases[i].Voters)
	}
	return p
}

// Equal compares the version, ordered steps and exact ordered voter bases.
func (p Plan) Equal(other Plan) bool {
	if p.Version != other.Version || len(p.VoterBases) != len(other.VoterBases) || !slices.Equal(p.Steps, other.Steps) {
		return false
	}
	for i, base := range p.VoterBases {
		if base.GroupID != other.VoterBases[i].GroupID || !slices.Equal(base.Voters, other.VoterBases[i].Voters) {
			return false
		}
	}
	return true
}

// IsEmpty reports whether the plan has no execution steps.
func (p Plan) IsEmpty() bool { return len(p.Steps) == 0 }

// Matches compares the complete authorization tuple with an expected operator
// step.
func (a AuthorizedStep) Matches(id OperatorID, attempt uint32, version uint64, step Step) bool {
	return a.OperatorID == id && a.Attempt == attempt && a.Version == version && a.StepSeq == step.Seq &&
		a.Kind == step.Kind && a.GroupID == step.GroupID && a.NodeName == step.NodeName &&
		a.ReplicaID == step.ReplicaID && a.TransferNodeName == step.TransferNodeName &&
		a.TransferReplicaID == step.TransferReplicaID && a.ExpectedVotersDigest == step.ExpectedVotersDigest &&
		a.ResultVotersDigest == step.ResultVotersDigest
}

// OperatorStatus reports execution progress, current subject and diagnostic
// failure or wait.
type OperatorStatus struct {
	ID            OperatorID    `json:"id"`
	Version       uint64        `json:"version"`
	Attempt       uint32        `json:"attempt"`
	State         OperatorState `json:"state"`
	CurrentStep   int           `json:"current_step"`
	TotalSteps    int           `json:"total_steps"`
	GroupID       string        `json:"group_id,omitempty"`
	NodeName      string        `json:"node_name,omitempty"`
	WaitingReason string        `json:"waiting_reason,omitempty"`
	ErrorCode     string        `json:"error_code,omitempty"`
	Error         string        `json:"error,omitempty"`
}

// MakeOperatorID derives an execution identifier from a version and attempt
// number.
func MakeOperatorID(version uint64, attempt uint32) OperatorID {
	return OperatorID(fmt.Sprintf("exec-v%d-a%d", version, attempt))
}

// Valid reports whether a state is part of the execution protocol.
func (s OperatorState) Valid() bool {
	return s == OperatorPending || s == OperatorRunning || s.Waiting() || s.Terminal()
}
