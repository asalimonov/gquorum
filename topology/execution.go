package topology

import (
	"fmt"
	"time"
)

type ExecutionID string
type ExecutionState string

const (
	ExecutionPending           ExecutionState = "pending"
	ExecutionRunning           ExecutionState = "running"
	ExecutionWaitingForReplica ExecutionState = "waiting_for_replica"
	ExecutionWaitingForCatchUp ExecutionState = "waiting_for_catch_up"
	ExecutionWaitingForLeader  ExecutionState = "waiting_for_leader"
	ExecutionSucceeded         ExecutionState = "succeeded"
	ExecutionFailed            ExecutionState = "failed"
	ExecutionSuperseded        ExecutionState = "superseded"
)

func (s ExecutionState) Terminal() bool {
	return s == ExecutionSucceeded || s == ExecutionFailed || s == ExecutionSuperseded
}

type StepKind string

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

type Step struct {
	Seq       int      `json:"seq"`
	Kind      StepKind `json:"kind"`
	GroupID   string   `json:"group_id"`
	NodeName  string   `json:"node_name,omitempty"`
	ReplicaID uint64   `json:"replica_id,omitempty"`
}

type Plan struct {
	Version uint64 `json:"version"`
	Steps   []Step `json:"steps"`
}

type AuthorizedStep struct {
	ExecutionID ExecutionID `json:"execution_id"`
	Version     uint64      `json:"version"`
	Attempt     uint32      `json:"attempt"`
	StepSeq     int         `json:"step_seq"`
	Kind        StepKind    `json:"kind"`
	GroupID     string      `json:"group_id"`
	NodeName    string      `json:"node_name,omitempty"`
	ReplicaID   uint64      `json:"replica_id,omitempty"`
}

func (a AuthorizedStep) Matches(id ExecutionID, attempt uint32, version uint64, step Step) bool {
	return a.ExecutionID == id && a.Attempt == attempt && a.Version == version && a.StepSeq == step.Seq && a.Kind == step.Kind && a.GroupID == step.GroupID && a.NodeName == step.NodeName && a.ReplicaID == step.ReplicaID
}

type ExecutionStatus struct {
	ID            ExecutionID    `json:"id"`
	Version       uint64         `json:"version"`
	Attempt       uint32         `json:"attempt"`
	State         ExecutionState `json:"state"`
	CurrentStep   int            `json:"current_step"`
	TotalSteps    int            `json:"total_steps"`
	WaitingReason string         `json:"waiting_reason,omitempty"`
	ErrorCode     string         `json:"error_code,omitempty"`
	Error         string         `json:"error,omitempty"`
}

type ExecutionRecord struct {
	Status    ExecutionStatus `json:"status"`
	Plan      Plan            `json:"plan"`
	Actor     string          `json:"actor"`
	Origin    Origin          `json:"origin"`
	StartedAt time.Time       `json:"started_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

func MakeExecutionID(version uint64, attempt uint32) ExecutionID {
	return ExecutionID(fmt.Sprintf("v%d-a%d", version, attempt))
}
