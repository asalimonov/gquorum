package topology

import "fmt"

// OperationID identifies one execution attempt of an approved topology version.
type OperationID string

// OperationState describes execution, waiting or a terminal block/operation result.
type OperationState string

// OperationPending and the following states retain the execution protocol values.
const (
	OperationPending           OperationState = "pending"
	OperationRunning           OperationState = "running"
	OperationWaitingForReplica OperationState = "waiting_for_replica"
	OperationWaitingForCatchUp OperationState = "waiting_for_catch_up"
	OperationWaitingForLeader  OperationState = "waiting_for_leader"
	OperationSucceeded         OperationState = "succeeded"
	OperationFailed            OperationState = "failed"
	OperationSuperseded        OperationState = "superseded"
)

// MakeOperationID derives an execution identifier from its version and attempt.
func MakeOperationID(version uint64, attempt uint32) OperationID {
	return OperationID(fmt.Sprintf("exec-v%d-a%d", version, attempt))
}

// Terminal reports whether execution succeeded, failed or was superseded.
func (s OperationState) Terminal() bool {
	return s == OperationSucceeded || s == OperationFailed || s == OperationSuperseded
}

// Waiting reports whether execution awaits a replica, catch-up or leadership.
func (s OperationState) Waiting() bool {
	return s == OperationWaitingForReplica || s == OperationWaitingForCatchUp || s == OperationWaitingForLeader
}

// Valid reports whether the state belongs to the execution protocol.
func (s OperationState) Valid() bool {
	return s == OperationPending || s == OperationRunning || s.Waiting() || s.Terminal()
}
