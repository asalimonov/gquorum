package load

import (
	"context"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type ReplicaRef struct {
	GroupID   string
	ReplicaID uint64
}
type ReplicaSpec struct {
	Identity        storage.Identity
	Node            topology.Identity
	Group           topology.Group
	ApprovedVersion uint64
}

// Application transfers machine lifetime ownership from a successful factory
// to Node. Close is idempotent, bounded by its context, and runs after engine
// work has joined. A failed Close retains ownership and prevents a successor.
// Callbacks may read status or RequestStop; they must not wait for their own
// group's lifecycle or a Stop that has to join them.
type Application struct {
	StateMachine StateMachine
	Close        func(context.Context) error
}
type RetirementReason string

const (
	RetirementTopology    RetirementReason = "topology"
	RetirementReadmission RetirementReason = "readmission"
)

// RetirementRequest authorizes cleanup of one exact application's retained
// replica resources. Topology and readmission proofs are deliberately distinct.
type RetirementRequest struct {
	Identity                          storage.Identity
	Reason                            RetirementReason
	Authorization                     *topology.AuthorizedStep
	AdmissionID                       string
	PreviousMemberID, GrantedMemberID uint64
}

// RetirementResult echoes the entire verified tuple. Verified is meaningful
// only after the callback has verified its own durable application identity.
// Idempotent verification must also succeed after already-completed cleanup.
type RetirementResult struct {
	Identity                          storage.Identity
	Reason                            RetirementReason
	Authorization                     *topology.AuthorizedStep
	AdmissionID                       string
	PreviousMemberID, GrantedMemberID uint64
	Verified                          bool
}
