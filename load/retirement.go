package load

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// Verify checks that r is a complete, internally consistent retirement proof
// for the application resources of id: exact identity, a valid grant for id's
// cluster and group, the host resource for id's node, a target naming this
// replica, and the reason-specific tuple. Topology retirement needs an effect
// grant whose action equals Authorization and cleans this replica or deletes
// its group. Readmission needs a readmit grant, its admission ID and a granted
// member ID greater than the previous one.
//
// Verify does not prove that the grant was committed through _meta; Node checks
// that before invoking LoadOptions.Retire. Failures return
// fault.ErrOperationAuthorization.
func (r RetirementRequest) Verify(id storage.Identity) error {
	denied := fault.ErrOperationAuthorization
	if r.Identity != id || id.ClusterID == "" || id.GroupID == "" || id.NodeName == "" || id.ReplicaID == 0 {
		return denied
	}
	if r.Grant.Key.ClusterID != id.ClusterID || r.Grant.Key.GroupID != id.GroupID || r.Grant.Validate() != nil {
		return denied
	}
	host := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: id.GroupID, NodeName: id.NodeName}
	if !slices.Contains(r.Grant.Resources, host) {
		return denied
	}
	var member uint64
	for _, target := range r.Grant.Targets {
		if target.NodeName == id.NodeName && target.ReplicaID == id.ReplicaID {
			member = target.MemberID
		}
	}
	if member == 0 {
		return denied
	}
	switch r.Reason {
	case RetirementTopology:
		return r.verifyTopologyRetirement(id)
	case RetirementReadmission:
		return r.verifyReadmissionRetirement(member)
	default:
		return denied
	}
}

func (r RetirementRequest) verifyTopologyRetirement(id storage.Identity) error {
	denied := fault.ErrOperationAuthorization
	a := r.Authorization
	if r.Grant.Purpose != topology.AuthorityEffect || a == nil || r.Grant.Action == nil || *a != *r.Grant.Action ||
		r.AdmissionID != "" || r.PreviousMemberID != 0 || r.GrantedMemberID != 0 {
		return denied
	}
	switch a.Kind {
	case topology.StepDeleteGroupState:
	case topology.StepCleanupReplicaState:
		if a.NodeName != id.NodeName || a.ReplicaID != id.ReplicaID {
			return denied
		}
	default:
		return denied
	}
	return nil
}

func (r RetirementRequest) verifyReadmissionRetirement(member uint64) error {
	denied := fault.ErrOperationAuthorization
	if r.Grant.Purpose != topology.AuthorityReadmit || r.Authorization != nil || r.AdmissionID == "" ||
		r.AdmissionID != r.Grant.AdmissionID || r.PreviousMemberID != member ||
		r.GrantedMemberID <= r.PreviousMemberID {
		return denied
	}
	return nil
}

// Result echoes the entire request tuple with Verified=true, using owned
// copies. Return it from LoadOptions.Retire only after verifying identity and
// completing (or confirming completed) cleanup.
func (r RetirementRequest) Result() RetirementResult {
	result := RetirementResult{
		Grant:            r.Grant.Clone(),
		Identity:         r.Identity,
		Reason:           r.Reason,
		AdmissionID:      r.AdmissionID,
		PreviousMemberID: r.PreviousMemberID,
		GrantedMemberID:  r.GrantedMemberID,
		Verified:         true,
	}
	if r.Authorization != nil {
		proof := *r.Authorization
		result.Authorization = &proof
	}
	return result
}

// RetirementReason identifies the authority path that permits application
// cleanup.
type RetirementReason string

// Retirement reasons distinguish approved topology cleanup from readmission
// replacement.
const (
	RetirementTopology    RetirementReason = "topology"
	RetirementReadmission RetirementReason = "readmission"
)

// RetirementRequest authorizes cleanup of one exact application's retained
// replica resources. Topology and readmission proofs are deliberately distinct.
type RetirementRequest struct {
	Grant                             topology.AuthorityGrant
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
	Grant                             topology.AuthorityGrant
	Identity                          storage.Identity
	Reason                            RetirementReason
	Authorization                     *topology.AuthorizedStep
	AdmissionID                       string
	PreviousMemberID, GrantedMemberID uint64
	Verified                          bool
}
