package metaengine

import (
	"errors"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

var errFoundingSeedMismatch = errors.New("meta: applied metadata seed contradicts founding document")

// FI_FOUNDING_SEED: only the native configuration at the committed command's
// position can establish the initial metadata checkpoint without effect grants.
// Caller plan bases and leader observations are not replicated completion proof.
func (s *scopedState) initialFounding(desired topology.Document) bool {
	return s.Target.Version == 0 && len(s.Groups) == 0 && len(s.Operations) == 0 &&
		len(s.AuthorityRecords) == 0 && len(s.Cleanup) == 0 && len(s.Admissions) == 0 &&
		desired.Version == 1 && desired.Origin == topology.OriginFounding
}

func exactSeedMembership(group *topology.Group, seed raftpb.ConfState) bool {
	if group == nil || seed.AutoLeave || len(seed.VotersOutgoing) != 0 || len(seed.LearnersNext) != 0 ||
		len(seed.Voters) == 0 || len(group.Replicas) != len(seed.Voters)+len(seed.Learners) {
		return false
	}
	for _, replica := range group.Replicas {
		members := seed.Voters
		if replica.Role == topology.ReplicaLearner {
			members = seed.Learners
		}
		if !slices.Contains(members, replica.ReplicaID) {
			return false
		}
	}
	return true
}

func (s *scopedState) validateFoundingApproval(desired topology.Document, plan topology.Plan) error {
	if s.Target.Version != 0 {
		return nil
	}
	if !s.initialFounding(desired) {
		return fault.New(fault.OperationAuthorization, "founding approval differs from applied metadata seed", nil)
	}
	for _, step := range plan.Steps {
		if step.GroupID == topology.MetaGroupID {
			return fault.ErrOperationAuthorization
		}
	}
	// The initial configuration is local seed input, not replicated metadata.
	// A divergent seed must stop this replica, never produce a different Apply
	// result/checkpoint from peers processing the identical approved document.
	if s.nativeConf == nil || !exactSeedMembership(desired.MetaGroup(), *s.nativeConf) {
		return errFoundingSeedMismatch
	}
	return nil
}
