package topology

import (
	"fmt"

	"github.com/asalimonov/gquorum/fault"
)

// MetaDrainInput supplies actual metadata membership, observed health and the
// final-convergence phase for a drain safety check.
type MetaDrainInput struct {
	Status GroupStatus
	Health map[string]Health
	Final  bool
}

// ValidateMetaDrainSafety evaluates the actual _meta voter set, never the
// declared target quorum. It returns a known-alive retained voter to use for a
// leadership transfer when the current leader is departing.
func ValidateMetaDrainSafety(planning PlanInput, target Document, input MetaDrainInput) (uint64, error) {
	status, health := input.Status, input.Health
	owners, err := metaMemberOwners(planning, target)
	if err != nil {
		return 0, err
	}
	departing := departingMetaNodes(planning, status, owners, target)
	if len(departing) == 0 {
		return 0, nil
	}
	if forcedRetirementOf(target, departing) {
		return 0, nil
	}
	if health == nil || !status.HealthKnown {
		return 0, drainRefusal("cannot change _meta membership: node health is unknown")
	}
	if !status.Found || len(status.ConfState.Voters) == 0 {
		return 0, drainRefusal("cannot change _meta membership: actual voter ConfState is unavailable")
	}
	if status.ConfState.AutoLeave || len(status.ConfState.VotersOutgoing) != 0 ||
		len(status.ConfState.LearnersNext) != 0 {
		return 0, drainRefusal("cannot change _meta membership while a joint configuration is active")
	}

	aliveRetained, err := retainedMetaVoters(target, input, owners, departing)
	if err != nil {
		return 0, err
	}
	required := QuorumOf(len(status.ConfState.Voters))
	if len(aliveRetained) < required {
		return 0, drainRefusal(
			"cannot change _meta membership: %d known-alive retained voters, need quorum %d of %d actual voters",
			len(aliveRetained), required, len(status.ConfState.Voters))
	}
	return validateDrainLeader(input, owners, departing, aliveRetained)
}

// Resolve exact historical and intermediate membership without inventing a
// whole converged document. Conflicting names for one member always refuse.
func metaMemberOwners(input PlanInput, target Document) (map[uint64]string, error) {
	owners := make(map[uint64]string)

	groups := []*Group{input.Groups[MetaGroupID].Converged, input.Approved.MetaGroup(), target.MetaGroup()}
	for _, group := range groups {
		if group == nil {
			continue
		}
		if err := addMetaReplicaOwners(owners, group.Replicas); err != nil {
			return nil, err
		}
	}
	for name, node := range input.Nodes {
		if node.MemberID != 0 {
			if err := addMetaMemberOwner(owners, node.MemberID, name); err != nil {
				return nil, err
			}
		}
	}
	for _, replica := range input.Retained[MetaGroupID].Replicas {
		if err := addMetaMemberOwner(owners, replica.ReplicaID, replica.NodeName); err != nil {
			return nil, err
		}
	}
	return owners, nil
}

func forcedRetirementOf(target Document, departing map[string]struct{}) bool {
	audit := target.Audit
	if audit == nil || !audit.Forced || audit.Operation != AuditOperationForceRetireNode || len(departing) != 1 {
		return false
	}
	_, ok := departing[audit.TargetNode]
	return ok
}
func drainRefusal(format string, args ...any) error {
	return fault.New(fault.UnsafeMembership, fmt.Sprintf(format, args...), nil)
}

func departingMetaNodes(input PlanInput, status GroupStatus, owners map[uint64]string,
	target Document) map[string]struct{} {
	departing := make(map[string]struct{})
	leaves := func(id uint64, name string) {
		next := target.Node(name)
		if next == nil || next.State != NodeActive || target.MemberID(name) != id {
			departing[name] = struct{}{}
		}
	}
	// Retained identities resolve actual members; their presence alone cannot
	// make an already removed member a departure in an unrelated operation.
	for _, group := range []*Group{input.Groups[MetaGroupID].Converged, input.Approved.MetaGroup()} {
		if group == nil {
			continue
		}
		for _, replica := range group.Replicas {
			leaves(replica.ReplicaID, replica.NodeName)
		}
	}
	conf := status.ConfState
	for _, ids := range [][]uint64{conf.Voters, conf.Learners, conf.VotersOutgoing, conf.LearnersNext} {
		for _, id := range ids {
			if name, ok := owners[id]; ok {
				leaves(id, name)
			}
		}
	}
	return departing
}

func retainedMetaVoters(target Document, input MetaDrainInput, owners map[uint64]string,
	departing map[string]struct{}) ([]uint64, error) {
	aliveRetained := make([]uint64, 0, len(input.Status.ConfState.Voters))
	for _, voterID := range input.Status.ConfState.Voters {
		owner, ok := owners[voterID]
		if !ok {
			return nil, drainRefusal("cannot resolve actual _meta voter %d through retained topology bindings",
				voterID)
		}
		if _, leaves := departing[owner]; leaves {
			continue
		}
		node := target.Node(owner)
		if input.Health[owner] == HealthAlive && node != nil && node.State == NodeActive &&
			target.MemberID(owner) == voterID {
			aliveRetained = append(aliveRetained, voterID)
		}
	}

	return aliveRetained, nil
}

func validateDrainLeader(input MetaDrainInput, owners map[uint64]string, departing map[string]struct{},
	aliveRetained []uint64) (uint64, error) {
	leaderOwner, ok := owners[input.Status.LeaderID]
	if input.Status.LeaderID == 0 || !ok {
		return 0, drainRefusal("cannot change _meta membership: actual leader is unknown or unresolved")
	}
	if _, leaves := departing[leaderOwner]; leaves {
		if input.Final {
			return 0, drainRefusal("cannot activate drain target: _meta leader %d is still on departing node %q",
				input.Status.LeaderID, leaderOwner)
		}
		return aliveRetained[0], nil
	}
	if input.Health[leaderOwner] != HealthAlive {
		return 0, drainRefusal("cannot change _meta membership: leader node %q is not known alive", leaderOwner)
	}
	return 0, nil
}

func addMetaReplicaOwners(owners map[uint64]string, replicas []Replica) error {
	for _, replica := range replicas {
		if err := addMetaMemberOwner(owners, replica.ReplicaID, replica.NodeName); err != nil {
			return err
		}
	}
	return nil
}

func addMetaMemberOwner(owners map[uint64]string, id uint64, name string) error {
	if id == 0 || name == "" {
		return drainRefusal("cannot resolve empty _meta owner")
	}
	if old, exists := owners[id]; exists && old != name {
		return drainRefusal("cannot resolve _meta voter %d: mapped to both %q and %q", id, old, name)
	}
	owners[id] = name
	return nil
}
