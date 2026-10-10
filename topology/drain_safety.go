package topology

import (
	"fmt"
	"github.com/asalimonov/gquorum/fault"
)

// validateMetaDrainSafety evaluates the actual _meta voter set, never the
// declared target quorum. It returns a known-alive retained voter to use for a
// leadership transfer when the current leader is departing.
func ValidateMetaDrainSafety(active, target Document, status GroupStatus, health map[string]Health, final bool) (uint64, error) {
	departing := make(map[string]struct{})
	for _, node := range active.Nodes {
		next := target.Node(node.Name)
		if next == nil {
			continue
		}
		if node.State == NodeActive && next.State != NodeActive {
			departing[node.Name] = struct{}{}
		}
	}
	activeMeta := active.MetaGroup()
	targetMeta := target.MetaGroup()
	if activeMeta != nil {
		for _, replica := range activeMeta.Replicas {
			nextNode := target.Node(replica.NodeName)
			if (targetMeta == nil || target.MemberID(replica.NodeName) != replica.ReplicaID) &&
				(nextNode == nil || nextNode.State != NodeActive) {
				departing[replica.NodeName] = struct{}{}
			}
		}
	}
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
	if status.ConfState.AutoLeave || len(status.ConfState.VotersOutgoing) != 0 || len(status.ConfState.LearnersNext) != 0 {
		return 0, drainRefusal("cannot change _meta membership while a joint configuration is active")
	}

	owners, err := metaMemberOwners(active, target)
	if err != nil {
		return 0, err
	}

	aliveRetained := make([]uint64, 0, len(status.ConfState.Voters))
	for _, voterID := range status.ConfState.Voters {
		owner, ok := owners[voterID]
		if !ok {
			return 0, drainRefusal("cannot resolve actual _meta voter %d through Active or Target", voterID)
		}
		if _, leaves := departing[owner]; leaves {
			continue
		}
		node := active.Node(owner)
		if node != nil && node.State == NodeActive && health[owner] == HealthAlive {
			aliveRetained = append(aliveRetained, voterID)
		}
	}
	required := QuorumOf(len(status.ConfState.Voters))
	if len(aliveRetained) < required {
		return 0, drainRefusal("cannot change _meta membership: %d known-alive retained voters, need quorum %d of %d actual voters", len(aliveRetained), required, len(status.ConfState.Voters))
	}
	leaderOwner, ok := owners[status.LeaderID]
	if status.LeaderID == 0 || !ok {
		return 0, drainRefusal("cannot change _meta membership: actual leader is unknown or unresolved")
	}
	if _, leaves := departing[leaderOwner]; leaves {
		if final {
			return 0, drainRefusal("cannot activate drain target: _meta leader %d is still on departing node %q", status.LeaderID, leaderOwner)
		}
		return aliveRetained[0], nil
	}
	if health[leaderOwner] != HealthAlive {
		return 0, drainRefusal("cannot change _meta membership: leader node %q is not known alive", leaderOwner)
	}
	return 0, nil
}

// metaMemberOwners maps every _meta raft member ID to the node that owns it,
// reading the given documents in order. A member ID that two documents map to
// DIFFERENT nodes is a resolution conflict, not a last-write-wins merge:
// answering with either name could aim a leadership transfer — or a forwarded
// Apply — at the wrong node.
func metaMemberOwners(docs ...Document) (map[uint64]string, error) {
	owners := make(map[uint64]string)
	for _, doc := range docs {
		meta := doc.MetaGroup()
		if meta == nil {
			continue
		}
		for _, replica := range meta.Replicas {
			if owner, exists := owners[replica.ReplicaID]; exists && owner != replica.NodeName {
				return nil, drainRefusal("cannot resolve _meta voter %d: mapped to both %q and %q", replica.ReplicaID, owner, replica.NodeName)
			}
			owners[replica.ReplicaID] = replica.NodeName
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
