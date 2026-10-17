package metaservice

import (
	"net"
	"reflect"
	"slices"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// validateJoinGrant runs before any resolver update or durable-store creation.
func validateJoinGrant(identity topology.Identity, rules topology.Rules, result joinResponse) error {
	if result.MemberID == 0 || result.Granted.ClusterID != identity.ClusterID ||
		!reflect.DeepEqual(result.Rules, rules) {
		return fault.ErrIdentityMismatch
	}
	if err := result.Granted.Validate(rules); err != nil {
		return fault.New(fault.Invalid, "invalid admitted topology", err)
	}
	if result.MemberID != result.Granted.MemberID(identity.NodeName) {
		return fault.ErrIdentityMismatch
	}
	if !result.Existing && (result.Admission.NodeName != identity.NodeName ||
		result.Admission.InstanceID != identity.InstanceID || result.Admission.MemberID != result.MemberID ||
		result.Admission.OperationID == "") {
		return fault.ErrIdentityMismatch
	}
	if result.Admission.ReadmissionInstanceID != "" &&
		(result.Admission.ReadmissionInstanceID == result.Admission.InstanceID ||
			strings.ContainsRune(result.Admission.ReadmissionInstanceID, 0)) {
		return fault.ErrIdentityMismatch
	}
	if origin := string(result.Admission.ReadmissionOperatorID); origin != "" &&
		(origin == string(result.Admission.OperationID) || strings.TrimSpace(origin) != origin ||
			strings.ContainsRune(origin, 0)) {
		return fault.ErrIdentityMismatch
	}
	if err := validateReadmissionResponse(identity, result); err != nil {
		return err
	}
	if err := validatePreviousJoinGroups(identity, result); err != nil {
		return err
	}
	return validateJoinMembers(result)
}

func mergePreviousGroup(stored, group topology.Group, next *topology.Group) topology.Group {
	previous := stored
	if previous.ID == "" {
		previous = group
		previous.Replicas = nil
	}
	for _, r := range group.Replicas {
		skip := retainedGroupReplica(previous, next, r)
		if !skip {
			previous.Replicas = append(previous.Replicas, r)
		}
	}
	return previous
}

func retainedGroupReplica(previous topology.Group, next *topology.Group, r topology.Replica) bool {
	skip := false
	if next != nil {
		for _, fresh := range next.Replicas {
			if fresh.NodeName == r.NodeName && fresh.ReplicaID == r.ReplicaID {
				skip = true
			}
		}
	}
	for _, old := range previous.Replicas {
		skip = skip || old.ReplicaID == r.ReplicaID
	}
	return skip
}

func validatePreviousJoinGroups(identity topology.Identity, result joinResponse) error {
	seenGroups := make(map[string]bool)
	for _, group := range result.PreviousGroups {
		if seenGroups[group.ID] || len(group.Replicas) == 0 || topology.ValidateLoadID(group.ID) != nil {
			return fault.ErrIdentityMismatch
		}
		seenGroups[group.ID] = true
		if err := validatePreviousJoinReplicas(identity.NodeName, group, result.Granted.Group(group.ID)); err != nil {
			return err
		}
	}
	return nil
}

func validatePreviousJoinReplicas(name string, group topology.Group, next *topology.Group) error {
	seenReplicas := make(map[uint64]bool)
	for _, r := range group.Replicas {
		if r.NodeName != name || r.ReplicaID == 0 || seenReplicas[r.ReplicaID] {
			return fault.ErrIdentityMismatch
		}
		seenReplicas[r.ReplicaID] = true
		if next != nil && slices.ContainsFunc(next.Replicas,
			func(fresh topology.Replica) bool { return fresh.ReplicaID == r.ReplicaID }) {
			return fault.ErrIdentityMismatch
		}
	}
	return nil
}

func joinMemberOwners(result joinResponse) (map[uint64]string, error) {
	owners := map[uint64]string{}
	if group := result.Granted.MetaGroup(); group != nil {
		for _, r := range group.Replicas {
			owners[r.ReplicaID] = r.NodeName
		}
	}
	return owners, nil
}

func validateJoinMembers(result joinResponse) error {
	owners, err := joinMemberOwners(result)
	if err != nil {
		return err
	}
	seen := map[uint64]bool{}
	for _, member := range result.Members {
		if member.MemberID == 0 || member.Name == "" ||
			owners[member.MemberID] != "" && owners[member.MemberID] != member.Name ||
			seen[member.MemberID] {
			return fault.ErrIdentityMismatch
		}
		seen[member.MemberID] = true
		if member.Address != "" {
			if _, _, err := net.SplitHostPort(member.Address); err != nil {
				return fault.ErrInvalid
			}
		}
	}
	for id := range owners {
		if !seen[id] {
			return fault.ErrIdentityMismatch
		}
	}
	if _, _, err := net.SplitHostPort(result.LeaderAddress); err != nil {
		return fault.ErrInvalid
	}
	return nil
}
