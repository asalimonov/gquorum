package metaengine

import "github.com/asalimonov/gquorum/topology"

func validateScopedIdentities(state *scopedState, index *scopedIndexes) error {
	legacy := smState{Latest: state.Latest, Target: state.Target, RetiredGroups: state.RetiredGroups,
		Sessions: state.Sessions, Locks: state.Locks}
	if err := validateRestoredRetirements(legacy); err != nil {
		return err
	}
	if err := validateRestoredSessions(legacy); err != nil {
		return err
	}
	check := scopedIdentityValidation{floors: map[string]uint64{}, names: map[voterSwitchKey]string{}}
	for _, floor := range state.Allocations {
		if floor.NextReplicaID == 0 || check.floors[floor.GroupID] != 0 || topology.ValidateID(floor.GroupID) != nil {
			return invalidScopedState("invalid identity allocation floor")
		}
		check.floors[floor.GroupID] = floor.NextReplicaID
	}
	for _, group := range state.Latest.Groups {
		if group.NextReplicaID != check.floors[group.ID] {
			return invalidScopedState("latest allocator regressed below retained high water")
		}
	}
	if err := check.declarations(state, index); err != nil {
		return err
	}
	if err := check.nodes(state.Nodes); err != nil {
		return err
	}
	if err := check.operations(state.Operations); err != nil {
		return err
	}
	return check.grants(state.AuthorityRecords)
}

type scopedIdentityValidation struct {
	floors map[string]uint64
	names  map[voterSwitchKey]string
}

func (check *scopedIdentityValidation) nodes(nodes []nodeConvergence) error {
	for _, row := range nodes {
		if row.MemberID == 0 {
			continue
		}
		if err := check.replica(topology.MetaGroupID, row.NodeName, row.MemberID); err != nil {
			return err
		}
	}
	return nil
}

func (check *scopedIdentityValidation) group(group topology.Group) error {
	if group.NextReplicaID == 0 || group.NextReplicaID > check.floors[group.ID] {
		return invalidScopedState("retained declaration exceeds its allocation floor")
	}
	for _, replica := range group.Replicas {
		if err := check.replica(group.ID, replica.NodeName, replica.ReplicaID); err != nil {
			return err
		}
	}
	return nil
}

func (check *scopedIdentityValidation) replica(group, name string, id uint64) error {
	key := voterSwitchKey{group, id}
	if id == 0 || id >= check.floors[group] || name == "" || check.names[key] != "" && check.names[key] != name {
		return invalidScopedState("retained identity exceeds its floor or changes host")
	}
	check.names[key] = name
	return nil
}

func (check *scopedIdentityValidation) operations(operations []topology.Operation) error {
	for _, operation := range operations {
		for _, step := range operation.Plan.Steps {
			if err := check.step(step); err != nil {
				return err
			}
		}
	}
	return nil
}

func (check *scopedIdentityValidation) declarations(state *scopedState, index *scopedIndexes) error {
	for _, doc := range index.documents {
		for _, group := range doc.Groups {
			if err := check.group(group); err != nil {
				return err
			}
		}
	}
	for _, row := range state.Groups {
		if row.Declaration != nil {
			if err := check.group(*row.Declaration); err != nil {
				return err
			}
		}
	}
	return nil
}

func (check *scopedIdentityValidation) step(step topology.Step) error {
	if step.ReplicaID != 0 {
		if err := check.replica(step.GroupID, step.NodeName, step.ReplicaID); err != nil {
			return err
		}
	}
	if step.TransferReplicaID != 0 {
		if err := check.replica(step.GroupID, step.TransferNodeName, step.TransferReplicaID); err != nil {
			return err
		}
	}
	return nil
}

func (check *scopedIdentityValidation) grants(records []scopedAuthorityRecord) error {
	for _, record := range records {
		for _, target := range record.Grant.Targets {
			if err := check.replica(record.Grant.Key.GroupID, target.NodeName, target.ReplicaID); err != nil {
				return err
			}
			if err := check.replica(topology.MetaGroupID, target.NodeName, target.MemberID); err != nil {
				return err
			}
		}
	}
	return nil
}
