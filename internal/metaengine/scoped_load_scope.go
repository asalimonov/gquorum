package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// WithLoadScope holds the FSM read boundary through a local ledger operation.
// fn must be bounded and must not call back into this machine.
func (m *Machine) WithLoadScope(id storage.Identity, member uint64, fn func(LoadScope) error) error {
	if err := topology.ValidateLoadID(id.GroupID); err != nil {
		return err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	scope, err := queryScopedLoadScope(nil, id, member) // Task78 installs complete scoped state.
	if err != nil {
		return err
	}
	return fn(scope)
}

func queryScopedLoadScope(s *scopedState, id storage.Identity, member uint64) (LoadScope, error) {
	if s == nil {
		return LoadScope{}, fault.ErrUnsupported
	}
	if slices.ContainsFunc(s.RetiredGroups, func(row retiredGroup) bool { return row.ID == id.GroupID }) {
		return LoadScope{}, fault.ErrGroupRetired
	}
	scope, err := approvedLoadScope(s.Target, id, member)
	if err != nil {
		return LoadScope{}, err
	}
	if err := scope.observeScopedCheckpoint(s, member); err != nil {
		return LoadScope{}, err
	}
	if err := scope.observeScopedFounding(s, member); err != nil {
		return LoadScope{}, err
	}
	target := topology.AuthorityTarget{NodeName: id.NodeName, MemberID: member, ReplicaID: id.ReplicaID}
	for _, record := range s.AuthorityRecords {
		scope.observeScopedCreation(record, target)
	}
	return scope, nil
}

func (scope *LoadScope) observeScopedCheckpoint(s *scopedState, member uint64) error {
	row := s.indexes.groups[scope.Group.ID]
	if row == nil || row.SourceVersion == 0 ||
		!groupHasMember(row.Declaration, scope.Replica.NodeName, scope.Replica.ReplicaID) {
		return nil
	}
	doc, found := checkpointDocument(s, s.indexes, *row)
	if !found {
		return fault.ErrHistoryUnavailable
	}
	if doc.MemberID(scope.Replica.NodeName) != member {
		return fault.ErrIdentityMismatch
	}
	scope.Started, scope.Established = true, true
	return nil
}

func (scope *LoadScope) observeScopedFounding(s *scopedState, member uint64) error {
	row := s.indexes.groups[scope.Group.ID]
	if row == nil || row.SourceVersion != 0 || row.Owner == "" || scope.Replica.Role != topology.ReplicaVoter {
		return nil
	}
	op := s.indexes.operations[row.Owner]
	if op == nil {
		return fault.ErrOperationAuthorization
	}
	doc := s.indexes.documents[op.Status.Version]
	if doc.Version == 0 {
		return fault.ErrHistoryUnavailable
	}
	if doc.MemberID(scope.Replica.NodeName) != member || !slices.Contains(doc.VotersOf(scope.Group.ID), scope.Replica) {
		return fault.ErrIdentityMismatch
	}
	scope.Founding = slices.ContainsFunc(op.Plan.Steps, func(step topology.Step) bool {
		return step.GroupID == scope.Group.ID && step.Kind == topology.StepCreateFoundingGroup
	})
	return nil
}

// Receipts are durable facts even after supersession closes their grant. Mere
// issuance is permission, never evidence that storage or membership existed.
func (scope *LoadScope) observeScopedCreation(record scopedAuthorityRecord, target topology.AuthorityTarget) {
	grant := record.Grant
	if grant.Action == nil || grant.Key.GroupID != scope.Group.ID || !slices.Contains(grant.Targets, target) {
		return
	}
	action := grant.Action
	switch action.Kind {
	case topology.StepCreateFoundingGroup:
		// Host readiness follows the synchronously persisted founding snapshot.
		// It establishes this voter even before the native group receipt arrives.
		if exactCreationReceipt(record, target) {
			scope.Started, scope.Established = true, true
		}
	case topology.StepPrepareReplica:
		if action.NodeName == target.NodeName && action.ReplicaID == target.ReplicaID &&
			exactCreationReceipt(record, target) {
			scope.Started = true // A prepared learner can legitimately have an empty log.
		}
	case topology.StepAddLearner, topology.StepPromoteVoter:
		// Grant targets include native recipients; only the action names the affected member.
		if action.NodeName == target.NodeName && action.ReplicaID == target.ReplicaID &&
			resourceConsumed(&record.authorityRecord,
				topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: scope.Group.ID}) {
			scope.Started, scope.Established = true, true
		}
	default:
	}
}

func exactCreationReceipt(record scopedAuthorityRecord, target topology.AuthorityTarget) bool {
	resource := topology.AuthorityResource{Kind: topology.AuthorityHost,
		GroupID: record.Grant.Key.GroupID, NodeName: target.NodeName}
	return slices.ContainsFunc(record.Effects, func(receipt topology.EffectReceipt) bool {
		return receipt.Resource == resource && receipt.Target == target
	})
}
