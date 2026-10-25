package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

const (
	operationPlanQuery = "operation_plan"
	retryPlanQuery     = "retry_plan"
	transferPlanQuery  = "transfer_plan"
	repairPlanQuery    = "repair_plan"
)

func scopedPlanQuery(kind string) bool {
	return kind == "planning" || kind == "approval_plan" || kind == operationPlanQuery ||
		kind == retryPlanQuery || kind == transferPlanQuery || kind == repairPlanQuery
}

func (q QueryRequest) validatePlanFields() error {
	if q.Progress != nil || q.CollectionIdentity != nil || q.AuthorityKey != nil {
		return fault.ErrInvalid
	}
	switch q.Kind {
	case "planning":
		if q.Desired == nil || q.OperationID != "" || q.Key != "" || q.Version != 0 {
			return fault.ErrInvalid
		}
	case "approval_plan":
		if q.Version == 0 || q.Desired != nil || q.OperationID != "" || q.Key != "" {
			return fault.ErrInvalid
		}
	case repairPlanQuery:
		return q.validateRepairPlanFields()
	case transferPlanQuery:
		if q.Desired != nil || q.OperationID != "" || q.Version != 0 || topology.ValidateID(q.Key) != nil {
			return fault.ErrInvalid
		}
	case operationPlanQuery, retryPlanQuery:
		return q.validateOperationPlanFields()
	default:
		return fault.ErrInvalid
	}
	return nil
}

func (q QueryRequest) validateOperationPlanFields() error {
	if q.OperationID == "" || q.Desired != nil || q.Version != 0 {
		return fault.ErrInvalid
	}
	if q.Kind == retryPlanQuery && q.Key != "" || q.Kind == operationPlanQuery && topology.ValidateID(q.Key) != nil {
		return fault.ErrInvalid
	}
	return nil
}

func queryScopedPlan(s *scopedState, q QueryRequest) (PlanRead, error) {
	if err := q.validatePlanFields(); err != nil {
		return PlanRead{}, err
	}
	if s == nil {
		return PlanRead{}, fault.ErrUnsupported
	}
	read, err := s.planReadBase(q)
	if err != nil || read.Accepted != "" {
		return read, err
	}
	read.Recovery = s.recoveryPlacementState()
	input := s.planningDeclarations()
	input.Native = make(map[string]topology.NativeState, len(q.Native))
	for id, native := range q.Native {
		if native.GroupID != id || native.Applied == 0 || native.ReplicaID == 0 ||
			!slices.ContainsFunc(read.Scopes, func(scope ScopeExpectation) bool { return scope.GroupID == id }) {
			return PlanRead{}, fault.ErrInvalid
		}
		input.Native[id] = native.Clone()
	}
	collector := s.planningCollector()
	read.Input, err = collector.collect(input, read.Target)
	if err != nil {
		return PlanRead{}, err
	}
	if read.Operation.Status.ID != "" {
		err = s.collectScopedPlanEvidence(&read)
	}
	return read, err
}

func (s *scopedState) planReadBase(q QueryRequest) (PlanRead, error) {
	read := PlanRead{}
	switch q.Kind {
	case repairPlanQuery:
		return s.repairPlanRead(q)
	case transferPlanQuery:
		return s.transferPlanRead(q.Key)
	case "planning":
		read.Target = q.Desired.Clone()
	case "approval_plan":
		id, err := retainedApproval(q.Version, topology.ReadView{Latest: s.Latest, Target: s.Target}, s.indexes)
		if id != "" || err != nil {
			return PlanRead{Accepted: id}, err
		}
		read.Target = s.Latest.Clone()
	default:
		return s.operationPlanRead(q)
	}
	if read.Target.ClusterID != s.Target.ClusterID && s.Target.ClusterID != "" {
		return PlanRead{}, fault.ErrIdentityMismatch
	}
	if err := s.validateRecoveryDocument(read.Target); err != nil {
		return PlanRead{}, err
	}
	read.Scopes = s.capturePlanScopes(topology.AffectedGroups(s.Target, read.Target))
	return read, nil
}

func (s *scopedState) capturePlanScopes(groups []string) []ScopeExpectation {
	scopes := make([]ScopeExpectation, 0, len(groups))
	for _, group := range groups {
		scope := ScopeExpectation{GroupID: group}
		if row := s.indexes.groups[group]; row != nil {
			scope.SourceVersion, scope.Owner = row.SourceVersion, row.Owner
		}
		scopes = append(scopes, scope)
	}
	return scopes
}

func (s *scopedState) operationPlanRead(q QueryRequest) (PlanRead, error) {
	if q.Kind == retryPlanQuery {
		if id := s.indexes.retries[q.OperationID]; id != "" {
			return PlanRead{Accepted: id}, nil
		}
	}
	op := s.indexes.operations[q.OperationID]
	if op == nil {
		return PlanRead{}, fault.ErrHistoryUnavailable
	}
	doc, found := s.indexes.documents[op.Status.Version]
	if !found {
		return PlanRead{}, fault.ErrHistoryUnavailable
	}
	read := PlanRead{Operation: op.Clone(), Target: doc.Clone()}
	if q.Kind == operationPlanQuery {
		if err := s.validatePlanOwner(op, q.Key); err != nil {
			return PlanRead{}, err
		}
		read.Scopes = s.capturePlanScopes([]string{q.Key})
		return read, nil
	}
	var groups []string
	for _, ref := range failedBlockRefs(op) {
		groups = append(groups, ref.GroupID)
	}
	read.Scopes = s.capturePlanScopes(groups)
	at := op.UpdatedAt
	for _, group := range groups {
		if row := s.indexes.groups[group]; row != nil && row.CompletedAt.After(at) {
			at = row.CompletedAt
		}
	}
	return read, s.validateRetryRequest(op, RetryOperationProposal{Previous: op.Status.ID, Scopes: read.Scopes, At: at})
}

func (s *scopedState) validatePlanOwner(op *topology.Operation, group string) error {
	position := readyBlockPosition(op, group)
	row := s.indexes.groups[group]
	if position < 0 || row == nil || row.Owner != op.Status.ID {
		return fault.ErrOperationAuthorization
	}
	if blocker := s.requiredBlocker(op, position); blocker != nil {
		if blocker.Code == topology.BlockedRevocationPending {
			return fault.ErrAuthorityPending
		}
		return fault.ErrUnavailable
	}
	return nil
}

func (s *scopedState) transferPlanRead(group string) (PlanRead, error) {
	row := s.indexes.groups[group]
	if row == nil && s.Target.Group(group) == nil {
		return PlanRead{}, fault.New(fault.Invalid, "unknown transfer group", nil)
	}
	if row == nil || row.SourceVersion == 0 || row.Declaration == nil || row.Deleting {
		return PlanRead{}, fault.ErrUnavailable
	}
	if row.Owner != "" {
		return PlanRead{}, fault.New(fault.Unavailable, "group is owned by operation "+string(row.Owner), nil)
	}
	read := PlanRead{Target: s.Target.Clone(), Scopes: s.capturePlanScopes([]string{group})}
	return read, nil
}

func (q QueryRequest) validateRepairPlanFields() error {
	if q.OperationID != "" || q.Version != 0 || q.Desired != nil && q.Key == "" {
		return fault.ErrInvalid
	}
	if q.Key != "" && topology.ValidateID(q.Key) != nil {
		return fault.ErrInvalid
	}
	return nil
}
