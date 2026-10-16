package metaengine

import (
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Capture both document CAS boundaries and exact group checkpoints under one
// FSM lock. No global convergence or current-operation test belongs here.
func (s *scopedState) repairPlanRead(q QueryRequest) (PlanRead, error) {
	read := PlanRead{Latest: s.Latest.Clone(), Target: s.Target.Clone()}
	var groups []string
	if q.Key != "" {
		if err := s.repairScopeReady(q.Key); err != nil {
			return PlanRead{}, err
		}
		groups = []string{q.Key}
	} else {
		for _, group := range s.Target.Groups {
			groups = append(groups, group.ID)
		}
		slices.Sort(groups)
	}
	read.Scopes = s.capturePlanScopes(groups)
	if q.Desired == nil {
		return read, nil
	}
	if q.Desired.ClusterID != s.Target.ClusterID {
		return PlanRead{}, fault.ErrIdentityMismatch
	}
	if err := q.Desired.Validate(s.Rules); err != nil {
		return PlanRead{}, err
	}
	if !slices.Equal(topology.AffectedGroups(s.Target, *q.Desired), []string{q.Key}) {
		return PlanRead{}, fault.ErrOperationAuthorization
	}
	read.Target = q.Desired.Clone()
	return read, nil
}

func (s *scopedState) repairScopeReady(group string) error {
	row := s.indexes.groups[group]
	if row == nil || row.SourceVersion == 0 || row.Declaration == nil || row.Deleting {
		return fault.ErrUnavailable
	}
	if row.Owner != "" {
		return fault.New(fault.AuthorityPending, "group "+group+" is owned by operation "+string(row.Owner), nil)
	}
	return nil
}
