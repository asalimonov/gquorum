package metaengine

import (
	"context"

	"github.com/asalimonov/gquorum/topology"
)

// PlanningInput captures owned declarations and retained effects at one metadata
// boundary. Native reads remain caller-supplied quorum observations; this method
// neither performs I/O nor authorizes effects. Apply revalidates retained evidence.
func (sm *topologySM) PlanningInput(ctx context.Context, desired topology.Document,
	native map[string]topology.NativeState) (topology.PlanInput, error) {
	if err := ctx.Err(); err != nil {
		return topology.PlanInput{}, err
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.planningInputLocked(desired, native)
}

func (sm *topologySM) planningInputLocked(desired topology.Document,
	native map[string]topology.NativeState) (topology.PlanInput, error) {
	active := sm.active.Clone()
	input := topology.PlanInput{Approved: sm.target.Clone(), Groups: make(map[string]topology.GroupProgress),
		Nodes: make(map[string]topology.NodeProgress), Native: make(map[string]topology.NativeState)}
	for i := range active.Groups {
		group := &active.Groups[i]
		input.Groups[group.ID] = topology.GroupProgress{GroupID: group.ID,
			ConvergedVersion: active.Version, Converged: group}
	}
	for id, version := range sm.retired {
		if version <= active.Version && active.Group(id) == nil {
			input.Groups[id] = topology.GroupProgress{GroupID: id, ConvergedVersion: version}
		}
	}
	for i := range active.Nodes {
		node := &active.Nodes[i]
		input.Nodes[node.Name] = topology.NodeProgress{NodeName: node.Name, ConvergedVersion: active.Version,
			Converged: node, MemberID: active.MemberID(node.Name)}
	}
	for id, state := range native {
		input.Native[id] = state.Clone()
	}
	evidence := planningEvidence{documents: sm.authorityDocumentsLocked(), authority: sm.authority,
		allocations: sm.allocations}
	for _, admission := range sm.admissions {
		evidence.admissions = append(evidence.admissions, *admission)
	}
	return collectPlanningEvidence(input, desired, evidence)
}

// The scoped adapter supplies the same validated sources under its FSM lock in
// Tasks 32/61. Schema-3 reads use actual Active, never DeletionBase/OperatorBase.
type planningEvidence struct {
	documents   []topology.Document
	authority   authorityCatalog
	allocations map[string]uint64
	admissions  []AdmissionRecord
	cleanup     []cleanupRecord
}

func (sm *topologySM) validatePlanShapeLocked(desired topology.Document, plan topology.Plan) error {
	input, err := sm.planningInputLocked(desired, nil)
	if err != nil {
		return err
	}
	return topology.ValidatePlanShape(input, desired, plan)
}
