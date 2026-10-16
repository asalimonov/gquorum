package metaservice

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

func unownedProvenGroup(input topology.PlanInput, id string) (*topology.Group, error) {
	row := input.Groups[id]
	if row.OperationID != "" {
		return nil, fault.New(fault.Unavailable, "group is owned by operation "+string(row.OperationID), nil)
	}
	if row.ConvergedVersion == 0 || row.Converged == nil || row.Deleting ||
		!reflect.DeepEqual(row.Converged, input.Approved.Group(id)) {
		return nil, fault.New(fault.VersionConflict,
			fmt.Sprintf("group %q is not converged to approved intent", id), nil)
	}
	return row.Converged, nil
}

func checkReadmissionScope(input topology.PlanInput, name string, member uint64) error {
	row := input.Nodes[name]
	if row.OperationID != "" {
		return fault.New(fault.Unavailable,
			"node is owned by operation "+string(row.OperationID), nil)
	}
	if row.ConvergedVersion == 0 || row.Converged == nil || row.MemberID != member ||
		input.Approved.MemberID(name) != member {
		return fault.ErrVersionConflict
	}
	for _, group := range input.Approved.Groups {
		hosts := slices.ContainsFunc(group.Replicas, func(replica topology.Replica) bool {
			return replica.NodeName == name
		})
		if !hosts {
			continue
		}
		if _, err := unownedProvenGroup(input, group.ID); err != nil {
			return err
		}
	}
	return nil
}

func checkReseatScope(input topology.PlanInput, id string, key topology.ReplicaKey) error {
	group, err := unownedProvenGroup(input, id)
	if err != nil {
		return err
	}
	for _, replica := range group.Replicas {
		if replica.NodeName == key.NodeName && replica.ReplicaID == key.ReplicaID {
			return nil
		}
	}
	return fault.New(fault.VersionConflict, "reseat predecessor no longer matches", nil)
}

func (g *Group) validateReadmissionScope(ctx context.Context, doc topology.Document,
	name string, expectedMemberID uint64) error {
	node := doc.Node(name)
	if node == nil {
		return unknownNode(name)
	}
	if doc.MemberID(name) != expectedMemberID {
		return fault.New(fault.VersionConflict, "readmission predecessor no longer matches", nil)
	}
	if node.State != topology.NodeActive {
		return fault.New(fault.Invalid, "readmission requires an admitted active node", nil)
	}
	input, err := g.state().PlanningInput(ctx, doc, nil)
	if err != nil {
		return err
	}
	return checkReadmissionScope(input, name, expectedMemberID)
}

func (g *Group) prepareReseat(ctx context.Context, doc *topology.Document,
	options metaengine.ReplicaRepairOptions) error {
	input, err := g.state().PlanningInput(ctx, *doc, nil)
	if err != nil {
		return err
	}
	key := topology.ReplicaKey{NodeName: options.NodeName, ReplicaID: options.ReplicaID}
	if err := checkReseatScope(input, options.GroupID, key); err != nil {
		return err
	}
	views := g.state().Views()
	// Task78 replaces these schema3 guards with scoped capture/approval.
	if !reflect.DeepEqual(views.Latest, views.Target) || !reflect.DeepEqual(views.Target, views.Active) {
		return fault.New(fault.OperatorAuthorization, "reseat requires equal Latest, Target and Active", nil)
	}
	if ex, ok := g.state().CurrentOperator(); ok && !ex.Status.State.Terminal() {
		return fault.New(fault.OperatorAuthorization, "reseat requires no running execution", nil)
	}
	return g.reseatDocument(doc, views, options)
}
