package metaengine

import "github.com/asalimonov/gquorum/topology"

func placementResult(approved, desired topology.Document, ids []string) (*topology.PlacementReport, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	report := topology.PlacementReport{
		Version:         desired.Version,
		ApprovedVersion: approved.Version,
		AffectedGroups:  append([]string(nil), ids...),
		Groups:          make([]topology.GroupPlacementReport, 0, len(ids)),
	}
	for _, id := range ids {
		if desired.Group(id) == nil {
			continue
		}
		group, err := topology.EvaluatePlacement(topology.PlacementInput{Approved: approved, Desired: desired,
			GroupID: id})
		if err != nil {
			return nil, err
		}
		report.Groups = append(report.Groups, group)
	}
	if len(report.Groups) == 0 {
		return nil, nil
	}
	return &report, nil
}

// An empty same-cluster reference provides no retained-placement exceptions.
func validateScopedPlacement(approved, desired topology.Document, rules topology.Rules) error {
	reference := approved
	if reference.ClusterID == "" {
		reference.ClusterID = desired.ClusterID
	}
	if err := topology.ValidateHostingCapacityFor(reference, desired, rules); err != nil {
		return err
	}
	return topology.ValidatePlacementTransition(reference, reference, desired)
}
