package metaservice

import (
	"errors"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func reportFromError(err error) *topology.PlacementReport {
	var placement *topology.PlacementError
	if !errors.As(err, &placement) {
		return nil
	}
	report := placement.Report.Clone()
	return &report
}

func placementReportFor(approved, desired topology.Document, ids []string) (*topology.PlacementReport, error) {
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

func explainUnsafePlacementPlan(approved, base, desired topology.Document, cause error) error {
	if !errors.Is(cause, fault.ErrUnsafeMembership) {
		return cause
	}
	report, err := placementReportFor(approved, desired, topology.AffectedPlacementGroups(base, desired))
	if err != nil || report == nil || len(report.Groups) == 0 {
		return cause
	}
	report.Groups[0].Issues = append(report.Groups[0].Issues,
		topology.PlacementIssue{Code: "no_safe_transition", GroupID: report.Groups[0].GroupID, Message: cause.Error()})
	return &topology.PlacementError{Code: fault.UnsafeMembership, Report: report.Clone()}
}
