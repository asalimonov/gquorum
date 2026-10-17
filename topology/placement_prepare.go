package topology

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/asalimonov/gquorum/fault"
)

// PreparedPlacement contains a completed edit and its placement explanation.
// Report remains available when preparation fails; Document is then empty.
type PreparedPlacement struct {
	Document Document
	Report   PlacementReport
}

// PreparePlacement completes a local topology edit using owned copies. It does
// not stage or approve the returned document; replicated validators must fetch
// their own current views before authorizing any effect.
func PreparePlacement(view ReadView, edited Document, rules Rules, at time.Time) (PreparedPlacement, error) {
	return resolvePlacement(view, edited, placementPrepareOptions{rules: rules, at: at})
}

// ExplainPlacement evaluates the same edit without exposing tentative replica
// allocations or requiring an audit actor. Its fixed analysis time has no
// effect
// on feasibility or the returned explanation.
func ExplainPlacement(view ReadView, edited Document, rules Rules) (PlacementReport, error) {
	scratch := edited.Clone()
	if strings.TrimSpace(scratch.ChangedBy) == "" {
		scratch.ChangedBy = "placement-explanation"
	}
	options := placementPrepareOptions{rules: rules, at: time.Unix(0, 0).UTC(), explain: true}
	prepared, err := resolvePlacement(view, scratch, options)
	return prepared.Report.Clone(), err
}

type placementPrepareOptions struct {
	rules   Rules
	at      time.Time
	explain bool
}

func resolvePlacement(view ReadView, edited Document,
	options placementPrepareOptions) (PreparedPlacement, error) {
	rules, at := options.rules, options.at
	if err := validatePlacementEdit(view, edited, options); err != nil {
		return PreparedPlacement{}, err
	}
	candidate := edited.Clone()
	if err := rejectConflictingReplicaEdits(view.Latest, candidate); err != nil {
		return PreparedPlacement{}, err
	}
	// Check declarations before any tentative allocation. Existing replicas may
	// be temporarily ineligible after a label edit; final placement is checked
	// after materialization rather than by this preflight.
	if err := validatePlacementDeclarations(candidate, rules); err != nil {
		return PreparedPlacement{}, err
	}
	affected := AffectedPlacementGroups(view.Latest, candidate)
	candidate.Version = view.Latest.Version + 1
	candidate.ParentVersion = view.Latest.Version
	candidate.Origin = OriginOperator
	candidate.CreatedAt = at
	candidate.Audit = nil
	report, err := reconcilePlacement(view, &candidate, affected, options)
	if err != nil {
		return PreparedPlacement{Report: report}, err
	}
	setPlacementTimestamps(view.Latest, &candidate, at)
	if err := ValidateTransition(view.Latest, candidate, rules, nil); err != nil {
		return PreparedPlacement{Report: report}, err
	}
	if err := ValidatePlacementTransition(view.Target, view.Target, candidate); err != nil {
		appendPlacementRejections(&report, err)
		return PreparedPlacement{Report: report}, err
	}
	return PreparedPlacement{Document: candidate, Report: report}, nil
}

func rejectConflictingReplicaEdits(before, edited Document) error {
	for _, old := range before.Groups {
		next := edited.Group(old.ID)
		if next != nil && old.ID == MetaGroupID && validMetaAdmissionEdit(before, edited, old, *next) {
			continue
		}
		if next != nil && validVoterConfigAdoptionEdit(old, *next) {
			continue
		}
		if next == nil || next.NextReplicaID != old.NextReplicaID || !reflect.DeepEqual(next.Replicas, old.Replicas) {
			return invalid("conflicting_replica_edit: use direct Stage for manual membership edits")
		}
	}
	for _, group := range edited.Groups {
		if (len(group.Replicas) != 0 || group.NextReplicaID != 1) && before.Group(group.ID) == nil {
			return invalid("conflicting_replica_edit: new groups require unallocated replicas")
		}
	}
	return nil
}

func validVoterConfigAdoptionEdit(old, next Group) bool {
	if old.VoterConfig != nil || next.VoterConfig == nil || next.NextReplicaID != old.NextReplicaID ||
		len(next.Replicas) != len(old.Replicas) {
		return false
	}
	for i, replica := range old.Replicas {
		want := replica
		if want.Role == ReplicaLearner {
			want.PinnedLearner = true
		}
		if !reflect.DeepEqual(next.Replicas[i], want) {
			return false
		}
	}
	return true
}

// A typed node admission necessarily appends one new _meta identity before
// placement preparation. Only that exact fresh append is accepted here;
// arbitrary meta membership changes remain expert complete-document edits.
func validMetaAdmissionEdit(before, edited Document, old, next Group) bool {
	if next.NextReplicaID == old.NextReplicaID && reflect.DeepEqual(next.Replicas, old.Replicas) {
		return true
	}
	if next.NextReplicaID <= old.NextReplicaID || len(next.Replicas) <= len(old.Replicas) {
		return false
	}
	if !retainedReplicasUnchanged(old, next) {
		return false
	}
	newIDs := make(map[uint64]bool)
	for _, replica := range next.Replicas {
		if groupReplicaOnNode(&old, replica.NodeName) != nil {
			continue
		}
		previous, node := before.Node(replica.NodeName), edited.Node(replica.NodeName)
		if node == nil || node.State != NodeActive || previous != nil && previous.State != NodeExcluded ||
			replica.ReplicaID < old.NextReplicaID || replica.ReplicaID >= next.NextReplicaID ||
			newIDs[replica.ReplicaID] ||
			replica.PinnedLearner || replica.Role != ReplicaLearner && (old.VoterConfig != nil || NodeLearnerOnly(
			*node)) {
			return false
		}
		newIDs[replica.ReplicaID] = true
	}
	return len(newIDs) == len(next.Replicas)-len(old.Replicas) &&
		uint64(len(newIDs)) == next.NextReplicaID-old.NextReplicaID
}

func validatePlacementDeclarations(doc Document, rules Rules) error {
	if rules.SchemaVersion != 1 {
		return invalid("unsupported topology schema")
	}
	if err := doc.Placement.validate(rules.Limits); err != nil {
		return err
	}
	for _, node := range doc.Nodes {
		if err := ValidateLabels(node.Labels, NodeLabels); err != nil {
			return err
		}
	}
	for _, group := range doc.Groups {
		if err := validateGroupDeclaration(doc, group, rules); err != nil {
			return err
		}
	}
	return nil
}

func reconcilePlacement(view ReadView, next *Document, affected []string,
	options placementPrepareOptions) (PlacementReport, error) {
	approved, rules := view.Target, options.rules
	report := PlacementReport{
		Version:         next.Version,
		ApprovedVersion: approved.Version,
		AffectedGroups:  slices.Clone(affected),
		Groups:          make([]GroupPlacementReport, 0, len(affected)),
	}
	for _, id := range affected {
		if next.Group(id) == nil { // Deletion belongs to the direct expert document path.
			return report, invalid("placement preparation cannot delete a group")
		}
		groupReport, err := EvaluatePlacement(PlacementInput{Approved: approved, Desired: *next, GroupID: id})
		if err != nil {
			return report, err
		}
		report.Groups = append(report.Groups, groupReport)
	}
	if err := ValidateHostingCapacityFor(approved, *next, rules); err != nil {
		return report, err
	}
	if err := materializePlacementGroups(view, next, affected, options); err != nil {
		return report, err
	}
	if err := next.Validate(rules); err != nil {
		return report, err
	}
	for i, id := range affected {
		final, err := EvaluatePlacement(PlacementInput{Approved: approved, Desired: *next, GroupID: id})
		if err != nil {
			return report, err
		}
		report.Groups[i] = final
	}
	return report, nil
}

func needsVoterReconciliation(before, next Document, groupID string) bool {
	old, group := before.Group(groupID), next.Group(groupID)
	if old == nil || group == nil {
		return true
	}
	oldTarget, nextTarget := before.GroupPlacement(groupID).MaxReplicas, next.GroupPlacement(groupID).MaxReplicas
	oldDomain, nextDomain := "", ""
	if old.VoterConfig != nil {
		oldTarget = old.VoterConfig.TargetVoters
		oldDomain = old.VoterConfig.DistinctVoterLabel
	}
	if group.VoterConfig != nil {
		nextTarget = group.VoterConfig.TargetVoters
		nextDomain = group.VoterConfig.DistinctVoterLabel
	}
	if oldTarget != nextTarget || oldDomain != nextDomain {
		return true
	}
	if legacyVoterAdmissionNeeded(before, group, nextTarget) {
		return true
	}
	voters := make([]ReplicaKey, 0)
	for _, replica := range old.Replicas {
		if replica.Role != ReplicaVoter {
			continue
		}
		current := groupReplicaOnNode(group, replica.NodeName)
		if current == nil || current.ReplicaID != replica.ReplicaID {
			return true
		}
		voters = append(voters, ReplicaKey{NodeName: replica.NodeName, ReplicaID: replica.ReplicaID})
	}
	return ValidateVoterPlacement(next, groupID, voters) != nil
}

func setPlacementTimestamps(before Document, next *Document, at time.Time) {
	for i := range next.Nodes {
		node := &next.Nodes[i]
		old := before.Node(node.Name)
		if old == nil {
			if node.CreatedAt.IsZero() {
				node.CreatedAt = at
			}
			node.UpdatedAt = at
		} else if node.State != old.State || node.Region != old.Region || !reflect.DeepEqual(node.Type, old.Type) ||
			!reflect.DeepEqual(node.Labels, old.Labels) {
			node.UpdatedAt = at
		}
	}
	for i := range next.Groups {
		group := &next.Groups[i]
		old := before.Group(group.ID)
		if old == nil {
			if group.CreatedAt.IsZero() {
				group.CreatedAt = at
			}
			group.UpdatedAt = at
		} else if group.NextReplicaID != old.NextReplicaID || !reflect.DeepEqual(group.Labels, old.Labels) ||
			!reflect.DeepEqual(group.Placement, old.Placement) ||
			!reflect.DeepEqual(group.VoterConfig, old.VoterConfig) ||
			!reflect.DeepEqual(group.Hosting, old.Hosting) || !reflect.DeepEqual(group.Replicas, old.Replicas) {
			group.UpdatedAt = at
		}
	}
}

// ValidatePlacementTransition checks the final declared assignment against the
// approved reservation reference. Native intermediate configurations are
// validated by the plan and execution paths using Active as their baseline.
func ValidatePlacementTransition(approved, base, desired Document) error {
	if approved.ClusterID != desired.ClusterID || base.ClusterID != desired.ClusterID {
		return invalid(fmt.Sprintf("placement transition has mismatched cluster identity: "+
			"approved=%q base=%q desired=%q",
			approved.ClusterID, base.ClusterID, desired.ClusterID))
	}
	for _, group := range desired.Groups {
		report, err := EvaluatePlacement(PlacementInput{Approved: approved, Desired: desired, GroupID: group.ID})
		if err != nil {
			return err
		}
		if group.VoterConfig != nil && group.VoterConfig.DistinctVoterLabel != "" &&
			report.AssignedVoters != report.RequiredVoters && base.Group(group.ID) == nil {
			report.Issues = append(report.Issues, PlacementIssue{
				Code: "insufficient_voters", GroupID: group.ID,
				Message: fmt.Sprintf("group %q requires %d founding voters; %d are assigned", group.ID,
					report.RequiredVoters, report.AssignedVoters),
			})
		}
		if len(report.Issues) != 0 {
			return &PlacementError{Code: fault.Invalid, Report: PlacementReport{
				Version: desired.Version, ApprovedVersion: approved.Version,
				Groups: []GroupPlacementReport{report},
			}}
		}
		if err := validateReplicaPlacement(desired, group, report); err != nil {
			return err
		}
	}
	return nil
}

func placementTransitionError(group GroupPlacementReport, nodeName, code string) error {
	group.Issues = append(group.Issues, PlacementIssue{
		Code: code, GroupID: group.GroupID, NodeName: nodeName,
		Message: fmt.Sprintf("group %q has an ineligible final placement on node %q", group.GroupID, nodeName),
	})
	return &PlacementError{Code: fault.Invalid, Report: PlacementReport{Groups: []GroupPlacementReport{group}}}
}

func validateGroupDeclaration(doc Document, group Group, rules Rules) error {
	if err := ValidateLabels(group.Labels, GroupLabels); err != nil {
		return err
	}
	if group.Placement != nil && group.ID != MetaGroupID {
		if err := group.Placement.validate(rules.Limits); err != nil {
			return err
		}
	}
	if group.Hosting != nil {
		if err := group.Hosting.Validate(); err != nil {
			return err
		}
	}
	if group.VoterConfig != nil {
		lo, hi := doc.GroupBounds(group.ID)
		if group.ID == MetaGroupID {
			lo, hi = 1, rules.Limits.MaxNodes
		}
		if err := group.VoterConfig.Validate(lo, hi); err != nil {
			return err
		}
	}
	return nil
}

func retainedReplicasUnchanged(old, next Group) bool {
	for _, replica := range old.Replicas {
		current := groupReplicaOnNode(&next, replica.NodeName)
		if current == nil || !reflect.DeepEqual(*current, replica) {
			return false
		}
	}

	return true
}

func legacyVoterAdmissionNeeded(before Document, group *Group, nextTarget int) bool {
	if group.VoterConfig == nil && len(before.VotersOf(group.ID)) < nextTarget {
		for _, replica := range group.Replicas {
			priorNode := before.Node(replica.NodeName)
			if priorNode == nil || priorNode.State != NodeActive {
				return true // admission or activation may fill legacy voter seats
			}
		}
	}

	return false
}

func validateReplicaPlacement(desired Document, group Group, report GroupPlacementReport) error {
	candidates := make(map[string]PlacementCandidate, len(report.Candidates))
	for _, candidate := range report.Candidates {
		candidates[candidate.NodeName] = candidate
	}
	var voters []ReplicaKey
	for _, replica := range group.Replicas {
		candidate, ok := candidates[replica.NodeName]
		if !ok || group.ID != MetaGroupID && !candidate.CanRetain {
			return placementTransitionError(report, replica.NodeName, "host_ineligible")
		}
		if replica.Role == ReplicaVoter {
			if !candidate.CanVote {
				return placementTransitionError(report, replica.NodeName, "voter_ineligible")
			}
			voters = append(voters, ReplicaKey{NodeName: replica.NodeName, ReplicaID: replica.ReplicaID})
		}
	}
	if err := ValidateVoterPlacement(desired, group.ID, voters); err != nil {
		return placementTransitionError(report, "", "voter_domain_conflict")
	}
	return nil
}

func validatePlacementEdit(view ReadView, edited Document, options placementPrepareOptions) error {
	rules, at, explain := options.rules, options.at, options.explain
	if !coherentPlacementEdit(view, edited) {
		return invalid("incoherent placement edit or topology view")
	}
	if !explain && (at.IsZero() || strings.TrimSpace(edited.ChangedBy) == "") {
		return invalid("placement edit requires time and actor")
	}
	if err := rules.Limits.Validate(); err != nil {
		return err
	}
	for _, doc := range []Document{view.Latest, view.Target} {
		if err := doc.Validate(rules); err != nil {
			return err
		}
	}

	return nil
}

func coherentPlacementEdit(view ReadView, edited Document) bool {
	return !(view.Latest.Version == 0 || view.Latest.Version == math.MaxUint64 || view.Target.Version == 0 ||
		view.Latest.Version < view.Target.Version || view.Latest.ClusterID == "" ||
		view.Target.ClusterID != view.Latest.ClusterID ||
		edited.ClusterID != view.Latest.ClusterID || edited.Version != view.Latest.Version)
}

func appendPlacementRejections(report *PlacementReport, err error) {
	var placement *PlacementError
	if errors.As(err, &placement) {
		for _, rejected := range placement.Report.Groups {
			for i := range report.Groups {
				if report.Groups[i].GroupID == rejected.GroupID {
					report.Groups[i].Issues = append(report.Groups[i].Issues, rejected.Issues...)
				}
			}
		}
	}
}

func materializePlacementGroups(view ReadView, next *Document, affected []string,
	options placementPrepareOptions) error {
	approved, before, at := view.Target, view.Latest, options.at
	for _, id := range affected {
		group := next.Group(id)
		if id != MetaGroupID {
			if err := materializeHosting(approved, next, group, at); err != nil {
				return err
			}
		}
		if needsVoterReconciliation(before, *next, id) {
			if err := reconcileGroupVoters(before, next, id, at); err != nil {
				return err
			}
		}
	}

	return nil
}
