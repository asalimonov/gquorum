package topology

import (
	"fmt"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
)

// reconcileGroupVoters chooses final seats after hosting has been materialized.
// The caller decides whether a constraint change actually calls for a new role
// selection; reservation release alone must leave healthy voter seats intact.
func reconcileGroupVoters(before Document, next *Document, groupID string, at time.Time) error {
	group := next.Group(groupID)
	if group == nil {
		return invalid("unknown placement group")
	}
	prior := before.Group(groupID)
	need := 0
	if group.VoterConfig != nil {
		need = group.VoterConfig.TargetVoters
	} else if prior != nil {
		need = next.GroupPlacement(groupID).MaxReplicas
	}
	if need == 0 {
		return invalid("group has no voter target")
	}
	report, err := EvaluatePlacement(PlacementInput{Approved: before, Desired: *next, GroupID: groupID})
	if err != nil {
		return err
	}
	candidates := slices.Clone(report.Candidates)
	selection := voterSeatSelection{group: group, prior: prior, need: need}
	slices.SortFunc(candidates, func(a, b PlacementCandidate) int {
		if ra, rb := selection.rank(a.NodeName), selection.rank(b.NodeName); ra != rb {
			return ra - rb
		}
		return compareNames(a.NodeName, b.NodeName)
	})
	selected := selection.selectCandidates(candidates)
	if err := selection.validateCount(before, next, selected, report); err != nil {
		return err
	}
	selection.assignRoles(selected, at)
	return nil
}

// materializeHosting applies the complete eligible host set to an owned
// candidate. Newly assigned replicas always begin as learners; role selection
// is a separate decision and an existing exact identity keeps its pin.
func materializeHosting(approved Document, doc *Document, group *Group, at time.Time) error {
	if group == nil || group.ID == MetaGroupID {
		return invalid("materialization requires a load group")
	}
	hosts, err := RequiredLoadHostsFor(approved, *doc, group.ID)
	if err != nil {
		return err
	}
	wanted := make(map[string]bool, len(hosts))
	for _, name := range hosts {
		wanted[name] = true
	}
	group.Replicas = slices.DeleteFunc(group.Replicas, func(replica Replica) bool {
		return !wanted[replica.NodeName]
	})
	for _, name := range hosts {
		if groupReplicaOnNode(group, name) != nil {
			continue
		}
		id, err := group.AllocateReplicaID()
		if err != nil {
			return err
		}
		group.Replicas = append(group.Replicas, Replica{NodeName: name, ReplicaID: id, Role: ReplicaLearner,
			CreatedAt: at, UpdatedAt: at})
	}
	slices.SortFunc(group.Replicas, func(a, b Replica) int { return compareNames(a.NodeName, b.NodeName) })
	return nil
}

type voterSeatSelection struct {
	group *Group
	prior *Group
	need  int
}

func (v *voterSeatSelection) rank(name string) int {
	current := groupReplicaOnNode(v.group, name)
	if current == nil || v.prior == nil {
		return 2
	}
	old := groupReplicaOnNode(v.prior, name)
	if old == nil || old.ReplicaID != current.ReplicaID {
		return 2
	}
	if old.Role == ReplicaVoter {
		return 0
	}
	return 1
}

func (v *voterSeatSelection) selectCandidates(candidates []PlacementCandidate) map[string]bool {
	selected := make(map[string]bool, v.need)
	domains := make(map[string]bool, v.need)
	for _, candidate := range candidates {
		if len(selected) == v.need {
			break
		}
		if !v.eligible(candidate, domains) {
			continue
		}
		selected[candidate.NodeName] = true
	}

	return selected
}

func (v *voterSeatSelection) eligible(candidate PlacementCandidate, domains map[string]bool) bool {
	replica := groupReplicaOnNode(v.group, candidate.NodeName)
	if replica == nil || !candidate.CanVote || replica.PinnedLearner {
		return false
	}
	if v.group.VoterConfig == nil && v.rank(candidate.NodeName) == 1 {
		return false // nil-config learners retain their intentional role
	}
	if v.group.VoterConfig != nil && v.group.VoterConfig.DistinctVoterLabel != "" {
		if candidate.Domain == "" || domains[candidate.Domain] {
			return false
		}
		domains[candidate.Domain] = true
	}

	return true
}

func (v *voterSeatSelection) validateCount(before Document, next *Document, selected map[string]bool,
	report GroupPlacementReport) error {
	minimum, _ := next.GroupBounds(v.group.ID)
	if len(selected) < minimum || v.group.VoterConfig != nil && v.group.VoterConfig.DistinctVoterLabel != "" &&
		len(selected) != v.need {
		code := "insufficient_voter_candidates"
		if v.group.VoterConfig != nil && v.group.VoterConfig.DistinctVoterLabel != "" {
			code = "insufficient_distinct_zones"
		}
		report.Issues = append(report.Issues, PlacementIssue{
			Code: code, GroupID: v.group.ID,
			Message: fmt.Sprintf("group %q requires %d eligible voters; %d can be selected", v.group.ID, v.need, len(
				selected)),
		})
		return &PlacementError{
			Code: fault.Invalid,
			Report: PlacementReport{
				Version:         next.Version,
				ApprovedVersion: before.Version,
				Groups:          []GroupPlacementReport{report},
			},
		}
	}

	return nil
}

func (v *voterSeatSelection) assignRoles(selected map[string]bool, at time.Time) {
	for i := range v.group.Replicas {
		replica := &v.group.Replicas[i]
		role := ReplicaLearner
		if selected[replica.NodeName] {
			role = ReplicaVoter
		}
		if replica.Role != role {
			replica.Role = role
			replica.UpdatedAt = at
		}
	}
}
