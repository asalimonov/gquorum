package topology

import "maps"

// GroupProgress separates staged, approved and proven group declarations.
type GroupProgress struct {
	GroupID          string         `json:"group_id"`
	Desired          *Group         `json:"desired,omitempty"`
	Approved         *Group         `json:"approved,omitempty"`
	Converged        *Group         `json:"converged,omitempty"`
	ConvergedVersion uint64         `json:"converged_version"`
	OperationID      OperationID    `json:"operation_id,omitempty"`
	Deleting         bool           `json:"deleting"`
	Blocked          *BlockedReason `json:"blocked,omitempty"`
	Observation      *GroupStatus   `json:"observation,omitempty"`
}

// NodeProgress reports proven node/member identity independently of observed health.
type NodeProgress struct {
	NodeName         string         `json:"node_name"`
	Desired          *Node          `json:"desired,omitempty"`
	Approved         *Node          `json:"approved,omitempty"`
	Converged        *Node          `json:"converged,omitempty"`
	ConvergedVersion uint64         `json:"converged_version"`
	MemberID         uint64         `json:"member_id"`
	OperationID      OperationID    `json:"operation_id,omitempty"`
	Blocked          *BlockedReason `json:"blocked,omitempty"`
}

// Clone returns owned declarations, blockers and cached observations.
func (p GroupProgress) Clone() GroupProgress {
	owned := p
	owned.Desired = cloneProgressGroup(p.Desired)
	owned.Approved = cloneProgressGroup(p.Approved)
	owned.Converged = cloneProgressGroup(p.Converged)
	owned.Blocked = cloneBlockedReason(p.Blocked)
	if p.Observation != nil {
		status := p.Observation.Clone()
		owned.Observation = &status
	}
	return owned
}

func cloneProgressGroup(group *Group) *Group {
	if group == nil {
		return nil
	}
	owned := cloneGroup(*group)
	return &owned
}

func cloneProgressNode(node *Node) *Node {
	if node == nil {
		return nil
	}
	owned := *node
	owned.Labels = maps.Clone(node.Labels)
	return &owned
}

// Clone returns owned node declarations and diagnostics.
func (p NodeProgress) Clone() NodeProgress {
	owned := p
	owned.Desired = cloneProgressNode(p.Desired)
	owned.Approved = cloneProgressNode(p.Approved)
	owned.Converged = cloneProgressNode(p.Converged)
	owned.Blocked = cloneBlockedReason(p.Blocked)
	return owned
}
