package topology

// Plan contains the ordered steps and initial voter sets for one topology
// version.
type Plan struct {
	Version      uint64           `json:"version"`
	Steps        []Step           `json:"steps"`
	VoterBases   []PlanVoterBase  `json:"voter_bases,omitempty"`
	Blocks       []PlanBlock      `json:"blocks"`
	Dependencies []StepDependency `json:"dependencies,omitempty"`
}

// PlanVoterBase records the native voter set used to plan a group's membership
// changes.
type PlanVoterBase struct {
	GroupID string   `json:"group_id"`
	Voters  []uint64 `json:"voters"`
}
