package metaengine

import (
	"strings"

	"github.com/asalimonov/gquorum/topology"
)

// A floor is permanent non-reuse evidence, not execution authority. Schema 4
// may retain its key after the settled holder and all its proof are pruned.
// Validate shape here: a missing holder can no longer supply Grant.Validate.
func validResourceFloor(f resourceFloor, state smState) bool {
	if f.Key.Revision == 0 || f.Key.Revision > state.AppliedIndex ||
		f.Key.ClusterID != state.Latest.ClusterID || f.Key.GroupID != f.Resource.GroupID ||
		strings.TrimSpace(f.Key.ClusterID) == "" || strings.ContainsRune(f.Key.ClusterID, 0) ||
		topology.ValidateID(f.Key.GroupID) != nil {
		return false
	}
	switch f.Resource.Kind {
	case topology.AuthorityGroup:
		return f.Resource.NodeName == ""
	case topology.AuthorityHost:
		return strings.TrimSpace(f.Resource.NodeName) != "" && !strings.ContainsRune(f.Resource.NodeName, 0)
	default:
		return false
	}
}
