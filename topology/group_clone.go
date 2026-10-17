package topology

import (
	"maps"
	"slices"
)

func cloneGroup(group Group) Group {
	owned := group
	owned.Replicas = slices.Clone(group.Replicas)
	owned.Labels = maps.Clone(group.Labels)
	if group.VoterConfig != nil {
		config := *group.VoterConfig
		owned.VoterConfig = &config
	}
	if group.Hosting != nil {
		hosting := *group.Hosting
		hosting.Nodes = slices.Clone(hosting.Nodes)
		owned.Hosting = &hosting
	}
	if group.Placement != nil {
		placement := group.Placement.Clone()
		owned.Placement = &placement
	}
	return owned
}
