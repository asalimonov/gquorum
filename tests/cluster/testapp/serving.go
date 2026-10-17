package main

import (
	"context"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

// Read the exact group and host at one metadata index. These two rows precede
// operation and cleanup rows, so retained history cannot displace either one.
// The historical FullyConverged marker is never a serving prerequisite.
func servingGroup(ctx context.Context, cluster gquorum.Cluster, id string, identity topology.Identity) (*topology.Group, error) {
	page, err := cluster.ReadProgress(ctx, topology.ProgressQuery{GroupID: id, NodeName: identity.NodeName, Limit: 2})
	if err != nil {
		return nil, err
	}
	if len(page.Nodes) != 1 || page.Nodes[0].MemberID != identity.MemberID {
		return nil, fault.ErrIdentityMismatch
	}
	if len(page.Groups) != 1 || page.Groups[0].Converged == nil || page.Groups[0].ConvergedVersion == 0 {
		return nil, fault.ErrReplicaNotReady
	}
	return page.Groups[0].Converged, nil
}
