package metaservice

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func (g *Group) snapshotSender(ctx context.Context, msg raftpb.Message, identity topology.Identity,
	cachedName string) (string, error) {
	if msg.Snapshot == nil {
		return "", fault.ErrInvalid
	}
	state, err := raftstate.DecodeRaftSnapshot(*msg.Snapshot, identity.ClusterID, topology.MetaGroupID)
	if err != nil {
		return "", err
	}
	check := metaengine.NewMachine(g.rules, nil)
	if err := check.Restore(ctx, state.Application); err != nil {
		return "", err
	}
	latest := check.Latest()
	if latest.Version > 0 && latest.ClusterID != identity.ClusterID {
		return "", fault.ErrIdentityMismatch
	}
	if cachedName != "" {
		return cachedName, nil
	}
	return snapshotMemberName(check, msg.From), nil
}

func snapshotMemberName(machine *metaengine.Machine, member uint64) string {
	var name string
	for _, doc := range []topology.Document{machine.Target(), machine.Active()} {
		meta := doc.MetaGroup()
		if meta == nil {
			continue
		}
		for _, replica := range meta.Replicas {
			if replica.ReplicaID == member {
				name = replica.NodeName
			}
		}
	}
	return name
}
