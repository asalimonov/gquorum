package metaengine

import (
	"context"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// HasLoadPeer checks the same approved and retained identities as LoadCatalog
// without constructing a sorted view. Process incarnation is checked separately
// by metaservice on every batch; this index is not cached authorization.
func (m *Machine) HasLoadPeer(ctx context.Context, id storage.Identity) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := topology.ValidateLoadID(id.GroupID); err != nil {
		return false, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, found := m.scoped.indexes.loadPeers[id]
	return found, nil
}

// Built with the candidate graph and published atomically with its validated
// metadata. Session-only changes can share this immutable index; graph changes,
// pruning and restore rebuild it. Latest alone never authorizes a route.
func indexLoadPeers(s *scopedState, index *scopedIndexes) map[storage.Identity]struct{} {
	peers := make(map[storage.Identity]struct{})
	addLoadPeerDocument(peers, s.Target)
	for _, row := range s.Groups {
		if row.Declaration == nil {
			continue
		}
		if doc, found := checkpointDocument(s, index, row); found {
			if group := doc.Group(row.GroupID); group != nil {
				addLoadPeerGroup(peers, doc, *group)
			}
		}
	}
	for _, operation := range s.Operations {
		addLoadPeerDocument(peers, index.documents[operation.Status.Version])
	}
	for _, record := range s.AuthorityRecords {
		if record.Grant.Key.GroupID == topology.MetaGroupID {
			continue
		}
		for _, target := range record.Grant.Targets {
			peers[storage.Identity{ClusterID: record.Grant.Key.ClusterID, NodeName: target.NodeName,
				GroupID: record.Grant.Key.GroupID, ReplicaID: target.ReplicaID}] = struct{}{}
		}
	}
	for _, debt := range s.Cleanup {
		peers[debt.Identity] = struct{}{}
	}
	return peers
}

func addLoadPeerDocument(peers map[storage.Identity]struct{}, doc topology.Document) {
	for _, group := range doc.Groups {
		addLoadPeerGroup(peers, doc, group)
	}
}

func addLoadPeerGroup(peers map[storage.Identity]struct{}, doc topology.Document, group topology.Group) {
	if group.ID == topology.MetaGroupID {
		return
	}
	for _, replica := range group.Replicas {
		peers[storage.Identity{ClusterID: doc.ClusterID, NodeName: replica.NodeName,
			GroupID: group.ID, ReplicaID: replica.ReplicaID}] = struct{}{}
	}
}
