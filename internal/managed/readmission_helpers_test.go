package managed

import (
	"testing"

	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/topology"
)

func writeMeta(t *testing.T, path string, node topology.Identity, replica uint64) {
	t.Helper()
	s, err := metastore.Open(path)
	must(t, err)
	must(t, s.EnsureIdentity(node.NodeName, node.ClusterID, node.MemberID))
	if replica > 0 {
		must(t, s.RecordReplica("work", metastore.ReplicaRecord{NodeName: node.NodeName, ClusterID: node.ClusterID, ReplicaID: replica}))
	}
	must(t, s.Close())
}
