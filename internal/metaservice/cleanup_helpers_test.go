package metaservice

import (
	"time"

	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func collectionWireReceipt() authoritystore.CollectionReceipt {
	return authoritystore.CollectionReceipt{Identity: storage.Identity{ClusterID: "cluster", NodeName: "n1",
		GroupID: "work", ReplicaID: 3}, MemberID: 1,
		RetirementKey: topology.AuthorityKey{ClusterID: "cluster", GroupID: "work", Revision: 10},
		Token:         "opaque", LocalSequence: 2, CollectedAt: time.Unix(100, 0).UTC()}
}
