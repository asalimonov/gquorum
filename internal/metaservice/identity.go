package metaservice

import (
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/topology"
)

// LoadStoredIdentity reads and validates a closed store without creating or
// changing it. InstanceID is empty: process identity is never a durable member
// pin. Caller-owned readmission/quarantine code can verify the exact node,
// cluster and member tuple before moving its supplied database path.
func LoadStoredIdentity(path string) (topology.IdentityLookup, error) {
	pin, err := metastore.LoadIdentity(path)
	if err != nil {
		return topology.IdentityLookup{}, err
	}
	return topology.IdentityLookup{Identity: topology.Identity{NodeName: pin.Identity.NodeName,
		ClusterID: pin.Identity.ClusterID, MemberID: pin.Identity.MemberID}, Found: pin.Found}, nil
}
