package metaservice

import "github.com/asalimonov/gquorum/topology"

import "github.com/asalimonov/gquorum/internal/metastore"

// LoadStoredIdentity reads and validates a closed store without creating or
// changing it. InstanceID is empty: process identity is never a durable member
// pin. Caller-owned readmission/quarantine code can verify the exact node,
// cluster and member tuple before moving its supplied database path.
func LoadStoredIdentity(path string) (topology.Identity, bool, error) {
	pin, found, err := metastore.LoadIdentity(path)
	if err != nil {
		return topology.Identity{}, false, err
	}
	return topology.Identity{NodeName: pin.NodeName, ClusterID: pin.ClusterID, MemberID: pin.MemberID}, found, nil
}
