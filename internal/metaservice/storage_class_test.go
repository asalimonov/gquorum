//go:build !functional

package metaservice

import (
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestCreateLoadGroupRecordsStorageClass(t *testing.T) {
	t.Parallel()
	base := topology.Document{ClusterID: "c", Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3},
		Nodes: []topology.Node{{Name: "a", Type: "worker", State: topology.NodeActive}}}
	now := time.Now().UTC()
	for class, valid := range map[topology.StorageClass]bool{topology.StorageDefault: true, topology.StorageMemory: true, "disk": false} {
		doc := base.Clone()
		err := createLoadGroup(&doc, GroupSpec{ID: "work", Storage: class}, now)
		if !valid {
			if !errors.Is(err, fault.ErrInvalid) || doc.Group("work") != nil {
				t.Errorf("%q accepted: %v", class, err)
			}
			continue
		}
		if err != nil || doc.Group("work") == nil || doc.Group("work").Storage != class {
			t.Errorf("%q not recorded: %v", class, err)
		}
	}
}
