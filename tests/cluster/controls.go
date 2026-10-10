//go:build cluster

package cluster

import (
	"context"
	"fmt"

	"github.com/asalimonov/gquorum/meta"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type LocalObservation struct {
	Identity          meta.Identity        `json:"identity"`
	Found             bool                 `json:"found"`
	ReplicaID         uint64               `json:"replica_id"`
	Status            topology.GroupStatus `json:"status"`
	Storage           storage.Stats        `json:"storage"`
	Values            map[string]string    `json:"values"`
	ReceivedSnapshots uint64               `json:"received_snapshots"`
}
type BarrierObservation struct {
	Group string                  `json:"group"`
	Kind  topology.StepKind       `json:"kind"`
	Hits  uint64                  `json:"hits"`
	Last  topology.AuthorizedStep `json:"last"`
}
type Controls struct {
	Barriers []BarrierObservation      `json:"barriers"`
	Receipts []topology.AuthorizedStep `json:"receipts"`
}

func (c *Cluster) Local(ctx context.Context, node, group string) (LocalObservation, error) {
	var result LocalObservation
	response, err := c.Request(ctx, node, "GET", "/internal/test/local/"+group, nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		return result, err
	}
	if err := response.Decode(&result); err != nil {
		return result, err
	}
	if result.Identity.NodeName != node || result.Identity.ClusterID != c.ID || result.Identity.MemberID == 0 || result.Identity.InstanceID == "" || result.Found && (result.Status.HostNode != node || result.Status.GroupID != group) {
		return result, fmt.Errorf("local observation identity does not match %s/%s", node, group)
	}
	return result, nil
}
func (c *Cluster) Controls(ctx context.Context, node string) (Controls, error) {
	var result Controls
	response, err := c.Request(ctx, node, "GET", "/internal/test/controls", nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		return result, err
	}
	return result, response.Decode(&result)
}
func (c *Cluster) SetBarrier(ctx context.Context, node, group string, kind topology.StepKind, enabled bool) error {
	method := "DELETE"
	if enabled {
		method = "PUT"
	}
	response, err := c.Request(ctx, node, method, "/internal/test/barriers/"+group+"/"+string(kind), nil, 0)
	return requireStatus(response, err, 200)
}
func (c *Cluster) Snapshot(ctx context.Context, node, group string) error {
	response, err := c.Request(ctx, node, "POST", "/internal/test/local/"+group+"/snapshot", nil, 0)
	return requireStatus(response, err, 200)
}
