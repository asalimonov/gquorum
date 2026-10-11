//go:build cluster

package cluster

import (
	"context"
	"fmt"
	"strconv"

	gquorum "github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type LocalObservation struct {
	Identity          topology.Identity          `json:"identity"`
	Found             bool                       `json:"found"`
	ReplicaID         uint64                     `json:"replica_id"`
	Status            topology.GroupStatus       `json:"status"`
	Storage           storage.Stats              `json:"storage"`
	Values            map[string]string          `json:"values"`
	ReceivedSnapshots uint64                     `json:"received_snapshots"`
	WAL               WALCounters                `json:"wal"`
	WALStallNS        int64                      `json:"wal_stall_ns"`
	Proposals         load.ProposalStats         `json:"proposals"`
	Parked            *gquorum.ParkedLoad        `json:"parked,omitempty"`
	Ledger            *metaservice.ReplicaRecord `json:"ledger,omitempty"`
	ServeOverride     int                        `json:"serve_override"`
}

type ProjectionObservation struct {
	Node                               string `json:"node"`
	Group                              string `json:"group"`
	Replica                            uint64 `json:"replica_id"`
	Attempts, Writes, Failures, Active uint64
	Blocked, Waiting, Stopped          bool
	State                              struct {
		SchemaVersion int               `json:"schema_version"`
		Values        map[string]string `json:"values"`
		Requests      map[string]string `json:"requests"`
		AppliedIndex  uint64            `json:"applied_index"`
	} `json:"state"`
}

func (c *Cluster) Projection(ctx context.Context, node, group string, replica uint64, method string, stall bool) (ProjectionObservation, error) {
	var view ProjectionObservation
	var body any
	if method == "PUT" {
		body = map[string]bool{"stall": stall}
	}
	response, err := c.Request(ctx, node, method, "/internal/test/projection/"+group+"/"+strconv.FormatUint(replica, 10), body, 0)
	if err := requireStatus(response, err, 200); err != nil {
		return view, err
	}
	if err := response.Decode(&view); err != nil {
		return view, err
	}
	if view.Node != node || view.Group != group || view.Replica != replica {
		return view, fmt.Errorf("projection belongs to another node/group/replica: %+v", view)
	}
	return view, nil
}

type BarrierObservation struct {
	Group  string                  `json:"group"`
	Kind   topology.StepKind       `json:"kind"`
	Hits   uint64                  `json:"hits"`
	Last   topology.AuthorizedStep `json:"last"`
	Refuse bool                    `json:"refuse"`
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

type WALCounters struct {
	Attempts          uint64 `json:"attempts"`
	EntryAttempts     uint64 `json:"entry_attempts"`
	Failures          uint64 `json:"failures"`
	Active            uint64 `json:"active"`
	EntryWrites       uint64 `json:"entry_writes"`
	Entries           uint64 `json:"entries"`
	Blocked           bool   `json:"blocked"`
	RemainingFailures uint64 `json:"remaining_failures"`
}

func (c *Cluster) WALFault(ctx context.Context, node, group string, replica uint64, stall bool, failNext uint64, release bool) error {
	method := "PUT"
	if release {
		method = "DELETE"
	}
	response, err := c.Request(ctx, node, method, "/internal/test/wal/"+group, map[string]any{"replica_id": replica, "stall": stall, "fail_next": failNext}, 0)
	return requireStatus(response, err, 200)
}
