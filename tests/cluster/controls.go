//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"strconv"

	gquorum "github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// ReadCounters reports exact fixture read, apply and Raft fault activity.
type ReadCounters struct {
	DropVoteRequestsFrom  []uint64 `json:"drop_vote_requests_from,omitempty"`
	VoteRequestsDropped   uint64   `json:"vote_requests_dropped"`
	DropAppend            bool     `json:"drop_append"`
	DropRaft              bool     `json:"drop_raft"`
	DropReadResponse      bool     `json:"drop_read_response"`
	HoldApply             bool     `json:"hold_apply"`
	HoldQuery             bool     `json:"hold_query"`
	ApplyCalls            uint64   `json:"apply_calls"`
	ApplyActive           uint64   `json:"apply_active"`
	ApplyCompleted        uint64   `json:"apply_completed"`
	AppliedIndex          uint64   `json:"applied_index"`
	QueryCalls            uint64   `json:"query_calls"`
	QueryActive           uint64   `json:"query_active"`
	QueryCompleted        uint64   `json:"query_completed"`
	AppendDropped         uint64   `json:"append_dropped"`
	RaftDropped           uint64   `json:"raft_dropped"`
	ReadRequestsSent      uint64   `json:"read_requests_sent"`
	ReadContextsSent      uint64   `json:"read_contexts_sent"`
	ReadResponsesReceived uint64   `json:"read_responses_received"`
	ReadResponsesDropped  uint64   `json:"read_responses_dropped"`
	ReadResponseIndex     uint64   `json:"read_response_index"`
}

// ReadFaults targets read and Raft fault controls at an exact process incarnation.
type ReadFaults struct {
	Identity             topology.Identity `json:"identity"`
	DropAppend           bool              `json:"drop_append"`
	DropRaft             bool              `json:"drop_raft"`
	DropReadResponse     bool              `json:"drop_read_response"`
	DropVoteRequestsFrom []uint64          `json:"drop_vote_requests_from,omitempty"`
	HoldApply            bool              `json:"hold_apply"`
	HoldQuery            bool              `json:"hold_query"`
}

// SetReadFaults changes or reads authenticated fault controls for an exact replica.
func (c *Cluster) SetReadFaults(ctx context.Context, node, group string, replica uint64, method string, faults ReadFaults) (ReadCounters, error) {
	var result ReadCounters
	response, err := c.Request(ctx, node, method, "/internal/test/reads/"+group+"/"+strconv.FormatUint(replica, 10), faults, 0)
	if err := requireStatus(response, err, 200); err != nil {
		return result, err
	}
	return result, response.Decode(&result)
}

// LocalObservation combines exact local process, replica, application and persistence observations.
type LocalObservation struct {
	Reads               ReadCounters         `json:"reads"`
	ReadQueue           metrics.ReadQueue    `json:"read_queue"`
	ReadQueueStamp      metrics.Stamp        `json:"read_queue_stamp"`
	Identity            topology.Identity    `json:"identity"`
	Found               bool                 `json:"found"`
	ReplicaID           uint64               `json:"replica_id"`
	Status              topology.GroupStatus `json:"status"`
	Storage             storage.Stats        `json:"storage"`
	Values              map[string]string    `json:"values"`
	ReceivedSnapshots   uint64               `json:"received_snapshots"`
	WAL                 WALCounters          `json:"wal"`
	WALStallNS          int64                `json:"wal_stall_ns"`
	Proposals           load.ProposalStats   `json:"proposals"`
	Parked              *gquorum.ParkedLoad  `json:"parked,omitempty"`
	StorageIdentity     *storage.Identity    `json:"storage_identity,omitempty"`
	ServeOverride       int                  `json:"serve_override"`
	StorageAvailable    bool                 `json:"storage_available"`
	WALControlAvailable bool                 `json:"wal_control_available"`
	GroupIdentity       *storage.Identity    `json:"group_identity,omitempty"`
}

// ProjectionObservation reports external projection progress and its exact replica identity.
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

// Projection reads or stalls projection work and verifies the returned exact replica identity.
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

// BarrierObservation records hits and refusals for a selected approved topology step.
type BarrierObservation struct {
	Group  string                  `json:"group"`
	Kind   topology.StepKind       `json:"kind"`
	Hits   uint64                  `json:"hits"`
	Last   topology.AuthorizedStep `json:"last"`
	Refuse bool                    `json:"refuse"`
}

// Controls contains fixture step barriers and acknowledged authorization tuples.
type Controls struct {
	Barriers []BarrierObservation      `json:"barriers"`
	Receipts []topology.AuthorizedStep `json:"receipts"`
}

// Local reads fixture observations and validates the returned node and group identity.
func (c *Cluster) Local(ctx context.Context, node, group string) (LocalObservation, error) {
	var result LocalObservation
	response, err := c.Request(ctx, node, "GET", "/internal/test/local/"+group, nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		return result, err
	}
	if err := response.Decode(&result); err != nil {
		return result, err
	}
	if result.Identity.NodeName != node || result.Identity.ClusterID != c.expectedCluster(node) || result.Identity.MemberID == 0 || result.Identity.InstanceID == "" || result.Found && (result.Status.HostNode != node || result.Status.GroupID != group) {
		return result, fmt.Errorf("local observation identity does not match %s/%s", node, group)
	}
	return result, nil
}

// Controls reads authenticated step barriers and acknowledged actions from a process.
func (c *Cluster) Controls(ctx context.Context, node string) (Controls, error) {
	var result Controls
	response, err := c.Request(ctx, node, "GET", "/internal/test/controls", nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		return result, err
	}
	return result, response.Decode(&result)
}

// SetBarrier enables or clears refusal of a selected group's topology step.
func (c *Cluster) SetBarrier(ctx context.Context, node, group string, kind topology.StepKind, enabled bool) error {
	method := "DELETE"
	if enabled {
		method = "PUT"
	}
	response, err := c.Request(ctx, node, method, "/internal/test/barriers/"+group+"/"+string(kind), nil, 0)
	return requireStatus(response, err, 200)
}

// Snapshot requests a local fixture replica snapshot through the authenticated control endpoint.
func (c *Cluster) Snapshot(ctx context.Context, node, group string) error {
	response, err := c.Request(ctx, node, "POST", "/internal/test/local/"+group+"/snapshot", nil, 0)
	return requireStatus(response, err, 200)
}

// WALCounters reports persistence attempts, blocked writes and injected failures.
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

// WALFault configures or releases write stalls and failures for an exact replica.
func (c *Cluster) WALFault(ctx context.Context, node, group string, replica uint64, stall bool, failNext uint64, release bool) error {
	method := "PUT"
	if release {
		method = "DELETE"
	}
	response, err := c.Request(ctx, node, method, "/internal/test/wal/"+group, map[string]any{"replica_id": replica, "stall": stall, "fail_next": failNext}, 0)
	return requireStatus(response, err, 200)
}

// FenceFault selects an exact process, replica and authority revision for a fence barrier.
type FenceFault struct {
	Identity    topology.Identity         `json:"identity"`
	ReplicaID   uint64                    `json:"replica_id,omitempty"`
	Purpose     topology.AuthorityPurpose `json:"purpose,omitempty"`
	MinRevision uint64                    `json:"min_revision,omitempty"`
	After       bool                      `json:"after"`
}

// MetaFaultObservation reports meta Raft frame drops for an exact process incarnation.
type MetaFaultObservation struct {
	Identity      topology.Identity `json:"identity"`
	DropRaft      bool              `json:"drop_raft"`
	FramesDropped uint64            `json:"frames_dropped"`
}

// MetaFault changes or reads authenticated meta Raft drop controls.
func (c *Cluster) MetaFault(ctx context.Context, node, method string, identity topology.Identity, drop bool) (MetaFaultObservation, error) {
	var view MetaFaultObservation
	response, err := c.Request(ctx, node, method, "/internal/test/meta", map[string]any{"identity": identity, "drop_raft": drop}, 0)
	if err := requireStatus(response, err, 200); err != nil {
		return view, err
	}
	return view, response.Decode(&view)
}

// FenceFaultObservation reports barrier hits and the exact last fence request.
type FenceFaultObservation struct {
	FenceFault
	Group string                         `json:"group"`
	Kind  topology.AuthorityResourceKind `json:"kind"`
	Hits  uint64                         `json:"hits"`
	Last  load.FenceRequest              `json:"last"`
}

// AuthorityObservation records granted actions, durable receipts, fences and barrier observations.
type AuthorityObservation struct {
	Actions  []load.ActionRequest    `json:"actions"`
	Receipts []load.ActionResult     `json:"receipts"`
	Fences   []load.FenceResult      `json:"fences"`
	Barriers []FenceFaultObservation `json:"barriers"`
}

// SetFenceFault changes authenticated fence barriers for a selected resource.
func (c *Cluster) SetFenceFault(ctx context.Context, node, group string, kind topology.AuthorityResourceKind, method string, fault FenceFault) error {
	response, err := c.Request(ctx, node, method, "/internal/test/fences/"+group+"/"+string(kind), fault, 0)
	return requireStatus(response, err, 200)
}

// Authority reads recorded fixture authority effects and fences for a group.
func (c *Cluster) Authority(ctx context.Context, node, group string) (AuthorityObservation, error) {
	var view AuthorityObservation
	response, err := c.Request(ctx, node, "GET", "/internal/test/authority/"+group, nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		return view, err
	}
	return view, response.Decode(&view)
}

// Revocation reads retained operator fence obligations through the fixture endpoint.
func (c *Cluster) Revocation(ctx context.Context, node string, operator topology.OperationID) (topology.AuthorityStatus, error) {
	var status topology.AuthorityStatus
	response, err := c.Request(ctx, node, "GET", "/internal/test/revocation/"+string(operator), nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		return status, err
	}
	return status, response.Decode(&status)
}

// ReplayAction submits an exact prior grant and action through the authenticated replay control.
func (c *Cluster) ReplayAction(ctx context.Context, node string, identity topology.Identity, action load.ActionRequest) (Response, error) {
	return c.Request(ctx, node, "POST", "/internal/test/authority/"+action.Grant.Key.GroupID+"/action", map[string]any{"identity": identity, "action": action}, 0)
}
