package load

import (
	"context"
	"github.com/asalimonov/gquorum/metrics"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// Transport is the application-selected network adapter. Authenticate the
// encoded sender before dispatching to Node. Honor cancellation. SendRaft calls
// to one destination are serialized while different destinations run independently.
// ReadReplica may run concurrently for metrics and native membership observation.
// ApplyAction can target this same node; forward it to Node.ApplyLoadAction
// through an in-process call or the application protocol.
// Preserve membership.SafetyRefusal details in ApplyAction errors so transient
// quorum or health loss remains distinguishable from permanent refusal.
// SendRaft receives owned messages. A nil return means acceptance, not completed
// snapshot transfer: report that exact delivery through Node.ReportLoadDelivery.
type Transport interface {
	SendRaft(context.Context, RaftBatch) error
	ReadReplica(context.Context, ReplicaQuery) (ReplicaStatus, error)
	ApplyAction(context.Context, ActionRequest) (ActionResult, error)
	InstallFence(context.Context, FenceRequest) (FenceResult, error)
}
type Peer struct {
	Node    topology.Identity
	Replica ReplicaRef
}
type RaftBatch struct {
	From, To   Peer
	DeliveryID uint64
	Messages   []raftpb.Message
}
type ReplicaQuery struct {
	From topology.Identity
	To   Peer
	// Native requests quorum-confirmed applied membership, independently of the
	// diagnostic Status below. Transports must preserve this flag and the reply.
	Native bool
}
type ReplicaStatus struct {
	Peer  Peer
	Found bool
	// Native is present only for a successful Native query on a ready replica.
	// Absence (including Found=false) never proves that a group does not exist.
	Native *topology.NativeState `json:"native,omitempty"`
	// Status is a raw replica observation. LeaderNodeName is not authoritative;
	// cluster status and scheduler inputs resolve it through their Active view.
	Status     topology.GroupStatus
	LocalState string
	Metrics    *metrics.ReplicaSample `json:"metrics,omitempty"`
}
type ActionRequest struct {
	Grant         topology.AuthorityGrant
	From          topology.Identity
	To            Peer
	Authorization topology.AuthorizedStep
	// LocalTarget identifies a host effect. For founding, an empty target asks
	// the running group to persist its separate native membership receipt.
	LocalTarget storage.Identity
}
type ActionResult struct {
	Receipts      []topology.EffectReceipt
	From          Peer
	Authorization topology.AuthorizedStep
	LocalTarget   storage.Identity
}

type FenceRequest struct {
	From      topology.Identity          `json:"from"`
	To        topology.Identity          `json:"to"`
	ReplicaID uint64                     `json:"replica_id,omitempty"`
	Resource  topology.AuthorityResource `json:"resource"`
	Grant     topology.AuthorityGrant    `json:"grant"`
}

type FenceResult struct {
	From     topology.Identity      `json:"from"`
	Evidence topology.FenceEvidence `json:"evidence"`
}
type DeliveryKind string

const (
	DeliveryUnreachable       DeliveryKind = "unreachable"
	DeliverySnapshotSucceeded DeliveryKind = "snapshot_succeeded"
	DeliverySnapshotFailed    DeliveryKind = "snapshot_failed"
)

type DeliveryReport struct {
	From, To   Peer
	DeliveryID uint64
	Kind       DeliveryKind
}
