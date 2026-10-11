package load

import (
	"context"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// Transport is the application-selected network adapter. Authenticate the
// encoded sender before dispatching to Node. Honor cancellation; calls to one
// destination are serialized while different destinations run independently.
// ApplyAction can target this same node; forward it to Node.ApplyLoadAction
// through an in-process call or the application protocol.
// SendRaft receives owned messages. A nil return means acceptance, not completed
// snapshot transfer: report that exact delivery through Node.ReportLoadDelivery.
type Transport interface {
	SendRaft(context.Context, RaftBatch) error
	ReadReplica(context.Context, ReplicaQuery) (ReplicaStatus, error)
	ApplyAction(context.Context, ActionRequest) (ActionResult, error)
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
}
type ReplicaStatus struct {
	Peer  Peer
	Found bool
	// Status is a raw replica observation. LeaderNodeName is not authoritative;
	// cluster status and scheduler inputs resolve it through their Active view.
	Status     topology.GroupStatus
	LocalState string
}
type ActionRequest struct {
	From          topology.Identity
	To            Peer
	Authorization topology.AuthorizedStep
	LocalTarget   storage.Identity
}
type ActionResult struct {
	From          Peer
	Authorization topology.AuthorizedStep
	LocalTarget   storage.Identity
	Verified      bool
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
