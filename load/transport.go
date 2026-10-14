package load

import (
	"context"

	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// Transport is the application-selected network adapter. Authenticate the
// encoded sender before dispatching to Node. Honor cancellation. SendRaft calls
// to one destination are serialized while different destinations run
// independently. ReadReplica may run concurrently for metrics and native
// membership observation.
// ApplyAction can target this same node; forward it to Node.ApplyLoadAction
// through an in-process call or the application protocol.
// Preserve membership.SafetyRefusal details in ApplyAction errors so transient
// quorum or health loss remains distinguishable from permanent refusal.
// SendRaft receives owned messages. A nil return means acceptance, not
// completed snapshot transfer: report that exact delivery through
// Node.ReportLoadDelivery.
type Transport interface {
	SendRaft(context.Context, RaftBatch) error
	ReadReplica(context.Context, ReplicaQuery) (ReplicaStatus, error)
	ApplyAction(context.Context, ActionRequest) (ActionResult, error)
	InstallFence(context.Context, FenceRequest) (FenceResult, error)
}

// Peer identifies an exact load replica and its hosting node incarnation.
type Peer struct {
	Node    topology.Identity
	Replica ReplicaRef
}

// RaftBatch carries owned Raft messages with exact endpoints and a delivery ID.
type RaftBatch struct {
	From, To   Peer
	DeliveryID uint64
	Messages   []raftpb.Message
}
