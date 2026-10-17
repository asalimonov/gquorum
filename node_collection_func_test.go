//go:build functional

package gquorum

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/walstore"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

type nodeCollectionMessage struct {
	Kind             string                           `json:"kind"`
	ReporterMemberID uint64                           `json:"reporter_member_id"`
	Receipt          authoritystore.CollectionReceipt `json:"receipt"`
	Identity         storage.Identity                 `json:"identity"`
	MemberID         uint64                           `json:"member_id"`
	RetirementKey    topology.AuthorityKey            `json:"retirement_key"`
	NotBefore        *time.Time                       `json:"not_before"`
	Blocked          *topology.BlockedReason          `json:"blocked"`
}

type nodeCollectionPeer struct {
	messages chan nodeCollectionMessage
	accept   atomic.Bool
}

func (*nodeCollectionPeer) Step(context.Context, *metapb.Header, raftpb.Message) error {
	return fault.ErrUnsupported
}

// This authenticated protocol double tests Node composition. Replicated receipt
// acceptance and historical courier authorization belong to metaengine tests.
func (p *nodeCollectionPeer) Call(ctx context.Context, method string, header *metapb.Header, data []byte) *metapb.Reply {
	if method != "MetaControl" {
		return metatransport.Reply(fault.ErrUnavailable)
	}
	var message nodeCollectionMessage
	if err := json.Unmarshal(data, &message); err != nil {
		return metatransport.Reply(err)
	}
	if header.ClusterId != "cluster" || header.NodeName != "n1" || header.InstanceId == "" || message.ReporterMemberID != 1 {
		return metatransport.Reply(fault.ErrIdentityMismatch)
	}
	select {
	case p.messages <- message:
	case <-ctx.Done():
		return metatransport.Reply(ctx.Err())
	}
	if !p.accept.Load() {
		<-ctx.Done() // The Node must cancel this wait without losing its receipt.
		return metatransport.Reply(ctx.Err())
	}
	if message.Kind == "report_cleanup_status" {
		return &metapb.Reply{}
	}
	encoded, err := json.Marshal(authoritystore.CollectionResult{Receipt: message.Receipt, AppliedIndex: 100, Settled: true})
	if err != nil {
		return metatransport.Reply(err)
	}
	return &metapb.Reply{Data: encoded}
}

func startNodeCollectionPeer(t *testing.T, peer *nodeCollectionPeer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	remote, err := metatransport.New(metatransport.Options{Header: &metapb.Header{ClusterId: "cluster", NodeName: "proof"}, Listener: listener,
		AllowPlaintext: true, Credential: []byte("test"), Handler: peer,
		Resolve:     func(context.Context, uint64) (string, error) { return "", fault.ErrUnavailable },
		SendTimeout: time.Second, SnapshotTimeout: time.Second, RetryInitial: time.Millisecond, RetryMax: time.Second, MaxSnapshotBytes: 1 << 20})
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	if err = remote.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := remote.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	return listener.Addr().String()
}

func seedNodeCollection(t *testing.T, root, mode string) authoritystore.CollectionRecord {
	t.Helper()
	o := authorityNodeOptions(t, root, "seed")
	public, err := NewNode(o)
	if err != nil {
		t.Fatal(err)
	}
	n := public.(*node)
	t.Cleanup(func() { stopAuthorityNode(t, n) })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err = n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = n.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	stop := metaHostGrant(n.Identity(), 100, topology.StepStopReplica)
	if _, err = n.installAuthorityFence(ctx, load.FenceRequest{From: n.Identity(), To: n.Identity(), Grant: stop, Resource: stop.Resources[0]}); err != nil {
		t.Fatal(err)
	}
	if _, err = n.applyMetaHostAction(ctx, metaHostRequest(n.Identity(), stop)); err != nil {
		t.Fatal(err)
	}
	store, err := walstore.Open(walstore.Options{RootDir: filepath.Join(root, "loads"), NodeName: "n1", ClusterID: "cluster", Logger: o.Logger})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	id := storage.Identity{ClusterID: "cluster", NodeName: "n1", GroupID: "work", ReplicaID: 7}
	if err = store.BindReplica(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err = store.Ensure(ctx, id.GroupID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Quarantine(ctx, id.GroupID, id.ReplicaID); err != nil {
		t.Fatal(err)
	}
	retired, err := store.Retired(ctx)
	if err != nil || len(retired) != 1 {
		t.Fatal(retired, err)
	}
	grant := metaHostGrant(n.Identity(), 101, topology.StepCleanupReplicaState)
	grant.Key.GroupID, grant.Action.GroupID, grant.Resources[0].GroupID = id.GroupID, id.GroupID, id.GroupID
	grant.Action.ReplicaID, grant.Targets[0].ReplicaID = id.ReplicaID, id.ReplicaID
	journal := n.localAuthority()
	floor, err := journal.Fence(ctx, grant)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = journal.CompleteFence(ctx, grant.Key, floor.Record.Sequence); err != nil {
		t.Fatal(err)
	}
	if _, err = journal.BeginEffect(ctx, grant, grant.Targets[0]); err != nil {
		t.Fatal(err)
	}
	receipt := topology.EffectReceipt{Key: grant.Key, Resource: grant.Resources[0], Target: grant.Targets[0],
		Action: grant.Action, Outcome: topology.EffectApplied, LocalSequence: floor.Record.Sequence}
	if err = journal.FinishEffect(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	row := authoritystore.CollectionRecord{Retired: retired[0], MemberID: 1, Grant: grant, Retirement: receipt, State: "prepared"}
	if mode != "status" {
		row, err = journal.PrepareCollection(ctx, row)
		if err != nil {
			t.Fatal(err)
		}
	}
	if mode == "deleted" || mode == "collected" || mode == "forgotten" {
		if err = store.Collect(ctx, row.Retired); err != nil {
			t.Fatal(err)
		}
	}
	if mode == "collected" || mode == "forgotten" {
		if _, err = journal.CompleteCollection(ctx, id, row.Sequence, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	if mode == "forgotten" {
		if err = store.ForgetCollection(ctx, row.Retired); err != nil {
			t.Fatal(err)
		}
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	stopAuthorityNode(t, n)
	return row
}

type nodeCollectionHooks struct{ lifecycle.Hooks }

func (nodeCollectionHooks) Retention(context.Context, lifecycle.RetentionInput) (lifecycle.RetentionDecision, error) {
	return lifecycle.RetentionDecision{NotBefore: time.Now().Add(time.Hour), Reason: "test retention"}, nil
}

func collectionNodeOptions(t *testing.T, root, address, instance string) NodeOptions {
	t.Helper()
	o := authorityNodeOptions(t, root, instance)
	o.Node = topology.Node{Name: "n1", Type: "worker"}
	o.Bootstrap = BootstrapOptions{Join: true, JoinSeeds: []string{address}}
	hooks, err := lifecycle.NewDefaultHooks(lifecycle.DefaultOptions{})
	if err != nil {
		t.Fatal(err)
	}
	o.Lifecycle.Hooks = nodeCollectionHooks{hooks}
	o.Loads = LoadOptions{Transport: unusedAuthorityTransport{}, Storage: LoadStorageOptions{WAL: &WALOptions{RootDir: filepath.Join(root, "loads")}},
		RetirementInterval: 20 * time.Millisecond, EffectTimeout: 10 * time.Second,
		Factory: func(context.Context, load.ReplicaSpec) (load.Application, error) {
			t.Error("control-only startup called application factory")
			return load.Application{}, fault.ErrStopped
		}}
	return o
}

func startCollectionNode(t *testing.T, root, address, instance string) *node {
	t.Helper()
	return startConfiguredCollectionNode(t, collectionNodeOptions(t, root, address, instance))
}

func startConfiguredCollectionNode(t *testing.T, o NodeOptions) *node {
	t.Helper()
	public, err := NewNode(o)
	if err != nil {
		t.Fatal(err)
	}
	n := public.(*node)
	t.Cleanup(func() { stopAuthorityNode(t, n) })
	if err = n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err = n.meta.ReadTopology(t.Context()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("control-only node admitted ordinary reads", err)
	}
	if _, err = n.StartLoadGroup(t.Context(), load.ReplicaRef{GroupID: "work", ReplicaID: 7}); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("control-only node admitted a replica", err)
	}
	return n
}

type blockedCollectionStore struct {
	*walstore.Store
	entered, release chan struct{}
	once             sync.Once
}

func (s *blockedCollectionStore) Collect(ctx context.Context, _ storage.RetiredReplica) error {
	s.once.Do(func() { close(s.entered) })
	<-s.release // Model a storage callback that has not observed cancellation yet.
	return ctx.Err()
}

func TestNodeCollectionShutdownJoinsCallbackBeforeClosingJournal(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	row := seedNodeCollection(t, root, "prepared")
	peer := &nodeCollectionPeer{messages: make(chan nodeCollectionMessage, 16)}
	peer.accept.Store(true)
	o := collectionNodeOptions(t, root, startNodeCollectionPeer(t, peer), "blocked-store")
	store, err := walstore.Open(walstore.Options{RootDir: filepath.Join(root, "loads"), NodeName: "n1", ClusterID: "cluster", Logger: o.Logger})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	blocked := &blockedCollectionStore{Store: store, entered: make(chan struct{}), release: make(chan struct{})}
	o.Loads.Storage = LoadStorageOptions{Store: blocked, Retirement: blocked}
	n := startConfiguredCollectionNode(t, o)
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(blocked.release) }) })
	select {
	case <-blocked.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("collection did not enter borrowed store")
	}
	journal := n.localAuthority()
	n.RequestStop()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := n.Stop(ctx); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal("shutdown abandoned collection callback", err)
	}
	current, err := journal.Collection(t.Context(), row.Retired.Identity)
	if err != nil || !current.Found || current.Record.State != "prepared" {
		t.Fatal("journal closed or lost pending work before callback joined", current, err)
	}
	release.Do(func() { close(blocked.release) })
	stopAuthorityNode(t, n)
	if _, err := journal.Collection(t.Context(), row.Retired.Identity); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("joined shutdown retained journal", err)
	}
	if rows, err := store.Retired(t.Context()); err != nil || len(rows) != 1 || rows[0] != row.Retired {
		t.Fatal("Node closed borrowed storage or lost pending data", rows, err)
	}
}

func nextNodeCollection(t *testing.T, peer *nodeCollectionPeer) nodeCollectionMessage {
	t.Helper()
	select {
	case message := <-peer.messages:
		return message
	case <-time.After(5 * time.Second):
		t.Fatal("Node collection worker did not report")
		return nodeCollectionMessage{}
	}
}

func TestNodeControlOnlyCollectionResumesAndRetainsUnsentReceipt(t *testing.T) {
	t.Parallel()
	// Reopen both stores at each handoff: before deletion, deletion without a
	// journal receipt, completed receipt, and marker release before row removal.
	for _, mode := range []string{"prepared", "deleted", "collected", "forgotten"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			row := seedNodeCollection(t, root, mode)
			peer := &nodeCollectionPeer{messages: make(chan nodeCollectionMessage, 16)}
			address := startNodeCollectionPeer(t, peer)
			n := startCollectionNode(t, root, address, "first-report")
			first := nextNodeCollection(t, peer)
			if first.Kind != "report_collection" || first.Receipt.Identity != row.Retired.Identity || first.Receipt.Token != row.Retired.Token {
				t.Fatal("wrong durable receipt", first)
			}
			retained, err := n.localAuthority().Collection(t.Context(), row.Retired.Identity)
			if err != nil || !retained.Found || retained.Record.State != "collected" ||
				retained.Record.Sequence != row.Sequence || first.Receipt.LocalSequence != row.Sequence {
				t.Fatal("restart lost the original collection proof", retained, first, err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if err := n.Stop(ctx); err != nil {
				t.Fatal("report wait blocked shutdown", err)
			}
			peer.accept.Store(true)
			restarted := startCollectionNode(t, root, address, "retry-report")
			second := nextNodeCollection(t, peer)
			if !first.Receipt.Equal(second.Receipt) {
				t.Fatal("restart changed the accepted collection", first, second)
			}
			deadline := time.After(5 * time.Second)
			for {
				got, err := restarted.localAuthority().Collection(t.Context(), row.Retired.Identity)
				if err != nil {
					t.Fatal(err)
				}
				if !got.Found {
					break
				}
				select {
				case <-deadline:
					t.Fatal("accepted report did not release journal")
				case <-time.After(time.Millisecond):
				}
			}
			if err := restarted.ownedWAL.Collect(t.Context(), row.Retired); !errors.Is(err, fs.ErrNotExist) {
				t.Fatal("accepted report retained collection marker", err)
			}
		})
	}
}

func TestNodeControlOnlyCollectionReportsRetention(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	row := seedNodeCollection(t, root, "status")
	peer := &nodeCollectionPeer{messages: make(chan nodeCollectionMessage, 16)}
	peer.accept.Store(true)
	n := startCollectionNode(t, root, startNodeCollectionPeer(t, peer), "retention-report")
	status := nextNodeCollection(t, peer)
	if status.Kind != "report_cleanup_status" || status.Identity != row.Retired.Identity || status.MemberID != 1 ||
		status.RetirementKey != row.Grant.Key || status.NotBefore == nil || status.Blocked == nil || status.Blocked.Code != "retention" {
		t.Fatal("retention lost exact proof binding", status)
	}
	rows, err := n.ownedWAL.Retired(t.Context())
	if err != nil || len(rows) != 1 || rows[0] != row.Retired {
		t.Fatal("retention removed data", rows, err)
	}
	journal, err := n.localAuthority().Collection(t.Context(), row.Retired.Identity)
	if err != nil || journal.Found {
		t.Fatal("retention accepted collection prematurely", journal, err)
	}
}
