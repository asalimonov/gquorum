//go:build functional

package gquorum

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/topology"
	"github.com/asalimonov/gquorum/transport/loadgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// One local participant exercises the real service/controller/managed ownership
// boundary. Multi-node quorum isolation belongs to the Docker suite.
func TestIndependentGroupConvergesWhileFactoryRemainsOwned(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		name := "resume original operation"
		if shutdown {
			name = "shutdown joins late factory"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			o := authorityNodeOptions(t, root, "isolation")
			o.CallbackTimeout = 30 * time.Second
			loopback := &loopbackTransport{}
			entered, release, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var starts atomic.Int32
			var releaseOnce, enteredOnce, closeOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			o.Loads = LoadOptions{Transport: loopback,
				Storage: LoadStorageOptions{WAL: &WALOptions{RootDir: filepath.Join(root, "loads")}},
				Factory: func(_ context.Context, spec load.ReplicaSpec) (load.Application, error) {
					if spec.Identity.GroupID == "blocked" {
						starts.Add(1)
						enteredOnce.Do(func() { close(entered) })
						<-release // Deliberately ignores cancellation; Node must retain ownership.
					}
					return load.Application{StateMachine: ensureMachine{}, Close: func(context.Context) error {
						if spec.Identity.GroupID == "blocked" {
							closeOnce.Do(func() { close(closed) })
						}
						return nil
					}}, nil
				}}
			n, err := NewNode(o)
			if err != nil {
				t.Fatal(err)
			}
			loopback.node = n
			t.Cleanup(func() { unblock(); stopAuthorityNode(t, n) })
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			if err := n.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := n.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			c := n.Cluster()
			first, err := c.CreateGroup(ctx, singleReplicaSpec("blocked", nil), MutationOptions{
				ExpectedVersion: 1, Actor: "app", Approve: true})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("factory was not admitted", ctx.Err())
			}
			second, err := c.CreateGroup(ctx, singleReplicaSpec("healthy", nil), MutationOptions{
				ExpectedVersion: first.Document.Version, Actor: "app", Approve: true})
			if err != nil {
				t.Fatal("unrelated approval waited on the first factory", err)
			}
			status, err := c.WaitOperation(ctx, second.OperationID)
			if err != nil || status.State != topology.OperationSucceeded {
				t.Fatal(status, err)
			}
			local, err := c.Status(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range local.Groups {
				if row.GroupID != topology.MetaGroupID && row.GroupID != "healthy" {
					continue
				}
				observed := row.Observation
				if observed == nil || !observed.Found || observed.Reporter != n.Identity() ||
					observed.SourceVersion != row.ConvergedVersion || observed.ReporterReplicaID == 0 ||
					observed.ReceivedAt.IsZero() || observed.LeaderID != observed.ReporterReplicaID {
					t.Fatal("real status producer lost exact observation provenance", row)
				}
			}
			page, err := c.ReadProgress(ctx, topology.ProgressQuery{})
			if err != nil || page.TargetVersion != second.Document.Version || page.FullyConverged.Version != 1 {
				t.Fatal("global completion hid pending work", page, err)
			}
			for _, row := range page.Groups {
				switch row.GroupID {
				case "blocked":
					if row.Converged != nil || row.OperationID != first.OperationID {
						t.Fatal("new approval replaced unrelated owner", row)
					}
				case "healthy":
					if row.ConvergedVersion != second.Document.Version || row.Converged == nil {
						t.Fatal(row)
					}
				}
			}
			if _, found := n.LoadGroup("blocked"); found || starts.Load() != 1 {
				t.Fatal("unready factory published or acquired twice", found, starts.Load())
			}
			wait, stopWait := context.WithTimeout(ctx, 30*time.Millisecond)
			_, err = c.WaitOperation(wait, first.OperationID)
			stopWait()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("waiter did not cancel independently", err)
			}
			if shutdown {
				n.RequestStop()
				join, stopJoin := context.WithTimeout(ctx, 30*time.Millisecond)
				err = n.Stop(join)
				stopJoin()
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("shutdown released an owned factory", err)
				}
				assertNodeStorageBusy(t, o)
				select {
				case <-closed:
					t.Fatal("application closed before factory returned")
				default:
				}
				unblock()
				if err := n.Stop(ctx); err != nil {
					t.Fatal(err)
				}
				select {
				case <-closed:
				default:
					t.Fatal("late factory application was not released by joined Stop")
				}
				assertNodeStorageReleased(t, o)
			} else {
				unblock()
				status, err := c.WaitOperation(ctx, first.OperationID)
				if err != nil || status.State != topology.OperationSucceeded {
					t.Fatal(status, err)
				}
				page, err := c.ReadProgress(ctx, topology.ProgressQuery{GroupID: "blocked"})
				if err != nil || len(page.Groups) != 1 || page.Groups[0].ConvergedVersion != first.Document.Version ||
					page.FullyConverged.Version != second.Document.Version {
					t.Fatal("resumed scope used a newer document as its source", page, err)
				}
			}
			if starts.Load() != 1 {
				t.Fatal("retry reacquired the application", starts.Load())
			}
		})
	}
}

type readmissionFencePeer struct {
	retainedProofPeer
	join atomic.Pointer[metapb.Reply]
}

func (p *readmissionFencePeer) Call(ctx context.Context, method string, header *metapb.Header, data []byte) *metapb.Reply {
	if method == "Join" {
		return p.join.Load()
	}
	if method == "JoinReady" {
		return metatransport.Reply(fault.ErrAuthorityPending)
	}
	return p.retainedProofPeer.Call(ctx, method, header, data)
}

func TestNodeReadmissionFenceJoinsRetirementBeforeAcknowledgement(t *testing.T) {
	t.Parallel()
	for _, group := range []string{topology.MetaGroupID, "work"} {
		t.Run(group, func(t *testing.T) {
			t.Parallel()
			testNodeReadmissionFence(t, group)
		})
	}
}

func testNodeReadmissionFence(t *testing.T, group string) {
	t.Helper()
	root := t.TempDir()
	o := authorityNodeOptions(t, root, "returning")
	previousID := o.Identity
	previousID.MemberID = 1
	authority, err := authoritystore.Open(o.authorityDBPath(), previousID, authoritystore.FreshEvidence)
	if err != nil {
		t.Fatal(err)
	}
	if err := authority.Close(); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	peer := &readmissionFencePeer{}
	peer.join.Store(metatransport.Reply(fault.ErrAuthorityPending))
	courier := topology.Identity{ClusterID: "cluster", NodeName: "courier", MemberID: 99, InstanceID: "current"}
	remote, err := metatransport.New(metatransport.Options{Header: &metapb.Header{ClusterId: courier.ClusterID, NodeName: courier.NodeName, MemberId: courier.MemberID, InstanceId: courier.InstanceID}, Listener: listener,
		AllowPlaintext: true, Credential: o.Meta.Network.ClusterCredential, Handler: peer,
		Resolve:     func(context.Context, uint64) (string, error) { return "", fault.ErrUnavailable },
		SendTimeout: time.Second, SnapshotTimeout: time.Second, RetryInitial: time.Millisecond, RetryMax: time.Second, MaxSnapshotBytes: 1 << 20})
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	if err = remote.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = remote.Stop(context.Background()) })
	o.Node = topology.Node{Name: "n1", Type: "worker"}
	o.Bootstrap = BootstrapOptions{Join: true, JoinSeeds: []string{listener.Addr().String()}}
	o.Meta.Timing.TickInterval = 10 * time.Millisecond
	entered, release := make(chan context.Context, 1), make(chan struct{})
	var releaseOnce sync.Once
	var calls atomic.Int32
	o.Loads = LoadOptions{Transport: unusedAuthorityTransport{}, Storage: LoadStorageOptions{WAL: &WALOptions{RootDir: filepath.Join(root, "loads")}},
		Factory: func(context.Context, load.ReplicaSpec) (load.Application, error) {
			return load.Application{}, fault.ErrReplicaNotReady
		},
		Retire: func(ctx context.Context, req load.RetirementRequest) (load.RetirementResult, error) {
			if calls.Add(1) == 1 {
				entered <- ctx
				<-release // deliberately retain ownership after cancellation
			}
			return load.RetirementResult{Grant: req.Grant, Identity: req.Identity, Reason: req.Reason, AdmissionID: req.AdmissionID, PreviousMemberID: req.PreviousMemberID, GrantedMemberID: req.GrantedMemberID, Verified: true}, nil
		}}
	public, err := NewNode(o)
	if err != nil {
		t.Fatal(err)
	}
	n := public.(*node)
	t.Cleanup(func() { stopAuthorityNode(t, n) })
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	previous := topology.Document{Version: 1, ClusterID: "cluster", Origin: topology.OriginFounding, Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3}, Nodes: []topology.Node{{Name: "n1", Type: "worker", State: topology.NodeActive}}, Groups: []topology.Group{
		{ID: topology.MetaGroupID, NextReplicaID: 2, Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter}}},
		{ID: "work", NextReplicaID: 2, Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter}}},
	}}
	response := func(member, revision uint64) []topology.AuthorityGrant {
		t.Helper()
		granted := previous.Clone()
		granted.Version = member
		for i := range granted.Groups {
			granted.Groups[i].NextReplicaID = member + 1
			granted.Groups[i].Replicas[0].ReplicaID = member
		}
		a := metaengine.AdmissionRecord{NodeName: previousID.NodeName, InstanceID: previousID.InstanceID, MemberID: member, OperationID: topology.MakeOperationID(member, 1), State: metaengine.AdmissionPending}
		var grants []topology.AuthorityGrant
		for _, name := range []string{topology.MetaGroupID, "work"} {
			grants = append(grants, topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "cluster", GroupID: name, Revision: revision}, Purpose: topology.AuthorityReadmit, AdmissionID: metaengine.ReadmissionID(a), Resources: []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: name, NodeName: "n1"}}, Targets: []topology.AuthorityTarget{{NodeName: "n1", MemberID: 1, ReplicaID: 1}}})
		}
		data, err := json.Marshal(map[string]any{"rules": topology.Rules{SchemaVersion: 1, Limits: n.metaOpts.Limits}, "member_id": member, "previous_member_id": 1, "readmitted": true, "admission": a, "Granted": granted, "Members": []map[string]any{{"Name": "n1", "MemberID": member, "Address": listener.Addr().String()}}, "previous_groups": previous.Groups[1:], "grants": grants, "leader_address": listener.Addr().String()})
		if err != nil {
			t.Fatal(err)
		}
		peer.join.Store(&metapb.Reply{Data: data})
		return grants
	}
	grants := response(2, 100)
	if err := n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var callback context.Context
	select {
	case callback = <-entered:
	case <-ctx.Done():
		t.Fatal("readmission did not enter retirement", n.Status(), ctx.Err())
	}
	peer.join.Store(metatransport.Reply(fault.ErrAuthorityPending))
	fence := grants[0].Clone()
	if group == "work" {
		fence = grants[1].Clone()
	}
	fence.Key.Revision, fence.Purpose, fence.AdmissionID = 101, topology.AuthorityFence, ""
	peer.proof.Store(&metaengine.AuthorityProof{Grant: &fence, Courier: courier})
	request := load.FenceRequest{From: courier, To: previousID, Grant: fence, Resource: fence.Resources[0]}
	// The grant-scoped probe returns the retained owner even though no metadata
	// member exists. It must not change the ordinary Raft identity.
	data, _ := json.Marshal(map[string]any{"grant": fence, "target": fence.Targets[0]})
	reply, err := remote.CallAddress(ctx, o.Meta.Network.AdvertiseAddress, "IdentityPing", data)
	if err != nil || metatransport.ReplyError(reply) != nil {
		t.Fatal(reply, err)
	}
	var found topology.Identity
	if json.Unmarshal(reply.Data, &found) != nil || found != previousID || n.meta.Identity().MemberID != 0 {
		t.Fatal("control probe changed or lost metadata identity", found, n.meta.Identity())
	}
	bad := request
	bad.From.InstanceID = "old-courier"
	if _, err := n.installReadmissionFence(ctx, bad); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("stale courier installed a floor", err)
	}
	data, _ = json.Marshal(request)
	wait, stopWaiting := context.WithCancel(ctx)
	defer stopWaiting()
	called := make(chan error, 1)
	go func() {
		reply, err := remote.CallAddress(wait, o.Meta.Network.AdvertiseAddress, "InstallFence", data)
		if err == nil {
			err = metatransport.ReplyError(reply)
		}
		called <- err
	}()
	select {
	case <-callback.Done():
	case <-ctx.Done():
		t.Fatal("newer fence did not cancel retirement")
	}
	floor, err := n.localAuthority().Current(ctx, group)
	if err != nil || !floor.Found || floor.Record.Complete || floor.Record.Grant.Key != fence.Key {
		t.Fatal("canceled callback before persisting pending boundary", floor.Record, floor.Found, err)
	}
	// Cancel only after the durable boundary is observable. A short wall-clock
	// timeout could expire before authorization dispatched the fence at all.
	stopWaiting()
	select {
	case err := <-called:
		if err == nil {
			t.Fatal("fence acknowledged while retirement callback remained owned")
		}
	case <-ctx.Done():
		t.Fatal("canceled courier did not return", ctx.Err())
	}
	n.mu.RLock()
	work := n.readmissionFenceWork
	n.mu.RUnlock()
	if work == nil || !workPending(work.done) {
		t.Fatal("courier cancellation released unfinished fence ownership")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-work.done:
	case <-ctx.Done():
		t.Fatal("retirement return did not release fence", ctx.Err())
	}
	if work.err != nil || work.evidence.Validate(fence) != nil {
		t.Fatal(work.evidence, work.err)
	}
	result, err := n.installAuthorityFence(ctx, request)
	if err != nil || result.Evidence.Validate(fence) != nil {
		t.Fatal("completed fence could not be acknowledged again", result, err)
	}
	if running, err := n.localAuthority().Running(ctx, group); err != nil || len(running) != 0 {
		t.Fatal("uncertain older retirement survived completed fence", running, err)
	}
	old := grants[0]
	if group == "work" {
		old = grants[1]
	}
	if found, err := n.localAuthority().Receipt(ctx, old, old.Targets[0]); err != nil || found.Found {
		t.Fatal("fencing fabricated retirement completion", found.Found, err)
	}
	if n.readmissionIdentity().MemberID != 1 || calls.Load() != 1 {
		t.Fatal("revoked retirement rebound or repeated", n.readmissionIdentity(), calls.Load())
	}
	// A newer authenticated readmission must be able to replace the canceled
	// reservation and reconcile the now-fenced owner's exact resources.
	newGrants := response(3, 120)
	for n.meta.Identity().MemberID != 3 {
		select {
		case <-ctx.Done():
			t.Fatal("successor readmission remained blocked", n.Status(), ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if calls.Load() != 2 {
		t.Fatal("successor retirement callbacks", calls.Load())
	}
	for _, grant := range newGrants {
		if receipt, err := n.localAuthority().Receipt(ctx, grant, grant.Targets[0]); err != nil || !receipt.Found || receipt.Receipt.Validate(grant) != nil {
			t.Fatal(receipt.Receipt, receipt.Found, err)
		}
	}
}

func TestNodeSnapshotBindingFollowsManagedLifetime(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	options := authorityNodeOptions(t, root, "snapshot-binding")
	adapter, err := loadgrpc.New(loadgrpc.Options{
		Resolve: func(context.Context, topology.Identity) (string, error) {
			return "passthrough:///unused-local-test", nil
		},
		Authenticate: func(context.Context, topology.Identity) error { return nil },
		DialOptions:  []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}})
	if err != nil {
		t.Fatal(err)
	}
	options.Loads = LoadOptions{Transport: adapter,
		Storage:          LoadStorageOptions{WAL: &WALOptions{RootDir: filepath.Join(root, "loads")}},
		SnapshotTransfer: snapshot.Options{MaxSnapshotBytes: 8 << 20, TotalTimeout: time.Second, ProgressTimeout: 100 * time.Millisecond},
		Factory: func(context.Context, load.ReplicaSpec) (load.Application, error) {
			return load.Application{StateMachine: ensureMachine{}}, nil
		}}
	participant, err := NewNode(options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopAuthorityNode(t, participant)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := adapter.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	view := participant.LoadSnapshots()
	if err = adapter.Bind(participant); err != nil {
		t.Fatal(err)
	}
	if _, err = view.Begin(t.Context(), snapshot.BeginRequest{}); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal(err)
	}
	if err = participant.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err = participant.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	owned := participant.(*node)
	coordinator, err := owned.snapshotCoordinator()
	if err != nil || coordinator == nil {
		t.Fatal(coordinator, err)
	}
	if participant.LoadSnapshots() != view || view.Options().MaxSnapshotBytes != 8<<20 {
		t.Fatal("snapshot binding/options changed")
	}
	if _, err = view.Begin(ctx, snapshot.BeginRequest{}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("managed snapshot ingress not attached", err)
	}
	if err = participant.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = view.Begin(ctx, snapshot.BeginRequest{}); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("stopped Node admitted snapshot", err)
	}
	if page := view.List(snapshot.Query{}); page.Incoming != 0 || page.Outgoing != 0 {
		t.Fatal(page)
	}
}
