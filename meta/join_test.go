package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/topology"
)

func joiningGroup(t *testing.T, base *Group, name, kind, instance string, seeds []string, modify func(*Options)) *Group {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	opts := Options{Identity: Identity{NodeName: name, ClusterID: base.currentIdentity().ClusterID, InstanceID: instance}, Node: topology.Node{Name: name, Type: topology.NodeType(kind)}, DBPath: filepath.Join(t.TempDir(), "meta.bolt"), Join: true, JoinSeeds: seeds, Limits: base.opts.Limits, Placement: base.opts.Placement, Timing: base.opts.Timing, SessionTTL: base.opts.SessionTTL, Network: NetworkOptions{Listener: listener, AdvertiseAddress: listener.Addr().String(), AllowPlaintext: true, ClusterCredential: base.opts.Network.ClusterCredential}}
	if modify != nil {
		modify(&opts)
	}
	group, err := New(opts)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := group.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	if err = group.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return group
}
func approvedNode(t *testing.T, leader *Group, name string, kind topology.NodeType) topology.Document {
	t.Helper()
	doc := leader.state().Latest()
	doc.Origin = topology.OriginOperator
	doc.Nodes = append(doc.Nodes, topology.Node{Name: name, Type: kind, State: topology.NodeActive})
	member, err := doc.AllocateMemberID()
	if err != nil {
		t.Fatal(err)
	}
	if err = doc.SetMemberID(name, member, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	return doc
}

func TestMetaJoinRequiresApprovalAndPreparedExactRuntimeBeforeLearner(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test")
	}
	groups := publicFoundingClusterWith(t, 2, func(o *Options) { o.Limits.MaxNodes = 3 })
	leader := readyPublic(t, groups)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	doc := approvedNode(t, leader, "n3", "coordinator")
	staged, err := leader.Stage(ctx, doc, 1)
	if err != nil {
		t.Fatal(err)
	}
	header := &metapb.Header{ClusterId: "cluster", NodeName: "n3"}
	request, _ := json.Marshal(joinRequest{Identity: Identity{NodeName: "n3", ClusterID: "cluster", InstanceID: "join-three"}, Node: topology.Node{Name: "n3", Type: "coordinator"}, Address: "127.0.0.1:1"})
	if err = metatransport.ReplyError(leader.handleJoin(ctx, header, request)); !errors.Is(err, fault.ErrExecutionAuthorization) {
		t.Fatal("staged node bypassed Apply", err)
	}
	id, err := leader.Apply(ctx, staged.Version)
	if err != nil {
		t.Fatal(err)
	}
	reply := leader.handleJoin(ctx, header, request)
	if err = metatransport.ReplyError(reply); err != nil {
		t.Fatal(err)
	}
	var grant joinResponse
	if err = json.Unmarshal(reply.Data, &grant); err != nil {
		t.Fatal(err)
	}
	if len(leader.parked.snapshot()) != 0 {
		t.Fatal("accepted admission retained park")
	}
	if grant.MemberID != 3 || grant.Admission.State != metaengine.AdmissionPending {
		t.Fatal("invalid admission", grant)
	}
	waitPublic(t, func() bool {
		status, ok := leader.state().Execution(id)
		return ok && status.State == topology.ExecutionWaitingForReplica
	})
	if containsMember(leader.engine.Load().Status().ConfState, 3) {
		t.Fatal("unprepared runtime entered meta")
	}
	malformed, _ := json.Marshal(readyRequest{NodeName: "n3", InstanceID: "wrong-instance", MemberID: 3, ExecutionID: grant.Admission.ExecutionID, Address: "127.0.0.1:2"})
	if err = metatransport.ReplyError(leader.handleReady(ctx, &metapb.Header{ClusterId: "cluster", NodeName: "n3", MemberId: 3}, malformed)); !errors.Is(err, fault.ErrExecutionAuthorization) {
		t.Fatal("wrong prepared identity was acknowledged", err)
	}
	if address, _ := leader.resolveNode(ctx, "n3"); address != "127.0.0.1:1" {
		t.Fatal("refused ready replaced route", address)
	}
	var follower *Group
	for _, g := range groups {
		if g != leader {
			follower = g
		}
	}
	// Only a follower seed is supplied; the meta protocol supplies a one-hop
	// address redirect before this blank joiner knows any member ID/name map.
	joined := joiningGroup(t, leader, "n3", "coordinator", "join-three", []string{follower.opts.Network.AdvertiseAddress}, nil)
	if err = joined.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	active := joined.state().Active()
	if joined.currentIdentity().MemberID != 3 || active.Node("n3").Type != "coordinator" {
		t.Fatal("lost caller node type or grant")
	}
	status, err := joined.WaitExecution(ctx, id)
	if err != nil || status.State != topology.ExecutionSucceeded {
		t.Fatal(status, err)
	}
	if len(joined.engine.Load().Status().ConfState.Voters) != 3 || !joined.OwnSessionLive() {
		t.Fatal("join readiness preceded voter membership and lease")
	}
	leader.acceptAddressHints(&metapb.Header{NodeName: follower.currentIdentity().NodeName, MemberId: follower.currentIdentity().MemberID, AdvertiseAddress: follower.opts.Network.AdvertiseAddress, Peers: []*metapb.PeerAddress{{NodeName: "n3", MemberId: 3, Address: "127.0.0.1:1"}}})
	if address, _ := leader.resolveNode(ctx, "n3"); address != joined.opts.Network.AdvertiseAddress {
		t.Fatal("stale follower hints replaced accepted join", address)
	}
	// Address hints propagate independently of topology/WAL documents.
	for _, g := range groups {
		waitPublic(t, func() bool {
			address, err := g.resolveMember(ctx, 3)
			return err == nil && address == joined.opts.Network.AdvertiseAddress
		})
	}
}

func TestMetaRetainedDiskJoinIsReadOnlyAndCopiedLiveIdentityIsFenced(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test")
	}
	groups := publicFoundingCluster(t, 3)
	leader := readyPublic(t, groups)
	var old *Group
	for _, g := range groups {
		if g != leader {
			old = g
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := old.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	copied, err := os.ReadFile(old.opts.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	opts := restartOptions(t, old)
	opts.Founders = nil
	opts.Join = true
	opts.JoinSeeds = []string{leader.opts.Network.AdvertiseAddress}
	opts.Identity.InstanceID = "retained-restart"
	restarted, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { restarted.Stop(context.Background()) })
	if err = restarted.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = restarted.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if restarted.currentIdentity().MemberID != old.currentIdentity().MemberID || leader.state().Latest().Version != 1 {
		t.Fatal("ordinary restart churned topology")
	}
	copiedPath := filepath.Join(t.TempDir(), "copied.bolt")
	if err = os.WriteFile(copiedPath, copied, 0600); err != nil {
		t.Fatal(err)
	}
	clone := joiningGroup(t, leader, old.currentIdentity().NodeName, "worker", "copied-process", []string{leader.opts.Network.AdvertiseAddress}, func(o *Options) { o.DBPath = copiedPath })
	if err := clone.WaitReady(ctx); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("copied live identity must fail startup", err)
	}
	if clone.engine.Load().Running() {
		t.Fatal("copied voter started before name fencing")
	}
	if leader.state().Latest().Version != 1 {
		t.Fatal("fenced clone changed topology")
	}
	if address, err := leader.resolveNode(ctx, old.currentIdentity().NodeName); err != nil || address != restarted.opts.Network.AdvertiseAddress {
		t.Fatal("refused clone overwrote the live holder route", address, err)
	}
}

func TestMetaDataLostReadmissionBlocksWithoutHookAndRetriesSameGrant(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test")
	}
	groups := publicFoundingCluster(t, 3)
	leader := readyPublic(t, groups)
	var old *Group
	for _, g := range groups {
		if g != leader {
			old = g
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := old.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	name := old.currentIdentity().NodeName
	blocked := joiningGroup(t, leader, name, "worker", "lost-disk", []string{leader.opts.Network.AdvertiseAddress}, nil)
	err := blocked.WaitReady(ctx)
	var required *ReadmissionRequiredError
	if !errors.As(err, &required) || required.Readmission.Previous.MemberID != old.currentIdentity().MemberID || required.Readmission.Granted.MemberID == required.Readmission.Previous.MemberID {
		t.Fatal("missing exact readmission requirement", err, required)
	}
	if _, err = os.Stat(blocked.opts.DBPath); !os.IsNotExist(err) {
		t.Fatal("missing hook wrote a new meta store", err)
	}
	blocked.mu.RLock()
	lifecycle := blocked.lifecycle
	blocked.mu.RUnlock()
	if lifecycle != "blocked" {
		t.Fatal("readmission failure did not remain blocked", lifecycle)
	}
	if err = blocked.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	var callback Readmission
	calls := 0
	retry := joiningGroup(t, leader, name, "worker", "lost-disk", []string{leader.opts.Network.AdvertiseAddress}, func(o *Options) {
		o.DBPath = blocked.opts.DBPath
		o.OnReadmission = func(ctx context.Context, r Readmission) error {
			callback = r
			calls++
			if _, err := os.Stat(r.MetaDBPath); !os.IsNotExist(err) {
				return errors.New("readmission opened storage before callback")
			}
			return nil
		}
	})
	if err = retry.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || callback.AdmissionID != required.Readmission.AdmissionID || callback.Granted != required.Readmission.Granted || callback.Previous.MemberID != old.currentIdentity().MemberID {
		t.Fatal("retry allocated a different authorized lineage", callback, required.Readmission, calls)
	}
	if len(callback.PreviousGroups) != 1 || len(callback.PreviousGroups[0].Replicas) != 1 || callback.PreviousGroups[0].Replicas[0].ReplicaID != old.currentIdentity().MemberID {
		t.Fatal("callback lost exact old node scope", callback.PreviousGroups)
	}
	if leader.state().Latest().Version != 2 || retry.state().Active().Version != 2 {
		t.Fatal("readmission retry churned topology")
	}
	cs := retry.engine.Load().Status().ConfState
	if len(cs.Voters) != 3 || containsMember(cs, old.currentIdentity().MemberID) || !containsMember(cs, retry.currentIdentity().MemberID) {
		t.Fatal("readmission did not converge fresh voter", cs)
	}
	// The same durable grant can be resumed after its replacement store was
	// pinned. Do not rerun quarantine against the healthy replacement itself.
	if err = retry.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	opts := restartOptions(t, retry)
	opts.OnReadmission = func(context.Context, Readmission) error {
		return errors.New("replacement store must be resumed, not quarantined")
	}
	resumed, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resumed.Stop(context.Background()) })
	if err = resumed.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = resumed.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if resumed.currentIdentity().MemberID != retry.currentIdentity().MemberID || leader.state().Latest().Version != 2 {
		t.Fatal("resumed grant changed identity/version")
	}
}

func TestMetaPinnedAdmissionRestartsBeforeAndAfterReadyWithoutAllocatingIDs(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test")
	}
	for _, prepared := range []bool{false, true} {
		t.Run(fmt.Sprint(prepared), func(t *testing.T) {
			groups := publicFoundingClusterWith(t, 2, func(o *Options) { o.Limits.MaxNodes = 3 })
			leader := readyPublic(t, groups)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			doc := approvedNode(t, leader, "n3", "worker")
			staged, err := leader.Stage(ctx, doc, 1)
			if err != nil {
				t.Fatal(err)
			}
			id, err := leader.Apply(ctx, staged.Version)
			if err != nil {
				t.Fatal(err)
			}
			req := joinRequest{Identity: Identity{NodeName: "n3", ClusterID: "cluster", InstanceID: "before-crash"}, Node: topology.Node{Name: "n3", Type: "worker"}, Address: "127.0.0.1:1"}
			data, _ := json.Marshal(req)
			reply := leader.handleJoin(ctx, &metapb.Header{ClusterId: "cluster", NodeName: "n3"}, data)
			if err = metatransport.ReplyError(reply); err != nil {
				t.Fatal(err)
			}
			var grant joinResponse
			if err = json.Unmarshal(reply.Data, &grant); err != nil {
				t.Fatal(err)
			}
			if prepared {
				ready, _ := json.Marshal(readyRequest{NodeName: "n3", InstanceID: "before-crash", MemberID: grant.MemberID, ExecutionID: grant.Admission.ExecutionID, Address: req.Address})
				if err = metatransport.ReplyError(leader.handleReady(ctx, &metapb.Header{ClusterId: "cluster", NodeName: "n3", MemberId: grant.MemberID}, ready)); err != nil {
					t.Fatal(err)
				}
			}
			// Constructor identity binding was durable before the process crashed. No
			// membership was added yet: recovery must preserve this reserved identity.
			path := filepath.Join(t.TempDir(), "pinned.bolt")
			store, err := metastore.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if err = store.EnsureIdentity("n3", "cluster", grant.MemberID); err != nil {
				t.Fatal(err)
			}
			if err = store.Close(); err != nil {
				t.Fatal(err)
			}
			joined := joiningGroup(t, leader, "n3", "worker", "after-crash", []string{leader.opts.Network.AdvertiseAddress}, func(o *Options) { o.DBPath = path })
			if err = joined.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			status, err := joined.WaitExecution(ctx, id)
			if err != nil || status.State != topology.ExecutionSucceeded {
				t.Fatal(status, err)
			}
			if joined.currentIdentity().MemberID != grant.MemberID || leader.state().Latest().Version != 2 {
				t.Fatal("restart allocated another identity/version")
			}
		})
	}
}
