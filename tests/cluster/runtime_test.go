//go:build clustertest

package cluster

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	docker "github.com/moby/moby/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// These are ports of the former in-process runtime scenarios. They remain
// sequential and acquire fresh real processes through the suite's bounded pool.
type runtimeObservation struct {
	Status         gquorum.NodeStatus `json:"status"`
	SchedulerCalls uint64             `json:"scheduler_calls"`
	Factories      uint64             `json:"factories"`
	RemovalCalls   uint64             `json:"removal_calls"`
}

func (c *Cluster) runtime(ctx context.Context, node string) (runtimeObservation, error) {
	var result runtimeObservation
	response, err := c.Request(ctx, node, "GET", "/internal/test/runtime", nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		return result, err
	}
	return result, response.Decode(&result)
}
func runtimeCall(ctx context.Context, t *testing.T, c *Cluster, node, path string, body any, want int) Response {
	t.Helper()
	response, err := c.Request(ctx, node, "POST", "/internal/test/"+path, body, 0)
	if err := requireStatus(response, err, want); err != nil {
		t.Fatal(err)
	}
	return response
}

func TestActivateDeactivateReleasesLocksAndFencesDelayedRevoke(t *testing.T) {
	t.Parallel()
	spec := matrixSpec(3)
	spec.PublishMeta = true
	ctx, c := performanceClusterSpec(t, spec)
	before, err := c.Document(ctx, "node1", "latest")
	if err != nil {
		t.Fatal(err)
	}
	status, err := c.Status(ctx, "node1")
	if err != nil {
		t.Fatal(err)
	}
	leader := status.MetaLeaderNodeName
	if leader == "" {
		t.Fatal("leader has no approved name", status)
	}
	follower := "node1"
	if follower == leader {
		follower = "node2"
	}
	activate := func(node string) topology.Session {
		t.Helper()
		var s topology.Session
		if err := runtimeCall(ctx, t, c, node, "session/activate", map[string]string{}, 200).Decode(&s); err != nil {
			t.Fatal(err)
		}
		if s.ID == "" || s.NodeName != node {
			t.Fatal("wrong session owner", s)
		}
		return s
	}
	session := activate(follower)
	call := func(node, action, id, key string, want int) {
		t.Helper()
		runtimeCall(ctx, t, c, node, "session/"+action, map[string]string{"id": id, "key": key}, want)
	}
	call(follower, "keepalive", session.ID, "", 200)
	call(leader, "keepalive", session.ID, "", 409)
	call(leader, "deactivate", session.ID, "", 409)
	call(follower, "acquire", session.ID, "node/"+leader, 409)
	call(follower, "acquire", session.ID, "application", 200)
	call(follower, "acquire", session.ID, "application", 200)
	holder := func(want string) {
		t.Helper()
		if err := Poll(ctx, func() (bool, error) {
			response, err := c.Request(ctx, leader, "POST", "/internal/test/session/holder", map[string]string{"key": "application"}, 0)
			if err := requireStatus(response, err, 200); err != nil {
				return false, err
			}
			var result struct {
				Session topology.Session `json:"session"`
				Found   bool             `json:"found"`
			}
			if err := response.Decode(&result); err != nil {
				return true, err
			}
			return result.Found == (want != "") && result.Session.ID == want, nil
		}); err != nil {
			t.Fatal("lock holder", err)
		}
	}
	holder(session.ID)
	own := activate(leader)
	call(leader, "acquire", own.ID, "application", 400)
	// The real wire heartbeat/lease protocol must retain exact process identity.
	local, err := c.Local(ctx, follower, topology.MetaGroupID)
	if err != nil {
		t.Fatal(err)
	}
	host, err := c.Node(leader).Container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := c.Node(leader).Container.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(net.JoinHostPort(host, port.Port()), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rpc := metapb.NewMetaClient(conn)
	valid := metadata.AppendToOutgoingContext(ctx, "gquorum-cluster-credential", hex.EncodeToString([]byte(c.Credential)))
	header := &metapb.Header{ClusterId: c.ID, NodeName: follower, MemberId: local.Identity.MemberID, InstanceId: local.Identity.InstanceID}
	for _, tc := range []struct{ id, instance, code string }{
		{session.ID, local.Identity.InstanceID, ""}, {"unknown", local.Identity.InstanceID, string(fault.IdentityMismatch)}, {session.ID, "wrong-instance", string(fault.IdentityMismatch)},
	} {
		data, _ := json.Marshal(map[string]string{"session_id": tc.id, "instance_id": tc.instance})
		reply, err := rpc.KeepAlive(valid, &metapb.MetaRequest{Header: header, Data: data})
		if err != nil || reply.ErrorCode != tc.code {
			t.Fatal("exact keepalive", reply, err)
		}
	}
	call(follower, "deactivate", session.ID, "", 200)
	// Several renewal ticks must not undo explicit deactivation.
	until := time.Now().Add(5 * c.Spec.Tick)
	for time.Now().Before(until) {
		view, err := c.runtime(ctx, follower)
		if err != nil || view.Status.SessionLive {
			t.Fatal("automatic activation ignored explicit deactivation", view, err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(c.Spec.Tick):
		}
	}
	holder("")
	call(follower, "deactivate", session.ID, "", 200)
	next := activate(follower)
	if next.ID == session.ID {
		t.Fatal("reused deactivated session", next)
	}
	call(follower, "deactivate", session.ID, "", 200)
	view, err := c.runtime(ctx, follower)
	if err != nil || !view.Status.SessionLive {
		t.Fatal("delayed revoke affected new session", view, err)
	}
	call(leader, "acquire", own.ID, "application", 200)
	call(follower, "acquire", next.ID, "application", 400)
	for _, node := range c.Nodes() {
		after, err := c.Document(ctx, node.Spec.Name, "latest")
		if err != nil || after.Version != before.Version {
			t.Fatal("session commands churned topology", after, err)
		}
	}
}

func TestPublicNodeSequentialStartManagedHoldsAndRecovery(t *testing.T) {
	t.Parallel()
	ctx, c := performanceCluster(t, 3)
	created, err := c.CreateGroup(ctx, "node2", gquorum.GroupSpec{ID: "counter"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "node1", "counter", "one", "one", "seven"); err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, created.Document, "counter", map[string]string{"one": "seven"}); err != nil {
		t.Fatal(err)
	}
	local, err := c.Local(ctx, "node1", "counter")
	if err != nil {
		t.Fatal(err)
	}
	held := "node1"
	if local.Status.LeaderID == local.ReplicaID {
		held = "node2"
	}
	ref := load.ReplicaRef{GroupID: "counter", ReplicaID: replicaID(created.Document, "counter", held)}
	runtimeCall(ctx, t, c, held, "replica/stop", ref, 200)
	// Observe across recovery ticks rather than checking only the immediate stop.
	until := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(until) {
		view, err := c.Local(ctx, held, "counter")
		if err != nil || view.Found || view.StorageIdentity == nil || view.StorageIdentity.ReplicaID != ref.ReplicaID {
			t.Fatal("explicit hold lost durable state or was erased", view, err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(60 * time.Millisecond):
		}
	}
	runtimeCall(ctx, t, c, held, "replica/start", ref, 200) // Also verifies old handle remains stopped.
	if err := c.WaitValue(ctx, held, "counter", "one", "seven"); err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		if err := c.Snapshot(ctx, node.Spec.Name, "counter"); err != nil {
			t.Fatal(err)
		}
	}
	identities := map[string]topology.Identity{}
	for _, node := range c.Nodes() {
		local, err := c.Local(ctx, node.Spec.Name, "counter")
		if err != nil {
			t.Fatal(err)
		}
		identities[node.Spec.Name] = local.Identity
	}
	for _, node := range c.Nodes() {
		if err := node.Stop(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, node := range c.Nodes() {
		if err := node.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	for _, node := range c.Nodes() {
		if err := c.WaitValue(ctx, node.Spec.Name, "counter", "one", "seven"); err != nil {
			t.Fatal(err)
		}
		local, err := c.Local(ctx, node.Spec.Name, "counter")
		before := identities[node.Spec.Name]
		if err != nil || local.ReplicaID != replicaID(created.Document, "counter", node.Spec.Name) || local.Identity.MemberID != before.MemberID || local.Identity.InstanceID == before.InstanceID {
			t.Fatal("retained restart lost exact identity", local, err)
		}
	}
}

func TestConcurrentAuthorizedCleanupRetriesAreIdempotent(t *testing.T) {
	t.Parallel()
	spec := matrixSpec(3)
	spec.Bootstrap = true
	ctx, c := performanceClusterSpec(t, spec)
	// Bootstrap creates both declared application groups asynchronously; wait
	// for their convergence before taking the CAS version used for deletion.
	assertFoundedTopology(ctx, t, c)
	if err := c.SetBarrier(ctx, "node3", "demo", topology.StepCleanupReplicaState, true); err != nil {
		t.Fatal(err)
	}
	base, err := c.Document(ctx, "node1", "latest")
	if err != nil {
		t.Fatal(err)
	}
	// Stage once; transient approval failures must retry this version without
	// issuing another drain or changing the authorized cleanup attempt.
	response, err := c.Request(ctx, "node1", "POST", "/api/nodes/node3/drain", gquorum.MutationOptions{Actor: "operator"}, base.Version)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var result gquorum.MutationResult
	if err := response.Decode(&result); err != nil {
		t.Fatal(err)
	}
	result.OperatorID = startApply(ctx, t, c, "node1", result.Document.Version)
	var approved topology.AuthorizedStep
	if err := Poll(ctx, func() (bool, error) {
		controls, err := c.Controls(ctx, "node3")
		if err != nil {
			return false, err
		}
		for _, barrier := range controls.Barriers {
			if barrier.Kind == topology.StepCleanupReplicaState && barrier.Hits > 0 {
				approved = barrier.Last
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	if approved.GroupID != "demo" || approved.NodeName != "node3" || approved.Kind != topology.StepCleanupReplicaState || approved.OperatorID != result.OperatorID || approved.ReplicaID == 0 {
		t.Fatal("wrong authorization", approved)
	}
	local, err := c.Local(ctx, "node3", topology.MetaGroupID)
	if err != nil {
		t.Fatal(err)
	}
	view, err := c.Authority(ctx, "node3", approved.GroupID)
	if err != nil {
		t.Fatal(err)
	}
	var req load.ActionRequest
	for _, action := range view.Actions {
		if action.Authorization == approved {
			req = action
			break
		}
	}
	if req.Grant.Validate() != nil || req.To.Node != local.Identity || req.Grant.Action == nil || *req.Grant.Action != approved {
		t.Fatal("cleanup retry did not capture the complete immutable grant", req)
	}
	var workers sync.WaitGroup
	start := make(chan struct{})
	results := make(chan error, 32)
	for range 32 {
		workers.Go(func() {
			<-start
			response, err := c.ReplayAction(ctx, "node3", local.Identity, req)
			results <- requireStatus(response, err, 200)
		})
	}
	close(start)
	workers.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal("concurrent approved retry", err)
		}
	}
	retired, err := c.Local(ctx, "node3", "demo")
	if err != nil || retired.Found || retired.StorageIdentity != nil {
		t.Fatal("retired replica/store remains", retired, err)
	}
	if err := c.SetBarrier(ctx, "node3", "demo", topology.StepCleanupReplicaState, false); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitOperator(ctx, "node1", result.OperatorID); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitActive(ctx, result.Document.Version); err != nil {
		t.Fatal(err)
	}
}

func TestPublicNodeCallerSchedulersFollowLeadershipWithoutAgreementGate(t *testing.T) {
	t.Parallel()
	spec := matrixSpec(3)
	spec.Scheduling = true
	spec.CallerScheduler = true
	spec.SchedulingInterval = 100 * time.Millisecond
	spec.SchedulingTimeout = 70 * time.Millisecond
	ctx, c := performanceClusterSpec(t, spec)
	before, err := c.Document(ctx, "node1", "latest")
	if err != nil {
		t.Fatal(err)
	}
	status, err := c.Status(ctx, "node1")
	if err != nil {
		t.Fatal(err)
	}
	first := status.MetaLeaderNodeName
	next := "node1"
	if first == next {
		next = "node2"
	}
	assertPolicy := func(name string) {
		t.Helper()
		if err := Poll(ctx, func() (bool, error) {
			view, err := c.runtime(ctx, name)
			return err == nil && view.SchedulerCalls > 0 && view.Status.Scheduling.Reason == "caller-policy-"+name, err
		}); err != nil {
			t.Fatal(err)
		}
	}
	assertPolicy(first)
	if err := transferMeta(ctx, c, "node3", next, before.MemberID(next)); err != nil {
		t.Fatal(err)
	}
	assertPolicy(next)
	for _, node := range c.Nodes() {
		view, err := c.runtime(ctx, node.Spec.Name)
		if err != nil || !view.Status.Ready || !view.Status.SessionLive {
			t.Fatal("policy interrupted participation", view, err)
		}
		after, err := c.Document(ctx, node.Spec.Name, "latest")
		if err != nil || after.Version != before.Version {
			t.Fatal("policy staged topology", after, err)
		}
	}
	for _, enabled := range []bool{false, true} {
		response, err := c.Request(ctx, next, "PUT", "/internal/test/scheduling", map[string]bool{"enabled": enabled}, 0)
		if err := requireStatus(response, err, 200); err != nil {
			t.Fatal(err)
		}
		if !enabled {
			if err := Poll(ctx, func() (bool, error) {
				view, err := c.runtime(ctx, next)
				return view.Status.Scheduling.State == topology.SchedulingDisabled, err
			}); err != nil {
				t.Fatal(err)
			}
		} else {
			assertPolicy(next)
		}
	}
}

func TestPublicNodeRemovalFencesBeforeLifecycleHooks(t *testing.T) {
	t.Parallel()
	spec := matrixSpec(3)
	spec.RetryRemoval = true
	ctx, c := performanceClusterSpec(t, spec)
	created, err := c.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: "drained"})
	if err != nil {
		t.Fatal(err)
	}
	if err := transferMeta(ctx, c, "node2", "node1", created.Document.MemberID("node1")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Mutate(ctx, "node1", "POST", "/api/nodes/node3/drain", gquorum.MutationOptions{Approve: true}); err != nil {
		t.Fatal(err)
	}
	base, err := c.Document(ctx, "node1", "latest")
	if err != nil {
		t.Fatal(err)
	}
	response, err := c.Request(ctx, "node1", "POST", "/api/nodes/node3/exclude", gquorum.MutationOptions{Approve: true}, base.Version)
	if err := requireStatus(response, err, 202); err != nil {
		t.Fatal(err)
	}
	var result gquorum.MutationResult
	if err := response.Decode(&result); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitOperator(ctx, "node1", result.OperatorID); err != nil {
		t.Fatal(err)
	}
	// A fenced participant keeps its removal diagnostics, not the successor's
	// Active view. Wait for convergence only on the remaining members.
	for _, name := range []string{"node1", "node2"} {
		if err := Poll(ctx, func() (bool, error) {
			doc, err := c.Document(ctx, name, "active")
			return doc.Version == result.Document.Version, err
		}); err != nil {
			t.Fatal(err)
		}
	}
	// The fixture checks the mandatory fence inside both hook invocations,
	// refuses the first, and keeps the real process alive for diagnostics.
	if err := Poll(ctx, func() (bool, error) {
		view, err := c.runtime(ctx, "node3")
		if err != nil {
			return false, err
		}
		return view.RemovalCalls == 2 && view.Status.State == gquorum.NodeFenced && !view.Status.Ready && !view.Status.SessionLive && !view.Status.Meta.Found && view.Status.SelfRemoval != nil && view.Status.SelfRemoval.Fenced, nil
	}); err != nil {
		t.Fatal("mandatory fencing/removal retry", err)
	}
	runtimeCall(ctx, t, c, "node3", "session/activate", map[string]string{}, 503)
	view, err := c.runtime(ctx, "node3")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Node("node3").Stop(ctx); err != nil {
		t.Fatal(err)
	}
	assertDurableSelfRemoval(ctx, t, c, view.Status)

}

func TestSlowDestinationCannotDelayHealthyMajorityAndStopJoinsSenders(t *testing.T) {
	t.Parallel()
	ctx, c := performanceCluster(t, 3)
	created, err := c.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: "stalled"})
	if err != nil {
		t.Fatal(err)
	}
	if err := requestLeaderTransfer(ctx, c, "node2", "stalled", "node1"); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		view, err := c.Local(ctx, "node1", "stalled")
		return view.Status.LeaderID == view.ReplicaID, err
	}); err != nil {
		t.Fatal(err)
	}
	if err := transferMeta(ctx, c, "node2", "node1", created.Document.MemberID("node1")); err != nil {
		t.Fatal(err)
	}
	victim := c.Node("node3")
	if _, err := c.docker.ContainerPause(ctx, victim.Container.GetContainerID(), docker.ContainerPauseOptions{}); err != nil {
		t.Fatal(err)
	}
	// Always unpause before lease cleanup, including failed assertions.
	paused := true
	defer func() {
		if paused {
			release, done := context.WithTimeout(context.Background(), 10*time.Second)
			defer done()
			if _, err := c.docker.ContainerUnpause(release, victim.Container.GetContainerID(), docker.ContainerUnpauseOptions{}); err != nil {
				t.Error("release paused peer", err)
			}
		}
	}()
	state, err := victim.Container.State(ctx)
	if err != nil || !state.Paused {
		t.Fatal("stall was not installed", state, err)
	}
	expected := map[string]string{}
	for i := range 50 {
		key := fmt.Sprintf("acknowledged-%02d", i)
		operation, done := context.WithTimeout(ctx, time.Second)
		err := c.Put(operation, "node1", "stalled", key, key, key)
		done()
		if err != nil {
			t.Fatal("healthy majority delayed by paused peer", i, err)
		}
		expected[key] = key
	}
	for _, name := range []string{"node1", "node2"} {
		if err := Poll(ctx, func() (bool, error) {
			view, err := c.Local(ctx, name, "stalled")
			if err != nil {
				return false, err
			}
			if len(view.Values) != len(expected) {
				return false, nil
			}
			for key, value := range expected {
				if view.Values[key] != value {
					return true, fmt.Errorf("%s lost %s", name, key)
				}
			}
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.docker.ContainerUnpause(ctx, victim.Container.GetContainerID(), docker.ContainerUnpauseOptions{}); err != nil {
		t.Fatal(err)
	}
	paused = false
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, created.Document, "stalled", expected); err != nil {
		t.Fatal("stalled peer recovery", err)
	}
	for _, node := range c.Nodes() {
		if err := node.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		state, err := node.Container.State(ctx)
		if err != nil || state.Running || state.ExitCode != 0 || state.OOMKilled {
			t.Fatal("senders not joined on process stop", node.Spec.Name, state, err)
		}
	}
}

func TestMetaCallerEffectCompletionLostAcrossLeaderReplacement(t *testing.T) {
	t.Parallel()
	ctx, c := performanceCluster(t, 3)
	base, err := c.Document(ctx, "node1", "latest")
	if err != nil {
		t.Fatal(err)
	}
	if err := transferMeta(ctx, c, "node2", "node1", base.MemberID("node1")); err != nil {
		t.Fatal(err)
	}
	response, err := c.Request(ctx, "node3", "PUT", "/internal/test/barriers/lost-ack/create_founding_group?after=true", nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	// Stage on the transferred leader, then retry only this exact version's
	// approval through the follower while its leader observation catches up.
	response, err = c.Request(ctx, "node1", "POST", "/api/groups", map[string]any{"group": gquorum.GroupSpec{ID: "lost-ack", Nodes: []string{"node2", "node3"}, VoterConfig: voterConfig(2), Placement: &topology.PlacementRule{MinReplicas: 2, MaxReplicas: 2}}, "mutation": gquorum.MutationOptions{}}, base.Version)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var created gquorum.MutationResult
	if err := response.Decode(&created); err != nil {
		t.Fatal(err)
	}
	created.OperatorID = startApply(ctx, t, c, "node2", created.Document.Version)
	var approved topology.AuthorizedStep
	if err := Poll(ctx, func() (bool, error) {
		controls, err := c.Controls(ctx, "node3")
		if err != nil {
			return false, err
		}
		for _, barrier := range controls.Barriers {
			if barrier.Kind == topology.StepCreateFoundingGroup && barrier.Hits > 0 && barrier.Last.OperatorID == created.OperatorID {
				approved = barrier.Last
				view, err := c.Local(ctx, "node3", "lost-ack")
				return view.Found, err
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal("effect did not complete before held acknowledgement", err)
	}
	// This is an abrupt process loss after the remote effect, not a manual
	// leadership transfer before the effect starts.
	if err := c.Node("node1").Kill(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		status, err := c.Status(ctx, "node2")
		return status.MetaLeaderID != 0 && status.MetaLeaderID != base.MemberID("node1"), err
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.SetBarrier(ctx, "node3", "lost-ack", topology.StepCreateFoundingGroup, false); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitOperator(ctx, "node2", created.OperatorID); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitActive(ctx, created.Document.Version); err != nil {
		t.Fatal(err)
	}
	if err := c.Node("node1").Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, "node2", "lost-ack", "after-loss", "after-loss", "retained"); err != nil {
		t.Fatal(err)
	}
	if err := exactOwnedValues(ctx, c, created.Document, "lost-ack", map[string]string{"after-loss": "retained"}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"node2", "node3"} {
		view, err := c.runtime(ctx, name)
		if err != nil || view.Factories != 1 {
			t.Fatal("leader replacement duplicated caller-owned factory", view, err)
		}
	}
	own, err := c.Local(ctx, "node3", topology.MetaGroupID)
	if err != nil {
		t.Fatal(err)
	}
	replica := replicaID(created.Document, "lost-ack", "node3")
	stale := load.ActionRequest{From: own.Identity, To: load.Peer{Node: own.Identity, Replica: load.ReplicaRef{GroupID: "lost-ack", ReplicaID: replica}}, Authorization: approved, LocalTarget: storage.Identity{NodeName: "node3", ClusterID: c.ID, GroupID: "lost-ack", ReplicaID: replica}}
	refused := runtimeCall(ctx, t, c, "node3", "action", stale, 409)
	var refusal struct {
		Code fault.Code `json:"code"`
	}
	if err := refused.Decode(&refusal); err != nil || refusal.Code != fault.OperatorAuthorization {
		t.Fatal("completed authorization remained usable", refusal, err)
	}
	operator, err := c.Operator(ctx, "node2", created.OperatorID)
	if err != nil || operator.Attempt != 1 || operator.State != topology.OperatorSucceeded {
		t.Fatal("leader replacement changed operator attempt", operator, err)
	}
}
