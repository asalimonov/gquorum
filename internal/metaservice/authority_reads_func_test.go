//go:build functional

package metaservice

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func guardedFixturePlan(version uint64, group string, base, founding []uint64, steps ...topology.Step) topology.Plan {
	plan := topology.Plan{Version: version, Steps: steps, VoterBases: []topology.PlanVoterBase{{GroupID: group, Voters: append([]uint64(nil), base...)}}}
	current := append([]uint64(nil), base...)
	for i := range plan.Steps {
		step := &plan.Steps[i]
		switch step.Kind {
		case topology.StepCreateFoundingGroup, topology.StepAddLearner, topology.StepPromoteVoter, topology.StepDemoteVoter, topology.StepRemoveReplica:
		default:
			continue
		}
		step.ExpectedVotersDigest = topology.VoterSetDigest(current)
		switch step.Kind {
		case topology.StepCreateFoundingGroup:
			current = append([]uint64(nil), founding...)
		case topology.StepPromoteVoter:
			current = append(current, step.ReplicaID)
		case topology.StepDemoteVoter, topology.StepRemoveReplica:
			current = slices.DeleteFunc(current, func(id uint64) bool { return id == step.ReplicaID })
		}
		step.ResultVotersDigest = topology.VoterSetDigest(current)
	}
	return plan
}

func TestAuthorityReadUsesLeaderForBlankAndFencedProtocolOwner(t *testing.T) {
	t.Parallel()
	g := readyPublic(t, singleMetaFixture(t))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result, err := g.propose(ctx, metaengine.GrantTransferProposal{ExpectedActiveVersion: g.state().Active().Version, GroupID: topology.MetaGroupID, NodeName: "n1", ReplicaID: 1, RequestID: "authority-read"})
	if err != nil {
		t.Fatal(err)
	}
	grant := *result.Grant
	receipts := protocolEffectReceipts(grant)
	if err = g.RecordEffect(ctx, receipts[0]); err != nil {
		t.Fatal(err)
	}
	proof, err := g.readAuthorityProof(ctx, grant.Key, g.Identity().NodeName)
	if err != nil || proof.Grant == nil || !proof.Grant.Equal(grant) || len(proof.Receipts) != 1 {
		t.Fatal(proof, err)
	}
	if err = g.ValidateAuthorityCourier(ctx, grant, g.Identity()); err != nil {
		t.Fatal("current courier refused", proof, err)
	}
	wrong := g.Identity()
	wrong.InstanceID = "former"
	if err = g.ValidateAuthorityCourier(ctx, grant, wrong); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("stale courier accepted", err)
	}
	// A protocol-only client models a joining/removed host. It owns no Raft engine
	// and cannot satisfy an ordinary local barrier; only the remote participant runs Raft.
	client := joinProtocolClient(t, []string{g.opts.Network.AdvertiseAddress}, func(o *Options) {
		o.Network.ClusterCredential = g.opts.Network.ClusterCredential
		retained := o.Identity
		retained.MemberID = 1
		o.ReadmissionIdentity = func() topology.Identity { return retained }
	})
	client.mu.Lock()
	client.started = true
	client.ctx, client.cancel = context.WithCancel(t.Context())
	client.workCtx, client.workCancel = context.WithCancel(client.ctx)
	client.mu.Unlock()
	for _, fenced := range []bool{false, true} {
		if fenced {
			if err = client.FenceParticipation(ctx); err != nil {
				t.Fatal(err)
			}
		}
		remote, err := client.readAuthorityProof(ctx, grant.Key, g.Identity().NodeName)
		if err != nil || remote.Grant == nil || !remote.Grant.Equal(grant) || len(remote.Receipts) != 1 || remote.Courier != proof.Courier {
			t.Fatal("remote proof unavailable", fenced, remote, err)
		}
		if err = client.ValidateGrant(ctx, grant); err != nil {
			t.Fatal("retained grant validation requires local participation", fenced, err)
		}
		if err = client.ValidateAuthorityCourier(ctx, grant, g.Identity()); err != nil {
			t.Fatal("fenced receiver rejected current courier", fenced, err)
		}
		if err = client.ValidateAuthorityCourier(ctx, grant, wrong); !errors.Is(err, fault.ErrIdentityMismatch) {
			t.Fatal("fenced receiver accepted former courier", fenced, err)
		}
		if _, err = client.ReadTopology(ctx); err == nil {
			t.Fatal("protocol proof reopened ordinary reads", fenced)
		}
		if client.Identity().MemberID != 0 {
			t.Fatal("retained authority identity became an ordinary metadata identity")
		}
	}
	if err = client.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = client.readAuthorityProof(ctx, grant.Key, ""); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("stopped owner redirected read", err)
	}
	if err = client.ValidateGrant(ctx, grant); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("fully stopped owner validated authority", err)
	}
	// The proof route rejects unknown identities, unknown fields and broad queries.
	header := &metapb.Header{ClusterId: g.Identity().ClusterID, NodeName: "foreign", MemberId: 99, InstanceId: "i"}
	encoded, _ := json.Marshal(authorityReadRequest{Key: grant.Key})
	if err = metatransport.ReplyError(g.Call(ctx, "AuthorityRead", header, encoded)); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("foreign reader accepted", err)
	}
	header.MemberId = 0
	encoded, _ = json.Marshal(authorityReadRequest{Key: grant.Key, ReaderMemberID: 1})
	if err = metatransport.ReplyError(g.Call(ctx, "AuthorityRead", header, encoded)); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("retained member claim crossed node ownership", err)
	}
	header.NodeName, header.MemberId = g.Identity().NodeName, g.Identity().MemberID
	if err = metatransport.ReplyError(g.Call(ctx, "AuthorityRead", header, []byte(`{"kind":"topology"}`))); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("broad query accepted", err)
	}
}

type approvedRouteResolver struct{ name, address string }

func (r approvedRouteResolver) Resolve(_ context.Context, name string) (string, error) {
	if name != r.name {
		return "", fault.ErrUnavailable
	}
	return r.address, nil
}

func TestAuthorityProofRoutesFromRetainedCatalogWithoutLeaderHintOrSeeds(t *testing.T) {
	t.Parallel()
	g := readyPublic(t, singleMetaFixtureWith(t, func(o *Options) { o.Limits.MaxNodes, o.Limits.MaxReplicas = 3, 3 }))
	g.controller.Stop()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result, err := g.propose(ctx, metaengine.GrantTransferProposal{ExpectedActiveVersion: g.state().Active().Version, GroupID: topology.MetaGroupID, NodeName: "n1", ReplicaID: 1, RequestID: "retained-route"})
	if err != nil {
		t.Fatal(err)
	}
	grant := *result.Grant
	doc := g.state().Latest()
	doc.Nodes = append(doc.Nodes, topology.Node{Name: "n2", Type: "worker", State: topology.NodeActive})
	doc.MetaGroup().Replicas = append(doc.MetaGroup().Replicas, topology.Replica{NodeName: "n2", ReplicaID: 2, Role: topology.ReplicaLearner})
	doc.MetaGroup().NextReplicaID = 3
	doc, err = g.Stage(ctx, doc, doc.Version)
	if err != nil {
		t.Fatal(err)
	}
	plan := guardedFixturePlan(doc.Version, topology.MetaGroupID, []uint64{1}, nil,
		topology.Step{Seq: 1, GroupID: topology.MetaGroupID, NodeName: "n2", ReplicaID: 2, Kind: topology.StepPrepareReplica},
		topology.Step{Seq: 2, GroupID: topology.MetaGroupID, NodeName: "n2", ReplicaID: 2, Kind: topology.StepAddLearner},
	)
	_, err = g.propose(ctx, metaengine.BeginOperatorProposal{Version: doc.Version, Plan: plan, At: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	data, err := g.state().Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id := topology.Identity{ClusterID: g.Identity().ClusterID, NodeName: "n2", MemberID: 2, InstanceID: "retained"}
	client := joinProtocolClient(t, []string{g.opts.Network.AdvertiseAddress}, func(o *Options) {
		o.Identity = id
		o.Identity.MemberID = 0
		o.Node = topology.Node{Name: id.NodeName, Type: "worker"}
		o.Network.ClusterCredential = g.opts.Network.ClusterCredential
		o.Limits = g.opts.Limits
		o.Network.Resolver = approvedRouteResolver{name: g.Identity().NodeName, address: g.opts.Network.AdvertiseAddress}
	})
	if err := client.state().Restore(ctx, data); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	client.identity = id
	client.opts.Join, client.opts.JoinSeeds = false, nil
	client.addresses = map[string]string{}
	client.started = true
	client.ctx, client.cancel = context.WithCancel(t.Context())
	client.workCtx, client.workCancel = context.WithCancel(client.ctx)
	client.mu.Unlock()
	client.transport.SetMemberID(id.MemberID)
	if len(client.opts.JoinSeeds) != 0 || len(client.addresses) != 0 || client.engine.Load() != nil {
		t.Fatal("fixture has an unintended live routing hint")
	}
	proof, err := client.readAuthorityProof(ctx, grant.Key, g.Identity().NodeName)
	if err != nil || proof.Grant == nil || !proof.Grant.Equal(grant) {
		t.Fatal("retained approved identities could not route narrow proof", proof, err)
	}
	if _, err := client.ReadTopology(ctx); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("proof routing reopened ordinary reads", err)
	}
}

func TestMetaHostControlSurvivesParticipationFence(t *testing.T) {
	t.Parallel()
	g := readyPublic(t, singleMetaFixture(t))
	g.controller.Stop()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	doc, err := g.Stage(ctx, g.state().Latest(), g.state().Latest().Version)
	if err != nil {
		t.Fatal(err)
	}
	step := topology.Step{Seq: 1, GroupID: topology.MetaGroupID, NodeName: "n1", ReplicaID: 1, Kind: topology.StepPrepareReplica}
	at := time.Now().UTC()
	plan := guardedFixturePlan(doc.Version, topology.MetaGroupID, []uint64{1}, nil, step, topology.Step{Seq: 2, GroupID: step.GroupID, NodeName: step.NodeName, ReplicaID: step.ReplicaID, Kind: topology.StepAddLearner})
	step = plan.Steps[0]
	started, err := g.propose(ctx, metaengine.BeginOperatorProposal{Version: doc.Version, Plan: plan, At: at})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = g.propose(ctx, metaengine.TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: started.OperatorID, Version: doc.Version, Attempt: 1, StepSeq: 1, GroupID: step.GroupID, NodeName: step.NodeName, State: topology.OperatorRunning, At: at}}); err != nil {
		t.Fatal(err)
	}
	action := topology.AuthorizedStep{OperatorID: started.OperatorID, Version: doc.Version, Attempt: 1, StepSeq: 1, GroupID: step.GroupID, NodeName: step.NodeName, ReplicaID: step.ReplicaID, Kind: step.Kind}
	grant, err := g.BeginEffect(ctx, action)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	client := joinProtocolClient(t, []string{g.opts.Network.AdvertiseAddress}, func(o *Options) {
		o.Network.ClusterCredential = g.opts.Network.ClusterCredential
		o.ApplyLocalMetaAction = func(_ context.Context, r load.ActionRequest) (load.ActionResult, error) {
			calls++
			return load.ActionResult{From: r.To, Authorization: r.Authorization, LocalTarget: r.LocalTarget, Receipts: protocolEffectReceipts(r.Grant)}, nil
		}
		o.InstallLocalFence = func(_ context.Context, r load.FenceRequest) (load.FenceResult, error) {
			return load.FenceResult{From: r.To, Evidence: topology.FenceEvidence{Key: r.Grant.Key, Resource: r.Resource, Receiver: r.To, LocalSequence: 1}}, nil
		}
	})
	client.mu.Lock()
	client.identity.MemberID = 1
	client.started = true
	client.ctx, client.cancel = context.WithCancel(t.Context())
	client.workCtx, client.workCancel = context.WithCancel(client.ctx)
	client.mu.Unlock()
	client.transport.SetMemberID(1)
	header := g.transport.Header()
	peer := load.Peer{Node: client.Identity(), Replica: load.ReplicaRef{GroupID: topology.MetaGroupID, ReplicaID: 1}}
	request := load.ActionRequest{From: g.Identity(), To: peer, Grant: grant, Authorization: action, LocalTarget: storage.Identity{ClusterID: peer.Node.ClusterID, NodeName: peer.Node.NodeName, GroupID: topology.MetaGroupID, ReplicaID: 1}}
	probe := authorityIdentityRequest{Grant: &grant, Target: grant.Targets[0]}
	encodedProbe, _ := json.Marshal(probe)
	for _, member := range []uint64{0, 99} {
		client.mu.Lock()
		client.identity.MemberID = member
		client.mu.Unlock()
		want := fault.ErrReplicaNotReady
		if member != 0 {
			want = fault.ErrIdentityMismatch
		}
		if err = metatransport.ReplyError(client.Call(ctx, "IdentityPing", header, encodedProbe)); !errors.Is(err, want) {
			t.Fatal("unacquired or conflicting host identity lost refusal boundary", member, err)
		}
	}
	client.mu.Lock()
	client.identity.MemberID = 1
	client.mu.Unlock()
	if calls != 0 {
		t.Fatal("identity probing applied a host action", calls)
	}
	for _, fenced := range []bool{false, true} {
		if fenced {
			if err = client.FenceParticipation(ctx); err != nil {
				t.Fatal(err)
			}
		}
		encoded, _ := json.Marshal(request)
		reply := client.Call(ctx, "MetaControl", header, encoded)
		if err = metatransport.ReplyError(reply); err != nil {
			t.Fatal("meta host action lost control path", fenced, err)
		}
		var result load.ActionResult
		if err = json.Unmarshal(reply.Data, &result); err != nil || len(result.Receipts) != 1 || result.Receipts[0].Validate(grant) != nil {
			t.Fatal(result, err)
		}
		fence := load.FenceRequest{From: g.Identity(), To: client.Identity(), Grant: grant, Resource: grant.Resources[0]}
		encoded, _ = json.Marshal(fence)
		if err = metatransport.ReplyError(client.Call(ctx, "InstallFence", header, encoded)); err != nil {
			t.Fatal("host fence lost control path", fenced, err)
		}
		probe := authorityIdentityRequest{Grant: &grant, Target: grant.Targets[0]}
		encoded, _ = json.Marshal(probe)
		if err = metatransport.ReplyError(client.Call(ctx, "IdentityPing", header, encoded)); err != nil {
			t.Fatal("scoped identity probe refused", err)
		}
		if err = metatransport.ReplyError(client.Call(ctx, "IdentityPing", header, nil)); !errors.Is(err, fault.ErrUnavailable) {
			t.Fatal("ordinary probe claimed participation", err)
		}
	}
	request.From.InstanceID = "stale"
	header.InstanceId = "stale"
	encoded, _ := json.Marshal(request)
	if err = metatransport.ReplyError(client.Call(ctx, "MetaControl", header, encoded)); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("stale courier reached fenced host", err)
	}
	if calls != 2 {
		t.Fatal("unauthorized action reached resource owner", calls)
	}
}
