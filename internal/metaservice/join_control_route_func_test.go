//go:build functional

package metaservice

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/topology"
)

func TestApprovedJoinPublishesControlRouteBeforePredecessorRevocation(t *testing.T) {
	t.Parallel()
	// Only the voter runs. The absent learner supplies historical ownership;
	// this test exercises metadata routing, never multi-node consensus.
	g := readyPublic(t, singleMetaFixtureWith(t, func(o *Options) {
		o.Limits.MaxNodes = 2
		o.Founders = append(o.Founders, FoundingMember{Node: topology.Node{Name: "n2", Type: "worker"}, MemberID: 2, Address: "127.0.0.1:1", PinnedLearner: true})
	}))
	g.controller.Stop()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	leader := g.Identity()
	if _, err := g.propose(ctx, metaengine.GrantTransferProposal{ExpectedActiveVersion: g.state().Active().Version, GroupID: topology.MetaGroupID, NodeName: leader.NodeName, ReplicaID: leader.MemberID, RequestID: "pending-before-readmission"}); err != nil {
		t.Fatal(err)
	}
	id := topology.Identity{ClusterID: leader.ClusterID, NodeName: "n2", MemberID: 2, InstanceID: "returning"}
	if _, err := g.propose(ctx, metaengine.GrantSession{NodeName: id.NodeName, InstanceID: id.InstanceID, SessionID: "previous-activation", TTLSeconds: 30}); err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	doc, err := metaengine.ReadmissionDocument(g.state().Active(), id.NodeName, at)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := g.PlanFor(ctx, doc)
	if err != nil {
		t.Fatal(err)
	}
	a := metaengine.AdmissionRecord{NodeName: id.NodeName, InstanceID: id.InstanceID, MemberID: doc.MemberID(id.NodeName)}
	if _, err := g.propose(ctx, metaengine.CommitReadmissionProposal{Admission: a, Doc: doc, Plan: plan, ExpectedVersion: g.state().Latest().Version, At: at}); err != nil {
		t.Fatal(err)
	}
	waitPublic(t, func() bool { return joinClassificationReady(g.engine.Load().RaftStatus(), g.state()) })
	controlID := id
	controlID.MemberID = 0
	client := processOnlyClient(t, g, controlID)
	request := joinRequest{Identity: id, AuthorityMemberID: id.MemberID, Node: g.opts.Node, Address: client.opts.Network.AdvertiseAddress}
	request.Identity.MemberID = 0 // only the independent authority store remains
	request.Node = *doc.Node(id.NodeName)
	header := &metapb.Header{ClusterId: id.ClusterID, NodeName: id.NodeName, InstanceId: id.InstanceID}
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := metatransport.ReplyError(g.handleJoin(ctx, header, data)); !errors.Is(err, fault.ErrAuthorityPending) {
		t.Fatal("readmission did not wait for predecessor fence", err)
	}
	g.mu.RLock()
	route := g.addresses[id.NodeName]
	g.mu.RUnlock()
	if route != request.Address {
		t.Fatal("pending revocation could not reach admitted control owner", route)
	}
	copyRequest := request
	copyRequest.Identity.InstanceID = "copied-process"
	copyRequest.Address = "127.0.0.1:54322"
	copyHeader := &metapb.Header{ClusterId: id.ClusterID, NodeName: id.NodeName, InstanceId: copyRequest.Identity.InstanceID}
	copyData, _ := json.Marshal(copyRequest)
	if err := metatransport.ReplyError(g.handleJoin(ctx, copyHeader, copyData)); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("live control-only admission was replaced", err)
	}
	if old, _ := g.state().Admission(id.NodeName, id.InstanceID); old.State != metaengine.AdmissionPending {
		t.Fatal("live admission was superseded", old)
	}
	request.Node.Type = "foreign-type"
	request.Address = "127.0.0.1:54322"
	data, _ = json.Marshal(request)
	if err := metatransport.ReplyError(g.handleJoin(ctx, header, data)); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	g.mu.RLock()
	retainedRoute := g.addresses[id.NodeName]
	g.mu.RUnlock()
	if retainedRoute != route {
		t.Fatal("refused identity replaced admitted control route")
	}
	if err := client.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := metatransport.ReplyError(g.handleJoin(ctx, copyHeader, copyData)); !errors.Is(err, fault.ErrAuthorityPending) {
		t.Fatal("stopped process prevented exact pending admission rebind", err)
	}
	if replacement, ok := g.state().Admission(id.NodeName, copyRequest.Identity.InstanceID); !ok || replacement.MemberID != doc.MemberID(id.NodeName) {
		t.Fatal("replacement lost exact pending member", replacement)
	}
}
