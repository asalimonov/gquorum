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

func TestJoinPreservesIndependentProgressAndProcessExclusivity(t *testing.T) {
	t.Parallel()
	// Only the voter runs. The absent learner supplies historical ownership;
	// this test exercises metadata routing, never multi-node consensus.
	g := readyPublic(t, singleMetaFixtureWith(t, func(o *Options) {
		o.Limits.MaxNodes = 2
		o.LoadGroups = &planningController{nativeErr: fault.ErrUnavailable}
		o.Founders = append(o.Founders, FoundingMember{Node: topology.Node{Name: "n2", Type: "worker"}, MemberID: 2, Address: "127.0.0.1:1", PinnedLearner: true})
	}))
	g.controller.Stop()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	leader := g.Identity()
	if _, err := g.propose(ctx, metaengine.GrantTransferProposal{ExpectedSourceVersion: 1, GroupID: topology.MetaGroupID, NodeName: leader.NodeName, ReplicaID: leader.MemberID, RequestID: "pending-before-readmission"}); err != nil {
		t.Fatal(err)
	}
	id := topology.Identity{ClusterID: leader.ClusterID, NodeName: "n2", MemberID: 2, InstanceID: "returning"}
	if _, err := g.propose(ctx, metaengine.GrantSession{NodeName: id.NodeName, InstanceID: id.InstanceID, SessionID: "previous-activation", TTLSeconds: 30}); err != nil {
		t.Fatal(err)
	}
	// Leave a load group owned and unconverged, then stage independent intent.
	// Neither the old completion marker nor Latest/Target equality admits n2.
	doc, err := g.Stage(ctx, loadDocument(t, g), 1)
	if err != nil {
		t.Fatal(err)
	}
	loadOperation, err := g.Approve(ctx, doc.Version)
	if err != nil {
		t.Fatal(err)
	}
	doc.Group("load-one").Labels = map[string]string{"owner": "application"}
	staged, err := g.Stage(ctx, doc, doc.Version)
	if err != nil {
		t.Fatal(err)
	}
	waitPublic(t, func() bool { return joinClassificationReady(g.engine.Load().RaftStatus(), g.state()) })
	controlID := id
	controlID.MemberID = 0
	client := processOnlyClient(t, g, controlID)
	request := joinRequest{Identity: id, AuthorityMemberID: id.MemberID, Node: g.opts.Node, Address: client.opts.Network.AdvertiseAddress}
	request.Node = *doc.Node(id.NodeName)
	header := &metapb.Header{ClusterId: id.ClusterID, NodeName: id.NodeName, InstanceId: id.InstanceID, MemberId: id.MemberID}
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	retained := g.handleJoin(ctx, header, data)
	var retainedResponse joinResponse
	if err := metatransport.ReplyError(retained); err != nil {
		t.Fatal("unrelated load owner blocked retained restart", err)
	}
	if err := json.Unmarshal(retained.Data, &retainedResponse); err != nil || !retainedResponse.Existing || retainedResponse.MemberID != id.MemberID {
		t.Fatal("retained restart lost exact identity", retainedResponse, err)
	}
	request.Identity.MemberID, header.MemberId = 0, 0 // only the independent authority store remains
	data, _ = json.Marshal(request)
	joined := g.handleJoin(ctx, header, data)
	if err := metatransport.ReplyError(joined); err != nil {
		t.Fatal("unrelated group work blocked readmission", err)
	}
	var response joinResponse
	if err := json.Unmarshal(joined.Data, &response); err != nil || !response.Readmitted ||
		response.PreviousMemberID != id.MemberID || len(response.Grants) != 1 || response.MemberID != 3 {
		t.Fatal("readmission lost its exact former-owner batch", response, err)
	}
	fences, err := g.state().ReadyFences(topology.AuthorityResource{}, 10)
	if err != nil || len(fences) == 0 {
		t.Fatal("readmission discarded predecessor group revocation", fences, err)
	}
	if _, err := g.propose(ctx, metaengine.ReadyAdmissionProposal{NodeName: id.NodeName,
		InstanceID: id.InstanceID, MemberID: response.MemberID, OperationID: response.Admission.OperationID}); !errors.Is(err, fault.ErrAuthorityPending) {
		t.Fatal("fresh membership became ready before former-owner retirement", err)
	}
	view := g.state().Views()
	doc = view.Target
	if doc.Version != staged.Version+1 || view.Latest.Version != staged.Version+2 ||
		view.Latest.RebasedFrom != staged.Version || view.FullyConverged.Version != 1 ||
		view.Latest.Group("load-one").Labels["owner"] != "application" || doc.Group("load-one").Labels != nil {
		t.Fatal("readmission lost unrelated staged intent or manufactured convergence", view)
	}
	progress, err := g.state().Progress(topology.ProgressQuery{GroupID: "load-one"})
	if err != nil || len(progress.Groups) != 1 || progress.Groups[0].OperationID != loadOperation {
		t.Fatal("readmission changed the independent load owner", progress, err)
	}
	g.mu.RLock()
	route := g.addresses[id.NodeName]
	g.mu.RUnlock()
	if route != request.Address {
		t.Fatal("readmission did not publish its exact control route", route)
	}
	lostAuthority := request
	lostAuthority.AuthorityMemberID = 0
	lostData, _ := json.Marshal(lostAuthority)
	if err := metatransport.ReplyError(g.handleJoin(ctx, header, lostData)); !errors.Is(err, fault.ErrReadmissionRequired) {
		t.Fatal("readmission accepted a process without its retained authority identity", err)
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
	if err := metatransport.ReplyError(g.handleJoin(ctx, copyHeader, copyData)); err != nil {
		t.Fatal("stopped process prevented exact pending admission rebind", err)
	}
	if replacement, ok := g.state().Admission(id.NodeName, copyRequest.Identity.InstanceID); !ok ||
		replacement.MemberID != doc.MemberID(id.NodeName) || metaengine.ReadmissionID(replacement) != metaengine.ReadmissionID(response.Admission) {
		t.Fatal("replacement lost exact pending member", replacement)
	}
	// The real ready receiver must encode a complete v4 receipt proposal. A
	// protocol double accepting JoinReady misses payload validation by the FSM.
	var receipts []topology.EffectReceipt
	for _, grant := range response.Grants {
		receipts = append(receipts, protocolEffectReceipts(grant)...)
	}
	ready := readyRequest{NodeName: id.NodeName, InstanceID: copyRequest.Identity.InstanceID,
		MemberID: response.MemberID, OperationID: response.Admission.OperationID,
		Address: copyRequest.Address, Receipts: receipts}
	data, err = json.Marshal(ready)
	if err != nil {
		t.Fatal(err)
	}
	copyHeader.MemberId = ready.MemberID
	for range 2 {
		if err := metatransport.ReplyError(g.Call(ctx, "JoinReady", copyHeader, data)); err != nil {
			t.Fatal("ready receipt batch did not reach the replicated FSM", err)
		}
	}
	if admission, ok := g.state().Admission(id.NodeName, ready.InstanceID); !ok || admission.State != metaengine.AdmissionReady {
		t.Fatal("retired former owner did not unblock the exact admission", admission)
	}
}
