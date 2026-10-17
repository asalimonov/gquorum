//go:build functional

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/tests/cluster/testapp/loadpb"
	"github.com/asalimonov/gquorum/topology"
	"google.golang.org/grpc/metadata"
)

type authorityProtocolNode struct {
	gquorum.Node
	id      topology.Identity
	fences  atomic.Uint64
	actions atomic.Uint64
	refuse  atomic.Bool
}

func (n *authorityProtocolNode) Identity() topology.Identity { return n.id }
func (n *authorityProtocolNode) InstallLoadFence(_ context.Context, req load.FenceRequest) (load.FenceResult, error) {
	n.fences.Add(1)
	return load.FenceResult{From: req.To, Evidence: topology.FenceEvidence{Key: req.Grant.Key, Resource: req.Resource, Receiver: req.To, LocalSequence: 7}}, nil
}
func (n *authorityProtocolNode) ApplyLoadAction(_ context.Context, req load.ActionRequest) (load.ActionResult, error) {
	n.actions.Add(1)
	if n.refuse.Load() {
		return load.ActionResult{}, fault.ErrAuthorityFenced
	}
	return load.ActionResult{From: req.To, Authorization: req.Authorization, LocalTarget: req.LocalTarget, Receipts: []topology.EffectReceipt{{Key: req.Grant.Key, Resource: req.Grant.Resources[0], Action: req.Grant.Action, Target: req.Grant.Targets[0], Outcome: topology.EffectApplied, LocalSequence: 9}}}, nil
}

func TestAuthorityFenceControlSeparatesInstallationAndResponseAndOwnsCopies(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "after"}[after], func(t *testing.T) {
			t.Parallel()
			id := topology.Identity{ClusterID: "cluster", NodeName: "one", MemberID: 1, InstanceID: "process"}
			node := &authorityProtocolNode{id: id}
			a := &application{node: node, cfg: config{Credential: "credential"}, workCtx: t.Context(), closing: make(chan struct{}), clusterHooks: newClusterHooks()}
			control := func(method, credential string, identity topology.Identity) *httptest.ResponseRecorder {
				body, _ := json.Marshal(map[string]any{"identity": identity, "after": after, "min_revision": uint64(9007199254740993)})
				request := httptest.NewRequest(method, "/internal/test/fences/work/host", bytes.NewReader(body))
				request.Header.Set("X-Gquorum-Test-Credential", credential)
				w := httptest.NewRecorder()
				a.routes().ServeHTTP(w, request)
				return w
			}
			if w := control("PUT", "foreign", id); w.Code != http.StatusForbidden {
				t.Fatal(w.Code, w.Body.String())
			}
			foreign := id
			foreign.InstanceID = "replacement"
			if w := control("PUT", a.cfg.Credential, foreign); w.Code != http.StatusConflict {
				t.Fatal(w.Code, w.Body.String())
			}
			if w := control("PUT", a.cfg.Credential, id); w.Code != http.StatusOK {
				t.Fatal(w.Code, w.Body.String())
			}
			resource := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: "work", NodeName: id.NodeName}
			grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: id.ClusterID, GroupID: "work", Revision: 9007199254740993}, Purpose: topology.AuthorityFence, Resources: []topology.AuthorityResource{resource}, Targets: []topology.AuthorityTarget{{NodeName: id.NodeName, MemberID: id.MemberID, ReplicaID: 7}}}
			fence := load.FenceRequest{From: id, To: id, Grant: grant, Resource: resource}
			data, _ := json.Marshal(fence)
			request := &loadpb.Request{Header: &loadpb.Header{ClusterId: id.ClusterID, NodeName: id.NodeName, MemberId: id.MemberID, InstanceId: id.InstanceID}, Method: "Fence", GroupId: "work", Data: data}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(credentialHeader, a.cfg.Credential))
			joined := make(chan error, 1)
			go func() {
				reply, err := a.Call(ctx, request)
				if err == nil {
					err = replyError(reply)
				}
				joined <- err
			}()
			apiEventually(t, func() bool { return a.authorityObservation("work").Barriers[0].Hits == 1 })
			view := a.authorityObservation("work")
			if node.fences.Load() != uint64(len(view.Fences)) || after && node.fences.Load() != 1 || !after && node.fences.Load() != 0 {
				t.Fatal("barrier misreported receiver installation", node.fences.Load(), view)
			}
			view.Barriers[0].Last.Grant.Targets[0].ReplicaID = 99
			if a.authorityObservation("work").Barriers[0].Last.Grant.Targets[0].ReplicaID != 7 {
				t.Fatal("capture aliases caller memory")
			}
			newReceiver := fence
			newReceiver.To.MemberID++
			if err := a.waitAuthorityFence(t.Context(), newReceiver, after); err != nil {
				t.Fatal("former-member control affected a successor", err)
			}
			cancel()
			if err := <-joined; err == nil {
				t.Fatal("blocked response manufactured completion")
			}
			if w := control("DELETE", a.cfg.Credential, foreign); w.Code != http.StatusConflict {
				t.Fatal("foreign process released fence", w.Code)
			}
			if w := control("DELETE", a.cfg.Credential, id); w.Code != http.StatusOK {
				t.Fatal(w.Code, w.Body.String())
			}
			reply, err := a.Call(metadata.NewIncomingContext(t.Context(), metadata.Pairs(credentialHeader, a.cfg.Credential)), request)
			if err != nil || replyError(reply) != nil {
				t.Fatal(reply, err)
			}
			var result load.FenceResult
			if decodeJSON(reply.Data, &result) != nil || result.Evidence.Validate(grant) != nil {
				t.Fatal(result)
			}
		})
	}
}

func TestAuthorityReplayKeepsExactIngressRefusalsAndBoundedOwnedCaptures(t *testing.T) {
	t.Parallel()
	id := topology.Identity{ClusterID: "cluster", NodeName: "one", MemberID: 1, InstanceID: "process"}
	node := &authorityProtocolNode{id: id}
	a := &application{node: node, cfg: config{Credential: "credential"}, workCtx: t.Context(), closing: make(chan struct{}), clusterHooks: newClusterHooks()}
	action := topology.AuthorizedStep{OperationID: topology.MakeOperationID(1, 1), Version: 1, Attempt: 1, StepSeq: 1, GroupID: "work", Kind: topology.StepStopReplica, NodeName: id.NodeName, ReplicaID: 7}
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: id.ClusterID, GroupID: "work", Revision: 17}, Purpose: topology.AuthorityEffect, OperationID: action.OperationID, Action: &action, Resources: []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: "work", NodeName: id.NodeName}}, Targets: []topology.AuthorityTarget{{NodeName: id.NodeName, MemberID: 1, ReplicaID: 7}}}
	request := load.ActionRequest{From: id, To: load.Peer{Node: id, Replica: load.ReplicaRef{GroupID: "work", ReplicaID: 7}}, Authorization: action, Grant: grant, LocalTarget: storage.Identity{ClusterID: id.ClusterID, NodeName: id.NodeName, GroupID: "work", ReplicaID: 7}}
	for revision := uint64(1); revision <= 200; revision++ {
		request.Grant.Key.Revision = revision
		a.captureAuthorityAction(request)
	}
	view := a.authorityObservation("work")
	if len(view.Actions) != 128 || view.Actions[0].Grant.Key.Revision != 73 || view.Actions[127].Grant.Key.Revision != 200 {
		t.Fatal("capture storage is not bounded", len(view.Actions))
	}
	request.Grant.Targets[0].ReplicaID = 99
	if a.authorityObservation("work").Actions[127].Grant.Targets[0].ReplicaID != 7 {
		t.Fatal("capture retained mutable input")
	}
	request = view.Actions[127]
	body, _ := json.Marshal(map[string]any{"identity": id, "action": request})
	call := func(credential string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/internal/test/authority/work/action", bytes.NewReader(body))
		r.Header.Set("X-Gquorum-Test-Credential", credential)
		w := httptest.NewRecorder()
		a.routes().ServeHTTP(w, r)
		return w
	}
	if w := call("foreign"); w.Code != 403 || node.actions.Load() != 0 {
		t.Fatal(w.Code, node.actions.Load())
	}
	node.refuse.Store(true)
	w := call(a.cfg.Credential)
	var refusal struct {
		Code fault.Code `json:"code"`
	}
	if w.Code != 409 || json.Unmarshal(w.Body.Bytes(), &refusal) != nil || refusal.Code != fault.AuthorityFenced || len(a.authorityObservation("work").Receipts) != 0 {
		t.Fatal("replay converted refusal into a receipt", w.Code, w.Body.String())
	}
	node.refuse.Store(false)
	w = call(a.cfg.Credential)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var result load.ActionResult
	if err := decodeJSON(w.Body.Bytes(), &result); err != nil || len(result.Receipts) != 1 || result.Receipts[0].Validate(request.Grant) != nil {
		t.Fatal(result, err)
	}
	result.Receipts[0].Action.Attempt++
	stored := a.authorityObservation("work").Receipts[0]
	if stored.Receipts[0].Action.Attempt != 1 {
		t.Fatal("result aliases capture")
	}
	if !errors.Is(fault.New(refusal.Code, "", nil), fault.ErrAuthorityFenced) {
		t.Fatal(refusal)
	}
}
