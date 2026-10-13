package main

import (
	"context"
	"net/http"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

type fenceControlKey struct {
	group string
	kind  topology.AuthorityResourceKind
}
type fenceControl struct {
	identity    topology.Identity
	replicaID   uint64
	release     chan struct{}
	purpose     topology.AuthorityPurpose
	minRevision uint64
	after       bool
	hits        uint64
	last        load.FenceRequest
}
type fenceControlObservation struct {
	Identity    topology.Identity              `json:"identity"`
	ReplicaID   uint64                         `json:"replica_id,omitempty"`
	Group       string                         `json:"group"`
	Kind        topology.AuthorityResourceKind `json:"kind"`
	Purpose     topology.AuthorityPurpose      `json:"purpose"`
	MinRevision uint64                         `json:"min_revision"`
	After       bool                           `json:"after"`
	Hits        uint64                         `json:"hits"`
	Last        load.FenceRequest              `json:"last"`
}
type authorityObservation struct {
	Actions  []load.ActionRequest      `json:"actions"`
	Receipts []load.ActionResult       `json:"receipts"`
	Fences   []load.FenceResult        `json:"fences"`
	Barriers []fenceControlObservation `json:"barriers"`
}

func cloneActionRequest(request load.ActionRequest) load.ActionRequest {
	request.Grant = request.Grant.Clone()
	return request
}
func cloneActionResult(result load.ActionResult) load.ActionResult {
	result.Receipts = slices.Clone(result.Receipts)
	for i := range result.Receipts {
		result.Receipts[i] = result.Receipts[i].Clone()
	}
	return result
}
func (a *application) captureAuthorityAction(request load.ActionRequest) {
	if a.clusterHooks == nil {
		return
	}
	h := a.clusterHooks
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, old := range h.authorityActions {
		if old.From == request.From && old.To == request.To && old.LocalTarget == request.LocalTarget && old.Authorization == request.Authorization && old.Grant.Equal(request.Grant) {
			return
		}
	}
	if len(h.authorityActions) == 128 {
		copy(h.authorityActions, h.authorityActions[1:])
		h.authorityActions[127] = cloneActionRequest(request)
		return
	}
	h.authorityActions = append(h.authorityActions, cloneActionRequest(request))
}
func (a *application) captureAuthorityReceipt(result load.ActionResult) {
	if a.clusterHooks == nil {
		return
	}
	h := a.clusterHooks
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.authorityReceipts) == 128 {
		copy(h.authorityReceipts, h.authorityReceipts[1:])
		h.authorityReceipts[127] = cloneActionResult(result)
		return
	}
	h.authorityReceipts = append(h.authorityReceipts, cloneActionResult(result))
}
func (a *application) captureAuthorityFence(result load.FenceResult) {
	if a.clusterHooks == nil {
		return
	}
	h := a.clusterHooks
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.authorityFences) == 128 {
		copy(h.authorityFences, h.authorityFences[1:])
		h.authorityFences[127] = result
		return
	}
	h.authorityFences = append(h.authorityFences, result)
}
func (a *application) waitAuthorityFence(ctx context.Context, request load.FenceRequest, after bool) error {
	if a.clusterHooks == nil {
		return nil
	}
	h := a.clusterHooks
	h.mu.Lock()
	barrier := h.fenceBarriers[fenceControlKey{request.Resource.GroupID, request.Resource.Kind}]
	if barrier == nil || barrier.identity != request.To || barrier.replicaID != request.ReplicaID || barrier.after != after || barrier.purpose != request.Grant.Purpose || request.Grant.Key.Revision < barrier.minRevision {
		h.mu.Unlock()
		return nil
	}
	barrier.hits++
	barrier.last = request
	barrier.last.Grant = request.Grant.Clone()
	h.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-barrier.release:
		return ctx.Err()
	}
}
func (a *application) authorityObservation(group string) authorityObservation {
	h := a.clusterHooks
	h.mu.Lock()
	defer h.mu.Unlock()
	var view authorityObservation
	for _, request := range h.authorityActions {
		if request.Grant.Key.GroupID == group {
			view.Actions = append(view.Actions, cloneActionRequest(request))
		}
	}
	for _, result := range h.authorityReceipts {
		if result.From.Replica.GroupID == group {
			view.Receipts = append(view.Receipts, cloneActionResult(result))
		}
	}
	for _, result := range h.authorityFences {
		if result.Evidence.Key.GroupID == group {
			view.Fences = append(view.Fences, result)
		}
	}
	for key, barrier := range h.fenceBarriers {
		if key.group != group {
			continue
		}
		last := barrier.last
		last.Grant = last.Grant.Clone()
		view.Barriers = append(view.Barriers, fenceControlObservation{Identity: barrier.identity, ReplicaID: barrier.replicaID, Group: key.group, Kind: key.kind, Purpose: barrier.purpose, MinRevision: barrier.minRevision, After: barrier.after, Hits: barrier.hits, Last: last})
	}
	slices.SortFunc(view.Barriers, func(a, b fenceControlObservation) int {
		return topology.AuthorityResource{Kind: a.Kind}.Compare(topology.AuthorityResource{Kind: b.Kind})
	})
	return view
}
func (a *application) registerAuthorityRoutes(mux *http.ServeMux, protect func(http.HandlerFunc) http.HandlerFunc) {
	a.registerMetaFaultRoutes(mux, protect)
	mutate := func(w http.ResponseWriter, r *http.Request) {
		group, kind := r.PathValue("group"), topology.AuthorityResourceKind(r.PathValue("kind"))
		if topology.ValidateLoadID(group) != nil || kind != topology.AuthorityHost && kind != topology.AuthorityGroup {
			httpError(w, fault.ErrInvalid)
			return
		}
		var body struct {
			ReplicaID   uint64                    `json:"replica_id,omitempty"`
			Identity    topology.Identity         `json:"identity"`
			Purpose     topology.AuthorityPurpose `json:"purpose"`
			MinRevision uint64                    `json:"min_revision"`
			After       bool                      `json:"after"`
		}
		if err := requestBody(w, r, &body); err != nil {
			httpError(w, err)
			return
		}
		if body.Identity != a.node.Identity() {
			httpError(w, fault.ErrIdentityMismatch)
			return
		}
		if body.Purpose == "" {
			body.Purpose = topology.AuthorityFence
		}
		if !slices.Contains([]topology.AuthorityPurpose{topology.AuthorityFence, topology.AuthorityEffect, topology.AuthorityTransfer, topology.AuthorityReadmit}, body.Purpose) {
			httpError(w, fault.ErrInvalid)
			return
		}
		if r.Method == "PUT" && kind == topology.AuthorityGroup {
			group, found := a.node.LoadGroup(group)
			if !found || body.ReplicaID != 0 && body.ReplicaID != group.ReplicaID() {
				httpError(w, fault.ErrIdentityMismatch)
				return
			}
			body.ReplicaID = group.ReplicaID()
		} else if kind == topology.AuthorityHost && body.ReplicaID != 0 {
			httpError(w, fault.ErrInvalid)
			return
		}
		h := a.clusterHooks
		h.mu.Lock()
		defer h.mu.Unlock()
		key := fenceControlKey{group, kind}
		if r.Method == "PUT" {
			if h.fenceBarriers[key] != nil || len(h.fenceBarriers) == 128 {
				httpError(w, fault.ErrOverloaded)
				return
			}
			h.fenceBarriers[key] = &fenceControl{identity: body.Identity, replicaID: body.ReplicaID, release: make(chan struct{}), purpose: body.Purpose, minRevision: body.MinRevision, after: body.After}
		} else {
			barrier := h.fenceBarriers[key]
			if barrier == nil {
				httpError(w, fault.ErrHistoryUnavailable)
				return
			}
			close(barrier.release)
			delete(h.fenceBarriers, key)
		}
		jsonResponse(w, http.StatusOK, map[string]bool{"updated": true})
	}
	mux.HandleFunc("PUT /internal/test/fences/{group}/{kind}", protect(mutate))
	mux.HandleFunc("DELETE /internal/test/fences/{group}/{kind}", protect(mutate))
	mux.HandleFunc("GET /internal/test/authority/{group}", protect(func(w http.ResponseWriter, r *http.Request) {
		if topology.ValidateLoadID(r.PathValue("group")) != nil {
			httpError(w, fault.ErrInvalid)
			return
		}
		jsonResponse(w, http.StatusOK, a.authorityObservation(r.PathValue("group")))
	}))
	// Replay is authenticated fault delivery of an exact captured request. Node
	// still validates the courier, immutable grant, target and installed floor.
	mux.HandleFunc("POST /internal/test/authority/{group}/action", protect(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Identity topology.Identity  `json:"identity"`
			Action   load.ActionRequest `json:"action"`
		}
		if err := requestBody(w, r, &body); err != nil {
			httpError(w, err)
			return
		}
		if body.Identity != a.node.Identity() || body.Action.To.Node != body.Identity || body.Action.To.Replica.GroupID != r.PathValue("group") {
			httpError(w, fault.ErrIdentityMismatch)
			return
		}
		if topology.ValidateLoadID(r.PathValue("group")) != nil {
			httpError(w, fault.ErrInvalid)
			return
		}
		result, err := a.node.ApplyLoadAction(r.Context(), body.Action)
		if err != nil {
			httpError(w, err)
			return
		}
		a.captureAuthorityReceipt(result)
		jsonResponse(w, http.StatusOK, result)
	}))
	mux.HandleFunc("GET /internal/test/revocation/{operator}", protect(func(w http.ResponseWriter, r *http.Request) {
		status, err := a.cluster.ReadAuthority(r.Context(), topology.OperatorID(r.PathValue("operator")))
		if err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, http.StatusOK, status)
	}))
}
