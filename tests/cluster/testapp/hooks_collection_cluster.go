package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
)

type collectionReportControl struct {
	Identity storage.Identity `json:"identity"`
	Held     bool             `json:"held"`
	Hits     uint64           `json:"hits"`
	Receipt  json.RawMessage  `json:"receipt,omitempty"`
	release  chan struct{}
}

type fixtureRetirement struct {
	storage.RetirementStore
	app *application
}

func (s fixtureRetirement) Collect(ctx context.Context, retired storage.RetiredReplica) error {
	h := s.app.clusterHooks
	h.mu.Lock()
	if h.collections == nil {
		h.collections = map[storage.Identity]uint64{}
	}
	// Bounded diagnostic counters; no collection behavior depends on them.
	if _, exists := h.collections[retired.Identity]; exists || len(h.collections) < 128 {
		h.collections[retired.Identity]++
	}
	h.mu.Unlock()
	return s.RetirementStore.Collect(ctx, retired)
}

func (a *application) registerCollectionRoutes(mux *http.ServeMux, protect func(http.HandlerFunc) http.HandlerFunc) {
	mux.HandleFunc("PUT /internal/test/collection-report", protect(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Identity storage.Identity `json:"identity"`
		}
		if err := requestBody(w, r, &body); err != nil {
			httpError(w, err)
			return
		}
		if a.metaProxy == nil || body.Identity.ClusterID != a.node.Identity().ClusterID || body.Identity.NodeName == "" || body.Identity.GroupID == "" || body.Identity.ReplicaID == 0 {
			httpError(w, fault.ErrIdentityMismatch)
			return
		}
		h := a.clusterHooks
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.collectionReport.Held {
			httpError(w, fault.ErrOverloaded)
			return
		}
		h.collectionReport = collectionReportControl{Identity: body.Identity, Held: true, release: make(chan struct{})}
		jsonResponse(w, 200, h.collectionReport)
	}))
	mux.HandleFunc("GET /internal/test/collection-report", protect(func(w http.ResponseWriter, _ *http.Request) {
		h := a.clusterHooks
		h.mu.Lock()
		defer h.mu.Unlock()
		jsonResponse(w, 200, h.collectionReport)
	}))
	mux.HandleFunc("DELETE /internal/test/collection-report", protect(func(w http.ResponseWriter, _ *http.Request) {
		h := a.clusterHooks
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.collectionReport.Held {
			close(h.collectionReport.release)
			h.collectionReport.Held = false
		}
		jsonResponse(w, 200, h.collectionReport)
	}))
	mux.HandleFunc("GET /internal/test/collections/{group}/{replica}", protect(func(w http.ResponseWriter, r *http.Request) {
		replica, err := positiveNumber(r.PathValue("replica"))
		if err != nil {
			httpError(w, err)
			return
		}
		identity := a.node.Identity()
		id := storage.Identity{ClusterID: identity.ClusterID, NodeName: identity.NodeName, GroupID: r.PathValue("group"), ReplicaID: replica}
		h := a.clusterHooks
		h.mu.Lock()
		defer h.mu.Unlock()
		jsonResponse(w, 200, map[string]any{"identity": id, "calls": h.collections[id]})
	}))
}

// Called only after the proxy authenticates the existing MetaControl envelope.
// Holding a report cannot block Raft or another identity's cleanup reporting.
func (a *application) waitCollectionReport(ctx context.Context, frame []byte) error {
	data, err := proxyBytesField(frame, 2)
	if err != nil {
		return err
	}
	var request struct {
		Kind    string          `json:"kind"`
		Receipt json.RawMessage `json:"receipt"`
	}
	if json.Unmarshal(data, &request) != nil || request.Kind != "report_collection" {
		return nil
	}
	var receipt struct {
		Identity storage.Identity `json:"identity"`
	}
	if err := json.Unmarshal(request.Receipt, &receipt); err != nil {
		return err
	}
	h := a.clusterHooks
	h.mu.Lock()
	control := &h.collectionReport
	if control.Identity != receipt.Identity {
		h.mu.Unlock()
		return nil
	}
	control.Hits++
	control.Receipt = bytes.Clone(request.Receipt)
	release, held := control.release, control.Held
	h.mu.Unlock()
	if !held {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-release:
		return ctx.Err()
	}
}
