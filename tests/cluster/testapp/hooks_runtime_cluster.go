package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/topology"
)

// Runtime controls belong to the Docker fixture, not the library's public HTTP API.
// Every route requires the lease credential and calls the public Node/Cluster API.
type fixtureScheduler struct {
	reason string
	calls  atomic.Uint64
}

func (s *fixtureScheduler) Schedule(ctx context.Context, _ scheduling.ScheduleInput) (scheduling.ScheduleDecision, error) {
	if err := ctx.Err(); err != nil {
		return scheduling.ScheduleDecision{}, err
	}
	s.calls.Add(1)
	return scheduling.ScheduleDecision{Reason: s.reason}, nil
}

type runtimeHooks struct {
	mu           sync.Mutex
	stopped      map[load.ReplicaRef]*load.Group
	removalCalls atomic.Uint64
	factories    atomic.Uint64
}

func (a *application) registerRuntimeRoutes(mux *http.ServeMux, protect func(http.HandlerFunc) http.HandlerFunc) {
	mux.HandleFunc("GET /internal/test/admission-pin", protect(func(w http.ResponseWriter, r *http.Request) {
		if a.cfg.AdmissionPinPhase == "" {
			httpError(w, fault.ErrInvalid)
			return
		}
		pin, err := gquorum.LoadStoredIdentity(a.cfg.DataDir)
		if err != nil {
			httpError(w, err)
			return
		}
		id := a.currentNode().Identity()
		if !pin.Found || pin.Identity.NodeName != id.NodeName || pin.Identity.ClusterID != id.ClusterID {
			httpError(w, fault.ErrIdentityMismatch)
			return
		}
		// The pin driver never starts Node; its constructed identity still has
		// member zero. Read the actual accepted disk member with this process ID.
		id.MemberID = pin.Identity.MemberID
		authority, err := authoritystore.Open(filepath.Join(a.cfg.DataDir, "_authority.bolt"), id,
			authoritystore.RetainedEvidence)
		if err != nil {
			httpError(w, err)
			return
		}
		acquired, err := authority.MetadataAcquired(r.Context())
		identity := authority.Identity()
		if err = errors.Join(err, authority.Close()); err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, 200, map[string]any{"identity": pin.Identity, "found": pin.Found, "instance_id": a.cfg.InstanceID, "phase": a.cfg.AdmissionPinPhase, "authority_identity": identity, "metadata_acquired": acquired})
	}))
	mux.HandleFunc("GET /internal/test/runtime", protect(func(w http.ResponseWriter, _ *http.Request) {
		calls := uint64(0)
		if a.callerScheduler != nil {
			calls = a.callerScheduler.calls.Load()
		}
		jsonResponse(w, 200, map[string]any{"status": a.currentNode().Status(), "scheduler_calls": calls, "removal_calls": a.clusterHooks.runtime.removalCalls.Load(), "factories": a.clusterHooks.runtime.factories.Load()})
	}))
	mux.HandleFunc("PUT /internal/test/scheduling", protect(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Enabled bool `json:"enabled"`
		}
		if err := requestBody(w, r, &body); err != nil {
			httpError(w, err)
			return
		}
		if err := a.currentNode().SetSchedulingEnabled(r.Context(), body.Enabled); err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, 200, a.currentNode().SchedulingStatus())
	}))
	mux.HandleFunc("POST /internal/test/session/{action}", protect(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ID  string `json:"id"`
			Key string `json:"key"`
		}
		if err := requestBody(w, r, &body); err != nil {
			httpError(w, err)
			return
		}
		var value any
		var err error
		switch r.PathValue("action") {
		case "activate":
			value, err = a.currentNode().ActivateSession(r.Context())
		case "deactivate":
			err = a.currentNode().DeactivateSession(r.Context(), body.ID)
		case "keepalive":
			err = a.currentNode().KeepAlive(r.Context(), body.ID)
		case "acquire":
			err = a.currentCluster().AcquireLock(r.Context(), body.Key, body.ID)
		case "holder":
			holder, readErr := a.currentCluster().LockHolder(r.Context(), body.Key)
			err = readErr
			value = map[string]any{"session": holder.Session, "found": holder.Held}
		default:
			err = fault.ErrInvalid
		}
		if err != nil {
			httpError(w, err)
			return
		}
		if value == nil {
			value = map[string]bool{"ok": true}
		}
		jsonResponse(w, 200, value)
	}))
	mux.HandleFunc("POST /internal/test/replica/{action}", protect(func(w http.ResponseWriter, r *http.Request) {
		var ref load.ReplicaRef
		if err := requestBody(w, r, &ref); err != nil {
			httpError(w, err)
			return
		}
		h := &a.clusterHooks.runtime
		h.mu.Lock()
		defer h.mu.Unlock()
		switch r.PathValue("action") {
		case "stop":
			old, found := a.currentNode().LoadGroup(ref.GroupID)
			if !found || old.ReplicaID() != ref.ReplicaID {
				httpError(w, fault.ErrIdentityMismatch)
				return
			}
			if len(h.stopped) >= 128 {
				httpError(w, fault.ErrOverloaded)
				return
			}
			if err := a.currentNode().StopLoadGroup(r.Context(), ref); err != nil {
				httpError(w, err)
				return
			}
			h.stopped[ref] = old
		case "start":
			old := h.stopped[ref]
			g, err := a.currentNode().StartLoadGroup(r.Context(), ref)
			if err != nil {
				httpError(w, err)
				return
			}
			if old != nil {
				if g == old {
					httpError(w, fault.ErrIdentityMismatch)
					return
				}
				_, err = old.Propose(r.Context(), load.Proposal{RequestID: "stale-handle", Data: []byte("stale")})
				if !errors.Is(err, fault.ErrStopped) {
					httpError(w, fault.New(fault.Invalid, "stopped handle redirected to successor", err))
					return
				}
				delete(h.stopped, ref)
			}
		default:
			httpError(w, fault.ErrInvalid)
			return
		}
		jsonResponse(w, 200, map[string]bool{"ok": true})
	}))
	mux.HandleFunc("POST /internal/test/action", protect(func(w http.ResponseWriter, r *http.Request) {
		var req load.ActionRequest
		if err := requestBody(w, r, &req); err != nil {
			httpError(w, err)
			return
		}
		if req.From != a.currentNode().Identity() || req.To.Node != a.currentNode().Identity() {
			httpError(w, fault.ErrIdentityMismatch)
			return
		}
		result, err := a.currentNode().ApplyLoadAction(r.Context(), req)
		if err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, 200, result)
	}))
	mux.HandleFunc("GET /internal/test/watch/{view}", protect(func(w http.ResponseWriter, r *http.Request) {
		updates, err := a.currentCluster().Watch(r.Context(), topology.View(r.PathValue("view")))
		if err != nil {
			httpError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		controller := http.NewResponseController(w)
		for {
			select {
			case <-r.Context().Done():
				return
			case update, ok := <-updates:
				if !ok {
					return
				}
				if err := controller.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
					return
				}
				if err := json.NewEncoder(w).Encode(update); err != nil {
					return
				}
				if err := controller.Flush(); err != nil {
					return
				}
			}
		}
	}))
}
