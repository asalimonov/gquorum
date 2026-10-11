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
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/policy"
	"github.com/asalimonov/gquorum/topology"
)

// Runtime controls belong to the Docker fixture, not the library's public HTTP API.
// Every route requires the lease credential and calls the public Node/Cluster API.
type fixtureScheduler struct {
	reason string
	calls  atomic.Uint64
}

func (s *fixtureScheduler) Schedule(ctx context.Context, _ policy.ScheduleInput) (policy.ScheduleDecision, error) {
	if err := ctx.Err(); err != nil {
		return policy.ScheduleDecision{}, err
	}
	s.calls.Add(1)
	return policy.ScheduleDecision{Reason: s.reason}, nil
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
		pin, found, err := gquorum.LoadStoredIdentity(filepath.Join(a.cfg.DataDir, "meta.bolt"))
		if err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, 200, map[string]any{"identity": pin, "found": found, "instance_id": a.cfg.InstanceID, "phase": a.cfg.AdmissionPinPhase})
	}))
	mux.HandleFunc("GET /internal/test/runtime", protect(func(w http.ResponseWriter, r *http.Request) {
		calls := uint64(0)
		if a.callerScheduler != nil {
			calls = a.callerScheduler.calls.Load()
		}
		jsonResponse(w, 200, map[string]any{"status": a.node.Status(), "scheduler_calls": calls, "removal_calls": a.clusterHooks.runtime.removalCalls.Load(), "factories": a.clusterHooks.runtime.factories.Load()})
	}))
	mux.HandleFunc("PUT /internal/test/scheduling", protect(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Enabled bool `json:"enabled"`
		}
		if err := requestBody(w, r, &body); err != nil {
			httpError(w, err)
			return
		}
		if err := a.node.SetSchedulingEnabled(r.Context(), body.Enabled); err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, 200, a.node.SchedulingStatus())
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
			value, err = a.node.ActivateSession(r.Context())
		case "deactivate":
			err = a.node.DeactivateSession(r.Context(), body.ID)
		case "keepalive":
			err = a.node.KeepAlive(r.Context(), body.ID)
		case "acquire":
			err = a.cluster.AcquireLock(r.Context(), body.Key, body.ID)
		case "holder":
			var session topology.Session
			var found bool
			session, found, err = a.cluster.LockHolder(r.Context(), body.Key)
			value = map[string]any{"session": session, "found": found}
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
			old, found := a.node.LoadGroup(ref.GroupID)
			if !found || old.ReplicaID() != ref.ReplicaID {
				httpError(w, fault.ErrIdentityMismatch)
				return
			}
			if len(h.stopped) >= 128 {
				httpError(w, fault.ErrOverloaded)
				return
			}
			if err := a.node.StopLoadGroup(r.Context(), ref); err != nil {
				httpError(w, err)
				return
			}
			h.stopped[ref] = old
		case "start":
			old := h.stopped[ref]
			g, err := a.node.StartLoadGroup(r.Context(), ref)
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
		if req.From != a.node.Identity() || req.To.Node != a.node.Identity() {
			httpError(w, fault.ErrIdentityMismatch)
			return
		}
		result, err := a.node.ApplyLoadAction(r.Context(), req)
		if err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, 200, result)
	}))
	mux.HandleFunc("GET /internal/test/watch/{view}", protect(func(w http.ResponseWriter, r *http.Request) {
		updates, err := a.cluster.Watch(r.Context(), topology.View(r.PathValue("view")))
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
