//go:build clustertest

package main

import (
	"context"
	"crypto/subtle"
	"net/http"
	"slices"
	"sync"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/meta"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// These controls exist only in the process-test image. They use the caller's
// lifecycle/transport; the library neither defines nor serves this protocol.
type effectKey struct {
	group string
	kind  topology.StepKind
}
type effectBarrier struct {
	release chan struct{}
	hits    uint64
	last    topology.AuthorizedStep
}
type clusterHooks struct {
	mu          sync.Mutex
	barriers    map[effectKey]*effectBarrier
	receipts    []topology.AuthorizedStep
	snapshots   map[string]uint64
	wals        map[walKey]*walControl
	projections map[walKey]*projectionControl
}

func newClusterHooks() *clusterHooks {
	return &clusterHooks{barriers: map[effectKey]*effectBarrier{}, snapshots: map[string]uint64{}, wals: map[walKey]*walControl{}, projections: map[walKey]*projectionControl{}}
}
func (a *application) beforeClusterEffect(ctx context.Context, approved topology.AuthorizedStep) error {
	if a.clusterHooks == nil {
		return nil
	}
	h := a.clusterHooks
	h.mu.Lock()
	b := h.barriers[effectKey{approved.GroupID, approved.Kind}]
	if b != nil {
		b.hits++
		b.last = approved
	}
	h.mu.Unlock()
	if b == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.release:
		return nil
	}
}
func (a *application) afterClusterEffect(approved topology.AuthorizedStep, err error) {
	if a.clusterHooks == nil || err != nil {
		return
	}
	h := a.clusterHooks
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.receipts) == 512 {
		copy(h.receipts, h.receipts[1:])
		h.receipts = h.receipts[:511]
	}
	h.receipts = append(h.receipts, approved)
}
func (a *application) receivedClusterSnapshot(group string) {
	if a.clusterHooks == nil {
		return
	}
	h := a.clusterHooks
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.snapshots[group]; exists || len(h.snapshots) < 512 {
		h.snapshots[group]++
	}
}

type barrierObservation struct {
	Group string                  `json:"group"`
	Kind  topology.StepKind       `json:"kind"`
	Hits  uint64                  `json:"hits"`
	Last  topology.AuthorizedStep `json:"last"`
}
type localObservation struct {
	Identity          meta.Identity        `json:"identity"`
	Found             bool                 `json:"found"`
	ReplicaID         uint64               `json:"replica_id"`
	Status            topology.GroupStatus `json:"status"`
	Storage           storage.Stats        `json:"storage"`
	Values            map[string]string    `json:"values"`
	ReceivedSnapshots uint64               `json:"received_snapshots"`
	WAL               walCounters          `json:"wal"`
	WALStallNS        int64                `json:"wal_stall_ns"`
}

func (a *application) registerClusterRoutes(mux *http.ServeMux) {
	if a.clusterHooks == nil {
		return
	}
	protect := func(handler http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Gquorum-Test-Credential")), []byte(a.cfg.Credential)) != 1 || a.cfg.Credential == "" {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			handler(w, r)
		}
	}
	a.registerWALRoutes(mux, protect)
	a.registerProjectionRoutes(mux, protect)
	mux.HandleFunc("PUT /internal/test/barriers/{group}/{kind}", protect(func(w http.ResponseWriter, r *http.Request) {
		group, kind := r.PathValue("group"), topology.StepKind(r.PathValue("kind"))
		if err := topology.ValidateLoadID(group); err != nil {
			httpError(w, err)
			return
		}
		if !slices.Contains([]topology.StepKind{topology.StepPrepareReplica, topology.StepPromoteVoter, topology.StepStopReplica, topology.StepCleanupReplicaState, topology.StepDeleteGroupState}, kind) {
			httpError(w, fault.ErrInvalid)
			return
		}
		h := a.clusterHooks
		h.mu.Lock()
		defer h.mu.Unlock()
		key := effectKey{group, kind}
		if h.barriers[key] != nil || len(h.barriers) == 128 {
			httpError(w, fault.ErrOverloaded)
			return
		}
		h.barriers[key] = &effectBarrier{release: make(chan struct{})}
		jsonResponse(w, http.StatusOK, map[string]bool{"armed": true})
	}))
	mux.HandleFunc("DELETE /internal/test/barriers/{group}/{kind}", protect(func(w http.ResponseWriter, r *http.Request) {
		h := a.clusterHooks
		h.mu.Lock()
		defer h.mu.Unlock()
		key := effectKey{r.PathValue("group"), topology.StepKind(r.PathValue("kind"))}
		b := h.barriers[key]
		if b == nil {
			httpError(w, fault.ErrHistoryUnavailable)
			return
		}
		close(b.release)
		delete(h.barriers, key)
		jsonResponse(w, http.StatusOK, map[string]bool{"released": true})
	}))
	mux.HandleFunc("GET /internal/test/controls", protect(func(w http.ResponseWriter, r *http.Request) {
		h := a.clusterHooks
		h.mu.Lock()
		barriers := make([]barrierObservation, 0, len(h.barriers))
		for key, b := range h.barriers {
			barriers = append(barriers, barrierObservation{key.group, key.kind, b.hits, b.last})
		}
		receipts := slices.Clone(h.receipts)
		h.mu.Unlock()
		jsonResponse(w, http.StatusOK, map[string]any{"barriers": barriers, "receipts": receipts})
	}))
	mux.HandleFunc("GET /internal/test/local/{group}", protect(func(w http.ResponseWriter, r *http.Request) {
		group := r.PathValue("group")
		if group == topology.MetaGroupID {
			status := a.meta.LocalStatus()
			jsonResponse(w, http.StatusOK, localObservation{Identity: a.meta.Identity(), Found: status.Found, ReplicaID: a.meta.Identity().MemberID, Status: status})
			return
		}
		if err := topology.ValidateLoadID(group); err != nil {
			httpError(w, err)
			return
		}
		observation := localObservation{Identity: a.meta.Identity()}
		a.clusterHooks.mu.Lock()
		observation.ReceivedSnapshots = a.clusterHooks.snapshots[group]
		a.clusterHooks.mu.Unlock()
		a.mu.RLock()
		local := a.locals[group]
		a.mu.RUnlock()
		if local != nil {
			observation.Found, observation.ReplicaID = true, local.group.ReplicaID()
			observation.Status = local.group.Status()
			observation.Values = local.machine.values()
			observation.WAL = a.clusterHooks.walMetrics(walKey{group, local.group.ReplicaID()})
			observation.WALStallNS = int64(local.group.WALStall())
			stats, err := a.store.Stats(r.Context(), group)
			if err != nil {
				httpError(w, err)
				return
			}
			observation.Storage = stats
		}
		jsonResponse(w, http.StatusOK, observation)
	}))
	mux.HandleFunc("POST /internal/test/local/{group}/snapshot", protect(func(w http.ResponseWriter, r *http.Request) {
		g, found := a.runtime.Load(r.PathValue("group"))
		if !found {
			httpError(w, fault.ErrReplicaNotReady)
			return
		}
		if err := g.Snapshot(r.Context()); err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, http.StatusOK, map[string]bool{"snapshotted": true})
	}))
}
