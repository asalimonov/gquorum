package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	gquorum "github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func clusterRequestTimeout(r *http.Request) time.Duration {
	if r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/internal/test/propose/") {
		return time.Minute
	}
	return 10 * time.Second
}

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
	refuse  bool
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
	if b.refuse {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fault.New(fault.UnsafeMembership, "caller refuses deletion while application data is occupied", nil)
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
	Group  string                  `json:"group"`
	Kind   topology.StepKind       `json:"kind"`
	Hits   uint64                  `json:"hits"`
	Last   topology.AuthorizedStep `json:"last"`
	Refuse bool                    `json:"refuse"`
}
type localObservation struct {
	Identity          topology.Identity          `json:"identity"`
	Found             bool                       `json:"found"`
	ReplicaID         uint64                     `json:"replica_id"`
	Status            topology.GroupStatus       `json:"status"`
	Storage           storage.Stats              `json:"storage"`
	Values            map[string]string          `json:"values"`
	ReceivedSnapshots uint64                     `json:"received_snapshots"`
	WAL               walCounters                `json:"wal"`
	WALStallNS        int64                      `json:"wal_stall_ns"`
	Proposals         load.ProposalStats         `json:"proposals"`
	Parked            *gquorum.ParkedLoad        `json:"parked,omitempty"`
	Ledger            *metaservice.ReplicaRecord `json:"ledger,omitempty"`
	ServeOverride     int                        `json:"serve_override"`
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
	a.registerResourceRoutes(mux, protect)
	a.registerWALRoutes(mux, protect)
	a.registerProjectionRoutes(mux, protect)

	mux.HandleFunc("POST /internal/test/serve-override", protect(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Identity topology.Identity `json:"identity"`
			Minimum  int               `json:"minimum"`
		}
		if err := requestBody(w, r, &body); err != nil {
			httpError(w, err)
			return
		}
		if body.Identity != a.meta.Identity() {
			httpError(w, fault.ErrIdentityMismatch)
			return
		}
		if err := a.meta.SetServeOverride(r.Context(), body.Minimum); err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, http.StatusOK, map[string]any{"identity": a.meta.Identity(), "minimum": a.meta.ServeOverride()})
	}))
	mux.HandleFunc("POST /internal/test/lose-meta/{member}", protect(func(w http.ResponseWriter, r *http.Request) {
		own := a.meta.Identity()
		if fmt.Sprint(own.MemberID) != r.PathValue("member") || !a.meta.OwnSessionLive() {
			httpError(w, fault.ErrIdentityMismatch)
			return
		}
		marker, err := os.ReadFile(filepath.Join(a.cfg.DataDir, "established.json"))
		if err != nil || len(marker) == 0 {
			httpError(w, fault.ErrReplicaNotReady)
			return
		}
		// An open bbolt descriptor survives rename. The next process sees no Raft
		// store and must obtain an approved fresh identity from the healthy majority.
		lost := filepath.Join(a.cfg.DataDir, "meta.bolt.lost")
		if _, err := os.Lstat(lost); !errors.Is(err, os.ErrNotExist) {
			httpError(w, fault.ErrIdentityMismatch)
			return
		}
		if err := os.Rename(filepath.Join(a.cfg.DataDir, "meta.bolt"), lost); err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, http.StatusOK, own)
	}))
	// Retain the node-local boot ledger while removing the exact replica's
	// usable store. Quarantine preserves evidence without making it reopenable.
	mux.HandleFunc("POST /internal/test/lose-store/{group}/{replica}", protect(func(w http.ResponseWriter, r *http.Request) {
		group := r.PathValue("group")
		if err := topology.ValidateLoadID(group); err != nil {
			httpError(w, err)
			return
		}
		if err := a.takeLocal(r.Context()); err != nil {
			httpError(w, err)
			return
		}
		defer func() { <-a.localGate }()
		g, found := a.runtime.Load(group)
		if !found || fmt.Sprint(g.ReplicaID()) != r.PathValue("replica") {
			httpError(w, fault.ErrIdentityMismatch)
			return
		}
		probe, err := a.store.Probe(r.Context(), group)
		want := storage.Identity{NodeName: a.cfg.NodeName, ClusterID: a.cfg.ClusterID, GroupID: group, ReplicaID: g.ReplicaID()}
		if err == nil && (!probe.Exists || probe.Identity != want) {
			err = fault.ErrIdentityMismatch
		}
		if err == nil {
			err = a.runtime.StopLoad(r.Context(), group, want.ReplicaID)
		}
		if err != nil {
			httpError(w, err)
			return
		}
		recovered, err := a.store.Load(r.Context(), group)
		if err != nil {
			httpError(w, err)
			return
		}
		a.mu.Lock()
		delete(a.locals, group)
		a.mu.Unlock()
		if _, err := a.store.Quarantine(r.Context(), group, want.ReplicaID); err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, http.StatusOK, map[string]any{"identity": want, "hard_state": recovered.HardState})
	}))
	mux.HandleFunc("POST /internal/test/propose/{group}/{replica}", protect(func(w http.ResponseWriter, r *http.Request) {
		group := r.PathValue("group")
		if err := topology.ValidateLoadID(group); err != nil {
			httpError(w, err)
			return
		}
		g, found := a.runtime.Load(group)
		if !found || fmt.Sprint(g.ReplicaID()) != r.PathValue("replica") {
			httpError(w, fault.ErrIdentityMismatch)
			return
		}
		var command putCommand
		if err := requestBody(w, r, &command); err != nil {
			httpError(w, err)
			return
		}
		data, err := json.Marshal(command)
		if err == nil {
			var result load.Result
			result, err = g.Propose(r.Context(), load.Proposal{RequestID: command.RequestID, Data: data})
			if err == nil && result.Code != "ok" {
				err = fault.New(fault.Code(result.Code), "proposal rejected", nil)
			}
		}
		if err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, http.StatusOK, map[string]bool{"applied": true})
	}))
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
		mode := r.URL.Query().Get("refuse")
		if mode != "" && mode != "true" || mode == "true" && kind != topology.StepStopReplica {
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
		h.barriers[key] = &effectBarrier{release: make(chan struct{}), refuse: mode == "true"}
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
			barriers = append(barriers, barrierObservation{key.group, key.kind, b.hits, b.last, b.refuse})
		}
		receipts := slices.Clone(h.receipts)
		h.mu.Unlock()
		jsonResponse(w, http.StatusOK, map[string]any{"barriers": barriers, "receipts": receipts})
	}))
	mux.HandleFunc("GET /internal/test/local/{group}", protect(func(w http.ResponseWriter, r *http.Request) {
		group := r.PathValue("group")
		if group == topology.MetaGroupID {
			status := a.meta.LocalStatus()
			jsonResponse(w, http.StatusOK, localObservation{Identity: a.meta.Identity(), Found: status.Found, ReplicaID: a.meta.Identity().MemberID, Status: status, ServeOverride: a.meta.ServeOverride()})
			return
		}
		if err := topology.ValidateLoadID(group); err != nil {
			httpError(w, err)
			return
		}
		observation := localObservation{Identity: a.meta.Identity()}
		if park, found := a.runtime.ParkedLoads()[group]; found {
			observation.Parked = &park
		}
		if record, found, err := a.meta.LookupLoadReplica(r.Context(), group); err != nil {
			httpError(w, err)
			return
		} else if found {
			observation.Ledger = &record
		}
		a.clusterHooks.mu.Lock()
		observation.ReceivedSnapshots = a.clusterHooks.snapshots[group]
		a.clusterHooks.mu.Unlock()
		a.mu.RLock()
		local := a.locals[group]
		a.mu.RUnlock()
		if local != nil {
			observation.Found, observation.ReplicaID = true, local.group.ReplicaID()
			observation.Status = local.group.Status()
			observation.Proposals = local.group.ProposalStats()
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
