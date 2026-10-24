package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// This caller-owned materialized projection deliberately has no role in Raft
// persistence or admission. Its worker and files exist only in the test image.
type projectionControl struct {
	release chan struct{}
	done    chan struct{}
	metrics projectionMetrics
}
type projectionMetrics struct {
	Attempts uint64 `json:"attempts"`
	Writes   uint64 `json:"writes"`
	Failures uint64 `json:"failures"`
	Active   uint64 `json:"active"`
	Blocked  bool   `json:"blocked"`
	Waiting  bool   `json:"waiting"`
	Stopped  bool   `json:"stopped"`
}
type projectionFile struct {
	Identity storage.Identity `json:"identity"`
	State    machineState     `json:"state"`
}

type projectionObservation struct {
	Node    string `json:"node"`
	Group   string `json:"group"`
	Replica uint64 `json:"replica_id"`
	projectionMetrics
	State machineState `json:"state"`
}

func (a *application) projectionPath(key walKey) string {
	return filepath.Join(a.cfg.DataDir, "projection", key.group+"-"+strconv.FormatUint(key.replica, 10)+".json")
}
func (a *application) projectionLoop(key walKey, machine *kvMachine, running func() bool, control *projectionControl) {
	h := a.clusterHooks
	defer func() {
		h.mu.Lock()
		control.metrics.Stopped = true
		if control.done != nil {
			close(control.done)
		}
		h.mu.Unlock()
	}()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for running() && a.workCtx.Err() == nil {
		data, err := machine.Snapshot(a.workCtx)
		if err != nil {
			return
		}
		h.mu.Lock()
		control.metrics.Attempts++
		control.metrics.Active++
		release := control.release
		control.metrics.Waiting = release != nil
		h.mu.Unlock()
		if release != nil {
			// Group retirement cancels this caller's pending write too. No
			// projection worker is left waiting on a retired local handle.
			for err == nil {
				select {
				case <-release:
					goto released
				case <-a.workCtx.Done():
					err = a.workCtx.Err()
				case <-ticker.C:
					if !running() {
						err = fault.ErrStopped
					}
				}
			}
		}
	released:
		if err == nil {
			var state machineState
			if err = json.Unmarshal(data, &state); err == nil {
				data, err = json.Marshal(projectionFile{Identity: a.projectionIdentity(key), State: state})
				if err == nil {
					err = writeProjection(a.workCtx, a.projectionPath(key), data)
				}
			}
		}
		h.mu.Lock()
		control.metrics.Active--
		control.metrics.Waiting = false
		if err == nil {
			control.metrics.Writes++
		} else {
			control.metrics.Failures++
		}
		h.mu.Unlock()
		select {
		case <-a.workCtx.Done():
			return
		case <-ticker.C:
		}
	}
}
func writeProjection(ctx context.Context, filename string, data []byte) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(filename), 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(filename), ".projection-*")
	if err != nil {
		return err
	}
	defer func() { _ = f.Close(); _ = os.Remove(f.Name()) }()
	if err := f.Chmod(0o600); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), filename); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(filename))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func (a *application) projectionView(key walKey) (projectionObservation, error) {
	view := projectionObservation{Node: a.cfg.NodeName, Group: key.group, Replica: key.replica}
	h := a.clusterHooks
	h.mu.Lock()
	control := h.projections[key]
	if control != nil {
		view.projectionMetrics = control.metrics
	}
	h.mu.Unlock()
	if control == nil {
		return view, fault.ErrReplicaNotReady
	}
	data, err := os.ReadFile(a.projectionPath(key))
	if os.IsNotExist(err) {
		return view, nil
	}
	if err != nil {
		return view, err
	}
	var saved projectionFile
	if err := decodeJSON(data, &saved); err != nil {
		return view, err
	}
	if saved.Identity != a.projectionIdentity(key) {
		return view, fault.ErrIdentityMismatch
	}
	view.State = saved.State
	return view, nil
}
func (a *application) registerProjectionRoutes(mux *http.ServeMux, protect func(http.HandlerFunc) http.HandlerFunc) {
	handler := protect(func(w http.ResponseWriter, r *http.Request) {
		group := r.PathValue("group")
		if err := topology.ValidateLoadID(group); err != nil {
			httpError(w, err)
			return
		}
		replica, err := strconv.ParseUint(r.PathValue("replica"), 10, 64)
		if err != nil || replica == 0 {
			httpError(w, fault.ErrInvalid)
			return
		}
		g, found := a.currentNode().LoadGroup(group)
		if !found || g.ReplicaID() != replica {
			httpError(w, fault.ErrIdentityMismatch)
			return
		}
		key := walKey{group, replica}
		h := a.clusterHooks
		if r.Method == "PUT" {
			var body struct {
				Stall bool `json:"stall"`
			}
			if err := requestBody(w, r, &body); err != nil {
				httpError(w, err)
				return
			}
			a.startMu.Lock()
			defer a.startMu.Unlock()
			select {
			case <-a.closing:
				httpError(w, fault.ErrStopped)
				return
			default:
			}
			local := a.local(group)
			if local == nil || local.group != g {
				httpError(w, fault.ErrIdentityMismatch)
				return
			}
			h.mu.Lock()
			control := h.projections[key]
			if control == nil {
				if len(h.projections) == 512 {
					h.mu.Unlock()
					httpError(w, fault.ErrOverloaded)
					return
				}
				control = &projectionControl{done: make(chan struct{})}
				h.projections[key] = control
				a.workWG.Go(func() { a.projectionLoop(key, local.machine, g.Running, control) })
			}
			if control.metrics.Stopped || control.release != nil {
				h.mu.Unlock()
				httpError(w, fault.ErrInvalid)
				return
			}
			if body.Stall {
				control.release = make(chan struct{})
				control.metrics.Blocked = true
			}
			h.mu.Unlock()
		} else if r.Method == "DELETE" {
			h.mu.Lock()
			control := h.projections[key]
			if control == nil {
				h.mu.Unlock()
				httpError(w, fault.ErrReplicaNotReady)
				return
			}
			if control.release != nil {
				close(control.release)
				control.release = nil
			}
			control.metrics.Blocked = false
			h.mu.Unlock()
		}
		view, err := a.projectionView(key)
		if err != nil {
			httpError(w, err)
			return
		}
		jsonResponse(w, http.StatusOK, view)
	})
	for _, method := range []string{"GET", "PUT", "DELETE"} {
		mux.HandleFunc(method+" /internal/test/projection/{group}/{replica}", handler)
	}
}

func (a *application) joinProjection(ctx context.Context, key walKey) error {
	h := a.clusterHooks
	h.mu.Lock()
	var done <-chan struct{}
	if control := h.projections[key]; control != nil {
		done = control.done
	}
	h.mu.Unlock()
	if done == nil {
		return ctx.Err()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *application) projectionIdentity(key walKey) storage.Identity {
	return storage.Identity{NodeName: a.cfg.NodeName, ClusterID: a.cfg.ClusterID, GroupID: key.group, ReplicaID: key.replica}
}
