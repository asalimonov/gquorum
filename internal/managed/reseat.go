package managed

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

const (
	reseatMinBackoff = 200 * time.Millisecond
	reseatMaxBackoff = 5 * time.Second
)

// startOwnReseat handles an in-memory replica that lost its state with the
// previous process. An established voter is replaced under a fresh identity by
// the existing operator reseat of this node's own replica. At most one loop
// runs per identity; it belongs to the host work lifetime.
func (h *Host) startOwnReseat(slot *replica, scope metaengine.LoadScope) {
	id := slot.identity
	h.mu.Lock()
	if h.ownReseats[id] || h.quiescing || h.stopped || h.operationContext == nil {
		h.mu.Unlock()
		return
	}
	h.ownReseats[id] = true
	run := h.cfg.ReseatOwn != nil && scope.Established && scope.Replica.Role == topology.ReplicaVoter
	ctx := h.operationContext
	if run {
		h.reseatWG.Add(1)
	}
	h.mu.Unlock()
	if !run {
		// TODO(not planned): reseat in-memory learners and replicas that
		// restart before joining their group.
		h.cfg.Logger.Warn("NotImplemented: in-memory replica lost its state and is not reseated automatically", "group",
			id.GroupID, "replica", id.ReplicaID, "role", scope.Replica.Role, "established", scope.Established)
		return
	}
	go func() {
		defer h.reseatWG.Done()
		h.ownReseatLoop(ctx, id)
	}()
}

// ownReseatLoop requests an exact predecessor replacement only when this
// group is unowned and its approved/proven voter assignments agree. TargetVersion
// supplies CAS; independent convergence never becomes a global startup gate.
func (h *Host) ownReseatLoop(ctx context.Context, id storage.Identity) {
	defer func() {
		h.mu.Lock()
		delete(h.ownReseats, id)
		h.mu.Unlock()
	}()
	backoff := reseatMinBackoff
	for {
		progress, err := h.cfg.Progress(ctx)
		if err == nil {
			row := localGroupProgress(progress, id.GroupID)
			replica := localProgressReplica(row.Approved, id)
			switch {
			case ctx.Err() != nil:
			case slices.Contains(h.Held(), id):
				return
			case replica == nil:
				h.clearPark(id)
				return
			case replica.Role != topology.ReplicaVoter:
				return
			case row.ConvergedVersion != 0 && row.Converged != nil && row.OperationID == "" && !row.Deleting &&
				reflect.DeepEqual(row.Converged, row.Approved):
				if err = h.cfg.ReseatOwn(ctx, id); err == nil {
					backoff = reseatMinBackoff // committed; approved plan replaces this exact identity
				}
			default: // Retain the parked replica while its own required scope is pending.
			}
		}
		if err != nil && ctx.Err() == nil && !expectedReseatRefusal(err) {
			h.cfg.Logger.Warn("in-memory replica reseat failed", "group", id.GroupID, "replica", id.ReplicaID, "error",
				err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, reseatMaxBackoff)
	}
}

// expectedReseatRefusal covers unconverged topology, a running operator, a lost
// CAS race and transient unavailability; the loop simply retries.
func expectedReseatRefusal(err error) bool {
	return fault.Retryable(err) || errors.Is(err, fault.ErrOperationAuthorization) ||
		errors.Is(err, fault.ErrVersionConflict)
}

func localGroupProgress(page topology.ProgressPage, id string) topology.GroupProgress {
	for _, row := range page.Groups {
		if row.GroupID == id {
			return row
		}
	}
	return topology.GroupProgress{GroupID: id}
}

func localProgressReplica(group *topology.Group, id storage.Identity) *topology.Replica {
	if group != nil && group.ID == id.GroupID {
		for i := range group.Replicas {
			r := &group.Replicas[i]
			if r.NodeName == id.NodeName && r.ReplicaID == id.ReplicaID {
				return r
			}
		}
	}
	return nil
}

func (h *Host) clearPark(id storage.Identity) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if parked, found := h.parked[id.GroupID]; found && parked.Identity == id {
		delete(h.parked, id.GroupID)
	}
}
