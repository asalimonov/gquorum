package managed

import (
	"context"
	"errors"
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

// ownReseatLoop asks for a reseat while the parked identity is in Active,
// pinned
// to the Active version it observed so a completed repair is never repeated on
// the successor. It waits while the identity is only approved, and ends once
// the identity is in neither Active nor Target, or once it is approved as a
// learner (for example after the voter configuration demoted it), which
// preflight
// then reports as not reseated.
func (h *Host) ownReseatLoop(ctx context.Context, id storage.Identity) {
	defer func() {
		h.mu.Lock()
		delete(h.ownReseats, id)
		h.mu.Unlock()
	}()
	backoff := reseatMinBackoff
	for {
		active, err := h.cfg.Active(ctx)
		if err == nil {
			scope, scopeErr := h.cfg.Scope(ctx, id)
			switch {
			case ctx.Err() != nil:
			case scopeErr == nil && scope.Replica.Role != topology.ReplicaVoter:
				return
			case holdsReplica(active, id):
				if err = h.cfg.ReseatOwn(ctx, id, active.Version); err == nil {
					backoff = reseatMinBackoff // committed; the approved plan replaces this identity
				}
			case scopeErr != nil:
				h.clearPark(id) // replaced or removed
				return
			default: // Retain the replica and wait before checking again.
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
	return fault.Retryable(err) || errors.Is(err, fault.ErrOperatorAuthorization) ||
		errors.Is(err, fault.ErrVersionConflict)
}

func holdsReplica(doc topology.Document, id storage.Identity) bool {
	if g := doc.Group(id.GroupID); g != nil {
		for _, r := range g.Replicas {
			if r.NodeName == id.NodeName && r.ReplicaID == id.ReplicaID {
				return true
			}
		}
	}
	return false
}

func (h *Host) clearPark(id storage.Identity) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if parked, found := h.parked[id.GroupID]; found && parked.Identity == id {
		delete(h.parked, id.GroupID)
	}
}
