package managed

import (
	"context"
	"errors"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/observe"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// LocalMetrics copies published exact owners only. It performs no callbacks or
// network I/O and remains safe while a different owner is being acquired.
func (h *Host) LocalMetrics() []metrics.ReplicaObservation {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]metrics.ReplicaObservation, 0, len(h.slots))
	for _, slot := range h.slots {
		if slot.state == "ready" && slot.handle != nil && slot.core != nil && !slot.released && slot.core.Running() {
			out = append(out, slot.core.Metrics())
		}
	}
	return out
}

// MetricsReads prepares exact replica operations for one cluster-wide pool.
// ResolvePeer reads the replicated process pin; applications are called only
// inside the owned Read callback. Unknown pins produce no failure evidence.
func (h *Host) MetricsReads(ctx context.Context, doc topology.Document) []observe.ReplicaRead {
	var reads []observe.ReplicaRead
	kept := map[storage.Identity]bool{}
	for _, group := range doc.LoadGroups() {
		for _, replica := range group.Replicas {
			identity := storage.Identity{ClusterID: doc.ClusterID, GroupID: group.ID, NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
			kept[identity] = true
			peer, err := h.peerFor(ctx, identity)
			h.mu.Lock()
			if h.metricsPeers == nil {
				h.metricsPeers = map[storage.Identity]load.Peer{}
			}
			if err == nil {
				h.metricsPeers[identity] = peer
			} else if errors.Is(err, fault.ErrReplicaNotReady) {
				if previous, ok := h.metricsPeers[identity]; ok {
					peer, err = previous, nil
				}
			} else {
				delete(h.metricsPeers, identity)
			}
			h.mu.Unlock()
			id := metrics.ReplicaIdentity{ClusterID: doc.ClusterID, GroupID: group.ID, NodeName: replica.NodeName, ReplicaID: replica.ReplicaID}
			item := observe.ReplicaRead{Identity: id, Error: err}
			if err == nil {
				item.Identity.InstanceID = peer.Node.InstanceID
				item.Read = func(ctx context.Context, attempt *observe.ReadAttempt) (observe.Probe, error) {
					work, done, err := h.beginWork(ctx)
					if err != nil {
						return observe.Probe{}, err
					}
					defer done()
					if err := h.cfg.ValidatePeer(work, peer); err != nil && !errors.Is(err, fault.ErrReplicaNotReady) {
						return observe.Probe{}, err
					}
					query := load.ReplicaQuery{From: h.cfg.Identity(), To: peer}
					attempt.Dispatch()
					var reply load.ReplicaStatus
					if peer.Node == h.cfg.Identity() {
						reply, err = h.ReadReplica(work, query)
					} else {
						reply, err = h.cfg.Transport.ReadReplica(work, query)
					}
					if err != nil {
						return observe.Probe{}, err
					}
					if reply.Peer != peer || reply.Found != reply.Status.Found || reply.Found && (reply.Status.GroupID != group.ID || reply.Status.HostNode != peer.Node.NodeName) {
						return observe.Probe{}, fault.ErrIdentityMismatch
					}
					result := observe.Probe{Reachability: "alive", Status: reply.Status.Clone(), Observation: metrics.ReplicaObservation{State: metrics.Unknown}}
					if reply.Metrics != nil {
						sample := reply.Metrics.Clone()
						if sample.Identity.ClusterID != id.ClusterID || sample.Identity.GroupID != id.GroupID || sample.Identity.NodeName != id.NodeName || sample.Identity.ReplicaID != id.ReplicaID || sample.Identity.InstanceID != peer.Node.InstanceID || sample.Identity.RunID == "" {
							return observe.Probe{}, fault.ErrIdentityMismatch
						}
						result.Observation = metrics.ReplicaObservation{State: metrics.Current, Sample: sample, Reachability: "alive"}
						if !reply.Found || reply.LocalState != "ready" {
							result.Observation.Sample.Ready = false
						}
					}
					if peer.Node == h.cfg.Identity() {
						h.mu.Lock()
						result.Held = h.holds[identity]
						h.mu.Unlock()
					}
					return result, nil
				}
			}
			reads = append(reads, item)
		}
	}
	h.mu.Lock()
	for id := range h.metricsPeers {
		if !kept[id] {
			delete(h.metricsPeers, id)
		}
	}
	h.mu.Unlock()
	return reads
}
