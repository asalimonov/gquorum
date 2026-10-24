//revive:disable:max-public-structs Keep local status and watch snapshots together.

package gquorum

import (
	"context"
	"errors"
	"reflect"
	"slices"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaservice"
	"github.com/asalimonov/gquorum/internal/observe"
	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// NodeState describes the local participant's lifecycle state.
type NodeState string

// NodeNew and the following states describe construction, startup and teardown.
const (
	NodeNew      NodeState = "new"
	NodeStarting NodeState = "starting"
	NodeReady    NodeState = "ready"
	NodeFenced   NodeState = "fenced"
	NodeStopping NodeState = "stopping"
	NodeStopped  NodeState = "stopped"
	NodeFailed   NodeState = "failed"
)

// NodeStatus is an owned snapshot of local lifecycle, meta and load
// observations.
type NodeStatus struct {
	Identity    topology.Identity           `json:"identity"`
	State       NodeState                   `json:"state"`
	Ready       bool                        `json:"ready"`
	SessionLive bool                        `json:"session_live"`
	Meta        topology.GroupStatus        `json:"meta"`
	Scheduling  topology.SchedulingStatus   `json:"scheduling"`
	SelfRemoval *topology.SelfRemovalStatus `json:"self_removal,omitempty"`
	Loads       []LocalReplicaStatus        `json:"loads"`
	ErrorCode   fault.Code                  `json:"error_code,omitempty"`
	Error       string                      `json:"error,omitempty"`
}

// LocalReplicaStatus identifies one local replica run and its observed state.
type LocalReplicaStatus struct {
	InstanceID string           `json:"instance_id"`
	RunID      string           `json:"run_id"`
	Identity   storage.Identity `json:"identity"`
	State      string           `json:"state"`
	ErrorCode  fault.Code       `json:"error_code,omitempty"`
	Error      string           `json:"error,omitempty"`
}

// Status takes owned local snapshots. It never probes peers or invokes caller
// hooks. Ready records the startup gate; SessionLive is a current input.
func (n *node) Status() NodeStatus {
	n.mu.RLock()
	s := NodeStatus{Identity: n.opts.Identity, State: n.state, Ready: n.ready && !n.fenced && !n.stopping}
	m, h, p := n.meta, n.host, n.lifecycle
	err := n.terminal
	if err == nil {
		err = n.lastError
	}
	stopping := n.stopping
	n.mu.RUnlock()
	if err == nil && p != nil {
		err = p.Error()
	}
	if m != nil {
		var terminal error
		s, terminal = metaNodeStatus(s, m, nodeStatusRead{stopping: stopping})
		if terminal != nil {
			err = terminal
		}
	}
	if h != nil {
		for _, r := range h.Snapshot() {
			s.Loads = append(s.Loads, LocalReplicaStatus{
				Identity:   r.Identity,
				InstanceID: r.InstanceID,
				RunID:      r.RunID,
				State:      r.State,
				ErrorCode:  r.ErrorCode,
				Error:      r.Error,
			})
		}
	}
	if err != nil {
		s.Error = err.Error()
		s.ErrorCode = fault.Unavailable
		var typed *fault.Error
		if errors.As(err, &typed) {
			s.ErrorCode = typed.Code
		}
	}
	return s
}

type nodeStatusRead struct{ stopping bool }

func metaNodeStatus(s NodeStatus, m *metaservice.Group, read nodeStatusRead) (NodeStatus, error) {
	s.Identity = m.Identity()
	s.Meta = m.LocalStatus()
	s.Scheduling = m.SchedulingStatus()
	observed, terminal := m.Lifecycle()
	removal := observed.Removal
	s.SelfRemoval = removal
	if terminal != nil {
		s.Ready = false
		s.State = NodeFailed
	}
	if removal != nil {
		s.Ready = false
		if !read.stopping {
			s.State = NodeFenced
		}
	}
	s.SessionLive = !read.stopping && removal == nil && m.OwnSessionLive()
	return s, terminal
}

// NodeObservation is an owned local lifecycle/status snapshot. Revision gaps
// indicate coalescing, not event loss from a replicated log. Status composes
// local observations; it does not provide a cross-component atomic barrier.
type NodeObservation struct {
	Revision uint64     `json:"revision"`
	Status   NodeStatus `json:"status"`
}

type observationFinish uint8

const (
	publishUpdate observationFinish = iota
	finishUpdates
)

// Watch registers with a current snapshot, including before Start or after
// Stop.
// Slow consumers retain only the latest state. Cancellation closes this watch;
// successful shutdown retains the final state and closes all watches. Replica
// InstanceID/RunID distinguish replacements; old Group watches remain exact.
func (n *node) Watch(ctx context.Context) (<-chan NodeObservation, error) {
	n.observationMu.Lock()
	defer n.observationMu.Unlock()
	n.publishObservationLocked(publishUpdate)
	return n.observations.Subscribe(ctx)
}

func cloneNodeObservation(o NodeObservation) NodeObservation {
	o.Status.Meta = o.Status.Meta.Clone()
	o.Status.Scheduling = o.Status.Scheduling.Clone()
	o.Status.Loads = slices.Clone(o.Status.Loads)
	if o.Status.SelfRemoval != nil {
		removal := *o.Status.SelfRemoval
		o.Status.SelfRemoval = &removal
	}
	return o
}

func (n *node) publishObservation() {
	n.observationMu.Lock()
	defer n.observationMu.Unlock()
	n.publishObservationLocked(publishUpdate)
}

// Capture inside observationMu so a slower publisher cannot overwrite a newer
// capture. Call only outside Node/host ownership locks; Status acquires them.
func (n *node) publishObservationLocked(finish observationFinish) {
	if n.observations == nil || n.observationsFinished {
		return
	}
	status := n.Status()
	changed := !reflect.DeepEqual(n.observation.Status, status)
	if changed {
		n.observation = NodeObservation{Revision: n.observation.Revision + 1, Status: status}
	}
	if finish == finishUpdates {
		n.observationsFinished = true
		n.observations.Finish(n.observation)
	} else if changed {
		n.observations.Publish(n.observation)
	}
}

func (n *node) finishObservations() {
	n.observationMu.Lock()
	n.publishObservationLocked(finishUpdates)
	n.observationMu.Unlock()
	if n.observations != nil {
		_ = n.observations.Join(context.Background())
	}
}

// wakeObservation is safe from internal state producers: it never reads status
// or recursively wakes host recovery while those producers are changing state.
func (n *node) wakeObservation() {
	select {
	case n.wake <- struct{}{}:
	default:
	}
}

func (n *node) localLoadMetrics() []metrics.ReplicaObservation {
	n.mu.RLock()
	h := n.host
	n.mu.RUnlock()
	if h == nil {
		return nil
	}
	return h.LocalMetrics()
}

func (n *node) loadMetricsReads(ctx context.Context, groups []topology.GroupProgress) []observe.ReplicaRead {
	n.mu.RLock()
	h := n.host
	n.mu.RUnlock()
	if h == nil {
		return nil
	}
	return h.MetricsReads(ctx, groups)
}
func (c *clusterView) Metrics() metrics.ClusterSnapshot {
	c.node.mu.RLock()
	m := c.node.meta
	c.node.mu.RUnlock()
	if m == nil {
		return (metrics.ClusterSnapshot{}).Clone()
	}
	return m.Metrics()
}

func (c *clusterView) MetricsHistory(q metrics.HistoryQuery) (metrics.HistoryPage, error) {
	c.node.mu.RLock()
	m := c.node.meta
	c.node.mu.RUnlock()
	if m == nil {
		return metrics.HistoryPage{}, fault.ErrHistoryUnavailable
	}
	return m.MetricsHistory(q)
}
