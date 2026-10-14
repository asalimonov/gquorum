package gquorum

import (
	"errors"
	"fmt"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaservice"
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

// ParkStoreMissing and the following reasons identify unavailable or
// contradictory storage.
const (
	ParkStoreMissing     ParkReason = "store_missing"
	ParkStoreUnreadable  ParkReason = "store_unreadable"
	ParkIdentityMismatch ParkReason = "identity_mismatch"
)

func (e *LoadParkedError) Error() string {
	return fmt.Sprintf("load group %s replica %d parked: %s", e.Identity.GroupID, e.Identity.ReplicaID, e.Reason)
}
func (e *LoadParkedError) Unwrap() error { return e.Cause }

// Is matches a parked boot against the replica-not-ready classification.
func (*LoadParkedError) Is(err error) bool {
	return errors.Is(fault.ErrReplicaNotReady, err)
}
