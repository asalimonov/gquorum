package gquorum

import (
	"errors"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type NodeState string

const (
	NodeNew      NodeState = "new"
	NodeStarting NodeState = "starting"
	NodeReady    NodeState = "ready"
	NodeFenced   NodeState = "fenced"
	NodeStopping NodeState = "stopping"
	NodeStopped  NodeState = "stopped"
	NodeFailed   NodeState = "failed"
)

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
type LocalReplicaStatus struct {
	Identity  storage.Identity `json:"identity"`
	State     string           `json:"state"`
	ErrorCode fault.Code       `json:"error_code,omitempty"`
	Error     string           `json:"error,omitempty"`
}

// Status takes owned local snapshots. It never probes peers or invokes caller
// policies. Ready records the startup gate; SessionLive is a current input.
func (n *node) Status() NodeStatus {
	n.mu.RLock()
	s := NodeStatus{Identity: n.opts.Identity, State: n.state, Ready: n.ready && !n.fenced && !n.stopping}
	m, h, p := n.meta, n.host, n.policies
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
		s.Identity = m.Identity()
		s.Meta = m.LocalStatus()
		s.Scheduling = m.SchedulingStatus()
		_, terminal, removal := m.Lifecycle()
		s.SelfRemoval = removal
		if terminal != nil {
			err = terminal
			s.Ready = false
			s.State = NodeFailed
		}
		if removal != nil {
			s.Ready = false
			if !stopping {
				s.State = NodeFenced
			}
		}
		s.SessionLive = !stopping && removal == nil && m.OwnSessionLive()
	}
	if h != nil {
		for _, r := range h.Snapshot() {
			s.Loads = append(s.Loads, LocalReplicaStatus{Identity: r.Identity, State: r.State, ErrorCode: r.ErrorCode, Error: r.Error})
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
