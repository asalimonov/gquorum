package operator

import (
	"github.com/asalimonov/gquorum/topology"
)

// GroupStatus aliases observed Raft membership, leadership and liveness.
type GroupStatus = topology.GroupStatus

// ReplicaProgress aliases observed per-peer replication progress.
type ReplicaProgress = topology.ReplicaProgress
