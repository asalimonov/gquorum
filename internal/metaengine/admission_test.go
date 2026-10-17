//go:build !functional

package metaengine

import (
	"github.com/asalimonov/gquorum/topology"
)

func assumedReadmissionStatuses(active topology.Document, returningNode string) map[string]topology.GroupStatus {
	statuses := make(map[string]topology.GroupStatus, len(active.Groups))
	for _, group := range active.Groups {
		status := topology.GroupStatus{GroupID: group.ID, Found: true, Commit: 1, HealthKnown: true, LiveVoters: make(map[uint64]bool)}
		for _, replica := range group.Replicas {
			switch replica.Role {
			case topology.ReplicaLearner:
				status.ConfState.Learners = append(status.ConfState.Learners, replica.ReplicaID)
			case topology.ReplicaVoter:
				status.ConfState.Voters = append(status.ConfState.Voters, replica.ReplicaID)
				status.LiveVoters[replica.ReplicaID] = replica.NodeName != returningNode
				if status.LeaderID == 0 && replica.NodeName != returningNode {
					status.LeaderID = replica.ReplicaID
				}
			}
		}
		statuses[group.ID] = status
	}
	return statuses
}
