package membership

import (
	"fmt"

	"go.etcd.io/raft/v3"
)

const PromotionLagLimit uint64 = 1000

func CaughtUp(g RaftGroup, replicaID uint64) bool {
	ok, _, _ := catchUpStatus(g, g.Status(), replicaID)
	return ok
}

func catchUpStatus(g RaftGroup, st raft.Status, replicaID uint64) (bool, uint64, string) {
	if !g.IsLeader() {
		return false, 0, "local replica is not the group leader"
	}
	if _, ok := st.Config.Learners[replicaID]; !ok {
		return false, 0, "replica is not a current learner"
	}
	if st.Commit == 0 {
		return false, 0, "group commit index is zero"
	}
	pr, ok := st.Progress[replicaID]
	if !ok {
		return false, 0, "learner progress is unavailable"
	}
	lag := uint64(0)
	if st.Commit > pr.Match {
		lag = st.Commit - pr.Match
	}
	if !pr.RecentActive && lag > 0 {
		return false, lag, "learner is not recently active"
	}
	if lag > PromotionLagLimit {
		return false, lag, fmt.Sprintf("learner lag exceeds %d", PromotionLagLimit)
	}
	return true, lag, ""
}
