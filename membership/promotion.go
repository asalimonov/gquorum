package membership

import (
	"fmt"

	"go.etcd.io/raft/v3"
)

// PromotionLagLimit bounds the committed-entry lag allowed when promoting a
// learner.
const PromotionLagLimit uint64 = 1000

// CaughtUp reports whether the local leader observes an eligible, sufficiently
// caught-up learner.
func CaughtUp(g RaftGroup, replicaID uint64) bool {
	status := catchUpStatus(g, g.Status(), replicaID)
	return status.caughtUp
}

type catchUpResult struct {
	caughtUp bool
	lag      uint64
	reason   string
}

func catchUpStatus(g RaftGroup, st raft.Status, replicaID uint64) catchUpResult {
	if !g.IsLeader() {
		return catchUpResult{caughtUp: false, lag: 0, reason: "local replica is not the group leader"}
	}
	if _, ok := st.Config.Learners[replicaID]; !ok {
		return catchUpResult{caughtUp: false, lag: 0, reason: "replica is not a current learner"}
	}
	if st.Commit == 0 {
		return catchUpResult{caughtUp: false, lag: 0, reason: "group commit index is zero"}
	}
	pr, ok := st.Progress[replicaID]
	if !ok {
		return catchUpResult{caughtUp: false, lag: 0, reason: "learner progress is unavailable"}
	}
	lag := uint64(0)
	if st.Commit > pr.Match {
		lag = st.Commit - pr.Match
	}
	if !pr.RecentActive && lag > 0 {
		return catchUpResult{caughtUp: false, lag: lag, reason: "learner is not recently active"}
	}
	if lag > PromotionLagLimit {
		return catchUpResult{caughtUp: false, lag: lag, reason: fmt.Sprintf("learner lag exceeds %d",
			PromotionLagLimit)}
	}
	return catchUpResult{caughtUp: true, lag: lag, reason: ""}
}
