package membership

import (
	"fmt"

	"github.com/asalimonov/gquorum/fault"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type SafetyPolicy struct{}

type SafetyRefusal struct {
	GroupID     string
	Operation   string
	ReplicaID   uint64
	Reason      string
	LiveVoters  int
	Quorum      int
	CatchUpLag  uint64
	HealthKnown bool
}

func (e *SafetyRefusal) Unwrap() error { return fault.ErrUnsafeMembership }

func (e *SafetyRefusal) Error() string {
	live := "unknown"
	if e.HealthKnown {
		live = fmt.Sprintf("%d", e.LiveVoters)
	}
	return fmt.Sprintf(
		"membership: group %q operation %s replica %d refused: %s (live_voters=%s quorum=%d catch_up_lag=%d)",
		e.GroupID, e.Operation, e.ReplicaID, e.Reason, live, e.Quorum, e.CatchUpLag,
	)
}

func (SafetyPolicy) Check(g RaftGroup, health HealthSource, cc raftpb.ConfChangeV2) error {
	op, replicaID := describeChange(cc)
	if len(cc.Changes) != 1 {
		return &SafetyRefusal{
			GroupID: g.GroupID(), Operation: op, ReplicaID: replicaID,
			Reason: fmt.Sprintf("single-change ConfChangeV2 required, got %d changes", len(cc.Changes)),
		}
	}
	if cc.Transition != raftpb.ConfChangeTransitionAuto {
		return &SafetyRefusal{
			GroupID: g.GroupID(), Operation: op, ReplicaID: replicaID,
			Reason: fmt.Sprintf("single-change Auto transition required, got %s", cc.Transition),
		}
	}

	ch := cc.Changes[0]
	st := g.Status()
	if ch.NodeID == 0 {
		return &SafetyRefusal{GroupID: g.GroupID(), Operation: op, Reason: "replica ID must be positive"}
	}
	if len(st.Config.Voters[1]) != 0 || len(st.Config.LearnersNext) != 0 || st.Config.AutoLeave {
		return &SafetyRefusal{GroupID: g.GroupID(), Operation: op, ReplicaID: ch.NodeID, Reason: "joint configuration is unsupported"}
	}
	cur := voterSet(st)
	post := postVoters(cur, ch)
	_, learner := st.Config.Learners[ch.NodeID]
	if ch.Type == raftpb.ConfChangeAddLearnerNode {
		if cur[ch.NodeID] {
			op = "demote_voter"
		} else {
			op = "add_learner"
		}
	}

	switch ch.Type {
	case raftpb.ConfChangeAddNode:
		if cur[ch.NodeID] {
			return nil
		}
	case raftpb.ConfChangeAddLearnerNode:
		if learner {
			return nil
		}
		if !cur[ch.NodeID] {
			return nil // learner-first grow is quorum-neutral
		}
	case raftpb.ConfChangeRemoveNode:
		if !cur[ch.NodeID] && !learner {
			return nil
		}
	default:
		return refusal(g, op, ch.NodeID, "unsupported ConfChange type", post, nil, false, 0)
	}
	live, known := liveSet(health, g.GroupID())

	lag := uint64(0)
	if ch.Type == raftpb.ConfChangeAddNode {
		if !learner {
			return refusal(g, op, ch.NodeID, "replica is not a current learner; add it as a learner first", post, live, known, 0)
		}
		caughtUp, candidateLag, reason := catchUpStatus(g, st, ch.NodeID)
		lag = candidateLag
		if !caughtUp {
			return refusal(g, op, ch.NodeID, reason, post, live, known, lag)
		}
	}

	if len(post) == 0 {
		return refusal(g, op, ch.NodeID, "change would remove the final voter", post, live, known, lag)
	}

	if st.Lead != 0 && cur[st.Lead] && !post[st.Lead] && ch.NodeID == st.Lead {
		return refusal(g, op, ch.NodeID, "leader must transfer before removal or demotion", post, live, known, lag)
	}

	if !known {
		return refusal(g, op, ch.NodeID, "replica liveness is unknown", post, nil, false, lag)
	}

	quorum := len(post)/2 + 1
	liveVoters := 0
	for id := range post {
		if !live[id] {
			continue
		}
		if cur[id] {
			liveVoters++
			continue
		}
		if id == ch.NodeID {
			liveVoters++
		}
	}
	if liveVoters < quorum {
		return refusal(g, op, ch.NodeID, "post-change live voter quorum would be unavailable", post, live, true, lag)
	}
	return nil
}

func refusal(
	g RaftGroup,
	op string,
	replicaID uint64,
	reason string,
	post map[uint64]bool,
	live map[uint64]bool,
	known bool,
	lag uint64,
) error {
	liveVoters := 0
	if known {
		for id := range post {
			if live[id] {
				liveVoters++
			}
		}
	}
	quorum := 0
	if len(post) > 0 {
		quorum = len(post)/2 + 1
	}
	return &SafetyRefusal{
		GroupID: g.GroupID(), Operation: op, ReplicaID: replicaID, Reason: reason,
		LiveVoters: liveVoters, Quorum: quorum, CatchUpLag: lag, HealthKnown: known,
	}
}

func describeChange(cc raftpb.ConfChangeV2) (string, uint64) {
	if len(cc.Changes) != 1 {
		return "invalid", 0
	}
	ch := cc.Changes[0]
	switch ch.Type {
	case raftpb.ConfChangeAddNode:
		return "promote_learner", ch.NodeID
	case raftpb.ConfChangeAddLearnerNode:
		return "add_learner_or_demote_voter", ch.NodeID
	case raftpb.ConfChangeRemoveNode:
		return "remove_replica", ch.NodeID
	case raftpb.ConfChangeUpdateNode:
		return "update_replica", ch.NodeID
	default:
		return fmt.Sprintf("confchange_%d", ch.Type), ch.NodeID
	}
}

func voterSet(st raft.Status) map[uint64]bool {
	out := make(map[uint64]bool)
	for id := range st.Config.Voters.IDs() {
		out[id] = true
	}
	return out
}

func postVoters(cur map[uint64]bool, ch raftpb.ConfChangeSingle) map[uint64]bool {
	out := make(map[uint64]bool, len(cur)+1)
	for id := range cur {
		out[id] = true
	}
	switch ch.Type {
	case raftpb.ConfChangeAddNode:
		out[ch.NodeID] = true
	case raftpb.ConfChangeRemoveNode, raftpb.ConfChangeAddLearnerNode:
		delete(out, ch.NodeID)
	}
	return out
}

func liveSet(health HealthSource, groupID string) (map[uint64]bool, bool) {
	if health == nil {
		return nil, false
	}
	return health.Live(groupID)
}
