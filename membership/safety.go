package membership

import (
	"fmt"

	"github.com/asalimonov/gquorum/fault"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// SafetyRules enforces learner-first changes, leadership transfer and
// post-change live quorum.
type SafetyRules struct{}

// SafetyRefusal explains a rejected membership change and its observed safety
// inputs.
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

func (*SafetyRefusal) Unwrap() error { return fault.ErrUnsafeMembership }

func (e *SafetyRefusal) Error() string {
	live := "unknown"
	if e.HealthKnown {
		live = fmt.Sprintf("%d", e.LiveVoters)
	}
	return fmt.Sprintf(
		"membership: group %q operation %s replica %d refused: %s (live_voters=%s quorum=%d catch_up_lag=%d)",
		e.GroupID, e.Operation, e.ReplicaID, e.Reason, live, e.Quorum, e.CatchUpLag)
}

// Check validates a single automatic membership change against native
// membership and liveness.
func (SafetyRules) Check(g RaftGroup, health HealthSource, cc raftpb.ConfChangeV2) error {
	change, err := inspectMembershipChange(g, cc)
	if err != nil {
		return err
	}
	ch, st := change.change, change.status
	op, cur, post, learner := change.operation, change.current, change.post, change.learner

	if noop, err := change.noop(g); noop || err != nil {
		return err
	}
	live, known := liveSet(health, g.GroupID())

	lag := uint64(0)
	if ch.Type == raftpb.ConfChangeAddNode {
		if !learner {
			return refusal(g, refusalInput{
				operation: op,
				replicaID: ch.NodeID,
				reason:    "replica is not a current learner; add it as a learner first",
				post:      post,
				live:      live,
				known:     known,
				lag:       0,
			})
		}
		candidate := catchUpStatus(g, st, ch.NodeID)
		caughtUp, candidateLag, reason := candidate.caughtUp, candidate.lag, candidate.reason
		lag = candidateLag
		if !caughtUp {
			return refusal(g,
				refusalInput{operation: op, replicaID: ch.NodeID, reason: reason, post: post, live: live, known: known,
					lag: lag})
		}
	}

	if len(post) == 0 {
		return refusal(g, refusalInput{
			operation: op,
			replicaID: ch.NodeID,
			reason:    "change would remove the final voter",
			post:      post,
			live:      live,
			known:     known,
			lag:       lag,
		})
	}

	if st.Lead != 0 && cur[st.Lead] && !post[st.Lead] && ch.NodeID == st.Lead {
		return refusal(g, refusalInput{
			operation: op,
			replicaID: ch.NodeID,
			reason:    "leader must transfer before removal or demotion",
			post:      post,
			live:      live,
			known:     known,
			lag:       lag,
		})
	}

	if !known {
		return refusal(g, refusalInput{
			operation: op,
			replicaID: ch.NodeID,
			reason:    "replica liveness is unknown",
			post:      post,
			live:      nil,
			known:     false,
			lag:       lag,
		})
	}

	quorum := len(post)/2 + 1
	liveVoters := postLiveVoters(cur, post, live, ch.NodeID)
	if liveVoters < quorum {
		return refusal(g, refusalInput{
			operation: op,
			replicaID: ch.NodeID,
			reason:    "post-change live voter quorum would be unavailable",
			post:      post,
			live:      live,
			known:     true,
			lag:       lag,
		})
	}
	return nil
}

type membershipChange struct {
	operation     string
	change        raftpb.ConfChangeSingle
	status        raft.Status
	current, post map[uint64]bool
	learner       bool
}

func inspectMembershipChange(g RaftGroup, cc raftpb.ConfChangeV2) (membershipChange, error) {
	op, replicaID := describeChange(cc)
	if len(cc.Changes) != 1 {
		return membershipChange{}, &SafetyRefusal{
			GroupID: g.GroupID(), Operation: op, ReplicaID: replicaID,
			Reason: fmt.Sprintf("single-change ConfChangeV2 required, got %d changes", len(cc.Changes)),
		}
	}
	if cc.Transition != raftpb.ConfChangeTransitionAuto {
		return membershipChange{}, &SafetyRefusal{
			GroupID: g.GroupID(), Operation: op, ReplicaID: replicaID,
			Reason: fmt.Sprintf("single-change Auto transition required, got %s", cc.Transition),
		}
	}

	ch := cc.Changes[0]
	st := g.Status()
	if ch.NodeID == 0 {
		return membershipChange{}, &SafetyRefusal{GroupID: g.GroupID(), Operation: op,
			Reason: "replica ID must be positive"}
	}
	if len(st.Config.Voters[1]) != 0 || len(st.Config.LearnersNext) != 0 || st.Config.AutoLeave {
		return membershipChange{}, &SafetyRefusal{GroupID: g.GroupID(), Operation: op, ReplicaID: ch.NodeID,
			Reason: "joint configuration is unsupported"}
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
	return membershipChange{operation: op, change: ch, status: st, current: cur,
		post: post, learner: learner}, nil
}

func (c membershipChange) noop(g RaftGroup) (bool, error) {
	ch, op, cur, post, learner := c.change, c.operation, c.current, c.post, c.learner
	switch ch.Type {
	case raftpb.ConfChangeAddNode:
		if cur[ch.NodeID] {
			return true, nil
		}
	case raftpb.ConfChangeAddLearnerNode:
		if learner {
			return true, nil
		}
		if !cur[ch.NodeID] {
			return true, nil // learner-first grow is quorum-neutral
		}
	case raftpb.ConfChangeRemoveNode:
		if !cur[ch.NodeID] && !learner {
			return true, nil
		}
	default:
		return false, refusal(g, refusalInput{
			operation: op,
			replicaID: ch.NodeID,
			reason:    "unsupported ConfChange type",
			post:      post,
			live:      nil,
			known:     false,
			lag:       0,
		})
	}
	return false, nil
}

func postLiveVoters(cur, post, live map[uint64]bool, candidate uint64) int {
	liveVoters := 0
	for id := range post {
		if !live[id] {
			continue
		}
		if cur[id] {
			liveVoters++
			continue
		}
		if id == candidate {
			liveVoters++
		}
	}
	return liveVoters
}

type refusalInput struct {
	operation  string
	replicaID  uint64
	reason     string
	post, live map[uint64]bool
	known      bool
	lag        uint64
}

func refusal(g RaftGroup, in refusalInput) error {
	op, replicaID, reason := in.operation, in.replicaID, in.reason
	post, live, known, lag := in.post, in.live, in.known, in.lag
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
	default: // Other configuration changes leave the voter set unchanged.
	}
	return out
}

func liveSet(health HealthSource, groupID string) (map[uint64]bool, bool) {
	if health == nil {
		return nil, false
	}
	return health.Live(groupID)
}
