//go:build !functional

package membership

import (
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

func TestChangeConstructorsUseLearnerThenVoterGrowth(t *testing.T) {
	t.Parallel()
	add := AddLearner(7)
	if len(add.Changes) != 1 || add.Changes[0].Type != raftpb.ConfChangeAddLearnerNode || add.Changes[0].NodeID != 7 {
		t.Fatalf("AddLearner = %+v", add)
	}
	if add.Transition != raftpb.ConfChangeTransitionAuto {
		t.Fatalf("transition = %v, want Auto", add.Transition)
	}
	if got := PromoteLearner(7).Changes[0].Type; got != raftpb.ConfChangeAddNode {
		t.Fatalf("PromoteLearner type = %v, want AddNode", got)
	}
	if got := RemoveReplica(7).Changes[0].Type; got != raftpb.ConfChangeRemoveNode {
		t.Fatalf("RemoveReplica type = %v, want RemoveNode", got)
	}
	if got := DemoteVoter(7).Changes[0].Type; got != raftpb.ConfChangeAddLearnerNode {
		t.Fatalf("DemoteVoter type = %v, want AddLearnerNode", got)
	}
}
