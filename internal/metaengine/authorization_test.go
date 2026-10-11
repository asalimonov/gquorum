package metaengine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestExactFutureProgressIsRetryableButNeverAuthorized(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	apply(t, sm, 1, ProposeDocument{Doc: baseDoc()})
	at := time.Unix(100, 0).UTC()
	begin := apply(t, sm, 2, beginProposal(1, 2, "begin", at))
	a := topology.AuthorizedStep{OperatorID: begin.OperatorID, Version: 1, Attempt: 1, StepSeq: 2, Kind: topology.StepTransferLeader, GroupID: "shard0", NodeName: "n1", ReplicaID: 1}
	transition := func(index uint64, seq int, state topology.OperatorState) {
		apply(t, sm, index, TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: begin.OperatorID, Version: 1, Attempt: 1, StepSeq: seq, State: state, GroupID: "shard0", NodeName: "n1", At: at.Add(time.Duration(index) * time.Second)}})
	}
	transition(3, 1, topology.OperatorRunning)
	if err := sm.Authorize(context.Background(), a); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("ahead-of-progress effect executed or failed permanently", err)
	}
	bad := a
	bad.Attempt++
	if err := sm.Authorize(context.Background(), bad); !errors.Is(err, fault.ErrOperatorAuthorization) {
		t.Fatal("wrong future attempt", err)
	}
	bad = a
	bad.Kind = topology.StepRemoveReplica
	if err := sm.Authorize(context.Background(), bad); !errors.Is(err, fault.ErrOperatorAuthorization) {
		t.Fatal("wrong future operation", err)
	}
	transition(4, 2, topology.OperatorRunning)
	if err := sm.Authorize(context.Background(), a); err != nil {
		t.Fatal("applied exact progress still unavailable", err)
	}
	stale := a
	stale.StepSeq = 1
	if err := sm.Authorize(context.Background(), stale); !errors.Is(err, fault.ErrOperatorAuthorization) {
		t.Fatal("old step accepted", err)
	}
	transition(5, 2, topology.OperatorSucceeded)
	if err := sm.Authorize(context.Background(), a); !errors.Is(err, fault.ErrOperatorAuthorization) {
		t.Fatal("terminal effect accepted", err)
	}
}
