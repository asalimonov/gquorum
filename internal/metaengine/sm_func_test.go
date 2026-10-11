//go:build functional

package metaengine

import (
	"fmt"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

// TestSM_ExecutionsBounded verifies terminal, unreferenced executions are
// pruned with the history ring while Target and Active remain pinned.
func TestSM_OperatorsBounded(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "p1"})

	idx := uint64(1)
	at := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	for expected := uint64(1); expected < 300; expected++ {
		idx++
		apply(t, sm, idx, ProposeDocument{Doc: baseDoc(), ExpectedVersion: expected, ProposalID: fmt.Sprintf("d%d", expected)})
		idx++
		newVersion := expected + 1
		apply(t, sm, idx, beginProposal(newVersion, 0, fmt.Sprintf("b%d", newVersion), at.Add(time.Duration(newVersion)*time.Second)))
		id := topology.MakeOperatorID(newVersion, 1)
		idx++
		apply(t, sm, idx, TransitionOperatorProposal{Transition: topology.OperatorTransition{
			OperatorID: id, Version: newVersion, Attempt: 1, State: topology.OperatorSucceeded,
			At: at.Add(time.Duration(newVersion)*time.Second + time.Millisecond),
		}, ProposalID: fmt.Sprintf("s%d", newVersion)})
		idx++
		apply(t, sm, idx, ActivateTargetProposal{OperatorID: id, Version: newVersion, Attempt: 1, ProposalID: fmt.Sprintf("a%d", newVersion)})
	}

	if got := len(sm.operators); got > historyRetention {
		t.Fatalf("executions map = %d records after 299 commits, want <= %d (must prune with the history ring)", got, historyRetention)
	}
	// No record may linger below the retained history horizon.
	all := sm.History(0, 0)
	horizon := all[len(all)-1].Version
	for _, ex := range sm.operators {
		if ex.Version < horizon && ex.Version != sm.target.Version && ex.Version != sm.active.Version {
			t.Fatalf("execution record for pruned version %d retained (history horizon %d)", ex.Version, horizon)
		}
	}
}
