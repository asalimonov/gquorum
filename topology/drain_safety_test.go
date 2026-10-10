package topology

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"go.etcd.io/raft/v3/raftpb"
)

func TestDrainSafetyUsesActualQuorumAndKnownRetainedLeader(t *testing.T) {
	t.Parallel()
	active := planDoc(1, 3, []string{"n1", "n2", "n3"})
	target := active.Clone()
	target.Version++
	target.Node("n1").State = NodeDraining
	status := GroupStatus{GroupID: MetaGroupID, Found: true, LeaderID: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}, HealthKnown: true}
	alive := map[string]Health{"n1": HealthAlive, "n2": HealthAlive, "n3": HealthAlive}
	transfer, err := ValidateMetaDrainSafety(active, target, status, alive, false)
	if err != nil || transfer != 2 {
		t.Fatal(transfer, err)
	}
	if _, err = ValidateMetaDrainSafety(active, target, status, alive, true); !errors.Is(err, fault.ErrUnsafeMembership) {
		t.Fatal("active drain kept departing leader", err)
	}
	status.LeaderID = 2
	if _, err = ValidateMetaDrainSafety(active, target, status, alive, true); err != nil {
		t.Fatal(err)
	}
	alive["n3"] = HealthDead
	if _, err = ValidateMetaDrainSafety(active, target, status, alive, false); !errors.Is(err, fault.ErrUnsafeMembership) {
		t.Fatal("unsafe retained majority", err)
	}
	if _, err = ValidateMetaDrainSafety(active, target, status, nil, false); !errors.Is(err, fault.ErrUnsafeMembership) {
		t.Fatal("unknown health allowed drain", err)
	}
	// Desired membership cannot lower the pre-change majority requirement.
	target.MetaGroup().Replicas = target.MetaGroup().Replicas[1:]
	if _, err = ValidateMetaDrainSafety(active, target, status, alive, false); !errors.Is(err, fault.ErrUnsafeMembership) {
		t.Fatal("desired majority replaced actual majority", err)
	}
}
