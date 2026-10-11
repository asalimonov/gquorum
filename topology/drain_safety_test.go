//go:build !functional

package topology

import (
	"errors"
	"fmt"
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

func TestMetaDrainSafetyActualQuorumSizes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name               string
		size, deadRetained int
		want               bool
	}{
		{"one_voter_cannot_drain", 1, 0, false},
		{"three_retain_two", 3, 0, true},
		{"three_retained_dead", 3, 1, false},
		{"five_retain_quorum", 5, 1, true},
		{"five_lose_retained_quorum", 5, 2, false},
		{"seven_retain_quorum", 7, 2, true},
		{"seven_lose_retained_quorum", 7, 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			names := make([]string, tc.size)
			health := map[string]Health{}
			for i := range tc.size {
				names[i] = fmt.Sprintf("n%d", i+1)
				health[names[i]] = HealthAlive
			}
			for i := range tc.deadRetained {
				health[names[i+1]] = HealthDead
			}
			active := planDoc(1, tc.size, names)
			target := active.Clone()
			target.Node(names[tc.size-1]).State = NodeDraining
			observed := statusForDocument(active)[MetaGroupID]
			observed.HealthKnown = true
			_, err := ValidateMetaDrainSafety(active, target, observed, health, false)
			if tc.want && err != nil || !tc.want && !errors.Is(err, fault.ErrUnsafeMembership) {
				t.Fatal("actual voter safety", err)
			}
			// A smaller desired membership must not lower the live majority
			// requirement of the configuration still running in Raft.
			if tc.size > 1 {
				target.MetaGroup().Replicas = target.MetaGroup().Replicas[:tc.size-1]
				_, err = ValidateMetaDrainSafety(active, target, observed, health, false)
				if tc.want && err != nil || !tc.want && !errors.Is(err, fault.ErrUnsafeMembership) {
					t.Fatal("desired quorum replaced actual quorum", err)
				}
			}
		})
	}
}

func TestMetaDrainSafetyFailsClosed(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		change        func(*GroupStatus)
		unknownHealth bool
	}{
		{name: "unresolved_actual_voter", change: func(s *GroupStatus) { s.ConfState.Voters = []uint64{1, 2, 99} }},
		{name: "unknown_health", unknownHealth: true},
		{name: "unsampled_status", change: func(s *GroupStatus) { s.HealthKnown = false }},
		{name: "absent_runtime", change: func(s *GroupStatus) { s.Found = false }},
		{name: "missing_confstate", change: func(s *GroupStatus) { s.ConfState.Voters = nil }},
		{name: "joint_configuration", change: func(s *GroupStatus) { s.ConfState.VotersOutgoing = []uint64{1, 2, 3} }},
		{name: "unknown_leader", change: func(s *GroupStatus) { s.LeaderID = 0 }},
		{name: "unresolved_leader", change: func(s *GroupStatus) { s.LeaderID = 99 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			active := planDoc(1, 3, []string{"n1", "n2", "n3"})
			target := active.Clone()
			target.Node("n3").State = NodeDraining
			observed := statusForDocument(active)[MetaGroupID]
			observed.HealthKnown = true
			health := map[string]Health{"n1": HealthAlive, "n2": HealthAlive, "n3": HealthAlive}
			if tc.change != nil {
				tc.change(&observed)
			}
			if tc.unknownHealth {
				health = nil
			}
			for _, final := range []bool{false, true} {
				if _, err := ValidateMetaDrainSafety(active, target, observed, health, final); !errors.Is(err, fault.ErrUnsafeMembership) {
					t.Fatal("unsafe observation accepted", final, err)
				}
			}
		})
	}
}
