//go:build !functional

package metaservice

import (
	"bytes"
	"testing"

	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestJoinClassificationWaitsForAppliedCurrentTerm(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                                                   string
		state                                                  raft.StateType
		term, commit, nativeApplied, machineIndex, machineTerm uint64
		want                                                   bool
	}{
		{"current_term_applied", raft.StateLeader, 7, 13, 13, 13, 7, true},
		{"old_term_equal_indexes", raft.StateLeader, 7, 12, 12, 12, 6, false},
		{"native_apply_behind", raft.StateLeader, 7, 13, 12, 12, 7, false},
		{"machine_apply_behind", raft.StateLeader, 7, 13, 13, 12, 7, false},
		{"nothing_committed", raft.StateLeader, 7, 0, 0, 0, 0, false},
		{"term_unknown", raft.StateLeader, 0, 12, 12, 12, 6, false},
		{"follower", raft.StateFollower, 7, 13, 13, 13, 7, false},
		{"machine_ahead_of_publication", raft.StateLeader, 7, 13, 13, 14, 7, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			machine := metaengine.NewMachine(topology.Rules{SchemaVersion: 1, Limits: (topology.Limits{}).WithDefaults()}, nil)
			if err := machine.AfterApply(t.Context(), tc.machineIndex, tc.machineTerm, raftpb.ConfState{Voters: []uint64{1}}); err != nil {
				t.Fatal(err)
			}
			status := raft.Status{}
			status.RaftState = tc.state
			status.Term = tc.term
			status.Commit = tc.commit
			status.Applied = tc.nativeApplied
			before, err := machine.Snapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if got := joinClassificationReady(status, machine); got != tc.want {
				t.Fatal("admission classification barrier", got, tc.want, status, tc.machineIndex, tc.machineTerm)
			}
			after, err := machine.Snapshot(t.Context())
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("barrier changed admission state")
			}
		})
	}
}
