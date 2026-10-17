//go:build !functional

package raftengine

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestIncomingSnapshotCannotLowerAuthority(t *testing.T) {
	t.Parallel()
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "cluster", GroupID: "load", Revision: 9}, Purpose: topology.AuthorityFence, Resources: []topology.AuthorityResource{{Kind: topology.AuthorityGroup, GroupID: "load"}}, Targets: []topology.AuthorityTarget{{NodeName: "n", MemberID: 1, ReplicaID: 1}}}
	g := &Group{opts: Options{ManagedAuthority: true, ClusterID: "cluster", GroupID: "load"}, authority: raftstate.AuthorityState{Grant: &grant, InstallIndex: 4, InstallTerm: 2}}
	for _, kind := range []string{"missing", "lower", "different", "index", "term", "newer_old_index"} {
		t.Run(kind, func(t *testing.T) {
			a := g.authority.Clone()
			switch kind {
			case "missing":
				a = raftstate.AuthorityState{}
			case "lower":
				a.Grant.Key.Revision--
			case "different":
				a.Grant.OperationID = "changed"
			case "index":
				a.InstallIndex++
			case "newer_old_index":
				a.Grant.Key.Revision++
				a.InstallIndex--
			case "term":
				a.InstallTerm = 4
			}
			data, err := encodeSnapshot(g.opts, []byte("app"), a)
			if err != nil {
				t.Fatal(err)
			}
			_, err = g.decodeSnapshot(raftpb.Snapshot{Data: data, Metadata: raftpb.SnapshotMetadata{Index: 8, Term: 3}})
			if !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal(err)
			}
		})
	}
}

func TestSnapshotCannotRewriteDemotionIntentOrFenceBeforeIt(t *testing.T) {
	t.Parallel()
	action := topology.AuthorizedStep{OperationID: topology.MakeOperationID(2, 1), Version: 2, Attempt: 1, StepSeq: 1, Kind: topology.StepDemoteVoter, GroupID: "load", NodeName: "a", ReplicaID: 1, TransferNodeName: "b", TransferReplicaID: 2}
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "cluster", GroupID: "load", Revision: 10}, Purpose: topology.AuthorityEffect, OperationID: action.OperationID, Action: &action, Resources: []topology.AuthorityResource{{Kind: topology.AuthorityGroup, GroupID: "load"}}, Targets: []topology.AuthorityTarget{{NodeName: "a", MemberID: 1, ReplicaID: 1}, {NodeName: "b", MemberID: 2, ReplicaID: 2}}}
	g := &Group{opts: Options{ManagedAuthority: true, ClusterID: "cluster", GroupID: "load"}, authority: raftstate.AuthorityState{Grant: &grant, InstallIndex: 2, InstallTerm: 1, DemotionTransferIndex: 5, DemotionTransferTerm: 2}}
	for _, kind := range []string{"missing", "changed", "newer_before_intent", "newer_before_receipt"} {
		t.Run(kind, func(t *testing.T) {
			next := g.authority.Clone()
			switch kind {
			case "missing":
				next.DemotionTransferIndex, next.DemotionTransferTerm = 0, 0
			case "changed":
				next.DemotionTransferIndex++
			case "newer_before_intent", "newer_before_receipt":
				next.Grant.Key.Revision++
				next.Grant.Action, next.Grant.Purpose = nil, topology.AuthorityFence
				next.InstallIndex, next.InstallTerm = 4, 2
				next.DemotionTransferIndex, next.DemotionTransferTerm = 0, 0
				if kind == "newer_before_receipt" {
					g.authority.Receipt = &topology.EffectReceipt{Key: grant.Key, Resource: grant.Resources[0], Action: &action, Target: grant.Targets[0], Outcome: topology.EffectApplied, AppliedIndex: 7, AppliedTerm: 3}
					next.InstallIndex, next.InstallTerm = 6, 3
				}
			}
			data, err := encodeSnapshot(g.opts, []byte("app"), next)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.decodeSnapshot(raftpb.Snapshot{Data: data, Metadata: raftpb.SnapshotMetadata{Index: 9, Term: 3}}); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal(kind, err)
			}
		})
	}
}
