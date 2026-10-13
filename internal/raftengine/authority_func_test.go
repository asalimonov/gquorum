//go:build functional

package raftengine

import (
	"errors"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func foundingAuthority() topology.AuthorityGrant {
	a := topology.AuthorizedStep{OperatorID: topology.MakeOperatorID(1, 1), Version: 1, Attempt: 1, StepSeq: 1, GroupID: "load", Kind: topology.StepCreateFoundingGroup}
	return topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "cluster", GroupID: "load", Revision: 10}, Purpose: topology.AuthorityEffect, OperatorID: a.OperatorID, Action: &a, Resources: []topology.AuthorityResource{{Kind: topology.AuthorityGroup, GroupID: "load"}, {Kind: topology.AuthorityHost, GroupID: "load", NodeName: "1"}}, Targets: []topology.AuthorityTarget{{NodeName: "1", MemberID: 7, ReplicaID: 1}}}
}

func learnerAuthority(revision, replica uint64) topology.AuthorityGrant {
	g := foundingAuthority()
	g.Key.Revision = revision
	g.Action.Kind, g.Action.NodeName, g.Action.ReplicaID = topology.StepAddLearner, "other", replica
	g.Resources = g.Resources[:1]
	g.Targets = append(g.Targets, topology.AuthorityTarget{NodeName: "other", MemberID: 8, ReplicaID: replica})
	return g
}

// A recovered stale ConfChangeV2 must be skipped without wedging the next valid
// configuration. Each case runs one real engine, with absent learner peers.
func TestRejectedManagedConfigurationAllowsLaterChange(t *testing.T) {
	t.Parallel()
	for _, consumed := range []bool{false, true} {
		t.Run(map[bool]string{false: "fenced", true: "already consumed"}[consumed], func(t *testing.T) {
			t.Parallel()
			s := openStore(t, t.TempDir(), "1")
			n := &network{nodes: make(map[uint64]*Group), disconnected: make(map[uint64]bool)}
			initial := foundingAuthority()
			seed := newManagedTestGroup(t, s, &machine{}, n, &initial)
			must(t, seed.Stop(testContext(t)))
			old := learnerAuthority(11, 2)
			installed := old.Clone()
			if !consumed {
				installed.Key.Revision++
				installed.Purpose, installed.Action = topology.AuthorityFence, nil
			}
			install, err := raftstate.EncodeEntry(raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.InstallAuthority, Correlation: "install", Grant: &installed})
			must(t, err)
			context, err := raftstate.EncodeEntry(raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.ManagedConfiguration, Correlation: "old", Grant: &old, Action: old.Action})
			must(t, err)
			data, err := (&raftpb.ConfChangeV2{Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddLearnerNode, NodeID: 2}}, Context: context}).Marshal()
			must(t, err)
			entries := []raftpb.Entry{{Index: 2, Term: 1, Data: install}, {Index: 3, Term: 1, Type: raftpb.EntryConfChangeV2, Data: data}}
			if consumed {
				entries = append(entries, raftpb.Entry{Index: 4, Term: 1, Type: raftpb.EntryConfChangeV2, Data: data})
			}
			last := entries[len(entries)-1].Index
			must(t, s.SaveReady(t.Context(), "load", storage.ReadyWrite{Entries: entries, HardState: raftpb.HardState{Term: 1, Commit: last - 1}}))
			g := newManagedTestGroup(t, s, &machine{}, n, nil)
			must(t, g.Start(t.Context()))
			must(t, g.WaitReady(testContext(t)))
			eventually(t, func() bool { return g.IsLeader() && g.Status().Applied >= last })
			if got := slices.Contains(g.Status().ConfState.Learners, uint64(2)); got != consumed {
				t.Fatal("stale entry changed membership", g.Status())
			}
			if _, found, err := g.AuthorityReceipt(testContext(t), installed.Key); err != nil || found != consumed {
				t.Fatal(found, err)
			}
			next := learnerAuthority(20, 3)
			installTestAuthority(t, g, next)
			cc := raftpb.ConfChangeV2{Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddLearnerNode, NodeID: 3}}}
			var receipt topology.EffectReceipt
			eventually(t, func() bool {
				var err error
				receipt, err = g.ProposeAuthorizedConfChange(testContext(t), next, cc, g.Status().ConfState)
				if errors.Is(err, fault.ErrAuthorityPending) {
					return false
				}
				must(t, err)
				return true
			})
			must(t, receipt.Validate(next))
			if !slices.Contains(g.Status().ConfState.Learners, uint64(3)) {
				t.Fatal("valid change was silently dropped")
			}
			again, found, err := g.AuthorityReceipt(testContext(t), next.Key)
			must(t, err)
			if !found || !reflect.DeepEqual(again, receipt) {
				t.Fatal(again, receipt)
			}
			must(t, g.Snapshot(testContext(t)))
			must(t, g.Stop(testContext(t)))
			restarted := newManagedTestGroup(t, s, &machine{}, n, nil)
			must(t, restarted.Start(t.Context()))
			must(t, restarted.WaitReady(testContext(t)))
			eventually(t, restarted.IsLeader)
			again, found, err = restarted.AuthorityReceipt(testContext(t), next.Key)
			must(t, err)
			if !found || !reflect.DeepEqual(again, receipt) {
				t.Fatal("receipt lost on restart", again, receipt)
			}
		})
	}
}
func newManagedTestGroup(t *testing.T, s storage.Store, m *machine, n *network, initial *topology.AuthorityGrant) *Group {
	t.Helper()
	opts := Options{GroupID: "load", NodeName: "1", ClusterID: "cluster", ReplicaID: 1, MemberID: 7, InstanceID: "process", ManagedAuthority: true, Mode: Joining, Timing: Timing{TickInterval: 10 * time.Millisecond, ElectionTicks: 20, HeartbeatTicks: 1}, Store: s, Machine: m, Transport: endpoint{n}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), InitialAuthority: initial}
	if initial != nil {
		opts.Mode, opts.Peers = Founding, []raft.Peer{{ID: 1}}
	}
	g, err := New(opts)
	must(t, err)
	n.mu.Lock()
	n.nodes[1] = g
	n.mu.Unlock()
	t.Cleanup(func() { must(t, g.Stop(testContext(t))) })
	return g
}
func installTestAuthority(t *testing.T, g *Group, grant topology.AuthorityGrant) topology.FenceEvidence {
	t.Helper()
	var proof topology.FenceEvidence
	eventually(t, func() bool {
		var err error
		proof, err = g.InstallAuthority(testContext(t), grant)
		if errors.Is(err, fault.ErrAuthorityPending) || errors.Is(err, fault.ErrNotLeader) {
			return false
		}
		must(t, err)
		return true
	})
	must(t, proof.Validate(grant))
	return proof
}

func TestManagedAuthorityDurableFloorAndRestart(t *testing.T) {
	t.Parallel()
	s := openStore(t, t.TempDir(), "1")
	n := &network{nodes: make(map[uint64]*Group), disconnected: make(map[uint64]bool)}
	initial := foundingAuthority()
	g := newManagedTestGroup(t, s, &machine{}, n, &initial)
	must(t, g.Start(t.Context()))
	must(t, g.WaitReady(testContext(t)))
	first := installTestAuthority(t, g, initial)
	if first.AppliedIndex != 1 || first.AppliedTerm != 1 {
		t.Fatal(first)
	}
	if err := g.ProposeConfChange(t.Context(), raftpb.ConfChangeV2{}); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal(err)
	}
	if err := g.TransferLeader(t.Context(), 1); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal(err)
	}
	fence := initial.Clone()
	fence.Key.Revision++
	fence.Purpose, fence.Action = topology.AuthorityFence, nil
	proof := installTestAuthority(t, g, fence)
	if proof.AppliedIndex <= first.AppliedIndex {
		t.Fatal(proof)
	}
	if second := installTestAuthority(t, g, fence); second != proof {
		t.Fatal(second, proof)
	}
	if _, err := g.InstallAuthority(testContext(t), initial); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal(err)
	}
	changed := fence.Clone()
	changed.OperatorID = "different"
	if _, err := g.InstallAuthority(testContext(t), changed); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal(err)
	}
	if _, found, err := g.AuthorityReceipt(testContext(t), fence.Key); err != nil || found {
		t.Fatal(found, err)
	}
	must(t, g.Snapshot(testContext(t)))
	must(t, g.Stop(testContext(t)))
	g2 := newManagedTestGroup(t, s, &machine{}, n, nil)
	must(t, g2.Start(t.Context()))
	must(t, g2.WaitReady(testContext(t)))
	if restored := installTestAuthority(t, g2, fence); !reflect.DeepEqual(restored, proof) {
		t.Fatal(restored, proof)
	}
	if _, err := g2.InstallAuthority(testContext(t), initial); !errors.Is(err, fault.ErrAuthorityFenced) {
		t.Fatal(err)
	}
}

func TestManagedMembershipRechecksNativeSafety(t *testing.T) {
	t.Parallel()
	s := openStore(t, t.TempDir(), "1")
	n := &network{nodes: make(map[uint64]*Group), disconnected: make(map[uint64]bool)}
	initial := foundingAuthority()
	g := newManagedTestGroup(t, s, &machine{}, n, &initial)
	must(t, g.Start(t.Context()))
	must(t, g.WaitReady(testContext(t)))
	grant := learnerAuthority(20, 2)
	installTestAuthority(t, g, grant)
	cc := raftpb.ConfChangeV2{Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddLearnerNode, NodeID: 2}}}
	if _, err := g.ProposeAuthorizedConfChange(testContext(t), grant, cc, raftpb.ConfState{Voters: []uint64{1, 2}}); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("stale native state accepted", err)
	}
	wrong := cc
	wrong.Changes = []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddNode, NodeID: 2}}
	if _, err := g.ProposeAuthorizedConfChange(testContext(t), grant, wrong, g.Status().ConfState); !errors.Is(err, fault.ErrUnsafeMembership) {
		t.Fatal("different opcode accepted", err)
	}
	_, err := g.ProposeAuthorizedConfChange(testContext(t), grant, cc, g.Status().ConfState)
	must(t, err)
	promote := grant.Clone()
	promote.Key.Revision++
	promote.Action.Kind = topology.StepPromoteVoter
	installTestAuthority(t, g, promote)
	if _, err := g.ProposeAuthorizedConfChange(testContext(t), promote, wrong, g.Status().ConfState); !errors.Is(err, fault.ErrUnsafeMembership) {
		t.Fatal("uncaught learner promoted", err)
	}
	remove := promote.Clone()
	remove.Key.Revision++
	remove.Action.Kind, remove.Action.NodeName, remove.Action.ReplicaID = topology.StepRemoveReplica, "1", 1
	installTestAuthority(t, g, remove)
	cc.Changes = []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeRemoveNode, NodeID: 1}}
	if _, err := g.ProposeAuthorizedConfChange(testContext(t), remove, cc, g.Status().ConfState); !errors.Is(err, fault.ErrUnsafeMembership) {
		t.Fatal("last voter removed", err)
	}
	if len(g.Status().ConfState.Voters) != 1 {
		t.Fatal(g.Status())
	}
}
