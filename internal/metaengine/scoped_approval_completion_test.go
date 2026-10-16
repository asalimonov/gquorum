//go:build !functional

package metaengine

import (
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func TestScopedZeroStepApprovalCompletesOnlyProvenGroups(t *testing.T) {
	t.Parallel()
	for _, group := range []string{topology.MetaGroupID, "shard0"} {
		t.Run(group, func(t *testing.T) {
			t.Parallel()
			s, proposal := approvalFixture(t, group)
			next, err := prepareScopedApproval(&s, s.Latest, proposal, explicitApproval)
			if err != nil {
				t.Fatal(err)
			}
			index, err := prepareScopedGraph(next, next.Rules)
			if err != nil {
				t.Fatal("complete candidate cannot restore", err)
			}
			row := index.groups[group]
			if row.SourceVersion != 3 || row.Owner != "" || !reflect.DeepEqual(row.Declaration, next.Target.Group(group)) ||
				next.Operations[2].Status.State != topology.OperationSucceeded {
				t.Fatal("declaration-only approval did not complete atomically", row)
			}
			if group == topology.MetaGroupID {
				if next.Nodes[0].SourceVersion != 3 || next.Nodes[0].Owner != "" || next.FullyConverged.Version != 1 ||
					index.groups["shard0"].Owner != s.Groups[1].Owner {
					t.Fatal("node declaration waited for unrelated work or erased it", next.Nodes, next.FullyConverged)
				}
			} else if next.FullyConverged.Version != 3 || !next.FullyConverged.At.Equal(proposal.At) {
				t.Fatal("matching declarations did not advance the historical marker", next.FullyConverged)
			}
			if !reflect.DeepEqual(index.documents[2], s.Target) {
				t.Fatal("approval lost the old owner's pinned document")
			}
			next.History[0].Nodes[0].Name = "owned-copy"
			if s.History[0].Nodes[0].Name != "n1" {
				t.Fatal("candidate history aliases source")
			}
		})
	}
}

func TestScopedZeroStepMembershipRequiresExactConsumedEvidence(t *testing.T) {
	t.Parallel()
	for _, count := range []int{0, 1, 2} {
		s := scopedAdmissionFixture()
		for seq := 1; seq <= count; seq++ {
			admissionPrefixReceipt(&s, 0, seq)
		}
		index, err := s.buildIndexes()
		if err != nil {
			t.Fatal(err)
		}
		s.indexes = index
		catalog, err := restoreScopedAuthority(&s, index)
		if err != nil {
			t.Fatal(err)
		}
		if got := zeroStepMembership(&s, s.Target, topology.MetaGroupID, catalog); got != (count == 2) {
			t.Fatal("approved membership substituted for exact consumed effects", count, got)
		}
		if count == 2 {
			wrong := s.Target.Clone()
			wrong.MetaGroup().Replicas[1].Role = topology.ReplicaVoter
			if zeroStepMembership(&s, wrong, topology.MetaGroupID, catalog) {
				t.Fatal("learner admission proved an unconsumed promotion")
			}
		}
	}
}

func TestScopedNodeReplacementWaitsForDependentLoadCheckpoint(t *testing.T) {
	t.Parallel()
	s, proposal := readmissionDeletionFixture(t)
	next := s.prepareApprovalOwners(s.Latest, proposal)
	// Isolate the node advancement predicate: metadata membership is proven,
	// while its old exact load owner still lacks a stop/retirement checkpoint.
	next.Groups[0] = groupConvergence{GroupID: topology.MetaGroupID, Declaration: next.Target.MetaGroup(),
		SourceVersion: 3, CompletedAt: proposal.At}
	index, err := next.buildIndexes()
	if err != nil {
		t.Fatal(err)
	}
	op := &next.Operations[len(next.Operations)-1]
	if _, ready := nodeApprovalProof(next.Nodes[0], next, index, op); ready {
		t.Fatal("replacement node ignored its pending former load owner")
	}
	next.Groups[1] = groupConvergence{GroupID: "shard0", SourceVersion: 3, CompletedAt: proposal.At}
	if _, ready := nodeApprovalProof(next.Nodes[0], next, index, op); !ready {
		t.Fatal("proven load exclusion still blocked the node")
	}
}

func TestScopedApprovalRejectsCheckpointClockReversal(t *testing.T) {
	t.Parallel()
	s, proposal := approvalFixture(t, topology.MetaGroupID)
	s.Groups[0].CompletedAt = proposal.At.Add(time.Second)
	if _, err := prepareScopedApproval(&s, s.Latest, proposal, explicitApproval); err == nil {
		t.Fatal("approval moved a proven checkpoint backward in time")
	}
}

func TestScopedApprovalClearsSupersededNodeOwner(t *testing.T) {
	t.Parallel()
	s, proposal := approvalFixture(t, "shard0")
	s.Nodes[0].Owner = s.Operations[1].Status.ID
	var err error
	s.indexes, err = prepareScopedGraph(&s, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	next, err := prepareScopedApproval(&s, s.Latest, proposal, explicitApproval)
	if err != nil {
		t.Fatal(err)
	}
	if next.Nodes[0].Owner != "" || next.Nodes[0].SourceVersion != s.Nodes[0].SourceVersion {
		t.Fatal("matching checkpoint retained a superseded owner or lost provenance", next.Nodes[0])
	}
	if _, err := prepareScopedGraph(next, next.Rules); err != nil {
		t.Fatal("load-only supersession left an invalid node owner", err)
	}
}
