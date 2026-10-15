//go:build !functional

package metaengine

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedCompletionInheritsExactPrefixWithoutReissuing(t *testing.T) {
	t.Parallel()
	s := scopedGraphFixture()
	for i := range s.Operations {
		op := &s.Operations[i]
		op.Plan.Steps = append(op.Plan.Steps, topology.Step{Seq: 3, Kind: topology.StepTransferLeader,
			GroupID: "shard0", NodeName: "n1", ReplicaID: 1})
		op.Plan.Blocks[1].Steps = []int{2, 3}
		op.Status.Blocks[1].CurrentStep, op.Status.Blocks[1].TotalSteps = 3, 2
	}
	grant := s.AuthorityRecords[0].Grant.Clone()
	grant.Key.GroupID, grant.Key.Revision = "shard0", 5
	grant.Action.GroupID, grant.Action.StepSeq = "shard0", 2
	grant.Resources[0].GroupID = "shard0"
	s.AuthorityRecords = append(s.AuthorityRecords, scopedAuthorityRecord{Class: "execution",
		authorityRecord: authorityRecord{Grant: grant, Effects: fixtureEffectReceipts(grant), Closed: true}})
	s.AuthorityFloors = append(s.AuthorityFloors, resourceFloor{Resource: grant.Resources[0], Key: grant.Key})
	if _, err := prepareScopedGraph(&s, s.Rules); err != nil {
		t.Fatal(err)
	}
	if len(s.AuthorityRecords) != 3 || s.AuthorityRecords[2].Grant.Action.Attempt != 1 {
		t.Fatal("retry manufactured successor receipts")
	}
	s.AuthorityRecords[2].Effects = nil
	if _, err := prepareScopedGraph(&s, s.Rules); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("completed retry prefix accepted without original receipt", err)
	}
}

func TestScopedCompletionFenceDoesNotProveTheEffect(t *testing.T) {
	t.Parallel()
	s := scopedGraphFixture()
	s.AuthorityRecords[0].Effects = nil // Newer fence still settles its resource.
	if _, err := prepareScopedGraph(&s, s.Rules); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("fencing manufactured successful sibling completion", err)
	}
}
