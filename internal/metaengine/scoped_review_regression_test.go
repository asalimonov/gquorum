//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestScopedAbandonedPreparationLeavesOnlyDetachedDebt(t *testing.T) {
	t.Parallel()
	s := loadAdmissionFixture(t)
	admissionPrefixReceipt(&s, 0, 1)
	s.Admissions[0].State = AdmissionReady
	prepareScopedFixture(t, &s)
	action := scopedPlanAction(&s.Operations[0], s.Operations[0].Plan.Steps[2])
	if _, err := s.applyBeginEffect(11, BeginEffectProposal{Action: action}); err != nil {
		t.Fatal(err)
	}
	s.Latest = s.Target.Clone()
	s.Latest.Version, s.Latest.ParentVersion = 3, 2
	s.Latest.Group("shard0").Replicas = s.Latest.Group("shard0").Replicas[:1]
	prepareScopedFixture(t, &s)
	p := BeginOperationProposal{Version: 3, Actor: "admin", Origin: topology.OriginAPI,
		At: s.Operations[0].UpdatedAt.Add(time.Second), ProposalID: "revert",
		Scopes: []ScopeExpectation{{GroupID: "shard0", SourceVersion: 1, Owner: s.Groups[1].Owner}},
		Plan: topology.Plan{Version: 3, Steps: []topology.Step{
			{Seq: 1, Kind: topology.StepRemoveReplica, GroupID: "shard0", NodeName: "n2", ReplicaID: 2,
				ExpectedVotersDigest: topology.VoterSetDigest([]uint64{1}), ResultVotersDigest: topology.VoterSetDigest([]uint64{1})},
			{Seq: 2, Kind: topology.StepStopReplica, GroupID: "shard0", NodeName: "n2", ReplicaID: 2},
			{Seq: 3, Kind: topology.StepCleanupReplicaState, GroupID: "shard0", NodeName: "n2", ReplicaID: 2}},
			VoterBases: []topology.PlanVoterBase{{GroupID: "shard0", Voters: []uint64{1}}},
			Blocks:     []topology.PlanBlock{{GroupID: "shard0", SourceVersion: 1, Steps: []int{1}, Cleanup: []int{2, 3}}}}}
	applyScopedApprovalFixture(t, &s, p)
	op := s.Operations[len(s.Operations)-1]
	applyScopedTransitionFixture(t, &s, scopedTransitionFor(op, "shard0", 1))
	issued, err := s.applyBeginEffect(s.AppliedIndex+1, BeginEffectProposal{Action: scopedPlanAction(&op, op.Plan.Steps[0])})
	if err != nil {
		t.Fatal(err)
	}
	recordScopedEffectFixture(t, &s, RecordEffectProposal{Receipt: fixtureEffectReceipts(*issued.result.Grant)[0],
		At: op.UpdatedAt.Add(time.Second)})
	// The next declaration-only approval must recognize exclusion even though
	// the older host Prepare never answered and its cleanup fence is unsettled.
	s.Latest = s.Target.Clone()
	s.Latest.Version, s.Latest.ParentVersion = 4, 3
	s.Latest.Group("shard0").Labels = map[string]string{"change": "label"}
	p.Version, p.At = 4, p.At.Add(time.Minute)
	p.Plan = topology.Plan{Version: 4, Blocks: []topology.PlanBlock{{GroupID: "shard0", SourceVersion: 1}}}
	p.Scopes[0].Owner = op.Status.ID
	prepareScopedFixture(t, &s)
	applyScopedApprovalFixture(t, &s, p)
	row := s.indexes.groups["shard0"]
	if row.Owner != "" || row.SourceVersion != 4 || len(s.Cleanup) != 1 ||
		s.Cleanup[0].Removed == nil || s.Cleanup[0].Stopped != nil || s.Cleanup[0].RetireAction == nil {
		t.Fatal("dead prepared host blocked convergence or lost retirement debt", row, s.Cleanup)
	}
}

func TestScopedUnprovenNativeMembershipGetsExplicitBlocker(t *testing.T) {
	t.Parallel()
	s, _ := approvalFixture(t, "shard0")
	// Isolate completion preparation: an issued native Add may have run even
	// after its grant closes. The settled fence is not its outcome receipt.
	action := topology.AuthorizedStep{OperationID: "exec-v2-a1", Version: 2, Attempt: 1, StepSeq: 2,
		GroupID: "shard0", Kind: topology.StepAddLearner, NodeName: "n1", ReplicaID: 2}
	resource := topology.AuthorityResource{Kind: topology.AuthorityGroup, GroupID: "shard0"}
	catalog := s.authorityCatalog().clone()
	key := topology.AuthorityKey{ClusterID: s.Target.ClusterID, GroupID: "shard0", Revision: 8}
	catalog.records[key] = &authorityRecord{Closed: true, Grant: topology.AuthorityGrant{
		Key: key, Purpose: topology.AuthorityEffect, Action: &action, Resources: []topology.AuthorityResource{resource}}}
	if zeroStepMembership(&s, s.Latest, "shard0", catalog) {
		t.Fatal("closed native authority manufactured nonexecution proof")
	}
	s = scopedAdmissionFixture()
	admissionPrefixReceipt(&s, 0, 1)
	admissionPrefixReceipt(&s, 0, 2)
	s.AuthorityRecords[1].Effects = nil
	s.Admissions[0].State = AdmissionReady
	prepareScopedFixture(t, &s)
	s.Latest = s.Target.Clone()
	s.Latest.Version, s.Latest.ParentVersion = 3, 2
	s.Latest.Node("n1").Labels = map[string]string{"change": "label"}
	p := BeginOperationProposal{Version: 3, Actor: "admin", Origin: topology.OriginAPI,
		At: s.Operations[0].UpdatedAt.Add(time.Second), ProposalID: "uncertain",
		Scopes: []ScopeExpectation{{GroupID: topology.MetaGroupID, SourceVersion: 1, Owner: s.Operations[0].Status.ID}},
		Plan:   topology.Plan{Version: 3, Blocks: []topology.PlanBlock{{GroupID: topology.MetaGroupID, SourceVersion: 1}}}}
	prepareScopedFixture(t, &s)
	// Admission handover keeps its full sequence; test the completion stage
	// directly so no malformed zero-step admission plan is claimed admissible.
	next := s.prepareApprovalOwners(s.Latest, p)
	if err := prepareApprovalCompletion(&s, next); err != nil {
		t.Fatal(err)
	}
	b := next.Operations[len(next.Operations)-1].Status.Blocks[0]
	if b.State != topology.OperationWaitingForReplica || b.Blocked == nil || b.Blocked.Code != "membership_unproven" {
		t.Fatal("uncertain membership stayed silently pending", b)
	}
}

func TestScopedApprovalReplayChecksPlanAndDoesNotRevokeSucceededBlock(t *testing.T) {
	t.Parallel()
	s, p := approvalFixture(t, topology.MetaGroupID)
	applyScopedApprovalFixture(t, &s, p)
	if slices.ContainsFunc(s.Revocations, func(r scopedRevocationRecord) bool {
		return r.OperationID == "exec-v2-a2" && r.GroupID == topology.MetaGroupID
	}) {
		t.Fatal("approval invented revocation for a succeeded sibling")
	}
	before, _ := json.Marshal(s)
	p.Scopes, p.Plan.Steps = nil, []topology.Step{}
	result, err := s.applyBeginOperation(s.AppliedIndex+1, p)
	if err != nil || !result.result.Accepted || result.changed {
		t.Fatal("canonical plan replay depends on old CAS captures", result, err)
	}
	p.Plan.Blocks[0].SourceVersion++
	result, err = s.applyBeginOperation(s.AppliedIndex+1, p)
	after, _ := json.Marshal(s)
	if err != nil || result.result.Accepted || result.result.OperationID != "exec-v3-a1" || !bytes.Equal(before, after) {
		t.Fatal("different plan was accepted as an approval replay", result, err)
	}
}

func TestScopedConsumedResourcesSurviveNewFloorsAndExecutorCannotSupersede(t *testing.T) {
	t.Parallel()
	s := scopedGraphFixture()
	prepareScopedFixture(t, &s)
	catalog := s.authorityCatalog()
	record := catalog.records[s.AuthorityRecords[0].Grant.Key]
	if err := catalog.currentGrant(record); err != nil {
		t.Fatal("consumed resource was fenced by a later floor", err)
	}
	copy := *record
	copy.Effects = nil
	if !errors.Is(catalog.currentGrant(&copy), fault.ErrAuthorityFenced) {
		t.Fatal("unconsumed stale resource was reusable")
	}
	transition := scopedTransitionFor(s.Operations[1], "shard0", 2)
	transition.State = topology.OperationSuperseded
	before, _ := json.Marshal(s)
	if _, err := s.applyTransitionOperation(11, TransitionOperationProposal{Transition: transition}); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("executor invented supersession", err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("refused transition changed ownership")
	}
}
