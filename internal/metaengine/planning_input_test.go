//go:build !functional

package metaengine

import (
	"errors"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func planningInputFixture() *topologySM {
	sm := newTopologySM(nil, testRules())
	sm.active = baseDoc()
	sm.active.Version = 1
	sm.target = sm.active.Clone()
	sm.target.Version = 3
	sm.target.Group("shard0").Replicas[0].ReplicaID = 4
	sm.target.Group("shard0").NextReplicaID = 5
	sm.latest = sm.target.Clone()
	sm.rememberAllocations(sm.target)
	return sm
}

func planningGrant(sm *topologySM, id uint64, kind topology.StepKind) *authorityRecord {
	key := topology.AuthorityKey{ClusterID: sm.target.ClusterID, GroupID: "shard0", Revision: uint64(len(sm.authority.records) + 10)}
	target := topology.AuthorityTarget{NodeName: "n1", MemberID: 1, ReplicaID: id}
	record := &authorityRecord{Grant: topology.AuthorityGrant{Key: key, Targets: []topology.AuthorityTarget{target},
		Action: &topology.AuthorizedStep{GroupID: key.GroupID, NodeName: target.NodeName, ReplicaID: id, Kind: kind}}}
	sm.authority.records[key] = record
	return record
}

func TestPlanningInputUsesIssuedEffectsWithoutInventingConvergence(t *testing.T) {
	t.Parallel()
	sm := planningInputFixture()
	prepare := planningGrant(sm, 2, topology.StepPrepareReplica)
	prepare.Closed = true // Refused or fenced issuance can still leave state.
	planningGrant(sm, 3, topology.StepCreateFoundingGroup)
	desired := sm.target.Clone()
	input, err := sm.PlanningInput(t.Context(), desired, nil)
	if err != nil {
		t.Fatal(err)
	}
	group := input.Retained["shard0"]
	if !group.FoundingIssued || len(group.Replicas) != 2 || group.Replicas[0].ReplicaID != 2 ||
		group.Replicas[1].ReplicaID != 3 || group.Replicas[0].Progress != topology.RetainedHosted {
		t.Fatal("successive issued identities were lost", group)
	}
	if input.Groups["shard0"].Converged.Replicas[0].ReplicaID != 1 || input.Groups["shard0"].ConvergedVersion != 1 {
		t.Fatal("retained effects became fabricated convergence", input.Groups)
	}
	input.Approved.Groups[1].Replicas[0].ReplicaID = 99
	input.Groups["shard0"].Converged.Replicas[0].ReplicaID = 99
	group.Replicas[0].ReplicaID = 99
	if sm.active.Group("shard0").Replicas[0].ReplicaID != 1 || sm.target.Group("shard0").Replicas[0].ReplicaID != 4 {
		t.Fatal("planning input aliases metadata")
	}
	prepare.Effects = []topology.EffectReceipt{{Target: prepare.Grant.Targets[0], Outcome: topology.EffectApplied}}
	desired.Group("shard0").Replicas = []topology.Replica{{NodeName: "n1", ReplicaID: 3, Role: topology.ReplicaVoter}}
	input, err = sm.PlanningInput(t.Context(), desired, nil)
	if err != nil || !input.Retained["shard0"].FoundingIssued || len(input.Retained["shard0"].Replicas) != 1 {
		t.Fatal("desired subtraction erased founding evidence", input.Retained, err)
	}
}

func TestPlanningInputApprovalAloneDoesNotCreateHostState(t *testing.T) {
	t.Parallel()
	sm := planningInputFixture()
	desired := sm.target.Clone()
	desired.Groups = desired.Groups[:1]
	input, err := sm.PlanningInput(t.Context(), desired, nil)
	if err != nil || len(input.Retained) != 0 {
		t.Fatal("unissued approved intent became host state", input.Retained, err)
	}
	native := map[string]topology.NativeState{"shard0": {GroupID: "shard0", ReplicaID: 4, Applied: 20,
		ConfState: raftpb.ConfState{Voters: []uint64{4}}}}
	input, err = sm.PlanningInput(t.Context(), desired, native)
	if err != nil || len(input.Retained["shard0"].Replicas) != 1 ||
		input.Retained["shard0"].Replicas[0].Progress != topology.RetainedHosted {
		t.Fatal("confirmed native membership lost its exact host identity", input.Retained, err)
	}
}

func TestPlanningInputClassifiesNativeRemovalEvidence(t *testing.T) {
	t.Parallel()
	sm := planningInputFixture()
	record := planningGrant(sm, 2, topology.StepRemoveReplica)
	receipt := topology.EffectReceipt{Target: record.Grant.Targets[0], Outcome: topology.EffectApplied, AppliedIndex: 20}
	record.Effects = []topology.EffectReceipt{receipt}
	native := map[string]topology.NativeState{"shard0": {GroupID: "shard0", ReplicaID: 4, Applied: 19,
		ConfState: raftpb.ConfState{Voters: []uint64{2, 4}}}}
	if _, err := sm.PlanningInput(t.Context(), sm.target, native); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("stale native read did not request fresh evidence", err)
	}
	state := native["shard0"]
	state.Applied = 20
	native["shard0"] = state
	if _, err := sm.PlanningInput(t.Context(), sm.target, native); !errors.Is(err, fault.ErrUnsafeMembership) {
		t.Fatal("recent contradictory native membership was accepted", err)
	}
	state.ConfState.Voters, state.ReplicaID = []uint64{4}, 2 // A reporter is not necessarily a native member.
	native["shard0"] = state
	input, err := sm.PlanningInput(t.Context(), sm.target, native)
	if err != nil || input.Retained["shard0"].Replicas[0].Progress != topology.RetainedCleanup {
		t.Fatal("consumed removal did not retain cleanup ownership", input.Retained, err)
	}
	input.Native["shard0"].ConfState.Voters[0] = 99
	if native["shard0"].ConfState.Voters[0] != 4 {
		t.Fatal("native membership was not copied")
	}
	stopped := planningGrant(sm, 2, topology.StepStopReplica)
	stopped.Effects = []topology.EffectReceipt{{Target: stopped.Grant.Targets[0], Outcome: topology.EffectApplied}}
	input, err = sm.PlanningInput(t.Context(), sm.target, nil)
	if err != nil || input.Retained["shard0"].Replicas[0].Progress != topology.RetainedStopped {
		t.Fatal("completed stop was lost", input.Retained, err)
	}
}

func TestPlanningInputRejectsForeignBindingsAndAllocationLoss(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"host", "member", "floor", "desired", "unknown native"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			sm := planningInputFixture()
			record := planningGrant(sm, 2, topology.StepPrepareReplica)
			desired := sm.target.Clone()
			var native map[string]topology.NativeState
			switch mode {
			case "host", "member":
				other := planningGrant(sm, 2, topology.StepPrepareReplica)
				if mode == "host" {
					other.Grant.Targets[0].NodeName = "foreign"
				} else {
					other.Grant.Targets[0].MemberID++
				}
			case "floor":
				record.Grant.Targets[0].ReplicaID = 5
			case "desired":
				desired.Group("shard0").Replicas[0] = topology.Replica{NodeName: "foreign", ReplicaID: 2}
			case "unknown native":
				native = map[string]topology.NativeState{"shard0": {GroupID: "shard0", ReplicaID: 4,
					Applied: 20, ConfState: raftpb.ConfState{Voters: []uint64{4, 99}}}}
			}
			if _, err := sm.PlanningInput(t.Context(), desired, native); !errors.Is(err, fault.ErrIdentityMismatch) {
				t.Fatal("contradictory retained identity accepted", mode, err)
			}
		})
	}
}

func TestPlanningInputReadmissionRequiresCompleteExactBatch(t *testing.T) {
	t.Parallel()
	sm := planningInputFixture()
	admission := AdmissionRecord{NodeName: "n1", MemberID: 2, InstanceID: "replacement", OperatorID: "exec-v4-a1"}
	sm.admissions["n1"] = &admission
	var records []*authorityRecord
	for _, group := range []string{"shard0", topology.MetaGroupID} {
		resource := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: group, NodeName: "n1"}
		key := topology.AuthorityKey{ClusterID: sm.target.ClusterID, GroupID: group, Revision: 20}
		target := topology.AuthorityTarget{NodeName: "n1", MemberID: 1, ReplicaID: 1}
		record := &authorityRecord{Grant: topology.AuthorityGrant{Key: key, Purpose: topology.AuthorityReadmit,
			AdmissionID: ReadmissionID(admission), Targets: []topology.AuthorityTarget{target}, Resources: []topology.AuthorityResource{resource}}}
		record.Effects = []topology.EffectReceipt{{Key: key, Resource: resource, Target: target, Outcome: topology.EffectApplied}}
		sm.authority.records[key], records = record, append(records, record)
	}
	native := map[string]topology.NativeState{"shard0": {GroupID: "shard0", ReplicaID: 4, Applied: 20,
		ConfState: raftpb.ConfState{Voters: []uint64{1, 4}}}}
	input, err := sm.PlanningInput(t.Context(), sm.target, native)
	if err != nil || input.Retained["shard0"].Replicas[0].Progress != topology.RetainedStopped {
		t.Fatal("complete readmission did not prove the exact old host stopped", input.Retained, err)
	}
	debt := cleanupRecord{Identity: storage.Identity{ClusterID: sm.target.ClusterID, GroupID: "shard0", NodeName: "n1", ReplicaID: 1},
		MemberID: 1, ReadmissionKey: &records[0].Grant.Key, Stopped: &records[0].Effects[0]}
	before := debt
	removed := planningGrant(sm, 1, topology.StepRemoveReplica)
	removed.Effects = []topology.EffectReceipt{{Target: removed.Grant.Targets[0], Outcome: topology.EffectApplied,
		AppliedIndex: 21}}
	_, err = collectPlanningEvidence(topology.PlanInput{}, sm.target, planningEvidence{
		documents: sm.authorityDocumentsLocked(), authority: sm.authority, allocations: sm.allocations,
		admissions: []AdmissionRecord{admission}, cleanup: []cleanupRecord{debt}})
	if err != nil || !reflect.DeepEqual(debt, before) || debt.Removed != nil {
		t.Fatal("readmission debt was rewritten as native-removal debt", err)
	}
	delete(sm.authority.records, removed.Grant.Key)
	records[1].Effects = nil
	input, err = sm.PlanningInput(t.Context(), sm.target, native)
	if err != nil || input.Retained["shard0"].Replicas[0].Progress != topology.RetainedHosted {
		t.Fatal("partial readmission proved host retirement", input.Retained, err)
	}
}

func TestPlanningInputPreservesCompletedDeletionCheckpoint(t *testing.T) {
	t.Parallel()
	sm := planningInputFixture()
	sm.active.Groups, sm.active.Version = sm.active.Groups[:1], 5
	sm.target = sm.active.Clone()
	sm.retired["shard0"] = 4
	input, err := sm.PlanningInput(t.Context(), sm.target, nil)
	if err != nil || input.Groups["shard0"].Converged != nil || input.Groups["shard0"].ConvergedVersion != 4 {
		t.Fatal("completed deletion became an unknown initial group", input.Groups, err)
	}
}
