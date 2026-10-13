//go:build !functional

package metaengine

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func admissionOperatorSM(t *testing.T) (*topologySM, AdmissionRecord, topology.Plan, time.Time, uint64) {
	t.Helper()
	return admissionOperatorForRole(t, topology.ReplicaVoter)
}

func admissionOperatorForRole(t *testing.T, role topology.ReplicaRole) (*topologySM, AdmissionRecord, topology.Plan, time.Time, uint64) {
	t.Helper()
	sm := newTopologySM(nil, testRules())
	at := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	if res := apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "seed"}); !res.Accepted {
		t.Fatalf("seed: %s", res.Reason)
	}
	founding := apply(t, sm, 2, BeginOperatorProposal{
		Version: 1, Plan: topology.Plan{Version: 1}, Actor: "n1", At: at, ProposalID: "founding",
	})
	if !founding.Accepted {
		t.Fatalf("begin founding activation: %s", founding.Reason)
	}
	if res := apply(t, sm, 3, TransitionOperatorProposal{Transition: topology.OperatorTransition{
		OperatorID: founding.OperatorID, Version: 1, Attempt: 1,
		State: topology.OperatorSucceeded, At: at.Add(time.Second),
	}, ProposalID: "founding-succeeded"}); !res.Accepted {
		t.Fatalf("complete founding activation: %s", res.Reason)
	}
	if res := apply(t, sm, 4, ActivateTargetProposal{
		OperatorID: founding.OperatorID, Version: 1, Attempt: 1, ProposalID: "activate-founding",
	}); !res.Accepted {
		t.Fatalf("activate founding: %s", res.Reason)
	}

	desired := sm.Active()
	desired.Nodes = append(desired.Nodes, topology.Node{Name: "n2", Type: "worker", State: topology.NodeActive})
	desired.MetaGroup().Replicas = append(desired.MetaGroup().Replicas, topology.Replica{
		NodeName: "n2", ReplicaID: 2, Role: role,
	})
	desired.MetaGroup().NextReplicaID = 3
	desired.ParentVersion = desired.Version
	desired.ChangedBy = "operator"
	desired.Origin = topology.OriginAPI
	if res := apply(t, sm, 5, ProposeDocument{Doc: desired, ExpectedVersion: 1, ProposalID: "target"}); !res.Accepted {
		t.Fatalf("propose target: %s", res.Reason)
	}
	target := sm.Current()
	plan, err := topology.BuildPlan(sm.Active(), target, assumedReadmissionStatuses(sm.Active(), ""), testRules())
	if err != nil {
		t.Fatal(err)
	}
	started := apply(t, sm, 6, BeginOperatorProposal{
		Version: target.Version, Plan: plan, Actor: target.ChangedBy, Origin: target.Origin,
		At: at.Add(2 * time.Second), ProposalID: "begin-target",
	})
	if !started.Accepted {
		t.Fatalf("begin target: %s", started.Reason)
	}
	return sm, AdmissionRecord{
		NodeName: "n2", InstanceID: "instance-n2", MemberID: 2, OperatorID: started.OperatorID,
	}, plan, at, 7
}

func TestAdmissionStableLearnerCompletesAndSurvivesSnapshot(t *testing.T) {
	t.Parallel()
	sm, admission, plan, at, index := admissionOperatorForRole(t, topology.ReplicaLearner)
	at = at.Add(3 * time.Second)
	if len(plan.Steps) != 2 {
		t.Fatal("learner admission includes promotion", plan)
	}
	if res := apply(t, sm, index, BindAdmissionProposal{Admission: admission, At: at}); !res.Accepted {
		t.Fatal(res)
	}
	index++
	if res := apply(t, sm, index, ReadyAdmissionProposal{NodeName: admission.NodeName, InstanceID: admission.InstanceID, MemberID: admission.MemberID, OperatorID: admission.OperatorID}); !res.Accepted {
		t.Fatal(res)
	}
	index++
	for _, step := range plan.Steps {
		applyAdmissionTransition(t, sm, index, admission.OperatorID, plan, step.Seq, topology.OperatorRunning, at)
		index++
		index = consumeTestStep(t, sm, admission.OperatorID, index)
	}
	applyAdmissionTransition(t, sm, index, admission.OperatorID, plan, 2, topology.OperatorSucceeded, at)
	index++
	requireAdmissionState(t, sm, admission.InstanceID, AdmissionLearner)
	if res := apply(t, sm, index, ActivateTargetProposal{OperatorID: admission.OperatorID, Version: plan.Version, Attempt: 1}); !res.Accepted {
		t.Fatal(res)
	}
	requireAdmissionState(t, sm, admission.InstanceID, AdmissionCompleted)
	data, err := sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := newTopologySM(nil, testRules())
	if err := restored.Restore(data); err != nil {
		t.Fatal(err)
	}
	requireAdmissionState(t, restored, admission.InstanceID, AdmissionCompleted)
	active := restored.Active()
	if active.MetaGroup().Replicas[1].Role != topology.ReplicaLearner {
		t.Fatal("learner promoted during restoration")
	}
}

func applyAdmissionTransition(
	t *testing.T,
	sm *topologySM,
	index uint64,
	id topology.OperatorID,
	plan topology.Plan,
	stepSeq int,
	state topology.OperatorState,
	at time.Time,
) {
	t.Helper()
	step := plan.Steps[stepSeq-1]
	res := apply(t, sm, index, TransitionOperatorProposal{Transition: topology.OperatorTransition{
		OperatorID: id, Version: plan.Version, Attempt: 1, StepSeq: stepSeq,
		State: state, GroupID: step.GroupID, NodeName: step.NodeName, At: at,
	}, ProposalID: fmt.Sprintf("transition-%d-%s", stepSeq, state)})
	if !res.Accepted {
		t.Fatalf("transition step %d to %s: %s", stepSeq, state, res.Reason)
	}
}

func requireAdmissionState(t *testing.T, sm *topologySM, instanceID string, want AdmissionState) AdmissionRecord {
	t.Helper()
	record, ok := sm.Admission("n2", instanceID)
	if !ok || record.State != want {
		t.Fatalf("admission n2/%s = (%+v,%v), want state %s", instanceID, record, ok, want)
	}
	return record
}

func TestAdmission_SameInstanceRetryAcrossPhasesAndSnapshot(t *testing.T) {
	t.Parallel()
	sm, admission, plan, at, index := admissionOperatorSM(t)
	bind := BindAdmissionProposal{Admission: admission, At: at.Add(3 * time.Second), ProposalID: "bind"}
	if res := apply(t, sm, index, bind); !res.Accepted || res.Admission.State != AdmissionPending {
		t.Fatalf("bind = %+v, want pending admission", res)
	}
	index++
	bind.ProposalID = "bind-retry-pending"
	if res := apply(t, sm, index, bind); !res.Accepted || res.Admission.OperatorID != admission.OperatorID {
		t.Fatalf("pending Join retry = %+v, want same execution", res)
	}
	index++

	wrong := apply(t, sm, index, ReadyAdmissionProposal{
		NodeName: "n2", InstanceID: admission.InstanceID, MemberID: 99,
		OperatorID: admission.OperatorID, ProposalID: "ready-wrong-member",
	})
	if wrong.Accepted {
		t.Fatal("JoinReady accepted a different member tuple")
	}
	index++
	ready := ReadyAdmissionProposal{
		NodeName: "n2", InstanceID: admission.InstanceID, MemberID: admission.MemberID,
		OperatorID: admission.OperatorID, ProposalID: "ready",
	}
	if res := apply(t, sm, index, ready); !res.Accepted || res.Admission.State != AdmissionReady {
		t.Fatalf("JoinReady = %+v, want ready", res)
	}
	index++
	ready.ProposalID = "ready-duplicate"
	if res := apply(t, sm, index, ready); !res.Accepted || res.Admission.State != AdmissionReady {
		t.Fatalf("duplicate JoinReady = %+v, want idempotent ready", res)
	}
	index++
	bind.ProposalID = "bind-retry-ready"
	if res := apply(t, sm, index, bind); !res.Accepted || res.Admission.State != AdmissionReady {
		t.Fatalf("ready Join retry = %+v", res)
	}
	index++

	snapshot, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if bytes.Contains(snapshot, []byte("9090")) {
		t.Fatalf("admission snapshot persisted a resolver address: %s", snapshot)
	}
	restored := newTopologySM(nil, testRules())
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got := requireAdmissionState(t, restored, admission.InstanceID, AdmissionReady); got != requireAdmissionState(t, sm, admission.InstanceID, AdmissionReady) {
		t.Fatalf("restored admission = %+v, want %+v", got, admission)
	}
	sm = restored

	applyAdmissionTransition(t, sm, index, admission.OperatorID, plan, 1, topology.OperatorRunning, at.Add(4*time.Second))
	index++
	index = consumeTestStep(t, sm, admission.OperatorID, index)
	applyAdmissionTransition(t, sm, index, admission.OperatorID, plan, 2, topology.OperatorRunning, at.Add(5*time.Second))
	index++
	index = consumeTestStep(t, sm, admission.OperatorID, index)
	applyAdmissionTransition(t, sm, index, admission.OperatorID, plan, 3, topology.OperatorRunning, at.Add(6*time.Second))
	index++
	index = consumeTestStep(t, sm, admission.OperatorID, index)
	requireAdmissionState(t, sm, admission.InstanceID, AdmissionLearner)
	bind.ProposalID = "bind-retry-learner"
	if res := apply(t, sm, index, bind); !res.Accepted || res.Admission.State != AdmissionLearner {
		t.Fatalf("learner Join retry = %+v", res)
	}
	index++
	ready.ProposalID = "ready-retry-learner"
	if res := apply(t, sm, index, ready); !res.Accepted || res.Admission.State != AdmissionLearner {
		t.Fatalf("learner JoinReady retry = %+v", res)
	}
	index++

	applyAdmissionTransition(t, sm, index, admission.OperatorID, plan, 4, topology.OperatorRunning, at.Add(7*time.Second))
	index++
	index = consumeTestStep(t, sm, admission.OperatorID, index)
	applyAdmissionTransition(t, sm, index, admission.OperatorID, plan, 4, topology.OperatorSucceeded, at.Add(8*time.Second))
	index++
	requireAdmissionState(t, sm, admission.InstanceID, AdmissionPromoted)
	bind.ProposalID = "bind-retry-promoted"
	if res := apply(t, sm, index, bind); !res.Accepted || res.Admission.State != AdmissionPromoted {
		t.Fatalf("promoted Join retry = %+v", res)
	}
	index++
	if res := apply(t, sm, index, ActivateTargetProposal{
		OperatorID: admission.OperatorID, Version: plan.Version, Attempt: 1, ProposalID: "activate",
	}); !res.Accepted {
		t.Fatalf("activate target: %s", res.Reason)
	}
	index++
	requireAdmissionState(t, sm, admission.InstanceID, AdmissionCompleted)
	bind.ProposalID = "bind-retry-completed"
	if res := apply(t, sm, index, bind); !res.Accepted || res.Admission.State != AdmissionCompleted {
		t.Fatalf("completed Join retry = %+v", res)
	}
	index++
	ready.ProposalID = "ready-retry-completed"
	if res := apply(t, sm, index, ready); !res.Accepted || res.Admission.State != AdmissionCompleted {
		t.Fatalf("completed JoinReady retry = %+v", res)
	}
}

func TestAdmission_SupersededInstanceCannotBeResurrected(t *testing.T) {
	t.Parallel()
	sm, oldAdmission, plan, at, index := admissionOperatorSM(t)
	if res := apply(t, sm, index, BindAdmissionProposal{
		Admission: oldAdmission, At: at.Add(3 * time.Second), ProposalID: "bind-old",
	}); !res.Accepted {
		t.Fatalf("bind old: %s", res.Reason)
	}
	index++
	newAdmission := oldAdmission
	newAdmission.InstanceID = "instance-n2-new"
	if res := apply(t, sm, index, BindAdmissionProposal{
		Admission: newAdmission, ReplaceInstance: true, At: at.Add(4 * time.Second), ProposalID: "bind-new",
	}); !res.Accepted {
		t.Fatalf("replace admission: %s", res.Reason)
	}
	index++
	requireAdmissionState(t, sm, oldAdmission.InstanceID, AdmissionSuperseded)

	for seq := 1; seq <= len(plan.Steps); seq++ {
		state := topology.OperatorRunning
		applyAdmissionTransition(t, sm, index, oldAdmission.OperatorID, plan, seq, state, at.Add(time.Duration(4+seq)*time.Second))
		index++
		index = consumeTestStep(t, sm, oldAdmission.OperatorID, index)
	}
	applyAdmissionTransition(t, sm, index, oldAdmission.OperatorID, plan, len(plan.Steps), topology.OperatorSucceeded, at.Add(10*time.Second))
	index++
	if res := apply(t, sm, index, ActivateTargetProposal{
		OperatorID: oldAdmission.OperatorID, Version: plan.Version, Attempt: 1, ProposalID: "activate",
	}); !res.Accepted {
		t.Fatalf("activate: %s", res.Reason)
	}
	requireAdmissionState(t, sm, oldAdmission.InstanceID, AdmissionSuperseded)
	requireAdmissionState(t, sm, newAdmission.InstanceID, AdmissionCompleted)
	if res := apply(t, sm, index+1, ReadyAdmissionProposal{
		NodeName: oldAdmission.NodeName, InstanceID: oldAdmission.InstanceID,
		MemberID: oldAdmission.MemberID, OperatorID: oldAdmission.OperatorID, ProposalID: "old-ready",
	}); res.Accepted {
		t.Fatal("superseded instance JoinReady was accepted")
	}
}

func TestAdmission_DifferentInstanceCannotReusePreparedMemberID(t *testing.T) {
	t.Parallel()
	sm, admission, _, at, index := admissionOperatorSM(t)
	if res := apply(t, sm, index, BindAdmissionProposal{
		Admission: admission, At: at.Add(3 * time.Second), ProposalID: "bind-old",
	}); !res.Accepted {
		t.Fatalf("bind old: %s", res.Reason)
	}
	index++
	if res := apply(t, sm, index, ReadyAdmissionProposal{
		NodeName: admission.NodeName, InstanceID: admission.InstanceID,
		MemberID: admission.MemberID, OperatorID: admission.OperatorID, ProposalID: "ready-old",
	}); !res.Accepted {
		t.Fatalf("ready old: %s", res.Reason)
	}
	index++
	replacement := admission
	replacement.InstanceID = "instance-n2-new"
	res := apply(t, sm, index, BindAdmissionProposal{
		Admission: replacement, ReplaceInstance: true, At: at.Add(4 * time.Second), ProposalID: "bind-new",
	})
	if res.Accepted {
		t.Fatal("different instance reused a prepared member ID")
	}
	requireAdmissionState(t, sm, admission.InstanceID, AdmissionReady)
}

func TestBuildReadmissionPlan_ShrinksBeforeLearnerFirstReplacementAtMaximum(t *testing.T) {
	t.Parallel()
	for _, count := range []int{3, 5, 7} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			rules := topology.Rules{SchemaVersion: 1, Limits: topology.Limits{MaxNodes: count, MaxReplicas: 5}}

			nodes := make([]topology.Node, 0, rules.Limits.MaxNodes)
			metaReplicas := make([]topology.Replica, 0, rules.Limits.MaxNodes)
			for i := 1; i <= rules.Limits.MaxNodes; i++ {
				name := fmt.Sprintf("n%d", i)
				nodes = append(nodes, topology.Node{Name: name, Type: "worker", State: topology.NodeActive})
				metaReplicas = append(metaReplicas, topology.Replica{NodeName: name, ReplicaID: uint64(i), Role: topology.ReplicaVoter})
			}
			active := topology.Document{
				Version: 9, ClusterID: "cluster-max", ChangedBy: "operator",
				Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3}, Nodes: nodes,
				Groups: []topology.Group{
					{ID: topology.MetaGroupID, NextReplicaID: uint64(rules.Limits.MaxNodes + 1), Replicas: metaReplicas},
					{ID: "shard0", NextReplicaID: 4, Replicas: []topology.Replica{
						{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter},
						{NodeName: "n2", ReplicaID: 2, Role: topology.ReplicaVoter},
						{NodeName: "n3", ReplicaID: 3, Role: topology.ReplicaVoter},
					}},
				},
			}
			desired, err := buildReadmissionDocument(active, "n3", time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC))
			if err != nil {
				t.Fatalf("buildReadmissionDocument: %v", err)
			}
			plan, err := topology.BuildPlan(active, desired, assumedReadmissionStatuses(active, "n3"), rules)
			if err != nil {
				t.Fatalf("BuildPlan: %v", err)
			}
			for _, groupID := range []string{topology.MetaGroupID, "shard0"} {
				oldID := active.Group(groupID).Replicas[2].ReplicaID
				newID := desired.Group(groupID).Replicas[2].ReplicaID
				indices := map[topology.StepKind]int{}
				for i, step := range plan.Steps {
					if step.GroupID != groupID {
						continue
					}
					if step.ReplicaID == oldID && step.Kind == topology.StepRemoveReplica {
						indices[step.Kind] = i
					}
					if step.ReplicaID == newID {
						switch step.Kind {
						case topology.StepPrepareReplica, topology.StepAddLearner, topology.StepPromoteVoter:
							indices[step.Kind] = i
						}
					}
				}
				if indices[topology.StepRemoveReplica] >= indices[topology.StepPrepareReplica] ||
					indices[topology.StepPrepareReplica] >= indices[topology.StepAddLearner] ||
					indices[topology.StepAddLearner] >= indices[topology.StepPromoteVoter] {
					t.Fatalf("group %s replacement is not shrink-first then learner-first: %+v (plan=%+v)", groupID, indices, plan.Steps)
				}
			}
		})
	}
}

func assumedReadmissionStatuses(active topology.Document, returningNode string) map[string]topology.GroupStatus {
	statuses := make(map[string]topology.GroupStatus, len(active.Groups))
	for _, group := range active.Groups {
		status := topology.GroupStatus{GroupID: group.ID, Found: true, Commit: 1}
		for _, replica := range group.Replicas {
			switch replica.Role {
			case topology.ReplicaLearner:
				status.ConfState.Learners = append(status.ConfState.Learners, replica.ReplicaID)
			case topology.ReplicaVoter:
				status.ConfState.Voters = append(status.ConfState.Voters, replica.ReplicaID)
				if status.LeaderID == 0 && replica.NodeName != returningNode {
					status.LeaderID = replica.ReplicaID
				}
			}
		}
		statuses[group.ID] = status
	}
	return statuses
}

func TestAdmissionRebindingNeverSelectsSupersededProcessForOperator(t *testing.T) {
	t.Parallel()
	sm, old, plan, at, index := admissionOperatorSM(t)
	if r := apply(t, sm, index, BindAdmissionProposal{Admission: old, At: at.Add(3 * time.Second)}); !r.Accepted {
		t.Fatal(r)
	}
	index++
	replacement := old
	replacement.InstanceID = "replacement"
	if r := apply(t, sm, index, BindAdmissionProposal{Admission: replacement, ReplaceInstance: true, At: at.Add(4 * time.Second)}); !r.Accepted {
		t.Fatal(r)
	}
	index++
	if r := apply(t, sm, index, ReadyAdmissionProposal{NodeName: replacement.NodeName, InstanceID: replacement.InstanceID, MemberID: replacement.MemberID, OperatorID: replacement.OperatorID}); !r.Accepted {
		t.Fatal(r)
	}
	for i := 0; i < 512; i++ {
		record, ok := sm.admissionForOperator(old.OperatorID, old.NodeName, old.MemberID)
		if !ok || record.InstanceID != replacement.InstanceID {
			t.Fatal("superseded process selected", record)
		}
	}
	sm.mu.Lock()
	changed := sm.updateAdmissionForCompletedStepLocked(sm.operators[old.OperatorID], plan.Steps[1])
	sm.mu.Unlock()
	if !changed {
		t.Fatal("replacement admission did not advance")
	}
	requireAdmissionState(t, sm, old.InstanceID, AdmissionSuperseded)
	requireAdmissionState(t, sm, replacement.InstanceID, AdmissionLearner)
	snapshot, err := sm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored := newTopologySM(nil, testRules())
	if err = restored.Restore(snapshot); err != nil {
		t.Fatal(err)
	}
	record, ok := restored.admissionForOperator(old.OperatorID, old.NodeName, old.MemberID)
	if !ok || record.InstanceID != replacement.InstanceID {
		t.Fatal("restored lookup chose superseded process", record)
	}
}
