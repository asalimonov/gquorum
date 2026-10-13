//go:build !functional

package metaengine

import (
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func TestReadmissionPlanConvergesWithoutCallingRetiredMember(t *testing.T) {
	t.Parallel()
	for _, successor := range []bool{false, true} {
		name := "original"
		if successor {
			name = "successor after receipts"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			m, p, index := readmissionAuthorityFixture(t)
			command := func(p any) ApplyResult {
				t.Helper()
				r := apply(t, m.topologySM, index, p)
				index++
				if !r.Accepted {
					t.Fatalf("%T: %+v", p, r)
				}
				return r
			}
			granted := command(p)
			a, _ := m.Admission(p.NodeName, p.InstanceID)
			var receipts []topology.EffectReceipt
			for _, grant := range granted.Grants {
				receipts = append(receipts, fixtureEffectReceipts(grant)...)
			}
			command(RecordReadmissionReceiptsProposal{Admission: a, Receipts: receipts})
			if successor {
				command(ProposeDocument{Doc: m.target.Clone(), ExpectedVersion: 2})
				base, err := m.DeletionBase(t.Context(), m.latest)
				if err != nil {
					t.Fatal(err)
				}
				if !slices.ContainsFunc(base.Group("shard0").Replicas, func(r topology.Replica) bool { return r.NodeName == "n1" && r.ReplicaID == 1 }) {
					t.Fatal("retirement proof hid old native membership", base)
				}
				plan, err := topology.BuildPlan(base, m.latest, assumedReadmissionStatuses(m.active, "n1"), m.rules)
				if err != nil {
					t.Fatal(err)
				}
				p.OperatorID = command(BeginOperatorProposal{Version: 3, Plan: plan, At: time.Unix(300, 0).UTC()}).OperatorID
			}
			data, err := m.Snapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			restored := NewMachine(m.rules, nil)
			if err := restored.Restore(t.Context(), data); err != nil {
				t.Fatal(err)
			}
			m = restored
			op := m.operators[p.OperatorID]
			plan := topology.Plan{Version: op.Version, Steps: op.Steps}
			for _, step := range plan.Steps {
				if step.NodeName == p.NodeName && step.ReplicaID == p.PreviousMemberID && (step.Kind == topology.StepStopReplica || step.Kind == topology.StepCleanupReplicaState) {
					t.Fatal("dispatch to rebound host", step)
				}
				applyAdmissionTransition(t, m.topologySM, index, op.ID, plan, step.Seq, topology.OperatorRunning, time.Unix(301, 0).UTC())
				index++
				index = consumeTestStep(t, m.topologySM, op.ID, index)
			}
			applyAdmissionTransition(t, m.topologySM, index, op.ID, plan, len(plan.Steps), topology.OperatorSucceeded, time.Unix(302, 0).UTC())
			index++
			command(ActivateTargetProposal{OperatorID: op.ID, Version: op.Version, Attempt: op.Attempt})
			if m.active.MemberID(p.NodeName) != p.MemberID {
				t.Fatal("replacement did not become active")
			}
			ids, err := m.deletionCatalogLocked("shard0")
			if err != nil {
				t.Fatal(err)
			}
			for id := range ids {
				if id.NodeName == p.NodeName && id.ReplicaID == 1 {
					t.Fatal("retired resource returned to cleanup catalog", id)
				}
			}
		})
	}
}

func TestReadmissionReceiptsExcludeOnlyRetiredHostsFromDeletion(t *testing.T) {
	t.Parallel()
	m := convergedAuthorityFixture(t)
	index := uint64(6)
	command := func(p any) ApplyResult {
		t.Helper()
		r := apply(t, m.topologySM, index, p)
		index++
		if !r.Accepted {
			t.Fatalf("%T: %+v", p, r)
		}
		return r
	}
	at := time.Unix(300, 0).UTC()
	desired, err := buildReadmissionDocument(m.active, "n1", at)
	if err != nil {
		t.Fatal(err)
	}
	desired.Groups = slices.DeleteFunc(desired.Groups, func(g topology.Group) bool { return g.ID == "shard0" })
	command(ProposeDocument{Doc: desired, ExpectedVersion: 1})
	plan, err := topology.BuildPlan(m.active, m.latest, assumedReadmissionStatuses(m.active, "n1"), m.rules)
	if err != nil {
		t.Fatal(err)
	}
	op := command(BeginOperatorProposal{Version: 2, Plan: plan, At: at}).OperatorID
	a := AdmissionRecord{NodeName: "n1", InstanceID: "replacement", MemberID: m.target.MemberID("n1"), OperatorID: op}
	command(BindAdmissionProposal{Admission: a, At: at})
	grants := command(BeginReadmissionAuthority{NodeName: a.NodeName, InstanceID: a.InstanceID, MemberID: a.MemberID, PreviousMemberID: 1, OperatorID: op}).Grants
	for _, grant := range grants {
		command(RecordFenceProposal{Evidence: topology.FenceEvidence{Key: grant.Key, Resource: grant.Resources[0], Receiver: topology.Identity{ClusterID: grant.Key.ClusterID, NodeName: a.NodeName, MemberID: 1, InstanceID: a.InstanceID}, LocalSequence: grant.Key.Revision*10 + 1}})
	}
	ids, err := m.deletionCatalogLocked("shard0")
	if err != nil || len(ids) != 3 {
		t.Fatal("fences hid unfinished retirement", ids, err)
	}
	for _, grant := range grants {
		command(RecordReadmissionReceiptsProposal{Admission: a, Receipts: fixtureEffectReceipts(grant)})
	}
	ids, err = m.deletionCatalogLocked("shard0")
	if err != nil || len(ids) != 2 {
		t.Fatal("wrong residual cleanup", ids, err)
	}
	for _, step := range plan.Steps {
		applyAdmissionTransition(t, m.topologySM, index, op, plan, step.Seq, topology.OperatorRunning, at)
		index++
		if step.Kind == topology.StepDeleteGroupState {
			action := topology.AuthorizedStep{OperatorID: op, Version: 2, Attempt: 1, StepSeq: step.Seq, Kind: step.Kind, GroupID: step.GroupID}
			grant := command(BeginEffectProposal{Action: action}).Grant
			if grant == nil || len(grant.Targets) != 2 {
				t.Fatal("wrong deletion scope", grant)
			}
			for _, target := range grant.Targets {
				if target.NodeName == "n1" {
					t.Fatal("deletion called rebound member", target)
				}
			}
		}
		index = consumeTestStep(t, m.topologySM, op, index)
	}
	applyAdmissionTransition(t, m.topologySM, index, op, plan, len(plan.Steps), topology.OperatorSucceeded, at)
	index++
	command(ActivateTargetProposal{OperatorID: op, Version: 2, Attempt: 1})
	if m.active.Group("shard0") != nil {
		t.Fatal("deleted group remains active")
	}
}
