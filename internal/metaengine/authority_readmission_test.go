//go:build !functional

package metaengine

import (
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func readmissionAuthorityFixture(t *testing.T) (*Machine, BeginReadmissionAuthority, uint64) {
	t.Helper()
	m := convergedAuthorityFixture(t)
	at := time.Unix(200, 0).UTC()
	doc, err := buildReadmissionDocument(m.active, "n1", at)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := topology.BuildPlan(m.active, doc, assumedReadmissionStatuses(m.active, "n1"), m.rules)
	if err != nil {
		t.Fatal(err)
	}
	a := AdmissionRecord{NodeName: "n1", InstanceID: "replacement", MemberID: doc.MemberID("n1"), OperatorID: topology.MakeOperatorID(2, 1)}
	if r := apply(t, m.topologySM, 6, CommitReadmissionProposal{Admission: a, Doc: doc, ExpectedVersion: 1, Plan: plan, At: at}); !r.Accepted {
		t.Fatal(r)
	}
	return m, BeginReadmissionAuthority{NodeName: a.NodeName, InstanceID: a.InstanceID, MemberID: a.MemberID, PreviousMemberID: 1, OperatorID: a.OperatorID}, 7
}

func TestReadmissionAuthorityBindsExactApprovedFormerMember(t *testing.T) {
	t.Parallel()
	m, p, index := readmissionAuthorityFixture(t)
	for _, previous := range []uint64{0, 2, p.MemberID, p.MemberID + 1} {
		bad := p
		bad.PreviousMemberID = previous
		if r := apply(t, m.topologySM, index, bad); r.Accepted || r.Code != proposalAuthorization {
			t.Fatalf("unapproved former member %d: %+v", previous, r)
		}
		index++
	}
	// Staged intent is not historical ownership proof, even when it assigns the
	// claimed old ID to the right name. n2 really owns member 2 in approved state.
	m.latest = m.target.Clone()
	m.latest.Version++
	m.latest.MetaGroup().Replicas[0].ReplicaID = 2
	bad := p
	bad.PreviousMemberID = 2
	if r := apply(t, m.topologySM, index, bad); r.Accepted {
		t.Fatal("staged identity authorized retirement", r)
	}
	index++
	granted := apply(t, m.topologySM, index, p)
	if !granted.Accepted || len(granted.Grants) != 2 {
		t.Fatal(granted)
	}
	index++
	if r := apply(t, m.topologySM, index, bad); r.Accepted || r.Code != proposalAuthorization {
		t.Fatal("retry changed former member", r)
	}
	index++
	if r := apply(t, m.topologySM, index, p); !r.Accepted || !r.Grants[0].Equal(granted.Grants[0]) {
		t.Fatal("exact retry lost immutable authority", r)
	}
}

func TestReadmissionAuthorityAfterCompletedExclusion(t *testing.T) {
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
	at := time.Unix(200, 0).UTC()
	excluded := m.active.Clone()
	excluded.Node("n1").State = topology.NodeExcluded
	for i := range excluded.Groups {
		excluded.Groups[i].Replicas = slices.DeleteFunc(excluded.Groups[i].Replicas, func(r topology.Replica) bool { return r.NodeName == "n1" })
	}
	command(ProposeDocument{Doc: excluded, ExpectedVersion: 1})
	plan, err := topology.BuildPlan(m.active, m.latest, assumedReadmissionStatuses(m.active, "n1"), m.rules)
	if err != nil {
		t.Fatal(err)
	}
	op := command(BeginOperatorProposal{Version: 2, Plan: plan, At: at}).OperatorID
	for _, step := range plan.Steps {
		applyAdmissionTransition(t, m.topologySM, index, op, plan, step.Seq, topology.OperatorRunning, at)
		index++
		index = consumeTestStep(t, m.topologySM, op, index)
	}
	applyAdmissionTransition(t, m.topologySM, index, op, plan, len(plan.Steps), topology.OperatorSucceeded, at)
	index++
	command(ActivateTargetProposal{OperatorID: op, Version: 2, Attempt: 1})
	if m.active.MemberID("n1") != 0 {
		t.Fatal("former member is still active")
	}
	returning := m.active.Clone()
	returning.Node("n1").State = topology.NodeActive
	for i := range returning.Groups {
		group := &returning.Groups[i]
		id, err := group.AllocateReplicaID()
		if err != nil {
			t.Fatal(err)
		}
		group.Replicas = append(group.Replicas, topology.Replica{NodeName: "n1", ReplicaID: id, Role: topology.ReplicaVoter})
	}
	command(ProposeDocument{Doc: returning, ExpectedVersion: 2})
	plan, err = topology.BuildPlan(m.active, m.latest, assumedReadmissionStatuses(m.active, "n1"), m.rules)
	if err != nil {
		t.Fatal(err)
	}
	op = command(BeginOperatorProposal{Version: 3, Plan: plan, At: at}).OperatorID
	a := AdmissionRecord{NodeName: "n1", InstanceID: "returning", MemberID: m.target.MemberID("n1"), OperatorID: op}
	command(BindAdmissionProposal{Admission: a, At: at})
	grants := command(BeginReadmissionAuthority{NodeName: a.NodeName, InstanceID: a.InstanceID, MemberID: a.MemberID, PreviousMemberID: 1, OperatorID: op}).Grants
	if len(grants) != 2 {
		t.Fatal(grants)
	}
	for _, grant := range grants {
		if len(grant.Targets) != 1 || grant.Targets[0] != (topology.AuthorityTarget{NodeName: "n1", MemberID: 1, ReplicaID: 1}) {
			t.Fatal("retirement scope included successor or lost former member", grant)
		}
	}
}

func TestReplacementPrepareWaitsForAllReadmissionReceipts(t *testing.T) {
	t.Parallel()
	m, p, index := readmissionAuthorityFixture(t)
	op := m.operators[p.OperatorID]
	plan := topology.Plan{Version: op.Version, Steps: op.Steps}
	var action topology.AuthorizedStep
	for _, step := range plan.Steps {
		applyAdmissionTransition(t, m.topologySM, index, op.ID, plan, step.Seq, topology.OperatorRunning, time.Unix(201, 0).UTC())
		index++
		if step.GroupID == topology.MetaGroupID && step.Kind == topology.StepPrepareReplica {
			action = topology.AuthorizedStep{OperatorID: op.ID, Version: op.Version, Attempt: op.Attempt, StepSeq: step.Seq, Kind: step.Kind, GroupID: step.GroupID, NodeName: step.NodeName, ReplicaID: step.ReplicaID}
			break
		}
		index = consumeTestStep(t, m.topologySM, op.ID, index)
	}
	if action.StepSeq == 0 {
		t.Fatal("fixture has no replacement preparation")
	}
	pending := func() {
		t.Helper()
		r := apply(t, m.topologySM, index, BeginEffectProposal{Action: action})
		index++
		if r.Accepted || r.Code != proposalAuthorityPending {
			t.Fatal("preparation preceded durable retirement", r)
		}
	}
	pending() // No grants yet: preparation must leave the host floor available.
	granted := apply(t, m.topologySM, index, p)
	index++
	if !granted.Accepted || len(granted.Grants) != 2 {
		t.Fatal(granted)
	}
	pending()
	for _, grant := range granted.Grants {
		e := topology.FenceEvidence{Key: grant.Key, Resource: grant.Resources[0], Receiver: topology.Identity{ClusterID: grant.Key.ClusterID, NodeName: p.NodeName, MemberID: p.PreviousMemberID, InstanceID: p.InstanceID}, LocalSequence: grant.Key.Revision*10 + 1}
		if r := apply(t, m.topologySM, index, RecordFenceProposal{Evidence: e}); !r.Accepted {
			t.Fatal(r)
		}
		index++
	}
	pending() // Completed fences cannot stand in for retirement receipts.
	for i, grant := range granted.Grants {
		for _, receipt := range fixtureEffectReceipts(grant) {
			if r := apply(t, m.topologySM, index, RecordEffectProposal{Receipt: receipt}); !r.Accepted {
				t.Fatal(r)
			}
			index++
		}
		if i < len(granted.Grants)-1 {
			pending() // Metadata retirement alone cannot bypass load retirement.
		}
	}
	data, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	restored := NewMachine(m.rules, nil)
	if err = restored.Restore(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	if r := apply(t, restored.topologySM, index, BeginEffectProposal{Action: action}); !r.Accepted || r.Grant == nil {
		t.Fatal("complete receipts did not survive restore", r)
	}
}

func TestReadmissionSnapshotRejectsContradictoryFormerMembers(t *testing.T) {
	t.Parallel()
	m, p, index := readmissionAuthorityFixture(t)
	if r := apply(t, m.topologySM, index, p); !r.Accepted {
		t.Fatal(r)
	}
	data, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*smState){
		"mixed_members":  func(s *smState) { s.AuthorityRecords[1].Grant.Targets[0].MemberID = 2 },
		"wrong_meta_pin": func(s *smState) { s.AuthorityRecords[0].Grant.Targets[0].ReplicaID = 2 },
		"missing_meta_grant": func(s *smState) {
			s.AuthorityRecords = s.AuthorityRecords[1:]
			s.AuthorityFloors = s.AuthorityFloors[1:]
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var state smState
			if err := json.Unmarshal(data, &state); err != nil {
				t.Fatal(err)
			}
			mutate(&state)
			bad, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			if err = NewMachine(m.rules, nil).Restore(t.Context(), bad); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("restored contradictory readmission identity", err)
			}
		})
	}
}

func TestReadmissionAuthoritySeparatesNodesWithSameInstanceID(t *testing.T) {
	t.Parallel()
	// Both returning members are learners; the retained voter keeps its quorum.
	m := convergedAuthorityFixtureWithLearners(t, "n1", "n2")
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
	at := time.Unix(200, 0).UTC()
	desired := m.active.Clone()
	for _, node := range []string{"n1", "n2"} {
		var err error
		desired, err = buildReadmissionDocument(desired, node, at)
		if err != nil {
			t.Fatal(err)
		}
	}
	command(ProposeDocument{Doc: desired, ExpectedVersion: 1})
	plan, err := topology.BuildPlan(m.active, m.latest, assumedReadmissionStatuses(m.active, "n1"), m.rules)
	if err != nil {
		t.Fatal(err)
	}
	op := command(BeginOperatorProposal{Version: 2, Plan: plan, At: at}).OperatorID
	var all []topology.AuthorityGrant
	for i, node := range []string{"n1", "n2"} {
		a := AdmissionRecord{NodeName: node, InstanceID: "same-process-id", MemberID: m.target.MemberID(node), OperatorID: op}
		command(BindAdmissionProposal{Admission: a, At: at})
		grants := command(BeginReadmissionAuthority{NodeName: a.NodeName, InstanceID: a.InstanceID, MemberID: a.MemberID, PreviousMemberID: uint64(i + 1), OperatorID: op}).Grants
		if len(grants) != 2 || grants[0].Resources[0].NodeName != node {
			t.Fatal("authority crossed node identity", grants)
		}
		all = append(all, grants...)
	}
	if all[0].AdmissionID == all[2].AdmissionID {
		t.Fatal("distinct admitted nodes share a readmission ID")
	}
	data, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	restored := NewMachine(m.rules, nil)
	if err = restored.Restore(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	for _, grant := range all {
		if err = restored.ValidateGrant(t.Context(), grant); err != nil {
			t.Fatal("restore conflated admitted identities", err)
		}
	}
}

func TestReadmissionReceiptBatchIsAtomicAndRequiredForReadiness(t *testing.T) {
	t.Parallel()
	m, p, index := readmissionAuthorityFixture(t)
	command := func(p any) ApplyResult { t.Helper(); r := apply(t, m.topologySM, index, p); index++; return r }
	granted := command(p)
	if !granted.Accepted {
		t.Fatal(granted)
	}
	a, _ := m.Admission(p.NodeName, p.InstanceID)
	var receipts []topology.EffectReceipt
	for _, grant := range granted.Grants {
		receipts = append(receipts, fixtureEffectReceipts(grant)...)
	}
	ready := ReadyAdmissionProposal{NodeName: a.NodeName, InstanceID: a.InstanceID, MemberID: a.MemberID, OperatorID: a.OperatorID}
	if r := command(ready); r.Accepted || r.Code != proposalAuthorityPending {
		t.Fatal("ready before receipts", r)
	}
	bad := []topology.EffectReceipt{receipts[0], receipts[0].Clone()}
	bad[1].LocalSequence++
	if r := command(RecordReadmissionReceiptsProposal{Admission: a, Receipts: bad}); r.Accepted {
		t.Fatal("contradictory batch accepted", r)
	}
	for _, grant := range granted.Grants {
		if got, err := m.EffectReceipts(t.Context(), grant); err != nil || len(got) != 0 {
			t.Fatal("rejected batch partially recorded", got, err)
		}
	}
	wrong := a
	wrong.InstanceID = "other-process"
	if r := command(RecordReadmissionReceiptsProposal{Admission: wrong, Receipts: receipts}); r.Accepted {
		t.Fatal("wrong admission recorded receipts", r)
	}
	if r := command(RecordReadmissionReceiptsProposal{Admission: a, Receipts: receipts[:1]}); !r.Accepted {
		t.Fatal(r)
	}
	if r := command(ready); r.Accepted || r.Code != proposalAuthorityPending {
		t.Fatal("partial receipts admitted readiness", r)
	}
	for range 2 {
		if r := command(RecordReadmissionReceiptsProposal{Admission: a, Receipts: receipts}); !r.Accepted {
			t.Fatal(r)
		}
	}
	if r := command(ready); !r.Accepted {
		t.Fatal(r)
	}
	data, err := m.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	restored := NewMachine(m.rules, nil)
	if err = restored.Restore(t.Context(), data); err != nil {
		t.Fatal(err)
	}
	if got, _ := restored.Admission(a.NodeName, a.InstanceID); got.State != AdmissionReady {
		t.Fatal(got)
	}
}

func TestReadmissionReceiptsSettleSupersededAdmissionWithoutReadiness(t *testing.T) {
	t.Parallel()
	m, p, index := readmissionAuthorityFixture(t)
	granted := apply(t, m.topologySM, index, p)
	index++
	if !granted.Accepted {
		t.Fatal(granted)
	}
	a, _ := m.Admission(p.NodeName, p.InstanceID)
	if r := apply(t, m.topologySM, index, ProposeDocument{Doc: m.target.Clone(), ExpectedVersion: 2}); !r.Accepted {
		t.Fatal(r)
	}
	index++
	old := m.operators[a.OperatorID]
	plan := topology.Plan{Version: 3, Steps: slices.Clone(old.Steps), VoterBases: old.VoterBases}
	if r := apply(t, m.topologySM, index, BeginOperatorProposal{Version: 3, Plan: plan, At: time.Unix(300, 0).UTC()}); !r.Accepted {
		t.Fatal(r)
	}
	index++
	var receipts []topology.EffectReceipt
	for _, grant := range granted.Grants {
		receipts = append(receipts, fixtureEffectReceipts(grant)...)
	}
	if r := apply(t, m.topologySM, index, RecordReadmissionReceiptsProposal{Admission: a, Receipts: receipts}); !r.Accepted {
		t.Fatal("historical completion refused", r)
	}
	index++
	status, err := m.AuthorityStatus(a.OperatorID)
	if err != nil || !status.RevocationComplete {
		t.Fatal("late receipts did not settle revocation", status, err)
	}
	if r := apply(t, m.topologySM, index, ReadyAdmissionProposal{NodeName: a.NodeName, InstanceID: a.InstanceID, MemberID: a.MemberID, OperatorID: a.OperatorID}); r.Accepted {
		t.Fatal("superseded admission became ready", r)
	}
}
