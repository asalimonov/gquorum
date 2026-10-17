//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func scopedReadmissionCommitFixture(t *testing.T) (scopedState, CommitReadmissionProposal) {
	t.Helper()
	s, _ := rebaseFixture(t)
	*s.Latest.Node("n3") = *s.Target.Node("n3")
	s.Activated = []string{"n3"}
	prepareScopedFixture(t, &s)
	at := s.Latest.CreatedAt.Add(time.Second)
	doc, err := buildReadmissionDocument(s.Target, "n3", at)
	if err != nil {
		t.Fatal(err)
	}
	doc.Version = s.Latest.Version + 1
	input := s.planningDeclarations()
	for id, status := range assumedReadmissionStatuses(s.Target, "n3") {
		input.Native[id] = topology.NativeState{GroupID: id, ReplicaID: status.ConfState.Voters[0],
			Applied: 1, ConfState: status.ConfState}
		row := input.Groups[id]
		row.Observation = &status
		input.Groups[id] = row
	}
	plan, err := topology.BuildPlanPreferred(input, doc, topology.PlanOptions{Rules: s.Rules})
	if err != nil {
		t.Fatal(err)
	}
	return s, CommitReadmissionProposal{Admission: AdmissionRecord{NodeName: "n3", InstanceID: "returning",
		MemberID: doc.MemberID("n3")}, Doc: doc, ExpectedVersion: s.Latest.Version,
		ExpectedTarget: s.Target.Version, Scopes: s.capturePlanScopes(topology.AffectedGroups(s.Target, doc)),
		Plan: plan, At: at, ProposalID: "readmit"}
}

func TestScopedReadmissionCommitsRebaseAndAdmissionAtomically(t *testing.T) {
	t.Parallel()
	s, p := scopedReadmissionCommitFixture(t)
	staged := s.Latest.Clone()
	result := applyAdmissionFixture(t, &s, KindCommitReadmission, p)
	a := result.result.Admission
	if !result.changed || s.Target.Version != 3 || s.Latest.Version != 4 || s.Latest.RebasedFrom != 2 ||
		s.Latest.ParentVersion != 3 || s.Target.ParentVersion != 1 || result.result.Doc.Version != 3 ||
		result.result.OperationID != "exec-v3-a1" || a.OperationID != "exec-v3-a1" ||
		a.MemberID != 4 || a.InstanceID != p.Admission.InstanceID || a.State != AdmissionPending ||
		s.Target.Group("shard0").Labels != nil || s.Latest.Group("shard0").Labels["owner"] != "app" ||
		s.Latest.ChangedBy != staged.ChangedBy || s.FullyConverged.Version != 1 {
		t.Fatal("readmission lost approval, identity or independent staged intent", result, s)
	}
	before, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var restored scopedState
	if err := json.Unmarshal(before, &restored); err != nil {
		t.Fatal(err)
	}
	replayed := applyAdmissionFixture(t, &restored, KindCommitReadmission, p)
	after, _ := json.Marshal(restored)
	if replayed.changed || !bytes.Equal(before, after) || replayed.result.OperationID != result.result.OperationID ||
		replayed.result.Doc.Version != 3 || replayed.result.Admission != a {
		t.Fatal("lost-response replay changed the accepted readmission", replayed)
	}
}

func TestScopedReadmissionRefusesConflictsWithoutPartialApproval(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"latest CAS", "target CAS", "scope CAS", "wrong successor", "other edit", "supplied origin", "not activated", "staged overlap", "invalid admission plan"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s, p := scopedReadmissionCommitFixture(t)
			switch mode {
			case "latest CAS":
				p.ExpectedVersion--
			case "target CAS":
				p.ExpectedTarget++
			case "scope CAS":
				p.Scopes[0].SourceVersion++
			case "wrong successor":
				p.Admission.MemberID++
			case "other edit":
				p.Doc.Comment = "unapproved additional intent"
			case "supplied origin":
				p.Admission.ReadmissionOperatorID = "exec-v1-a1"
			case "not activated":
				s.Activated = nil
			case "staged overlap":
				s.Latest.Node("n3").Region = "changed"
				prepareScopedFixture(t, &s)
			case "invalid admission plan":
				p.Plan.Steps = slices.DeleteFunc(p.Plan.Steps, func(step topology.Step) bool {
					return step.Kind == topology.StepPrepareReplica
				})
			}
			before, _ := json.Marshal(s)
			result, err := s.applyCommitReadmission(s.AppliedIndex+1, p)
			after, _ := json.Marshal(s)
			if err == nil && result.result.Accepted || result.changed || !bytes.Equal(before, after) {
				t.Fatal("refusal partially installed readmission", result, err)
			}
		})
	}
}

func TestScopedReadmissionPlannerReleasesSeatAtNativeLimit(t *testing.T) {
	t.Parallel()
	doc := multiNodeDoc()
	s := convergedScopedFixture(t, doc)
	s.Rules.Limits.MaxNodes = 3
	m := restoreMachineFixture(t, s)
	desired, err := ReadmissionDocument(s.Target, "node3", s.FullyConverged.At.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	input := testPlanningInput(t, m, desired, assumedReadmissionStatuses(s.Target, "node3"))
	plan, err := topology.BuildPlan(input, desired, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	var removed, prepared int
	for _, step := range plan.Steps {
		if step.GroupID != topology.MetaGroupID || step.NodeName != "node3" {
			continue
		}
		if step.Kind == topology.StepRemoveReplica {
			removed = step.Seq
		}
		if step.Kind == topology.StepPrepareReplica {
			prepared = step.Seq
		}
	}
	if removed == 0 || prepared <= removed {
		t.Fatal("full native seat reused before retiring old identity", plan)
	}
}

func TestScopedReadmissionAllocatorOverflow(t *testing.T) {
	t.Parallel()
	doc := baseDoc()
	doc.MetaGroup().NextReplicaID = math.MaxUint64
	if _, err := ReadmissionDocument(doc, "n1", time.Unix(1, 0)); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("overflow accepted", err)
	}
}

func TestReadmissionNativeRemovalReceiptWaitsForCompleteRetirement(t *testing.T) {
	t.Parallel()
	doc := multiNodeDoc()
	doc.Group("shard0").Replicas = slices.Clone(doc.MetaGroup().Replicas)
	doc.Group("shard0").NextReplicaID = doc.MetaGroup().NextReplicaID
	s := convergedScopedFixture(t, doc)
	doc = s.Target.Clone()
	m := restoreMachineFixture(t, s)
	submit := func(p any) ApplyResult {
		t.Helper()
		r := applyMachine(t, m, p)
		if !r.Accepted {
			t.Fatalf("%T: %+v", p, r)
		}
		return r
	}
	at := s.FullyConverged.At.Add(time.Second)
	desired, err := ReadmissionDocument(doc, "node3", at)
	if err != nil {
		t.Fatal(err)
	}
	desired = submit(ProposeDocument{Doc: desired, ExpectedVersion: doc.Version, ProposalID: "stage"}).Doc
	input := testPlanningInput(t, m, desired, assumedReadmissionStatuses(doc, "node3"))
	plan, err := topology.BuildPlan(input, desired, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	begin := submit(BeginOperationProposal{Version: desired.Version, Plan: plan,
		Scopes: m.scoped.capturePlanScopes(topology.AffectedGroups(doc, desired)),
		Actor:  "admin", Origin: topology.OriginAPI, At: at, ProposalID: "approve"})
	op := m.scoped.indexes.operations[begin.OperationID]
	position := slices.IndexFunc(plan.Steps, func(step topology.Step) bool {
		return step.GroupID == "shard0" && step.Kind == topology.StepRemoveReplica
	})
	if position < 0 {
		t.Fatal("missing native removal", plan)
	}
	action := scopedPlanAction(op, plan.Steps[position])
	submit(TransitionOperationProposal{Transition: topology.OperationTransition{OperationID: op.Status.ID,
		Version: desired.Version, Attempt: 1, GroupID: action.GroupID, StepSeq: action.StepSeq,
		State: topology.OperationRunning, At: at}, ProposalID: "running"})
	grant := submit(BeginEffectProposal{Action: action, ProposalID: "remove"}).Grant
	receipt := fixtureEffectReceipts(*grant)[0]
	checkPending := func() {
		t.Helper()
		before, _ := m.Snapshot(t.Context())
		r := applyMachine(t, m, RecordEffectProposal{Receipt: receipt, At: at, ProposalID: "record-remove"})
		if r.Accepted || r.Code != proposalErrorCode(fault.Unavailable) {
			t.Fatal("pending independent readmission must retry the same receipt", r)
		}
		if len(m.scoped.Cleanup) != 0 || len(m.scoped.indexes.grants[grant.Key].Effects) != 0 {
			t.Fatal("partial retirement manufactured cleanup or consumed native proof", string(before))
		}
	}
	checkPending()
	admission := AdmissionRecord{NodeName: "node3", InstanceID: "returning", MemberID: desired.MemberID("node3"), OperationID: begin.OperationID}
	submit(BindAdmissionProposal{Admission: admission, At: at, ProposalID: "bind"})
	batch := submit(BeginReadmissionAuthority{NodeName: admission.NodeName, InstanceID: admission.InstanceID,
		MemberID: admission.MemberID, PreviousMemberID: doc.MemberID("node3"), OperationID: admission.OperationID,
		ProposalID: "retire"}).Grants
	if len(batch) != 2 {
		t.Fatal("fixture requires both metadata and load retirement", batch)
	}
	for i, memberGrant := range batch {
		for _, hostReceipt := range fixtureEffectReceipts(memberGrant) {
			submit(RecordEffectProposal{Receipt: hostReceipt, At: at, ProposalID: "retired"})
		}
		if i == 0 {
			checkPending()
		}
	}
	submit(RecordEffectProposal{Receipt: receipt, At: at, ProposalID: "record-remove"})
	submit(RecordEffectProposal{Receipt: receipt, At: at, ProposalID: "replay-remove"})
	for _, debt := range m.scoped.Cleanup {
		if debt.Removed != nil || debt.ReadmissionKey == nil || debt.Stopped == nil || debt.Retired == nil {
			t.Fatal("native receipt replaced independent retirement proof", debt)
		}
	}
	_ = restoreMachineFixture(t, *m.scoped)
}

func TestScopedReadmissionSeparatesNodesWithSameInstanceID(t *testing.T) {
	t.Parallel()
	doc := baseDoc()
	for i := 2; i <= 3; i++ {
		name := fmt.Sprint("n", i)
		doc.Nodes = append(doc.Nodes, topology.Node{Name: name, Type: "worker", State: topology.NodeActive})
		for j := range doc.Groups {
			doc.Groups[j].Replicas = append(doc.Groups[j].Replicas, topology.Replica{NodeName: name, ReplicaID: uint64(i), Role: topology.ReplicaVoter})
			doc.Groups[j].NextReplicaID = 4
		}
	}
	for i := range doc.Groups {
		for j := range doc.Groups[i].Replicas[:2] {
			doc.Groups[i].Replicas[j].Role = topology.ReplicaLearner
		}
	}
	s := convergedScopedFixture(t, doc)
	m := restoreMachineFixture(t, s)
	desired := s.Target.Clone()
	at := s.FullyConverged.At.Add(time.Second)
	for _, name := range []string{"n1", "n2"} {
		var err error
		desired, err = ReadmissionDocument(desired, name, at)
		if err != nil {
			t.Fatal(err)
		}
	}
	desired.ParentVersion = s.Target.Version // Both replacements share one approval.
	staged := applyMachine(t, m, ProposeDocument{Doc: desired, ExpectedVersion: 1, ProposalID: "readmit-two"})
	if !staged.Accepted {
		t.Fatal(staged)
	}
	desired = staged.Doc
	input := testPlanningInput(t, m, desired, assumedReadmissionStatuses(s.Target, ""))
	plan, err := topology.BuildPlan(input, desired, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	begin := applyMachine(t, m, BeginOperationProposal{Version: desired.Version, Plan: plan,
		Scopes: m.scoped.capturePlanScopes(topology.AffectedGroups(s.Target, desired)),
		Actor:  "admin", Origin: topology.OriginAPI, At: at, ProposalID: "approve-two"})
	if !begin.Accepted {
		t.Fatal(begin)
	}
	var grants []topology.AuthorityGrant
	for i, name := range []string{"n1", "n2"} {
		admission := AdmissionRecord{NodeName: name, InstanceID: "same-process-id",
			MemberID: desired.MemberID(name), OperationID: begin.OperationID}
		bound := applyMachine(t, m, BindAdmissionProposal{Admission: admission, At: at, ProposalID: "bind-" + name})
		if !bound.Accepted {
			t.Fatal(bound)
		}
		issued := applyMachine(t, m, BeginReadmissionAuthority{NodeName: name, InstanceID: admission.InstanceID,
			MemberID: admission.MemberID, PreviousMemberID: uint64(i + 1), OperationID: begin.OperationID,
			ProposalID: "retire-" + name})
		if !issued.Accepted || len(issued.Grants) != 2 {
			t.Fatal(issued)
		}
		for _, grant := range issued.Grants {
			for _, target := range grant.Targets {
				if target.NodeName != name || target.MemberID != uint64(i+1) {
					t.Fatal("batch crossed host identity", target)
				}
			}
		}
		grants = append(grants, issued.Grants...)
	}
	if grants[0].AdmissionID == grants[2].AdmissionID {
		t.Fatal("same process string aliased distinct hosts")
	}
	m = restoreMachineFixture(t, *m.scoped)
	for _, grant := range grants {
		if err := m.ValidateGrant(t.Context(), grant); err != nil {
			t.Fatal("restore conflated readmissions", err)
		}
	}
}

func TestScopedReadmissionAfterCompletedExclusion(t *testing.T) {
	t.Parallel()
	for _, retireLoad := range []bool{false, true} {
		t.Run(fmt.Sprintf("all_retired_%t", retireLoad), func(t *testing.T) {
			t.Parallel()
			doc := multiNodeDoc()
			doc.Group("shard0").Replicas = append(doc.Group("shard0").Replicas,
				topology.Replica{NodeName: "node2", ReplicaID: 2, Role: topology.ReplicaVoter},
				topology.Replica{NodeName: "node3", ReplicaID: 3, Role: topology.ReplicaVoter})
			doc.Group("shard0").NextReplicaID = 4
			for i := range doc.Groups {
				doc.Groups[i].Replicas[0].Role = topology.ReplicaLearner
			}
			s := convergedScopedFixture(t, doc)
			m := restoreMachineFixture(t, s)
			submit := func(p any) ApplyResult {
				t.Helper()
				result := applyMachine(t, m, p)
				if !result.Accepted {
					t.Fatalf("%T: %+v", p, result)
				}
				return result
			}
			approve := func(desired topology.Document, observed topology.Document) topology.OperationID {
				t.Helper()
				staged := submit(ProposeDocument{Doc: desired, ExpectedVersion: m.Latest().Version, ProposalID: "stage"})
				plan, err := topology.BuildPlan(testPlanningInput(t, m, staged.Doc, assumedReadmissionStatuses(observed, "node1")), staged.Doc, s.Rules)
				if err != nil {
					t.Fatal(err)
				}
				return submit(BeginOperationProposal{Version: staged.Doc.Version, Plan: plan,
					Scopes: m.scoped.capturePlanScopes(topology.AffectedGroups(m.Target(), staged.Doc)),
					At:     s.FullyConverged.At.Add(time.Duration(m.scoped.AppliedIndex) * time.Hour),
					Actor:  "admin", Origin: topology.OriginAPI, ProposalID: "approve"}).OperationID
			}
			excluded := s.Target.Clone()
			excluded.Node("node1").State = topology.NodeExcluded
			for i := range excluded.Groups {
				excluded.Groups[i].Replicas = slices.DeleteFunc(excluded.Groups[i].Replicas,
					func(r topology.Replica) bool { return r.NodeName == "node1" })
			}
			exclusion := approve(excluded, s.Target)
			completeRequiredFixture(t, m, exclusion)
			if m.scoped.indexes.groups[topology.MetaGroupID].Declaration.Replicas[0].NodeName == "node1" || m.scoped.Target.MemberID("node1") != 0 {
				t.Fatal("completed exclusion still owns the former member")
			}
			for _, debt := range slices.Clone(m.scoped.Cleanup) {
				if debt.Retired != nil || debt.Identity.GroupID != topology.MetaGroupID && !retireLoad {
					continue
				}
				actions := []topology.AuthorizedStep{*debt.StopAction}
				if *debt.RetireAction != *debt.StopAction {
					actions = append(actions, *debt.RetireAction)
				}
				for _, action := range actions {
					grant := submit(BeginCleanupEffectProposal{Identity: debt.Identity, Action: action, ProposalID: "cleanup"}).Grant
					for _, receipt := range fixtureEffectReceipts(*grant) {
						submit(RecordEffectProposal{Receipt: receipt, At: s.FullyConverged.At.Add(100 * time.Hour), ProposalID: "retired"})
					}
				}
			}
			ageScopedHistory(t, m.scoped)
			pruneScopedFixture(t, m.scoped)
			m = restoreMachineFixture(t, *m.scoped)
			returning := m.Target()
			returning.Node("node1").State = topology.NodeActive
			for i := range returning.Groups {
				group := &returning.Groups[i]
				id, err := group.AllocateReplicaID()
				if err != nil {
					t.Fatal(err)
				}
				group.Replicas = append(group.Replicas, topology.Replica{NodeName: "node1", ReplicaID: id, Role: topology.ReplicaLearner})
			}
			returning.ParentVersion = m.Target().Version
			op := approve(returning, m.Target())
			admission := AdmissionRecord{NodeName: "node1", InstanceID: "returning", MemberID: m.scoped.Target.MemberID("node1"), OperationID: op}
			submit(BindAdmissionProposal{Admission: admission, At: m.scoped.indexes.operations[op].UpdatedAt, ProposalID: "bind"})
			if retireLoad {
				submit(ReadyAdmissionProposal{NodeName: admission.NodeName, InstanceID: admission.InstanceID,
					MemberID: admission.MemberID, OperationID: op, ProposalID: "ready"})
				for _, record := range m.scoped.AuthorityRecords {
					if record.Grant.Purpose == topology.AuthorityReadmit {
						t.Fatal("already retired ownership created a readmission batch")
					}
				}
			} else {
				grants := submit(BeginReadmissionAuthority{NodeName: admission.NodeName, InstanceID: admission.InstanceID,
					MemberID: admission.MemberID, PreviousMemberID: 1, OperationID: op, ProposalID: "readmit"}).Grants
				if len(grants) != 2 {
					t.Fatal("lost former metadata or load identity", grants)
				}
				for _, grant := range grants {
					if len(grant.Targets) != 1 || grant.Targets[0] != (topology.AuthorityTarget{NodeName: "node1", MemberID: 1, ReplicaID: 1}) {
						t.Fatal("batch includes successor or lost former owner", grant)
					}
				}
			}
			_ = restoreMachineFixture(t, *m.scoped)
		})
	}
}

// Drive only required work through real proposals; detached cleanup stays debt.
func completeRequiredFixture(t *testing.T, m *Machine, id topology.OperationID) {
	t.Helper()
	for budget := 100; budget > 0; budget-- {
		op := m.scoped.indexes.operations[id]
		if op.Status.State == topology.OperationSucceeded {
			return
		}
		work, err := m.ReadyBlocks("", 1)
		if err != nil || len(work) != 1 || work[0].Operation.Status.ID != id {
			t.Fatal("required work stalled", work, err)
		}
		item := work[0]
		pos := slices.IndexFunc(op.Plan.Blocks, func(block topology.PlanBlock) bool { return block.GroupID == item.GroupID })
		definition, status := op.Plan.Blocks[pos], op.Status.Blocks[pos]
		seq := status.CurrentStep
		if seq == 0 && len(definition.Steps) != 0 {
			seq = definition.Steps[0]
		}
		transition := topology.OperationTransition{OperationID: id, Version: op.Status.Version, Attempt: op.Status.Attempt,
			GroupID: item.GroupID, StepSeq: seq, State: topology.OperationRunning, At: op.UpdatedAt.Add(time.Second)}
		submit := func(p any) ApplyResult {
			t.Helper()
			result := applyMachine(t, m, p)
			if !result.Accepted {
				t.Fatalf("%T: %+v", p, result)
			}
			return result
		}
		submit(TransitionOperationProposal{Transition: transition, ProposalID: "running"})
		if seq != 0 && !item.Consumed && op.Plan.Steps[seq-1].Kind != topology.StepWaitCaughtUp {
			grant := submit(BeginEffectProposal{Action: scopedPlanAction(op, op.Plan.Steps[seq-1]), ProposalID: "effect"}).Grant
			for _, receipt := range fixtureEffectReceipts(*grant) {
				submit(RecordEffectProposal{Receipt: receipt, At: transition.At, ProposalID: "consumed"})
			}
		}
		position := slices.Index(definition.Steps, seq)
		if position+1 < len(definition.Steps) {
			transition.StepSeq = definition.Steps[position+1]
		} else {
			transition.State = topology.OperationSucceeded
		}
		submit(TransitionOperationProposal{Transition: transition, ProposalID: "advance"})
	}
	t.Fatal("required work exceeded its fixture bound")
}
