//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func admissionApprovalFixture(t *testing.T, s scopedState, omit int) (scopedState, BeginOperationProposal) {
	t.Helper()
	s.Latest = s.Target.Clone()
	s.Latest.Version, s.Latest.ParentVersion = s.Target.Version+1, s.Target.Version
	s.Latest.Node("n2").Labels = map[string]string{"owner": "next"}
	plan := s.Operations[len(s.Operations)-1].Plan.Clone()
	plan.Version, plan.Steps, plan.Blocks[0].Steps = s.Latest.Version, plan.Steps[omit:], nil
	for i := range plan.Steps {
		plan.Steps[i].Seq = i + 1
		plan.Blocks[0].Steps = append(plan.Blocks[0].Steps, i+1)
	}
	if len(plan.Steps) == 0 {
		plan.VoterBases = nil
	}
	var err error
	s.indexes, err = prepareScopedGraph(&s, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	row := s.indexes.groups[topology.MetaGroupID]
	p := BeginOperationProposal{Version: s.Latest.Version, Plan: plan,
		Scopes: []ScopeExpectation{{GroupID: topology.MetaGroupID, SourceVersion: row.SourceVersion, Owner: row.Owner}},
		At:     s.Operations[len(s.Operations)-1].UpdatedAt.Add(time.Second), Actor: "admin", Origin: topology.OriginAPI}
	return s, p
}

func TestScopedApprovalHandsOverOneAdmissionWithoutRelabelingEffects(t *testing.T) {
	t.Parallel()
	for _, count := range []int{0, 1, 2} {
		t.Run(string(rune('0'+count)), func(t *testing.T) {
			t.Parallel()
			s := scopedAdmissionFixture()
			for seq := 1; seq <= count; seq++ {
				admissionPrefixReceipt(&s, 0, seq)
			}
			if count != 0 {
				s.Admissions[0].State = AdmissionReady
			}
			omit := 0
			if count == 2 {
				omit = 2
			}
			s, proposal := admissionApprovalFixture(t, s, omit)
			before, _ := json.Marshal(s)
			next, err := prepareScopedApproval(&s, s.Latest, proposal, explicitApproval)
			if err != nil {
				t.Fatal(err)
			}
			want := []AdmissionState{AdmissionPending, AdmissionReady, AdmissionLearner}[count]
			if len(next.Admissions) != 1 || next.Admissions[0].OperatorID != "exec-v3-a1" ||
				next.Admissions[0].State != want || ReadmissionID(next.Admissions[0]) != ReadmissionID(s.Admissions[0]) ||
				!reflect.DeepEqual(next.AuthorityRecords, s.AuthorityRecords) || next.Operations[1].Previous != "" {
				t.Fatal("handover lost readiness, identity, immutable facts or ancestry", next.Admissions)
			}
			// Finish only the surrounding declarations that Task 35 prepares.
			next.History = append(slices.Clone(next.History), s.Target.Clone())
			next.Nodes = slices.Clone(next.Nodes)
			next.Nodes[1].Owner = "exec-v3-a1"
			if _, err := prepareScopedGraph(next, next.Rules); err != nil {
				t.Fatal("prepared admission cannot restore with its successor", err)
			}
			next.Admissions[0].InstanceID = "copy"
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("handover changed the source row or its receipts")
			}
		})
	}
}

func admissionRetryCandidate(t *testing.T, s *scopedState) *scopedState {
	t.Helper()
	previous := &s.Operations[len(s.Operations)-1]
	previous.Status.State = topology.OperationFailed
	for i := range previous.Status.Blocks {
		previous.Status.Blocks[i].State = topology.OperationFailed
	}
	for i := range s.Groups {
		if s.Groups[i].Owner == previous.Status.ID {
			s.Groups[i].Owner = ""
		}
	}
	for i := range s.Nodes {
		if s.Nodes[i].Owner == previous.Status.ID {
			s.Nodes[i].Owner = ""
		}
	}
	var err error
	s.indexes, err = prepareScopedGraph(s, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	next := *s
	retry := previous.Clone()
	retry.Previous, retry.Status.Attempt = previous.Status.ID, previous.Status.Attempt+1
	retry.Status.ID = topology.MakeOperationID(retry.Status.Version, retry.Status.Attempt)
	retry.Status.State = topology.OperationPending
	for i := range retry.Status.Blocks {
		retry.Status.Blocks[i].State = topology.OperationPending
	}
	next.Operations = append(slices.Clone(s.Operations), retry)
	return &next
}

func TestScopedAdmissionHandoverPreservesProcessOriginAndWriteOnceOperator(t *testing.T) {
	t.Parallel()
	s := scopedAdmissionFixture()
	old := s.Admissions[0]
	old.State = AdmissionSuperseded
	s.Admissions[0].InstanceID, s.Admissions[0].ReadmissionInstanceID = "new-process", old.InstanceID
	s.Admissions = append(s.Admissions, old)
	admissionPrefixReceipt(&s, 0, 1)
	admissionPrefixReceipt(&s, 0, 2)
	next := admissionRetryCandidate(t, &s)
	if err := prepareAdmissionHandovers(&s, next); err != nil {
		t.Fatal(err)
	}
	a := next.Admissions[0]
	if a.State != AdmissionPending || a.ReadmissionOperatorID != "exec-v2-a1" ||
		a.ReadmissionInstanceID != old.InstanceID || ReadmissionID(a) != ReadmissionID(old) || next.Admissions[1] != old {
		t.Fatal("retry authenticated the replacement process or changed retirement identity", next.Admissions)
	}
	// A second handover retains the first operator rather than the latest retry.
	s = *next
	next = admissionRetryCandidate(t, &s)
	if err := prepareAdmissionHandovers(&s, next); err != nil {
		t.Fatal(err)
	}
	if next.Admissions[0].ReadmissionOperatorID != a.ReadmissionOperatorID ||
		next.Admissions[0].OperatorID != "exec-v2-a3" || next.Admissions[0].State != AdmissionPending {
		t.Fatal("repeated handover rewrote origin", next.Admissions[0])
	}
}

func TestScopedAdmissionHandoverRefusesStaleOwnersAndMissingPrefix(t *testing.T) {
	t.Parallel()
	for name, corrupt := range map[string]func(*scopedState){
		"wrong owner": func(s *scopedState) { s.indexes.groups[topology.MetaGroupID].Owner = "exec-v9-a1" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s, proposal := admissionApprovalFixture(t, scopedAdmissionFixture(), 0)
			next := s.prepareApprovalOwners(s.Latest, proposal)
			corrupt(&s)
			before, _ := json.Marshal(s)
			if err := prepareAdmissionHandovers(&s, next); !errors.Is(err, fault.ErrOperationAuthorization) {
				t.Fatal("invalid owner accepted", err)
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("refusal modified source")
			}
		})
	}
	s, proposal := admissionApprovalFixture(t, scopedAdmissionFixture(), 2)
	if _, err := prepareScopedApproval(&s, s.Latest, proposal, explicitApproval); err == nil {
		t.Fatal("prospective zero-step checkpoint supplied its own admission proof")
	}
	next := s.prepareApprovalOwners(s.Latest, proposal)
	wrong := s.Admissions[0]
	wrong.MemberID = 1
	h := admissionHandover{source: &s, proof: s.indexes}
	if _, err := h.prepare(wrong, &next.Operations[len(next.Operations)-1]); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("direct handover changed the member identity", err)
	}
}

func TestScopedAdmissionHandoverKeepsRetirementUsableWithoutRevivingFences(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name             string
		consumed, closed bool
	}{
		{"ongoing", false, false}, {"canceled", false, true},
		{"completed", true, false}, {"completed closed", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			s := admissionOriginFixture()
			if !test.consumed {
				// Remove cleanup claims that depend on the complete batch; retain
				// the older independently completed cleanup evidence instead.
				s.Cleanup = scopedCodecFixture().Cleanup
				s.AuthorityRecords[0].Effects = fixtureEffectReceipts(s.AuthorityRecords[0].Grant)
				s.Admissions[len(s.Admissions)-1].State = AdmissionPending
			}
			for i := range s.AuthorityRecords {
				record := &s.AuthorityRecords[i]
				if record.Grant.Purpose == topology.AuthorityReadmit {
					record.Closed = test.closed
					if !test.consumed {
						record.Effects = nil
					}
				}
			}
			next := admissionRetryCandidate(t, &s)
			before, _ := json.Marshal(s)
			err := prepareAdmissionHandovers(&s, next)
			if !test.consumed && test.closed {
				if !errors.Is(err, fault.ErrAuthorityFenced) {
					t.Fatal("canceled retirement revived", err)
				}
			} else if err != nil {
				t.Fatal("usable exact retirement refused", err)
			} else if !reflect.DeepEqual(next.AuthorityRecords, s.AuthorityRecords) ||
				ReadmissionID(next.Admissions[len(next.Admissions)-1]) != ReadmissionID(s.Admissions[len(s.Admissions)-1]) {
				t.Fatal("handover changed original retirement authority")
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("handover mutated original batch or admission")
			}
		})
	}
}

func TestScopedAdmissionHandoverRejectsNewerUnconsumedRetirementFloor(t *testing.T) {
	t.Parallel()
	s := admissionOriginFixture()
	index, err := prepareScopedGraph(&s, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := restoreScopedAuthority(&s, index)
	if err != nil {
		t.Fatal(err)
	}
	h := admissionHandover{catalog: catalog}
	a := s.Admissions[len(s.Admissions)-1]
	for _, record := range catalog.records {
		if record.Grant.Purpose == topology.AuthorityReadmit {
			record.Effects = nil
			key := record.Grant.Key
			key.Revision++
			catalog.floors[record.Grant.Resources[0]] = key
		}
	}
	if h.retirementUsable(a) {
		t.Fatal("later resource floor allowed old unconsumed retirement")
	}
}

func TestScopedAdmissionHandoverIgnoresCompletedMetadataSibling(t *testing.T) {
	t.Parallel()
	s := scopedAdmissionFixture()
	admissionPrefixReceipt(&s, 0, 1)
	admissionPrefixReceipt(&s, 0, 2)
	op := &s.Operations[0]
	op.Status.State, op.Status.TotalBlocks = topology.OperationFailed, 2
	op.Status.Blocks[0].State, op.Status.Blocks[0].CurrentStep = topology.OperationSucceeded, 2
	op.Status.Blocks = append(op.Status.Blocks, topology.BlockStatus{GroupID: "shard0",
		State: topology.OperationFailed, CurrentStep: 3, TotalSteps: 1})
	op.Plan.Blocks = append(op.Plan.Blocks, topology.PlanBlock{GroupID: "shard0", SourceVersion: 1, Steps: []int{3}})
	op.Plan.Steps = append(op.Plan.Steps, topology.Step{Seq: 3, Kind: topology.StepPrepareReplica,
		GroupID: "shard0", NodeName: "n1", ReplicaID: 1})
	s.Groups[0].SourceVersion, s.Groups[0].Declaration, s.Groups[0].Owner = 2, s.Target.MetaGroup(), ""
	s.Nodes[1].Owner, s.Admissions[0].State = "", AdmissionCompleted
	var err error
	s.indexes, err = prepareScopedGraph(&s, s.Rules)
	if err != nil {
		t.Fatal(err)
	}
	next := s
	retry := op.Clone()
	retry.Previous, retry.Status.ID, retry.Status.Attempt = op.Status.ID, "exec-v2-a2", 2
	retry.Status.State, retry.Status.Blocks[1].State = topology.OperationPending, topology.OperationPending
	next.Operations = append(slices.Clone(s.Operations), retry)
	next.Groups = slices.Clone(s.Groups)
	next.Groups[1].Owner = retry.Status.ID
	if err := prepareAdmissionHandovers(&s, &next); err != nil || !reflect.DeepEqual(next.Admissions, s.Admissions) {
		t.Fatal("load retry took over completed metadata admission", err)
	}
	if _, err := prepareScopedGraph(&next, next.Rules); err != nil {
		t.Fatal("retained completed metadata admission cannot restore", err)
	}
}
