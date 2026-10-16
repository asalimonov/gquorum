//go:build !functional

package metaengine

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func stageIntentFixture(t *testing.T) (scopedState, ProposeDocument) {
	t.Helper()
	s := scopedGraphFixture()
	prepareScopedFixture(t, &s)
	doc := s.Latest.Clone()
	doc.ParentVersion = s.Latest.Version
	doc.Comment = "activate n1 for operator purpose"
	intent, err := NewIntent("activate_node", map[string]string{"node": "n1", "comment": doc.Comment})
	if err != nil {
		t.Fatal(err)
	}
	intent.Version, intent.ExpectedVersion, intent.NodeName = 3, 2, "n1"
	return s, ProposeDocument{Doc: doc, ExpectedVersion: 2, Intent: &intent, ProposalID: "stage"}
}

func TestScopedStagePersistsIntentAndReplaysRestoredAcceptance(t *testing.T) {
	t.Parallel()
	s, p := stageIntentFixture(t)
	target, groups, operations := s.Target.Clone(), s.Groups, s.Operations
	encoded, _ := json.Marshal(p)
	result, err := applyScopedProposal(&s, 11, KindProposeDocument, encoded)
	if err != nil || !result.changed || !result.result.Accepted || result.result.Doc.Version != 3 || len(s.Intents) != 1 {
		t.Fatal(result, err)
	}
	if !reflect.DeepEqual(s.Target, target) || !reflect.DeepEqual(s.Groups, groups) || !reflect.DeepEqual(s.Operations, operations) {
		t.Fatal("staging authorized effects")
	}
	before, _ := json.Marshal(s)
	var restored scopedState
	if err := json.Unmarshal(before, &restored); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		result, err = applyScopedProposal(&restored, 12, KindProposeDocument, encoded)
		if err != nil || result.changed || !result.result.Accepted || result.result.Doc.Version != 3 {
			t.Fatal("lost response created a new document", result, err)
		}
	}
	after, _ := json.Marshal(restored)
	if !bytes.Equal(before, after) {
		t.Fatal("replay mutated restored state")
	}
	changed := *p.Intent
	changed.Digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	p.Intent = &changed
	if _, err := restored.applyProposeDocument(12, p); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal("different purpose matched", err)
	}
}

func TestScopedStageRefusesMalformedIntentAtomically(t *testing.T) {
	t.Parallel()
	for name, edit := range map[string]func(*ProposeDocument){
		"unknown kind":           func(p *ProposeDocument) { p.Intent.Kind = "request" },
		"digest":                 func(p *ProposeDocument) { p.Intent.Digest = "123" },
		"intent parent":          func(p *ProposeDocument) { p.Intent.ExpectedVersion = 1 },
		"intent version":         func(p *ProposeDocument) { p.Intent.Version = 4 },
		"document parent":        func(p *ProposeDocument) { p.Doc.ParentVersion = 1 },
		"floor":                  func(p *ProposeDocument) { p.Doc.Group("shard0").NextReplicaID = 1 },
		"unexpected predecessor": func(p *ProposeDocument) { p.Intent.PreviousID = 1; p.Intent.SuccessorID = 2 },
		"foreign successor":      func(p *ProposeDocument) { p.Intent.SuccessorID = 1; p.Intent.NodeName = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s, p := stageIntentFixture(t)
			edit(&p)
			before, _ := json.Marshal(s)
			result, err := s.applyProposeDocument(11, p)
			after, _ := json.Marshal(s)
			if err == nil || result.changed || !bytes.Equal(before, after) {
				t.Fatal("invalid provenance installed", result, err)
			}
		})
	}
}

func TestScopedStageReplacementTupleMatchesDiff(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"readmit_node", "reseat_replica"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			s, _ := rebaseFixture(t)
			doc := s.Latest.Clone()
			name, group, prior, next := "n2", "b", uint64(1), uint64(2)
			if kind == "readmit_node" {
				var err error
				name, group = "n1", topology.MetaGroupID
				prior = doc.MemberID(name)
				doc, err = ReadmissionDocument(doc, name, time.Now().UTC())
				if err != nil {
					t.Fatal(err)
				}
				next = doc.MemberID(name)
			} else {
				doc.Group(group).Replicas[0].ReplicaID = next
				doc.Group(group).NextReplicaID = next + 1
			}
			doc.ParentVersion = s.Latest.Version
			intent, err := NewIntent(kind, map[string]any{"node": name, "previous": prior})
			if err != nil {
				t.Fatal(err)
			}
			intent.Version, intent.ExpectedVersion, intent.GroupID, intent.NodeName, intent.PreviousID, intent.SuccessorID = 3, 2, group, name, prior, next
			p := ProposeDocument{Doc: doc, ExpectedVersion: 2, Intent: &intent}
			wrong := intent
			wrong.PreviousID = next + 10
			p.Intent = &wrong
			before, _ := json.Marshal(s)
			if result, err := s.applyProposeDocument(11, p); err == nil || result.changed {
				t.Fatal("wrong predecessor accepted", result, err)
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("refused tuple changed state")
			}
			p.Intent = &intent
			result, err := s.applyProposeDocument(11, p)
			if err != nil || !result.changed {
				t.Fatal("exact replacement refused", result, err)
			}
			encoded, _ := json.Marshal(s)
			var restored scopedState
			if err := json.Unmarshal(encoded, &restored); err != nil {
				t.Fatal(err)
			}
			replay, err := restored.applyProposeDocument(12, p)
			if err != nil || replay.changed || replay.result.Doc.Version != 3 {
				t.Fatal(replay, err)
			}
		})
	}
}

func TestScopedRollbackIntentSeparatesCASFromHistoricalLineage(t *testing.T) {
	t.Parallel()
	s, p := stageIntentFixture(t)
	p.Doc.ParentVersion = 1
	p.Intent.Kind = "rollback_to"
	result, err := s.applyProposeDocument(11, p)
	if err != nil || !result.changed || result.result.Doc.ParentVersion != 1 || s.Intents[0].ExpectedVersion != 2 {
		t.Fatal(result, err)
	}
	encoded, _ := json.Marshal(s)
	var restored scopedState
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal("rollback provenance cannot restore", err)
	}
}

func TestApprovalLookupReturnsOriginalAndNeverSubstitutesRetry(t *testing.T) {
	t.Parallel()
	s := scopedGraphFixture()
	prepareScopedFixture(t, &s)
	view := topology.ReadView{Latest: s.Latest, Target: s.Target}
	first, err := retainedApproval(2, view, s.indexes)
	if err != nil || first != "exec-v2-a1" {
		t.Fatal("failed first approval replaced by live retry", first, err)
	}
	// Incomplete lookup input is not a restorable graph, but must still fail closed.
	delete(s.indexes.first, 2)
	if first, err := retainedApproval(2, view, s.indexes); first != "" || !errors.Is(err, fault.ErrHistoryUnavailable) {
		t.Fatal(first, err)
	}
	delete(s.indexes.latest, 2)
	delete(s.indexes.documents, 2)
	if first, err := retainedApproval(2, view, s.indexes); first != "" || !errors.Is(err, fault.ErrHistoryUnavailable) {
		t.Fatal(first, err)
	}
	if _, err := retainedApproval(3, view, s.indexes); !errors.Is(err, fault.ErrVersionConflict) {
		t.Fatal(err)
	}
}

func TestApprovalLookupFollowsRebasedUnapprovedIntent(t *testing.T) {
	t.Parallel()
	s, repair := rebaseFixture(t)
	pair, err := s.prepareRepairDocuments(repair, s.Latest.CreatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	s.installRepairDocuments(pair)
	prepareScopedFixture(t, &s)
	before, _ := json.Marshal(s)
	_, err = s.applyBeginOperation(11, BeginOperationProposal{Version: 2})
	var conflict *fault.Error
	if !errors.As(err, &conflict) || conflict.Code != fault.VersionConflict || conflict.ExpectedVersion != 2 || conflict.CurrentVersion != 4 || conflict.RebasedTo != 4 {
		t.Fatal("unapproved rebase lost structured successor", err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("rebased approval changed state")
	}
}

func TestScopedRawStageClearsRebaseAndKeepsOriginalReplay(t *testing.T) {
	t.Parallel()
	s, repair := rebaseFixture(t)
	pair, err := s.prepareRepairDocuments(repair, s.Latest.CreatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	s.installRepairDocuments(pair)
	prepareScopedFixture(t, &s)
	request := ProposeDocument{ExpectedVersion: 4, Doc: s.Latest.Clone()}
	request.Doc.Comment = "raw caller edit"
	result, err := s.applyProposeDocument(11, request)
	if err != nil || !result.changed || s.Latest.Version != 5 || s.Latest.RebasedFrom != 0 || s.indexes.rebases[2] != 4 || len(s.Intents) != 0 {
		t.Fatal(result, err)
	}
	before, _ := json.Marshal(s)
	replay, err := s.applyProposeDocument(12, request)
	after, _ := json.Marshal(s)
	if err != nil || replay.changed || replay.result.Doc.Version != 5 || !bytes.Equal(before, after) {
		t.Fatal("raw response loss changed accepted document", replay, err)
	}
}
