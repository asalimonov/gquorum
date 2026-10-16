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

func deletionApprovalFixture(t *testing.T) (scopedState, BeginOperationProposal) {
	t.Helper()
	s, p := approvalFixture(t, "shard0")
	s.Latest.Groups = slices.DeleteFunc(s.Latest.Groups, func(g topology.Group) bool { return g.ID == "shard0" })
	p.Plan.Steps = []topology.Step{
		{Seq: 1, Kind: topology.StepStopReplica, GroupID: "shard0", NodeName: "n1", ReplicaID: 1},
		{Seq: 2, Kind: topology.StepDeleteGroupState, GroupID: "shard0"},
	}
	p.Plan.Blocks[0].Steps, p.Plan.Blocks[0].Cleanup = []int{1}, []int{2}
	return s, p
}

func TestScopedApprovalPreparesExactDeletionDebt(t *testing.T) {
	t.Parallel()
	s, p := deletionApprovalFixture(t)
	before, _ := json.Marshal(s)
	next, err := prepareScopedApproval(&s, s.Latest, p, explicitApproval)
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Cleanup) != 1 || len(next.RetiredGroups) != len(s.RetiredGroups)+1 {
		t.Fatal("deletion lost former ownership or permanent ID retirement", next.Cleanup, next.RetiredGroups)
	}
	d := next.Cleanup[0]
	if d.Identity.GroupID != "shard0" || d.Identity.NodeName != "n1" || d.Identity.ReplicaID != 1 ||
		d.MemberID != 1 || d.Storage != s.Target.Group("shard0").Storage || d.OperationID != "exec-v3-a1" ||
		d.StopAction.StepSeq != 1 || d.RetireAction.StepSeq != 2 || d.RetireAction.Kind != topology.StepDeleteGroupState ||
		d.Removed != nil || d.RemovedAt != nil || d.Stopped != nil || d.Retired != nil {
		t.Fatal("deletion fabricated progress or changed exact provenance", d)
	}
	next.History = append(slices.Clone(next.History), s.Target.Clone())
	if _, err := prepareScopedGraph(next, next.Rules); err != nil {
		t.Fatal("first deletion debt cannot restore", err)
	}
	after, _ := json.Marshal(s)
	if !bytes.Equal(before, after) {
		t.Fatal("deletion preparation modified source proof")
	}
}

func TestScopedDeletionRefusesMissingActionsWithoutPartialMutation(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"missing stop", "foreign stop", "missing delete"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s, p := deletionApprovalFixture(t)
			switch mode {
			case "missing stop":
				p.Plan.Blocks[0].Steps = nil
			case "foreign stop":
				p.Plan.Steps[0].NodeName = "foreign"
			case "missing delete":
				p.Plan.Blocks[0].Cleanup = nil
			default:
			}
			collector := s.planningCollector()
			if _, err := collector.collect(s.planningDeclarations(), s.Latest); err != nil {
				t.Fatal(err)
			}
			collector.markConvergedOwners(s.planningDeclarations().Groups)
			next := s.prepareApprovalOwners(s.Latest, p)
			before, _ := json.Marshal(s)
			if err := prepareDeletionDebts(&s, next, collector); !errors.Is(err, fault.ErrOperationAuthorization) {
				t.Fatal("deletion lost a required exact action", err)
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) || !reflect.DeepEqual(next.Cleanup, s.Cleanup) ||
				!reflect.DeepEqual(next.RetiredGroups, s.RetiredGroups) {
				t.Fatal("refusal partially installed deletion proof")
			}
		})
	}
}

func TestScopedDeletionPreservesInheritedRetirement(t *testing.T) {
	t.Parallel()
	s, p := cleanupApprovalFixture(t)
	s.Latest.Groups = slices.DeleteFunc(s.Latest.Groups, func(g topology.Group) bool { return g.ID == "a" })
	next, err := prepareScopedApproval(&s, s.Latest, p, explicitApproval)
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Cleanup) != len(s.Cleanup) || next.Cleanup[0].OperationID != s.Cleanup[0].OperationID ||
		!reflect.DeepEqual(next.Cleanup[0].RetireAction, s.Cleanup[0].RetireAction) ||
		!reflect.DeepEqual(next.Cleanup[1], s.Cleanup[1]) {
		t.Fatal("deletion replaced inherited or consumed cleanup proof")
	}
	if next.Cleanup[0].StopAction.OperatorID != "exec-v7-a1" ||
		!slices.Contains(next.RetiredGroups, retiredGroup{ID: "a", Version: 7}) {
		t.Fatal("deletion lost its required stop or permanent identity")
	}
	next.History = append(slices.Clone(next.History), s.Target.Clone())
	if _, err := prepareScopedGraph(next, next.Rules); err != nil {
		t.Fatal("inherited removal debt cannot restore during deletion", err)
	}
}

func TestScopedDeletionBeforeFirstConvergence(t *testing.T) {
	t.Parallel()
	for _, issued := range []bool{false, true} {
		s, p := deletionApprovalFixture(t)
		s.History[0].Groups = slices.DeleteFunc(s.History[0].Groups, func(g topology.Group) bool { return g.ID == "shard0" })
		s.Groups[1].SourceVersion, s.Groups[1].Declaration, s.Groups[1].CompletedAt = 0, nil, time.Time{}
		p.Scopes[0].SourceVersion, p.Plan.Blocks[0].SourceVersion = 0, 0
		for i := range s.Operations {
			s.Operations[i].Plan.Blocks[1].SourceVersion = 0
			s.Operations[i].Plan.Steps[1].Kind = topology.StepPrepareReplica
		}
		if issued {
			action := scopedPlanAction(&s.Operations[0], s.Operations[0].Plan.Steps[1])
			key := topology.AuthorityKey{ClusterID: s.Target.ClusterID, GroupID: "shard0", Revision: 5}
			host := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: "shard0", NodeName: "n1"}
			s.AuthorityRecords = append(s.AuthorityRecords, scopedAuthorityRecord{Class: "execution",
				authorityRecord: authorityRecord{Grant: topology.AuthorityGrant{Key: key, Purpose: topology.AuthorityEffect,
					OperatorID: action.OperatorID, Action: &action, Resources: []topology.AuthorityResource{host},
					Targets: []topology.AuthorityTarget{{NodeName: "n1", MemberID: 1, ReplicaID: 1}}}}})
			s.AuthorityFloors = append(s.AuthorityFloors, resourceFloor{Resource: host, Key: key})
		} else {
			p.Plan.Steps, p.Plan.Blocks[0].Steps, p.Plan.Blocks[0].Cleanup = nil, nil, nil
		}
		var err error
		s.indexes, err = prepareScopedGraph(&s, s.Rules)
		if err != nil {
			t.Fatal(err)
		}
		next, err := prepareScopedApproval(&s, s.Latest, p, explicitApproval)
		if err != nil {
			t.Fatal(err)
		}
		row := next.Groups[1]
		if row.SourceVersion != 0 || row.Declaration != nil || !row.Deleting || row.Owner != "exec-v3-a1" ||
			(len(next.Cleanup) != 0) != issued || !slices.Contains(next.RetiredGroups, retiredGroup{ID: "shard0", Version: 3}) {
			t.Fatal("deletion invented convergence or lost issued ownership", row, next.Cleanup)
		}
		next.History = append(slices.Clone(next.History), s.Target.Clone())
		if _, err := prepareScopedGraph(next, next.Rules); err != nil {
			t.Fatal("unknown deletion cannot restore", err)
		}
		next.Groups[1].SourceVersion, next.Groups[1].CompletedAt = 1, p.At
		if _, err := prepareScopedGraph(next, next.Rules); !errors.Is(err, fault.ErrUnreadable) {
			t.Fatal("pending deletion accepted a fabricated tombstone", err)
		}
	}
}

func TestScopedDeletionReadmissionCoverage(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"missing", "foreign", "partial", "closed", "displaced", "consumed closed", "mixed"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s, p := deletionApprovalFixture(t)
			if mode == "mixed" {
				doc := s.Target.Clone()
				doc.MetaGroup().Replicas = append(doc.MetaGroup().Replicas, topology.Replica{NodeName: "n2", ReplicaID: 2})
				doc.Group("shard0").Replicas = append(doc.Group("shard0").Replicas, topology.Replica{NodeName: "n2", ReplicaID: 2})
				s.indexes.documents[2] = doc
				for i := range s.Allocations {
					s.Allocations[i].NextReplicaID = 3
				}
				p.Plan.Steps[0].NodeName, p.Plan.Steps[0].ReplicaID = "n2", 2
			} else {
				p.Plan.Steps, p.Plan.Blocks[0].Steps, p.Plan.Blocks[0].Cleanup = nil, nil, nil
			}
			collector := s.planningCollector()
			if _, err := collector.collect(s.planningDeclarations(), s.Latest); err != nil {
				t.Fatal(err)
			}
			collector.markConvergedOwners(s.planningDeclarations().Groups)
			if mode == "mixed" {
				collector.owners[planningReplicaKey{"shard0", 2}].hosted()
			}
			next := s.prepareApprovalOwners(s.Latest, p)
			next.Target.MetaGroup().Replicas[0].ReplicaID = 2
			a := AdmissionRecord{NodeName: "n1", MemberID: 2, InstanceID: "replacement", OperatorID: "exec-v2-a1"}
			collector.evidence.admissions = append(slices.Clone(s.Admissions), a)
			for _, group := range []string{topology.MetaGroupID, "shard0"} {
				if mode == "missing" {
					continue
				}
				host := topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: group, NodeName: "n1"}
				key := topology.AuthorityKey{ClusterID: s.Target.ClusterID, GroupID: group, Revision: 7}
				grant := topology.AuthorityGrant{Key: key, Purpose: topology.AuthorityReadmit, AdmissionID: ReadmissionID(a),
					Resources: []topology.AuthorityResource{host}, Targets: []topology.AuthorityTarget{{NodeName: "n1", MemberID: 1, ReplicaID: 1}}}
				if mode == "foreign" {
					grant.Targets[0].ReplicaID++
				}
				r := &authorityRecord{Grant: grant, Effects: fixtureEffectReceipts(grant)}
				if mode != "consumed closed" && group == topology.MetaGroupID {
					r.Effects = nil
				}
				r.Closed = mode == "closed" || mode == "consumed closed"
				collector.evidence.authority.records[key], collector.evidence.authority.floors[host] = r, key
				if mode == "displaced" {
					key.Revision++
					collector.evidence.authority.floors[host] = key
				}
			}
			before, _ := json.Marshal(s)
			err := prepareDeletionDebts(&s, next, collector)
			want := map[string]error{"missing": fault.ErrUnavailable, "foreign": fault.ErrOperationAuthorization,
				"closed": fault.ErrAuthorityFenced, "displaced": fault.ErrAuthorityFenced}[mode]
			if !errors.Is(err, want) {
				t.Fatal("incorrect pending/foreign/fenced decision", err, want)
			}
			if err == nil && len(next.Cleanup) != map[string]int{"mixed": 1}[mode] {
				t.Fatal("approval fabricated readmission debt or lost the ordinary host", next.Cleanup)
			}
			after, _ := json.Marshal(s)
			if !bytes.Equal(before, after) {
				t.Fatal("deletion changed retained readmission proof")
			}
			if mode == "partial" {
				later := next.Target.Clone()
				later.MetaGroup().Replicas[0].ReplicaID = 3
				_, err := collector.deletionReadmission("shard0",
					topology.AuthorityTarget{NodeName: "n1", MemberID: 2, ReplicaID: 2}, later)
				if !errors.Is(err, fault.ErrUnavailable) {
					t.Fatal("older batch hid missing successor issuance", err)
				}
			}
			if mode == "partial" || mode == "consumed closed" {
				// Test deletion's restore predicate independently of metadata handover.
				next.Latest = next.Target.Clone()
				next.Allocations = slices.Clone(next.Allocations)
				next.Allocations[0].NextReplicaID = 3
				next.History = append(slices.Clone(next.History), s.Target.Clone())
				next.Admissions, next.AuthorityRecords, next.AuthorityFloors = collector.evidence.admissions, nil, nil
				for _, r := range collector.evidence.authority.records {
					next.AuthorityRecords = append(next.AuthorityRecords, scopedAuthorityRecord{Class: "execution", authorityRecord: *r})
				}
				for resource, key := range collector.evidence.authority.floors {
					next.AuthorityFloors = append(next.AuthorityFloors, resourceFloor{Resource: resource, Key: key})
				}
				index, err := next.buildIndexes()
				if err != nil {
					t.Fatal(err)
				}
				if err := validateScopedDeletion(next, index); err != nil {
					t.Fatal("pending coverage lost on restore", err)
				}
				next.Admissions = nil
				if err := validateScopedDeletion(next, index); !errors.Is(err, fault.ErrUnreadable) {
					t.Fatal("restore accepted unowned omitted host", err)
				}
			}
		})
	}
}
