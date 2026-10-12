//go:build !functional

package metaengine

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func voterChangeFixture(t *testing.T, groupID string, surge int) (*topologySM, CommitVoterChangeProposal, uint64) {
	t.Helper()
	rules := topology.Rules{SchemaVersion: 1, Limits: topology.Limits{MaxNodes: 5, MaxReplicas: 5}}
	sm := newTopologySM(nil, rules)
	p := topology.DefaultVoterPolicy()
	p.MaxSurgeVoters = surge
	doc := topology.Document{ClusterID: "cluster", Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 5}, Groups: []topology.Group{
		{ID: topology.MetaGroupID, NextReplicaID: 6, VoterPolicy: &p},
		{ID: "work", NextReplicaID: 6, VoterPolicy: &p, Hosting: &topology.HostingRule{Mode: topology.HostingAllEligible}},
	}}
	for i := 1; i <= 5; i++ {
		name := fmt.Sprintf("n%d", i)
		doc.Nodes = append(doc.Nodes, topology.Node{Name: name, Type: "worker", State: topology.NodeActive})
		role := topology.ReplicaVoter
		if i > 3 {
			role = topology.ReplicaLearner
		}
		for j := range doc.Groups {
			doc.Groups[j].Replicas = append(doc.Groups[j].Replicas, topology.Replica{NodeName: name, ReplicaID: uint64(i), Role: role})
		}
	}
	at := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	if r := apply(t, sm, 1, ProposeDocument{Doc: doc}); !r.Accepted {
		t.Fatal(r)
	}
	op := apply(t, sm, 2, BeginOperatorProposal{Version: 1, Plan: topology.Plan{Version: 1}, At: at})
	if !op.Accepted {
		t.Fatal(op)
	}
	if r := apply(t, sm, 3, TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: op.OperatorID, Version: 1, Attempt: 1, State: topology.OperatorSucceeded, At: at}}); !r.Accepted {
		t.Fatal(r)
	}
	if r := apply(t, sm, 4, ActivateTargetProposal{OperatorID: op.OperatorID, Version: 1, Attempt: 1}); !r.Accepted {
		t.Fatal(r)
	}
	change := topology.VoterChange{GroupID: groupID, Promote: &topology.ReplicaKey{NodeName: "n4", ReplicaID: 4}, Demote: &topology.ReplicaKey{NodeName: "n3", ReplicaID: 3}}
	active := sm.Active()
	desired, err := VoterChangeDocument(active, change, at.Add(time.Second), "scheduler")
	if err != nil {
		t.Fatal(err)
	}
	statuses := assumedReadmissionStatuses(active, "")
	for id, st := range statuses {
		st.HealthKnown = true
		st.LiveVoters = map[uint64]bool{1: true, 2: true, 3: false, 4: true, 5: true}
		statuses[id] = st
	}
	plan, err := topology.BuildPlan(active, desired, statuses, rules)
	if err != nil {
		t.Fatal(err)
	}
	return sm, CommitVoterChangeProposal{Change: change, Doc: desired, ExpectedVersion: 1, Plan: plan, At: at.Add(time.Second), ProposalID: "replace"}, 5
}

func TestVoterChangeScopedCASAndCodec(t *testing.T) {
	t.Parallel()
	for _, groupID := range []string{topology.MetaGroupID, "work"} {
		t.Run(groupID, func(t *testing.T) {
			t.Parallel()
			sm, p, index := voterChangeFixture(t, groupID, 1)
			data, err := Encode(p)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := DecodeProposal(data)
			if err != nil || !reflect.DeepEqual(decoded, &p) {
				t.Fatal("command round trip", decoded, err)
			}
			active := sm.Active()
			result := apply(t, sm, index, p)
			if !result.Accepted || result.OperatorID == "" {
				t.Fatal(result)
			}
			if !reflect.DeepEqual(active, sm.Active()) || !reflect.DeepEqual(sm.Latest(), sm.Target()) || len(sm.VoterSwitches()) != 0 {
				t.Fatal("approval advanced active or invented completed role records")
			}
			ex, ok := sm.CurrentOperator()
			if !ok || !reflect.DeepEqual(ex.Plan, p.Plan) {
				t.Fatal("immutable plan lost", ex)
			}
			if retry := apply(t, sm, index+1, p); retry.Accepted || retry.Code != proposalVersionConflict {
				t.Fatal("stale CAS accepted", retry)
			}
		})
	}
}

func TestVoterChangeRefusesUnrelatedIntentAndPlanEdits(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*CommitVoterChangeProposal){
		"policy": func(p *CommitVoterChangeProposal) { p.Doc.Group("work").VoterPolicy.FailureWindow++ },
		"hosting": func(p *CommitVoterChangeProposal) {
			p.Doc.Group("work").Hosting = &topology.HostingRule{Mode: topology.HostingExplicit, Nodes: []string{"n1"}}
		},
		"pin":             func(p *CommitVoterChangeProposal) { p.Doc.Group("work").Replicas[4].PinnedLearner = true },
		"label":           func(p *CommitVoterChangeProposal) { p.Doc.Nodes[0].Labels = map[string]string{"staged": "unrelated"} },
		"allocator":       func(p *CommitVoterChangeProposal) { p.Doc.Group("work").NextReplicaID++ },
		"identity":        func(p *CommitVoterChangeProposal) { p.Change.Promote.ReplicaID = 5 },
		"omitted effect":  func(p *CommitVoterChangeProposal) { p.Plan.Steps = p.Plan.Steps[:2] },
		"guard":           func(p *CommitVoterChangeProposal) { p.Plan.Steps[2].TransferReplicaID = 5 },
		"ordering":        func(p *CommitVoterChangeProposal) { p.Plan.Steps[0].Seq = 8 },
		"unrelated group": func(p *CommitVoterChangeProposal) { p.Plan.Steps[0].GroupID = topology.MetaGroupID },
		"timestamp":       func(p *CommitVoterChangeProposal) { p.At = p.At.Add(time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sm, p, index := voterChangeFixture(t, "work", 1)
			before := sm.Active()
			mutate(&p)
			result := apply(t, sm, index, p)
			if result.Accepted || !reflect.DeepEqual(before, sm.Latest()) || !reflect.DeepEqual(before, sm.Target()) || !reflect.DeepEqual(before, sm.Active()) {
				t.Fatal("scope escaped refusal", result)
			}
		})
	}
	t.Run("staged intent wins", func(t *testing.T) {
		t.Parallel()
		sm, p, index := voterChangeFixture(t, "work", 1)
		staged := sm.Active()
		staged.Comment = "operator intent"
		if r := apply(t, sm, index, ProposeDocument{Doc: staged, ExpectedVersion: 1}); !r.Accepted {
			t.Fatal(r)
		}
		p.ExpectedVersion = 2 // Even a caller guessing the newer CAS cannot approve it.
		if r := apply(t, sm, index+1, p); r.Accepted {
			t.Fatal("automatically approved staged intent")
		}
		if sm.Target().Version != 1 || sm.Latest().Comment != "operator intent" {
			t.Fatal("unrelated stage lost")
		}
	})
}

func TestVoterRoleRecordsResumeWithoutCooldownReset(t *testing.T) {
	t.Parallel()
	for _, surge := range []int{0, 1} {
		t.Run(fmt.Sprint(surge), func(t *testing.T) {
			t.Parallel()
			sm, p, index := voterChangeFixture(t, "work", surge)
			result := apply(t, sm, index, p)
			index++
			if !result.Accepted {
				t.Fatal(result)
			}
			for _, s := range p.Plan.Steps {
				applyAdmissionTransition(t, sm, index, result.OperatorID, p.Plan, s.Seq, topology.OperatorRunning, p.At)
				index++
				data, err := sm.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				restored := newTopologySM(nil, sm.rules)
				if err := restored.Restore(data); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(restored.VoterSwitches(), sm.VoterSwitches()) {
					t.Fatal("role effect disappeared through custom disk codec")
				}
				sm = restored
			}
			last := p.Plan.Steps[len(p.Plan.Steps)-1]
			transition := TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: result.OperatorID, Version: p.Plan.Version, Attempt: 1, StepSeq: last.Seq, State: topology.OperatorSucceeded, GroupID: last.GroupID, NodeName: last.NodeName, At: p.At}}
			if r := apply(t, sm, index, transition); !r.Accepted {
				t.Fatal(r)
			}
			index++
			records := sm.VoterSwitches()
			if len(records) != 1 || len(records[0].Replicas) != 2 || records[0].Last.Index != index-1 {
				t.Fatal("role effects not recorded", records)
			}
			transition.Transition.At = p.At.Add(time.Second)
			if r := apply(t, sm, index, transition); !r.Accepted {
				t.Fatal(r)
			}
			index++
			if !reflect.DeepEqual(records, sm.VoterSwitches()) {
				t.Fatal("idempotent acknowledgement reset cooldown")
			}
			if r := apply(t, sm, index, ActivateTargetProposal{OperatorID: result.OperatorID, Version: p.Plan.Version, Attempt: 1}); !r.Accepted {
				t.Fatal(r)
			}
			index++
			// Role records survive the ordinary history/old operator horizon.
			for range historyRetention + 2 {
				d := sm.Latest()
				d.Comment = fmt.Sprint(index)
				if r := apply(t, sm, index, ProposeDocument{Doc: d, ExpectedVersion: d.Version}); !r.Accepted {
					t.Fatal(r)
				}
				index++
				// Converge each metadata-only version so Active/Target no longer
				// pin the replacement operator or its document in history.
				version := sm.Latest().Version
				next := apply(t, sm, index, BeginOperatorProposal{Version: version, Plan: topology.Plan{Version: version}, At: p.At})
				if !next.Accepted {
					t.Fatal(next)
				}
				index++
				if r := apply(t, sm, index, TransitionOperatorProposal{Transition: topology.OperatorTransition{OperatorID: next.OperatorID, Version: version, Attempt: 1, State: topology.OperatorSucceeded, At: p.At}}); !r.Accepted {
					t.Fatal(r)
				}
				index++
				if r := apply(t, sm, index, ActivateTargetProposal{OperatorID: next.OperatorID, Version: version, Attempt: 1}); !r.Accepted {
					t.Fatal(r)
				}
				index++
			}
			if _, retained := sm.operators[result.OperatorID]; retained {
				t.Fatal("test did not prune the original replacement operator")
			}
			data, err := sm.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			restored := newTopologySM(nil, sm.rules)
			if err := restored.Restore(data); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(records, restored.VoterSwitches()) {
				t.Fatal("history pruning erased cooldown")
			}
			for name, corrupt := range map[string]func(*smState){
				"zero identity": func(st *smState) { st.VoterSwitches[0].Replicas[0].ReplicaID = 0 },
				"foreign node": func(st *smState) {
					st.VoterSwitches[0].Replicas[0].NodeName = "foreign"
					if st.VoterSwitches[0].Last.ReplicaID == st.VoterSwitches[0].Replicas[0].ReplicaID {
						st.VoterSwitches[0].Last.NodeName = "foreign"
					}
				},
				"unallocated identity": func(st *smState) {
					state := &st.VoterSwitches[0]
					state.Last.ReplicaID = st.Active.Group("work").NextReplicaID
					state.Replicas = []topology.RoleChangeRecord{state.Last}
				},
				"unknown group": func(st *smState) {
					state := &st.VoterSwitches[0]
					state.GroupID, state.Last.GroupID = "foreign", "foreign"
					state.Replicas = []topology.RoleChangeRecord{state.Last}
				},
			} {
				var state smState
				if err := json.Unmarshal(data, &state); err != nil {
					t.Fatal(err)
				}
				corrupt(&state)
				broken, err := json.Marshal(state)
				if err != nil {
					t.Fatal(err)
				}
				if err := restored.Restore(broken); err == nil {
					t.Fatal(name, "corrupt switch accepted after operator pruning")
				}
			}
		})
	}
}
