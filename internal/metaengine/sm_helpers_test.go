package metaengine

import (
	"slices"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

// withTestVoterProof binds a handwritten fixture plan to its declared base.
// Production plans obtain these proofs from topology.BuildPlan.
func withTestVoterProof(base, desired topology.Document, plan topology.Plan) topology.Plan {
	plan.VoterBases = nil
	groups := make(map[string]bool)
	for _, step := range plan.Steps {
		switch step.Kind {
		case topology.StepCreateFoundingGroup, topology.StepAddLearner, topology.StepPromoteVoter, topology.StepDemoteVoter, topology.StepRemoveReplica:
			groups[step.GroupID] = true
		}
	}
	ids := make([]string, 0, len(groups))
	for id := range groups {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b string) int {
		if a == b {
			return 0
		}
		if a == topology.MetaGroupID {
			return -1
		}
		if b == topology.MetaGroupID {
			return 1
		}
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	})
	sets := make(map[string]map[uint64]bool, len(ids))
	for _, id := range ids {
		set := make(map[uint64]bool)
		if group := base.Group(id); group != nil {
			for _, replica := range group.Replicas {
				if replica.Role == topology.ReplicaVoter {
					set[replica.ReplicaID] = true
				}
			}
		}
		sets[id] = set
		plan.VoterBases = append(plan.VoterBases, topology.PlanVoterBase{GroupID: id, Voters: testVoterIDs(set)})
	}
	for i := range plan.Steps {
		step := &plan.Steps[i]
		set, member := sets[step.GroupID]
		if !member {
			continue
		}
		switch step.Kind {
		case topology.StepCreateFoundingGroup, topology.StepAddLearner, topology.StepPromoteVoter, topology.StepDemoteVoter, topology.StepRemoveReplica:
		default:
			continue
		}
		step.ExpectedVotersDigest = topology.VoterSetDigest(testVoterIDs(set))
		switch step.Kind {
		case topology.StepCreateFoundingGroup:
			for _, replica := range desired.VotersOf(step.GroupID) {
				set[replica.ReplicaID] = true
			}
		case topology.StepPromoteVoter:
			set[step.ReplicaID] = true
		case topology.StepDemoteVoter, topology.StepRemoveReplica:
			delete(set, step.ReplicaID)
		}
		step.ResultVotersDigest = topology.VoterSetDigest(testVoterIDs(set))
	}
	return plan
}

func testVoterIDs(set map[uint64]bool) []uint64 {
	ids := make([]uint64, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// baseDoc returns a minimal structurally valid document (one active node, one
// shard with a single voter). The state machine assigns the version, so the
// Version field here is advisory.
func baseDoc() topology.Document {
	return topology.Document{
		ClusterID: "cluster-abc",
		Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3},
		Nodes: []topology.Node{
			{Name: "n1", Type: "worker", State: topology.NodeActive},
		},
		Groups: []topology.Group{
			{ID: topology.MetaGroupID, NextReplicaID: 2, Replicas: []topology.Replica{
				{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter},
			}},
			{ID: "shard0", NextReplicaID: 2, Replicas: []topology.Replica{
				{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter},
			}},
		},
		ChangedBy: "n1",
	}
}

// mustEncode encodes a proposal or fails the test.
func mustEncode(t *testing.T, p any) []byte {
	t.Helper()
	data, err := Encode(p)
	if err != nil {
		t.Fatalf("Encode(%T): %v", p, err)
	}
	return data
}

// apply applies an encoded proposal at the next index (term 1) or fails.
func apply(t *testing.T, sm *topologySM, index uint64, p any) ApplyResult {
	t.Helper()
	res, err := sm.Apply(index, 1, mustEncode(t, p))
	if err != nil {
		t.Fatalf("Apply(%T): %v", p, err)
	}
	return res
}

func testPlan(version uint64, count int) topology.Plan {
	plan := topology.Plan{Version: version}
	for seq := 1; seq <= count; seq++ {
		plan.Steps = append(plan.Steps, topology.Step{
			Seq: seq, GroupID: "shard0", Kind: topology.StepTransferLeader,
			NodeName: "n1", ReplicaID: 1,
		})
	}
	return plan
}

func beginProposal(version uint64, count int, id string, at time.Time) BeginOperatorProposal {
	return BeginOperatorProposal{
		Version: version, Plan: testPlan(version, count), Actor: "n1",
		At: at, ProposalID: id,
	}
}

func testRules() topology.Rules {
	return topology.Rules{SchemaVersion: 1, Limits: topology.Limits{MaxNodes: 4, MaxReplicas: 5}}
}
