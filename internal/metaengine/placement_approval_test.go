//go:build !functional

package metaengine

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func TestStagePlacementUsesReplicatedApprovedViewAtomically(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	initial := baseDoc()
	if result := apply(t, sm, 1, ProposeDocument{Doc: initial, ExpectedVersion: 0, ProposalID: "initial"}); !result.Accepted {
		t.Fatalf("initial stage: %s", result.Reason)
	}

	cases := []struct {
		name string
		edit func(*topology.Document)
		want string
	}{
		{"self-approved reservation", func(d *topology.Document) {
			d.Node("n1").Labels = map[string]string{topology.LabelReserved: "true"}
		}, "ineligible"},
		{"unmatched selector", func(d *topology.Document) {
			d.Group("shard0").Placement = &topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3,
				NodeSelector: map[string]string{"network": "public_ip"}}
		}, "ineligible"},
		{"learner-only voter", func(d *topology.Document) {
			d.Node("n1").Labels = map[string]string{topology.LabelLearnerOnly: "true"}
		}, "ineligible"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidate := sm.latest.Clone()
			tc.edit(&candidate)
			result := apply(t, sm, sm.appliedIndex+1, ProposeDocument{Doc: candidate, ExpectedVersion: 1, ProposalID: tc.name})
			if result.Accepted || !strings.Contains(result.Reason, tc.want) {
				t.Fatalf("unexpected stage result: %+v", result)
			}
			if result.Placement == nil || len(result.Placement.Groups) == 0 || len(result.Placement.Groups[0].Issues) == 0 {
				t.Fatalf("rejected placement lost structured report: %+v", result)
			}
			encoded, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			var decoded ApplyResult
			if err := json.Unmarshal(encoded, &decoded); err != nil || decoded.Placement == nil || len(decoded.Placement.Groups) == 0 {
				t.Fatal("custom apply-result codec lost placement", err, decoded)
			}
			if sm.latest.Version != 1 || sm.target.Version != 0 || len(sm.history) != 1 || sm.allocations["shard0"] != 2 {
				t.Fatalf("rejected placement changed state: latest=%d target=%d history=%d allocations=%v", sm.latest.Version, sm.target.Version, len(sm.history), sm.allocations)
			}
		})
	}
}

func TestFailedApprovalRetainsStagedPlacementVersion(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	staged := apply(t, sm, 1, ProposeDocument{Doc: baseDoc(), ExpectedVersion: 0, ProposalID: "stage"})
	if !staged.Accepted || staged.Doc.Version != 1 {
		t.Fatal(staged)
	}
	forged := topology.Plan{Version: 1, Steps: []topology.Step{{
		Seq: 1, Kind: topology.StepPromoteVoter, GroupID: "shard0", NodeName: "n1", ReplicaID: 1,
	}}}
	result := apply(t, sm, 2, BeginOperatorProposal{Version: 1, Plan: forged, Actor: "n1", At: time.Now().UTC(), ProposalID: "approve"})
	if result.Accepted || result.Code != proposalAuthorization {
		t.Fatalf("forged approval accepted: %+v", result)
	}
	if sm.latest.Version != staged.Doc.Version || sm.target.Version != 0 || len(sm.operators) != 0 {
		t.Fatalf("failed approval discarded staged version or published authority: latest=%d target=%d operators=%d", sm.latest.Version, sm.target.Version, len(sm.operators))
	}
}

func TestStageRejectsStrictDomainShortage(t *testing.T) {
	t.Parallel()
	sm := newTopologySM(nil, testRules())
	doc := baseDoc()
	doc.Node("n1").Labels = map[string]string{"az": "one"}
	for _, name := range []string{"n2", "n3"} {
		doc.Nodes = append(doc.Nodes, topology.Node{Name: name, Type: "worker", State: topology.NodeActive,
			Labels: map[string]string{"az": "two"}})
		if err := doc.SetMemberID(name, uint64(len(doc.Nodes)), doc.CreatedAt); err != nil {
			t.Fatal(err)
		}
	}
	voterConfig := topology.DefaultVoterConfig()
	voterConfig.DistinctVoterLabel = "az"
	doc.Group("shard0").VoterConfig = &voterConfig
	doc.Group("shard0").Hosting = &topology.HostingRule{Mode: topology.HostingAllEligible}
	result := apply(t, sm, 1, ProposeDocument{Doc: doc, ExpectedVersion: 0, ProposalID: "strict"})
	if result.Accepted || !strings.Contains(result.Reason, "distinct voter domains") {
		t.Fatalf("strict shortage was accepted or unexplained: %+v", result)
	}
	if sm.latest.Version != 0 || len(sm.history) != 0 || len(sm.allocations) != 0 {
		t.Fatalf("strict rejection changed state: %+v", sm)
	}
	doc.Node("n3").Labels["az"] = "three"
	result = apply(t, sm, 2, ProposeDocument{Doc: doc, ExpectedVersion: 0, ProposalID: "partial-founding"})
	if result.Accepted || !strings.Contains(result.Reason, "founding voters") {
		t.Fatalf("partial strict founding was accepted or unexplained: %+v", result)
	}
}
