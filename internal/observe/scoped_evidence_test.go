//go:build !functional

package observe

import (
	"testing"
	"time"

	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

func TestEvidenceScopeChangePreservesCooldownAndRejectsPredecessor(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"source", "assignment", "foreign_leader"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			doc, settings := evidenceFixture()
			page := evidenceTopology(doc)
			tracker := newEvidenceTracker(settings)
			tracker.reset("epoch")
			start := time.Unix(100, 0)
			record := topology.RoleChangeRecord{Index: 10, GroupID: topology.MetaGroupID, NodeName: "n4", ReplicaID: 4, From: topology.ReplicaVoter, To: topology.ReplicaLearner}
			input := evidenceInput{clusterID: doc.ClusterID, switches: []topology.VoterSwitchState{{GroupID: record.GroupID, Last: record, Replicas: []topology.RoleChangeRecord{record}}}}
			round, local := evidenceRound(doc, 1)
			input.local = local
			tracker.apply(start, page, round, input)
			round, input.local = evidenceRound(doc, 2)
			samples := round.Groups[topology.MetaGroupID]
			switch change {
			case "source":
				page.Groups[0].ConvergedVersion++
				samples.SourceVersion++
			case "assignment":
				page.Groups[0].Converged.Replicas[2].ReplicaID = 33
			case "foreign_leader":
				samples.Probes[0].Identity.GroupID = "other"
				samples.Probes[0].Observation.Sample.Identity.GroupID = "other"
			}
			round.Groups[topology.MetaGroupID] = samples
			now := start.Add(time.Second)
			tracker.apply(now, page, round, input)
			group := tracker.snapshot(now)[0]
			if group.CooldownRemaining != 299*time.Second || observedReplica(t, tracker, now, 4).Evidence.DemotionCooldownRemaining != 299*time.Second {
				t.Fatal("scope change rearmed retained role cooldown", group)
			}
			if change == "assignment" {
				successor := observedReplica(t, tracker, now, 33)
				if successor.Sample.Identity.ReplicaID != 33 || successor.State != metrics.Unknown ||
					!successor.ReceivedAt.IsZero() || successor.Sample.Meter.Sequence != 0 || successor.Reachability != "unknown" {
					t.Fatal("predecessor observation relabeled as successor", successor)
				}
			}
			if change == "foreign_leader" && group.State == metrics.Current {
				t.Fatal("foreign exact probe became this group's leader evidence", group)
			}
		})
	}
}

func TestMetricsRoundMatchesOnlyProvenSourceAndDeclaration(t *testing.T) {
	t.Parallel()
	doc, _ := evidenceFixture()
	before := evidenceTopology(doc)
	round, _ := evidenceRound(doc, 1)
	for _, change := range []string{"context", "approval", "source", "assignment", "sample_source", "absent"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			after := before.Clone()
			candidate := Round{Epoch: round.Epoch, Groups: map[string]GroupSamples{topology.MetaGroupID: round.Groups[topology.MetaGroupID]}}
			switch change {
			case "context":
				after.TargetVersion = 99
				after.Nodes = []topology.NodeProgress{{NodeName: "n1", MemberID: 999}}
			case "approval":
				after.Groups[0].Approved.Replicas[0].ReplicaID = 999
				after.Groups[0].OperationID = topology.MakeOperationID(99, 1)
			case "source":
				after.Groups[0].ConvergedVersion++
			case "assignment":
				after.Groups[0].Converged.Replicas[0].ReplicaID++
			case "sample_source":
				samples := candidate.Groups[topology.MetaGroupID]
				samples.SourceVersion++
				candidate.Groups[topology.MetaGroupID] = samples
			case "absent":
				after.Groups = nil
			}
			kept := filterMetricsRound(before, after, candidate)
			want := change == "context" || change == "approval"
			if (len(kept.Groups) == 1) != want || kept.Epoch != round.Epoch {
				t.Fatal("wrong per-group capture decision", kept)
			}
		})
	}
}
