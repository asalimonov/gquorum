package observe

import (
	"fmt"

	"github.com/asalimonov/gquorum/metrics"
	"github.com/asalimonov/gquorum/topology"
)

func evidenceFixture() (topology.Document, MetricsSettings) {
	p := topology.DefaultVoterConfig()
	d := topology.Document{ClusterID: "cluster", Version: 1, Groups: []topology.Group{{ID: topology.MetaGroupID, VoterConfig: &p}}}
	for i := uint64(1); i <= 5; i++ {
		role := topology.ReplicaVoter
		if i > 3 {
			role = topology.ReplicaLearner
		}
		d.Groups[0].Replicas = append(d.Groups[0].Replicas, topology.Replica{NodeName: fmt.Sprintf("n%d", i), ReplicaID: i, Role: role})
	}
	s, _ := (MetricsSettings{}).Resolve()
	return d, s
}
func evidenceRound(d topology.Document, seq uint64) (Round, []metrics.ReplicaObservation) {
	probes := make([]Probe, 0, 5)
	for _, r := range d.Groups[0].Replicas {
		id := metrics.ReplicaIdentity{ClusterID: d.ClusterID, GroupID: topology.MetaGroupID, NodeName: r.NodeName, ReplicaID: r.ReplicaID, InstanceID: r.NodeName + "-process", RunID: r.NodeName + "-run"}
		stamp := metrics.Stamp{Sequence: seq, State: metrics.Current}
		sample := metrics.ReplicaSample{Identity: id, Meter: stamp, Raft: stamp, Writer: stamp, Running: true, Ready: true, LeaderID: 1, Term: 1, Commit: 2000, Applied: 2000, Voters: []uint64{1, 2, 3}, Learners: []uint64{4, 5}, MembershipDigest: "members"}
		if r.ReplicaID == 1 {
			for i := uint64(1); i <= 5; i++ {
				sample.Progress = append(sample.Progress, metrics.PeerProgress{ReplicaID: i, Match: 2000, AckKnown: true, AckSequence: seq})
			}
		}
		obs := metrics.ReplicaObservation{State: metrics.Current, Reachability: "alive", Sample: sample}
		probes = append(probes, Probe{Identity: id, Observation: obs, Dispatched: true, Reachability: "alive"})
	}
	return Round{Epoch: LeaderEpoch{InstanceID: "n1-process", Term: 1, LeaderID: 1}, Groups: map[string]GroupSamples{topology.MetaGroupID: {SourceVersion: d.Version, Probes: probes}}}, []metrics.ReplicaObservation{probes[0].Observation.Clone()}
}

func evidenceTopology(doc topology.Document) topology.ProgressPage {
	page := topology.ProgressPage{TargetVersion: doc.Version}
	for _, g := range doc.Groups {
		group := g
		page.Groups = append(page.Groups, (topology.GroupProgress{GroupID: group.ID,
			ConvergedVersion: doc.Version, Converged: &group, Approved: &group}).Clone())
	}
	return page
}
