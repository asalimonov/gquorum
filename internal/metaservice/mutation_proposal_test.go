//go:build !functional

package metaservice

import (
	"testing"

	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
)

func TestMutationProposalOwnsExactProvenanceAndSuccessor(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"add_node", "readmit_node", "reseat_replica"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			docs := mutationDocuments{Base: topology.Document{Version: 4}, Next: topology.Document{Version: 5, Groups: []topology.Group{{ID: topology.MetaGroupID, Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 9}}}, {ID: "work", Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 11}}}}}, Intent: &metaengine.Intent{Kind: kind, NodeName: "n1", PreviousID: 1, Version: 5, ExpectedVersion: 4}}
			want := uint64(9)
			if kind == "reseat_replica" {
				docs.Intent.GroupID = "work"
				want = 11
			}
			p := mutationProposal(docs)
			if p.ExpectedVersion != 4 || p.Intent.SuccessorID != want || p.Intent.PreviousID != 1 || p.Intent.Version != 5 {
				t.Fatal(p)
			}
			p.Intent.SuccessorID = 100
			p.Doc.Groups[0].Replicas[0].ReplicaID = 100
			if docs.Intent.SuccessorID != 0 || docs.Next.Groups[0].Replicas[0].ReplicaID != 9 {
				t.Fatal("proposal aliases caller-owned state")
			}
		})
	}
}
