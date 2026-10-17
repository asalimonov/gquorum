package metaservice

import (
	"context"

	"github.com/asalimonov/gquorum/internal/metaengine"
)

func mutationProposal(documents mutationDocuments) metaengine.ProposeDocument {
	proposal := metaengine.ProposeDocument{Doc: documents.Next.Clone(), ExpectedVersion: documents.Base.Version}
	if documents.Intent == nil {
		return proposal
	}
	intent := *documents.Intent
	if intent.Kind == "add_node" || intent.Kind == "readmit_node" {
		intent.SuccessorID = proposal.Doc.MemberID(intent.NodeName)
	}
	if intent.Kind == "reseat_replica" {
		if group := proposal.Doc.Group(intent.GroupID); group != nil {
			for _, replica := range group.Replicas {
				if replica.NodeName == intent.NodeName {
					intent.SuccessorID = replica.ReplicaID
				}
			}
		}
	}
	proposal.Intent = &intent
	return proposal
}

func (g *Group) stageMutation(ctx context.Context, documents mutationDocuments) (metaengine.ApplyResult, error) {
	return g.propose(ctx, mutationProposal(documents))
}
