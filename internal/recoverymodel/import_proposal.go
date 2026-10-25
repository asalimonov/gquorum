package recoverymodel

import (
	"slices"

	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// ImportFromManifest preserves the exact accepted audit, timestamp, identities,
// CAS and proposal ID across uncertain metadata responses. Metadata is founded
// normally; only load sources are transplanted by the replicated import.
func ImportFromManifest(manifest LocalManifest) ImportProposal {
	proposal := ImportProposal{Schema: SchemaVersion, RecoveryID: manifest.Request.RecoveryID,
		PlanDigest:      manifest.PlanDigest,
		ExpectedVersion: 1, Source: manifest.Source,
		Survivor: storage.Identity{NodeName: manifest.Identity.NodeName,
			ClusterID: manifest.Identity.ClusterID, GroupID: topology.MetaGroupID,
			ReplicaID: manifest.Identity.MemberID},
		ApprovedSource: manifest.Approved.Clone(), Floors: slices.Clone(manifest.Floors),
		Retired: slices.Clone(manifest.Retired),
		Actor:   manifest.Request.Actor, Comment: manifest.Request.Comment, At: manifest.AcceptedAt,
		ProposalID: manifest.Request.RecoveryID + ".import"}
	for _, source := range manifest.Sources {
		if source.GroupID != topology.MetaGroupID {
			proposal.Sources = append(proposal.Sources, source)
		}
	}
	return proposal
}
