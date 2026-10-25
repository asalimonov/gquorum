package recoverydisk

import (
	"encoding/base64"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftstate"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// ValidateApprovalShape verifies future journal/import encodings without I/O.
func ValidateApprovalShape(input model.AcceptedPlan) error {
	accepted := model.CanonicalAccepted(input)
	digest, err := model.DigestAccepted(accepted)
	if err != nil {
		return err
	}
	const shapeYear = 2000
	at := time.Date(shapeYear, time.January, 1, 0, 0, 0, 0, time.UTC)
	name, err := model.BackupName(at, accepted.Request.RecoveryID)
	if err != nil {
		return err
	}
	manifest := newManifest(accepted, model.Acceptance{PlanDigest: digest, AcknowledgeDataLoss: true,
		OldDeploymentFenced: true}, at, name)
	if _, err := model.EncodeManifest(manifest); err != nil {
		return err
	}
	identityHeader, err := model.EncodeCanonical(struct {
		Source,
		Destination topology.Identity
	}{accepted.Source, accepted.Identity})
	if err != nil {
		return err
	}
	if uint64(len(identityHeader)) > model.HeaderMaxBytes {
		return fault.New(fault.Invalid, "recovery identity exceeds metadata header bound", nil)
	}
	if accepted.Request.Mode == model.ResetForJoin {
		return nil
	}
	proposal := model.ImportProposal{Schema: model.SchemaVersion, RecoveryID: accepted.Request.RecoveryID,
		PlanDigest:      digest,
		ExpectedVersion: 1, Source: accepted.Source,
		Survivor: storage.Identity{NodeName: accepted.Identity.NodeName,
			ClusterID: accepted.Identity.ClusterID, GroupID: topology.MetaGroupID,
			ReplicaID: accepted.Identity.MemberID},
		ApprovedSource: accepted.Approved, Sources: accepted.Sources, Floors: accepted.Floors,
		Retired: accepted.Retired,
		Actor:   accepted.Request.Actor, Comment: accepted.Request.Comment, At: at,
		ProposalID: accepted.Request.RecoveryID}
	data, err := model.EncodeCanonical(proposal)
	if err != nil {
		return err
	}
	const normalMetadataProposalBytes = 64 << 20
	if len(data) > normalMetadataProposalBytes {
		return fault.New(fault.Invalid, "recovery import exceeds ordinary metadata proposal bound", nil)
	}
	// The metadata envelope embeds its body as raw JSON; the shared engine then
	// base64-encodes that complete proposal in an application entry. Reserve one
	// bounded header for correlation/request identity supplied by normal startup.
	const metadataEnvelopeBytes = len(`{"schema_version":4,"kind":"import_recovery","body":}`)
	entryHeader, err := raftstate.EncodeEntry(raftstate.Entry{Schema: raftstate.Schema, Kind: raftstate.Application,
		Correlation: accepted.Request.RecoveryID, RequestID: proposal.ProposalID})
	if err != nil {
		return err
	}
	bodyBytes := base64.StdEncoding.EncodedLen(len(data) + metadataEnvelopeBytes)
	if bodyBytes < 0 {
		return fault.ErrInvalid
	}
	encoded := uint64(bodyBytes) + uint64(len(entryHeader)) + model.HeaderMaxBytes
	if encoded > normalMetadataProposalBytes {
		return fault.New(fault.Invalid, "encoded recovery import exceeds ordinary Raft entry bound", nil)
	}
	return validateFutureSeedHeaders(accepted, digest)
}

func validateFutureSeedHeaders(accepted model.AcceptedPlan, digest string) error {
	floors := make(map[string]uint64, len(accepted.Floors))
	for _, floor := range accepted.Floors {
		floors[floor.GroupID] = floor.NextID
	}
	for _, source := range accepted.Sources {
		if source.GroupID == topology.MetaGroupID {
			continue
		}
		marker := model.SeedMarker{Schema: model.SchemaVersion, RecoveryID: accepted.Request.RecoveryID,
			PlanDigest: digest,
			Source:     source, Destination: storage.Identity{NodeName: accepted.Identity.NodeName,
				ClusterID: accepted.Identity.ClusterID,
				GroupID:   source.GroupID, ReplicaID: floors[source.GroupID]},
			MemberID: accepted.Identity.MemberID, ImageDigest: digest}
		data, err := model.EncodeCanonical(marker)
		if err != nil {
			return err
		}
		grant, err := model.EncodeCanonical(marker.Grant)
		if err != nil {
			return err
		}
		if len(data) < len(grant) {
			return fault.ErrInvalid
		}
		if uint64(len(data))-uint64(len(grant)) > model.HeaderMaxBytes {
			return fault.New(fault.Invalid, "recovery seed identity exceeds header bound", nil)
		}
	}
	return nil
}
