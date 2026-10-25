package recoveryinspect

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metastore"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/topology"
)

func systemInventory(ctx context.Context, state *SourceState, accepted *model.AcceptedPlan) error {
	for name, digest := range map[string]string{"_meta.bolt": state.metadataDigest,
		"_authority.bolt": state.authorityDigest} {
		if err := state.owner.ValidateArtifact(ctx, "system", name, state.resources.MaxMetadataBytes); err != nil {
			return err
		}
		accepted.Archive = append(accepted.Archive, model.ArchiveEntry{Root: "system", Name: name,
			Kind: "regular_file", Classification: "source", EvidenceDigest: digest})
	}
	return scanDirectory(ctx, state.paths.StorageDir, state.resources.MaxMetadataBytes,
		func(entry os.DirEntry) error {
			if !strings.HasPrefix(entry.Name(), "_meta.bolt.retired-") {
				return nil
			}
			fact := localArtifact{Root: "system", Name: entry.Name(),
				Classification: "unrecognized_readmission_backup"}
			defer func() { state.local = append(state.local, fact) }()
			if err := verifyReadmissionBackup(ctx, *state, entry.Name()); err != nil {
				accepted.Issues = append(accepted.Issues, model.Issue{Code: "unrecognized_readmission_backup",
					Blocking: true,
					Message:  "readmission backup lacks exact retained name and owner proof: " + entry.Name(),
					Remedy:   model.RemedyInvestigate})
				return ctx.Err()
			}
			digest, err := state.owner.ArtifactDigest(ctx, "system", entry.Name(), state.resources.MaxMetadataBytes)
			if err != nil {
				return err
			}
			fact.Classification, fact.Digest = "readmission_backup", digest
			accepted.Archive = append(accepted.Archive, model.ArchiveEntry{Root: "system", Name: entry.Name(),
				Kind: "regular_file", Classification: fact.Classification, EvidenceDigest: digest})
			return nil
		})
}

func verifyReadmissionBackup(ctx context.Context, state SourceState, name string) error {
	if err := state.owner.ValidateArtifact(ctx, "system", name, state.resources.MaxMetadataBytes); err != nil {
		return err
	}
	owner, found := state.inventory.ReadmissionBackups[name]
	if !found && state.authority.AdmissionID != "" && state.authority.PreviousMemberID != 0 {
		nonce := sha256.Sum256([]byte(state.authority.AdmissionID))
		expected := fmt.Sprintf("_meta.bolt.retired-%d-",
			state.authority.PreviousMemberID) + hex.EncodeToString(nonce[:8])
		if name == expected {
			owner = topology.Identity{ClusterID: state.identity.ClusterID,
				NodeName: state.identity.NodeName, MemberID: state.authority.PreviousMemberID}
			found = true
		}
	}
	if !found || owner.NodeName != state.identity.NodeName || owner.ClusterID != state.identity.ClusterID ||
		owner.MemberID == 0 || owner.MemberID >= state.identity.MemberID {
		return fault.ErrIdentityMismatch
	}
	reader, err := metastore.OpenReadOnly(ctx, filepath.Join(state.paths.StorageDir, name),
		state.resources.MaxMetadataBytes)
	if err != nil {
		return err
	}
	defer reader.Close()
	pin, err := reader.Identity()
	if err != nil {
		return err
	}
	if pin.NodeName != owner.NodeName || pin.ClusterID != owner.ClusterID || pin.MemberID != owner.MemberID {
		return fault.ErrIdentityMismatch
	}
	return nil
}
