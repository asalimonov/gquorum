package recoveryinspect

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"slices"

	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/storage"
)

func hashValue(value any) (string, error) {
	sum := sha256.New()
	if err := json.NewEncoder(sum).Encode(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(sum.Sum(nil)), nil
}

func sourceFingerprint(state SourceState, accepted model.AcceptedPlan) (string, error) {
	checkpoint := sha256.Sum256(state.checkpoint)
	canonical := model.CanonicalAccepted(accepted)
	local := slices.Clone(state.local)
	slices.SortFunc(local, func(a, b localArtifact) int {
		return cmp.Or(cmp.Compare(a.Root, b.Root),
			cmp.Compare(a.Name, b.Name))
	})
	return hashValue(struct {
		Metadata, Authority, Checkpoint string
		Ledger                          any
		Replicas                        []model.ReplicaEvidence
		Archive                         []model.ArchiveEntry
		Local                           []localArtifact
	}{state.metadataDigest, state.authorityDigest, hex.EncodeToString(checkpoint[:]), state.ledger,
		state.inventory.Replicas, canonical.Archive, local})
}

func recoveryBytes(retained storage.Recovery) uint64 {
	snapshotSize := retained.Snapshot.Size()
	if snapshotSize < 0 {
		return 0
	}
	bytes := uint64(snapshotSize)
	for _, entry := range retained.Entries {
		size := entry.Size()
		if size < 0 || uint64(size) > math.MaxUint64-bytes {
			return 0
		}
		bytes += uint64(size)
	}
	return bytes
}
