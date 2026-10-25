package walbackend

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/storage"
)

// ReadReplicaPin validates an exact active or quarantined directory owner without
// requiring its application payload to be replayable. Reset still needs this pin.
func ReadReplicaPin(ctx context.Context, dir, group string) (storage.Identity, error) {
	pin, err := readOfflinePin(ctx, dir, group)
	return pin.Identity, err
}

// QuarantineGroup parses only the canonical library token; it proves no ownership.
func QuarantineGroup(name string) (string, bool) {
	parsed, ok := retirementToken(name)
	return parsed.GroupID, ok
}

// ReadCollectionEvidence bounds and validates one exact library marker binding.
func ReadCollectionEvidence(ctx context.Context, root, name string) (storage.RetiredReplica, error) {
	if filepath.Base(name) != name || !strings.HasSuffix(name, collectionSuffix) {
		return storage.RetiredReplica{}, fault.ErrInvalid
	}
	token := strings.TrimSuffix(name, collectionSuffix)
	parsed, ok := retirementToken(token)
	if !ok {
		return storage.RetiredReplica{}, fault.ErrInvalid
	}
	data, err := readBoundedFile(ctx, filepath.Join(root, name), model.HeaderMaxBytes)
	if err != nil {
		return storage.RetiredReplica{}, err
	}
	var marker collectionMarker
	if err := model.DecodeStrict(data, &marker, model.HeaderMaxBytes); err != nil {
		return storage.RetiredReplica{}, err
	}
	if marker.SchemaVersion != 1 {
		return storage.RetiredReplica{}, fault.ErrUnsupported
	}
	r := marker.Retired
	if r.Token != token || r.Identity.GroupID != parsed.GroupID || !r.QuarantinedAt.Equal(parsed.At) {
		return storage.RetiredReplica{}, fault.ErrIdentityMismatch
	}
	return r, nil
}
