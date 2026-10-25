package inspect

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/topology"
)

// EffectiveRequest generates an absent inspection ID and owns mutable assertions.
func EffectiveRequest(input model.Request) (model.Request, error) {
	if err := model.ValidateRequest(input); err != nil {
		return model.Request{}, err
	}
	request := input
	request.RequiredGroups = append([]string(nil), input.RequiredGroups...)
	if request.RecoveryID == "" {
		var entropy [16]byte
		if _, err := rand.Read(entropy[:]); err != nil {
			return model.Request{}, err
		}
		request.RecoveryID = "rcv-" + hex.EncodeToString(entropy[:])
	}
	return request, nil
}

// DeriveIdentity chooses a fresh lineage and the first never-allocated meta ID.
//
//revive:disable-next-line:argument-limit The approved internal contract binds all identity inputs explicitly.
func DeriveIdentity(source topology.Identity, recoveryID, fingerprint, override string,
	metaFloor uint64) (topology.Identity, error) {
	if source.InstanceID != "" || source.MemberID == 0 || topology.ValidateID(source.NodeName) != nil ||
		!validLineage(source.ClusterID) || model.ValidateRecoveryID(recoveryID) != nil {
		return topology.Identity{}, fault.ErrIdentityMismatch
	}
	decoded, err := hex.DecodeString(fingerprint)
	if err != nil || len(decoded) != sha256.Size || fingerprint != strings.ToLower(fingerprint) {
		return topology.Identity{}, fault.ErrInvalid
	}
	if metaFloor <= source.MemberID || metaFloor == math.MaxUint64 {
		return topology.Identity{}, fault.New(fault.Invalid,
			"recovery identity floor is exhausted or contradictory", nil)
	}
	cluster := override
	if cluster == "" {
		data, err := model.EncodeCanonical(struct {
			Source                  topology.Identity
			RecoveryID, Fingerprint string
		}{source, recoveryID, fingerprint})
		if err != nil {
			return topology.Identity{}, err
		}
		sum := sha256.Sum256(data)
		cluster = "rcv-" + hex.EncodeToString(sum[:])
	}
	if cluster == source.ClusterID || !validLineage(cluster) {
		return topology.Identity{}, fault.New(fault.IdentityMismatch,
			"recovery requires a fresh cluster lineage", nil)
	}
	return topology.Identity{NodeName: source.NodeName, ClusterID: cluster, MemberID: metaFloor}, nil
}

func validLineage(value string) bool {
	return strings.TrimSpace(value) != "" && !strings.ContainsRune(value, 0) && utf8.ValidString(value)
}
