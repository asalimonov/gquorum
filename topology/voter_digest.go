package topology

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"slices"
)

// VoterSetDigest hashes canonical numeric voter IDs. The approved grant
// supplies
// the exact group identity; this digest binds only one group's voter set.
func VoterSetDigest(ids []uint64) string {
	canonical := slices.Clone(ids)
	slices.Sort(canonical)
	canonical = slices.Compact(canonical)
	hash := sha256.New()
	_, _ = hash.Write([]byte("gquorum-voter-set/v1\x00"))
	var encoded [8]byte
	for _, id := range canonical {
		binary.BigEndian.PutUint64(encoded[:], id)
		_, _ = hash.Write(encoded[:])
	}
	return hex.EncodeToString(hash.Sum(nil))
}
