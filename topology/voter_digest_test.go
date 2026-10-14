//go:build !functional

package topology

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func TestVoterSetDigestCanonicalAndDomainSeparated(t *testing.T) {
	t.Parallel()
	got := VoterSetDigest([]uint64{9, 1, 9})
	if got != VoterSetDigest([]uint64{1, 9}) || got != VoterSetDigest([]uint64{9, 1}) || len(got) != 64 || got != strings.ToLower(got) {
		t.Fatalf("digest is not canonical: %q", got)
	}
	h := sha256.New()
	_, _ = h.Write([]byte("gquorum-voter-set/v1\x00"))
	var b [8]byte
	for _, id := range []uint64{1, 9} {
		binary.BigEndian.PutUint64(b[:], id)
		_, _ = h.Write(b[:])
	}
	if got != hex.EncodeToString(h.Sum(nil)) || got == VoterSetDigest(nil) {
		t.Fatalf("digest encoding mismatch: %q", got)
	}
}

func TestPlanProofCloneEqualityAndAuthorization(t *testing.T) {
	t.Parallel()
	p := Plan{Version: 3, Steps: []Step{{Seq: 1, Kind: StepPromoteVoter, GroupID: "work", NodeName: "b", ReplicaID: 2,
		ExpectedVotersDigest: VoterSetDigest([]uint64{1}), ResultVotersDigest: VoterSetDigest([]uint64{1, 2})}},
		VoterBases: []PlanVoterBase{{GroupID: "work", Voters: []uint64{1}}}}
	copy := p.Clone()
	if !copy.Equal(p) {
		t.Fatal("cloned plan differs")
	}
	copy.VoterBases[0].Voters[0] = 7
	if p.VoterBases[0].Voters[0] != 1 || copy.Equal(p) {
		t.Fatal("plan clone shared voter base or equality ignored it")
	}
	copy = p.Clone()
	copy.Steps[0].ResultVotersDigest = VoterSetDigest([]uint64{1, 3})
	if copy.Equal(p) {
		t.Fatal("plan equality ignored step guard")
	}
	a := AuthorizedStep{OperatorID: "exec-v3-a1", Version: 3, Attempt: 1, StepSeq: 1,
		Kind: StepPromoteVoter, GroupID: "work", NodeName: "b", ReplicaID: 2,
		ExpectedVotersDigest: p.Steps[0].ExpectedVotersDigest,
		ResultVotersDigest:   p.Steps[0].ResultVotersDigest}
	if !a.Matches(a.OperatorID, a.Attempt, a.Version, p.Steps[0]) {
		t.Fatal("exact action did not match")
	}
	wrong := p.Steps[0]
	wrong.ResultVotersDigest = "forged"
	if a.Matches(a.OperatorID, a.Attempt, a.Version, wrong) {
		t.Fatal("mismatched guard matched")
	}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var restored Plan
	if err := json.Unmarshal(raw, &restored); err != nil || !restored.Equal(p) {
		t.Fatalf("plan proof did not survive encoding: %v", err)
	}
}
