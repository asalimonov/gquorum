//go:build !functional

package recoveryinspect

import (
	"math"
	"strings"
	"testing"

	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/topology"
)

func TestDerivedIdentityBindsSourceEvidenceAndEffectiveID(t *testing.T) {
	t.Parallel()
	source := topology.Identity{NodeName: "n1", ClusterID: "old", MemberID: 7}
	fingerprint := strings.Repeat("a", 64)
	first, err := DeriveIdentity(source, "accepted", fingerprint, "", 23)
	if err != nil {
		t.Fatal(err)
	}
	again, err := DeriveIdentity(source, "accepted", fingerprint, "", 23)
	if err != nil || again != first || first.ClusterID == source.ClusterID || first.MemberID != 23 || first.InstanceID != "" {
		t.Fatal(first, again, err)
	}
	for _, input := range []struct{ id, fingerprint, override string }{
		{"different", fingerprint, ""},
		{"accepted", strings.Repeat("b", 64), ""},
		{"accepted", fingerprint, "named-recovered"},
	} {
		next, err := DeriveIdentity(source, input.id, input.fingerprint, input.override, 23)
		if err != nil || next.ClusterID == first.ClusterID {
			t.Fatal(next, err)
		}
	}
	for _, floor := range []uint64{0, 7, math.MaxUint64} {
		if _, err := DeriveIdentity(source, "accepted", fingerprint, "", floor); err == nil {
			t.Fatal("unsafe floor", floor)
		}
	}
	if _, err := DeriveIdentity(source, "accepted", fingerprint, source.ClusterID, 23); err == nil {
		t.Fatal("old lineage reused")
	}
	if _, err := DeriveIdentity(source, "accepted", "not-a-digest", "", 23); err == nil {
		t.Fatal("invalid evidence digest")
	}
	if _, err := DeriveIdentity(source, "accepted", fingerprint, "\x00", 23); err == nil {
		t.Fatal("invalid destination")
	}
}

func TestEffectiveInspectionRequestOwnsGeneratedIdentityAndAssertions(t *testing.T) {
	t.Parallel()
	input := model.Request{Mode: model.Rebuild, Actor: "operator", Comment: "majority permanently lost", RequiredGroups: []string{"work"}}
	first, err := EffectiveRequest(input)
	if err != nil || model.ValidateRecoveryID(first.RecoveryID) != nil {
		t.Fatal(first, err)
	}
	second, err := EffectiveRequest(input)
	if err != nil || second.RecoveryID == first.RecoveryID {
		t.Fatal(second, err)
	}
	first.RequiredGroups[0] = "changed"
	if input.RequiredGroups[0] != "work" || input.RecoveryID != "" {
		t.Fatal("request mutated")
	}
	input.RecoveryID = "explicit"
	explicit, err := EffectiveRequest(input)
	if err != nil || explicit.RecoveryID != input.RecoveryID {
		t.Fatal(explicit, err)
	}
}
