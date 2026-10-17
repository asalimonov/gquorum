//go:build !functional

package metaengine

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
)

func TestScopedCleanupCourierPreflightReusesHistoricalEndpoint(t *testing.T) {
	t.Parallel()
	s, p := collectionReadmissionFixture(t)
	if err := queryScopedCleanupCourier(&s, p.Receipt.Identity, p.Receipt.MemberID, p.Courier); err != nil {
		t.Fatal("historical journal endpoint refused", err)
	}
	for _, member := range []uint64{1, 2} {
		old := p.Courier
		old.MemberID = member
		if err := queryScopedCleanupCourier(&s, p.Receipt.Identity, p.Receipt.MemberID, old); !errors.Is(err, fault.ErrAuthorityFenced) {
			t.Fatal("preflight accepted superseded courier", err)
		}
	}
	if err := queryScopedCleanupCourier(&s, p.Receipt.Identity, p.Receipt.MemberID+1, p.Courier); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("preflight rebound old receipt", err)
	}
	unknown := p.Receipt.Identity
	unknown.ReplicaID++
	if err := queryScopedCleanupCourier(&s, unknown, p.Receipt.MemberID, p.Courier); !errors.Is(err, fault.ErrHistoryUnavailable) {
		t.Fatal("missing debt did not permit safe-read recheck", err)
	}
	if err := mustMachine(t, testRules(), nil).ValidateCleanupCourier(p.Receipt.Identity, p.Receipt.MemberID, p.Courier); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("preflight did not reject an unbound courier", err)
	}
}
