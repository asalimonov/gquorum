//go:build !functional

package metaservice

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestCollectionAcceptanceRechecksOnlyMissingEvidence(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name                 string
		courierErr, applyErr error
		absent, pruned       bool
		checks, proposals    int
		settled              bool
		want                 error
	}{
		{name: "already untracked", absent: true, checks: 1, settled: true},
		{name: "accepted", checks: 1, proposals: 1},
		{name: "courier pruned", courierErr: fault.ErrHistoryUnavailable, pruned: true, checks: 2, settled: true},
		{name: "proposal pruned", applyErr: fault.ErrHistoryUnavailable, pruned: true, checks: 2, proposals: 1, settled: true},
		{name: "still referenced", applyErr: fault.ErrHistoryUnavailable, checks: 2, proposals: 1, want: fault.ErrHistoryUnavailable},
		{name: "wrong courier", courierErr: fault.ErrIdentityMismatch, pruned: true, checks: 1, want: fault.ErrIdentityMismatch},
		{name: "fenced courier", courierErr: fault.ErrAuthorityFenced, pruned: true, checks: 1, want: fault.ErrAuthorityFenced},
		{name: "pending retirement", applyErr: fault.ErrUnavailable, pruned: true, checks: 1, proposals: 1, want: fault.ErrUnavailable},
		{name: "conflicting receipt", applyErr: fault.ErrIdentityMismatch, pruned: true, checks: 1, proposals: 1, want: fault.ErrIdentityMismatch},
		{name: "invalid authority", applyErr: fault.ErrOperationAuthorization, pruned: true, checks: 1, proposals: 1, want: fault.ErrOperationAuthorization},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := metaengine.RecordCollectionProposal{Receipt: collectionWireReceipt(), Courier: topology.Identity{
				ClusterID: "cluster", NodeName: "n1", MemberID: 1, InstanceID: "restart"}}
			checks, proposals, couriers := 0, 0, 0
			a := collectionReportAccess{
				check: func(_ context.Context, id storage.Identity) (metaengine.CollectionCheck, error) {
					checks++
					if id != p.Receipt.Identity {
						t.Fatal("check changed exact identity")
					}
					return metaengine.CollectionCheck{AppliedIndex: uint64(100 + checks), Untracked: tc.absent || checks == 2 && tc.pruned}, nil
				},
				courier: func(id storage.Identity, member uint64, courier topology.Identity) error {
					couriers++
					if id != p.Receipt.Identity || member != p.Receipt.MemberID || courier != p.Courier {
						t.Fatal("historical reporter binding changed")
					}
					return tc.courierErr
				},
				propose: func(_ context.Context, value any) (metaengine.ApplyResult, error) {
					proposals++
					got := value.(metaengine.RecordCollectionProposal)
					if !got.Receipt.Equal(p.Receipt) || got.Courier != p.Courier {
						t.Fatal("proposal changed original facts")
					}
					return metaengine.ApplyResult{Accepted: true, Collection: &authoritystore.CollectionResult{
						Receipt: got.Receipt, AppliedIndex: 500}}, tc.applyErr
				},
			}
			result, err := a.accept(t.Context(), p)
			if !errors.Is(err, tc.want) || checks != tc.checks || proposals != tc.proposals || tc.absent && couriers != 0 {
				t.Fatal("wrong proof/recheck/append path", err, checks, proposals, couriers)
			}
			if err == nil {
				index := uint64(500)
				if tc.settled {
					index = uint64(100 + checks)
				}
				if result.Settled != tc.settled || result.AppliedIndex != index || !result.Receipt.Equal(p.Receipt) {
					t.Fatal("echo or safe/applied index was fabricated", result)
				}
			}
		})
	}
}
