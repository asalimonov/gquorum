//go:build !functional

package metaservice

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestCleanupCourierUsesOnlyHeaderProcessAndMonotonicMemberHint(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		header, hint, want uint64
	}{
		{0, 0, 0}, {0, 4, 4}, {3, 0, 3}, {3, 3, 3}, {3, 4, 4}, {4, 3, 0},
	} {
		header := &metapb.Header{ClusterId: "cluster", NodeName: "n1", InstanceId: "process", MemberId: tc.header}
		courier, err := cleanupCourier(header, tc.hint)
		if tc.want == 0 {
			if !errors.Is(err, fault.ErrIdentityMismatch) {
				t.Fatal("missing/regressing pin accepted", tc, err)
			}
		} else if err != nil || courier != (topology.Identity{ClusterID: "cluster", NodeName: "n1", InstanceID: "process", MemberID: tc.want}) {
			t.Fatal("header binding changed", tc, courier, err)
		}
	}
	for _, header := range []*metapb.Header{nil, {ClusterId: "cluster", NodeName: "n1", MemberId: 1},
		{ClusterId: "cluster", NodeName: "n1", InstanceId: "x\x00", MemberId: 1}} {
		if _, err := cleanupCourier(header, 1); !errors.Is(err, fault.ErrIdentityMismatch) {
			t.Fatal("missing/corrupt process accepted", header, err)
		}
	}
}

func TestCollectionPayloadRejectsBodyCourierAndUnrelatedAuthority(t *testing.T) {
	t.Parallel()
	request := collectionRequest{Kind: collectionReportKind, Receipt: collectionWireReceipt(), ReporterMemberID: 1}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	for _, extra := range []string{`"courier":{}`, `"action":{}`, `"proposal":{}`, `"at":"2026-01-01T00:00:00Z"`} {
		bad := append(append([]byte(nil), encoded[:len(encoded)-1]...), []byte(","+extra+"}")...)
		var got collectionRequest
		if err := decodeCleanupRequest(bad, &got); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal("body injected authentication or general authority", extra, err)
		}
	}
	var got collectionRequest
	if err := decodeCleanupRequest(append(encoded, []byte(` {}`)...), &got); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("trailing control payload accepted", err)
	}
}

func TestCollectionAndStatusCannotBypassRestrictedRouteThroughProposeMeta(t *testing.T) {
	t.Parallel()
	g := &Group{}
	for _, p := range []any{&metaengine.RecordCollectionProposal{}, &metaengine.RecordCleanupStatusProposal{}} {
		if err := g.validateProposalCourier(p, &metapb.Header{MemberId: 1}); !errors.Is(err, fault.ErrOperationAuthorization) {
			t.Fatal("general proposal route bypassed restricted authentication", err)
		}
	}
}

func TestCollectionReceiverAuthenticatesBeforeSafeAbsence(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"node", "cluster", "sequence", "process", "member", "regression"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			request := collectionRequest{Kind: collectionReportKind, Receipt: collectionWireReceipt(), ReporterMemberID: 4}
			header := &metapb.Header{ClusterId: "cluster", NodeName: "n1", MemberId: 0, InstanceId: "process"}
			switch change {
			case "node":
				request.Receipt.Identity.NodeName = "foreign"
			case "cluster":
				request.Receipt.Identity.ClusterID = "foreign"
			case "sequence":
				request.Receipt.LocalSequence = 0
			case "process":
				header.InstanceId = ""
			case "member":
				request.ReporterMemberID = 0
			case "regression":
				header.MemberId = 5
			}
			checks := 0
			a := collectionReportAccess{check: func(context.Context, storage.Identity) (metaengine.CollectionCheck, error) {
				checks++
				return metaengine.CollectionCheck{AppliedIndex: 42, Untracked: true}, nil
			}}
			data, _ := json.Marshal(request)
			result, err := a.receiveCollection(t.Context(), header, data)
			if err == nil || checks != 0 || result.Settled {
				t.Fatal("invalid authentication/shape was hidden by absence", result, err, checks)
			}
		})
	}
}
