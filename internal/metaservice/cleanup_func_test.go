//go:build functional

package metaservice

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/managed"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type cleanupProtocolHandler struct {
	*Group
	call func(context.Context, *metapb.Header, []byte) *metapb.Reply
}

func (h cleanupProtocolHandler) Call(ctx context.Context, method string, header *metapb.Header, data []byte) *metapb.Reply {
	if method != "MetaControl" {
		return metatransport.Reply(fault.ErrUnsupported)
	}
	return h.call(ctx, header, data)
}

// One authenticated caller and protocol doubles isolate forwarding from multi-
// node consensus. Scoped FSM acceptance/lineage tests live in metaengine; the
// actual safe read and old-format refusal are exercised by their owning suites.
func collectionProtocolClient(t *testing.T, seeds []string) *Group {
	t.Helper()
	client := joinProtocolClient(t, seeds, func(o *Options) {
		o.Network.ClusterCredential = []byte("collection-test")
		retained := o.Identity
		retained.MemberID = 4
		o.ReadmissionIdentity = func() topology.Identity { return retained }
	})
	client.mu.Lock()
	client.started, client.controlOnly = true, true
	client.ctx, client.cancel = context.WithCancel(t.Context())
	client.workCtx, client.workCancel = context.WithCancel(client.ctx)
	client.mu.Unlock()
	return client
}

func TestCollectionProtocolForwardsHistoricalJournalWithZeroParticipationHeader(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"accepted", "settled", "status"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			var reads, proposals, couriers atomic.Int32
			a := collectionReportAccess{
				check: func(context.Context, storage.Identity) (metaengine.CollectionCheck, error) {
					reads.Add(1)
					return metaengine.CollectionCheck{Untracked: mode == "settled", AppliedIndex: 42}, nil
				},
				courier: func(id storage.Identity, member uint64, courier topology.Identity) error {
					couriers.Add(1)
					if courier.MemberID != 4 || courier.ClusterID != id.ClusterID || courier.NodeName != id.NodeName ||
						courier.InstanceID == "" || member != 1 {
						return fault.ErrIdentityMismatch
					}
					return nil
				},
				propose: func(_ context.Context, proposal any) (metaengine.ApplyResult, error) {
					proposals.Add(1)
					switch p := proposal.(type) {
					case metaengine.RecordCollectionProposal:
						if p.At.IsZero() || p.ProposalID == "" {
							return metaengine.ApplyResult{}, fault.ErrInvalid
						}
						return metaengine.ApplyResult{Accepted: true, Collection: &authoritystore.CollectionResult{
							Receipt: p.Receipt, AppliedIndex: 43}}, nil
					case metaengine.RecordCleanupStatusProposal:
						if p.At.IsZero() || p.ProposalID == "" || p.Blocked == nil || p.Blocked.Code != "retention" {
							return metaengine.ApplyResult{}, fault.ErrInvalid
						}
						return metaengine.ApplyResult{Accepted: true}, nil
					default:
						return metaengine.ApplyResult{}, fault.ErrInvalid
					}
				},
			}
			leader := joinProtocolServer(t, cleanupProtocolHandler{call: func(ctx context.Context, header *metapb.Header, data []byte) *metapb.Reply {
				if header.MemberId != 0 {
					return metatransport.Reply(fault.ErrIdentityMismatch)
				}
				if mode == "status" {
					return metatransport.Reply(a.receiveStatus(ctx, header, data))
				}
				result, err := a.receiveCollection(ctx, header, data)
				if err != nil {
					return metatransport.Reply(err)
				}
				encoded, _ := json.Marshal(result)
				return &metapb.Reply{Data: encoded}
			}}, []byte("collection-test"))
			follower := joinProtocolServer(t, cleanupProtocolHandler{call: func(context.Context, *metapb.Header, []byte) *metapb.Reply {
				reply := metatransport.Reply(fault.ErrNotLeader)
				reply.LeaderAddress = leader
				return reply
			}}, []byte("collection-test"))
			client := collectionProtocolClient(t, []string{follower})
			receipt := collectionWireReceipt()
			if mode == "status" {
				future := time.Now().Add(time.Hour).UTC()
				err := client.ReportCleanupStatus(t.Context(), managed.CleanupStatus{Identity: receipt.Identity,
					MemberID: receipt.MemberID, RetirementKey: receipt.RetirementKey, NotBefore: &future,
					Blocked: &topology.BlockedReason{Code: "retention"}})
				if err != nil {
					t.Fatal(err)
				}
			} else {
				result, err := client.ReportCollection(t.Context(), receipt)
				if err != nil || !result.Receipt.Equal(receipt) || result.Settled != (mode == "settled") ||
					mode == "settled" && result.AppliedIndex != 42 || mode == "accepted" && result.AppliedIndex != 43 {
					t.Fatal("forwarding lost exact original receipt or index", result, err)
				}
			}
			want := int32(1)
			if mode == "settled" {
				want = 0
			}
			if reads.Load() != 1 || proposals.Load() != want || couriers.Load() != want {
				t.Fatal("settlement appended or looked up pruned proof", reads.Load(), proposals.Load(), couriers.Load())
			}
		})
	}
}

func TestCollectionProtocolDoesNotRoutePastAuthenticationOrProofRefusal(t *testing.T) {
	t.Parallel()
	for _, refusal := range []error{fault.ErrIdentityMismatch, fault.ErrAuthorityFenced, fault.ErrOperationAuthorization} {
		t.Run(refusal.Error(), func(t *testing.T) {
			t.Parallel()
			var second atomic.Int32
			first := joinProtocolServer(t, cleanupProtocolHandler{call: func(context.Context, *metapb.Header, []byte) *metapb.Reply {
				return metatransport.Reply(refusal)
			}}, []byte("collection-test"))
			next := joinProtocolServer(t, cleanupProtocolHandler{call: func(context.Context, *metapb.Header, []byte) *metapb.Reply {
				second.Add(1)
				return &metapb.Reply{}
			}}, []byte("collection-test"))
			client := collectionProtocolClient(t, []string{first, next})
			request, _ := json.Marshal(collectionRequest{Kind: collectionReportKind, Receipt: collectionWireReceipt(), ReporterMemberID: 4})
			_, err := client.callAuthorityRoutes(t.Context(), "MetaControl", request, []string{first, next})
			if !errors.Is(err, refusal) || second.Load() != 0 {
				t.Fatal("routing suppressed a proof/authentication refusal", err, second.Load())
			}
		})
	}
}

func TestCollectionProtocolRejectsWrongCredentialBeforeHandlerOrFallback(t *testing.T) {
	t.Parallel()
	var handlers atomic.Int32
	h := cleanupProtocolHandler{call: func(context.Context, *metapb.Header, []byte) *metapb.Reply {
		handlers.Add(1)
		return &metapb.Reply{}
	}}
	first := joinProtocolServer(t, h, []byte("different-secret"))
	next := joinProtocolServer(t, h, []byte("collection-test"))
	client := collectionProtocolClient(t, []string{first, next})
	request, _ := json.Marshal(collectionRequest{Kind: collectionReportKind, Receipt: collectionWireReceipt(), ReporterMemberID: 4})
	if _, err := client.callAuthorityRoutes(t.Context(), "MetaControl", request, []string{first, next}); err == nil || handlers.Load() != 0 {
		t.Fatal("failed credentials reached a handler or fallback", err, handlers.Load())
	}
}
