//go:build functional

package managed

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type grantedLoopback struct{ loopbackTransport }

func (t *grantedLoopback) ReadReplica(ctx context.Context, request load.ReplicaQuery) (load.ReplicaStatus, error) {
	return t.host.ReadReplica(ctx, request)
}

func configureGrantDispatch(f *fixture) {
	f.cfg.ResolveAuthorityPeer = func(ctx context.Context, grant topology.AuthorityGrant, target topology.AuthorityTarget) (load.Peer, error) {
		return f.cfg.ResolvePeer(ctx, load.ReplicaRef{GroupID: grant.Key.GroupID, ReplicaID: target.ReplicaID})
	}
	f.cfg.RecordFence = func(ctx context.Context, _ topology.FenceEvidence) error { return ctx.Err() }
}

func TestGrantedDispatcherCollectsDurableFoundingAndMembershipReceipts(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	s, founding := fixtureAuthority(t, f)
	configureGrantDispatch(f)
	var factories atomic.Int32
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		factories.Add(1)
		return load.Application{StateMachine: &stateMachine{}}, nil
	}
	transport := &grantedLoopback{}
	f.cfg.Transport = transport
	h := f.host(t)
	transport.host = h
	var receipts []topology.EffectReceipt
	dispatchCtx := deadline(t)
	await(dispatchCtx, t, func() bool {
		var err error
		receipts, err = h.DispatchGrant(dispatchCtx, founding)
		// Each dispatch has its own EffectTimeout. Initial election and durable
		// startup can outlast one attempt under the race detector; retry the
		// same immutable grant within this test's overall bounded wait.
		if errors.Is(err, context.DeadlineExceeded) && dispatchCtx.Err() == nil {
			return false
		}
		if errors.Is(err, fault.ErrReplicaNotReady) || errors.Is(err, fault.ErrUnavailable) || errors.Is(err, fault.ErrAuthorityPending) || errors.Is(err, fault.ErrNotLeader) {
			return false
		}
		must(t, err)
		return true
	})
	if len(receipts) != 2 || factories.Load() != 1 || receipts[0].Resource.Kind != topology.AuthorityGroup || receipts[0].AppliedIndex == 0 || receipts[1].LocalSequence == 0 {
		t.Fatal(receipts, factories.Load())
	}
	for _, receipt := range receipts {
		must(t, receipt.Validate(founding))
	}
	if proof, err := s.Receipt(t.Context(), founding, founding.Targets[0]); err != nil || !proof.Found || !reflect.DeepEqual(proof.Receipt, receipts[1]) {
		t.Fatal(proof.Receipt, proof.Found, err)
	}
	var again []topology.EffectReceipt
	await(deadline(t), t, func() bool {
		var err error
		again, err = h.DispatchGrant(deadline(t), founding)
		if errors.Is(err, fault.ErrAuthorityPending) {
			return false
		}
		must(t, err)
		return true
	})
	if !reflect.DeepEqual(receipts, again) || factories.Load() != 1 {
		t.Fatal("founding retry lost exact durable evidence", receipts, again)
	}
	membership := founding.Clone()
	membership.Key.Revision++
	a := f.approve(topology.StepAddLearner, storage.Identity{ClusterID: "cluster", GroupID: "work", NodeName: "b", ReplicaID: 2})
	membership.Action, membership.Resources = &a, membership.Resources[:1]
	membership.Targets = append(membership.Targets, topology.AuthorityTarget{NodeName: "b", MemberID: 2, ReplicaID: 2})
	await(deadline(t), t, func() bool {
		var err error
		receipts, err = h.DispatchGrant(deadline(t), membership)
		if errors.Is(err, fault.ErrAuthorityPending) {
			return false
		}
		must(t, err)
		return true
	})
	if len(receipts) != 1 || receipts[0].Resource.Kind != topology.AuthorityGroup {
		t.Fatal(receipts)
	}
	must(t, receipts[0].Validate(membership))
	await(deadline(t), t, func() bool {
		_, err := h.DispatchGrant(deadline(t), founding)
		if errors.Is(err, fault.ErrAuthorityPending) {
			return false
		}
		if !errors.Is(err, fault.ErrAuthorityFenced) {
			t.Fatal("old creation receipt crossed the newer group floor", err)
		}
		return true
	})
}

type grantedProtocolTransport struct {
	discardTransport
	fence  func(context.Context, load.FenceRequest) (load.FenceResult, error)
	action func(context.Context, load.ActionRequest) (load.ActionResult, error)
}

func (t grantedProtocolTransport) InstallFence(ctx context.Context, request load.FenceRequest) (load.FenceResult, error) {
	return t.fence(ctx, request)
}
func (t grantedProtocolTransport) ApplyAction(ctx context.Context, request load.ActionRequest) (load.ActionResult, error) {
	return t.action(ctx, request)
}

func hostFenceReply(request load.FenceRequest) load.FenceResult {
	return load.FenceResult{From: request.To, Evidence: topology.FenceEvidence{Key: request.Grant.Key, Resource: request.Resource, Receiver: request.To, LocalSequence: 1}}
}
func hostActionReply(request load.ActionRequest) load.ActionResult {
	return grantedResult(request, topology.EffectReceipt{Key: request.Grant.Key, Resource: topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: request.Grant.Key.GroupID, NodeName: request.To.Node.NodeName}, Action: request.Grant.Action, Target: actionTarget(request), Outcome: topology.EffectApplied, LocalSequence: 1})
}

func TestGrantedDispatcherPreservesPartialReceiptsAcrossIndependentHosts(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	_, grant := fixtureAuthority(t, f)
	configureGrantDispatch(f)
	grant.Resources = append(grant.Resources, topology.AuthorityResource{Kind: topology.AuthorityHost, GroupID: "work", NodeName: "b"})
	grant.Targets = append(grant.Targets, topology.AuthorityTarget{NodeName: "b", MemberID: 2, ReplicaID: 2})
	entered := make(chan uint64, 2)
	proceed := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(proceed) }) })
	f.cfg.Transport = grantedProtocolTransport{
		fence: func(_ context.Context, req load.FenceRequest) (load.FenceResult, error) {
			return hostFenceReply(req), nil
		},
		action: func(ctx context.Context, req load.ActionRequest) (load.ActionResult, error) {
			entered <- req.To.Replica.ReplicaID
			select {
			case <-proceed:
			case <-ctx.Done():
				return load.ActionResult{}, ctx.Err()
			}
			if req.To.Replica.ReplicaID == 2 {
				return load.ActionResult{}, fault.ErrUnavailable
			}
			return hostActionReply(req), nil
		},
	}
	h := f.host(t)
	result := make(chan []topology.EffectReceipt, 1)
	failed := make(chan error, 1)
	go func() { got, err := h.DispatchGrant(deadline(t), grant); result <- got; failed <- err }()
	for range 2 {
		select {
		case <-entered:
		case <-deadline(t).Done():
			t.Fatal("one host blocked another host's founding")
		}
	}
	once.Do(func() { close(proceed) })
	if err := <-failed; !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal(err)
	}
	if got := <-result; len(got) != 1 || got[0].Target.ReplicaID != 1 {
		t.Fatal("successful host evidence was discarded", got)
	}
}

func TestGrantedDispatcherRejectsIncompleteOrMisdirectedEvidence(t *testing.T) {
	for _, mode := range []string{"missing", "other_receipt", "other_process", "other_action", "other_local_target", "wrong_fence", "fence_not_recorded", "successor_member"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			_, grant := fixtureAuthority(t, f)
			configureGrantDispatch(f)
			a := f.approve(topology.StepPrepareReplica, f.localID(1))
			grant.Action, grant.Resources = &a, grant.Resources[1:]
			var actions atomic.Int32
			f.cfg.Transport = grantedProtocolTransport{
				fence: func(_ context.Context, req load.FenceRequest) (load.FenceResult, error) {
					result := hostFenceReply(req)
					if mode == "wrong_fence" {
						result.Evidence.Receiver.InstanceID = "old"
					}
					return result, nil
				},
				action: func(_ context.Context, req load.ActionRequest) (load.ActionResult, error) {
					actions.Add(1)
					result := hostActionReply(req)
					switch mode {
					case "missing":
						result.Receipts = nil
					case "other_receipt":
						result.Receipts[0].Key.Revision++
					case "other_process":
						result.From.Node.InstanceID = "old"
					case "other_action":
						result.Authorization.Attempt++
					case "other_local_target":
						result.LocalTarget.ReplicaID++
					}
					return result, nil
				},
			}
			want := fault.ErrOperatorAuthorization
			if mode == "fence_not_recorded" {
				f.cfg.RecordFence = func(context.Context, topology.FenceEvidence) error { return fault.ErrUnavailable }
				want = fault.ErrUnavailable
			}
			if mode == "successor_member" {
				f.cfg.ResolveAuthorityPeer = func(context.Context, topology.AuthorityGrant, topology.AuthorityTarget) (load.Peer, error) {
					id := f.node
					id.MemberID++
					return load.Peer{Node: id, Replica: load.ReplicaRef{GroupID: "work", ReplicaID: 1}}, nil
				}
				want = fault.ErrIdentityMismatch
			}
			h := f.host(t)
			if got, err := h.DispatchGrant(deadline(t), grant); !errors.Is(err, want) || len(got) != 0 {
				t.Fatal("incomplete evidence advanced the effect", got, err)
			}
			if (mode == "wrong_fence" || mode == "fence_not_recorded" || mode == "successor_member") && actions.Load() != 0 {
				t.Fatal("action ran without its exact recorded fence")
			}
		})
	}
}

func TestGrantedDispatcherPreservesIdentityRefusalAndRejectsClosedGrant(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	_, grant := fixtureAuthority(t, f)
	configureGrantDispatch(f)
	target := topology.AuthorityTarget{NodeName: "b", MemberID: 2, ReplicaID: 2}
	a := f.approve(topology.StepPrepareReplica, storage.Identity{NodeName: "b", ClusterID: "cluster", GroupID: "work", ReplicaID: 2})
	grant.Action, grant.Targets = &a, []topology.AuthorityTarget{target}
	grant.Resources = []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: "work", NodeName: "b"}}
	var actions atomic.Int32
	var revoked atomic.Bool
	f.cfg.ValidateGrant = func(ctx context.Context, candidate topology.AuthorityGrant) error {
		if revoked.Load() {
			return fault.ErrAuthorityFenced
		}
		if !candidate.Equal(grant) {
			return fault.ErrOperatorAuthorization
		}
		return ctx.Err()
	}
	f.cfg.Transport = grantedProtocolTransport{
		fence: func(_ context.Context, req load.FenceRequest) (load.FenceResult, error) {
			return hostFenceReply(req), nil
		},
		action: func(context.Context, load.ActionRequest) (load.ActionResult, error) {
			actions.Add(1)
			return load.ActionResult{}, fault.ErrIdentityMismatch
		},
	}
	h := f.host(t)
	if receipts, err := h.DispatchGrant(deadline(t), grant); !errors.Is(err, fault.ErrIdentityMismatch) || len(receipts) != 0 {
		t.Fatal(receipts, err)
	}
	revoked.Store(true)
	if receipts, err := h.DispatchGrant(deadline(t), grant); !errors.Is(err, fault.ErrAuthorityFenced) || len(receipts) != 0 {
		t.Fatal(receipts, err)
	}
	if actions.Load() != 1 {
		t.Fatal("closed grant reached transport", actions.Load())
	}
}
