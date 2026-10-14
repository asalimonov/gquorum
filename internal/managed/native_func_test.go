//go:build functional

package managed

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

func TestNativeReadLocalIngressAndStoppedReplica(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	h := f.host(t)
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	handle, err := h.StartReplica(deadline(t), ref, StartExplicit)
	must(t, err)
	await(deadline(t), t, handle.Core().IsLeader)
	state, err := h.ReadNative(deadline(t), "work")
	must(t, err)
	if state.GroupID != "work" || state.ReplicaID != 1 || state.Applied == 0 || !reflect.DeepEqual(state.ConfState.Voters, []uint64{1}) {
		t.Fatal(state)
	}
	query := load.ReplicaQuery{From: f.node, To: load.Peer{Node: f.node, Replica: ref}}
	observation, err := h.ReadReplica(deadline(t), query)
	must(t, err)
	if !observation.Found || observation.Native != nil {
		t.Fatal("ordinary status claimed a native read", observation)
	}
	must(t, h.StopReplica(deadline(t), ref, StopExplicit))
	query.Native = true
	absent, err := h.ReadReplica(deadline(t), query)
	must(t, err)
	if absent.Found || absent.Native != nil {
		t.Fatal("stopped replica certified membership", absent)
	}
	if _, err := h.ReadNative(deadline(t), "work"); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal(err)
	}
}

func TestNativeReadRequiresExactConfirmedTransportReply(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"diagnostic_only", "absent", "wrong_peer", "wrong_replica", "wrong_group", "no_index", "expired", "ready_learner"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			if mode == "expired" {
				f.cfg.EffectTimeout = 50 * time.Millisecond
			}
			f.cfg.Catalog = func(context.Context, string) ([]storage.Identity, error) {
				return []storage.Identity{{ClusterID: "cluster", GroupID: "work", NodeName: "b", ReplicaID: 2}}, nil
			}
			f.cfg.Transport = observationTransport{read: func(ctx context.Context, q load.ReplicaQuery) (load.ReplicaStatus, error) {
				if !q.Native {
					t.Error("native read sent an observation query")
				}
				r := load.ReplicaStatus{Peer: q.To, Found: true, Native: &topology.NativeState{GroupID: "work", ReplicaID: 2, Applied: 10, ConfState: raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}}}
				switch mode {
				case "diagnostic_only":
					r.Native = nil
				case "absent":
					r.Found = false
				case "wrong_peer":
					r.Peer.Node.InstanceID = "former"
				case "wrong_replica":
					r.Native.ReplicaID = 3
				case "wrong_group":
					r.Native.GroupID = "other"
				case "no_index":
					r.Native.Applied = 0
				case "expired":
					<-ctx.Done()
				}
				return r, nil
			}}
			h := f.host(t)
			state, err := h.ReadNative(deadline(t), "work")
			if mode == "ready_learner" {
				must(t, err)
				if state.ReplicaID != 2 || state.Applied != 10 {
					t.Fatal(state)
				}
			} else if err == nil || state.Applied != 0 {
				t.Fatal("accepted unconfirmed or foreign membership", state, err)
			}
		})
	}
}

func TestNativeReadTriesNextReplicaAfterUnavailableOwner(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.cfg.EffectTimeout = 500 * time.Millisecond
	ids := []storage.Identity{{ClusterID: "cluster", GroupID: "work", NodeName: "b", ReplicaID: 2}, {ClusterID: "cluster", GroupID: "work", NodeName: "b", ReplicaID: 3}}
	f.cfg.Catalog = func(context.Context, string) ([]storage.Identity, error) { return ids, nil }
	f.cfg.Transport = observationTransport{read: func(ctx context.Context, q load.ReplicaQuery) (load.ReplicaStatus, error) {
		if q.To.Replica.ReplicaID == 2 {
			<-ctx.Done()
			return load.ReplicaStatus{}, ctx.Err()
		}
		return load.ReplicaStatus{Peer: q.To, Found: true, Native: &topology.NativeState{GroupID: "work", ReplicaID: 3, Applied: 10, ConfState: raftpb.ConfState{Voters: []uint64{2, 3}}}}, nil
	}}
	h := f.host(t)
	state, err := h.ReadNative(deadline(t), "work")
	must(t, err)
	if state.ReplicaID != 3 {
		t.Fatal(state)
	}
	if ids[0].ReplicaID != 2 || ids[1].ReplicaID != 3 {
		t.Fatal("mutated caller's catalog", ids)
	}
}

func TestNativeReadUsesRetainedCandidateWithoutRelaxingOrdinaryPeerAdmission(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.cfg.Catalog = func(context.Context, string) ([]storage.Identity, error) { return nil, nil }
	f.cfg.ValidatePeer = func(context.Context, load.Peer) error { return fault.ErrReplicaNotReady }
	h := f.host(t)
	ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
	handle, err := h.StartReplica(deadline(t), ref, StartExplicit)
	must(t, err)
	await(deadline(t), t, handle.Core().IsLeader)
	if _, err := h.ReadNative(deadline(t), "work"); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("empty current catalog unexpectedly found an owner", err)
	}
	state, err := h.ReadNative(deadline(t), "work", f.localID(1))
	must(t, err)
	if state.ReplicaID != 1 || state.Applied == 0 {
		t.Fatal(state)
	}
	query := load.ReplicaQuery{From: f.node, To: load.Peer{Node: f.node, Replica: ref}}
	if _, err := h.ReadReplica(deadline(t), query); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("native read widened ordinary admission", err)
	}
	query.Native = true
	query.To.Node.InstanceID = "former-process"
	if _, err := h.ReadReplica(deadline(t), query); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("native read accepted foreign process", err)
	}
	wrong := f.localID(1)
	wrong.GroupID = "other"
	if _, err := h.ReadNative(deadline(t), "work", wrong); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("native read crossed group scope", err)
	}
}
