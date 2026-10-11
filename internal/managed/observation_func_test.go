//go:build functional

package managed

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type observationTransport struct {
	discardTransport
	read func(context.Context, load.ReplicaQuery) (load.ReplicaStatus, error)
}

func (t observationTransport) ReadReplica(ctx context.Context, q load.ReplicaQuery) (load.ReplicaStatus, error) {
	return t.read(ctx, q)
}

func TestObservationRefusalDoesNotAttestAReplicaButMalformedSuccessFails(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"stale_process", "unreadable", "wrong_process", "wrong_group", "false_found", "valid"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			f.cfg.Active = func(context.Context) (topology.Document, error) {
				return topology.Document{Groups: []topology.Group{{ID: "work"}}}, nil
			}
			f.cfg.Catalog = func(context.Context, string) ([]storage.Identity, error) {
				return []storage.Identity{{NodeName: "b", ClusterID: "cluster", GroupID: "work", ReplicaID: 2}}, nil
			}
			f.cfg.Transport = observationTransport{read: func(_ context.Context, q load.ReplicaQuery) (load.ReplicaStatus, error) {
				if mode == "stale_process" {
					return load.ReplicaStatus{}, fault.ErrIdentityMismatch
				}
				if mode == "unreadable" {
					return load.ReplicaStatus{}, fault.ErrUnreadable
				}
				result := load.ReplicaStatus{Peer: q.To, Found: true, Status: topology.GroupStatus{GroupID: "work", HostNode: "b", Found: true, LeaderID: 2, Term: 1}}
				if mode == "wrong_process" {
					result.Peer.Node.InstanceID = "other"
				}
				if mode == "wrong_group" {
					result.Status.GroupID = "other"
				}
				if mode == "false_found" {
					result.Found = false
				}
				return result, nil
			}}
			h := f.host(t)
			status, err := h.ObserveGroup(t.Context(), "work")
			switch mode {
			case "stale_process", "unreadable":
				if !errors.Is(err, fault.ErrUnavailable) || status.LiveVoters[2] {
					t.Fatal("refusal attested a peer or became permanent failure", status, err)
				}
			case "valid":
				if err != nil || !status.HealthKnown || !status.LiveVoters[2] || status.LeaderID != 2 {
					t.Fatal(status, err)
				}
			default:
				if !errors.Is(err, fault.ErrIdentityMismatch) {
					t.Fatal("malformed success accepted", status, err)
				}
			}
		})
	}
}
