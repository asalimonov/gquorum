//go:build functional

package metaservice

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/load"
)

func TestIndexedLoadPeerRechecksCurrentProcessAndApprovedReplica(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	f.g.started, f.g.ctx = true, t.Context()
	f.command(t, metaengine.GrantSession{SessionID: "first", NodeName: "n2", InstanceID: "process-1", TTLSeconds: 30})
	f.command(t, metaengine.AcquireLock{Key: "node/n2", SessionID: "first"})
	id, found := f.g.machine.ProcessIdentity("n2")
	if !found {
		t.Fatal("missing current process")
	}
	peer := load.Peer{Node: id, Replica: load.ReplicaRef{GroupID: "work", ReplicaID: 2}}
	if err := f.g.ValidateLoadPeer(t.Context(), peer); err != nil {
		t.Fatal(err)
	}
	desired := f.g.machine.Latest().Clone()
	desired.Group("work").Replicas[1].ReplicaID = 4
	desired.Group("work").NextReplicaID = 5
	f.commit(t, desired, false)
	staged := peer
	staged.Replica.ReplicaID = 4
	if err := f.g.ValidateLoadPeer(t.Context(), staged); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("staged peer authorized", err)
	}
	if err := f.g.ValidateLoadPeer(t.Context(), peer); err != nil {
		t.Fatal("staging revoked current peer", err)
	}
	f.command(t, metaengine.RevokeSession{SessionID: "first"})
	if err := f.g.ValidateLoadPeer(t.Context(), peer); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("revoked process authorized", err)
	}
	f.command(t, metaengine.GrantSession{SessionID: "second", NodeName: "n2", InstanceID: "process-2", TTLSeconds: 30})
	f.command(t, metaengine.AcquireLock{Key: "node/n2", SessionID: "second"})
	if err := f.g.ValidateLoadPeer(t.Context(), peer); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("old incarnation authorized", err)
	}
	peer.Node.InstanceID = "process-2"
	if err := f.g.ValidateLoadPeer(t.Context(), peer); err != nil {
		t.Fatal("new incarnation cannot route", err)
	}
	f.g.quiescing = true
	if err := f.g.ValidateLoadPeer(t.Context(), peer); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("quiesced service authorized", err)
	}
}
