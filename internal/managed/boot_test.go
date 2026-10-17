//go:build !functional

package managed

import (
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
	"testing"
)

func TestLoadBootCurrentFormatMatrix(t *testing.T) {
	t.Parallel()
	id := storage.Identity{NodeName: "a", ClusterID: "cluster", GroupID: "work", ReplicaID: 7}
	good := storage.Probe{Exists: true, HasLog: true, Identity: id}
	foreign := good
	foreign.Identity.ReplicaID = 99
	empty := good
	empty.HasLog = false
	cases := []struct {
		name     string
		record   uint64
		probe    storage.Probe
		scope    metaengine.LoadScope
		origin   string
		reason   ParkReason
		previous uint64
	}{
		{name: "fresh", origin: "fresh"},
		{name: "missing known store", record: 7, reason: ParkStoreMissing},
		{name: "fresh reseat", record: 3, origin: "reseat", previous: 3},
		{name: "adopt matching store", probe: good, origin: "adopt"},
		{name: "unrecorded foreign store retained", probe: foreign, reason: ParkIdentityMismatch},
		{name: "resume", record: 7, probe: good, origin: "resume"},
		{name: "replacement landed before ledger", record: 3, probe: good, origin: "reseat", previous: 3},
		{name: "recorded foreign store retained", record: 7, probe: foreign, reason: ParkIdentityMismatch},
		{name: "prepared empty learner resumes", record: 7, probe: empty, scope: metaengine.LoadScope{Started: true}, origin: "resume"},
		{name: "empty learner adoption", probe: empty, scope: metaengine.LoadScope{Started: true}, origin: "adopt"},
		{name: "active proof survives lost ledger", scope: metaengine.LoadScope{Started: true, Established: true}, reason: ParkStoreMissing},
		{name: "active proof refuses empty WAL", probe: empty, scope: metaengine.LoadScope{Started: true, Established: true}, reason: ParkStoreMissing},
		{name: "replacement already started then lost", record: 3, scope: metaengine.LoadScope{Started: true}, reason: ParkStoreMissing},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			evidence := bootEvidence{record: metastore.ReplicaRecord{ReplicaID: tc.record}, recorded: tc.record != 0,
				probe: tc.probe, scope: tc.scope}
			got, reason := decideLoadBoot(id, evidence)
			if reason != tc.reason || reason == "" && (got.origin != tc.origin || got.previous != tc.previous) {
				t.Fatal(got, reason)
			}
		})
	}
}

func TestReplicaFoundingSeedUsesImmutableGrantTargets(t *testing.T) {
	t.Parallel()
	scope := metaengine.LoadScope{Founding: true, Group: topology.Group{ID: "work", Replicas: []topology.Replica{{NodeName: "later", ReplicaID: 99, Role: topology.ReplicaVoter}}}}
	grant := &topology.AuthorityGrant{Targets: []topology.AuthorityTarget{{NodeName: "a", MemberID: 1, ReplicaID: 2}, {NodeName: "b", MemberID: 2, ReplicaID: 7}}}
	seed := replicaMembership(scope, storage.Probe{}, grant)
	if seed.mode != raftengine.Founding || len(seed.peers) != 2 || seed.peers[0].ID != 2 || seed.peers[1].ID != 7 {
		t.Fatal("seed came from newer approved declaration", seed)
	}
	retained := replicaMembership(scope, storage.Probe{HasLog: true}, grant)
	if retained.mode != raftengine.Joining || len(retained.peers) != 0 {
		t.Fatal("retained log received founding seed", retained)
	}
}
