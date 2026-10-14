//go:build functional

package managed

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

// These exact ledger assertions formerly depended on application access to the
// private meta service. Keep them at the owner boundary with real retained bbolt.
func TestDurableLedgerSurvivesParkedRestartAndExactRetirement(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	_, initial := fixtureAuthority(t, f)
	initial.Targets[0].ReplicaID = 7
	f.cfg.AuthorityFloors = func(ctx context.Context) ([]topology.AuthorityGrant, error) {
		return []topology.AuthorityGrant{initial.Clone()}, ctx.Err()
	}
	path := filepath.Join(t.TempDir(), "meta.bolt")
	db, err := metastore.Open(path)
	must(t, err)
	defer func() { must(t, db.Close()) }()
	must(t, db.EnsureIdentity(f.node.NodeName, f.node.ClusterID, f.node.MemberID))
	record := metastore.ReplicaRecord{NodeName: f.node.NodeName, ClusterID: f.node.ClusterID, ReplicaID: 7, Origin: "fresh", CreatedAt: time.Unix(100, 0).UTC()}
	must(t, db.RecordReplica("work", record))
	f.setScope("work", 7, false, true)
	f.cfg.LookupRecord = func(_ context.Context, group string) (metastore.ReplicaLookup, error) {
		return db.LookupReplica(group)
	}
	f.cfg.ForgetGrantedReplica = func(ctx context.Context, grant topology.AuthorityGrant, id uint64) error {
		if err := f.cfg.ValidateGrant(ctx, grant); err != nil {
			return err
		}
		return db.ForgetReplica(grant.Key.GroupID, id)
	}
	var host *Host
	for range 2 {
		f.cfg.Transport = discardTransport{}
		host = f.host(t)
		_, err := host.StartReplica(t.Context(), load.ReplicaRef{GroupID: "work", ReplicaID: 7}, StartExplicit)
		var parked *ParkedError
		if !errors.As(err, &parked) || parked.Reason != ParkStoreMissing {
			t.Fatal(err)
		}
		got, err := db.LookupReplica("work")
		if err != nil || !got.Found || got.Record != record {
			t.Fatal("parking rewrote full durable evidence", got.Record, got.Found, err)
		}
		must(t, host.Stop(t.Context()))
		must(t, db.Close())
		db, err = metastore.Open(path)
		must(t, err)
	}
	// A fresh owner can retire the missing old store only with exact approval.
	f.cfg.Transport = discardTransport{}
	host = f.host(t)
	old := f.localID(7)
	f.setScope("work", 8, false, false)
	request := installFixtureHostAction(t, f, host, initial, topology.StepCleanupReplicaState, old, 11)
	result, err := host.ApplyAction(t.Context(), request)
	must(t, err)
	if len(result.Receipts) != 1 {
		t.Fatal(result)
	}
	if found, err := db.LookupReplica("work"); err != nil || found.Found {
		t.Fatal("retirement retained ledger", found.Found, err)
	}
	successor := record
	successor.ReplicaID = 8
	successor.Origin = "reseat"
	must(t, db.RecordReplica("work", successor))
	if err := db.ForgetReplica("work", 7); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("old cleanup forgot successor", err)
	}
	got, err := db.LookupReplica("work")
	if err != nil || !got.Found || got.Record != successor {
		t.Fatal(got.Record, got.Found, err)
	}
	must(t, host.Stop(t.Context()))
}
