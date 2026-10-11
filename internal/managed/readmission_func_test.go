//go:build functional

package managed

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

func configureReadmission(t *testing.T, f *fixture, ids ...uint64) Readmission {
	t.Helper()
	f.cfg.MetaDBPath = filepath.Join(t.TempDir(), "meta.bolt")
	grant := Readmission{Previous: f.node, Granted: f.node, AdmissionID: "exec-v7-a1/" + f.node.InstanceID, MetaDBPath: f.cfg.MetaDBPath}
	grant.Granted.MemberID = 9
	prior := topology.Group{ID: "work"}
	for _, id := range ids {
		prior.Replicas = append(prior.Replicas, topology.Replica{NodeName: f.node.NodeName, ReplicaID: id})
	}
	grant.PreviousGroups = []topology.Group{prior}
	grant.GrantedGroups = []topology.Group{{ID: "work", Replicas: []topology.Replica{{NodeName: f.node.NodeName, ReplicaID: 9}}}}
	expected := grant.Clone()
	f.cfg.AuthorizeReadmission = func(ctx context.Context, got Readmission) error {
		if !reflect.DeepEqual(got, expected) {
			return fault.ErrOperatorAuthorization
		}
		return ctx.Err()
	}
	writeMeta(t, f.cfg.MetaDBPath, f.node, ids[len(ids)-1])
	return grant
}

func TestReadmissionRetryRetainsUncataloguedOwnedIdentity(t *testing.T) {
	functional(t)
	f := newFixture(t)
	grant := configureReadmission(t, f, 1)
	h := f.host(t)
	_, err := h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "other", ReplicaID: 1}, StartExplicit)
	must(t, err)
	for attempt := 0; attempt < 2; attempt++ {
		if err := h.Readmit(deadline(t), grant); !errors.Is(err, fault.ErrIdentityMismatch) {
			t.Fatal("retry forgot uncatalogued ownership after closing its handle", attempt, err)
		}
		if _, found, err := metastore.LoadIdentity(grant.MetaDBPath); err != nil || !found {
			t.Fatal("refusal lost metadata evidence", attempt, found, err)
		}
	}
	probe, err := f.store.Probe(t.Context(), "other")
	must(t, err)
	if !probe.Exists {
		t.Fatal("retired an identity outside the exact grant")
	}
}

func TestReadmissionJoinsMachineBeforeRetirementAndPreservesMetaBackup(t *testing.T) {
	functional(t)
	f := newFixture(t)
	grant := configureReadmission(t, f, 1)
	var closed, retired atomic.Bool
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		return load.Application{StateMachine: &stateMachine{}, Close: func(context.Context) error { closed.Store(true); return nil }}, nil
	}
	f.cfg.RetireApplication = func(_ context.Context, req load.RetirementRequest) (load.RetirementResult, error) {
		if !closed.Load() {
			return load.RetirementResult{}, errors.New("machine was not joined and closed")
		}
		if req.Reason != load.RetirementReadmission || req.Authorization != nil || req.AdmissionID != grant.AdmissionID || req.PreviousMemberID != 1 || req.GrantedMemberID != 9 {
			return load.RetirementResult{}, fault.ErrOperatorAuthorization
		}
		retired.Store(true)
		return retirementProof(req), nil
	}
	h := f.host(t)
	_, err := h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartExplicit)
	must(t, err)
	must(t, h.Readmit(deadline(t), grant))
	if !retired.Load() || len(h.IDs()) != 0 {
		t.Fatal("readmission did not join and retire old work")
	}
	if err := h.Start(t.Context()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("old host lifetime restarted", err)
	}
	probe, err := f.store.Probe(t.Context(), "work")
	must(t, err)
	if probe.Exists {
		t.Fatal("old Raft namespace remains live")
	}
	pin, found, err := metastore.LoadIdentity(readmissionBackup(grant))
	must(t, err)
	if !found || pin.MemberID != 1 {
		t.Fatal("old pin was not retained", pin)
	}
	records, err := metastore.LoadReplicaRecords(readmissionBackup(grant))
	must(t, err)
	if records["work"].ReplicaID != 1 {
		t.Fatal("ledger was erased before backup", records)
	}
	if fresh, err := metastore.StoreIsFresh(grant.MetaDBPath); err != nil || !fresh {
		t.Fatal("live meta path is not fresh", fresh, err)
	}
}

func TestReadmissionRetriesActualPreparedIdentityAfterQuarantineMove(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	grant := configureReadmission(t, f, 1, 2)
	must(t, f.store.BindReplica(t.Context(), f.localID(2)))
	must(t, f.store.Ensure(t.Context(), "work"))
	f.cfg.Store = &interruptedQuarantine{Store: f.store}
	var calls []uint64
	f.cfg.RetireApplication = func(_ context.Context, req load.RetirementRequest) (load.RetirementResult, error) {
		calls = append(calls, req.Identity.ReplicaID)
		return retirementProof(req), nil
	}
	h := f.host(t)
	if err := h.Readmit(t.Context(), grant); err == nil {
		t.Fatal("lost quarantine acknowledgement was ignored")
	}
	if _, found, err := metastore.LoadIdentity(grant.MetaDBPath); err != nil || !found {
		t.Fatal("uncertain move retired meta evidence", found, err)
	}
	probe, err := f.store.Probe(t.Context(), "work")
	must(t, err)
	if probe.Exists {
		t.Fatal("injected move did not move actual prepared identity")
	}
	must(t, h.Readmit(t.Context(), grant))
	must(t, h.Readmit(t.Context(), grant)) // matching backup + absent original
	if !reflect.DeepEqual(calls, []uint64{1, 2, 1, 2, 1, 2}) {
		t.Fatal("retry skipped application verification", calls)
	}
	// A new live pin must never be quarantined by the older cached grant.
	writeMeta(t, grant.MetaDBPath, grant.Granted, 9)
	if err := h.Readmit(t.Context(), grant); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("retired the replacement pin", err)
	}
	if len(calls) != 6 {
		t.Fatal("called application retirement before checking replacement pin")
	}
	pin, _, err := metastore.LoadIdentity(grant.MetaDBPath)
	must(t, err)
	if pin.MemberID != 9 {
		t.Fatal("replacement pin changed", pin)
	}
}

func TestReadmissionRefusesForeignEvidenceAndUnverifiedCallbacks(t *testing.T) {
	cases := []string{"grant", "catalog", "ledger", "backup", "foreign_live", "symlink", "unverified", "revoked"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			grant := configureReadmission(t, f, 1)
			actual := uint64(1)
			if name == "catalog" {
				actual = 2
			}
			must(t, f.store.BindReplica(t.Context(), f.localID(actual)))
			must(t, f.store.Ensure(t.Context(), "work"))
			if name == "ledger" {
				s, err := metastore.Open(grant.MetaDBPath)
				must(t, err)
				must(t, s.RecordReplicaCAS("work", metastore.ReplicaRecord{NodeName: f.node.NodeName, ClusterID: f.node.ClusterID, ReplicaID: 2}, 1))
				must(t, s.Close())
			}
			if name == "backup" {
				must(t, os.Rename(grant.MetaDBPath, grant.MetaDBPath+".original"))
				foreign := f.node
				foreign.MemberID = 55
				writeMeta(t, readmissionBackup(grant), foreign, 0)
			}
			if name == "foreign_live" || name == "symlink" {
				must(t, os.Rename(grant.MetaDBPath, grant.MetaDBPath+".original"))
				if name == "symlink" {
					must(t, os.Symlink(grant.MetaDBPath+".original", grant.MetaDBPath))
				} else {
					foreign := f.node
					foreign.MemberID = 55
					writeMeta(t, grant.MetaDBPath, foreign, 0)
				}
			}
			var called atomic.Int32
			var revoked atomic.Bool
			authorize := f.cfg.AuthorizeReadmission
			f.cfg.AuthorizeReadmission = func(ctx context.Context, grant Readmission) error {
				if revoked.Load() {
					return fault.ErrOperatorAuthorization
				}
				return authorize(ctx, grant)
			}
			f.cfg.RetireApplication = func(_ context.Context, req load.RetirementRequest) (load.RetirementResult, error) {
				called.Add(1)
				proof := retirementProof(req)
				if name == "unverified" {
					proof.AdmissionID = "another-admission"
				}
				if name == "revoked" {
					revoked.Store(true)
				}
				return proof, nil
			}
			h := f.host(t)
			if name == "grant" {
				grant.AdmissionID = "forged"
			}
			err := h.Readmit(t.Context(), grant)
			if !errors.Is(err, fault.ErrIdentityMismatch) && !errors.Is(err, fault.ErrOperatorAuthorization) {
				t.Fatal("unsafe readmission was accepted", err)
			}
			probe, probeErr := f.store.Probe(t.Context(), "work")
			must(t, probeErr)
			if !probe.Exists || probe.Identity.ReplicaID != actual {
				t.Fatal("refusal changed Raft state", probe)
			}
			if name != "unverified" && name != "revoked" && called.Load() != 0 {
				t.Fatal("invalid evidence reached application callback")
			}
			if name == "grant" {
				h.mu.Lock()
				fenced := h.quiescing
				h.mu.Unlock()
				if fenced {
					t.Fatal("unauthorized grant fenced host")
				}
			}
		})
	}
}

func TestStopCancelsAndJoinsReadmissionCallback(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	grant := configureReadmission(t, f, 1)
	must(t, f.store.BindReplica(t.Context(), f.localID(1)))
	must(t, f.store.Ensure(t.Context(), "work"))
	entered, release := make(chan struct{}), make(chan struct{})
	f.cfg.RetireApplication = func(_ context.Context, req load.RetirementRequest) (load.RetirementResult, error) {
		close(entered)
		<-release
		return retirementProof(req), nil
	}
	h := f.host(t)
	done := make(chan error, 1)
	go func() { done <- h.Readmit(t.Context(), grant) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("readmission callback did not start")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	err := h.Stop(ctx)
	cancel()
	if !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal("shutdown abandoned readmission callback", err)
	}
	close(release)
	select {
	case err = <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("readmission did not join")
	}
	must(t, h.Stop(t.Context()))
	probe, err := f.store.Probe(t.Context(), "work")
	must(t, err)
	if !probe.Exists {
		t.Fatal("canceled callback retired live state")
	}
	if _, found, err := metastore.LoadIdentity(grant.MetaDBPath); err != nil || !found {
		t.Fatal("canceled callback retired meta evidence", found, err)
	}
}

// Ported from tests/cluster/testapp: catalog selection is now owned by Host.
func TestReadmissionVerifiesActualRetainedIDWithinEntirePreviousCatalog(t *testing.T) {
	for _, retained := range []uint64{4, 20} {
		name := "known-interrupted-target"
		if retained == 20 {
			name = "foreign-id"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			grant := configureReadmission(t, f, 3, 4)
			must(t, f.store.BindReplica(t.Context(), f.localID(retained)))
			must(t, f.store.Ensure(t.Context(), "work"))
			h := f.host(t)
			err := h.Readmit(t.Context(), grant)
			probe, probeErr := f.store.Probe(t.Context(), "work")
			must(t, probeErr)
			if retained == 20 {
				if !errors.Is(err, fault.ErrIdentityMismatch) || !probe.Exists || probe.Identity.ReplicaID != 20 {
					t.Fatal("foreign state was changed", err, probe)
				}
				if _, found, err := metastore.LoadIdentity(grant.MetaDBPath); err != nil || !found {
					t.Fatal("refusal moved meta pin", found, err)
				}
				return
			}
			if err != nil || probe.Exists {
				t.Fatal("historical ordering blocked exact known state", err, probe)
			}
			must(t, h.Readmit(t.Context(), grant))
			backups, err := filepath.Glob(grant.MetaDBPath + ".retired-*")
			if err != nil || len(backups) != 1 {
				t.Fatal(backups, err)
			}
		})
	}
}
