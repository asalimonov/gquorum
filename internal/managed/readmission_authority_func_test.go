//go:build functional

package managed

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

func TestReadmissionAuthorityRetriesIntentAndRetainsReceiptsThroughRebind(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	s, initial := fixtureAuthority(t, f)
	grant := configureReadmission(t, f, 1)
	var closed atomic.Bool
	var calls atomic.Int32
	f.cfg.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		return load.Application{StateMachine: &stateMachine{}, Close: func(context.Context) error { closed.Store(true); return nil }}, nil
	}
	f.cfg.RetireApplication = func(ctx context.Context, req load.RetirementRequest) (load.RetirementResult, error) {
		if !closed.Load() || !req.Grant.Equal(grant.Grants[1]) {
			return load.RetirementResult{}, errors.New("retirement preceded close or omitted authority")
		}
		floor, found, err := s.Current(ctx, "work")
		if err != nil || !found || !floor.Complete || !floor.Grant.Equal(req.Grant) {
			return load.RetirementResult{}, errors.New("retirement preceded durable fence")
		}
		running, err := s.Running(ctx, "work")
		if err != nil || len(running) != 1 || !running[0].Grant.Equal(req.Grant) {
			return load.RetirementResult{}, errors.New("retirement lacks exact durable intent")
		}
		if calls.Add(1) == 1 {
			return load.RetirementResult{}, fault.ErrUnavailable
		}
		return retirementProof(req), nil
	}
	h := f.host(t)
	_, err := h.InstallHostFence(deadline(t), initial)
	must(t, err)
	_, err = h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartOperator)
	must(t, err)
	if err = h.Readmit(deadline(t), grant); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("failed retirement did not retain its failure", err)
	}
	for _, g := range grant.Grants {
		if _, found, err := s.Receipt(t.Context(), g, g.Targets[0]); err != nil || found {
			t.Fatal("incomplete retirement produced a receipt", found, err)
		}
	}
	if _, found, err := metastore.LoadIdentity(grant.MetaDBPath); err != nil || !found {
		t.Fatal("incomplete load retirement moved metadata", found, err)
	}
	must(t, h.Readmit(deadline(t), grant))
	must(t, h.Readmit(deadline(t), grant))
	if calls.Load() != 2 {
		t.Fatal("completed receipt did not suppress repeated application retirement", calls.Load())
	}
	must(t, f.store.BindReplica(t.Context(), f.localID(1)))
	must(t, f.store.Ensure(t.Context(), "work"))
	if err = h.Readmit(deadline(t), grant); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("cached receipt hid reappeared former storage", err)
	}
	if probe, err := f.store.Probe(t.Context(), "work"); err != nil || !probe.Exists || calls.Load() != 2 {
		t.Fatal("receipt retry changed contradictory storage", probe, err, calls.Load())
	}
	_, err = f.store.Quarantine(t.Context(), "work", 1)
	must(t, err)
	must(t, s.Rebind(t.Context(), grant.Previous, grant.Granted, grant.AdmissionID))
	for _, g := range grant.Grants {
		r, found, err := s.Receipt(t.Context(), g, g.Targets[0])
		if err != nil || !found || r.Validate(g) != nil {
			t.Fatal("rebind lost exact retirement receipt", r, found, err)
		}
	}
	if _, found, err := metastore.LoadIdentity(readmissionBackup(grant)); err != nil || !found {
		t.Fatal("readmission lost old metadata evidence", found, err)
	}
}

func TestReadmissionAuthorityValidatesWholeScopeBeforeFencing(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*Readmission){
		"missing":   func(g *Readmission) { g.Grants = nil },
		"partial":   func(g *Readmission) { g.Grants = g.Grants[:1] },
		"admission": func(g *Readmission) { g.Grants[1].AdmissionID = "foreign" },
		"member":    func(g *Readmission) { g.Grants[1].Targets[0].MemberID++ },
		"replica":   func(g *Readmission) { g.Grants[1].Targets[0].ReplicaID++ },
		"revision":  func(g *Readmission) { g.Grants[1].Key.Revision++ },
		"duplicate": func(g *Readmission) { g.Grants = append(g.Grants, g.Grants[1]) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			s, _ := fixtureAuthority(t, f)
			grant := configureReadmission(t, f, 1)
			mutate(&grant)
			f.cfg.AuthorizeReadmission = func(ctx context.Context, _ Readmission) error { return ctx.Err() }
			var calls atomic.Int32
			f.cfg.RetireApplication = func(_ context.Context, req load.RetirementRequest) (load.RetirementResult, error) {
				calls.Add(1)
				return retirementProof(req), nil
			}
			h := f.host(t)
			if err := h.Readmit(deadline(t), grant); err == nil {
				t.Fatal("invalid scope admitted")
			}
			for _, group := range []string{topology.MetaGroupID, "work"} {
				if _, found, err := s.Current(t.Context(), group); err != nil || found {
					t.Fatal("invalid batch partially changed authority", group, found, err)
				}
			}
			if calls.Load() != 0 {
				t.Fatal("invalid batch invoked retirement")
			}
			if _, found, err := metastore.LoadIdentity(grant.MetaDBPath); err != nil || !found {
				t.Fatal("invalid batch lost retained evidence", found, err)
			}
		})
	}
}

func TestMetaOnlyReadmissionPersistsIntentAndReceipt(t *testing.T) {
	t.Parallel()
	id := topology.Identity{ClusterID: "cluster", NodeName: "one", MemberID: 1, InstanceID: "process"}
	path := filepath.Join(t.TempDir(), "authority.bolt")
	s, err := authoritystore.Open(path, id, true)
	must(t, err)
	t.Cleanup(func() { must(t, s.Close()) })
	grant := Readmission{Previous: id, Granted: id, AdmissionID: "approved", MetaDBPath: filepath.Join(t.TempDir(), "meta.bolt")}
	grant.Granted.MemberID = 2
	grant.Grants = readmissionTestGrants(grant)
	writeMeta(t, grant.MetaDBPath, id, 0)
	authorize := func(ctx context.Context, _ Readmission) error { return ctx.Err() }
	must(t, ReadmitMeta(t.Context(), id, grant.MetaDBPath, grant, s, authorize))
	must(t, ReadmitMeta(t.Context(), id, grant.MetaDBPath, grant, s, authorize))
	g := grant.Grants[0]
	r, found, err := s.Receipt(t.Context(), g, g.Targets[0])
	must(t, err)
	if !found || r.Validate(g) != nil {
		t.Fatal("metadata retirement lacks exact receipt", r, found)
	}
	must(t, os.Rename(readmissionBackup(grant), grant.MetaDBPath))
	if err = ReadmitMeta(t.Context(), id, grant.MetaDBPath, grant, s, authorize); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("cached metadata receipt hid a reappeared former pin", err)
	}
	if pin, found, err := metastore.LoadIdentity(grant.MetaDBPath); err != nil || !found || pin.MemberID != id.MemberID {
		t.Fatal("receipt retry changed contradictory metadata", pin, found, err)
	}
	must(t, os.Rename(grant.MetaDBPath, readmissionBackup(grant)))
	must(t, s.Rebind(t.Context(), grant.Previous, grant.Granted, grant.AdmissionID))
	must(t, s.Close())
	restarted := grant.Granted
	restarted.InstanceID = "new-process"
	s, err = authoritystore.Open(path, restarted, false)
	must(t, err)
	if s.Identity() != restarted {
		t.Fatal("rebound identity did not survive restart", s.Identity())
	}
	if got, found, err := s.Receipt(t.Context(), g, g.Targets[0]); err != nil || !found || got != r {
		t.Fatal("restart lost receipt for the former identity", got, found, err)
	}
}
