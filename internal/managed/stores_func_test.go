//go:build functional

package managed

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage/memstore"
	"github.com/asalimonov/gquorum/topology"
)

// withMemory gives the fixture a Node-style in-memory store.
func withMemory(t *testing.T, f *fixture) *memstore.Store {
	t.Helper()
	mem, err := memstore.New(memstore.Options{NodeName: f.node.NodeName, ClusterID: f.node.ClusterID})
	must(t, err)
	t.Cleanup(func() { must(t, mem.Close()) })
	f.cfg.MemoryStore, f.cfg.MemoryRetirement = mem, mem
	return mem
}

// proposeEventually retries until the single-voter replica has elected itself.
func proposeEventually(t *testing.T, core *raftengine.Group, request string) {
	t.Helper()
	ctx := deadline(t)
	for {
		_, err := core.Propose(ctx, raftengine.Proposal{RequestID: request, Data: []byte(request)})
		if err == nil {
			return
		}
		if !fault.Retryable(err) || ctx.Err() != nil {
			t.Fatal(request, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (f *fixture) setStorage(group string, class topology.StorageClass) {
	f.mu.Lock()
	defer f.mu.Unlock()
	scope := f.scopes[group]
	scope.Group.Storage = class
	f.scopes[group] = scope
}

func TestReplicasUseTheStoreOfTheirStorageClass(t *testing.T) {
	functional(t)
	f := newFixture(t)
	mem := withMemory(t, f)
	f.setStorage("work", topology.StorageMemory)
	h := f.host(t)
	for _, group := range []string{"work", "other"} {
		g, err := h.StartReplica(deadline(t), load.ReplicaRef{GroupID: group, ReplicaID: 1}, StartExplicit)
		must(t, err)
		proposeEventually(t, g.Core(), group)
	}
	for group, wantMemory := range map[string]bool{"work": true, "other": false} {
		memory, err := mem.Probe(deadline(t), group)
		must(t, err)
		durable, err := f.store.Probe(deadline(t), group)
		must(t, err)
		if memory.Exists != wantMemory || durable.Exists == wantMemory || !memory.HasLog && !durable.HasLog {
			t.Fatalf("%s: memory %+v, durable %+v", group, memory, durable)
		}
	}
}

func TestMemoryGroupWithoutMemoryStoreRefusesStart(t *testing.T) {
	functional(t)
	f := newFixture(t)
	f.setStorage("work", topology.StorageMemory)
	h := f.host(t)
	if _, err := h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartExplicit); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal(err)
	}
	if probe, err := f.store.Probe(deadline(t), "work"); err != nil || probe.Exists {
		t.Fatal("memory group fell back to the durable store", probe, err)
	}
}

func TestGroupStateInTheWrongStoreParks(t *testing.T) {
	functional(t)
	for name, memoryGroup := range map[string]bool{"memory group with durable state": true, "durable group with memory state": false} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			mem := withMemory(t, f)
			other := f.cfg.Store
			if memoryGroup {
				f.setStorage("work", topology.StorageMemory)
			} else {
				other = mem
			}
			must(t, other.BindReplica(deadline(t), f.localID(1)))
			must(t, other.Ensure(deadline(t), "work"))
			h := f.host(t)
			_, err := h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartExplicit)
			var parked *ParkedError
			if !errors.As(err, &parked) || parked.Reason != ParkIdentityMismatch {
				t.Fatal(err)
			}
		})
	}
	t.Run("both stores", func(t *testing.T) {
		t.Parallel()
		f := newFixture(t)
		mem := withMemory(t, f)
		must(t, f.store.BindReplica(deadline(t), f.localID(1)))
		must(t, f.store.Ensure(deadline(t), "work"))
		must(t, mem.BindReplica(deadline(t), f.localID(1)))
		must(t, mem.Ensure(deadline(t), "work"))
		h := f.host(t)
		if _, err := h.locate(deadline(t), "work"); !errors.Is(err, fault.ErrIdentityMismatch) {
			t.Fatal(err)
		}
		_, err := h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartExplicit)
		var parked *ParkedError
		if !errors.As(err, &parked) || parked.Reason != ParkIdentityMismatch {
			t.Fatal(err)
		}
	})
}

func TestMemoryReplicaRetiresAndCollectsThroughTheMemoryStore(t *testing.T) {
	functional(t)
	f := newFixture(t)
	mem := withMemory(t, f)
	_, initial := fixtureAuthority(t, f)
	f.setStorage("work", topology.StorageMemory)
	f.cfg.Retention = func(context.Context, lifecycle.RetentionInput) (lifecycle.RetentionDecision, error) {
		return lifecycle.RetentionDecision{Collect: true}, nil
	}
	h := f.host(t)
	_, err := h.InstallHostFence(deadline(t), initial)
	must(t, err)
	_, err = h.StartReplica(deadline(t), load.ReplicaRef{GroupID: "work", ReplicaID: 1}, StartOperator)
	must(t, err)
	id := f.localID(1)
	f.setScope("work", 2, false, false)
	f.setStorage("work", topology.StorageMemory)
	req := installFixtureHostAction(t, f, h, initial, topology.StepCleanupReplicaState, id, 11)
	result, err := h.ApplyAction(deadline(t), req)
	must(t, err)
	if len(result.Receipts) != 1 {
		t.Fatal(result)
	}
	if probe, err := mem.Probe(deadline(t), "work"); err != nil || probe.Exists {
		t.Fatal("memory replica was not quarantined", probe, err)
	}
	retired, err := mem.Retired(deadline(t))
	must(t, err)
	if len(retired) != 1 || retired[0].Identity != id {
		t.Fatal(retired)
	}
	if found, err := f.cfg.LookupRecord(deadline(t), "work"); err != nil || found.Found {
		t.Fatal("verified retirement retained ledger", found.Found, err)
	}
	must(t, h.CollectRetired(deadline(t)))
	if retired, err = mem.Retired(deadline(t)); err != nil || len(retired) != 0 {
		t.Fatal("memory retirement was not collected", retired, err)
	}
}
