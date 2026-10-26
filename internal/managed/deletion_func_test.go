//go:build functional

package managed

import (
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
	"testing"
)

func TestDeletionCleanupJoinsCurrentPreparedAndHeldReplica(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"current", "prepared", "held"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			_, initial := fixtureAuthority(t, f)
			if mode == "prepared" {
				f.setScope("work", 1, false, false)
				action := f.approve(topology.StepPrepareReplica, f.localID(1))
				initial.Action = &action
				initial.Resources = initial.Resources[1:]
			}
			h := f.host(t)
			_, err := h.InstallHostFence(deadline(t), initial)
			must(t, err)
			ref := load.ReplicaRef{GroupID: "work", ReplicaID: 1}
			handle, err := h.StartReplica(deadline(t), ref, StartOperator)
			must(t, err)
			if mode == "held" {
				must(t, h.StopReplica(deadline(t), ref, StopExplicit))
			}
			stop := installFixtureHostAction(t, f, h, initial, topology.StepStopReplica, f.localID(1), 11)
			result, err := h.ApplyAction(deadline(t), stop)
			must(t, err)
			if len(result.Receipts) != 1 || handle.Core().Running() {
				t.Fatal("stop did not join exact replica", result)
			}
			must(t, result.Receipts[0].Validate(stop.Grant))
			retire := installFixtureHostAction(t, f, h, initial, topology.StepDeleteGroupState, f.localID(1), 12)
			result, err = h.ApplyAction(deadline(t), retire)
			must(t, err)
			if len(result.Receipts) != 1 {
				t.Fatal(result)
			}
			must(t, result.Receipts[0].Validate(retire.Grant))
			probe, err := f.store.Probe(t.Context(), "work")
			must(t, err)
			ledger, err := f.cfg.LookupRecord(t.Context(), "work")
			must(t, err)
			if probe.Exists || ledger.Found {
				t.Fatal("retirement retained live identity", probe, ledger)
			}
		})
	}
}
