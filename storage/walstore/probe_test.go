package walstore

import (
	"context"
	"testing"
)

func TestColdProbeReadsRetainedLogWithoutBindingOrWorkers(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	ctx := context.Background()
	s := boundTest(t, root, false)
	seed(t, s, 3)
	check(t, s.Close())
	cold := openTest(t, root, "a", "test-cluster", false)
	probe, err := cold.Probe(ctx, "group")
	check(t, err)
	if !probe.Exists || !probe.HasLog || probe.Identity.ReplicaID != 7 {
		t.Fatal(probe)
	}
	cold.mu.Lock()
	handles, bindings := len(cold.groups), len(cold.bindings)
	cold.mu.Unlock()
	if handles != 0 || bindings != 0 {
		t.Fatal("read-only probe opened a writable group", handles, bindings)
	}
	missing, err := cold.Probe(ctx, "absent")
	check(t, err)
	if missing.Exists || missing.HasLog {
		t.Fatal(missing)
	}
}
