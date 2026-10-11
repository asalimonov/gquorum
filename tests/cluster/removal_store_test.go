//go:build clustertest

package cluster

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/topology"
)

// Read only after the actual participant has stopped and released bbolt. This
// preserves the former private runtime assertion without a second live owner.
func assertDurableSelfRemoval(t *testing.T, ctx context.Context, c *Cluster, status gquorum.NodeStatus) {
	t.Helper()
	source, err := c.Node(status.Identity.NodeName).Container.CopyFileFromContainer(ctx, "/data/meta.bolt")
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(io.LimitReader(source, 16<<20))
	closeErr := source.Close()
	if readErr != nil || closeErr != nil {
		t.Fatal(readErr, closeErr)
	}
	path := filepath.Join(t.TempDir(), "removed.bolt")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	store, err := metastore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()
	conf, found, err := store.AppliedConfState()
	if err != nil || !found || slices.Contains(conf.Voters, status.Identity.MemberID) || slices.Contains(conf.VotersOutgoing, status.Identity.MemberID) || slices.Contains(conf.Learners, status.Identity.MemberID) || slices.Contains(conf.LearnersNext, status.Identity.MemberID) {
		t.Fatal("removal membership was not durable", conf, err)
	}
	seen, err := store.MemberSeen()
	if err != nil || !seen {
		t.Fatal("membership history missing", seen, err)
	}
	index, err := store.AppliedIndex()
	if err != nil || status.SelfRemoval == nil || index < status.SelfRemoval.AppliedIndex {
		t.Fatal("removal notification preceded applied persistence", index, status.SelfRemoval, err)
	}
	data, err = store.LoadState()
	if err != nil {
		t.Fatal(err)
	}
	machine := metaengine.NewMachine(topology.Rules{SchemaVersion: 1, Limits: c.Spec.Limits.WithDefaults()}, nil)
	if err := machine.Restore(ctx, data); err != nil {
		t.Fatal(err)
	}
	doc := machine.Target()
	if node := doc.Node(status.Identity.NodeName); node == nil || node.State != topology.NodeExcluded {
		t.Fatal("approved exclusion not durable", doc)
	}
}
