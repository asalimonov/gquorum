//go:build clustertest

package cluster

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type collectionReportView struct {
	Identity storage.Identity                 `json:"identity"`
	Hits     uint64                           `json:"hits"`
	Receipt  authoritystore.CollectionReceipt `json:"receipt"`
}

func collectionReport(ctx context.Context, t *testing.T, c *Cluster, node, method string, id storage.Identity) collectionReportView {
	t.Helper()
	response, err := c.Request(ctx, node, method, "/internal/test/collection-report", map[string]any{"identity": id}, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var view collectionReportView
	if err := response.Decode(&view); err != nil {
		t.Fatal(err)
	}
	return view
}

func collectionCalls(ctx context.Context, t *testing.T, c *Cluster, id storage.Identity) uint64 {
	t.Helper()
	response, err := c.Request(ctx, id.NodeName, "GET", fmt.Sprintf("/internal/test/collections/%s/%d", id.GroupID, id.ReplicaID), nil, 0)
	if err := requireStatus(response, err, 200); err != nil {
		t.Fatal(err)
	}
	var result struct {
		Identity storage.Identity `json:"identity"`
		Calls    uint64           `json:"calls"`
	}
	if err := response.Decode(&result); err != nil || result.Identity != id {
		t.Fatal(result, err)
	}
	return result.Calls
}

// Copy and open only after the actual process is dead; never acquire a second
// owner of a live journal. This observes the process's crash evidence unchanged.
func crashedCollection(ctx context.Context, t *testing.T, c *Cluster, identity topology.Identity, id storage.Identity) authoritystore.CollectionRecord {
	t.Helper()
	source, err := c.Node(identity.NodeName).Container.CopyFileFromContainer(ctx, "/data/_authority.bolt")
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(io.LimitReader(source, (16<<20)+1))
	closeErr := source.Close()
	if readErr != nil || closeErr != nil || len(data) > 16<<20 {
		t.Fatal(readErr, closeErr, len(data))
	}
	path := filepath.Join(t.TempDir(), "authority.bolt")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	journal, err := authoritystore.Open(path, identity, authoritystore.RetainedEvidence)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	row, err := journal.Collection(ctx, id)
	if err != nil || !row.Found {
		t.Fatal("crash lost collection proof", row, err)
	}
	return row.Record
}

func TestFailureIsolation_CollectionReporting(t *testing.T) {
	t.Parallel()
	spec := matrixSpec(3)
	spec.MetaFaultProxy, spec.Retention = true, time.Nanosecond
	c := acquire(t, spec).Cluster
	ctx, cancel := context.WithTimeout(t.Context(), 110*time.Second)
	defer cancel()
	created, err := c.CreateGroup(ctx, "node1", gquorum.GroupSpec{ID: "collected"})
	if err != nil {
		t.Fatal(err)
	}
	if err := transferMeta(ctx, c, "node1", "node1", created.Document.MemberID("node1")); err != nil {
		t.Fatal(err)
	}
	owner, err := c.Local(ctx, "node3", "collected")
	if err != nil || owner.StorageIdentity == nil {
		t.Fatal(owner, err)
	}
	id := *owner.StorageIdentity
	identities := map[string]topology.Identity{}
	for _, n := range c.Nodes() {
		local, err := c.Local(ctx, n.Spec.Name, topology.MetaGroupID)
		if err != nil {
			t.Fatal(err)
		}
		identities[n.Spec.Name] = local.Identity
		collectionReport(ctx, t, c, n.Spec.Name, "PUT", id)
	}
	deleted, err := c.Mutate(ctx, "node1", "DELETE", "/api/groups/collected", gquorum.MutationOptions{Actor: "collection-test", Approve: true})
	if err != nil {
		t.Fatal(err)
	}
	var first authoritystore.CollectionReceipt
	if err := Poll(ctx, func() (bool, error) {
		for _, n := range c.Nodes() {
			view := collectionReport(ctx, t, c, n.Spec.Name, "GET", id)
			if view.Hits > 0 {
				first = view.Receipt
				return true, nil
			}
		}
		return false, nil
	}); err != nil {
		t.Fatal("no durable receipt reached reporting", err)
	}
	if first.Identity != id || first.LocalSequence == 0 || first.Token == "" || collectionCalls(ctx, t, c, id) != 1 {
		t.Fatal("missing exact successful collection", first)
	}
	page, err := c.Progress(ctx, "node1", topology.ProgressQuery{GroupID: id.GroupID, NodeName: id.NodeName})
	debt, found := exactCleanup(page, id)
	if err != nil || !found || debt.RetiredAt == nil || debt.CollectedAt != nil || page.FullyConverged.Version != deleted.Document.Version {
		t.Fatal("held report manufactured collection or blocked deletion", page, err)
	}
	for _, name := range []string{"node1", "node2"} {
		if _, err := c.MetaFault(ctx, name, "PUT", identities[name], true); err != nil {
			t.Fatal(err)
		}
	}
	// A fresh safe read must lose quorum; collection replies stay held until
	// after this observable boundary, so no acceptance can sneak through it.
	if err := Poll(ctx, func() (bool, error) {
		readCtx, stopRead := context.WithTimeout(ctx, 800*time.Millisecond)
		defer stopRead()
		_, err := c.Progress(readCtx, "node1", topology.ProgressQuery{GroupID: id.GroupID})
		return err != nil, nil
	}); err != nil {
		t.Fatal("metadata read retained quorum after isolation", err)
	}
	for _, n := range c.Nodes() {
		collectionReport(ctx, t, c, n.Spec.Name, "DELETE", id)
	}
	if err := c.Node("node3").Kill(ctx); err != nil {
		t.Fatal(err)
	}
	row := crashedCollection(ctx, t, c, owner.Identity, id)
	if row.State != "collected" || row.CollectedAt == nil || row.Sequence != first.LocalSequence || row.Retired.Token != first.Token || !row.CollectedAt.Equal(first.CollectedAt) || row.Grant.Key != first.RetirementKey {
		t.Fatal("report failure/crash changed local receipt", row, first)
	}
	for _, name := range []string{"node1", "node2"} {
		if _, err := c.MetaFault(ctx, name, "PUT", identities[name], false); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Node("node3").Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if err := Poll(ctx, func() (bool, error) {
		page, err := c.Progress(ctx, "node1", topology.ProgressQuery{GroupID: id.GroupID, NodeName: id.NodeName})
		debt, found := exactCleanup(page, id)
		return found && debt.CollectedAt != nil && debt.CollectedAt.Equal(first.CollectedAt), err
	}); err != nil {
		t.Fatal("retained receipt did not reach metadata", err)
	}
	if calls := collectionCalls(ctx, t, c, id); calls != 0 {
		t.Fatal("collected journal repeated destructive work after restart", calls)
	}
}
