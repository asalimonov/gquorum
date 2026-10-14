//go:build functional

package gquorum

import (
	"context"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/topology"
)

func TestPublicLabelEditUsesDocumentWorkflow(t *testing.T) {
	t.Parallel()
	opts := authorityNodeOptions(t, t.TempDir(), "labels")
	n, err := NewNode(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopAuthorityNode(t, n) })
	if err := n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := n.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	c := n.Cluster()
	latest, err := c.Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	target, err := c.Target(ctx)
	if err != nil {
		t.Fatal(err)
	}
	active, err := c.Active(ctx)
	if err != nil {
		t.Fatal(err)
	}
	edited := latest.Clone()
	labels := edited.Node("n1").GetLabels()
	labels["network"] = "public_ip"
	if err := edited.Node("n1").SetLabels(labels); err != nil {
		t.Fatal(err)
	}
	if err := edited.MetaGroup().SetLabels(map[string]string{"purpose": "accounting"}); err != nil {
		t.Fatal(err)
	}
	edited.ChangedBy = "operator"
	preparation, err := topology.PreparePlacement(topology.ReadView{Latest: latest, Target: target, Active: active}, edited,
		topology.Rules{SchemaVersion: 1, Limits: opts.Limits.WithDefaults()}, time.Now().UTC())
	prepared := preparation.Document
	if err != nil {
		t.Fatal(err)
	}
	staged, err := c.Stage(ctx, prepared, latest.Version)
	if err != nil {
		t.Fatal(err)
	}
	if staged.Version != latest.Version+1 || staged.Node("n1").GetLabels()["network"] != "public_ip" ||
		staged.MetaGroup().GetLabels()["purpose"] != "accounting" || staged.MetaGroup().NextReplicaID != latest.MetaGroup().NextReplicaID {
		t.Fatal("Stage did not preserve the complete prepared document", staged)
	}
	if _, err := c.Approve(ctx, staged.Version); err != nil {
		t.Fatal("prepared label edit could not be approved", err)
	}
}
