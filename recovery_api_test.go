//go:build !functional

package gquorum_test

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/recovery"
	"github.com/asalimonov/gquorum/topology"
)

var _ interface {
	Inspect(context.Context, recovery.Request) (recovery.Plan, error)
	Approve(context.Context, recovery.Request, recovery.Acceptance) (recovery.Operation, error)
	Run(context.Context, string) (recovery.Status, error)
	Cancel(context.Context, string, string, string) (recovery.Status, error)
	Status(context.Context, string) (recovery.Status, error)
	List(context.Context) ([]recovery.Status, error)
} = (*gquorum.Recovery)(nil)

func TestRecoveryPublicContractsAreExplicitlyPending(t *testing.T) {
	t.Parallel()
	r, err := gquorum.NewRecovery(gquorum.NodeOptions{StorageDir: "unused", Identity: topology.Identity{NodeName: "n1"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Inspect(context.Background(), recovery.Request{})
	var refusal *recovery.Refusal
	if !errors.As(err, &refusal) || refusal.Issue.Code != "implementation_pending" || !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal("unimplemented operation looked successful", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Status(ctx, ""); !errors.Is(err, context.Canceled) {
		t.Fatal("lost cancellation cause", err)
	}
	if (load.ReplicaSpec{}).Recovery != nil {
		t.Fatal("ordinary replica unexpectedly has recovery input")
	}
}
