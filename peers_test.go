//go:build !functional

package gquorum

import (
	"context"
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func TestStaticPeersResolveAndFounders(t *testing.T) {
	t.Parallel()
	peers := StaticPeers{
		"n2": {MetaAddress: "n2:9000", LoadAddress: "n2:9001"},
		"n1": {MetaAddress: "n1:9000", LoadAddress: "n1:9001"},
	}
	ctx := context.Background()
	if got, err := peers.Resolve(ctx, "n1"); err != nil || got != "n1:9000" {
		t.Fatal(got, err)
	}
	if got, err := peers.ResolveLoad(ctx, topology.Identity{NodeName: "n2"}); err != nil || got != "n2:9001" {
		t.Fatal(got, err)
	}
	if _, err := peers.Resolve(ctx, "n3"); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("unknown meta peer resolved", err)
	}
	if _, err := peers.ResolveLoad(ctx, topology.Identity{NodeName: "n3"}); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal("unknown load peer resolved", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := peers.Resolve(canceled, "n1"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	founders, err := peers.Founders("cluster", "worker")
	if err != nil || len(founders) != 2 || founders[0].Node.Name != "n1" || founders[1].Address != "n2:9000" {
		t.Fatal(founders, err)
	}
	ids, err := FoundingIDs("cluster", []string{"n1", "n2"})
	if err != nil || founders[0].MemberID != ids["n1"] || founders[1].MemberID != ids["n2"] || founders[0].Node.Type != "worker" {
		t.Fatal("founders do not use FoundingIDs", founders, ids, err)
	}
	if _, err := (StaticPeers{}).Founders("cluster", "worker"); err == nil {
		t.Fatal("empty roster produced founders")
	}
}
