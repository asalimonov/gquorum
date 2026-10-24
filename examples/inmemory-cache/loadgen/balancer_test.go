//go:build !functional

package main

import (
	"context"
	"testing"
)

func TestBalancerRoundRobinSkipsUnhealthyAndReadmitsRecoveredNodes(t *testing.T) {
	t.Parallel()
	b := newBalancer(nil, []string{"node1:6379", "node2:6379", "node3:6379"}, 8080)
	for i := range b.nodes {
		b.setHealth(i, backendHealthy)
	}
	check := func(want []int) {
		t.Helper()
		for _, expected := range want {
			index, err := b.selectNode(t.Context())
			if err != nil || index != expected {
				t.Fatalf("selected=%d error=%v want=%d", index, err, expected)
			}
		}
	}
	check([]int{0, 1, 2, 0, 1, 2})
	b.setHealth(1, backendUnavailable)
	check([]int{0, 2, 0, 2, 0, 2})
	b.setHealth(1, backendHealthy)
	check([]int{0, 1, 2})
	for i := range b.nodes {
		b.setHealth(i, backendUnavailable)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := b.selectNode(ctx); err != context.Canceled {
		t.Fatalf("no healthy nodes ignored cancellation: %v", err)
	}
}

func TestBalancerWaitsForFirstSuccessfulHealthProbe(t *testing.T) {
	t.Parallel()
	b := newBalancer(nil, []string{"node1:6379"}, 8080)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan int, 1)
	go func() { index, _ := b.selectNode(ctx); result <- index }()
	b.setHealth(0, backendHealthy)
	if index := <-result; index != 0 {
		t.Fatal(index)
	}
}

func TestBalancerNodeConfiguration(t *testing.T) {
	t.Parallel()
	for _, nodes := range [][]string{{"host"}, {":6379"}, {"node1:0"}, {"node1:65536"}, {"node1:6379", "node1:6379"}} {
		if err := (config{Nodes: nodes, HealthPort: 8080}).validateNodes(); err == nil {
			t.Errorf("accepted invalid nodes: %v", nodes)
		}
	}
	b := newBalancer(nil, []string{"[::1]:6379"}, 8080)
	if b.nodes[0].healthURL != "http://[::1]:8080/health" {
		t.Fatal(b.nodes[0].healthURL)
	}
}
