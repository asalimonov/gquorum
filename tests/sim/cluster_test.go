//go:build sim

package sim_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/netip"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/topology"
	simnet "github.com/neria-cloud/gosim/net"
	"github.com/neria-cloud/gosim/rt"
)

const (
	probeInterval = 100 * time.Millisecond
	// Follower reads may wait for an active cohort before forwarding a new
	// ReadIndex round. Allow both rounds at the 400ms maximum network RTT.
	probeTimeout = 5 * time.Second
)

func TestClusterLatencyZero(t *testing.T) { testLatency(t, 0, 0) }
func TestClusterLatency50ms(t *testing.T) { testLatency(t, 50*time.Millisecond, 50*time.Millisecond) }
func TestClusterLatency100ms(t *testing.T) {
	testLatency(t, 100*time.Millisecond, 100*time.Millisecond)
}
func TestClusterLatency200ms(t *testing.T) {
	testLatency(t, 200*time.Millisecond, 200*time.Millisecond)
}
func TestClusterLatencyJitter(t *testing.T) { testLatency(t, 0, 200*time.Millisecond) }

func testLatency(t *testing.T, minDelay, maxDelay time.Duration) {
	t.Helper()
	configure(t, minDelay, maxDelay)
	cluster := startCluster(t)
	cluster.writeAndCheck(t, 0, "before-latency")
	monitor := startMonitor(cluster.nodes[:])
	defer monitor.stop(t)
	started := time.Now()
	for i := range 10 {
		time.Sleep(time.Until(started.Add(time.Duration(i+1) * 3 * time.Second)))
		cluster.write(t, i%len(cluster.nodes), fmt.Sprintf("latency-%d", i))
	}
	cluster.writeAndCheck(t, 1, "after-latency")
	monitor.stop(t)
	t.Logf("three voters served quorum reads and committed mutations with one-way delay %s..%s", minDelay, maxDelay)
}

func TestClusterIntermittentFollower(t *testing.T) {
	configure(t, 0, 200*time.Millisecond)
	cluster := startCluster(t)
	leader := cluster.leader(t)
	flapping := (leader + 1) % len(cluster.nodes)
	stable := []*simNode{cluster.nodes[leader], cluster.nodes[(leader+2)%len(cluster.nodes)]}
	monitor := startMonitor(stable)
	defer monitor.stop(t)

	// Each onset is 3..10 seconds after the previous onset. Two-second
	// outages allow the leader's multi-round mutation to finish at 400ms
	// maximum RTT and leave a connected period before the next partition.
	nextOnset := time.Now()
	for cycle := range 10 {
		interval := time.Duration(3+rand.IntN(8)) * time.Second
		nextOnset = nextOnset.Add(interval)
		time.Sleep(time.Until(nextOnset))
		if !time.Now().Equal(nextOnset) {
			t.Fatalf("partition schedule slipped: expected %s, got %s", nextOnset, time.Now())
		}
		const outage = 2 * time.Second
		t.Logf("cycle=%d isolate=%s interval=%s outage=%s", cycle, cluster.nodes[flapping].name, interval, outage)
		cluster.partition(t, flapping, true)
		// The surviving pair must commit a new mutation during the outage.
		cluster.write(t, leader, fmt.Sprintf("partition-%d", cycle))
		if time.Now().After(nextOnset.Add(outage)) {
			t.Errorf("mutation exceeded scheduled outage of %s", outage)
		}
		time.Sleep(max(0, time.Until(nextOnset.Add(outage))))
		cluster.partition(t, flapping, false)
	}
	cluster.waitRejoined(t, flapping)
	cluster.writeAndCheck(t, leader, "after-partitions")
	monitor.stop(t)
}

// The negative control prevents cached readiness or a local leader observation
// from making the quorum monitor pass without any peer communication.
func TestQuorumProbeRejectsIsolatedNode(t *testing.T) {
	configure(t, 0, 0)
	cluster := startCluster(t)
	isolated := (cluster.leader(t) + 1) % len(cluster.nodes)
	cluster.partition(t, isolated, true)
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()
	if err := cluster.nodes[isolated].call(ctx, func(ctx context.Context, n gquorum.Node) error {
		return n.Cluster().ReadBarrier(ctx)
	}); err == nil {
		t.Fatal("isolated voter confirmed quorum")
	}
	cluster.partition(t, isolated, false)
	cluster.waitRejoined(t, isolated)
	cluster.writeAndCheck(t, 0, "repaired")
}

func configure(t *testing.T, minDelay, maxDelay time.Duration) {
	t.Helper()
	t.Logf("gosim identity: %+v", rt.Info())
	if err := rt.SetTimeout(5 * time.Minute); err != nil {
		t.Fatal(err)
	}
	cfg := simnet.DefaultConfig()
	cfg.Latency = simnet.LatencyConfig{Min: minDelay, Max: maxDelay, Lambda: 5}
	cfg.Loss = simnet.LossConfig{RepairProbability: 1}
	cfg.ConnectionTimeout = 5 * time.Second
	// A realistic TCP window avoids imposing the model's legacy 1KiB
	// bandwidth-delay bottleneck on these latency-only scenarios.
	cfg.TCPBufferSize = 256 * 1024
	if err := simnet.Configure(cfg); err != nil {
		t.Fatal(err)
	}
}

func (c *simCluster) partition(t *testing.T, node int, isolate bool) {
	t.Helper()
	for peer := range c.nodes {
		if peer == node {
			continue
		}
		link := simnet.Link{From: netip.MustParseAddr(c.nodes[node].ip), To: netip.MustParseAddr(c.nodes[peer].ip), Direction: simnet.Both}
		var err error
		if isolate {
			err = simnet.Partition(link)
		} else {
			err = simnet.Repair(link)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Document edits are exercised through real Node APIs and replication, inside
// each simulated machine rather than a Docker fixture or root-package test.
func TestPublicLabelEditUsesDocumentWorkflow(t *testing.T) {
	configure(t, 0, 0)
	cluster := startCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var version, nextID uint64
	if err := cluster.nodes[cluster.leader(t)].call(ctx, func(ctx context.Context, n gquorum.Node) error {
		c := n.Cluster()
		view, err := c.ReadTopology(ctx)
		if err != nil {
			return err
		}
		edited := view.Latest.Clone()
		labels := edited.Node("node1").GetLabels()
		labels["network"] = "public_ip"
		if err := edited.Node("node1").SetLabels(labels); err != nil {
			return err
		}
		if err := edited.MetaGroup().SetLabels(map[string]string{"purpose": "accounting"}); err != nil {
			return err
		}
		edited.ChangedBy = "operator"
		prepared, err := topology.PreparePlacement(view, edited, c.Rules(), time.Now().UTC())
		if err != nil {
			return err
		}
		staged, err := c.Stage(ctx, prepared.Document, view.Latest.Version)
		if err != nil {
			return err
		}
		nextID = view.Latest.MetaGroup().NextReplicaID
		version = staged.Version
		if version != view.Latest.Version+1 || staged.Node("node1").GetLabels()["network"] != "public_ip" || staged.MetaGroup().GetLabels()["purpose"] != "accounting" || staged.MetaGroup().NextReplicaID != nextID {
			return fmt.Errorf("Stage changed prepared labels or replica allocation: %+v", staged)
		}
		operation, err := c.Approve(ctx, version)
		if err != nil {
			return err
		}
		_, err = c.WaitOperation(ctx, operation)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, node := range cluster.nodes {
		if err := node.call(ctx, func(ctx context.Context, n gquorum.Node) error {
			view, err := n.Cluster().ReadTopology(ctx)
			if err != nil {
				return err
			}
			for _, doc := range []topology.Document{view.Latest, view.Target} {
				if doc.Version != version || doc.Node("node1").GetLabels()["network"] != "public_ip" || doc.MetaGroup().GetLabels()["purpose"] != "accounting" || doc.MetaGroup().NextReplicaID != nextID {
					return fmt.Errorf("replicated document lost prepared labels or allocation: %+v", doc)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(node.name, err)
		}
	}
}
