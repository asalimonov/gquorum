//go:build sim

package sim_test

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/topology"
	"github.com/neria-cloud/gosim"
	simfs "github.com/neria-cloud/gosim/fs"
	simnet "github.com/neria-cloud/gosim/net"
)

type nodeCall struct {
	ctx  context.Context
	run  func(context.Context, gquorum.Node) error
	done chan error
}

type simNode struct {
	name, ip      string
	requests      chan nodeCall
	stop          chan struct{}
	ready, exited chan error
}

type simCluster struct{ nodes [3]*simNode }

type addresses map[string]string

func (a addresses) Resolve(ctx context.Context, name string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	address, ok := a[name]
	if !ok {
		return "", fmt.Errorf("unknown peer %q", name)
	}
	return address, nil
}

func startCluster(t *testing.T) *simCluster {
	t.Helper()
	c := new(simCluster)
	founders := make([]gquorum.FoundingMember, len(c.nodes))
	resolve := addresses{}
	for i := range c.nodes {
		name, ip := fmt.Sprintf("node%d", i+1), fmt.Sprintf("10.77.0.%d", i+1)
		resolve[name] = net.JoinHostPort(ip, "9000")
		founders[i] = gquorum.FoundingMember{Node: topology.Node{Name: name, Type: "worker", State: topology.NodeActive}, MemberID: uint64(i + 1), Address: resolve[name]}
		c.nodes[i] = &simNode{name: name, ip: ip, requests: make(chan nodeCall, 64), stop: make(chan struct{}), ready: make(chan error, 1), exited: make(chan error, 1)}
	}
	for i, node := range c.nodes {
		machine, err := gosim.NewMachine(gosim.MachineConfig{Label: node.name, Main: func() { node.main(i, founders, resolve) }})
		if err != nil {
			t.Fatal(err)
		}
		if err := simfs.ConfigureMachine(machine, simfs.DefaultConfig()); err != nil {
			t.Fatal(err)
		}
		if err := simnet.ConfigureMachine(machine, simnet.MachineConfig{Addr: netip.MustParseAddr(node.ip)}); err != nil {
			t.Fatal(err)
		}
		if err := machine.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			close(node.stop)
			select {
			case err := <-node.exited:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(15 * time.Second):
				t.Errorf("%s did not join shutdown", node.name)
				if err := machine.Stop(); err != nil {
					t.Error(err)
				}
			}
			if _, err := machine.Wait(); err != nil {
				t.Error(err)
			}
			if err := machine.Destroy(); err != nil {
				t.Error(err)
			}
		})
	}
	for _, node := range c.nodes {
		select {
		case err := <-node.ready:
			if err != nil {
				t.Fatalf("%s startup: %v", node.name, err)
			}
		case <-time.After(90 * time.Second):
			t.Fatalf("%s startup timed out", node.name)
		}
	}
	return c
}

func (s *simNode) main(index int, founders []gquorum.FoundingMember, resolve addresses) {
	err := s.serve(index, founders, resolve)
	s.exited <- err
}

func (s *simNode) serve(index int, founders []gquorum.FoundingMember, resolve addresses) (err error) {
	listener, err := net.Listen("tcp", resolve[s.name])
	if err != nil {
		s.ready <- err
		return err
	}
	defer listener.Close()
	// The same path belongs to a different simulated filesystem on each
	// machine. No host directory or borrowed in-memory store is involved.
	n, err := gquorum.NewNode(gquorum.NodeOptions{
		StorageDir: "/app/state",
		Identity:   topology.Identity{NodeName: s.name, MemberID: uint64(index + 1), ClusterID: "simulation", InstanceID: s.name + "-boot1"},
		Node:       founders[index].Node,
		Bootstrap:  gquorum.BootstrapOptions{Founders: founders},
		Scheduling: scheduling.Options{Disabled: true},
		Meta: gquorum.MetaOptions{
			Network: gquorum.MetaNetworkOptions{Listener: listener, AdvertiseAddress: resolve[s.name], Resolver: resolve, AllowPlaintext: true, ClusterCredential: []byte("simulation-cluster-credential")},
			// A 3s election minimum and 2s peer calls accommodate 400ms RTT
			// plus forwarding. These are application settings, not library overrides.
			Timing: gquorum.MetaTiming{TickInterval: 100 * time.Millisecond, HeartbeatTicks: 1, ElectionTicks: 30, DialTimeout: 2 * time.Second, SendTimeout: 2 * time.Second, PeerRetryInitial: 100 * time.Millisecond, PeerRetryMax: 500 * time.Millisecond},
		},
	})
	if err != nil {
		s.ready <- err
		return err
	}
	lifetime, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-s.stop:
			cancel()
		case <-lifetime.Done():
		}
	}()
	defer func() {
		ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if stopErr := n.Stop(ctx); err == nil {
			err = stopErr
		}
	}()
	if err := n.Start(lifetime); err != nil {
		s.ready <- err
		return err
	}
	ctx, stop := context.WithTimeout(lifetime, 80*time.Second)
	err = n.WaitReady(ctx)
	stop()
	if err != nil {
		err = fmt.Errorf("wait for readiness: %w; status=%+v", err, n.Status())
	}
	if err == nil {
		_, err = os.Stat("/app/state/_meta.bolt")
	}
	s.ready <- err
	if err != nil {
		return err
	}
	var calls sync.WaitGroup
	defer calls.Wait()
	for {
		select {
		case <-s.stop:
			return nil
		case request := <-s.requests:
			calls.Add(1)
			go func() {
				defer calls.Done()
				request.done <- request.run(request.ctx, n)
			}()
		}
	}
}

// Controller channels only invoke a local application API in the target
// machine. No Node handle leaves its machine; all Raft traffic uses real gRPC.
func (s *simNode) call(ctx context.Context, run func(context.Context, gquorum.Node) error) error {
	request := nodeCall{ctx: ctx, run: run, done: make(chan error, 1)}
	select {
	case s.requests <- request:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-request.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *simCluster) leader(t *testing.T) int {
	t.Helper()
	for i, node := range c.nodes {
		leader := false
		ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
		err := node.call(ctx, func(_ context.Context, n gquorum.Node) error { leader = n.Cluster().IsLeader(); return nil })
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if leader {
			return i
		}
	}
	t.Fatal("ready cluster has no leader")
	return -1
}

func (c *simCluster) write(t *testing.T, writer int, key string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()
	if err := c.nodes[writer].call(ctx, func(ctx context.Context, n gquorum.Node) error {
		session, err := n.ActivateSession(ctx)
		if err != nil {
			return err
		}
		return n.Cluster().AcquireLock(ctx, key, session.ID)
	}); err != nil {
		t.Fatalf("%s commit %s: %v", c.nodes[writer].name, key, err)
	}
}

func (c *simCluster) writeAndCheck(t *testing.T, writer int, key string) {
	t.Helper()
	c.write(t, writer, key)
	for _, node := range c.nodes {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := node.call(ctx, func(ctx context.Context, n gquorum.Node) error {
			holder, err := n.Cluster().ReadLockHolder(ctx, key)
			if err != nil {
				return err
			}
			if !holder.Held || holder.Session.NodeName != c.nodes[writer].name {
				return fmt.Errorf("wrong lock holder: %+v", holder)
			}
			return nil
		})
		cancel()
		if err != nil {
			t.Fatalf("%s read committed %s: %v", node.name, key, err)
		}
	}
}

// Recovery is asynchronous after link repair. This bounded wait applies only
// to the returning minority, never to the uninterrupted majority monitor.
func (c *simCluster) waitRejoined(t *testing.T, index int) {
	t.Helper()
	recovery, stop := context.WithTimeout(context.Background(), 15*time.Second)
	defer stop()
	var last error
	for recovery.Err() == nil {
		ctx, cancel := context.WithTimeout(recovery, probeTimeout)
		last = c.nodes[index].call(ctx, func(ctx context.Context, n gquorum.Node) error {
			return n.Cluster().ReadBarrier(ctx)
		})
		cancel()
		if last == nil {
			return
		}
		time.Sleep(probeInterval)
	}
	t.Fatalf("%s failed to rejoin after repair: %v", c.nodes[index].name, last)
}
