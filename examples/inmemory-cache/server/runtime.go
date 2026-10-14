package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/examples/inmemory-cache/cache"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
	"github.com/asalimonov/gquorum/transport/loadgrpc"
	"github.com/asalimonov/gquorum/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const groupID = "cache"

// runtime owns process resources. Node owns the replica and its in-memory
// machine; the Redis server, gRPC server and borrowed transport close around
// it.
type runtime struct {
	cfg    config
	logger *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc
	errors chan error

	node      gquorum.Node
	transport *loadgrpc.Transport
	redis     *redisServer
	grpc      *grpc.Server
	listeners []net.Listener
	workers   sync.WaitGroup
}

func (r *runtime) Start(parent context.Context) error {
	r.ctx, r.cancel = context.WithCancel(parent)
	data, err := os.ReadFile(r.cfg.CredentialFile)
	if err != nil {
		return err
	}
	secret := strings.TrimSpace(string(data))
	credential, err := loadgrpc.NewSharedToken(secret, false)
	if err != nil {
		return err
	}
	peers := r.cfg.peers()
	// Node validates both peers of every incoming batch against approved
	// topology, so the shared credential is the only check needed here.
	r.transport, err = loadgrpc.New(loadgrpc.Options{
		Resolve:      peers.ResolveLoad,
		Authenticate: credential.Authenticate,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(
			insecure.NewCredentials()), grpc.WithPerRPCCredentials(credential)},
	})
	if err != nil {
		return err
	}
	var meta, loadListener, redisListener net.Listener
	for _, l := range []struct {
		target *net.Listener
		addr   string
	}{{&meta, ":" + strconv.Itoa(r.cfg.MetaPort)}, {&loadListener, ":" + strconv.Itoa(r.cfg.LoadPort)},
		{&redisListener, r.cfg.RedisListen}} {
		if *l.target, err = net.Listen("tcp", l.addr); err != nil {
			return err
		}
		r.listeners = append(r.listeners, *l.target)
	}
	founders, err := peers.Founders(r.cfg.Cluster, "cache")
	if err != nil {
		return err
	}
	identity := topology.Identity{NodeName: r.cfg.Node, ClusterID: r.cfg.Cluster, InstanceID: util.NewInstanceID()}
	var local topology.Node
	for _, founder := range founders {
		if founder.Node.Name == r.cfg.Node {
			local, identity.MemberID = founder.Node, founder.MemberID
		}
	}
	r.node, err = gquorum.NewNode(gquorum.NodeOptions{
		Identity:   identity,
		Node:       local,
		StorageDir: filepath.Join(r.cfg.Data, "system"),
		Bootstrap:  gquorum.BootstrapOptions{Founders: founders},
		Logger:     r.logger,
		Meta: gquorum.MetaOptions{Network: gquorum.MetaNetworkOptions{
			Listener: meta, AdvertiseAddress: peers[r.cfg.Node].MetaAddress, Resolver: peers,
			AllowPlaintext: true, ClusterCredential: []byte(secret),
		}},
		Loads: gquorum.LoadOptions{
			// The machine keeps no local files, so no Retire callback is
			// needed.
			Factory: func(context.Context, load.ReplicaSpec) (load.Application, error) {
				return load.Application{StateMachine: cache.NewMachine()}, nil
			},
			Transport: r.transport,
			// Persist the Raft log and snapshots; replay reconstructs the
			// cache. The demo can skip per-write fsync while keeping
			// OS-buffered files.
			Storage: gquorum.LoadStorageOptions{
				WAL: &gquorum.WALOptions{RootDir: filepath.Join(r.cfg.Data, "raft"), DisableFSync: r.cfg.DisableFSync},
			},
			// A snapshot every 4096 entries bounds the retained log; a replica
			// that falls further behind receives the leader's snapshot. Much
			// smaller thresholds spend CPU on serializing snapshots.
			Engine: load.EngineOptions{SnapshotThreshold: 4096},
		},
	})
	if err != nil {
		return err
	}
	if err := r.transport.Bind(r.node); err != nil {
		return err
	}
	r.grpc = grpc.NewServer(grpc.MaxRecvMsgSize(16<<20), grpc.MaxSendMsgSize(16<<20))
	r.transport.Register(r.grpc)
	r.redis = newRedisServer(r.localGroup, r.logger)
	r.goWorker(func() error { return r.grpc.Serve(loadListener) })
	r.goWorker(func() error { return r.redis.Serve(r.ctx, redisListener) })
	if err := r.node.Start(r.ctx); err != nil {
		return err
	}
	r.goWorker(func() error { return r.ensureGroup(r.ctx) })
	return nil
}

func (r *runtime) goWorker(work func() error) {
	r.workers.Go(func() {
		if err := work(); err != nil && r.ctx.Err() == nil {
			r.errors <- err
		}
	})
}

func (r *runtime) localGroup() (*load.Group, error) {
	if group, ok := r.node.LoadGroup(groupID); ok {
		return group, nil
	}
	return nil, fault.ErrReplicaNotReady
}

// ensureGroup makes the cache group Active with one voter per peer and
// disk-backed Raft storage. Every node runs it; EnsureGroup makes concurrent
// calls converge. A restarted node replays its snapshot and committed log under
// the same replica identity, then catches up from the leader.
func (r *runtime) ensureGroup(ctx context.Context) error {
	for !r.node.Status().Ready {
		if err := wait(ctx, 200*time.Millisecond); err != nil {
			return err
		}
	}
	voterConfig := topology.DefaultVoterConfig()
	voterConfig.TargetVoters = len(r.cfg.Peers)
	spec := gquorum.GroupSpec{
		ID:          groupID,
		Labels:      map[string]string{"example": "inmemory-cache"},
		VoterConfig: &voterConfig,
		Nodes:       r.cfg.Peers,
		Storage:     topology.StorageDefault,
	}
	_, err := fault.Retry(ctx, fault.RetryOptions{AttemptTimeout: time.Minute},
		func(call context.Context) (topology.Group, error) {
			return r.node.Cluster().
				EnsureGroup(call, spec, gquorum.EnsureOptions{Actor: "inmemory-cache-example"})
		})
	if err == nil {
		r.logger.Info("cache_group_ready", "node", r.cfg.Node)
	}
	return err
}

func (r *runtime) Stop(ctx context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	if r.redis != nil {
		if err := r.redis.Wait(ctx); err != nil {
			return err
		}
	}
	if r.node != nil {
		if err := r.node.Stop(ctx); err != nil {
			return err
		}
	}
	if r.transport != nil {
		if err := r.transport.Close(ctx); err != nil {
			return err
		}
	}
	if r.grpc != nil {
		r.grpc.Stop()
	}
	for _, l := range r.listeners {
		_ = l.Close()
	}
	r.workers.Wait()
	return nil
}

func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
