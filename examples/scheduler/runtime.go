package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/examples/scheduler/localdb"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
	"github.com/asalimonov/gquorum/transport/loadgrpc"
	"github.com/asalimonov/gquorum/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	groupID                 = "scheduler"
	workerErrorCapacity     = 4
	grpcStreamLimit         = 128
	serverReadHeaderTimeout = 3 * time.Second
	serverReadTimeout       = 5 * time.Second
	serverWriteTimeout      = 10 * time.Second
	serverIdleTimeout       = 30 * time.Second
)

// runtime owns process resources. Node owns the replica, its Run callback and
// SQLite lifetime; servers and the borrowed load transport close after Node.
type runtime struct {
	cfg    config
	logger *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc
	errors chan error

	secret    string
	node      gquorum.Node
	transport *loadgrpc.Transport
	service   atomic.Pointer[replicaService] // current ready replica, for HTTP

	grpc         *grpc.Server
	http         *http.Server
	metaListener net.Listener
	loadListener net.Listener
	httpListener net.Listener
	workers      sync.WaitGroup
}

func newRuntime(cfg config, logger *slog.Logger) (*runtime, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	log := logger
	if log == nil {
		log = slog.Default()
	}
	return &runtime{cfg: cfg, logger: log, errors: make(chan error, workerErrorCapacity)}, nil
}

func (r *runtime) Start(parent context.Context) error {
	r.ctx, r.cancel = context.WithCancel(parent)
	data, err := os.ReadFile(r.cfg.CredentialFile)
	if err != nil {
		return err
	}
	r.secret = strings.TrimSuffix(string(data), "\n")
	credential, err := loadgrpc.NewSharedToken(r.secret, false)
	if err != nil {
		return err
	}
	peers := r.cfg.peers()
	// Node validates both peers of each incoming batch against approved
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
	for _, listen := range []struct {
		target *net.Listener
		addr   string
	}{{&r.metaListener, ":" + strconv.Itoa(r.cfg.MetaPort)}, {&r.loadListener, ":" + strconv.Itoa(r.cfg.LoadPort)},
		{&r.httpListener, r.cfg.HTTPListen}} {
		if *listen.target, err = net.Listen("tcp", listen.addr); err != nil {
			return err
		}
	}
	if r.node, err = r.newNode(peers); err != nil {
		return err
	}
	if err := r.transport.Bind(r.node); err != nil {
		return err
	}
	r.grpc = grpc.NewServer(grpc.MaxRecvMsgSize(16<<20), grpc.MaxSendMsgSize(16<<20),
		grpc.MaxConcurrentStreams(grpcStreamLimit))
	r.transport.Register(r.grpc)
	r.http = &http.Server{
		Handler:           newHTTPHandler(r),
		ReadHeaderTimeout: serverReadHeaderTimeout,
		ReadTimeout:       serverReadTimeout,
		WriteTimeout:      serverWriteTimeout,
		IdleTimeout:       serverIdleTimeout,
		BaseContext:       func(net.Listener) context.Context { return r.ctx },
	}
	r.goWorker(func() error { return r.grpc.Serve(r.loadListener) })
	r.goWorker(func() error {
		if err := r.http.Serve(r.httpListener); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	if err := r.node.Start(r.ctx); err != nil {
		return err
	}
	r.goWorker(func() error { return r.ensureGroup(r.ctx) })
	return nil
}

// goWorker runs a process-owned goroutine and reports unexpected failures.
func (r *runtime) goWorker(work func() error) {
	r.workers.Go(func() {
		if err := work(); err != nil && r.ctx.Err() == nil {
			r.errors <- err
		}
	})
}

func (r *runtime) newNode(peers gquorum.StaticPeers) (gquorum.Node, error) {
	founders, err := peers.Founders(r.cfg.Cluster, "worker")
	if err != nil {
		return nil, err
	}
	identity := topology.Identity{NodeName: r.cfg.Node, ClusterID: r.cfg.Cluster, InstanceID: util.NewInstanceID()}
	var local topology.Node
	for _, founder := range founders {
		if founder.Node.Name == r.cfg.Node {
			local, identity.MemberID = founder.Node, founder.MemberID
		}
	}
	return gquorum.NewNode(gquorum.NodeOptions{
		Identity:   identity,
		Node:       local,
		StorageDir: filepath.Join(r.cfg.Data, "system"),
		Bootstrap:  gquorum.BootstrapOptions{Founders: founders},
		Logger:     r.logger,
		Meta: gquorum.MetaOptions{Network: gquorum.MetaNetworkOptions{
			Listener:          r.metaListener,
			AdvertiseAddress:  peers[r.cfg.Node].MetaAddress,
			Resolver:          peers,
			AllowPlaintext:    true,
			ClusterCredential: []byte(r.secret),
		}},
		Loads: gquorum.LoadOptions{
			Factory:   r.newApplication,
			Retire:    r.retireApplication,
			Transport: r.transport,
			Storage:   gquorum.LoadStorageOptions{WAL: &gquorum.WALOptions{RootDir: filepath.Join(r.cfg.Data, "raft")}},
			Engine: load.EngineOptions{
				SnapshotThreshold: 1000,
				Reads:             load.ReadOptions{MaxResultBytes: localdb.MaxSnapshotBytes},
			},
		},
	})
}

// schedulerGroup declares one voter on every configured peer.
func schedulerGroup(peers []string) gquorum.GroupSpec {
	voterConfig := topology.DefaultVoterConfig()
	voterConfig.TargetVoters = len(peers)
	return gquorum.GroupSpec{
		ID:          groupID,
		Labels:      map[string]string{"example": "scheduler", "schema": "2"},
		VoterConfig: &voterConfig,
		Nodes:       peers,
	}
}

// ensureGroup waits for this node to be ready, then makes the scheduler group
// Active. Every node runs it; EnsureGroup makes concurrent calls converge.
func (r *runtime) ensureGroup(ctx context.Context) error {
	for !r.node.Status().Ready {
		if err := waitContext(ctx, 200*time.Millisecond); err != nil {
			return err
		}
	}
	_, err := fault.Retry(ctx, fault.RetryOptions{AttemptTimeout: time.Minute},
		func(call context.Context) (topology.Group, error) {
			return r.node.Cluster().
				EnsureGroup(call, schedulerGroup(r.cfg.Peers), gquorum.EnsureOptions{Actor: "scheduler-example"})
		})
	if err == nil {
		r.logger.Info("scheduler_topology_ready", "node", r.cfg.Node)
	}
	return err
}

func (r *runtime) Stop(ctx context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}
	if r.http != nil {
		if err := r.http.Shutdown(ctx); err != nil {
			return err
		}
	}
	if r.node != nil {
		if err := r.node.Stop(ctx); err != nil {
			return err
		}
	}
	// Node has joined every callback; the borrowed transport can now close.
	if r.transport != nil {
		if err := r.transport.Close(ctx); err != nil {
			return err
		}
	}
	if r.grpc != nil {
		r.grpc.Stop()
	}
	for _, listener := range []net.Listener{r.metaListener, r.loadListener, r.httpListener} {
		if listener != nil {
			_ = listener.Close()
		}
	}
	r.workers.Wait()
	return nil
}

func (r *runtime) newApplication(ctx context.Context, spec load.ReplicaSpec) (load.Application, error) {
	if spec.Identity.GroupID != groupID {
		return load.Application{}, errors.New("unsupported application group")
	}
	machine, err := localdb.Open(ctx, filepath.Join(r.cfg.Data, "application"), spec.Identity)
	if err != nil {
		return load.Application{}, err
	}
	service := newReplicaService(r, spec)
	return load.Application{StateMachine: machine, Run: service.Run, Close: machine.Close}, nil
}

func (r *runtime) retireApplication(ctx context.Context, req load.RetirementRequest) (load.RetirementResult, error) {
	return localdb.Retire(ctx, filepath.Join(r.cfg.Data, "application"), req)
}
