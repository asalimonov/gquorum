package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/storage/walstore"
	"github.com/asalimonov/gquorum/tests/cluster/testapp/loadpb"
	"github.com/asalimonov/gquorum/topology"
	"google.golang.org/grpc"
)

const (
	loadAddressLabel = "example.load_address"
	metaAddressLabel = "example.meta_address"
	httpAddressLabel = "example.http_address"
)

type listeners struct{ meta, load, http net.Listener }
type localReplica struct {
	group   *load.Group
	machine *kvMachine
}
type application struct {
	loadpb.UnimplementedLoadServer
	cfg                          config
	logger                       *slog.Logger
	node                         gquorum.Node
	nodeOptions                  gquorum.NodeOptions
	generationMu                 sync.RWMutex
	nodeOpMu                     sync.Mutex
	generation                   *nodeGeneration
	cluster                      gquorum.Cluster
	store                        *walstore.Store
	journal                      *events.Journal
	bus                          *events.Bus
	httpListener, loadListener   net.Listener
	httpServer                   *http.Server
	loadServer                   *grpc.Server
	metaProxy                    *metaFaultProxy
	mu                           sync.RWMutex
	machines                     map[load.ReplicaRef]*kvMachine
	serving                      map[string]topology.GroupProgress
	startupMissing               string
	startupObserved              bool
	clients                      map[string]*loadClient
	workCtx, lifeCtx, httpCtx    context.Context
	stopWork, stopLife, stopHTTP context.CancelFunc
	workWG, serveWG              sync.WaitGroup
	effectMu                     sync.Mutex
	effectWG                     sync.WaitGroup
	startMu                      sync.Mutex
	started                      bool
	closing, closed              chan struct{}
	closeOnce                    sync.Once
	closeErr                     error
	clusterHooks                 *clusterHooks
	fatalMu                      sync.Mutex
	fatalErr                     error
	callerScheduler              *fixtureScheduler
}

func nodeForPeer(p peer) topology.Node {
	return topology.Node{Name: p.Name, Type: p.Type, Labels: map[string]string{loadAddressLabel: p.LoadAddress, metaAddressLabel: p.MetaAddress, httpAddressLabel: p.HTTPAddress}}
}
func (c config) nodeForPeer(p peer) topology.Node {
	node := nodeForPeer(p)
	if c.AddressFree {
		node.Labels = nil
	}
	return node
}

// The caller retains a cluster-affiliation marker separately from Raft storage.
// Retained stores resume their pin; missing stores on established nodes join.
// Malformed or foreign markers fail closed; this is fixture startup lifecycle.
func (c *config) recoverBootMode() (bool, error) {
	pin, err := gquorum.LoadStoredIdentity(c.DataDir)
	if err != nil {
		return false, err
	}
	if pin.Found {
		if pin.Identity.ClusterID != c.ClusterID || pin.Identity.NodeName != c.NodeName {
			return false, fault.ErrIdentityMismatch
		}
		// Resume directly, without founding declarations that could conflict with a
		// readmitted pin. Explicit joining requests retain their admission semantics.
		return !c.Join, nil
	}
	raw, err := os.ReadFile(filepath.Join(c.DataDir, "established.json"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var marker struct{ ClusterID, NodeName string }
	if err := decodeJSON(raw, &marker); err != nil {
		return false, err
	}
	if marker.ClusterID != c.ClusterID || marker.NodeName != c.NodeName {
		return false, fault.ErrIdentityMismatch
	}
	c.Join = true
	return false, nil
}
func (a *application) markEstablished() {
	a.markEstablishedFor(a.workCtx, a.currentNode())
}
func (a *application) markEstablishedFor(ctx context.Context, n gquorum.Node) {
	if err := n.WaitReady(ctx); err != nil {
		status := n.Status()
		if errors.Is(err, fault.ErrStopped) && status.State == gquorum.NodeFenced && status.SelfRemoval != nil && status.SelfRemoval.Fenced &&
			status.SelfRemoval.MemberID == status.Identity.MemberID && status.SelfRemoval.Version != 0 && status.SelfRemoval.AppliedIndex != 0 {
			// An admission can be superseded before serving. Keep authenticated
			// control available until the library finishes required retirement
			// and invokes the configured removal lifecycle.
			return
		}
		select {
		case <-a.closing:
		default:
			if ctx.Err() == nil {
				a.fail(err)
			}
		}
		return
	}
	if err := a.persistEstablished(); err != nil {
		a.fail(err)
	}
}
func (a *application) persistEstablished() error {
	raw, err := json.Marshal(struct{ ClusterID, NodeName string }{a.cfg.ClusterID, a.cfg.NodeName})
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(a.cfg.DataDir, ".established-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, err = file.Write(raw)
	err = errors.Join(err, file.Sync(), file.Close())
	if err != nil {
		return err
	}
	if err := os.Rename(file.Name(), filepath.Join(a.cfg.DataDir, "established.json")); err != nil {
		return err
	}
	dir, err := os.Open(a.cfg.DataDir)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
func newApplication(cfg config, logger *slog.Logger, supplied *listeners) (_ *application, err error) {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.NodeName == "" || cfg.ClusterID == "" || cfg.InstanceID == "" || cfg.DataDir == "" || cfg.Credential == "" || cfg.NodeType == "" {
		return nil, fault.New(fault.Invalid, "node, type, cluster, process, data path and credential required", nil)
	}
	cfg.Peers = slices.Clone(cfg.Peers)
	if cfg.MetaVoterConfig != nil {
		v := *cfg.MetaVoterConfig
		cfg.MetaVoterConfig = &v
	}
	cfg.Limits = cfg.Limits.WithDefaults()
	if err = cfg.Limits.Validate(); err != nil {
		return nil, err
	}
	if cfg.WALWriteWait < 0 || cfg.WALWriteWait > 10*time.Millisecond {
		return nil, fault.New(fault.Invalid, "load WAL coalescing wait must be between zero and 10ms", nil)
	}
	if cfg.FoundingTimeout < 0 {
		return nil, fault.New(fault.Invalid, "negative founding timeout", nil)
	}
	for _, p := range cfg.Peers {
		if p.Name == cfg.NodeName {
			if p.Type != cfg.NodeType {
				return nil, fault.New(fault.IdentityMismatch, "local type differs from manifest", nil)
			}
			if cfg.MetaAddress == "" {
				cfg.MetaAddress = p.MetaAddress
			}
			if cfg.LoadAddress == "" {
				cfg.LoadAddress = p.LoadAddress
			}
			if cfg.HTTPAddress == "" {
				cfg.HTTPAddress = p.HTTPAddress
			}
		}
	}
	if cfg.MetaAddress == "" || cfg.LoadAddress == "" || cfg.HTTPAddress == "" {
		return nil, fault.New(fault.Invalid, "advertised addresses required", nil)
	}
	if cfg.AdmissionPinPhase != "" {
		if err := cfg.prepareAdmissionPin(); err != nil {
			return nil, err
		}
	}
	resume, err := cfg.recoverBootMode()
	if err != nil {
		return nil, err
	}
	a := &application{cfg: cfg, logger: logger, machines: map[load.ReplicaRef]*kvMachine{}, clients: map[string]*loadClient{}, closing: make(chan struct{}), closed: make(chan struct{})}
	a.workCtx, a.stopWork = context.WithCancel(context.Background())
	a.clusterHooks = newClusterHooks()
	a.lifeCtx, a.stopLife = context.WithCancel(context.Background())
	a.httpCtx, a.stopHTTP = context.WithCancel(context.Background())
	var ls listeners
	if supplied != nil {
		ls = *supplied
	}
	defer func() {
		if err != nil {
			_ = a.Close(context.Background())
			if ls.meta != nil {
				_ = ls.meta.Close()
			}
			if ls.load != nil {
				_ = ls.load.Close()
			}
			if ls.http != nil {
				_ = ls.http.Close()
			}
		}
	}()
	if ls.meta == nil {
		if ls.meta, err = net.Listen("tcp", cfg.MetaListen); err != nil {
			return nil, err
		}
	}
	if ls.load == nil {
		if ls.load, err = net.Listen("tcp", cfg.LoadListen); err != nil {
			return nil, err
		}
	}
	if ls.http == nil {
		if ls.http, err = net.Listen("tcp", cfg.HTTPListen); err != nil {
			return nil, err
		}
	}
	a.httpListener, a.loadListener = ls.http, ls.load
	a.journal, err = events.OpenJournal(events.JournalOptions{Path: filepath.Join(cfg.DataDir, "events.bolt"), Logger: logger})
	if err != nil {
		return nil, err
	}
	a.bus, err = events.NewBus(events.BusOptions{Journal: a.journal, Logger: logger})
	if err != nil {
		return nil, err
	}
	prewrite := false
	if !cfg.NodeOwnedWAL {
		a.store, err = walstore.Open(walstore.Options{RootDir: filepath.Join(cfg.DataDir, "load"), NodeName: cfg.NodeName, ClusterID: cfg.ClusterID, SegmentBytes: 1 << 20, Prewrite: &prewrite, Logger: logger})
		if err != nil {
			return nil, err
		}
	}
	var founders []gquorum.FoundingMember
	var seeds []string
	var member uint64
	for _, p := range cfg.Peers {
		if cfg.Join {
			seeds = append(seeds, p.MetaAddress)
		} else if !resume {
			founders = append(founders, gquorum.FoundingMember{Node: cfg.nodeForPeer(p), MemberID: p.MemberID, Address: p.MetaAddress})
		}
		if p.Name == cfg.NodeName && !cfg.Join && !resume {
			member = p.MemberID
		}
	}
	placement := topology.PlacementRule{MinReplicas: min(2, len(cfg.Peers)), MaxReplicas: min(3, len(cfg.Peers))}
	if placement.MinReplicas == 0 {
		placement = topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3}
	}
	lifecycleOptions := lifecycle.DefaultOptions{Retention: cfg.Retention}
	if cfg.GateFounders {
		lifecycleOptions.Startup = lifecycle.AllFounders
		lifecycleOptions.FoundersTimeout = cfg.FoundingTimeout
	}
	defaults, err := lifecycle.NewDefaultHooks(lifecycleOptions)
	if err != nil {
		return nil, err
	}
	var scheduler scheduling.RepairPolicy
	if cfg.CallerScheduler {
		a.callerScheduler = &fixtureScheduler{reason: "caller-policy-" + cfg.NodeName}
		scheduler = a.callerScheduler
	}
	if cfg.MetaFaultProxy {
		ls.meta, err = a.newMetaFaultProxy(ls.meta)
		if err != nil {
			return nil, err
		}
	}
	scheduleOptions := cfg.Scheduling
	scheduleOptions.Policy = scheduler
	a.nodeOptions = gquorum.NodeOptions{
		StorageDir: cfg.DataDir,
		Identity:   topology.Identity{NodeName: cfg.NodeName, ClusterID: cfg.ClusterID, InstanceID: cfg.InstanceID, MemberID: member},
		Node:       cfg.nodeForPeer(peer{Name: cfg.NodeName, Type: cfg.NodeType, MetaAddress: cfg.MetaAddress, LoadAddress: cfg.LoadAddress, HTTPAddress: cfg.HTTPAddress}),
		Bootstrap:  gquorum.BootstrapOptions{Founders: founders, Join: cfg.Join, JoinSeeds: seeds},
		Limits:     cfg.Limits, Placement: placement, Events: a.bus, Logger: logger,
		Meta: gquorum.MetaOptions{SessionTTL: cfg.SessionTTL, VoterConfig: cfg.MetaVoterConfig,
			Timing:  gquorum.MetaTiming{TickInterval: cfg.TickInterval, ElectionTicks: cfg.ElectionTicks},
			Network: gquorum.MetaNetworkOptions{Listener: ls.meta, AdvertiseAddress: cfg.MetaAddress, AllowPlaintext: true, ClusterCredential: []byte(cfg.Credential), Resolver: metaResolver{a}}},
		Loads: gquorum.LoadOptions{Factory: a.newReplica, Retire: a.retireReplica, Transport: loadTransport{app: a},
			// Process tests exercise two collection passes across a crash. Keep
			// maintenance prompt; retention still decides whether deletion is due.
			RetirementInterval: 250 * time.Millisecond,
			Storage:            a.loadStorageOptions(),
			Engine:             load.EngineOptions{Timing: load.Timing{TickInterval: cfg.TickInterval, ElectionTicks: cfg.ElectionTicks, WALWriteWait: cfg.WALWriteWait}, SnapshotThreshold: cfg.SnapshotThreshold, ProposalTimeout: cfg.ProposalTimeout}},
		Scheduling: scheduleOptions,
		Metrics:    cfg.Metrics,
		Lifecycle:  lifecycle.Options{Hooks: &fixtureLifecycle{DefaultHooks: defaults, app: a}},
	}

	a.node, err = gquorum.NewNode(a.nodeOptions)
	if err != nil {
		return nil, err
	}
	if cfg.AdmissionPinPhase != "" {
		_ = ls.meta.Close()
	}
	a.cluster = a.currentNode().Cluster()
	a.generation = a.makeNodeGeneration(a.node, a.nodeOptions.Meta.Network.Listener)
	a.loadServer = grpc.NewServer(grpc.MaxRecvMsgSize(64<<20), grpc.MaxSendMsgSize(64<<20), grpc.MaxConcurrentStreams(128), grpc.WaitForHandlers(true))
	loadpb.RegisterLoadServer(a.loadServer, a)
	a.httpServer = &http.Server{Handler: a.routes(), ReadHeaderTimeout: 3 * time.Second, IdleTimeout: 30 * time.Second, BaseContext: func(net.Listener) context.Context { return a.httpCtx }}
	return a, nil
}

func (a *application) Start(parent context.Context) error {
	a.startMu.Lock()
	defer a.startMu.Unlock()
	if err := parent.Err(); err != nil {
		return err
	}
	select {
	case <-a.closing:
		return fault.ErrStopped
	default:
	}
	if a.started {
		return fault.ErrStopped
	}
	a.started = true
	a.serveWG.Go(func() {
		if err := a.loadServer.Serve(a.loadListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			a.logger.Error("load server stopped", "error", err)
			a.RequestClose()
		}
	})
	a.serveWG.Go(func() {
		if err := a.httpServer.Serve(a.httpListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.logger.Error("HTTP server stopped", "error", err)
			a.RequestClose()
		}
	})
	if a.cfg.AdmissionPinPhase != "" {
		return nil
	}
	if a.metaProxy != nil {
		a.serveWG.Go(func() {
			if err := a.metaProxy.server.Serve(a.metaProxy.listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				a.logger.Error("metadata proxy stopped", "error", err)
				a.RequestClose()
			}
		})
	}
	if err := a.startNodeGeneration(); err != nil {
		a.RequestClose()
		return err
	}
	a.workWG.Go(func() {
		select {
		case <-parent.Done():
			a.RequestClose()
		case <-a.workCtx.Done():
		}
	})

	return nil
}
func (a *application) RequestClose() {
	a.closeOnce.Do(func() { a.effectMu.Lock(); close(a.closing); a.effectMu.Unlock(); go a.finishClose() })
}
func (a *application) fail(err error) {
	a.fatalMu.Lock()
	if a.fatalErr == nil {
		a.fatalErr = err
	}
	a.fatalMu.Unlock()
	a.logger.Error("caller startup failed", "error", err)
	a.RequestClose()
}
func (a *application) Close(ctx context.Context) error {
	a.RequestClose()
	select {
	case <-a.closed:
		return a.closeErr
	case <-ctx.Done():
		return fault.New(fault.ShutdownTimeout, "example shutdown still owns its resources", ctx.Err())
	}
}
func (a *application) finishClose() {
	a.startMu.Lock()
	a.stopWork()
	a.stopHTTP()
	a.startMu.Unlock()
	remember := func(err error) {
		if err != nil && a.closeErr == nil {
			a.closeErr = err
		}
	}
	a.workWG.Wait()
	a.effectWG.Wait()
	if a.currentNode() != nil {
		remember(a.stopNodeGeneration(context.Background()))
	}
	if a.metaProxy != nil {
		a.metaProxy.server.Stop()
		_ = a.metaProxy.listener.Close()
		_ = a.metaProxy.internal.Close()
		remember(a.metaProxy.conn.Close())
	}
	a.stopLife()
	if a.loadServer != nil {
		a.loadServer.Stop()
	}
	if a.loadListener != nil {
		_ = a.loadListener.Close()
	}
	if a.httpServer != nil {
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		remember(a.httpServer.Shutdown(shutdown))
		cancel()
		_ = a.httpServer.Close()
	}
	if a.httpListener != nil {
		_ = a.httpListener.Close()
	}
	a.serveWG.Wait()
	a.mu.Lock()
	for _, client := range a.clients {
		remember(client.conn.Close())
	}
	a.clients = nil
	a.mu.Unlock()
	if a.store != nil {
		remember(a.store.Close())
	}
	if a.bus != nil {
		remember(a.bus.Close(context.Background()))
	}
	if a.journal != nil {
		remember(a.journal.Close())
	}
	a.fatalMu.Lock()
	remember(a.fatalErr)
	a.fatalMu.Unlock()
	close(a.closed)
}

// readConvergedTopology resolves the pinned complete historical baseline;
// current per-group convergence is exposed separately through ReadProgress.
func readConvergedTopology(ctx context.Context, cluster gquorum.Cluster) (topology.Document, error) {
	view, err := cluster.ReadTopology(ctx)
	if err != nil {
		return topology.Document{}, err
	}
	return cluster.Get(ctx, view.FullyConverged.Version)
}

func (a *application) address(ctx context.Context, name, label string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if a.currentCluster() != nil {
		// Routing must work before election and during metadata quorum loss.
		// The receiver validates identity; resolving an address grants no authority.
		target, err := a.currentCluster().Target(ctx)
		if node := target.Node(name); err == nil && node != nil && node.Labels[label] != "" {
			return node.Labels[label], nil
		}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	for _, p := range a.cfg.Peers {
		if p.Name == name {
			switch label {
			case loadAddressLabel:
				return p.LoadAddress, nil
			case metaAddressLabel:
				return p.MetaAddress, nil
			case httpAddressLabel:
				return p.HTTPAddress, nil
			}
		}
	}
	return "", fault.New(fault.Unavailable, fmt.Sprintf("no address for node %s", name), nil)
}

type metaResolver struct{ a *application }

func (r metaResolver) Resolve(ctx context.Context, name string) (string, error) {
	return r.a.address(ctx, name, metaAddressLabel)
}
func (a *application) owner(ctx context.Context, group string, replica uint64) (string, error) {
	doc, err := a.approvedRoute(ctx, func(doc topology.Document) bool {
		return replicaOwner(doc.Group(group), replica) != ""
	})
	if err != nil {
		return "", err
	}
	return replicaOwner(doc.Group(group), replica), nil
}

func (a *application) bootstrapContext(parent context.Context, n gquorum.Node) {
	if err := n.WaitReady(parent); err != nil {
		return
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if a.currentCluster().IsLeader() {
			ctx, cancel := context.WithTimeout(parent, 5*time.Second)
			done, err := a.bootstrapOnce(ctx)
			cancel()
			if done {
				return
			}
			if err != nil && parent.Err() == nil {
				a.logger.Debug("demo bootstrap pending", "error", err)
			}
		}
		select {
		case <-parent.Done():
			return
		case <-ticker.C:
		}
	}
}
func (a *application) bootstrapOnce(ctx context.Context) (bool, error) {
	workersConfig := topology.DefaultVoterConfig()
	workersConfig.TargetVoters = 2
	for _, spec := range []gquorum.GroupSpec{{ID: "demo"}, {ID: "workers", VoterConfig: &workersConfig, Placement: &topology.PlacementRule{MinReplicas: 1, MaxReplicas: 2, AllowedNodeTypes: []topology.NodeType{"worker"}}}} {
		latest, err := a.currentCluster().Latest(ctx)
		if err != nil {
			return false, err
		}
		if latest.Group(spec.ID) == nil {
			created, err := a.currentCluster().CreateGroup(ctx, spec, gquorum.MutationOptions{ExpectedVersion: latest.Version, Actor: a.cfg.NodeName, Approve: true})
			if errors.Is(err, fault.ErrGroupRetired) {
				continue
			}
			if err != nil {
				return false, err
			}
			status, err := a.currentCluster().WaitOperation(ctx, created.OperationID)
			if err != nil {
				return false, err
			}
			if status.State != topology.OperationSucceeded {
				return false, fault.New(fault.Unavailable, status.Error, nil)
			}
		} else {
			active, err := readConvergedTopology(ctx, a.currentCluster())
			if err != nil {
				return false, err
			}
			if active.Group(spec.ID) == nil {
				id, err := a.currentCluster().Approve(ctx, latest.Version)
				if err != nil {
					return false, err
				}
				status, err := a.currentCluster().WaitOperation(ctx, id)
				if err != nil || status.State != topology.OperationSucceeded {
					return false, err
				}
			}
		}
	}
	return true, nil
}
