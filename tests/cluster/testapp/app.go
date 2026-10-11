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
	"sync"
	"time"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/membership"
	"github.com/asalimonov/gquorum/meta"
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
	runtime                      *gquorum.Runtime
	meta                         *meta.Group
	store                        *walstore.Store
	journal                      *events.Journal
	bus                          *events.Bus
	httpListener, loadListener   net.Listener
	httpServer                   *http.Server
	loadServer                   *grpc.Server
	mu                           sync.RWMutex
	locals                       map[string]*localReplica
	clients                      map[string]*loadClient
	localGate                    chan struct{}
	coordinator                  *membership.Coordinator
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
	foundingReady                chan struct{}
	foundingGate                 bool
	fatalMu                      sync.Mutex
	fatalErr                     error
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
// Malformed or foreign markers fail closed; this is fixture startup policy.
func (c *config) recoverBootMode() (bool, error) {
	pin, found, err := meta.LoadStoredIdentity(filepath.Join(c.DataDir, "meta.bolt"))
	if err != nil {
		return false, err
	}
	if found {
		if pin.ClusterID != c.ClusterID || pin.NodeName != c.NodeName {
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
	if err := a.meta.WaitReady(a.workCtx); err != nil {
		select {
		case <-a.closing:
		default:
			if a.workCtx.Err() == nil {
				a.fail(err)
			}
		}
		return
	}
	if a.foundingGate {
		// The gated worker owns the marker and opens readiness only after sync.
		_ = a.waitFounders(a.workCtx)
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
	resume, err := cfg.recoverBootMode()
	if err != nil {
		return nil, err
	}
	a := &application{cfg: cfg, logger: logger, locals: map[string]*localReplica{}, clients: map[string]*loadClient{}, localGate: make(chan struct{}, 1), coordinator: membership.NewCoordinator(), closing: make(chan struct{}), closed: make(chan struct{})}
	a.foundingReady = make(chan struct{})
	if cfg.GateFounders && !cfg.Join {
		raw, markerErr := os.ReadFile(filepath.Join(cfg.DataDir, "established.json"))
		if markerErr == nil {
			var marker struct{ ClusterID, NodeName string }
			if decodeJSON(raw, &marker) != nil || marker.ClusterID != cfg.ClusterID || marker.NodeName != cfg.NodeName {
				return nil, fault.ErrIdentityMismatch
			}
		} else if errors.Is(markerErr, os.ErrNotExist) {
			a.foundingGate = true
		} else {
			return nil, markerErr
		}
	}
	if !a.foundingGate {
		close(a.foundingReady)
	}
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
	a.store, err = walstore.Open(walstore.Options{RootDir: filepath.Join(cfg.DataDir, "load"), NodeName: cfg.NodeName, ClusterID: cfg.ClusterID, SegmentBytes: 1 << 20, Prewrite: &prewrite, Logger: logger})
	if err != nil {
		return nil, err
	}
	a.runtime, err = gquorum.New(gquorum.Options{Events: a.bus, Logger: logger})
	if err != nil {
		return nil, err
	}
	var founders []meta.FoundingMember
	var seeds []string
	var member uint64
	for _, p := range cfg.Peers {
		if cfg.Join {
			seeds = append(seeds, p.MetaAddress)
		} else if !resume {
			founders = append(founders, meta.FoundingMember{Node: cfg.nodeForPeer(p), MemberID: p.MemberID, Address: p.MetaAddress})
		}
		if p.Name == cfg.NodeName && !cfg.Join && !resume {
			member = p.MemberID
		}
	}
	placement := topology.PlacementRule{MinReplicas: min(2, len(cfg.Peers)), MaxReplicas: min(3, len(cfg.Peers))}
	if placement.MinReplicas == 0 {
		placement = topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3}
	}
	a.meta, err = a.runtime.OpenMeta(meta.Options{Identity: meta.Identity{NodeName: cfg.NodeName, ClusterID: cfg.ClusterID, InstanceID: cfg.InstanceID, MemberID: member}, Node: cfg.nodeForPeer(peer{Name: cfg.NodeName, Type: cfg.NodeType, MetaAddress: cfg.MetaAddress, LoadAddress: cfg.LoadAddress, HTTPAddress: cfg.HTTPAddress}), DBPath: filepath.Join(cfg.DataDir, "meta.bolt"), Founders: founders, Join: cfg.Join, JoinSeeds: seeds, Limits: cfg.Limits, Placement: placement, SessionTTL: cfg.SessionTTL, Timing: meta.Timing{TickInterval: cfg.TickInterval, ElectionTicks: 10}, Network: meta.NetworkOptions{Listener: ls.meta, AdvertiseAddress: cfg.MetaAddress, AllowPlaintext: true, ClusterCredential: []byte(cfg.Credential), Resolver: metaResolver{a}}, LoadGroups: a, Reconciliation: cfg.Reconciliation, OnSelfRemoved: func(context.Context, string) error { a.RequestClose(); return nil }, OnReadmission: a.readmission})
	if err != nil {
		return nil, err
	}
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
	if err := a.meta.Start(a.lifeCtx); err != nil {
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
	a.workWG.Go(a.markEstablished)
	if a.foundingGate {
		a.workWG.Go(a.runFoundingGate)
	}
	a.workWG.Go(a.superviseLoads)
	if a.cfg.Bootstrap {
		a.workWG.Go(a.bootstrap)
	}
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
	if a.meta != nil {
		remember(a.meta.Quiesce(context.Background()))
	}
	a.workWG.Wait()
	a.effectWG.Wait()
	if a.runtime != nil {
		remember(a.runtime.Close(context.Background()))
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

func (a *application) documents(ctx context.Context) (topology.Document, topology.Document, error) {
	target, err := a.meta.Target(ctx)
	if err != nil {
		return target, topology.Document{}, err
	}
	active, err := a.meta.Active(ctx)
	return target, active, err
}
func (a *application) address(ctx context.Context, name, label string) (string, error) {
	if a.meta != nil {
		target, active, err := a.documents(ctx)
		if err != nil {
			return "", err
		}
		for _, doc := range []*topology.Document{&target, &active} {
			if node := doc.Node(name); node != nil && node.Labels[label] != "" {
				return node.Labels[label], nil
			}
		}
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
	target, active, err := a.documents(ctx)
	if err != nil {
		return "", err
	}
	for _, doc := range []*topology.Document{&target, &active} {
		if g := doc.Group(group); g != nil {
			for _, r := range g.Replicas {
				if r.ReplicaID == replica {
					return r.NodeName, nil
				}
			}
		}
	}
	return "", fault.ErrIdentityMismatch
}

func (a *application) superviseLoads() {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	lastErrors := map[string]string{}
	for {
		select {
		case <-a.workCtx.Done():
			return
		case <-ticker.C:
		}
		if !a.meta.OwnSessionLive() {
			continue
		}
		active, err := a.meta.Active(a.workCtx)
		if err != nil {
			continue
		}
		for _, group := range active.LoadGroups() {
			for _, replica := range group.Replicas {
				if replica.NodeName == a.cfg.NodeName {
					if _, found := a.runtime.Load(group.ID); found {
						continue
					}
					ctx, cancel := context.WithTimeout(a.workCtx, 3*time.Second)
					_, err := a.openLocal(ctx, group.ID, replica.ReplicaID, load.Joining, nil)
					cancel()
					if err != nil {
						message := fmt.Sprintf("%v: %v", err, errors.Unwrap(err))
						if lastErrors[group.ID] != message {
							lastErrors[group.ID] = message
							a.logger.Warn("load recovery pending", "group", group.ID, "replica", replica.ReplicaID, "error", err, "cause", errors.Unwrap(err))
						}
					} else {
						delete(lastErrors, group.ID)
					}
				}
			}
		}
	}
}
func (a *application) bootstrap() {
	if err := a.waitFounders(a.workCtx); err != nil {
		return
	}
	if err := a.meta.WaitReady(a.workCtx); err != nil {
		return
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if a.meta.IsLeader() {
			ctx, cancel := context.WithTimeout(a.workCtx, 5*time.Second)
			done, err := a.bootstrapOnce(ctx)
			cancel()
			if done {
				return
			}
			if err != nil && a.workCtx.Err() == nil {
				a.logger.Debug("demo bootstrap pending", "error", err)
			}
		}
		select {
		case <-a.workCtx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (a *application) bootstrapOnce(ctx context.Context) (bool, error) {
	for _, spec := range []meta.GroupSpec{{ID: "demo"}, {ID: "workers", Placement: &topology.PlacementRule{MinReplicas: 1, MaxReplicas: 2, AllowedNodeTypes: []topology.NodeType{"worker"}}}} {
		latest, err := a.meta.Latest(ctx)
		if err != nil {
			return false, err
		}
		if latest.Group(spec.ID) == nil {
			created, err := a.meta.CreateLoadGroup(ctx, spec, meta.MutationOptions{ExpectedVersion: latest.Version, Actor: a.cfg.NodeName, Apply: true})
			if errors.Is(err, fault.ErrGroupRetired) {
				continue
			}
			if err != nil {
				return false, err
			}
			status, err := a.meta.WaitExecution(ctx, created.ExecutionID)
			if err != nil {
				return false, err
			}
			if status.State != topology.ExecutionSucceeded {
				return false, fault.New(fault.Unavailable, status.Error, nil)
			}
		} else {
			active, err := a.meta.Active(ctx)
			if err != nil {
				return false, err
			}
			if active.Group(spec.ID) == nil {
				id, err := a.meta.Apply(ctx, latest.Version)
				if err != nil {
					return false, err
				}
				status, err := a.meta.WaitExecution(ctx, id)
				if err != nil || status.State != topology.ExecutionSucceeded {
					return false, err
				}
			}
		}
	}
	return true, nil
}
