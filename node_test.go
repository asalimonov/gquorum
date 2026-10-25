//go:build !functional

package gquorum

import (
	"context"
	"crypto/tls"
	"errors"
	"math"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/load/snapshot"
	"github.com/asalimonov/gquorum/scheduling"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func TestNodeConstructionAndStopBeforeStartAcquireNothing(t *testing.T) {
	t.Parallel()
	o := nodeOptionsForTest(t)
	o.Loads = loadOptionsForTest(t)
	n, err := NewNode(o)
	if err != nil {
		t.Fatal(err)
	}
	if n.Status().State != NodeNew || n.Cluster() != n.Cluster() {
		t.Fatal("invalid new node or unstable cluster facade")
	}
	if _, err := os.Stat(o.metaDBPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("constructor opened storage", err)
	}
	if _, err := n.StartLoadGroup(context.Background(), load.ReplicaRef{GroupID: "x", ReplicaID: 1}); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal(err)
	}
	// The listener and dependencies panic on use. An unstarted Node owns none.
	if err := n.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n.Status().State != NodeStopped || n.Status().Ready {
		t.Fatal(n.Status())
	}
	if err := n.Start(context.Background()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("stopped lifetime restarted", err)
	}
	if err := n.WaitReady(context.Background()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
}

// Any attempt to use a dependency during validation panics. All actual I/O and
// callback execution must wait for the node's startup/lifecycle workers.
type unopenedListener struct{ net.Listener }
type unopenedTransport struct{ load.Transport }
type unopenedStorage struct {
	storage.Store
	storage.RetirementStore
}
type unopenedHealth struct{ topology.HealthSource }
type unopenedScheduler struct{ scheduling.RepairPolicy }
type unopenedLifecycle struct{ lifecycle.Hooks }

func nodeOptionsForTest(t *testing.T) NodeOptions {
	t.Helper()
	return NodeOptions{
		StorageDir: filepath.Join(t.TempDir(), "unopened"),
		Identity:   topology.Identity{NodeName: "n1", ClusterID: "cluster", InstanceID: "process"},
		Bootstrap:  BootstrapOptions{Founders: []FoundingMember{{Node: topology.Node{Name: "n1", Type: "worker", Labels: map[string]string{"zone": "a"}}, MemberID: 1, Address: "127.0.0.1:10001"}}},
		Meta: MetaOptions{Network: MetaNetworkOptions{
			Listener: unopenedListener{}, AdvertiseAddress: "127.0.0.1:10001", AllowPlaintext: true, ClusterCredential: []byte("credential"),
		}},
		Health: unopenedHealth{},
	}
}

func loadOptionsForTest(t *testing.T) LoadOptions {
	t.Helper()
	return LoadOptions{
		Factory: func(context.Context, load.ReplicaSpec) (load.Application, error) {
			panic("factory called during validation")
		},
		Transport: unopenedTransport{}, Storage: LoadStorageOptions{WAL: &WALOptions{RootDir: filepath.Join(t.TempDir(), "unopened", "loads")}},
	}
}

func TestNodeOptionsPureDefaultsAndStorageSelection(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"meta_only", "owned_wal", "borrowed_store"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			o := nodeOptionsForTest(t)
			if mode != "meta_only" {
				o.Loads = loadOptionsForTest(t)
			}
			if mode == "borrowed_store" {
				o.Loads.Storage = LoadStorageOptions{Store: unopenedStorage{}, Retirement: unopenedStorage{}}
			}
			resolved, err := resolveNodeOptions(o)
			d, meta := resolved.node, resolved.meta
			if err != nil {
				t.Fatal(err)
			}
			if d.Identity.MemberID != 1 || d.Node.Type != "worker" || d.Limits != (topology.Limits{MaxNodes: 3, MaxReplicas: 5, MaxConcurrentGroups: 4, MaxConcurrentCleanup: 2}) || d.Meta.Timing != DefaultMetaTiming() || d.Meta.SessionTTL != 15*time.Second || d.Meta.MaxWatchers != 256 || d.Meta.SnapshotThreshold != 512 || d.Meta.Network.MaxSnapshotBytes != 64<<20 {
				t.Fatal("incorrect node defaults", d)
			}
			if d.Meta.Observation != (ObservationOptions{Interval: 5 * time.Second, Timeout: 2 * time.Second}) || (d.Scheduling.Disabled || d.Scheduling.Interval != 30*time.Second || d.Scheduling.Timeout != 20*time.Second || d.Scheduling.Cooldown != 5*time.Minute) || d.CallbackTimeout != 10*time.Second {
				t.Fatal("incorrect observation or callback defaults", d)
			}
			if _, ok := d.Scheduling.Policy.(*scheduling.DefaultRepairPolicy); !ok {
				t.Fatal("missing built-in scheduler")
			}
			if _, ok := d.Lifecycle.Hooks.(*lifecycle.DefaultHooks); !ok {
				t.Fatal("missing built-in lifecycle")
			}
			if meta.RepairPolicy != d.Scheduling.Policy || meta.CallbackTimeout != d.CallbackTimeout {
				t.Fatal("meta lost selected policy")
			}
			if mode != "meta_only" && (d.Loads.EffectTimeout != 10*time.Second || d.Loads.RecoveryInterval != 250*time.Millisecond || d.Loads.RetirementInterval != time.Minute) {
				t.Fatal("incorrect load defaults", d.Loads)
			}
			if _, err := os.Stat(filepath.Dir(o.metaDBPath())); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("validation acquired meta storage", err)
			}
			if _, err := os.Stat(o.StorageDir); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("validation acquired authority storage", err)
			}
			if o.Loads.Storage.WAL != nil {
				if _, err := os.Stat(o.Loads.Storage.WAL.RootDir); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("validation acquired WAL storage", err)
				}
			}
		})
	}
}

func TestNodeOptionsRejectAmbiguousOrUnsafeSettings(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*NodeOptions){
		"missing storage directory":                     func(o *NodeOptions) { o.StorageDir = "" },
		"NUL storage directory":                         func(o *NodeOptions) { o.StorageDir = "invalid\x00path" },
		"storage directory within load retirement root": func(o *NodeOptions) { o.StorageDir = filepath.Join(o.Loads.Storage.WAL.RootDir, "system") },
		"custom scheduler with built-in config": func(o *NodeOptions) {
			o.Scheduling.Policy = unopenedScheduler{}
			o.Scheduling.DefaultRepair.StableFor = time.Second
		},
		"custom lifecycle with built-in config": func(o *NodeOptions) {
			o.Lifecycle.Hooks = unopenedLifecycle{}
			o.Lifecycle.Default.RetainForever = true
		},
		"negative node watchers":     func(o *NodeOptions) { o.MaxWatchers = -1 },
		"negative scheduler window":  func(o *NodeOptions) { o.Scheduling.DefaultRepair.StableFor = -1 },
		"negative callback timeout":  func(o *NodeOptions) { o.CallbackTimeout = -1 },
		"negative removal retention": func(o *NodeOptions) { o.Lifecycle.Default.Retention = -1 },
		"ambiguous retention": func(o *NodeOptions) {
			o.Lifecycle.Default.RetainForever = true
			o.Lifecycle.Default.Retention = time.Hour
		},
		"disabled invalid scheduler":       func(o *NodeOptions) { o.Scheduling.Disabled = true; o.Scheduling.Timeout = -1 },
		"invalid observation":              func(o *NodeOptions) { o.Meta.Observation.Timeout = -1 },
		"TTL election relation":            func(o *NodeOptions) { o.Meta.SessionTTL = 2 * time.Second },
		"load without factory":             func(o *NodeOptions) { o.Loads.Factory = nil },
		"load without transport":           func(o *NodeOptions) { o.Loads.Transport = nil },
		"load without storage":             func(o *NodeOptions) { o.Loads.Storage = LoadStorageOptions{} },
		"borrowed without retirement":      func(o *NodeOptions) { o.Loads.Storage = LoadStorageOptions{Store: unopenedStorage{}} },
		"retirement without store":         func(o *NodeOptions) { o.Loads.Storage = LoadStorageOptions{Retirement: unopenedStorage{}} },
		"WAL and store":                    func(o *NodeOptions) { o.Loads.Storage.Store = unopenedStorage{} },
		"WAL and retirement":               func(o *NodeOptions) { o.Loads.Storage.Retirement = unopenedStorage{} },
		"missing WAL path":                 func(o *NodeOptions) { o.Loads.Storage.WAL.RootDir = " " },
		"negative WAL size":                func(o *NodeOptions) { o.Loads.Storage.WAL.SegmentBytes = -1 },
		"negative effect timeout":          func(o *NodeOptions) { o.Loads.EffectTimeout = -1 },
		"negative recovery":                func(o *NodeOptions) { o.Loads.RecoveryInterval = -1 },
		"negative retirement":              func(o *NodeOptions) { o.Loads.RetirementInterval = -1 },
		"load timing overflow":             func(o *NodeOptions) { o.Loads.Engine.Timing.TickInterval = math.MaxInt64 },
		"load negative tick":               func(o *NodeOptions) { o.Loads.Engine.Timing.TickInterval = -1 },
		"load heartbeat election relation": func(o *NodeOptions) { o.Loads.Engine.Timing.HeartbeatTicks = 10 },
		"load negative watchers":           func(o *NodeOptions) { o.Loads.Engine.MaxWatchers = -1 },
		"load negative pending":            func(o *NodeOptions) { o.Loads.Engine.MaxPendingProposals = -1 },
		"load excessive write wait":        func(o *NodeOptions) { o.Loads.Engine.Timing.WALWriteWait = time.Second },
		"implicit load configuration":      func(o *NodeOptions) { o.Loads = LoadOptions{EffectTimeout: time.Second} },
		"snapshot without loads": func(o *NodeOptions) {
			o.Loads = LoadOptions{SnapshotTransfer: snapshot.Options{MaxIncoming: 2}}
		},
		"negative snapshot timeout": func(o *NodeOptions) {
			o.Loads.SnapshotTransfer.TotalTimeout = -time.Second
		},
		"snapshot progress exceeds total": func(o *NodeOptions) {
			o.Loads.SnapshotTransfer.TotalTimeout = time.Second
		},
		"snapshot receive budget": func(o *NodeOptions) {
			o.Loads.SnapshotTransfer.MaxReceiveBytes = 1
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			o := nodeOptionsForTest(t)
			o.Loads = loadOptionsForTest(t)
			mutate(&o)
			if err := o.Validate(); !errors.Is(err, fault.ErrInvalid) {
				t.Fatal("invalid settings accepted", err)
			}
		})
	}
}

func TestNodeOptionsOwnMutableSettingsAndPreserveCustomDecisions(t *testing.T) {
	t.Parallel()
	o := nodeOptionsForTest(t)
	o.Loads = loadOptionsForTest(t)
	enabled := false
	o.Loads.Storage.WAL.Prewrite = &enabled
	o.Loads.Storage.WAL.DisableFSync = true
	o.Placement.AllowedNodeTypes = []topology.NodeType{"worker"}
	o.Scheduling.Policy, o.Lifecycle.Hooks = &unopenedScheduler{}, &unopenedLifecycle{}
	o.CallbackTimeout = 750 * time.Millisecond
	o.Scheduling.Interval, o.Scheduling.Timeout, o.Scheduling.Cooldown = 3*time.Second, time.Second, 7*time.Second
	o.Meta.Network.TLS = &tls.Config{NextProtos: []string{"h2"}, CipherSuites: []uint16{tls.TLS_AES_128_GCM_SHA256}, Certificates: []tls.Certificate{{Certificate: [][]byte{[]byte("certificate")}}}}
	o.Meta.Network.ClientTLS = o.Meta.Network.TLS
	resolved, err := resolveNodeOptions(o)
	d, meta := resolved.node, resolved.meta
	if err != nil {
		t.Fatal(err)
	}
	if d.Scheduling.Policy != o.Scheduling.Policy || d.Lifecycle.Hooks != o.Lifecycle.Hooks || d.Scheduling.DefaultRepair != (scheduling.DefaultRepairOptions{}) || d.Lifecycle.Default != (lifecycle.DefaultOptions{}) {
		t.Fatal("custom decisions changed or unused built-in settings manufactured")
	}
	if d.Scheduling != o.Scheduling || d.CallbackTimeout != o.CallbackTimeout || meta.CallbackTimeout != o.CallbackTimeout || meta.RepairPolicy != o.Scheduling.Policy || meta.Scheduling.Interval != o.Scheduling.Interval || meta.Scheduling.TickTimeout != o.Scheduling.Timeout || meta.Scheduling.Cooldown != o.Scheduling.Cooldown {
		t.Fatal("custom decision settings were lost during composition", d.Scheduling, meta.Scheduling)
	}
	o.Bootstrap.Founders[0].Node.Labels["zone"] = "changed"
	o.Placement.AllowedNodeTypes[0] = "changed"
	o.Meta.Network.ClusterCredential[0] = 'X'
	o.Meta.Network.TLS.NextProtos[0] = "changed"
	o.Meta.Network.TLS.CipherSuites[0] = 0
	o.Meta.Network.TLS.Certificates[0].Certificate[0][0] = 'X'
	o.Loads.Storage.WAL.RootDir = "changed"
	o.Loads.Storage.WAL.DisableFSync = false
	enabled = true
	if d.Node.Labels["zone"] != "a" || d.Bootstrap.Founders[0].Node.Labels["zone"] != "a" || d.Placement.AllowedNodeTypes[0] != "worker" || string(d.Meta.Network.ClusterCredential) != "credential" || d.Loads.Storage.WAL.RootDir == "changed" || *d.Loads.Storage.WAL.Prewrite || !d.Loads.Storage.WAL.DisableFSync {
		t.Fatal("mutable caller settings retained")
	}
	for _, cfg := range []*tls.Config{d.Meta.Network.TLS, d.Meta.Network.ClientTLS} {
		if cfg.NextProtos[0] != "h2" || cfg.CipherSuites[0] != tls.TLS_AES_128_GCM_SHA256 || string(cfg.Certificates[0].Certificate[0]) != "certificate" {
			t.Fatal("nested TLS values borrowed")
		}
	}
}

func TestNodeOptionsOnlyLimitsBoundClusterSize(t *testing.T) {
	t.Parallel()
	o := nodeOptionsForTest(t)
	for _, name := range []string{"n2", "n3", "n4", "n5", "n6", "n7"} {
		o.Bootstrap.Founders = append(o.Bootstrap.Founders, FoundingMember{Node: topology.Node{Name: name, Type: "observer"}, MemberID: uint64(len(o.Bootstrap.Founders) + 1), Address: "127.0.0.1:10002"})
	}
	if err := o.Validate(); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("default limit ignored", err)
	}
	o.Limits = topology.Limits{MaxNodes: 7, MaxReplicas: 7}
	if err := o.Validate(); err != nil {
		t.Fatal("additional hard-coded cluster limit", err)
	}
}

func TestConsistentReadOptionsValidationAndWiring(t *testing.T) {
	t.Parallel()
	o := nodeOptionsForTest(t)
	o.Meta.Reads = load.ReadOptions{Timeout: time.Second, MaxPending: 2, MaxRequestBytes: 128, MaxResultBytes: 4096}
	resolved, err := resolveNodeOptions(o)
	meta := resolved.meta
	if err != nil || load.ReadOptions(meta.Reads) != o.Meta.Reads {
		t.Fatal(meta.Reads, err)
	}
	o.Meta.Reads.MaxPending = -1
	if _, err := resolveNodeOptions(o); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	o = nodeOptionsForTest(t)
	o.Loads = loadOptionsForTest(t)
	o.Loads.Engine.Reads.MaxResultBytes = -1
	if _, err := resolveNodeOptions(o); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
}

func TestAuthorityPathRejectsAliasesAndRetirementRoots(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"separate", "symlink", "hardlink", "parent symlink", "dangling symlink"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			meta, authority := filepath.Join(root, "_meta.bolt"), filepath.Join(root, "_authority.bolt")
			if err := os.WriteFile(meta, []byte("retained"), 0600); err != nil {
				t.Fatal(err)
			}
			o := NodeOptions{StorageDir: root}
			switch name {
			case "symlink":
				if err := os.Symlink(meta, authority); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(meta, authority); err != nil {
					t.Fatal(err)
				}
			case "parent symlink":
				wal := filepath.Join(root, "wal")
				if err := os.Mkdir(wal, 0700); err != nil {
					t.Fatal(err)
				}
				alias := filepath.Join(root, "alias")
				if err := os.Symlink(wal, alias); err != nil {
					t.Fatal(err)
				}
				o.StorageDir = filepath.Join(alias, "new")
				o.Loads.Storage.WAL = &WALOptions{RootDir: wal}
			case "dangling symlink":
				if err := os.Symlink(filepath.Join(root, "absent"), authority); err != nil {
					t.Fatal(err)
				}
			}
			// Lexical validation intentionally does no filesystem inspection.
			if err := validateStorageDir(o); err != nil {
				t.Fatal(err)
			}
			err := (&node{opts: o}).checkAuthorityPath()
			if name == "separate" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, fault.ErrInvalid) {
				t.Fatal("unsafe path accepted", err)
			}
			data, err := os.ReadFile(meta)
			if err != nil || string(data) != "retained" {
				t.Fatal("preflight modified evidence", err)
			}
		})
	}
}

func TestSystemStorageRejectsOldFilenamesWithoutCreatingReplacement(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"meta.bolt", "authority.bolt"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			old := filepath.Join(dir, name)
			if err := os.WriteFile(old, []byte("retained evidence"), 0600); err != nil {
				t.Fatal(err)
			}
			n := &node{opts: NodeOptions{StorageDir: dir}}
			if err := n.checkAuthorityPath(); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal("old filenames were treated as fresh storage", err)
			}
			data, err := os.ReadFile(old)
			if err != nil || string(data) != "retained evidence" {
				t.Fatal("retained evidence changed", err)
			}
			for _, name := range []string{metaDBFilename, authorityDBFilename} {
				if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("replacement storage was created", name, err)
				}
			}
		})
	}
}

func TestNodeSnapshotFacadeIsPureStableAndOwnsOptions(t *testing.T) {
	t.Parallel()
	o := nodeOptionsForTest(t)
	o.Loads = loadOptionsForTest(t)
	o.Loads.SnapshotTransfer = snapshot.Options{MaxSnapshotBytes: 8 << 20, MaxOutgoing: 2}
	want, err := o.Loads.SnapshotTransfer.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	n, err := NewNode(o)
	if err != nil {
		t.Fatal(err)
	}
	facade := n.LoadSnapshots()
	if facade == nil || facade != n.LoadSnapshots() || facade.Options() != want {
		t.Fatal("missing, unstable or unresolved snapshot facade")
	}
	o.Loads.SnapshotTransfer.MaxSnapshotBytes = 1
	observed := facade.Options()
	observed.TotalTimeout = time.Second
	if facade.Options() != want {
		t.Fatal("constructor/options borrowed mutable settings")
	}
	if _, err := facade.Begin(context.Background(), snapshot.BeginRequest{}); !errors.Is(err, fault.ErrReplicaNotReady) {
		t.Fatal("unstarted Node accepted snapshot work", err)
	}
	if page := facade.List(snapshot.Query{}); len(page.Items) != 0 || page.Incoming != 0 || page.Outgoing != 0 {
		t.Fatal("unstarted Node acquired transfer state", page)
	}
	select {
	case <-facade.SessionDone(snapshot.Session{}):
	default:
		t.Fatal("unknown session should already be closed")
	}
	for _, path := range []string{o.StorageDir, o.Loads.Storage.WAL.RootDir} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("construction or observations acquired storage", path, err)
		}
	}
	if err := n.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n.LoadSnapshots() != facade || facade.Options() != want {
		t.Fatal("stop replaced immutable facade/settings")
	}
	if _, err := facade.Finish(context.Background(), snapshot.Session{}); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("stopped Node accepted transfer work", err)
	}
}

func TestMetaOnlyNodeHasNoSnapshotService(t *testing.T) {
	t.Parallel()
	n, err := NewNode(nodeOptionsForTest(t))
	if err != nil {
		t.Fatal(err)
	}
	if n.LoadSnapshots() != nil {
		t.Fatal("metadata-only Node acquired a load service")
	}
}

func (unopenedTransport) SendSnapshot(context.Context, snapshot.Send) (snapshot.Ack, error) {
	panic("snapshot transport used before resource acquisition")
}

type ordinaryOnlyTransport struct{ load.Transport }

func TestManagedNodeRequiresSnapshotCapabilityWithoutCallingIt(t *testing.T) {
	t.Parallel()
	for _, transport := range []load.Transport{ordinaryOnlyTransport{}, (*unopenedTransport)(nil)} {
		options := nodeOptionsForTest(t)
		options.Loads = loadOptionsForTest(t)
		options.Loads.Transport = transport
		if _, err := NewNode(options); !errors.Is(err, fault.ErrUnsupported) {
			t.Fatal("missing/nil snapshot transport accepted", err)
		}
	}
}
