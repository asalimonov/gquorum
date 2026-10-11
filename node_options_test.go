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
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/policy"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// Any attempt to use a dependency during validation panics. All actual I/O and
// callback execution must wait for the node's startup/lifecycle workers.
type unopenedListener struct{ net.Listener }
type unopenedTransport struct{ load.Transport }
type unopenedStorage struct {
	storage.Store
	storage.RetirementStore
}
type unopenedHealth struct{ topology.HealthSource }
type unopenedScheduler struct{ policy.Scheduler }
type unopenedLifecycle struct{ policy.LifecyclePolicy }

func nodeOptionsForTest(t *testing.T) NodeOptions {
	t.Helper()
	return NodeOptions{
		Identity:  topology.Identity{NodeName: "n1", ClusterID: "cluster", InstanceID: "process"},
		Bootstrap: BootstrapOptions{Founders: []FoundingMember{{Node: topology.Node{Name: "n1", Type: "worker", Labels: map[string]string{"zone": "a"}}, MemberID: 1, Address: "127.0.0.1:10001"}}},
		Meta: MetaOptions{DBPath: filepath.Join(t.TempDir(), "unopened", "meta.bolt"), Network: MetaNetworkOptions{
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
			d, meta, err := resolveNodeOptions(o)
			if err != nil {
				t.Fatal(err)
			}
			if d.Identity.MemberID != 1 || d.Node.Type != "worker" || d.Limits != (topology.Limits{MaxNodes: 3, MaxReplicas: 5}) || d.Meta.Timing != DefaultMetaTiming() || d.Meta.SessionTTL != 15*time.Second || d.Meta.MaxWatchers != 256 || d.Meta.SnapshotThreshold != 512 || d.Meta.Network.MaxSnapshotBytes != 64<<20 {
				t.Fatal("incorrect node defaults", d)
			}
			if d.Meta.Observation != (ObservationOptions{Interval: 5 * time.Second, Timeout: 2 * time.Second}) || d.Policies.Scheduling != (SchedulingOptions{Interval: 30 * time.Second, Timeout: 20 * time.Second, Cooldown: 5 * time.Minute}) || d.Policies.CallbackTimeout != 10*time.Second {
				t.Fatal("incorrect observation or policy defaults", d)
			}
			if _, ok := d.Policies.Scheduler.(*policy.RestoreDegraded); !ok {
				t.Fatal("missing built-in scheduler")
			}
			if _, ok := d.Policies.Lifecycle.(*policy.DefaultLifecycle); !ok {
				t.Fatal("missing built-in lifecycle")
			}
			if meta.Scheduler != d.Policies.Scheduler || meta.CallbackTimeout != d.Policies.CallbackTimeout {
				t.Fatal("meta lost selected policy")
			}
			if mode != "meta_only" && (d.Loads.EffectTimeout != 10*time.Second || d.Loads.RecoveryInterval != 250*time.Millisecond || d.Loads.RetirementInterval != time.Minute) {
				t.Fatal("incorrect load defaults", d.Loads)
			}
			if _, err := os.Stat(filepath.Dir(o.Meta.DBPath)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("validation acquired meta storage", err)
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
		"custom scheduler with built-in config": func(o *NodeOptions) {
			o.Policies.Scheduler = unopenedScheduler{}
			o.Policies.RestoreDegraded.StableFor = time.Second
		},
		"custom lifecycle with built-in config": func(o *NodeOptions) {
			o.Policies.Lifecycle = unopenedLifecycle{}
			o.Policies.DefaultLifecycle.RetainForever = true
		},
		"negative scheduler window":  func(o *NodeOptions) { o.Policies.RestoreDegraded.StableFor = -1 },
		"negative policy timeout":    func(o *NodeOptions) { o.Policies.CallbackTimeout = -1 },
		"negative removal retention": func(o *NodeOptions) { o.Policies.DefaultLifecycle.Retention = -1 },
		"ambiguous retention": func(o *NodeOptions) {
			o.Policies.DefaultLifecycle.RetainForever = true
			o.Policies.DefaultLifecycle.Retention = time.Hour
		},
		"disabled invalid scheduler":       func(o *NodeOptions) { o.Policies.Scheduling.Disabled = true; o.Policies.Scheduling.Timeout = -1 },
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
		"load negative pending":            func(o *NodeOptions) { o.Loads.Engine.MaxPendingProposals = -1 },
		"load excessive write wait":        func(o *NodeOptions) { o.Loads.Engine.Timing.WALWriteWait = time.Second },
		"implicit load configuration":      func(o *NodeOptions) { o.Loads = LoadOptions{EffectTimeout: time.Second} },
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

func TestNodeOptionsOwnMutableSettingsAndPreserveCustomPolicies(t *testing.T) {
	t.Parallel()
	o := nodeOptionsForTest(t)
	o.Loads = loadOptionsForTest(t)
	enabled := false
	o.Loads.Storage.WAL.Prewrite = &enabled
	o.Placement.AllowedNodeTypes = []topology.NodeType{"worker"}
	o.Policies.Scheduler, o.Policies.Lifecycle = &unopenedScheduler{}, &unopenedLifecycle{}
	o.Meta.Network.TLS = &tls.Config{NextProtos: []string{"h2"}, CipherSuites: []uint16{tls.TLS_AES_128_GCM_SHA256}, Certificates: []tls.Certificate{{Certificate: [][]byte{[]byte("certificate")}}}}
	o.Meta.Network.ClientTLS = o.Meta.Network.TLS
	d, _, err := resolveNodeOptions(o)
	if err != nil {
		t.Fatal(err)
	}
	if d.Policies.Scheduler != o.Policies.Scheduler || d.Policies.Lifecycle != o.Policies.Lifecycle || d.Policies.RestoreDegraded != (policy.RestoreDegradedOptions{}) || d.Policies.DefaultLifecycle != (policy.DefaultLifecycleOptions{}) {
		t.Fatal("custom policies changed or unused built-in settings manufactured")
	}
	o.Bootstrap.Founders[0].Node.Labels["zone"] = "changed"
	o.Placement.AllowedNodeTypes[0] = "changed"
	o.Meta.Network.ClusterCredential[0] = 'X'
	o.Meta.Network.TLS.NextProtos[0] = "changed"
	o.Meta.Network.TLS.CipherSuites[0] = 0
	o.Meta.Network.TLS.Certificates[0].Certificate[0][0] = 'X'
	o.Loads.Storage.WAL.RootDir = "changed"
	enabled = true
	if d.Node.Labels["zone"] != "a" || d.Bootstrap.Founders[0].Node.Labels["zone"] != "a" || d.Placement.AllowedNodeTypes[0] != "worker" || string(d.Meta.Network.ClusterCredential) != "credential" || d.Loads.Storage.WAL.RootDir == "changed" || *d.Loads.Storage.WAL.Prewrite {
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
