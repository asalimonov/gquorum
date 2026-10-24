//go:build !functional

package gquorum

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/recovery"
	"github.com/asalimonov/gquorum/topology"
)

func TestRecoveryConstructionDoesNotAcquireOrValidateRuntimeDependencies(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "absent")
	o := NodeOptions{StorageDir: root, Identity: topology.Identity{NodeName: "n1", ClusterID: "a"}}
	o.CallbackTimeout = -time.Second
	o.Meta.Network.Listener = unopenedListener{}
	o.Meta.Network.AdvertiseAddress = "invalid"
	o.Bootstrap = BootstrapOptions{Join: true, Founders: []FoundingMember{{}}}
	o.Loads.Factory = func(context.Context, load.ReplicaSpec) (load.Application, error) {
		panic("constructor called application factory")
	}
	o.Loads.Transport = unopenedTransport{}
	prewrite := false
	o.Loads.Storage.WAL = &WALOptions{RootDir: filepath.Join(root, "load"), Prewrite: &prewrite}
	options := recovery.Options{Files: func(context.Context, recovery.FilesTask) error {
		panic("constructor called files hook")
	}}
	r, err := NewRecovery(o, options)
	if err != nil {
		t.Fatal(err)
	}
	o.Loads.Storage.WAL.RootDir = "changed"
	o.Identity.NodeName = "changed"
	options.Resources.MaxGroupBytes = 42
	if r.source.Paths.LoadWALDir != filepath.Join(root, "load") || r.source.NodeName != "n1" || r.options.Resources != (recovery.Resources{}) {
		t.Fatal("configuration was aliased or zero overrides were resolved early", r.source, r.options.Resources)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("constructor acquired storage", err)
	}
}

func TestRecoveryConstructionRefusesUnsupportedOrInvalidSources(t *testing.T) {
	t.Parallel()
	base := NodeOptions{StorageDir: "data", Identity: topology.Identity{NodeName: "n1"}}
	for _, tc := range []struct {
		name string
		edit func(*NodeOptions)
		want error
	}{
		{"directory", func(o *NodeOptions) { o.StorageDir = "" }, fault.ErrInvalid},
		{"node", func(o *NodeOptions) { o.Identity.NodeName = "" }, fault.ErrInvalid},
		{"cluster", func(o *NodeOptions) { o.Identity.ClusterID = "\x00" }, fault.ErrInvalid},
		{"borrowed", func(o *NodeOptions) { o.Loads.Storage.Store = unopenedStorage{} }, fault.ErrUnsupported},
		{"retirement", func(o *NodeOptions) { o.Loads.Storage.Retirement = unopenedStorage{} }, fault.ErrUnsupported},
		{"wal-root", func(o *NodeOptions) { o.Loads.Storage.WAL = &WALOptions{} }, fault.ErrInvalid},
		{"nested-system", func(o *NodeOptions) { o.StorageDir = "data/system"; o.Loads.Storage.WAL = &WALOptions{RootDir: "data"} }, fault.ErrInvalid},
		{"private-root", func(o *NodeOptions) { o.StorageDir = "data/@gquorum-recovery/source" }, fault.ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := base
			tc.edit(&o)
			_, err := NewRecovery(o)
			var refusal *recovery.Refusal
			if !errors.Is(err, tc.want) || !errors.As(err, &refusal) || !refusal.Issue.Blocking {
				t.Fatal("missing classified refusal", err)
			}
		})
	}
	if _, err := NewRecovery(base); err != nil {
		t.Fatal("metadata-only configuration refused", err)
	}
	if _, err := NewRecovery(base, recovery.Options{}, recovery.Options{}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("multiple options accepted", err)
	}
	if _, err := NewRecovery(base, recovery.Options{Resources: recovery.Resources{MaxRecordBytes: math.MaxUint64}}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("overflowing byte bounds accepted", err)
	}
}

func TestRecoveryStartOptionsCopiesOnlyStartupChanges(t *testing.T) {
	t.Parallel()
	prewrite := false
	policy := topology.DefaultVoterConfig()
	opts := NodeOptions{
		StorageDir: "same/system", Identity: topology.Identity{NodeName: "n1", ClusterID: "a", MemberID: 4, InstanceID: "new-process"},
		Node:      topology.Node{Name: "n1", Type: "worker", Labels: map[string]string{"az": "one"}},
		Placement: topology.PlacementRule{MinReplicas: 3, MaxReplicas: 5, NodeSelector: map[string]string{"x": "y"}, AllowedNodeTypes: []topology.NodeType{"worker"}},
		Bootstrap: BootstrapOptions{Join: true, JoinSeeds: []string{"127.0.0.1:4000"}},
		Meta:      MetaOptions{VoterConfig: &policy, Network: MetaNetworkOptions{AdvertiseAddress: "127.0.0.1:3000", Listener: unopenedListener{}, ClusterCredential: []byte("secret")}},
		Loads:     LoadOptions{Storage: LoadStorageOptions{WAL: &WALOptions{RootDir: "same/load", Prewrite: &prewrite}}},
	}
	s := recovery.Status{Mode: recovery.Rebuild, Phase: recovery.AwaitingStart, Identity: topology.Identity{NodeName: "n1", ClusterID: "b", MemberID: 51}}
	out, err := RecoveryStartOptions(s, opts)
	if err != nil {
		t.Fatal(err)
	}
	if out.Identity.ClusterID != "b" || out.Identity.MemberID != 51 || out.Identity.InstanceID != "new-process" || out.StorageDir != opts.StorageDir || out.Bootstrap.Join || len(out.Bootstrap.JoinSeeds) != 0 || len(out.Bootstrap.Founders) != 1 {
		t.Fatal("wrong identity/bootstrap transformation", out.Identity, out.Bootstrap)
	}
	if out.Bootstrap.Founders[0].MemberID != 51 || out.Bootstrap.Founders[0].Address != opts.Meta.Network.AdvertiseAddress || out.Placement.MinReplicas != 3 || out.Meta.VoterConfig.TargetVoters != 3 {
		t.Fatal("founder is wrong or policy was silently weakened")
	}
	out.Node.Labels["az"] = "changed"
	out.Bootstrap.Founders[0].Node.Labels["az"] = "founder-only"
	out.Placement.NodeSelector["x"] = "changed"
	out.Placement.AllowedNodeTypes[0] = "changed"
	out.Meta.VoterConfig.TargetVoters = 1
	out.Meta.Network.ClusterCredential[0] = 'x'
	*out.Loads.Storage.WAL.Prewrite = true
	if opts.Node.Labels["az"] != "one" || opts.Placement.NodeSelector["x"] != "y" || opts.Placement.AllowedNodeTypes[0] != "worker" || opts.Meta.VoterConfig.TargetVoters != 3 || string(opts.Meta.Network.ClusterCredential) != "secret" || prewrite || out.Node.Labels["az"] != "changed" {
		t.Fatal("caller settings or sibling founder/node labels were aliased")
	}
	s.Mode, s.Phase, s.Identity.MemberID = recovery.ResetForJoin, recovery.JoinReady, 0
	join, err := RecoveryStartOptions(s, opts)
	if err != nil || !join.Bootstrap.Join || join.Identity.MemberID != 0 || len(join.Bootstrap.Founders) != 0 || len(join.Bootstrap.JoinSeeds) != 1 {
		t.Fatal("wrong reset startup configuration", join.Identity, join.Bootstrap, err)
	}
	join.Bootstrap.JoinSeeds[0] = "changed"
	if opts.Bootstrap.JoinSeeds[0] != "127.0.0.1:4000" {
		t.Fatal("join seeds were shared")
	}
}

func TestRecoveryStartOptionsRejectsUnstartableReports(t *testing.T) {
	t.Parallel()
	base := NodeOptions{Identity: topology.Identity{NodeName: "n1", InstanceID: "fresh"}, Node: topology.Node{Name: "n1", Type: "worker"}, Meta: MetaOptions{Network: MetaNetworkOptions{AdvertiseAddress: "127.0.0.1:1234"}}}
	report := recovery.Status{Mode: recovery.Rebuild, Phase: recovery.AwaitingStart, Identity: topology.Identity{NodeName: "n1", ClusterID: "b", MemberID: 9}}
	for _, phase := range []recovery.Phase{recovery.Approved, recovery.Files, recovery.Cancelled, recovery.Archiving, recovery.Complete, recovery.RollingBack} {
		s := report
		s.Phase = phase
		if _, err := RecoveryStartOptions(s, base); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal("unstartable phase accepted", phase, err)
		}
	}
	for _, edit := range []func(*NodeOptions, *recovery.Status){
		func(o *NodeOptions, _ *recovery.Status) { o.Identity.InstanceID = "" },
		func(_ *NodeOptions, s *recovery.Status) { s.Identity.MemberID = 0 },
		func(_ *NodeOptions, s *recovery.Status) { s.Identity.NodeName = "other" },
		func(o *NodeOptions, _ *recovery.Status) { o.Node.State = topology.NodeExcluded },
		func(o *NodeOptions, _ *recovery.Status) { o.Meta.Network.AdvertiseAddress = "invalid" },
		func(_ *NodeOptions, s *recovery.Status) {
			s.Mode = recovery.ResetForJoin
			s.Phase = recovery.JoinReady
			s.Identity.MemberID = 0
		},
	} {
		o, s := base, report
		edit(&o, &s)
		if _, err := RecoveryStartOptions(s, o); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal("invalid startup configuration accepted", err)
		}
	}
}
