package meta

import (
	"crypto/tls"
	"errors"
	"math"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/topology"
)

func optionsForTest(t *testing.T) Options {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	return Options{Identity: Identity{NodeName: "n1", ClusterID: "cluster", InstanceID: "instance", MemberID: 1}, DBPath: filepath.Join(t.TempDir(), "meta.bolt"), Founders: []FoundingMember{{Node: topology.Node{Name: "n1", Type: "worker"}, MemberID: 1, Address: listener.Addr().String()}}, Network: NetworkOptions{Listener: listener, AdvertiseAddress: listener.Addr().String(), AllowPlaintext: true, ClusterCredential: []byte("test-secret")}}
}
func TestMetaOptionsDefaultsAndOnlyLimitsControlClusterSize(t *testing.T) {
	t.Parallel()
	o := optionsForTest(t)
	if err := o.Validate(); err != nil {
		t.Fatal(err)
	}
	d := o.defaults()
	if d.Limits.MaxNodes != 3 || d.Limits.MaxReplicas != 5 || d.SnapshotThreshold != 512 || d.SessionTTL != 15*time.Second || d.Identity.MemberID != 1 {
		t.Fatal(d)
	}
	o.Founders = append(o.Founders, FoundingMember{Node: topology.Node{Name: "n2", Type: "observer"}, MemberID: 2, Address: "127.0.0.1:2"}, FoundingMember{Node: topology.Node{Name: "n3", Type: "worker"}, MemberID: 3, Address: "127.0.0.1:3"}, FoundingMember{Node: topology.Node{Name: "n4", Type: "storage"}, MemberID: 4, Address: "127.0.0.1:4"})
	if err := o.Validate(); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal("default node limit ignored", err)
	}
	o.Limits = topology.Limits{MaxNodes: 7}
	if err := o.Validate(); err != nil {
		t.Fatal("explicit Limits did not enable larger cluster", err)
	}
}
func TestMetaOptionsRejectUnsafeInputs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*Options)
	}{
		{"path", func(o *Options) { o.DBPath = "" }}, {"identity", func(o *Options) { o.Identity.InstanceID = "" }},
		{"null", func(o *Options) { o.Identity.NodeName = "n1\x00other" }}, {"credential", func(o *Options) { o.Network.ClusterCredential = nil }},
		{"plaintext", func(o *Options) { o.Network.AllowPlaintext = false }}, {"type", func(o *Options) { o.Founders[0].Node.Type = "" }},
		{"type conflict", func(o *Options) { o.Node = topology.Node{Type: "other"} }}, {"identity conflict", func(o *Options) { o.Identity.MemberID = 2 }},
		{"duplicate", func(o *Options) { o.Founders = append(o.Founders, o.Founders[0]) }}, {"counter overflow", func(o *Options) { o.Founders[0].MemberID = math.MaxUint64 }},
		{"negative timing", func(o *Options) { o.Timing.TickInterval = -1 }}, {"heartbeat", func(o *Options) { o.Timing.ElectionTicks = 5; o.Timing.HeartbeatTicks = 5 }},
		{"timing overflow", func(o *Options) { o.Timing.TickInterval = time.Duration(math.MaxInt64); o.Timing.ElectionTicks = 2 }},
		{"TTL election", func(o *Options) { o.SessionTTL = 2 * time.Second }}, {"fractional TTL", func(o *Options) { o.SessionTTL = 15*time.Second + time.Millisecond }},
		{"retry", func(o *Options) { o.Timing.PeerRetryInitial = time.Second; o.Timing.PeerRetryMax = time.Millisecond }},
		{"advertised address", func(o *Options) { o.Network.AdvertiseAddress = "bad" }}, {"join seed", func(o *Options) { o.Join = true; o.Founders = nil }},
		{"join founding", func(o *Options) { o.Join = true; o.JoinSeeds = []string{"127.0.0.1:1234"} }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o := optionsForTest(t)
			tc.mutate(&o)
			if err := o.Validate(); !errors.Is(err, fault.ErrInvalid) {
				t.Fatal("invalid constructor inputs accepted", err)
			}
		})
	}
}
func TestMetaOptionClonesCallerInputs(t *testing.T) {
	t.Parallel()
	o := optionsForTest(t)
	o.Founders[0].Node.Labels = map[string]string{"zone": "a"}
	o.Network.TLS = &tls.Config{ServerName: "server"}
	o.Network.ClientTLS = &tls.Config{ServerName: "client"}
	d := o.defaults()
	o.Founders[0].Node.Labels["zone"] = "changed"
	o.Network.ClusterCredential[0] = 'x'
	o.Network.TLS.ServerName = "changed"
	o.Network.ClientTLS.ServerName = "changed"
	if d.Founders[0].Node.Labels["zone"] != "a" || d.Node.Labels["zone"] != "a" || d.Network.ClusterCredential[0] != 't' || d.Network.TLS.ServerName != "server" || d.Network.ClientTLS.ServerName != "client" {
		t.Fatal("constructor retained caller mutable inputs")
	}
}
func TestFoundingIDsAreIndependentOfOrdering(t *testing.T) {
	t.Parallel()
	a, err := FoundingIDs("cluster", []string{"n1", "n2", "n3"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := FoundingIDs("cluster", []string{"n3", "n1", "n2"})
	if err != nil || !reflect.DeepEqual(a, b) {
		t.Fatal(a, b, err)
	}
	c, err := FoundingIDs("other", []string{"n1", "n2", "n3"})
	if err != nil || reflect.DeepEqual(a, c) {
		t.Fatal("IDs did not bind cluster", err)
	}
	for _, names := range [][]string{nil, {""}, {"n1", "n1"}, {"n1\x00n2"}} {
		if _, err := FoundingIDs("cluster", names); err == nil {
			t.Fatal("invalid names accepted", names)
		}
	}
}

func TestMetaObservationOptionsRefusedBeforeOpeningStorage(t *testing.T) {
	t.Parallel()
	for _, settings := range []ObservationOptions{{Interval: -time.Second}, {Timeout: -time.Second}, {Interval: time.Second, Timeout: time.Second}, {Interval: time.Nanosecond, Timeout: time.Nanosecond}} {
		o := optionsForTest(t)
		o.Observation = settings
		if _, err := New(o); !errors.Is(err, fault.ErrInvalid) {
			t.Fatal(err)
		}
		if _, err := os.Stat(o.DBPath); !os.IsNotExist(err) {
			t.Fatal("invalid observer settings changed storage", err)
		}
	}
}
