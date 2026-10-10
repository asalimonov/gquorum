package meta

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/topology"
)

func TestReadmissionHookMustReleaseAndQuarantineOldMetaIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.bolt")
	store, err := metastore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.EnsureIdentity("one", "cluster", 1); err != nil {
		t.Fatal(err)
	}
	g := &Group{store: store, opts: Options{DBPath: path}}
	g.opts.OnReadmission = func(ctx context.Context, r Readmission) error {
		// This read takes an independent file lock; it succeeds only after Close.
		pin, found, err := metastore.LoadIdentity(path)
		if err != nil || !found || pin.MemberID != 1 {
			t.Fatalf("old handle was not released: %v", err)
		}
		if r.Previous.MemberID != 1 || r.Granted.MemberID != 2 {
			t.Fatal("wrong identities", r)
		}
		return nil // Deliberately fails to quarantine despite returning success.
	}
	result := joinResponse{MemberID: 2}
	err = g.prepareReadmission(context.Background(), Identity{NodeName: "one", ClusterID: "cluster", MemberID: 1}, result)
	if !errors.Is(err, fault.ErrReadmissionRequired) {
		t.Fatal("unquarantined lineage accepted", err)
	}
	pin, found, err := metastore.LoadIdentity(path)
	if err != nil || !found || pin.MemberID != 1 {
		t.Fatal("library rebound old storage", pin, err)
	}
}

func TestReadmissionPreviousScopeNeverIncludesGrantedOrOtherNodeReplicas(t *testing.T) {
	previous := topology.Document{Groups: []topology.Group{{ID: "work", Replicas: []topology.Replica{{NodeName: "one", ReplicaID: 1}, {NodeName: "two", ReplicaID: 2}}}}}
	granted := previous.Clone()
	granted.Groups[0].Replicas[0].ReplicaID = 3
	ahead := granted.Clone()
	ahead.Groups[0].Replicas = append(ahead.Groups[0].Replicas, topology.Replica{NodeName: "one", ReplicaID: 4})
	scope := previousNodeGroups([]topology.Document{previous, previous, ahead}, granted, "one")
	if len(scope) != 1 || len(scope[0].Replicas) != 2 || scope[0].Replicas[0].ReplicaID != 1 || scope[0].Replicas[1].ReplicaID != 4 {
		t.Fatal("wrong quarantine scope", scope)
	}
	scope[0].Replicas[0].ReplicaID = 999
	if previous.Groups[0].Replicas[0].ReplicaID != 1 {
		t.Fatal("scope aliases old document")
	}
}

func TestJoinGrantValidatesEveryHintBeforeRoutingMutation(t *testing.T) {
	rules := topology.Rules{SchemaVersion: 1, Limits: topology.Limits{}.WithDefaults()}
	now := time.Now().UTC()
	doc := topology.Document{Version: 1, ClusterID: "cluster", Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3}, Nodes: []topology.Node{{Name: "one", Type: "worker", State: topology.NodeActive}}, Groups: []topology.Group{{ID: topology.MetaGroupID, NextReplicaID: 2, Replicas: []topology.Replica{{NodeName: "one", ReplicaID: 1, Role: topology.ReplicaVoter}}}}, Origin: topology.OriginFounding, CreatedAt: now}
	valid := joinResponse{Rules: rules, Existing: true, MemberID: 1, Granted: doc, Members: []joinMember{{Name: "one", MemberID: 1, Address: "127.0.0.1:1234"}}, LeaderAddress: "127.0.0.1:1234"}
	identity := Identity{NodeName: "one", ClusterID: "cluster", MemberID: 1}
	if err := validateJoinGrant(identity, rules, valid); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*joinResponse){
		"wrong owner":            func(r *joinResponse) { r.Members[0].Name = "intruder" },
		"unapproved member":      func(r *joinResponse) { r.Members[0].MemberID = 99 },
		"zero member":            func(r *joinResponse) { r.Members[0].MemberID = 0 },
		"invalid address":        func(r *joinResponse) { r.Members[0].Address = "bad" },
		"duplicate member":       func(r *joinResponse) { r.Members = append(r.Members, r.Members[0]) },
		"invalid leader address": func(r *joinResponse) { r.LeaderAddress = "bad" },
		"wrong cluster":          func(r *joinResponse) { r.Granted.ClusterID = "other" },
		"wrong limits":           func(r *joinResponse) { r.Rules.Limits.MaxNodes++ },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := valid
			r.Granted = valid.Granted.Clone()
			r.Members = append([]joinMember(nil), valid.Members...)
			mutate(&r)
			if err := validateJoinGrant(identity, rules, r); err == nil {
				t.Fatal("invalid grant accepted")
			}
		})
	}
}

func TestJoiningCannotClaimUsedMemberWithoutPinnedStore(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	path := filepath.Join(t.TempDir(), "unopened.bolt")
	opts := Options{Identity: Identity{NodeName: "one", ClusterID: "cluster", InstanceID: "fresh-process", MemberID: 1}, Node: topology.Node{Name: "one", Type: "worker"}, DBPath: path, Join: true, JoinSeeds: []string{"127.0.0.1:1"}, Network: NetworkOptions{Listener: listener, AdvertiseAddress: listener.Addr().String(), AllowPlaintext: true, ClusterCredential: []byte("test-credential")}}
	group, err := New(opts)
	if group != nil {
		group.Stop(context.Background())
	}
	if !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("unbacked retained identity accepted", err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("identity refusal touched storage", err)
	}
}
