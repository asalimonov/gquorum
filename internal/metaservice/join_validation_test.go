//go:build !functional

package metaservice

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/topology"
)

func TestReadmissionHookMustReleaseAndQuarantineOldMetaIdentity(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "meta.bolt")
	store, err := metastore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.EnsureIdentity("one", "cluster", 1); err != nil {
		t.Fatal(err)
	}
	g := &Group{store: store, opts: Options{DBPath: path}}
	g.opts.OnReadmission = func(_ context.Context, r Readmission) error {
		// This read takes an independent file lock; it succeeds only after Close.
		pin, err := metastore.LoadIdentity(path)
		if err != nil || !pin.Found || pin.Identity.MemberID != 1 {
			t.Fatalf("old handle was not released: %v", err)
		}
		if r.Previous.MemberID != 1 || r.Granted.MemberID != 2 {
			t.Fatal("wrong identities", r)
		}
		return nil // Deliberately fails to quarantine despite returning success.
	}
	result := joinResponse{MemberID: 2}
	err = g.prepareReadmission(context.Background(), topology.Identity{NodeName: "one", ClusterID: "cluster", MemberID: 1}, result)
	if !errors.Is(err, fault.ErrReadmissionRequired) {
		t.Fatal("unquarantined lineage accepted", err)
	}
	pin, err := metastore.LoadIdentity(path)
	if err != nil || !pin.Found || pin.Identity.MemberID != 1 {
		t.Fatal("library rebound old storage", pin.Identity, err)
	}
}

func TestReadmissionPreviousScopeNeverIncludesGrantedOrOtherNodeReplicas(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	rules := topology.Rules{SchemaVersion: 1, Limits: topology.Limits{}.WithDefaults()}
	now := time.Now().UTC()
	doc := topology.Document{Version: 1, ClusterID: "cluster", Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3}, Nodes: []topology.Node{{Name: "one", Type: "worker", State: topology.NodeActive}}, Groups: []topology.Group{{ID: topology.MetaGroupID, NextReplicaID: 2, Replicas: []topology.Replica{{NodeName: "one", ReplicaID: 1, Role: topology.ReplicaVoter}}}}, Origin: topology.OriginFounding, CreatedAt: now}
	valid := joinResponse{Rules: rules, Existing: true, MemberID: 1, Granted: doc, Members: []joinMember{{Name: "one", MemberID: 1, Address: "127.0.0.1:1234"}}, LeaderAddress: "127.0.0.1:1234"}
	identity := topology.Identity{NodeName: "one", ClusterID: "cluster", MemberID: 1}
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
		"foreign cleanup owner": func(r *joinResponse) {
			r.PreviousGroups = []topology.Group{{ID: "work", Replicas: []topology.Replica{{NodeName: "intruder", ReplicaID: 1}}}}
		},
		"zero cleanup identity": func(r *joinResponse) {
			r.PreviousGroups = []topology.Group{{ID: "work", Replicas: []topology.Replica{{NodeName: "one"}}}}
		},
		"invalid cleanup group": func(r *joinResponse) {
			r.PreviousGroups = []topology.Group{{ID: "../work", Replicas: []topology.Replica{{NodeName: "one", ReplicaID: 1}}}}
		},
		"duplicate cleanup identity": func(r *joinResponse) {
			r.PreviousGroups = []topology.Group{{ID: "work", Replicas: []topology.Replica{{NodeName: "one", ReplicaID: 1}, {NodeName: "one", ReplicaID: 1}}}}
		},
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

func TestReadmissionCallbackUsesOwnedExactGrant(t *testing.T) {
	t.Parallel()
	g := &Group{opts: Options{DBPath: filepath.Join(t.TempDir(), "missing.bolt")}}
	previous := topology.Identity{NodeName: "one", ClusterID: "cluster", InstanceID: "process", MemberID: 1}
	r := joinResponse{MemberID: 2, PreviousGroups: []topology.Group{{ID: "old", Replicas: []topology.Replica{{NodeName: "one", ReplicaID: 7}}}}}
	r.Admission.OperatorID, r.Admission.InstanceID = "exec-v3-a1", "process"
	g.opts.OnReadmission = func(ctx context.Context, grant Readmission) error {
		if err := g.AuthorizeReadmission(ctx, grant); err != nil {
			t.Fatal(err)
		}
		if len(grant.PreviousGroups) != 1 || grant.PreviousGroups[0].Replicas[0].ReplicaID != 7 {
			t.Fatal("lost remote approved catalog", grant)
		}
		grant.PreviousGroups[0].Replicas[0].ReplicaID = 9
		if !errors.Is(g.AuthorizeReadmission(ctx, grant), fault.ErrOperatorAuthorization) {
			t.Fatal("mutated callback grant altered authorization")
		}
		return nil
	}
	if err := g.prepareReadmission(t.Context(), previous, r); err != nil {
		t.Fatal(err)
	}
	if r.PreviousGroups[0].Replicas[0].ReplicaID != 7 {
		t.Fatal("callback mutated source grant")
	}
}
