//go:build functional

package gquorum

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

func authorityNodeOptions(t *testing.T, root, instance string) NodeOptions {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return NodeOptions{StorageDir: root, Identity: topology.Identity{ClusterID: "cluster", NodeName: "n1", InstanceID: instance}, Bootstrap: BootstrapOptions{Founders: []FoundingMember{{Node: topology.Node{Name: "n1", Type: "worker"}, MemberID: 1, Address: listener.Addr().String()}}}, Meta: MetaOptions{Network: MetaNetworkOptions{Listener: listener, AdvertiseAddress: listener.Addr().String(), AllowPlaintext: true, ClusterCredential: []byte("test")}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}
func stopAuthorityNode(t *testing.T, n Node) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestNodeAuthorityLifecycleAndMissingRetainedEvidence(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	o := authorityNodeOptions(t, root, "first")
	n, err := NewNode(o)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	stopAuthorityNode(t, n)
	check, err := authoritystore.Open(o.authorityDBPath(), topology.Identity{ClusterID: "cluster", NodeName: "n1", MemberID: 1,
		InstanceID: "inspect"}, authoritystore.RetainedEvidence)
	if err != nil {
		t.Fatal(err)
	}
	acquired, err := check.MetadataAcquired(t.Context())
	if err != nil || !acquired {
		t.Fatal("Node started before recording metadata acquisition", acquired, err)
	}
	if err := check.Close(); err != nil {
		t.Fatal(err)
	}
	id, err := authoritystore.LoadIdentity(o.authorityDBPath())
	if err != nil || !id.Found || id.Identity.MemberID != 1 || id.Identity.InstanceID != "" {
		t.Fatal(id.Identity, id.Found, err)
	}
	second := authorityNodeOptions(t, root, "second")
	n, err = NewNode(second)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(t.Context()); err != nil {
		t.Fatal("retained restart", err)
	}
	stopAuthorityNode(t, n)
	before, err := os.ReadFile(o.metaDBPath())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(o.authorityDBPath()); err != nil {
		t.Fatal(err)
	}
	n, err = NewNode(authorityNodeOptions(t, root, "third"))
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(t.Context()); !errors.Is(err, fault.ErrUnreadable) {
		t.Fatal("lost authority silently recreated", err)
	}
	stopAuthorityNode(t, n)
	after, err := os.ReadFile(o.metaDBPath())
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("failed startup changed retained metadata", err)
	}
	if _, err := os.Stat(o.authorityDBPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("authority file recreated", err)
	}
}

func TestNodeReboundMetadataAcquisitionCrashBoundaries(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"before-pin", "after-pin", "after-marker", "lost-acquired-metadata"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			previous := topology.Identity{ClusterID: "cluster", NodeName: "n1", MemberID: 1, InstanceID: "old"}
			granted := previous
			granted.MemberID, granted.InstanceID = 2, "replacement"
			path := filepath.Join(root, "_authority.bolt")
			s, err := authoritystore.Open(path, previous, authoritystore.FreshEvidence)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			if err := s.MarkMetadataAcquired(t.Context(), previous); err != nil {
				t.Fatal(err)
			}
			if err := s.Rebind(t.Context(), previous, granted, "exact-admission"); err != nil {
				t.Fatal(err)
			}
			metaPath := filepath.Join(root, "_meta.bolt")
			if phase != "before-pin" {
				meta, err := metastore.Open(metaPath)
				if err != nil {
					t.Fatal(err)
				}
				if err := meta.EnsureIdentity(granted.NodeName, granted.ClusterID, granted.MemberID); err != nil {
					_ = meta.Close()
					t.Fatal(err)
				}
				if err := meta.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "after-marker" || phase == "lost-acquired-metadata" {
				if err := s.MarkMetadataAcquired(t.Context(), granted); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "lost-acquired-metadata" {
				if err := os.Remove(metaPath); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			granted.InstanceID = "restarted"
			s, err = authoritystore.Open(path, granted, authoritystore.RetainedEvidence)
			if err != nil {
				t.Fatal(err)
			}
			n := &node{ctx: t.Context(), authority: s, opts: NodeOptions{Identity: granted, StorageDir: root}}
			err = n.acquireMetaAuthority(granted)
			if phase == "lost-acquired-metadata" {
				if !errors.Is(err, fault.ErrUnreadable) {
					t.Fatal("lost established member was recreated", err)
				}
				if _, err := os.Stat(metaPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("lost metadata was mutated", err)
				}
			} else if err != nil {
				t.Fatal("authorized acquisition could not resume", err)
			}
		})
	}
}

type retainedAuthorityStore struct {
	storage.Store
	storage.RetirementStore
}

func (retainedAuthorityStore) HasState(context.Context) (bool, error) { return true, nil }

type unusedAuthorityTransport struct{ load.Transport }

func TestNodeRefusesLoadEvidenceWithoutAuthority(t *testing.T) {
	t.Parallel()
	for _, borrowed := range []bool{false, true} {
		t.Run(map[bool]string{false: "WAL", true: "borrowed"}[borrowed], func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			o := authorityNodeOptions(t, root, "new")
			o.Loads = LoadOptions{Factory: func(context.Context, load.ReplicaSpec) (load.Application, error) {
				t.Error("factory ran before authority validation")
				return load.Application{}, fault.ErrInvalid
			}, Transport: unusedAuthorityTransport{}}
			if borrowed {
				s := retainedAuthorityStore{}
				o.Loads.Storage = LoadStorageOptions{Store: s, Retirement: s}
			} else {
				wal := filepath.Join(root, "loads")
				if err := os.Mkdir(wal, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(wal, "unknown-retained-state"), []byte("evidence"), 0600); err != nil {
					t.Fatal(err)
				}
				o.Loads.Storage.WAL = &WALOptions{RootDir: wal}
			}
			n, err := NewNode(o)
			if err != nil {
				t.Fatal(err)
			}
			if err := n.Start(t.Context()); !errors.Is(err, fault.ErrUnreadable) {
				t.Fatal(err)
			}
			stopAuthorityNode(t, n)
			for _, path := range []string{o.metaDBPath(), o.authorityDBPath()} {
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("created storage over retained evidence", path, err)
				}
			}
		})
	}
}
