package metaservice

import (
	"bytes"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metastore"
	bolt "go.etcd.io/bbolt"
)

func TestMetaConstructorRefusesOrphanStateBeforeFoundingOrJoin(t *testing.T) {
	t.Parallel()
	for _, join := range []bool{false, true} {
		for _, bucket := range []string{"load_replicas", "serve_override", "hardstate", "entries", "snapshot", "applied", "state", "identity"} {
			t.Run(bucket+map[bool]string{false: "/found", true: "/join"}[join], func(t *testing.T) {
				t.Parallel()
				opts := optionsForTest(t)
				opts.Join = join
				if join {
					opts.Identity.MemberID = 0
					opts.Founders = nil
					opts.Node.Type = "worker"
					opts.JoinSeeds = []string{"127.0.0.1:12345"}
				}
				s, err := metastore.Open(opts.DBPath)
				if err != nil {
					t.Fatal(err)
				}
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
				db, err := bolt.Open(opts.DBPath, 0o600, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err = db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte(bucket)).Put([]byte("orphan"), []byte("retained")) }); err != nil {
					t.Fatal(err)
				}
				if err = db.Close(); err != nil {
					t.Fatal(err)
				}
				before, err := os.ReadFile(opts.DBPath)
				if err != nil {
					t.Fatal(err)
				}
				if group, err := New(opts); !errors.Is(err, fault.ErrUnreadable) {
					if group != nil {
						_ = group.Stop(t.Context())
					}
					t.Fatal("retained state accepted for new identity", err)
				}
				after, err := os.ReadFile(opts.DBPath)
				if err != nil || !bytes.Equal(before, after) {
					t.Fatal("refused constructor mutated retained store", err)
				}
				conn, err := net.DialTimeout("tcp", opts.Network.Listener.Addr().String(), time.Second)
				if err != nil {
					t.Fatal("refused constructor took listener ownership", err)
				}
				_ = conn.Close()
			})
		}
	}
}

func TestMetaConstructorAllowsAnEmptyVersionedStore(t *testing.T) {
	t.Parallel()
	opts := optionsForTest(t)
	s, err := metastore.Open(opts.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	g, err := New(opts)
	if err != nil {
		t.Fatal("empty initialized store refused", err)
	}
	if err = g.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	id, found, err := metastore.LoadIdentity(opts.DBPath)
	if err != nil || !found || id.MemberID != 1 || id.NodeName != opts.Identity.NodeName {
		t.Fatal(id, found, err)
	}
}
