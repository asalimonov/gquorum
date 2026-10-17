//go:build functional

package metaservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/topology"
	bolt "go.etcd.io/bbolt"
)

func TestControlStoreRejectsOldMetadataFormatWithoutRewrite(t *testing.T) {
	t.Parallel()
	g := readyPublic(t, singleMetaFixture(t))
	opts := g.opts
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := g.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(opts.DBPath, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("format")).Put([]byte("schema_version"), []byte("2"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(opts.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	control := &Group{opts: opts, rules: g.rules}
	if err := control.openControlStore(opts.Identity); !errors.Is(err, fault.ErrUnsupported) {
		t.Fatal("control-only open accepted old format", err)
	}
	after, err := os.ReadFile(opts.DBPath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("control-only rejection changed retained evidence", err)
	}
}

func TestStoppedRetainedJoinUsesControlStoreWithoutReopeningRaft(t *testing.T) {
	t.Parallel()
	g := readyPublic(t, singleMetaFixture(t))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	issued, err := g.propose(ctx, metaengine.GrantTransferProposal{ExpectedSourceVersion: g.state().Views().FullyConverged.Version, GroupID: topology.MetaGroupID, NodeName: "n1", ReplicaID: 1, RequestID: "retained-proof"})
	if err != nil {
		t.Fatal(err)
	}
	doc := fixtureConvergedDocument(t, g.state())
	opts := g.opts
	if err = g.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	handler := &joinProtocolHandler{}
	address := joinProtocolServer(t, handler, opts.Network.ClusterCredential)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	opts.Network.Listener, opts.Network.AdvertiseAddress = listener, listener.Addr().String()
	opts.Founders, opts.Join, opts.JoinSeeds = nil, true, []string{address}
	opts.Identity.InstanceID = "retained-reader"
	var authorizations atomic.Int32
	opts.BeforeOpenEngine = func(topology.Identity) error { authorizations.Add(1); return fault.ErrAuthorityFenced }
	response := joinResponse{Rules: g.rules, MemberID: 1, Existing: true, Granted: doc, LeaderAddress: address,
		Members: []joinMember{{Name: "n1", MemberID: 1, Address: listener.Addr().String()}}}
	data, _ := json.Marshal(response)
	handler.reply = &metapb.Reply{Data: data}
	retained, err := New(opts)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		bounded, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := retained.Stop(bounded); err != nil {
			t.Error(err)
		}
	})
	if retained.engine.Load() != nil || !retained.controlOnly || retained.store == nil {
		t.Fatal("stopped member acquired participation")
	}
	before, err := retained.store.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if err = retained.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	for {
		retained.mu.RLock()
		parked := retained.lifecycle == "parked"
		retained.mu.RUnlock()
		if parked {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("same-member response did not remain parked")
		case <-time.After(time.Millisecond):
		}
	}
	if authorizations.Load() != 1 || retained.engine.Load() != nil || retained.IsLeader() {
		t.Fatal("same-member join response reopened stopped participation", authorizations.Load())
	}
	if _, err = retained.ReadTopology(ctx); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("control-only owner served ordinary read", err)
	}
	if err = retained.VerifyPreparedMeta(ctx, retained.Identity()); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("control store counted as prepared replay", err)
	}
	if err = retained.ValidateGrant(ctx, *issued.Grant); err != nil {
		t.Fatal("retained immutable proof unavailable", err)
	}
	after, err := retained.store.LoadAll()
	if err != nil || !reflect.DeepEqual(before.HardState, after.HardState) || !reflect.DeepEqual(before.Entries, after.Entries) || !reflect.DeepEqual(before.Snapshot, after.Snapshot) {
		t.Fatal("control-only join mutated retained Raft evidence", err)
	}
	if retained.Identity().MemberID != 1 || retained.Identity().InstanceID != opts.Identity.InstanceID {
		t.Fatal("control-only join changed member identity", retained.Identity())
	}
}
