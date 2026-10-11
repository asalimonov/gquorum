//go:build functional

package metaservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"google.golang.org/protobuf/proto"
)

func TestSessionRPCRejectsAnotherProcessInstanceAndReservedNodeLocks(t *testing.T) {
	t.Parallel()
	g := readyPublic(t, singleMetaFixture(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := g.Activate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	good := g.transport.Header()
	bad := proto.Clone(good).(*metapb.Header)
	bad.InstanceId = "other-process"
	for _, proposal := range []any{
		metaengine.GrantSession{SessionID: "foreign", NodeName: s.NodeName, InstanceID: s.InstanceID, TTLSeconds: 3},
		metaengine.AcquireLock{Key: "application", SessionID: s.ID},
		metaengine.RevokeSession{SessionID: s.ID},
	} {
		data, err := metaengine.Encode(proposal)
		if err != nil {
			t.Fatal(err)
		}
		if err = metatransport.ReplyError(g.Call(ctx, "ProposeMeta", bad, data)); !errors.Is(err, fault.ErrIdentityMismatch) {
			t.Fatal("accepted wrong instance", err)
		}
	}
	if err = metatransport.ReplyError(keepAliveCall(g, bad, s.ID, s.InstanceID)); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	data, err := metaengine.Encode(metaengine.AcquireLock{Key: "node/other", SessionID: s.ID})
	if err != nil {
		t.Fatal(err)
	}
	if err = metatransport.ReplyError(g.Call(ctx, "ProposeMeta", good, data)); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("reserved node lock accepted", err)
	}
	if _, ok := g.state().Session(s.ID); !ok {
		t.Fatal("rejected RPC changed session")
	}
	if _, ok := g.state().Session("foreign"); ok {
		t.Fatal("rejected RPC granted session")
	}
}
