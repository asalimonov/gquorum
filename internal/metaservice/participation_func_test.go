//go:build functional

package metaservice

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metatransport"
)

func TestParticipationFenceJoinsEffectsAndRaftBeforeProtocolShutdown(t *testing.T) {
	t.Parallel()
	g := readyPublic(t, singleMetaFixture(t))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	effectCtx, releaseEffect, err := g.beginEffect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var effectOnce sync.Once
	defer effectOnce.Do(releaseEffect)
	rpcCtx, releaseRPC, err := g.beginRPC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var rpcOnce sync.Once
	defer rpcOnce.Do(releaseRPC)
	short, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	err = g.FenceParticipation(short)
	stop()
	if !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal("participation fence abandoned owned work", err)
	}
	select {
	case <-effectCtx.Done():
	case <-ctx.Done():
		t.Fatal("ordinary work was not canceled")
	}
	if rpcCtx.Err() != nil {
		t.Fatal("participation fence canceled the protocol owner", rpcCtx.Err())
	}
	effectOnce.Do(releaseEffect)
	if err := g.FenceParticipation(ctx); err != nil {
		t.Fatal(err)
	}
	if g.engine.Load().Running() {
		t.Fatal("participation fence left Raft running")
	}
	if _, err := g.ReadTopology(ctx); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("fenced participant served an authoritative data read", err)
	}
	if _, _, err := g.beginEffect(ctx); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("fenced participant admitted ordinary work", err)
	}
	if _, err := g.store.LoadState(); err != nil {
		t.Fatal("participation fence closed durable authority evidence", err)
	}
	// The real authenticated protocol still answers, but the stopped engine
	// cannot report itself as a live participant. No consensus is running here.
	reply, err := g.transport.CallAddress(ctx, g.opts.Network.AdvertiseAddress, "IdentityPing", nil)
	if err != nil || !errors.Is(metatransport.ReplyError(reply), fault.ErrUnavailable) {
		t.Fatal("participation fence closed the protocol transport", reply, err)
	}
	if err := g.Start(ctx); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("fenced participant restarted", err)
	}
	short, stop = context.WithTimeout(ctx, 20*time.Millisecond)
	err = g.Quiesce(short)
	stop()
	if !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal("full quiescence abandoned an owned protocol call", err)
	}
	select {
	case <-rpcCtx.Done():
	case <-ctx.Done():
		t.Fatal("full quiescence did not cancel its protocol owner")
	}
	rpcOnce.Do(releaseRPC)
	if err := g.Quiesce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := g.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := g.store.LoadState(); err == nil {
		t.Fatal("final stop retained the metadata database")
	}
}
