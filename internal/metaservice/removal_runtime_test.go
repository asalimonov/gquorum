package metaservice

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/events"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

// Verify from the callback itself, before returning and allowing Stop to close
// its store. This pins the real apply-loop call site and the durable ordering.
func verifyRemovalStore(g *Group, reason string) error {
	id := g.currentIdentity()
	if !strings.Contains(reason, fmt.Sprintf(`%q member %d`, id.NodeName, id.MemberID)) {
		return fmt.Errorf("wrong removed identity: %s", reason)
	}
	cs, found, err := g.store.AppliedConfState()
	if err != nil || !found || containsMember(cs, id.MemberID) {
		return fmt.Errorf("removal not durable: %v %v %v", cs, found, err)
	}
	if seen, err := g.store.MemberSeen(); err != nil || !seen {
		return fmt.Errorf("membership history missing: %v %v", seen, err)
	}
	data, err := g.store.LoadState()
	if err != nil {
		return err
	}
	m := metaengine.NewMachine(g.rules, nil)
	if err := m.Restore(context.Background(), data); err != nil {
		return err
	}
	target := m.Target()
	if n := target.Node(id.NodeName); n == nil || n.State != topology.NodeExcluded {
		return fmt.Errorf("approved exclusion not durable")
	}
	index, err := g.store.AppliedIndex()
	if err != nil {
		return err
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.selfRemoval == nil || index < g.selfRemoval.AppliedIndex {
		return fmt.Errorf("notification precedes persisted applied index")
	}
	return nil
}

func TestSelfRemoval_FiresThroughApplyEntry(t *testing.T) {
	if testing.Short() {
		t.Skip("functional test; run make test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	results := make(chan error, 4)
	var groups []*Group
	groups = publicFoundingClusterWith(t, 3, func(o *Options) {
		name := o.Identity.NodeName
		o.OnSelfRemoved = func(_ context.Context, reason string) error {
			for _, g := range groups {
				if g.currentIdentity().NodeName == name {
					err := verifyRemovalStore(g, reason)
					results <- err
					g.RequestStop()
					return err
				}
			}
			return fmt.Errorf("missing handle %s", name)
		}
	})
	leader := readyPublic(t, groups)
	var departing *Group
	for _, g := range groups {
		if g != leader {
			departing = g
			break
		}
	}
	name := departing.currentIdentity().NodeName
	drain, err := leader.DrainNode(ctx, name, MutationOptions{ExpectedVersion: 1, Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = leader.WaitOperator(ctx, drain.OperatorID); err != nil {
		t.Fatal(err)
	}
	waitPublic(t, func() bool { return leader.state().Active().Version == drain.Document.Version })
	if containsMember(departing.engine.Load().Status().ConfState, departing.currentIdentity().MemberID) == false {
		t.Fatal("drain removed meta vote")
	}
	select {
	case err := <-results:
		t.Fatal("drain notified removal", err)
	default:
	}
	exclude, err := leader.ExcludeNode(ctx, name, MutationOptions{ExpectedVersion: drain.Document.Version})
	if err != nil {
		t.Fatal(err)
	}
	waitPublic(t, func() bool { return departing.state().Latest().Version == exclude.Document.Version })
	if departing.state().Target().Version != drain.Document.Version {
		t.Fatal("staged exclusion was approved")
	}
	select {
	case err := <-results:
		t.Fatal("staged exclusion notified removal", err)
	default:
	}
	operator, err := leader.Apply(ctx, exclude.Document.Version)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = leader.WaitOperator(ctx, operator); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-results:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		for _, g := range groups {
			t.Logf("%s status=%+v target=%d active=%d", g.currentIdentity().NodeName, g.engine.Load().Status(), g.state().Target().Version, g.state().Active().Version)
		}
		t.Fatal("real Raft removal did not notify", ctx.Err())
	}
	if err = departing.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if departing.engine.Load().Running() {
		t.Fatal("self-removal teardown left Raft running")
	}
	status := readStatus(t, departing)
	if status.SelfRemoval == nil || !status.SelfRemoval.Fenced || !status.SelfRemoval.Notified || status.SessionLive || status.ErrorCode != string(fault.IdentityMismatch) {
		t.Fatal(status)
	}
	status.SelfRemoval.Reason = "mutated"
	if readStatus(t, departing).SelfRemoval.Reason == "mutated" {
		t.Fatal("status aliases local fence")
	}
	select {
	case err := <-results:
		t.Fatal("duplicate or other-node notification", err)
	default:
	}
	// Model compaction after removal, eliminating the earlier membership entries
	// from recovery. Only the local sticky marker can prove prior participation.
	store, err := metastore.Open(departing.opts.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	state, err := store.LoadState()
	if err != nil {
		t.Fatal(err)
	}
	index, err := store.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	cs, _, err := store.AppliedConfState()
	if err != nil {
		t.Fatal(err)
	}
	hs, entries, snap, err := store.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	term := snap.Metadata.Term
	for _, e := range entries {
		if e.Index == index {
			term = e.Term
		}
	}
	if err = store.InstallSnapshot(raftpb.Snapshot{Data: state, Metadata: raftpb.SnapshotMetadata{Index: index, Term: term, ConfState: cs}}, hs); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}

	// Recovery after a persisted removal must re-notify idempotently, including
	// a snapshot whose membership never contains the retired member anymore.
	opts := restartOptions(t, departing)
	opts.Founders = nil
	opts.Identity.InstanceID = "restarted-removed-process"
	var restarted *Group
	opts.OnSelfRemoved = func(_ context.Context, reason string) error {
		err := verifyRemovalStore(restarted, reason)
		results <- err
		restarted.RequestStop()
		return err
	}
	restarted, err = New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { restarted.Stop(context.Background()) })
	if err = restarted.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-results:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("persisted removal was lost on restart", ctx.Err())
	}
	if err = restarted.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSelfRemovalCallbackFailureStaysFencedAndIsLocallyObservable(t *testing.T) {
	t.Parallel()
	bus, journal := eventFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := bus.Subscribe(ctx, events.Filter{}, 32)
	if err != nil {
		t.Fatal(err)
	}
	g, fired := removalFixture(t, func(context.Context, string) error { return errors.New("application teardown failed") })
	g.opts.Events = bus
	g.observeRemoval(g.identity, removalDocument(topology.NodeExcluded, 2), raftpb.ConfState{Voters: []uint64{1}}, 42)
	awaitRemoval(t, fired)
	g.removalWG.Wait()
	g.mu.RLock()
	status := *g.selfRemoval
	g.mu.RUnlock()
	if !status.Fenced || !status.Notified || status.ErrorCode != "callback_failed" || g.OwnSessionLive() {
		t.Fatal(status)
	}
	if err := g.Authorize(context.Background(), topology.AuthorizedStep{}); !errors.Is(err, fault.ErrStopped) {
		t.Fatal("callback failure restored authority", err)
	}
	eventBarrier(t, bus, stream)
	var removal, failure bool
	for _, e := range journalEvents(t, journal, events.Filter{}) {
		if e.Type == events.SelfRemoved {
			removal = e.MemberID == 2 && e.GroupID == "_meta" && e.Version == 4
		}
		if e.Type == events.RuntimeError && e.Code == "self_removal_callback_failed" {
			failure = true
		}
	}
	if !removal || !failure {
		t.Fatal("missing local removal events", removal, failure)
	}
}
