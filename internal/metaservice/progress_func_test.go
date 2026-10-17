//go:build functional

package metaservice

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

type progressNoProbe struct{ *verifiedLoadController }

func (*progressNoProbe) Status(context.Context, string) (topology.GroupStatus, error) {
	panic("progress attempted a remote observation refresh")
}

func TestProgressDecoratesOnlyCachedReturnedRows(t *testing.T) {
	t.Parallel()
	f := newMutationFixture(t)
	f.g.opts.LoadGroups = &progressNoProbe{}
	f.command(t, metaengine.GrantSession{SessionID: "progress", NodeName: "n1", InstanceID: "current", TTLSeconds: 30})
	f.command(t, metaengine.AcquireLock{Key: "node/n1", SessionID: "progress"})
	identity, found := f.g.machine.ProcessIdentity("n1")
	if !found {
		t.Fatal("fixture process not current")
	}
	at := time.Unix(100, 0).UTC()
	observed := topology.GroupStatus{GroupID: "work", Found: true, Reporter: identity, ReporterReplicaID: 1,
		SourceVersion: 1, ReceivedAt: at, LeaderID: 3, LeaderNodeName: "untrusted",
		ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}, Progress: map[uint64]topology.ReplicaProgress{3: {Match: 22}}}
	f.g.lastLoadStatus = map[string]topology.GroupStatus{"work": observed, "unreturned": {GroupID: "unreturned"}}
	active, err := f.g.machine.FullyConvergedDocument()
	if err != nil {
		t.Fatal(err)
	}
	declaration := active.Group("work")
	rows := []topology.GroupProgress{{GroupID: "work", Converged: declaration, ConvergedVersion: 1},
		{GroupID: "unknown", Observation: &observed}}
	f.g.decorateProgress(rows)
	got := rows[0].Observation
	if got == nil || got.ReceivedAt != at || got.SourceVersion != 1 || got.Reporter != identity ||
		got.ReporterReplicaID != 1 || got.LeaderID != 3 || got.LeaderNodeName != "n3" || rows[1].Observation != nil {
		t.Fatal("cache decoration changed identity/freshness or invented unknown", rows)
	}
	got.ConfState.Voters[0], got.Progress[3] = 99, topology.ReplicaProgress{}
	if !reflect.DeepEqual(f.g.lastLoadStatus["work"], observed) {
		t.Fatal("cache aliased returned page")
	}

	// A newer desired/approved declaration cannot relabel the old observation.
	desired := active.Clone()
	rows[0].Desired = desired.Group("work")
	rows[0].Desired.Labels = map[string]string{"unapproved": "new"}
	rows[0].Approved = rows[0].Desired
	f.g.decorateProgress(rows)
	if got := rows[0].Observation; got == nil || got.SourceVersion != 1 || got.ReceivedAt != at {
		t.Fatal("topology decoration renewed sample", got)
	}
	oldLeader := observed.Clone()
	oldLeader.LeaderID = 999
	f.g.lastLoadStatus["work"] = oldLeader
	f.g.decorateProgress(rows)
	if got := rows[0].Observation; got == nil || got.LeaderID != 999 || got.LeaderNodeName != "" || got.ReceivedAt != at {
		t.Fatal("unknown leader ID was erased or renamed", got)
	}
	rows[0].ConvergedVersion++
	f.g.decorateProgress(rows)
	if rows[0].Observation != nil {
		t.Fatal("old observation relabeled to new source")
	}
	rows[0].ConvergedVersion = 1
	f.g.lastLoadStatus["work"] = observed

	f.command(t, metaengine.RevokeSession{SessionID: "progress"})
	f.command(t, metaengine.GrantSession{SessionID: "next", NodeName: "n1", InstanceID: "successor", TTLSeconds: 30})
	f.command(t, metaengine.AcquireLock{Key: "node/n1", SessionID: "next"})
	f.g.decorateProgress(rows)
	if rows[0].Observation != nil {
		t.Fatal("old process sample attached after successor became current")
	}
}

func TestReadProgressUsesSafeReadAndCancellation(t *testing.T) {
	t.Parallel()
	g := singleMetaFixtureWith(t, func(o *Options) { o.LoadGroups = &progressNoProbe{} })[0]
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := g.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	page, err := g.ReadProgress(ctx, topology.ProgressQuery{GroupID: topology.MetaGroupID})
	if err != nil || page.AppliedIndex == 0 || page.TargetVersion != 1 || len(page.Groups) != 1 ||
		page.Groups[0].ConvergedVersion != 1 || page.Groups[0].Converged == nil {
		t.Fatal("safe read lost current scoped progress", page, err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := g.ReadProgress(canceled, topology.ProgressQuery{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := g.ReadProgress(ctx, topology.ProgressQuery{Limit: 1001}); !errors.Is(err, fault.ErrInvalid) {
		t.Fatal(err)
	}
	if err := g.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := g.ReadProgress(ctx, topology.ProgressQuery{}); !errors.Is(err, fault.ErrStopped) {
		t.Fatal(err)
	}
}

func TestReadProgressCannotReturnMetadataWithoutQuorum(t *testing.T) {
	t.Parallel()
	// One participant and two absent peers exercise the real safe-read boundary;
	// no multi-node application or consensus fixture is started in this suite.
	g := singleMetaFixtureWith(t, func(o *Options) {
		o.Limits.MaxNodes, o.VoterConfig.TargetVoters = 3, 3
		for i, name := range []string{"n2", "n3"} {
			o.Founders = append(o.Founders, FoundingMember{Node: topology.Node{Name: name, Type: "worker"},
				MemberID: uint64(i + 2), Address: "127.0.0.1:1"})
		}
	})[0]
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	page, err := g.ReadProgress(ctx, topology.ProgressQuery{})
	if err == nil || errors.Is(err, fault.ErrUnsupported) || page.AppliedIndex != 0 {
		t.Fatal("read bypassed the quorum barrier", page, err)
	}
}
