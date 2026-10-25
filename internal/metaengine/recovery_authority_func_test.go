//go:build functional

package metaengine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/internal/raftstate"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/storage/walstore"
	"github.com/asalimonov/gquorum/topology"
	"go.etcd.io/raft/v3/raftpb"
)

type recoveredCatalogMachine struct{}

func (recoveredCatalogMachine) Restore(ctx context.Context, _ []byte) error { return ctx.Err() }
func (recoveredCatalogMachine) Snapshot(ctx context.Context) ([]byte, error) {
	return []byte("portable-B"), ctx.Err()
}
func (recoveredCatalogMachine) Apply(ctx context.Context, _ raftengine.Command) (raftengine.Result, error) {
	return raftengine.Result{}, ctx.Err()
}

type recoveredCatalogTransport struct{}

func (recoveredCatalogTransport) Send(ctx context.Context, _ []raftpb.Message) error {
	return ctx.Err()
}

func catalogCheck(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func applyRecoveryCatalog(t *testing.T, m *Machine, proposal any) ApplyResult {
	t.Helper()
	raw, err := Encode(proposal)
	catalogCheck(t, err)
	index, term := m.AppliedState()
	result, err := m.Apply(t.Context(), raftengine.Command{Index: index + 1, Term: term, Data: raw,
		ConfState: raftpb.ConfState{Voters: []uint64{5}}})
	catalogCheck(t, err)
	var decoded ApplyResult
	catalogCheck(t, json.Unmarshal(result.Data, &decoded))
	if !decoded.Accepted {
		t.Fatal("catalog rejected", decoded)
	}
	return decoded
}

func recoveredCatalogGrant(t *testing.T) (*Machine, topology.AuthorityGrant) {
	t.Helper()
	raw, err := os.ReadFile("testdata/recovery-schema1/importing.json")
	catalogCheck(t, err)
	var source scopedState
	catalogCheck(t, json.Unmarshal(raw, &source))
	m, err := NewMachine(source.Rules, nil)
	catalogCheck(t, err)
	catalogCheck(t, m.Restore(t.Context(), raw))
	op := m.scoped.indexes.operations[m.scoped.indexes.first[2]].Clone()
	action := scopedPlanAction(&op, op.Plan.Steps[0])
	applyRecoveryCatalog(t, m, TransitionOperationProposal{ProposalID: "start",
		Transition: topology.OperationTransition{OperationID: op.Status.ID, Version: 2, Attempt: 1,
			GroupID: "shard0", StepSeq: action.StepSeq, State: topology.OperationRunning, At: op.UpdatedAt.Add(time.Second)}})
	result := applyRecoveryCatalog(t, m, BeginEffectProposal{Action: action, ProposalID: "create"})
	if result.Grant == nil {
		t.Fatal("no ordinary creation grant")
	}
	return m, *result.Grant
}

func TestRecoveredCreationReceiptValidatesThroughRealCatalogAfterRestartAndPrune(t *testing.T) {
	t.Parallel()
	m, grant := recoveredCatalogGrant(t)
	group := m.RecoveryState().Groups[0]
	store, err := walstore.Open(walstore.Options{RootDir: t.TempDir(), NodeName: "n1", ClusterID: "recovered"})
	catalogCheck(t, err)
	t.Cleanup(func() { catalogCheck(t, store.Close()) })
	catalogCheck(t, store.BindReplica(t.Context(), group.Destination))
	data, err := raftstate.EncodeSnapshot(raftstate.Snapshot{Schema: raftstate.Schema, ClusterID: "recovered",
		GroupID: "shard0", Application: []byte("source"), Authority: raftstate.AuthorityState{Grant: &grant, InstallIndex: 1, InstallTerm: 1}})
	catalogCheck(t, err)
	catalogCheck(t, store.SaveSnapshot(t.Context(), "shard0", raftpb.Snapshot{Data: data,
		Metadata: raftpb.SnapshotMetadata{Index: 1, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{7}}}}))
	catalogCheck(t, store.SaveReady(t.Context(), "shard0", storage.ReadyWrite{Entries: []raftpb.Entry{{Index: 2, Term: 1}},
		HardState: raftpb.HardState{Term: 1, Commit: 2}}))
	opts := raftengine.Options{Store: store, Machine: recoveredCatalogMachine{}, Transport: recoveredCatalogTransport{},
		NodeName: "n1", ClusterID: "recovered", GroupID: "shard0", ReplicaID: 7, MemberID: 5,
		InstanceID: "recovered-run", ManagedAuthority: true, Mode: raftengine.Joining,
		Timing: raftengine.Timing{TickInterval: 10 * time.Millisecond}}
	core, err := raftengine.NewContext(t.Context(), opts)
	catalogCheck(t, err)
	catalogCheck(t, core.Start(t.Context()))
	t.Cleanup(func() { catalogCheck(t, core.Stop(context.Background())) })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	for {
		state, err := core.LeadershipState()
		catalogCheck(t, err)
		if state.Epoch.Context != nil {
			break
		}
		select {
		case <-state.Changed:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	catalogCheck(t, core.ReadBarrier(ctx))
	fence, err := core.InstallAuthority(ctx, grant)
	catalogCheck(t, err)
	receipt, err := core.ObserveAuthorizedEffect(ctx, grant)
	catalogCheck(t, err)
	if fence.AppliedIndex != 1 || fence.AppliedTerm != 1 || receipt.AppliedIndex <= 2 || receipt.AppliedTerm <= 1 {
		t.Fatal("real creation evidence has wrong position", fence, receipt)
	}
	var host topology.EffectReceipt
	for _, r := range fixtureEffectReceipts(grant) {
		if r.Resource.Kind == topology.AuthorityHost {
			host = r
		}
	}
	for _, r := range []topology.EffectReceipt{host, receipt} {
		applyRecoveryCatalog(t, m, RecordEffectProposal{Receipt: r, At: time.Unix(200, 0), ProposalID: "receipt"})
	}
	catalogCheck(t, core.Snapshot(ctx))
	retained, err := store.Load(ctx, "shard0")
	catalogCheck(t, err)
	raw, err := retained.Snapshot.Marshal()
	catalogCheck(t, err)
	digest := sha256.Sum256(raw)
	evidence := model.Consumed{Identity: group.Destination, Checkpoint: model.Position{Index: retained.Snapshot.Metadata.Index,
		Term: retained.Snapshot.Metadata.Term}, SnapshotDigest: hex.EncodeToString(digest[:]), HostReceipt: host, GroupReceipt: receipt}
	applyRecoveryCatalog(t, m, model.GroupConsumedProposal{RecoveryID: m.RecoveryState().RecoveryID,
		PlanDigest: m.RecoveryState().PlanDigest, GroupID: "shard0", Evidence: evidence, ProposalID: "consumed"})
	catalogCheck(t, core.Stop(ctx))
	opts.InstanceID = "restart"
	restarted, err := raftengine.NewContext(ctx, opts)
	catalogCheck(t, err)
	catalogCheck(t, restarted.Start(t.Context()))
	t.Cleanup(func() { catalogCheck(t, restarted.Stop(context.Background())) })
	catalogCheck(t, restarted.WaitReady(ctx))
	for {
		state, err := restarted.LeadershipState()
		catalogCheck(t, err)
		if state.Epoch.Context != nil {
			break
		}
		select {
		case <-state.Changed:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	catalogCheck(t, restarted.ReadBarrier(ctx))
	again, err := restarted.AuthorityReceipt(ctx, grant.Key)
	catalogCheck(t, err)
	if !again.Found || !reflect.DeepEqual(again.Receipt, receipt) {
		t.Fatal("restart changed creation receipt", again)
	}
	catalogCheck(t, m.scoped.pruneRetainedProofs())
	snapshot, err := m.Snapshot(ctx)
	catalogCheck(t, err)
	restored, err := NewMachine(m.rules, nil)
	catalogCheck(t, err)
	catalogCheck(t, restored.Restore(ctx, snapshot))
	receipts, err := restored.EffectReceipts(ctx, grant)
	catalogCheck(t, err)
	if len(receipts) != 2 {
		t.Fatal("prune lost actual creation pair", receipts)
	}
}
