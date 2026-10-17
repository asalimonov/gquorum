//go:build functional

package metaservice

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/operator"
	"github.com/asalimonov/gquorum/topology"
)

type interruptedTransferOwner struct {
	operator.GroupEffects
	mu         sync.Mutex
	grants     []topology.AuthorityGrant
	entered    chan struct{}
	firstError error
}

func (o *interruptedTransferOwner) DispatchEffect(ctx context.Context, grant topology.AuthorityGrant,
	resource topology.AuthorityResource, target topology.AuthorityTarget) (topology.EffectReceipt, error) {
	if grant.Action != nil && grant.Action.Kind != topology.StepTransferLeader {
		return o.GroupEffects.DispatchEffect(ctx, grant, resource, target)
	}
	if err := operator.ValidateEffectTarget(grant, resource, target); err != nil {
		return topology.EffectReceipt{}, err
	}
	o.mu.Lock()
	o.grants = append(o.grants, grant.Clone())
	first := len(o.grants) == 1
	o.mu.Unlock()
	if first {
		close(o.entered)
		if o.firstError != nil {
			return topology.EffectReceipt{}, o.firstError
		}
		<-ctx.Done()
		return topology.EffectReceipt{}, ctx.Err()
	}
	return protocolEffectReceipts(grant)[0], nil
}

func TestDirectTransferResumesAfterCanceledCaller(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"canceled", "recipient election"} {
		t.Run(mode, func(t *testing.T) { t.Parallel(); testDirectTransferRetry(t, mode) })
	}
}

func testDirectTransferRetry(t *testing.T, mode string) {
	t.Helper()
	owner := &verifiedLoadController{groups: map[string]topology.Group{}, hosts: map[string]map[string]uint64{}}
	interrupted := &interruptedTransferOwner{GroupEffects: owner, entered: make(chan struct{})}
	if mode == "recipient election" {
		interrupted.firstError = fault.ErrNotLeader
	}
	groups := singleMetaFixtureWith(t, func(o *Options) { o.LoadGroups = interrupted })
	g := readyPublic(t, groups)
	owner.mu.Lock()
	owner.meta = groups
	owner.mu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	doc, err := g.Stage(ctx, loadDocument(t, g), 1)
	if err != nil {
		t.Fatal(err)
	}
	op, err := g.Approve(ctx, doc.Version)
	if err != nil {
		t.Fatal(err)
	}
	if status, err := g.WaitOperation(ctx, op); err != nil || status.State != topology.OperationSucceeded {
		t.Fatal(status, err)
	}
	waitPublic(t, func() bool { return g.state().Views().FullyConverged.Version == doc.Version })
	caller, cancelCaller := context.WithCancel(ctx)
	defer cancelCaller()
	result := make(chan error, 1)
	go func() { result <- g.TransferLeader(caller, "load-one", "n1") }()
	select {
	case <-interrupted.entered:
	case <-ctx.Done():
		t.Fatal("transfer did not reach resource owner", ctx.Err())
	}
	retries := 1
	var expected error
	if mode == "canceled" {
		cancelCaller()
		expected = context.Canceled
		retries = 2
	}
	if err := <-result; !errors.Is(err, expected) {
		t.Fatal(err)
	}
	for range retries {
		if err := g.TransferLeader(ctx, "load-one", "n1"); err != nil {
			t.Fatal("retry stranded transfer authority", err)
		}
	}
	interrupted.mu.Lock()
	defer interrupted.mu.Unlock()
	if len(interrupted.grants) != 3 || !interrupted.grants[0].Equal(interrupted.grants[1]) || interrupted.grants[2].Key == interrupted.grants[1].Key {
		t.Fatal("incorrect transfer retry boundaries", interrupted.grants)
	}
	if receipts, err := g.state().EffectReceipts(ctx, interrupted.grants[0]); err != nil || len(receipts) != 1 {
		t.Fatal("original authority was not consumed", receipts, err)
	}
}

// Metadata authority installation and effect initiation share their own Raft
// loop with the proposals that issue the grant and record its fence. Direct
// callers must not get stuck repeatedly reinserting those metadata proposals.
func TestMetaTransferWaitsForNativePhaseAfterMetadataProposals(t *testing.T) {
	t.Parallel()
	groups := singleMetaFixture(t)
	g := readyPublic(t, groups)
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	for iteration := range 4 {
		if err := g.TransferLeader(ctx, topology.MetaGroupID, "n1"); err != nil {
			t.Fatal("metadata persistence boundary stranded direct authority", iteration, err)
		}
	}
	if err := g.ReadBarrier(ctx); err != nil {
		t.Fatal(err)
	}
	// A grant's revision is its metadata apply index. Inspect retained group
	// grants without mistaking the separate host-floor reader for group state.
	var current *topology.AuthorityGrant
	for index := g.engine.Load().Status().Applied; index > 0; index-- {
		grant, found := g.state().AuthorityGrant(topology.AuthorityKey{ClusterID: g.Identity().ClusterID, GroupID: topology.MetaGroupID, Revision: index})
		if found {
			current = &grant
			break
		}
	}
	if current == nil || current.Purpose != topology.AuthorityTransfer {
		t.Fatal("successful transfer lost its installed grant", current)
	}
	receipts, err := g.state().EffectReceipts(ctx, *current)
	if err != nil || len(receipts) != 1 || receipts[0].Validate(*current) != nil || g.engine.Load().Status().LeaderID != 1 {
		t.Fatal("success preceded exact durable intent and target leadership", receipts, err)
	}
}
