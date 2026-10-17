package metaservice

import (
	"context"
	"errors"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

// These are existing service/FSM boundaries, not a second persistence owner.
// Keeping the sequence together makes the pruning race and its single recheck
// explicit at the live service/FSM boundary.
type collectionReportAccess struct {
	check   func(context.Context, storage.Identity) (metaengine.CollectionCheck, error)
	courier func(storage.Identity, uint64, topology.Identity) error
	propose func(context.Context, any) (metaengine.ApplyResult, error)
}

func (g *Group) collectionAccess() collectionReportAccess {
	return collectionReportAccess{check: g.collectionCheck,
		courier: g.state().ValidateCleanupCourier, propose: g.propose}
}

func (g *Group) collectionCheck(ctx context.Context, id storage.Identity) (metaengine.CollectionCheck, error) {
	var result metaengine.CollectionCheck
	err := g.readControl(ctx, metaengine.QueryRequest{Kind: "collection_check", CollectionIdentity: &id}, &result)
	if err == nil && result.AppliedIndex == 0 {
		err = fault.ErrUnreadable
	}
	return result, err
}

func (a collectionReportAccess) accept(ctx context.Context,
	p metaengine.RecordCollectionProposal) (authoritystore.CollectionResult, error) {
	check, err := a.check(ctx, p.Receipt.Identity)
	if err != nil {
		return authoritystore.CollectionResult{}, err
	}
	if check.Untracked {
		return settledCollection(p.Receipt, check), nil
	}
	err = a.courier(p.Receipt.Identity, p.Receipt.MemberID, p.Courier)
	if err != nil {
		return a.recheckMissing(ctx, p.Receipt, err)
	}
	result, err := a.propose(ctx, p)
	if err != nil {
		return a.recheckMissing(ctx, p.Receipt, err)
	}
	if !result.Accepted || result.Collection == nil || result.Collection.Settled ||
		result.Collection.AppliedIndex == 0 || !result.Collection.Receipt.Equal(p.Receipt) {
		return authoritystore.CollectionResult{}, fault.ErrUnreadable
	}
	return *result.Collection, nil
}

func settledCollection(receipt authoritystore.CollectionReceipt,
	check metaengine.CollectionCheck) authoritystore.CollectionResult {
	return authoritystore.CollectionResult{Receipt: receipt, AppliedIndex: check.AppliedIndex, Settled: true}
}

func (a collectionReportAccess) recheckMissing(ctx context.Context, receipt authoritystore.CollectionReceipt,
	err error) (authoritystore.CollectionResult, error) {
	if !errors.Is(err, fault.ErrHistoryUnavailable) {
		return authoritystore.CollectionResult{}, err
	}
	check, checkErr := a.check(ctx, receipt.Identity)
	if checkErr != nil {
		return authoritystore.CollectionResult{}, checkErr
	}
	if check.Untracked {
		return settledCollection(receipt, check), nil
	}
	return authoritystore.CollectionResult{}, err
}
