//go:build functional

package managed

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/lifecycle"
	"github.com/asalimonov/gquorum/storage"
	"github.com/asalimonov/gquorum/topology"
)

type collectionReleaseStore struct {
	storage.RetirementStore
	forget func(context.Context, storage.RetiredReplica) error
}

func (s *collectionReleaseStore) ForgetCollection(ctx context.Context, retired storage.RetiredReplica) error {
	return s.forget(ctx, retired)
}

func collectedHostFixture(t *testing.T, f *fixture) (*Host, authoritystore.CollectionReceipt) {
	t.Helper()
	h, row := preparedHostCollection(t, f)
	must(t, f.store.Collect(deadline(t), row.Retired))
	receipt, err := f.cfg.Authority().CompleteCollection(deadline(t), row.Retired.Identity,
		row.Sequence, time.Now().UTC())
	must(t, err)
	return h, receipt
}

func acceptedCollection(_ context.Context, receipt authoritystore.CollectionReceipt) (authoritystore.CollectionResult, error) {
	return authoritystore.CollectionResult{Receipt: receipt, AppliedIndex: 123}, nil
}

func TestCollectionReportRetriesLostReplyBeforeReleasingProof(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"accepted", "settled"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			var reports atomic.Int32
			var first authoritystore.CollectionReceipt
			f.cfg.Collection.Report = func(_ context.Context, receipt authoritystore.CollectionReceipt) (authoritystore.CollectionResult, error) {
				if reports.Add(1) == 1 {
					first = receipt // Metadata may commit before the transport loses its reply.
					return authoritystore.CollectionResult{}, fault.ErrUnavailable
				}
				if !first.Equal(receipt) {
					return authoritystore.CollectionResult{}, fault.ErrIdentityMismatch
				}
				return authoritystore.CollectionResult{Receipt: receipt, AppliedIndex: 124, Settled: mode == "settled"}, nil
			}
			h, receipt := collectedHostFixture(t, f)
			if err := h.CollectRetired(deadline(t)); !errors.Is(err, fault.ErrUnavailable) {
				t.Fatal(err)
			}
			row, err := f.cfg.Authority().Collection(deadline(t), receipt.Identity)
			must(t, err)
			if !row.Found || row.Record.State != "collected" {
				t.Fatal("lost reply discarded durable collection", row)
			}
			must(t, f.store.Collect(deadline(t), row.Record.Retired)) // Marker still proves deletion.
			must(t, h.CollectRetired(deadline(t)))
			got, err := f.cfg.Authority().Collection(deadline(t), receipt.Identity)
			must(t, err)
			if got.Found || reports.Load() != 2 {
				t.Fatal("exact retry did not release journal", got, reports.Load())
			}
			if err := f.store.Collect(deadline(t), row.Record.Retired); !errors.Is(err, fs.ErrNotExist) {
				t.Fatal("accepted receipt failed to release marker", err)
			}
		})
	}
}

func TestCollectionReportRejectsWrongEchoAndZeroIndex(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"identity", "member", "grant", "token", "sequence", "time", "index"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			f.cfg.Collection.Report = func(ctx context.Context, receipt authoritystore.CollectionReceipt) (authoritystore.CollectionResult, error) {
				result, _ := acceptedCollection(ctx, receipt)
				switch field {
				case "identity":
					result.Receipt.Identity.ReplicaID++
				case "member":
					result.Receipt.MemberID++
				case "grant":
					result.Receipt.RetirementKey.Revision++
				case "token":
					result.Receipt.Token += "wrong"
				case "sequence":
					result.Receipt.LocalSequence++
				case "time":
					result.Receipt.CollectedAt = result.Receipt.CollectedAt.Add(time.Second)
				case "index":
					result.AppliedIndex = 0
				}
				return result, nil
			}
			h, receipt := collectedHostFixture(t, f)
			if err := h.CollectRetired(deadline(t)); !errors.Is(err, fault.ErrOperationAuthorization) {
				t.Fatal("unverified response released proof", err)
			}
			row, err := f.cfg.Authority().Collection(deadline(t), receipt.Identity)
			must(t, err)
			if !row.Found {
				t.Fatal("wrong echo removed journal")
			}
			must(t, f.store.Collect(deadline(t), row.Record.Retired))
		})
	}
}

func TestCollectionReportRetainsJournalAcrossForgetFailureAndCrash(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"forget failure", "crash after forget"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			path := filepath.Join(t.TempDir(), "authority.bolt")
			journal, err := authoritystore.Open(path, f.node, authoritystore.FreshEvidence)
			must(t, err)
			var current atomic.Pointer[authoritystore.Store]
			current.Store(journal)
			f.cfg.Authority = current.Load
			t.Cleanup(func() { must(t, current.Load().Close()) })
			f.cfg.Collection.Report = acceptedCollection
			var first atomic.Bool
			first.Store(true)
			f.cfg.Retirement = &collectionReleaseStore{RetirementStore: f.store,
				forget: func(ctx context.Context, retired storage.RetiredReplica) error {
					fail := first.Swap(false)
					if fail && mode == "forget failure" {
						return fault.ErrUnavailable
					}
					if err := f.store.ForgetCollection(ctx, retired); err != nil {
						return err
					}
					if fail {
						return current.Load().Close()
					}
					return nil
				}}
			h, receipt := collectedHostFixture(t, f)
			if err := h.CollectRetired(deadline(t)); err == nil {
				t.Fatal("injected proof-release failure disappeared")
			}
			must(t, current.Load().Close())
			reopened, err := authoritystore.Open(path, f.node, authoritystore.RetainedEvidence)
			must(t, err)
			current.Store(reopened)
			row, err := reopened.Collection(deadline(t), receipt.Identity)
			must(t, err)
			if !row.Found || row.Record.State != "collected" {
				t.Fatal("release failure lost collected journal", row)
			}
			if mode == "crash after forget" {
				if err := f.store.Collect(deadline(t), row.Record.Retired); !errors.Is(err, fs.ErrNotExist) {
					t.Fatal("fixture did not cross marker-release boundary", err)
				}
			}
			must(t, h.CollectRetired(deadline(t))) // Must never repeat Collect after marker release.
			row, err = reopened.Collection(deadline(t), receipt.Identity)
			must(t, err)
			if row.Found {
				t.Fatal("release retry did not remove exact collected row")
			}
		})
	}
}

func TestCollectionReportStopCancelsAndJoinsNetworkWait(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.cfg.Collection.Report = func(ctx context.Context, receipt authoritystore.CollectionReceipt) (authoritystore.CollectionResult, error) {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return authoritystore.CollectionResult{}, ctx.Err()
	}
	h, receipt := collectedHostFixture(t, f)
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	done := make(chan error, 1)
	go func() { done <- h.CollectRetired(t.Context()) }()
	waitSignal(t, entered)
	stop, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if err := h.Stop(stop); !errors.Is(err, fault.ErrShutdownTimeout) {
		t.Fatal("reporting owner was released before callback returned", err)
	}
	waitSignal(t, canceled)
	once.Do(func() { close(release) })
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	must(t, h.Stop(deadline(t)))
	row, err := f.cfg.Authority().Collection(deadline(t), receipt.Identity)
	must(t, err)
	if !row.Found || row.Record.State != "collected" {
		t.Fatal("canceled report lost restart evidence", row)
	}
}

func TestCollectionReportDefersReleaseForReadmission(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"reservation", "load floor", "meta floor", "superseded"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			f.cfg.Collection.Report = acceptedCollection
			h, receipt := collectedHostFixture(t, f)
			if mode == "reservation" {
				h.readmissionGate <- struct{}{}
			} else {
				group := receipt.Identity.GroupID
				if mode == "meta floor" {
					group = topology.MetaGroupID
				}
				grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: f.node.ClusterID, GroupID: group, Revision: 20},
					Purpose: topology.AuthorityReadmit, AdmissionID: "pending-readmission",
					Resources: []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: group, NodeName: f.node.NodeName}},
					Targets:   []topology.AuthorityTarget{{NodeName: f.node.NodeName, MemberID: 1, ReplicaID: 1}}}
				_, err := f.cfg.Authority().Fence(deadline(t), grant)
				must(t, err) // Durable pending floor survives a restart with no in-memory reservation.
			}
			if err := h.CollectRetired(deadline(t)); !errors.Is(err, fault.ErrAuthorityPending) {
				t.Fatal("readmission reference did not retain proof", err)
			}
			row, err := f.cfg.Authority().Collection(deadline(t), receipt.Identity)
			must(t, err)
			if !row.Found {
				t.Fatal("readmission lost journal proof")
			}
			must(t, f.store.Collect(deadline(t), row.Record.Retired))
			if mode == "reservation" {
				<-h.readmissionGate
			} else if mode == "superseded" {
				current, err := f.cfg.Authority().Current(deadline(t), receipt.Identity.GroupID)
				must(t, err)
				grant := current.Record.Grant.Clone()
				grant.Key.Revision++
				grant.Purpose, grant.AdmissionID = topology.AuthorityFence, ""
				installed, err := f.cfg.Authority().Fence(deadline(t), grant)
				must(t, err)
				if err := h.CollectRetired(deadline(t)); !errors.Is(err, fault.ErrAuthorityPending) {
					t.Fatal("unjoined superseding fence released readmission proof", err)
				}
				_, err = f.cfg.Authority().CompleteFence(deadline(t), grant.Key, installed.Record.Sequence)
				must(t, err)
			} else {
				// Model the existing Node boundary: only a proven successor rebind
				// makes historical local readmission references non-blocking.
				group := receipt.Identity.GroupID
				if mode == "meta floor" {
					group = topology.MetaGroupID
				}
				floor, err := f.cfg.Authority().Current(deadline(t), group)
				must(t, err)
				_, err = f.cfg.Authority().CompleteFence(deadline(t), floor.Record.Grant.Key, floor.Record.Sequence)
				must(t, err)
				granted := f.node
				granted.MemberID++
				must(t, f.cfg.Authority().Rebind(deadline(t), f.node, granted, "pending-readmission"))
			}
			must(t, h.CollectRetired(deadline(t)))
			row, err = f.cfg.Authority().Collection(deadline(t), receipt.Identity)
			must(t, err)
			if row.Found {
				t.Fatal("resolved readmission permanently pinned proof")
			}
		})
	}
}

func TestCollectionReportMaintenanceSurvivesParticipationFence(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	f.cfg.RetirementInterval = 5 * time.Millisecond
	var eligible atomic.Bool
	f.cfg.Retention = func(context.Context, lifecycle.RetentionInput) (lifecycle.RetentionDecision, error) {
		return lifecycle.RetentionDecision{Collect: eligible.Load()}, nil
	}
	reported := make(chan authoritystore.CollectionReceipt, 1)
	f.cfg.Collection.Report = func(ctx context.Context, receipt authoritystore.CollectionReceipt) (authoritystore.CollectionResult, error) {
		reported <- receipt
		return acceptedCollection(ctx, receipt)
	}
	h, retired := retiredHostCollection(t, f)
	h.Fence()
	eligible.Store(true)
	select {
	case receipt := <-reported:
		if receipt.Identity != retired.Identity {
			t.Fatal("maintenance reported another identity", receipt)
		}
	case <-deadline(t).Done():
		t.Fatal("participation fence stopped collection maintenance")
	}
	await(deadline(t), t, func() bool {
		row, err := f.cfg.Authority().Collection(deadline(t), retired.Identity)
		must(t, err)
		return !row.Found
	})
	must(t, h.Stop(deadline(t)))
}

func TestCollectionReportPagesContinueAfterOneFailedIdentity(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	var released atomic.Int32
	var fail atomic.Bool
	fail.Store(true)
	f.cfg.Collection.Report = func(ctx context.Context, receipt authoritystore.CollectionReceipt) (authoritystore.CollectionResult, error) {
		if receipt.Identity.ReplicaID == 1 && fail.Load() {
			return authoritystore.CollectionResult{}, fault.ErrUnavailable
		}
		return acceptedCollection(ctx, receipt)
	}
	f.cfg.Retirement = &collectionReleaseStore{RetirementStore: f.store,
		forget: func(ctx context.Context, retired storage.RetiredReplica) error {
			row, err := f.cfg.Authority().Collection(ctx, retired.Identity)
			if err != nil || !row.Found || row.Record.State != "collected" {
				return errors.New("journal was removed before store proof release")
			}
			released.Add(1)
			return nil
		}}
	h, receipt := collectedHostFixture(t, f)
	journal := f.cfg.Authority()
	base, err := journal.Collection(deadline(t), receipt.Identity)
	must(t, err)
	// Only journal paging/release is under test; extra records use protocol
	// proofs and a store double, without creating application replicas.
	for replica := uint64(2); replica <= 101; replica++ {
		row := base.Record.Clone()
		row.State, row.Sequence, row.CollectedAt = "prepared", 0, nil
		row.Retired.Identity.ReplicaID = replica
		row.Grant.Action.ReplicaID, row.Grant.Targets[0].ReplicaID = replica, replica
		row.Retirement.Action.ReplicaID, row.Retirement.Target.ReplicaID = replica, replica
		row, err = journal.PrepareCollection(deadline(t), row)
		must(t, err)
		_, err = journal.CompleteCollection(deadline(t), row.Retired.Identity, row.Sequence, receipt.CollectedAt)
		must(t, err)
	}
	if err := h.CollectRetired(deadline(t)); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal(err)
	}
	page, err := journal.Collections(deadline(t), "", 0)
	must(t, err)
	if released.Load() != 100 || len(page.Records) != 1 || page.Records[0].Retired.Identity != receipt.Identity {
		t.Fatal("one failure or removed cursor stranded another page", released.Load(), page)
	}
	fail.Store(false)
	must(t, h.CollectRetired(deadline(t)))
	page, err = journal.Collections(deadline(t), "", 0)
	must(t, err)
	if released.Load() != 101 || len(page.Records) != 0 {
		t.Fatal("failed identity did not retry exactly", released.Load(), page)
	}
}
