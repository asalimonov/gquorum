//go:build functional

package metaservice

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/managed"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/metatransport"
	"github.com/asalimonov/gquorum/topology"
)

func TestReadmissionJoinCarriesDurableAuthorityAndRetriesReadyReceipts(t *testing.T) {
	t.Parallel()
	for _, retained := range []bool{false, true} {
		name := "missing_meta"
		if retained {
			name = "retained_meta"
		}
		t.Run(name, func(t *testing.T) { t.Parallel(); testReadmissionJoin(t, retained) })
	}
}

func testReadmissionJoin(t *testing.T, retained bool) {
	t.Helper()
	var metaPath string
	var retainedOptions Options
	if retained {
		old := readyPublic(t, singleMetaFixture(t))
		metaPath = old.opts.DBPath
		retainedOptions = old.opts
		if err := old.Stop(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	peer := &joinProtocolHandler{readyReply: metatransport.Reply(fault.ErrUnavailable)}
	address := joinProtocolServer(t, peer, []byte("test-secret"))
	var g *Group
	var authority *authoritystore.Store
	var callbacks, acquisitions, proofReads atomic.Int32
	g = joinProtocolClient(t, []string{address}, func(o *Options) {
		if retained {
			o.DBPath = metaPath
			o.Limits, o.VoterPolicy = retainedOptions.Limits, retainedOptions.VoterPolicy
		}
		former := o.Identity
		former.MemberID = 1
		var err error
		authority, err = authoritystore.Open(filepath.Join(t.TempDir(), "authority.bolt"), former, true)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := authority.Close(); err != nil {
				t.Error(err)
			}
		})
		o.ReadmissionIdentity = authority.Identity
		o.OnReadmission = func(ctx context.Context, grant Readmission) error {
			callbacks.Add(1)
			if err := g.AuthorizeReadmission(ctx, grant); err != nil {
				return err
			}
			err := managed.ReadmitMeta(ctx, g.Identity(), o.DBPath, managed.Readmission(grant), authority, func(ctx context.Context, got managed.Readmission) error {
				return g.AuthorizeReadmission(ctx, Readmission(got))
			})
			if err != nil {
				return err
			}
			return authority.Rebind(ctx, grant.Previous, grant.Granted, grant.AdmissionID)
		}
		o.BeforeOpenEngine = func(id topology.Identity) error {
			if retained && id.MemberID == 1 {
				return fault.ErrAuthorityFenced
			}
			acquisitions.Add(1)
			if id != authority.Identity() || id.MemberID != 2 {
				return fault.ErrIdentityMismatch
			}
			floor, found, err := authority.Current(t.Context(), topology.MetaGroupID)
			if err != nil {
				return err
			}
			if !found {
				return fault.ErrAuthorityPending
			}
			if _, found, err := authority.Receipt(t.Context(), floor.Grant, floor.Grant.Targets[0]); err != nil {
				return err
			} else if !found {
				return fault.ErrAuthorityPending
			}
			return nil
		}
		o.ReadmissionReceipts = func(ctx context.Context, grant Readmission) ([]topology.EffectReceipt, error) {
			if proofReads.Add(1) == 1 {
				return nil, fault.ErrUnavailable
			}
			if authority.Identity() != grant.Granted {
				return nil, fault.ErrIdentityMismatch
			}
			var receipts []topology.EffectReceipt
			for _, g := range grant.Grants {
				for _, target := range g.Targets {
					r, found, err := authority.Receipt(ctx, g, target)
					if err != nil {
						return nil, err
					}
					if !found {
						return nil, fault.ErrAuthorityPending
					}
					receipts = append(receipts, r)
				}
			}
			return receipts, nil
		}
	})
	g.ctx = t.Context()
	previous := topology.Document{Version: 1, ClusterID: "cluster", Origin: topology.OriginFounding, Placement: topology.PlacementRule{MinReplicas: 1, MaxReplicas: 3}, Nodes: []topology.Node{{Name: "n1", Type: "worker", State: topology.NodeActive}}, Groups: []topology.Group{{ID: topology.MetaGroupID, NextReplicaID: 2, Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 1, Role: topology.ReplicaVoter}}}}}
	granted := previous.Clone()
	granted.Version = 2
	granted.MetaGroup().NextReplicaID = 3
	granted.MetaGroup().Replicas[0].ReplicaID = 2
	a := metaengine.AdmissionRecord{NodeName: "n1", InstanceID: g.Identity().InstanceID, MemberID: 2, OperatorID: topology.MakeOperatorID(2, 1), State: metaengine.AdmissionPending}
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "cluster", GroupID: topology.MetaGroupID, Revision: 10}, Purpose: topology.AuthorityReadmit, AdmissionID: metaengine.ReadmissionID(a), Resources: []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: topology.MetaGroupID, NodeName: "n1"}}, Targets: []topology.AuthorityTarget{{NodeName: "n1", MemberID: 1, ReplicaID: 1}}}
	response := joinResponse{Rules: g.rules, MemberID: 2, PreviousMemberID: 1, Readmitted: true, Admission: a, Previous: previous, Granted: granted, LeaderAddress: address, Grants: []topology.AuthorityGrant{grant}}
	data, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	peer.reply = &metapb.Reply{Data: data}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err = g.tryJoin(ctx); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal(err)
	}
	if g.engine.Load() != nil || acquisitions.Load() != 0 || authority.Identity().MemberID != 2 {
		t.Fatal("missing receipt proof opened replacement", acquisitions.Load(), authority.Identity())
	}
	response.Readmitted = false // same admission, already rebound authority, no metadata pin yet
	data, err = json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	peer.mu.Lock()
	peer.reply = &metapb.Reply{Data: data}
	peer.mu.Unlock()
	if err = g.tryJoin(ctx); err != nil {
		t.Fatal(err)
	}
	if callbacks.Load() != 1 || acquisitions.Load() != 1 || g.Identity().MemberID != 2 || authority.Identity().MemberID != 2 {
		t.Fatal("replacement crossed acquisition before retirement/rebind", callbacks.Load(), acquisitions.Load(), g.Identity(), authority.Identity())
	}
	peer.mu.Lock()
	requests := append([]joinRequest(nil), peer.requests...)
	peer.mu.Unlock()
	var headerMember uint64
	if retained {
		headerMember = 1
	}
	if len(requests) != 2 || requests[0].Identity.MemberID != headerMember || requests[0].AuthorityMemberID != 1 || requests[1].Identity.MemberID != headerMember || requests[1].AuthorityMemberID != 2 {
		t.Fatal("missing metadata erased independent authority identity", requests)
	}
	if err = g.acknowledgeJoin(ctx); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal(err)
	}
	if g.joinReady {
		t.Fatal("failed acknowledgement became ready")
	}
	peer.mu.Lock()
	peer.readyReply = nil
	peer.mu.Unlock()
	if err = g.acknowledgeJoin(ctx); err != nil {
		t.Fatal(err)
	}
	peer.mu.Lock()
	readies := append([]readyRequest(nil), peer.readies...)
	peer.mu.Unlock()
	if len(readies) != 2 || len(readies[0].Receipts) != 1 || !reflect.DeepEqual(readies[0], readies[1]) || readies[0].Receipts[0].Validate(grant) != nil || readies[0].MemberID != 2 {
		t.Fatal("ready retry lost exact historical receipt", readies)
	}
	if err = g.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	pin, found, err := metastore.LoadIdentity(g.opts.DBPath)
	if err != nil || !found || pin.MemberID != 2 {
		t.Fatal("replacement metadata pin missing", pin, found, err)
	}
}
