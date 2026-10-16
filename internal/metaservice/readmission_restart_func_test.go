//go:build functional

package metaservice

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/managed"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/topology"
)

func TestReadmissionResumesExactRetirementAcrossProcessRestart(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"retired-before-rebind", "rebound-before-pin", "pin-before-marker", "acquired-before-ready"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			testReadmissionRestart(t, phase)
		})
	}
}

func testReadmissionRestart(t *testing.T, phase string) {
	t.Helper()
	old := readyPublic(t, singleMetaFixture(t))
	previous, err := old.Active(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	metaPath := old.opts.DBPath
	if err := old.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	authorityPath := filepath.Join(t.TempDir(), "authority.bolt")
	peer := &joinProtocolHandler{}
	address := joinProtocolServer(t, peer, []byte("test-secret"))
	granted := previous.Clone()
	granted.Version++
	granted.MetaGroup().NextReplicaID = 3
	granted.MetaGroup().Replicas[0].ReplicaID = 2
	admission := metaengine.AdmissionRecord{NodeName: "n1", InstanceID: "first-readmission", MemberID: 2, OperatorID: topology.MakeOperatorID(granted.Version, 1), State: metaengine.AdmissionPending}
	grant := topology.AuthorityGrant{Key: topology.AuthorityKey{ClusterID: "cluster", GroupID: topology.MetaGroupID, Revision: 100}, Purpose: topology.AuthorityReadmit, AdmissionID: metaengine.ReadmissionID(admission), Resources: []topology.AuthorityResource{{Kind: topology.AuthorityHost, GroupID: topology.MetaGroupID, NodeName: "n1"}}, Targets: []topology.AuthorityTarget{{NodeName: "n1", MemberID: 1, ReplicaID: 1}}}
	var firstReceipt topology.EffectReceipt
	for attempt := range 2 {
		var g *Group
		var authority *authoritystore.Store
		retirementCalls := 0
		g = joinProtocolClient(t, []string{address}, func(o *Options) {
			o.DBPath = metaPath
			o.Limits, o.VoterConfig = old.opts.Limits, old.opts.VoterConfig
			o.Identity.InstanceID = admission.InstanceID
			if attempt == 1 {
				o.Identity.InstanceID = "restarted-readmission"
			}
			id := o.Identity
			id.MemberID = 1
			if attempt == 1 {
				pin, err := authoritystore.LoadIdentity(authorityPath)
				if err != nil || !pin.Found {
					t.Fatal(pin.Identity, pin.Found, err)
				}
				id.MemberID = pin.Identity.MemberID
			}
			var err error
			freshness := authoritystore.RetainedEvidence
			if attempt == 0 {
				freshness = authoritystore.FreshEvidence
			}
			authority, err = authoritystore.Open(authorityPath, id, freshness)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = authority.Close() })
			o.ReadmissionIdentity = authority.Identity
			o.BeforeOpenEngine = func(id topology.Identity) error {
				if id.MemberID == 1 {
					return fault.ErrAuthorityFenced
				}
				if id != authority.Identity() {
					return fault.ErrIdentityMismatch
				}
				return nil
			}
			o.AfterOpenEngine = func(id topology.Identity) error {
				if attempt == 0 && phase == "pin-before-marker" {
					return fault.ErrUnavailable
				}
				return authority.MarkMetadataAcquired(t.Context(), id)
			}
			o.OnReadmission = func(ctx context.Context, proof Readmission) error {
				retirementCalls++
				err := managed.ReadmitMeta(ctx, managed.Readmission(proof), managed.MetaReadmissionOptions{Identity: g.Identity(), Path: metaPath,
					Authority: authority, Authorize: func(ctx context.Context, proof managed.Readmission) error {
						return g.AuthorizeReadmission(ctx, Readmission(proof))
					}})
				if err != nil {
					return err
				}
				if attempt == 0 && phase == "retired-before-rebind" {
					return fault.ErrUnavailable
				}
				return authority.Rebind(ctx, proof.Previous, proof.Granted, proof.AdmissionID)
			}
			o.ReadmissionReceipts = func(ctx context.Context, proof Readmission) ([]topology.EffectReceipt, error) {
				if attempt == 0 && phase == "rebound-before-pin" {
					return nil, fault.ErrUnavailable
				}
				if err := authority.VerifyRebind(ctx, proof.Previous, proof.Granted, proof.AdmissionID); err != nil {
					return nil, err
				}
				r, err := authority.Receipt(ctx, grant, grant.Targets[0])
				if err != nil || !r.Found {
					return nil, fault.ErrAuthorityPending
				}
				return []topology.EffectReceipt{r.Receipt}, nil
			}
		})
		g.ctx = t.Context()
		response := joinResponse{Rules: g.rules, MemberID: 2, PreviousMemberID: 1,
			Readmitted: authority.Identity().MemberID == 1, Admission: admission, Granted: granted,
			Members: []joinMember{{Name: "n1", MemberID: 2}}, LeaderAddress: address,
			Grants: []topology.AuthorityGrant{grant}}
		if attempt == 1 {
			response.Admission.InstanceID = g.Identity().InstanceID
			response.Admission.ReadmissionInstanceID = admission.InstanceID
		}
		data, err := json.Marshal(response)
		if err != nil {
			t.Fatal(err)
		}
		peer.mu.Lock()
		peer.reply = &metapb.Reply{Data: data}
		peer.mu.Unlock()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		err = g.tryJoin(ctx)
		if attempt == 0 && phase != "acquired-before-ready" {
			if !errors.Is(err, fault.ErrUnavailable) || g.engine.Load() != nil {
				t.Fatal("crash boundary acquired unexpected engine", err)
			}
		} else if err != nil {
			t.Fatal("exact readmission did not resume", err)
		}
		if attempt == 0 {
			if retirementCalls != 1 {
				t.Fatal("initial retirement missing", retirementCalls)
			}
			lookup, readErr := authority.Receipt(ctx, grant, grant.Targets[0])
			firstReceipt, err = lookup.Receipt, readErr
			if err != nil || !lookup.Found {
				t.Fatal(firstReceipt, lookup.Found, err)
			}
		} else {
			wantCalls := 0
			if phase == "retired-before-rebind" {
				wantCalls = 1 // reconcile the completed receipt and unchanged backup
			}
			if retirementCalls != wantCalls {
				t.Fatal("rebound successor was retired again", retirementCalls)
			}
			if len(g.joinReceipts) != 1 || !reflect.DeepEqual(g.joinReceipts[0], firstReceipt) {
				t.Fatal("restart changed completed retirement receipt", g.joinReceipts, firstReceipt)
			}
			if err := g.acknowledgeJoin(ctx); err != nil {
				t.Fatal(err)
			}
			if acquired, err := authority.MetadataAcquired(ctx); err != nil || !acquired {
				t.Fatal(acquired, err)
			}
		}
		if err := g.Stop(ctx); err != nil {
			t.Fatal(err)
		}
		if err := authority.Close(); err != nil {
			t.Fatal(err)
		}
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if len(peer.readies) != 1 || peer.readies[0].InstanceID != "restarted-readmission" || len(peer.readies[0].Receipts) != 1 || !reflect.DeepEqual(peer.readies[0].Receipts[0], firstReceipt) {
		t.Fatal("ready proof lost new process or original receipt", peer.readies)
	}
	pin, err := metastore.LoadIdentity(metaPath)
	if err != nil || !pin.Found || pin.Identity.MemberID != 2 {
		t.Fatal(pin.Identity, pin.Found, err)
	}
	backups, err := filepath.Glob(metaPath + ".retired-*")
	if err != nil || len(backups) != 1 {
		t.Fatal("restart changed exact backup identity", backups, err)
	}
}
