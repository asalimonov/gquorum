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
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	recoverytest "github.com/asalimonov/gquorum/internal/recovery/testutil"
	"github.com/asalimonov/gquorum/topology"
)

func TestRecoveryServiceMetadataImportUsesPrivateOwnerAndKeepsPublicGateClosed(t *testing.T) {
	t.Parallel()
	accepted := recoverytest.Manifest(model.Paths{StorageDir: "/system"}, "service-recovery", time.Now().UTC(), model.Importing)
	groups := singleMetaFixtureWith(t, func(options *Options) {
		options.Identity.ClusterID, options.Identity.MemberID = accepted.Identity.ClusterID, accepted.Identity.MemberID
		options.Founders[0].MemberID = accepted.Identity.MemberID
		options.Limits = accepted.Rules.Limits
		options.Recovery = &accepted
	})
	group := groups[0]
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := group.WaitRecoveryReplay(ctx); err != nil {
		t.Fatal(err)
	}
	if err := group.guardRecoveryMutation(); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("pre-import local gate was not installed", err)
	}
	if before, err := group.ReadRecovery(ctx); err != nil || before != nil {
		t.Fatal(before, err)
	}
	proposal := model.ImportFromManifest(accepted)
	changed := proposal
	changed.Comment = "changed accepted payload"
	if _, err := group.ImportRecovery(ctx, changed); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	operation, err := group.ImportRecovery(ctx, proposal)
	if err != nil {
		t.Fatal("ordinary singleton import failed", err)
	}
	repeated, err := group.ImportRecovery(ctx, proposal)
	if err != nil || operation != repeated {
		t.Fatal("lost import reply was not idempotent", operation, repeated, err)
	}
	record, err := group.ReadRecovery(ctx)
	if err != nil || record == nil || record.Phase != "importing" || record.Survivor.ReplicaID != accepted.Identity.MemberID {
		t.Fatal(record, err)
	}
	record.Policies[0].PolicyDigest = "caller mutation"
	again, err := group.ReadRecovery(ctx)
	if err != nil || reflect.DeepEqual(record, again) {
		t.Fatal("read exposed mutable recovery state", err)
	}
	if _, err := group.AddNode(ctx, topology.Node{Name: "n2", Type: "worker"}, preview()); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("public mutation bypassed import", err)
	}
	if err := group.SetSchedulingEnabled(ctx, true); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal(err)
	}
	for _, private := range []any{proposal, model.GroupConsumedProposal{}, model.CompleteImportProposal{}} {
		encoded, err := metaengine.Encode(private)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := metaengine.DecodeProposal(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if err := group.validateProposalCourier(decoded, &metapb.Header{MemberId: accepted.Identity.MemberID}); !errors.Is(err, fault.ErrOperationAuthorization) {
			t.Fatal("generic ingress accepted private command", err)
		}
	}
	completion := model.CompleteImportProposal{RecoveryID: accepted.Request.RecoveryID,
		PlanDigest: accepted.PlanDigest, At: accepted.AcceptedAt.Add(time.Second), ProposalID: "service-complete"}
	if err := group.CompleteRecovery(ctx, completion); err != nil {
		t.Fatal(err)
	}
	if err := group.guardRecoveryMutation(); err != nil {
		t.Fatal("completion did not reopen public mutation", err)
	}
	if err := group.CompleteRecovery(ctx, completion); err != nil {
		t.Fatal("completion retry failed", err)
	}
	if _, err := group.ImportRecovery(ctx, proposal); err != nil {
		t.Fatal("historical exact import retry failed", err)
	}
}

func TestRecoveryServicePreparationRaisesPreviouslyStagedOnlyGroupFloor(t *testing.T) {
	t.Parallel()
	fixture := newMutationFixture(t)
	base := fixture.g.state().Latest()
	staged := base.Clone()
	staged.Groups = append(staged.Groups, topology.Group{ID: "later", NextReplicaID: 50,
		Replicas: []topology.Replica{{NodeName: "n1", ReplicaID: 49, Role: topology.ReplicaVoter}}})
	fixture.command(t, metaengine.ProposeDocument{Doc: staged, ExpectedVersion: base.Version})
	fixture.command(t, metaengine.ProposeDocument{Doc: base, ExpectedVersion: base.Version + 1})
	result, err := fixture.g.CreateLoadGroup(t.Context(), GroupSpec{ID: "later", Nodes: []string{"n1"}}, preview())
	if err != nil {
		t.Fatal(err)
	}
	group := result.Document.Group("later")
	if group == nil || len(group.Replicas) != 1 || group.Replicas[0].ReplicaID != 50 || group.NextReplicaID != 51 {
		t.Fatal("ordinary creation ignored retained allocation floor", group)
	}
	fixture.command(t, metaengine.ProposeDocument{Doc: result.Document, ExpectedVersion: fixture.g.state().Latest().Version})
}
