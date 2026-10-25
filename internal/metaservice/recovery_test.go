//go:build !functional

package metaservice

import (
	"errors"
	"testing"
	"time"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metapb"
	"github.com/asalimonov/gquorum/internal/recovery/model"
	recoverytest "github.com/asalimonov/gquorum/internal/recovery/testutil"
)

func TestRecoveryPrivateIngressCannotBorrowOrdinaryMutationAuthorization(t *testing.T) {
	t.Parallel()
	group := &Group{}
	for _, proposal := range []any{model.ImportProposal{}, model.GroupConsumedProposal{}, model.CompleteImportProposal{}} {
		data, err := metaengine.Encode(proposal)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := metaengine.DecodeProposal(data)
		if err != nil {
			t.Fatal(err)
		}
		if err := group.validateProposalCourier(decoded, &metapb.Header{}); !errors.Is(err, fault.ErrOperationAuthorization) {
			t.Fatal("ordinary ingress accepted private command", err)
		}
	}
	if err := group.authorizeRecoveryOwner(); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal(err)
	}
}

func TestRecoveryLocalOwnerRequiresExactIdentityRulesAndSuccessorMarker(t *testing.T) {
	t.Parallel()
	manifest := recoverytest.Manifest(model.Paths{StorageDir: "/system"}, "private-owner", time.Unix(110, 0).UTC(), model.Importing)
	group := &Group{opts: Options{Recovery: &manifest}, identity: manifest.Identity, rules: manifest.Rules}
	if err := group.authorizeRecoveryOwner(); err != nil {
		t.Fatal(err)
	}
	group.rules.Limits.MaxNodes++
	if err := group.authorizeRecoveryOwner(); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	group.rules = manifest.Rules
	group.identity.MemberID++
	if err := group.authorizeRecoveryOwner(); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal(err)
	}
	group.identity = manifest.Identity
	manifest.SuccessorStartedAt = nil
	if err := group.authorizeRecoveryOwner(); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal(err)
	}
}
