//go:build functional

package managed

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/topology"
)

func TestMetaOnlyReadmissionRetainsExactBackup(t *testing.T) {
	t.Parallel()
	before := topology.Identity{NodeName: "a", ClusterID: "cluster", InstanceID: "process", MemberID: 1}
	grant := Readmission{Previous: before, Granted: before, MetaDBPath: filepath.Join(t.TempDir(), "new-parent", "meta.bolt"), AdmissionID: "exec-v2-a1/process"}
	grant.Granted.MemberID = 2
	grant.Grants = readmissionTestGrants(grant)
	s, err := authoritystore.Open(filepath.Join(t.TempDir(), "authority.bolt"), before, authoritystore.FreshEvidence)
	must(t, err)
	t.Cleanup(func() { must(t, s.Close()) })
	authorize := func(ctx context.Context, got Readmission) error {
		if !reflect.DeepEqual(got, grant) {
			return fault.ErrOperationAuthorization
		}
		return ctx.Err()
	}
	if err := ReadmitMeta(t.Context(), grant, MetaReadmissionOptions{Identity: before, Path: grant.MetaDBPath,
		Authority: s, Authorize: authorize}); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("missing retained lineage treated as a fresh join", err)
	}
	writeMeta(t, grant.MetaDBPath, before, 0)
	must(t, ReadmitMeta(t.Context(), grant, MetaReadmissionOptions{Identity: before, Path: grant.MetaDBPath,
		Authority: s, Authorize: authorize}))
	must(t, ReadmitMeta(t.Context(), grant, MetaReadmissionOptions{Identity: before, Path: grant.MetaDBPath,
		Authority: s, Authorize: authorize}))
	if pin, err := metastore.LoadIdentity(readmissionBackup(grant)); err != nil || !pin.Found || pin.Identity.MemberID != 1 {
		t.Fatal(pin.Identity, pin.Found, err)
	}
}

func TestMetaOnlyReadmissionFreshJoinRequiresAuthority(t *testing.T) {
	t.Parallel()
	before := topology.Identity{NodeName: "a", ClusterID: "cluster", InstanceID: "process", MemberID: 1}
	grant := Readmission{Previous: before, Granted: before, MetaDBPath: filepath.Join(t.TempDir(), "new-parent", "meta.bolt"), AdmissionID: "exec-v2-a1/process"}
	grant.Granted.MemberID = 2
	grant.Grants = readmissionTestGrants(grant)
	fresh := before
	fresh.MemberID = 0
	authorize := func(ctx context.Context, _ Readmission) error { return ctx.Err() }
	if err := ReadmitMeta(t.Context(), grant, MetaReadmissionOptions{Identity: fresh, Path: grant.MetaDBPath,
		Authority: nil, Authorize: authorize}); !errors.Is(err, fault.ErrOperationAuthorization) {
		t.Fatal("readmission without durable authority succeeded", err)
	}
	s, err := authoritystore.Open(filepath.Join(t.TempDir(), "authority.bolt"), before, authoritystore.FreshEvidence)
	must(t, err)
	t.Cleanup(func() { must(t, s.Close()) })
	must(t, ReadmitMeta(t.Context(), grant, MetaReadmissionOptions{Identity: fresh, Path: grant.MetaDBPath,
		Authority: s, Authorize: authorize}))
	if _, err := os.Stat(filepath.Dir(grant.MetaDBPath)); !os.IsNotExist(err) {
		t.Fatal("fresh inspection created metadata storage", err)
	}
	g := grant.Grants[0]
	if receipt, err := s.Receipt(t.Context(), g, g.Targets[0]); err != nil || !receipt.Found || receipt.Receipt.Validate(g) != nil {
		t.Fatal("fresh retirement lacks durable evidence", receipt.Receipt, receipt.Found, err)
	}
}
