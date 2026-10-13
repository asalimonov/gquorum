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
	s, err := authoritystore.Open(filepath.Join(t.TempDir(), "authority.bolt"), before, true)
	must(t, err)
	t.Cleanup(func() { must(t, s.Close()) })
	authorize := func(ctx context.Context, got Readmission) error {
		if !reflect.DeepEqual(got, grant) {
			return fault.ErrOperatorAuthorization
		}
		return ctx.Err()
	}
	if err := ReadmitMeta(t.Context(), before, grant.MetaDBPath, grant, s, authorize); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("missing retained lineage treated as a fresh join", err)
	}
	writeMeta(t, grant.MetaDBPath, before, 0)
	must(t, ReadmitMeta(t.Context(), before, grant.MetaDBPath, grant, s, authorize))
	must(t, ReadmitMeta(t.Context(), before, grant.MetaDBPath, grant, s, authorize))
	if pin, found, err := metastore.LoadIdentity(readmissionBackup(grant)); err != nil || !found || pin.MemberID != 1 {
		t.Fatal(pin, found, err)
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
	if err := ReadmitMeta(t.Context(), fresh, grant.MetaDBPath, grant, nil, authorize); !errors.Is(err, fault.ErrOperatorAuthorization) {
		t.Fatal("readmission without durable authority succeeded", err)
	}
	s, err := authoritystore.Open(filepath.Join(t.TempDir(), "authority.bolt"), before, true)
	must(t, err)
	t.Cleanup(func() { must(t, s.Close()) })
	must(t, ReadmitMeta(t.Context(), fresh, grant.MetaDBPath, grant, s, authorize))
	if _, err := os.Stat(filepath.Dir(grant.MetaDBPath)); !os.IsNotExist(err) {
		t.Fatal("fresh inspection created metadata storage", err)
	}
	g := grant.Grants[0]
	if receipt, found, err := s.Receipt(t.Context(), g, g.Targets[0]); err != nil || !found || receipt.Validate(g) != nil {
		t.Fatal("fresh retirement lacks durable evidence", receipt, found, err)
	}
}
