//go:build !functional

package managed

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/topology"
)

func TestMetaOnlyReadmissionFreshJoinAndExactBackup(t *testing.T) {
	t.Parallel()
	before := topology.Identity{NodeName: "a", ClusterID: "cluster", InstanceID: "process", MemberID: 1}
	grant := Readmission{Previous: before, Granted: before, MetaDBPath: filepath.Join(t.TempDir(), "new-parent", "meta.bolt"), AdmissionID: "exec-v2-a1/process"}
	grant.Granted.MemberID = 2
	authorize := func(ctx context.Context, got Readmission) error {
		if !reflect.DeepEqual(got, grant) {
			return fault.ErrOperatorAuthorization
		}
		return ctx.Err()
	}
	fresh := before
	fresh.MemberID = 0
	must(t, ReadmitMeta(t.Context(), fresh, grant.MetaDBPath, grant, authorize))
	if _, err := os.Stat(filepath.Dir(grant.MetaDBPath)); !os.IsNotExist(err) {
		t.Fatal("fresh inspection created storage", err)
	}
	if err := ReadmitMeta(t.Context(), before, grant.MetaDBPath, grant, authorize); !errors.Is(err, fault.ErrIdentityMismatch) {
		t.Fatal("missing retained lineage treated as a fresh join", err)
	}
	writeMeta(t, grant.MetaDBPath, before, 0)
	must(t, ReadmitMeta(t.Context(), before, grant.MetaDBPath, grant, authorize))
	must(t, ReadmitMeta(t.Context(), before, grant.MetaDBPath, grant, authorize))
	if pin, found, err := metastore.LoadIdentity(readmissionBackup(grant)); err != nil || !found || pin.MemberID != 1 {
		t.Fatal(pin, found, err)
	}
}
