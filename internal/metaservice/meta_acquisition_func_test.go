//go:build functional

package metaservice

import (
	"errors"
	"testing"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/internal/raftengine"
	"github.com/asalimonov/gquorum/topology"
)

func TestMetadataAcquisitionFailureRetainsPinWithoutPublishingEngine(t *testing.T) {
	t.Parallel()
	var calls int
	g := joinProtocolClient(t, []string{"127.0.0.1:1"}, func(o *Options) {
		o.AfterOpenEngine = func(id topology.Identity) error {
			calls++
			if id.MemberID != 2 {
				t.Fatal("wrong acquisition identity", id)
			}
			if calls == 1 {
				return fault.ErrUnavailable
			}
			return nil
		}
	})
	id := g.Identity()
	id.MemberID = 2
	if err := g.openEngine(id, raftengine.Joining, nil); !errors.Is(err, fault.ErrUnavailable) {
		t.Fatal(err)
	}
	if g.engine.Load() != nil || g.store != nil || g.Identity().MemberID != 0 {
		t.Fatal("failed acquisition published metadata ownership")
	}
	pin, found, err := metastore.LoadIdentity(g.opts.DBPath)
	if err != nil || !found || pin.MemberID != 2 {
		t.Fatal("acquisition callback ran before durable metadata pin", pin, found, err)
	}
	if err := g.openEngine(id, raftengine.Joining, nil); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || g.engine.Load() == nil || g.engine.Load().Running() || g.Identity() != id {
		t.Fatal("retained acquisition did not resume before participation", calls)
	}
}
