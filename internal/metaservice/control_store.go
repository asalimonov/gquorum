package metaservice

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/metaengine"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/topology"
)

// Explicit readmission may inspect retained evidence after Node's durable stop
// guard rejects participation. This path never constructs an engine, rewrites a
// pin or treats the materialized state as replay/application readiness.
func (g *Group) openControlStore(identity topology.Identity) error {
	store, err := metastore.Open(g.opts.DBPath)
	if err != nil {
		return err
	}
	accepted := false
	defer func() {
		if !accepted {
			_ = store.Close()
		}
	}()
	err = store.VerifyIdentity(metastore.Identity{NodeName: identity.NodeName, ClusterID: identity.ClusterID,
		MemberID: identity.MemberID})
	if err != nil {
		return err
	}
	materialized, err := store.LoadState()
	if err != nil {
		return err
	}
	if len(materialized) == 0 {
		return fault.New(fault.Unreadable, "stopped metadata has no retained topology evidence", nil)
	}
	machine, err := metaengine.NewMachine(g.rules, nil)
	if err != nil {
		return err
	}
	if err = machine.Restore(context.Background(), materialized); err != nil {
		return err
	}
	groups, err := machine.RecoveryCatalog(context.Background())
	if err != nil {
		return err
	}
	override, err := store.LoadOverride()
	if err != nil {
		return err
	}
	if override < 0 || override > g.rules.Limits.MaxNodes {
		return fault.New(fault.Unreadable, "invalid persisted local serving override", nil)
	}
	g.mu.Lock()
	g.store, g.machine = store, machine
	g.identity, g.controlOnly, g.serveOverride = identity, true, override
	g.recoveryGroups, g.recoveryVersion = groups, machine.Latest().Version
	g.mu.Unlock()
	accepted = true
	return nil
}
