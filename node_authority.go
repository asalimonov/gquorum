package gquorum

import (
	"errors"
	"os"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/topology"
)

func (n *node) localAuthority() *authoritystore.Store {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.authority
}

// Run before meta acquisition can establish a new pin. A lost catalog cannot
// make retained load state disappear from the fresh-start decision.
func (n *node) inspectAuthority() error {
	if err := n.ctx.Err(); err != nil {
		return err
	}
	meta, err := metastore.LoadIdentity(n.opts.metaDBPath())
	if err != nil {
		return err
	}
	_, statErr := os.Stat(n.opts.authorityDBPath())
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	authorityExists := statErr == nil
	if meta.Found {
		if !authorityExists {
			return fault.New(fault.Unreadable, "retained meta identity has no authority database", nil)
		}
		return n.acquirePinnedAuthority(meta.Identity)
	}
	fresh, err := metastore.StoreIsFresh(n.opts.metaDBPath())
	if err != nil {
		return err
	}
	if !fresh {
		return fault.New(fault.Unreadable, "unpinned retained metadata cannot initialize authority", nil)
	}
	if authorityExists {
		return n.acquireReadmissionAuthority()
	}
	if err := n.inspectFreshLoadStorage(); err != nil {
		return err
	}
	// A fresh join may not have a member ID yet. The private pre-engine hook
	// acquires under its exact granted ID before any meta pin is created.
	n.authorityFreshness = authoritystore.FreshEvidence
	return nil
}

func (n *node) acquireAuthority(id topology.Identity) error {
	n.authorityGate.Lock()
	defer n.authorityGate.Unlock()
	if err := n.ctx.Err(); err != nil {
		return err
	}
	if id.ClusterID != n.opts.Identity.ClusterID || id.NodeName != n.opts.Identity.NodeName ||
		id.InstanceID != n.opts.Identity.InstanceID {
		return fault.ErrIdentityMismatch
	}
	n.mu.RLock()
	current, stopping := n.authority, n.stopping
	n.mu.RUnlock()
	if stopping {
		return fault.ErrStopped
	}
	if current != nil {
		if current.Identity() != id {
			return fault.ErrIdentityMismatch
		}
		return nil
	}
	if err := n.checkAuthorityPath(); err != nil {
		return err
	}
	s, err := authoritystore.Open(n.opts.authorityDBPath(), id, n.authorityFreshness)
	if err != nil {
		return err
	}
	n.mu.Lock()
	n.authority = s
	n.mu.Unlock()
	n.authorityFreshness = authoritystore.RetainedEvidence
	return n.ctx.Err()
}

func (n *node) closeAuthority() error {
	n.authorityGate.Lock()
	defer n.authorityGate.Unlock()
	n.mu.RLock()
	s := n.authority
	n.mu.RUnlock()
	if s != nil {
		return s.Close()
	}
	return nil
}

func (n *node) acquireMetaAuthority(id topology.Identity) error {
	if err := n.acquireAuthority(id); err != nil {
		return err
	}
	acquired, err := n.localAuthority().MetadataAcquired(n.ctx)
	if err != nil {
		return err
	}
	if acquired {
		meta, err := metastore.LoadIdentity(n.opts.metaDBPath())
		if err != nil {
			return err
		}
		if !meta.Found {
			return fault.New(fault.Unreadable, "acquired metadata is missing; the same member cannot be recreated", nil)
		}
		if meta.Identity.ClusterID != id.ClusterID || meta.Identity.NodeName != id.NodeName ||
			meta.Identity.MemberID != id.MemberID {
			return fault.ErrIdentityMismatch
		}
	}
	stopped, err := n.localAuthority().MetaParticipationStopped(n.ctx)
	if err != nil {
		return err
	}
	if stopped {
		return fault.New(fault.AuthorityFenced, "retained metadata stop requires fresh-member readmission", nil)
	}
	return nil
}

func (n *node) markMetaAcquired(id topology.Identity) error {
	return n.localAuthority().MarkMetadataAcquired(n.ctx, id)
}

func (n *node) acquirePinnedAuthority(pin metastore.Identity) error {
	id := n.opts.Identity
	if pin.ClusterID != id.ClusterID || pin.NodeName != id.NodeName || id.MemberID != 0 &&
		id.MemberID != pin.MemberID {
		return fault.ErrIdentityMismatch
	}
	id.MemberID = pin.MemberID
	return n.acquireAuthority(id)
}

func (n *node) acquireReadmissionAuthority() error {
	if !n.opts.Bootstrap.Join {
		return fault.New(fault.Unreadable, "retained authority without metadata requires explicit readmission", nil)
	}
	id, err := authoritystore.LoadIdentity(n.opts.authorityDBPath())
	if err != nil {
		return err
	}
	if id.Identity.ClusterID != n.opts.Identity.ClusterID || id.Identity.NodeName != n.opts.Identity.NodeName {
		return fault.ErrIdentityMismatch
	}
	id.Identity.InstanceID = n.opts.Identity.InstanceID
	return n.acquireAuthority(id.Identity)
}

func (n *node) inspectFreshLoadStorage() error {
	if wal := n.opts.Loads.Storage.WAL; wal != nil {
		entries, err := os.ReadDir(wal.RootDir)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if len(entries) != 0 {
			return fault.New(fault.Unreadable, "retained load storage has no authority database", nil)
		}
	} else if store := n.opts.Loads.Storage.Store; store != nil {
		retained, err := store.HasState(n.ctx)
		if err != nil {
			return err
		}
		if retained {
			return fault.New(fault.Unreadable, "retained borrowed load storage has no authority database", nil)
		}
		retired, err := n.opts.Loads.Storage.Retirement.Retired(n.ctx)
		if err != nil {
			return err
		}
		if len(retired) != 0 {
			return fault.New(fault.Unreadable, "retired load evidence has no authority database", nil)
		}
	}
	return nil
}
