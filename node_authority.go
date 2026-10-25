package gquorum

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metastore"
	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/topology"
)

func pathWithin(path, root string) bool {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(path))
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

const (
	metaDBFilename      = "_meta.bolt"
	authorityDBFilename = "_authority.bolt"
)

func (o NodeOptions) metaDBPath() string { return filepath.Join(o.StorageDir, metaDBFilename) }
func (o NodeOptions) authorityDBPath() string {
	return filepath.Join(o.StorageDir, authorityDBFilename)
}

// Validation is lexical: NewNode must not inspect or acquire caller resources.
func validateStorageDir(o NodeOptions) error {
	if strings.TrimSpace(o.StorageDir) == "" || strings.ContainsRune(o.StorageDir, 0) {
		return fault.New(fault.Invalid, "StorageDir requires an explicit directory", nil)
	}
	if wal := o.Loads.Storage.WAL; wal != nil && pathWithin(o.StorageDir, wal.RootDir) {
		return fault.New(fault.Invalid, "StorageDir must be outside the load WAL retirement root", nil)
	}
	return nil
}

// Resolve existing ancestors as well as complete files, so a symlinked parent
// cannot hide an absent authority file inside a replaceable storage directory.
func resolveStoragePath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	current := abs
	var suffix []string
	for {
		resolved, err := filepath.EvalSymlinks(current)
		if err == nil {
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}
			return resolved, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		if info, linkErr := os.Lstat(current); linkErr == nil && info.Mode()&os.ModeSymlink != 0 {
			return "", fault.New(fault.Invalid, "storage path contains a dangling symlink", err)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", err
		}
		suffix = append(suffix, filepath.Base(current))
		current = parent
	}
}

func (n *node) checkAuthorityPath() error {
	dir, err := resolveStoragePath(n.opts.StorageDir)
	if err != nil {
		return err
	}
	if err := n.checkSystemDirectory(dir); err != nil {
		return err
	}
	path, err := resolveStoragePath(n.opts.authorityDBPath())
	if err != nil {
		return err
	}
	meta, err := resolveStoragePath(n.opts.metaDBPath())
	if err != nil {
		return err
	}
	if path == meta {
		return fault.New(fault.Invalid, "authority and meta databases resolve to the same path", nil)
	}
	if err := checkSystemFiles(path, meta); err != nil {
		return err
	}
	if wal := n.opts.Loads.Storage.WAL; wal != nil {
		root, err := resolveStoragePath(wal.RootDir)
		if err != nil {
			return err
		}
		if pathWithin(dir, root) || pathWithin(path, root) || pathWithin(meta, root) {
			return fault.New(fault.Invalid, "system storage resolves inside the load WAL retirement root", nil)
		}
	}
	return nil
}

func (n *node) checkSystemDirectory(dir string) error {
	if info, err := os.Stat(dir); err == nil {
		if !info.IsDir() {
			return fault.New(fault.Invalid, "StorageDir is not a directory", nil)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// A directory with former system filenames is retained evidence, never a
	// fresh node. There is no implicit migration or permission to reset its
	// pins.
	for _, name := range []string{"meta.bolt", "authority.bolt"} {
		if _, err := os.Lstat(filepath.Join(n.opts.StorageDir, name)); err == nil {
			return fault.New(fault.Unreadable, "StorageDir contains unsupported system filename "+name, nil)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func checkSystemFiles(path, meta string) error {
	a, aerr := os.Stat(path)
	m, merr := os.Stat(meta)
	if aerr != nil && !errors.Is(aerr, os.ErrNotExist) {
		return aerr
	}
	if merr != nil && !errors.Is(merr, os.ErrNotExist) {
		return merr
	}
	if merr == nil && !m.Mode().IsRegular() {
		return fault.New(fault.Invalid, "metadata database is not a regular file", nil)
	}
	if aerr == nil && (!a.Mode().IsRegular() || merr == nil && os.SameFile(a, m)) {
		return fault.New(fault.Invalid, "authority database is not an independent regular file", nil)
	}
	return nil
}

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
		return n.inspectPinnedRecoveryAuthority(meta.Identity, statErr)
	}
	fresh, err := metastore.StoreIsFresh(n.opts.metaDBPath())
	if err != nil {
		return err
	}
	if !fresh {
		return fault.New(fault.Unreadable, "unpinned retained metadata cannot initialize authority", nil)
	}
	if authorityExists {
		if resumed, err := n.resumeRecoveryAuthority(); resumed || err != nil {
			return err
		}
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
			return fault.New(fault.Unreadable,
				"acquired metadata is missing; the same member cannot be recreated", nil)
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
	if err := n.localAuthority().MarkMetadataAcquired(n.ctx, id); err != nil {
		return err
	}
	n.mu.RLock()
	journal := n.recoveryJournal
	n.mu.RUnlock()
	if journal != nil && journal.View().Phase == model.JoinReady {
		return journal.MarkJoined(n.ctx, id)
	}
	return nil
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
		retained, err := n.storageOwner.HasLiveLoadEvidence(n.ctx)
		if err != nil {
			return err
		}
		if retained {
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

func (n *node) inspectPinnedRecoveryAuthority(identity metastore.Identity, missing error) error {
	if missing != nil {
		return fault.New(fault.Unreadable, "retained meta identity has no authority database", missing)
	}
	if resumed, err := n.resumePinnedRecoveryFounding(); resumed || err != nil {
		return err
	}
	return n.acquirePinnedAuthority(identity)
}
