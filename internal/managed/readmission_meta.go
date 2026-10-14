package managed

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/internal/authoritystore"
	"github.com/asalimonov/gquorum/internal/metastore"
	"github.com/asalimonov/gquorum/topology"
)

type retainedMeta struct {
	live, fresh bool
	backup      string
	records     map[string]metastore.ReplicaRecord
}

type readmissionMetaMode uint8

const (
	retainedReadmissionMeta readmissionMetaMode = iota
	freshReadmissionMetaAllowed
)

func readmissionMetaModeFor(identity topology.Identity) readmissionMetaMode {
	if identity.MemberID == 0 {
		return freshReadmissionMetaAllowed
	}
	return retainedReadmissionMeta
}

func readmissionBackup(grant Readmission) string {
	nonce := sha256.Sum256([]byte(grant.AdmissionID))
	return grant.MetaDBPath + fmt.Sprintf(".retired-%d-", grant.Previous.MemberID) + hex.EncodeToString(nonce[:8])
}

func regularMetaFile(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() {
		return false, fault.New(fault.IdentityMismatch, "retained meta path is not a regular database file", nil)
	}
	return true, nil
}

func inspectReadmissionMeta(ctx context.Context, grant Readmission, mode readmissionMetaMode) (retainedMeta, error) {
	state := retainedMeta{backup: readmissionBackup(grant)}
	if err := ctx.Err(); err != nil {
		return state, err
	}
	live, err := regularMetaFile(grant.MetaDBPath)
	if err != nil {
		return state, err
	}
	backup, err := regularMetaFile(state.backup)
	if err != nil {
		return state, err
	}
	if live && backup {
		return state, fault.New(fault.IdentityMismatch, "readmission backup and live database both exist", nil)
	}
	if !live && !backup {
		if mode != freshReadmissionMetaAllowed {
			return state, fault.New(fault.IdentityMismatch,
				"previous meta pin and its exact readmission backup are absent", nil)
		}
		state.fresh = true
		return state, nil
	}
	state.live = live
	return inspectReadmissionPin(ctx, grant, mode, state)
}

func retireReadmissionMeta(ctx context.Context, grant Readmission, mode readmissionMetaMode) error {
	state, err := inspectReadmissionMeta(ctx, grant, mode)
	if err != nil {
		return err
	}
	if state.live && !state.fresh {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := os.Rename(grant.MetaDBPath, state.backup); err != nil {
			return err
		}
	}
	if err := syncReadmissionParent(grant.MetaDBPath, state); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	fresh, err := metastore.StoreIsFresh(grant.MetaDBPath)
	if err != nil {
		return err
	}
	if !fresh {
		return fault.New(fault.IdentityMismatch, "readmission live database is not fresh", nil)
	}
	return nil
}

// MetaReadmissionOptions supplies the local identity, retained path and authorization owner.
type MetaReadmissionOptions struct {
	Identity  topology.Identity
	Path      string
	Authority *authoritystore.Store
	Authorize func(context.Context, Readmission) error
}

// ReadmitMeta is the same verified metadata retirement for a meta-only Node.
// It refuses a grant with former load resources, which require a managed host.
func ReadmitMeta(ctx context.Context, grant Readmission, options MetaReadmissionOptions) error {
	own, path, authorityStore, authorize := options.Identity, options.Path, options.Authority, options.Authorize
	cloned := grant.Clone()
	if err := cloned.Validate(own, path); err != nil {
		return err
	}
	if authorize == nil {
		return fault.ErrUnsupported
	}
	if err := authorize(ctx, cloned.Clone()); err != nil {
		return err
	}
	authority, err := beginReadmissionAuthority(ctx, authorityStore, cloned)
	if err != nil {
		return err
	}
	if err := authority.joined(ctx); err != nil {
		return err
	}
	for _, group := range cloned.PreviousGroups {
		if group.ID != topology.MetaGroupID {
			return fault.ErrNoController
		}
	}
	state, err := inspectReadmissionMeta(ctx, cloned, readmissionMetaModeFor(own))
	if err != nil {
		return err
	}
	if len(state.records) != 0 {
		return fault.ErrNoController
	}
	if err := authorize(ctx, cloned.Clone()); err != nil {
		return err
	}
	return authority.retireMeta(ctx, cloned, readmissionMetaModeFor(own))
}

func inspectReadmissionPin(ctx context.Context, grant Readmission,
	mode readmissionMetaMode, initial retainedMeta) (retainedMeta, error) {
	state := initial
	path := state.backup
	if state.live {
		path = grant.MetaDBPath
	}
	pin, err := metastore.LoadIdentity(path)
	if err != nil {
		return state, err
	}
	if !pin.Found {
		if state.live && mode == freshReadmissionMetaAllowed {
			fresh, err := metastore.StoreIsFresh(path)
			if err != nil {
				return state, err
			}
			if fresh {
				state.fresh = true
				return state, nil
			}
		}
		return state, fault.New(fault.Unreadable, "retained readmission database lacks its exact identity", nil)
	}
	want := metastore.Identity{NodeName: grant.Previous.NodeName, ClusterID: grant.Previous.ClusterID,
		MemberID: grant.Previous.MemberID}
	if pin.Identity != want {
		return state, fault.ErrIdentityMismatch
	}
	state.records, err = metastore.LoadReplicaRecords(path)
	if err == nil {
		err = ctx.Err()
	}
	return state, err
}

func syncReadmissionParent(path string, state retainedMeta) error {
	parent, err := os.Open(filepath.Dir(path))
	if err != nil {
		// A fresh join may have neither a file nor a parent directory yet.
		if !(state.fresh && errors.Is(err, os.ErrNotExist)) {
			return err
		}
	} else {
		err = parent.Sync()
		closeErr := parent.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}
