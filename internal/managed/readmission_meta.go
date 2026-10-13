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

func inspectReadmissionMeta(ctx context.Context, grant Readmission, allowMissing bool) (retainedMeta, error) {
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
		if !allowMissing {
			return state, fault.New(fault.IdentityMismatch, "previous meta pin and its exact readmission backup are absent", nil)
		}
		state.fresh = true
		return state, nil
	}
	state.live = live
	path := state.backup
	if live {
		path = grant.MetaDBPath
	}
	pin, found, err := metastore.LoadIdentity(path)
	if err != nil {
		return state, err
	}
	if !found {
		if live && allowMissing {
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
	want := metastore.Identity{NodeName: grant.Previous.NodeName, ClusterID: grant.Previous.ClusterID, MemberID: grant.Previous.MemberID}
	if pin != want {
		return state, fault.ErrIdentityMismatch
	}
	state.records, err = metastore.LoadReplicaRecords(path)
	if err == nil {
		err = ctx.Err()
	}
	return state, err
}

func retireReadmissionMeta(ctx context.Context, grant Readmission, allowMissing bool) error {
	state, err := inspectReadmissionMeta(ctx, grant, allowMissing)
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
	parent, err := os.Open(filepath.Dir(grant.MetaDBPath))
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

// ReadmitMeta is the same verified metadata retirement for a meta-only Node.
// It refuses a grant with former load resources, which require a managed host.
func ReadmitMeta(ctx context.Context, own topology.Identity, path string, grant Readmission, authorityStore *authoritystore.Store, authorize func(context.Context, Readmission) error) error {
	grant = grant.Clone()
	if err := grant.Validate(own, path); err != nil {
		return err
	}
	if authorize == nil {
		return fault.ErrUnsupported
	}
	if err := authorize(ctx, grant.Clone()); err != nil {
		return err
	}
	authority, err := beginReadmissionAuthority(ctx, authorityStore, grant)
	if err != nil {
		return err
	}
	if err := authority.joined(ctx); err != nil {
		return err
	}
	for _, group := range grant.PreviousGroups {
		if group.ID != topology.MetaGroupID {
			return fault.ErrNoController
		}
	}
	state, err := inspectReadmissionMeta(ctx, grant, own.MemberID == 0)
	if err != nil {
		return err
	}
	if len(state.records) != 0 {
		return fault.ErrNoController
	}
	if err := authorize(ctx, grant.Clone()); err != nil {
		return err
	}
	return authority.retireMeta(ctx, grant, own.MemberID == 0)
}
