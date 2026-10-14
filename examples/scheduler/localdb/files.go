package localdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"

	"github.com/asalimonov/gquorum/examples/scheduler/internal/jsonwire"
	"github.com/asalimonov/gquorum/examples/scheduler/task"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"

	// Register the SQLite driver used by XORM.
	_ "modernc.org/sqlite"
	"xorm.io/xorm"
)

const (
	maxEvidenceFileBytes = 64 << 10
	storageGroupID       = "scheduler"
	replicaIDBase        = 10
	privateDirectoryMode = 0700
)

type identityFile struct {
	FormatVersion int              `json:"format_version"`
	Identity      storage.Identity `json:"identity"`
}
type retirementFile struct {
	FormatVersion int                    `json:"format_version"`
	Request       load.RetirementRequest `json:"request"`
}

func applicationDir(root string, id storage.Identity) (string, error) {
	if root == "" || id.GroupID != storageGroupID || id.ClusterID == "" || id.NodeName == "" || id.ReplicaID == 0 {
		return "", fault.ErrIdentityMismatch
	}
	if _, err := task.CheckedInt(id.ReplicaID); err != nil {
		return "", err
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	// The root is caller-selected. Reject symbolic links in the
	// application-owned components; no group or node name from the wire becomes
	// an arbitrary path.
	for _, path := range []string{absoluteRoot, filepath.Join(absoluteRoot, storageGroupID),
		filepath.Join(absoluteRoot, storageGroupID, strconv.FormatUint(id.ReplicaID, replicaIDBase))} {
		if err := os.MkdirAll(path, privateDirectoryMode); err != nil {
			return "", err
		}
		info, err := os.Lstat(path)
		if err != nil {
			return "", err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return "", fault.ErrIdentityMismatch
		}
		if err := syncDirectory(filepath.Dir(path)); err != nil {
			return "", err
		}
	}
	return filepath.Join(absoluteRoot, storageGroupID, strconv.FormatUint(id.ReplicaID, replicaIDBase)), nil
}
func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
func readJSONFile(path string, dst any) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > maxEvidenceFileBytes {
		return fault.ErrIdentityMismatch
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxEvidenceFileBytes+1))
	if err != nil {
		return err
	}
	if len(data) > maxEvidenceFileBytes {
		return fault.ErrIdentityMismatch
	}
	return jsonwire.Decode(data, dst)
}

// atomicNewFile publishes without replacing an existing identity/receipt. The
// Node factory reservation serializes owners; a concurrent publication is still
// refused rather than overwriting evidence. A crash can leave a temporary file,
// which is preserved and refused for operator inspection.
func atomicNewFile(dir, name string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, ".pending-")
	if err != nil {
		return err
	}
	tmp := file.Name()
	defer os.Remove(tmp)
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return err
	}
	if err := os.Link(tmp, filepath.Join(dir, name)); err != nil {
		return err
	}
	if err := os.Remove(tmp); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func verifyOrCreateIdentity(ctx context.Context, dir string, id storage.Identity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !slices.Contains([]string{"identity.json", "retired.json", "tasks.sqlite", "tasks.sqlite-wal",
			"tasks.sqlite-shm"}, entry.Name()) {
			return fault.New(fault.IdentityMismatch, "unexpected application file", nil)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fault.ErrIdentityMismatch
		}
	}
	var saved identityFile
	err = readJSONFile(filepath.Join(dir, "identity.json"), &saved)
	if os.IsNotExist(err) {
		if len(entries) != 0 {
			return fault.New(fault.IdentityMismatch, "application files lack identity marker", nil)
		}
		return atomicNewFile(dir, "identity.json", identityFile{FormatVersion: 1, Identity: id})
	}
	if err != nil {
		return err
	}
	if saved.FormatVersion != 1 || saved.Identity != id {
		return fault.ErrIdentityMismatch
	}
	return nil
}

// Open verifies ownership before opening the local projection and ensures the
// current schema. Business rows come from Restore and Apply only.
func Open(ctx context.Context, root string, id storage.Identity) (*Machine, error) {
	dir, err := applicationDir(root, id)
	if err != nil {
		return nil, err
	}
	if err := verifyOrCreateIdentity(ctx, dir, id); err != nil {
		return nil, err
	}
	if _, err := os.Lstat(filepath.Join(dir, "retired.json")); err == nil {
		return nil, fault.ErrGroupRetired
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	path := filepath.Join(dir, "tasks.sqlite")
	if err := inspectSQLite(ctx, path); err != nil {
		return nil, err
	}
	db, err := openSQLite(path, sqliteReadWrite)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Machine, error) { return nil, errors.Join(err, db.Close()) }
	if err := checkDatabase(ctx, db); err != nil {
		return fail(err)
	}
	for pragma, want := range map[string]string{"foreign_keys": "1", "journal_mode": "wal", "synchronous": "2",
		"busy_timeout": "1000"} {
		var got string
		if err := db.DB().DB.QueryRowContext(ctx, "PRAGMA "+pragma).Scan(&got); err != nil {
			return fail(err)
		}
		if got != want {
			return fail(fmt.Errorf("unexpected SQLite %s=%s", pragma, got))
		}
	}
	if err := ensureSchema(ctx, db); err != nil {
		return fail(err)
	}
	if err := syncDirectory(dir); err != nil {
		return fail(err)
	}
	return &Machine{db: db, identity: id}, nil
}

type sqliteMode uint8

const (
	sqliteReadWrite sqliteMode = iota
	sqliteReadOnly
)

func openSQLite(path string, mode sqliteMode) (*xorm.Engine, error) {
	query := url.Values{"_foreign_keys": {"on"}, "_busy_timeout": {"1000"}}
	if mode == sqliteReadOnly {
		query.Set("mode", "ro")
	} else {
		query.Set("_journal_mode", "wal")
		query.Set("_synchronous", "full")
	}
	dsn := url.URL{Scheme: "file", Path: path, RawQuery: query.Encode()}
	db, err := xorm.NewEngine("sqlite", dsn.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}

// checkDatabase refuses a corrupt file before journaling settings change it.
// Table contents are a projection that Restore replaces on every open.
func checkDatabase(ctx context.Context, db *xorm.Engine) error {
	var integrity string
	if err := db.DB().DB.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&integrity); err != nil {
		return err
	}
	if integrity != "ok" {
		return errors.New("corrupt scheduler SQLite database")
	}
	return nil
}

// Retire removes only this replica’s SQLite files after validating the complete
// retirement proof supplied by Node; identity and retry evidence are retained.
func Retire(ctx context.Context, root string, req load.RetirementRequest) (load.RetirementResult, error) {
	empty := load.RetirementResult{}
	// Node authorized the grant through _meta; Verify checks the complete tuple
	// names exactly this replica before any file is touched.
	if err := req.Verify(req.Identity); err != nil {
		return empty, err
	}
	dir, err := applicationDir(root, req.Identity)
	if err != nil {
		return empty, err
	}
	if err := verifyOrCreateIdentity(ctx, dir, req.Identity); err != nil {
		return empty, err
	}
	receipt := retirementFile{FormatVersion: 1, Request: req}
	var saved retirementFile
	err = readJSONFile(filepath.Join(dir, "retired.json"), &saved)
	if os.IsNotExist(err) {
		if err := atomicNewFile(dir, "retired.json", receipt); err != nil {
			return empty, err
		}
	} else if err != nil {
		return empty, err
	} else if !reflect.DeepEqual(saved, receipt) {
		return empty, fault.ErrOperatorAuthorization
	}
	if err := removeSQLiteFiles(ctx, dir, req.Identity); err != nil {
		return empty, err
	}
	if err := syncDirectory(dir); err != nil {
		return empty, err
	}
	return req.Result(), nil
}

func inspectSQLite(ctx context.Context, path string) error {
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() {
			return fault.ErrIdentityMismatch
		}
		// Inspect existing evidence read-only before enabling/changing
		// journaling.
		inspect, err := openSQLite(path, sqliteReadOnly)
		if err != nil {
			return err
		}
		err = checkDatabase(ctx, inspect)
		err = errors.Join(err, inspect.Close())
		if err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	} else {
		for _, suffix := range []string{"-wal", "-shm"} {
			if _, err := os.Lstat(path + suffix); err == nil {
				return fault.New(fault.Unreadable, "SQLite journal without database", nil)
			} else if !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

func removeSQLiteFiles(ctx context.Context, dir string, id storage.Identity) error {
	for _, name := range []string{"tasks.sqlite-wal", "tasks.sqlite-shm", "tasks.sqlite"} {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Revalidate the marker and every file before each destructive step. A
		// stopped exact owner cannot be replaced until its retirement
		// completes.
		if err := verifyOrCreateIdentity(ctx, dir, id); err != nil {
			return err
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
