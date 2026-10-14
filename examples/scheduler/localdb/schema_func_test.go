//go:build functional

package localdb

import (
	"path/filepath"
	"testing"

	"github.com/asalimonov/gquorum/examples/scheduler/task"
	_ "modernc.org/sqlite"
	"xorm.io/xorm"
)

func schemaFixture(t *testing.T) *xorm.Engine {
	t.Helper()
	db, err := xorm.NewEngine("sqlite", filepath.Join(t.TempDir(), "tasks.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func TestEnsureSchemaReplacesOlderProjection(t *testing.T) {
	t.Parallel()
	db := schemaFixture(t)
	ctx := t.Context()
	// A v1 projection from an earlier example run.
	for _, statement := range []string{
		"CREATE TABLE members (node_name TEXT PRIMARY KEY)",
		"CREATE TABLE scheduler_state (id INTEGER PRIMARY KEY)",
		"CREATE TABLE app_meta (id INTEGER PRIMARY KEY, schema_version INTEGER)",
		"CREATE TABLE tasks (task_id INTEGER PRIMARY KEY)",
		"PRAGMA user_version=1",
	} {
		if _, err := db.DB().DB.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 { // idempotent
		if err := ensureSchema(ctx, db); err != nil {
			t.Fatal(err)
		}
	}
	var version, legacy int
	if err := db.DB().DB.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil || version != task.SchemaVersion {
		t.Fatal(version, err)
	}
	if err := db.DB().DB.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE name IN ('members','scheduler_state')").Scan(&legacy); err != nil || legacy != 0 {
		t.Fatal("legacy tables retained", legacy, err)
	}
	var meta task.Metadata
	if found, err := db.ID(1).Get(&meta); err != nil || !found || meta.LastTaskID != 0 {
		t.Fatal(meta, found, err)
	}
}
