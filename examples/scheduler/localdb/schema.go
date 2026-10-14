package localdb

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/asalimonov/gquorum/examples/scheduler/task"
	"xorm.io/xorm"
)

//go:embed schema.sql
var schemaDDL string

// ensureSchema creates the current tables. The database is a projection that
// Restore repopulates on every engine open, so an older or partial schema is
// dropped and recreated rather than migrated.
func ensureSchema(ctx context.Context, db *xorm.Engine) error {
	tx, err := beginTransaction(ctx, db, nil)
	if err != nil {
		return err
	}
	defer tx.Close()
	var version int
	if err := tx.Tx().Tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version == task.SchemaVersion {
		return nil
	}
	// v1 tables are dropped too; their rows are rebuilt from the Raft log.
	for _, table := range []string{"tasks", "scheduler_state", "members", "app_meta"} {
		if _, err := tx.Tx().Tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			return err
		}
	}
	if _, err := tx.Tx().Tx.ExecContext(ctx, schemaDDL); err != nil {
		return err
	}
	if _, err := tx.Insert(&task.Metadata{ID: 1}); err != nil {
		return err
	}
	if _, err := tx.Tx().Tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version=%d", task.SchemaVersion)); err != nil {
		return err
	}
	return tx.Commit()
}
