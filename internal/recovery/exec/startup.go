package exec

import (
	"context"

	"github.com/asalimonov/gquorum/fault"
	recoverydisk "github.com/asalimonov/gquorum/internal/recovery/disk"
	"github.com/asalimonov/gquorum/internal/recovery/model"
)

// VerifyArchived checks accepted backup bytes before first successor acquisition.
// It neither invokes application hooks nor moves source or destination data.
func VerifyArchived(ctx context.Context, owner *recoverydisk.Owner, journal *recoverydisk.Journal) error {
	if owner == nil || journal == nil {
		return fault.ErrInvalid
	}
	e := &Executor{config: Config{Owner: owner, Journal: journal}}
	manifest := journal.View()
	for _, entry := range manifest.Archive {
		if !entry.Moved || entry.Restored {
			return fault.ErrConflict
		}
		err := e.verifyArtifact(ctx, entry, archiveRelative(manifest.BackupName, entry.Name),
			manifest.Resources)
		if err != nil {
			return err
		}
	}
	if manifest.Phase != model.AwaitingStart && manifest.Phase != model.JoinReady {
		return fault.ErrConflict
	}
	return nil
}
