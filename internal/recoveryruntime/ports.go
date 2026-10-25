package recoveryruntime

import (
	"context"

	model "github.com/asalimonov/gquorum/internal/recoverymodel"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/topology"
)

// MetaPort supplies ordinary founding, approved execution and exact private import.
type MetaPort interface {
	Identity() topology.Identity
	WaitRecoveryReplay(context.Context) error
	ImportRecovery(context.Context, model.ImportProposal) (topology.OperationID, error)
	ReadRecovery(context.Context) (*model.ImportState, error)
	RecordRecoveredGroup(context.Context, model.GroupConsumedProposal) error
	CompleteRecovery(context.Context, model.CompleteImportProposal) error
	Operation(context.Context, topology.OperationID) (topology.OperationStatus, error)
	ReadAuthority(context.Context, topology.OperationID) (topology.AuthorityStatus, error)
	RetryOperation(context.Context, topology.OperationID) (topology.OperationID, error)
}

// HostPort never transfers application ownership out of the managed host.
type HostPort interface {
	SetRecovery(context.Context, *model.ImportState, model.LocalManifest) error
	RecoveryCheckpoint(context.Context, load.ReplicaRef) (model.CheckpointProof, error)
	PublishRecovery(context.Context, topology.Identity) error
	ReleaseRecoveryRun(context.Context, topology.Identity) error
}
