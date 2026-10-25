package recovery_test

import (
	"net"

	"github.com/asalimonov/gquorum/internal/recovery/model"
	"github.com/asalimonov/gquorum/recovery"

	quorum "github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/load"
	"github.com/asalimonov/gquorum/storage"
)

type unopenedListener struct{ net.Listener }
type unopenedTransport struct{ load.Transport }
type unopenedStorage struct {
	storage.Store
	storage.RetirementStore
}
type recoveryFacade struct {
	quorum.Recovery
	paths model.Paths
	files recovery.FilesHook
}

func newRecovery(opts quorum.NodeOptions, options ...recovery.Options) (*recoveryFacade, error) {
	handle, err := quorum.NewRecovery(opts, options...)
	if err != nil {
		return nil, err
	}
	paths := model.Paths{StorageDir: opts.StorageDir}
	if opts.Loads.Storage.WAL != nil {
		paths.LoadWALDir = opts.Loads.Storage.WAL.RootDir
	}
	var files recovery.FilesHook
	if len(options) == 1 {
		files = options[0].Files
	}
	return &recoveryFacade{Recovery: handle, paths: paths, files: files}, nil
}
func sourcePaths(facade *recoveryFacade) model.Paths      { return facade.paths }
func filesHook(facade *recoveryFacade) recovery.FilesHook { return facade.files }

const metaDBFilename = "_meta.bolt"
const authorityDBFilename = "_authority.bolt"
