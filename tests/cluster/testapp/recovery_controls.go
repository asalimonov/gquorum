package main

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/asalimonov/gquorum/recovery"
)

type recoveryControls struct {
	mu                                                            sync.Mutex
	filesRelease                                                  chan struct{}
	afterBackup                                                   chan struct{}
	failBackup, failUndo                                          bool
	holdReadiness                                                 bool
	backups, backedUp, undos, runs, recoveredFactories, readiness atomic.Uint64
}

func (a *application) recoveryControlFiles(ctx context.Context, action recovery.FilesAction) error {
	control := &a.recoveryControls
	if action == recovery.FilesBackup {
		control.backups.Add(1)
	} else {
		control.undos.Add(1)
	}
	control.mu.Lock()
	release := control.filesRelease
	fail := &control.failBackup
	if action == recovery.FilesUndo {
		fail = &control.failUndo
	}
	failed := *fail
	*fail = false
	control.mu.Unlock()
	if failed {
		return errors.New("fixture application file callback failed")
	}
	if release != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
		}
	}
	return ctx.Err()
}
func (a *application) recoveryReadinessHeld() bool {
	control := &a.recoveryControls
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.holdReadiness {
		control.readiness.Add(1)
	}
	return control.holdReadiness
}
func (a *application) recoveryControlsHTTP(w http.ResponseWriter, r *http.Request) {
	var body struct {
		FailBackup      bool `json:"fail_backup_once"`
		FailUndo        bool `json:"fail_undo_once"`
		HoldFiles       bool `json:"hold_files"`
		HoldAfterBackup bool `json:"hold_after_backup"`
		HoldReadiness   bool `json:"hold_readiness"`
	}
	if err := requestBody(w, r, &body); err != nil {
		httpError(w, err)
		return
	}
	control := &a.recoveryControls
	control.mu.Lock()
	if body.HoldFiles && control.filesRelease == nil {
		control.filesRelease = make(chan struct{})
	}
	if !body.HoldFiles && control.filesRelease != nil {
		close(control.filesRelease)
		control.filesRelease = nil
	}
	if body.HoldAfterBackup && control.afterBackup == nil {
		control.afterBackup = make(chan struct{})
	}
	if !body.HoldAfterBackup && control.afterBackup != nil {
		close(control.afterBackup)
		control.afterBackup = nil
	}
	control.holdReadiness = body.HoldReadiness
	control.failBackup, control.failUndo = body.FailBackup, body.FailUndo
	control.mu.Unlock()
	jsonResponse(w, http.StatusOK, map[string]bool{"ok": true})
}
func (a *application) recoveryMetricsHTTP(w http.ResponseWriter, _ *http.Request) {
	control := &a.recoveryControls
	jsonResponse(w, http.StatusOK, map[string]uint64{"backups": control.backups.Load(), "backed_up": control.backedUp.Load(), "undos": control.undos.Load(),
		"runs": control.runs.Load(), "recovered_factories": control.recoveredFactories.Load(), "readiness": control.readiness.Load()})
}

func (a *application) recoveryAfterBackup(ctx context.Context) error {
	control := &a.recoveryControls
	control.backedUp.Add(1)
	control.mu.Lock()
	release := control.afterBackup
	control.mu.Unlock()
	if release != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
		}
	}
	return ctx.Err()
}
