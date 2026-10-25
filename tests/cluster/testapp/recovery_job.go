package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"

	"github.com/asalimonov/gquorum"
	"github.com/asalimonov/gquorum/fault"
	"github.com/asalimonov/gquorum/recovery"
)

// This is caller state, outside the library's archive. Never parse private
// recovery journals to decide whether the process may start consensus.
type recoveryJob struct {
	ID            string                `json:"id"`
	SourceCluster string                `json:"source_cluster"`
	Maintenance   bool                  `json:"maintenance"`
	Phase         string                `json:"phase"`
	Request       recovery.Request      `json:"request"`
	Plan          *recovery.Plan        `json:"plan,omitempty"`
	Acceptance    *recovery.Acceptance  `json:"acceptance,omitempty"`
	Operation     *recovery.Operation   `json:"operation,omitempty"`
	Status        *recovery.Status      `json:"status,omitempty"`
	Cancellation  *recoveryCancellation `json:"cancellation,omitempty"`
	Error         string                `json:"error,omitempty"`
}
type recoveryCancellation struct {
	Actor  string `json:"actor"`
	Reason string `json:"reason"`
}
type recoveryJobs struct {
	mu      sync.Mutex
	app     *application
	job     *recoveryJob
	cancel  context.CancelFunc
	done    chan struct{}
	workers sync.WaitGroup
}

const recoveryJobFile = "recovery-job.json"
const recoveryJobMaxBytes = 64 << 20

func readRecoveryJob(root string) (*recoveryJob, error) {
	file, err := os.Open(filepath.Join(root, recoveryJobFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() > recoveryJobMaxBytes {
		return nil, fault.ErrInvalid
	}
	raw, err := io.ReadAll(io.LimitReader(file, recoveryJobMaxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > recoveryJobMaxBytes {
		return nil, fault.ErrInvalid
	}
	var job recoveryJob
	if err := decodeJSON(raw, &job); err != nil {
		return nil, err
	}
	if err := validateRecoveryJob(job); err != nil {
		return nil, err
	}
	return &job, nil
}
func validateRecoveryJob(job recoveryJob) error {
	if job.ID == "" || job.SourceCluster == "" || job.Request.Actor == "" || job.Request.Comment == "" {
		return fault.ErrInvalid
	}
	switch job.Phase {
	case "preparing", "prepared", "confirmed", "running", "starting", "complete", "cancelling", "cancelled":
	default:
		return fault.ErrInvalid
	}
	if job.Request.Mode != recovery.Rebuild && job.Request.Mode != recovery.ResetForJoin {
		return fault.ErrInvalid
	}
	if !job.Maintenance && job.Phase != "complete" && job.Phase != "cancelled" {
		return fault.ErrInvalid
	}
	if job.Acceptance != nil && (job.Plan == nil || job.Acceptance.PlanDigest != job.Plan.Digest) {
		return fault.ErrInvalid
	}
	if job.Operation != nil && (job.Acceptance == nil || job.Operation.RecoveryID != job.Request.RecoveryID) {
		return fault.ErrInvalid
	}
	return nil
}
func cloneRecoveryJob(job *recoveryJob) *recoveryJob {
	if job == nil {
		return nil
	}
	data, _ := json.Marshal(job)
	var result recoveryJob
	_ = json.Unmarshal(data, &result)
	return &result
}
func (s *recoveryJobs) saveLocked(job *recoveryJob) error {
	if err := validateRecoveryJob(*job); err != nil {
		return err
	}
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	if len(data) > recoveryJobMaxBytes {
		return fault.ErrInvalid
	}
	// Intent publication must complete even when an HTTP waiter disconnects.
	if err := writeProjection(context.Background(), filepath.Join(s.app.cfg.DataDir, recoveryJobFile), data); err != nil {
		return err
	}
	s.job = cloneRecoveryJob(job)
	return nil
}
func (s *recoveryJobs) update(change func(*recoveryJob)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := cloneRecoveryJob(s.job)
	change(job)
	return s.saveLocked(job)
}
func (a *application) recoveryMaintenance() bool {
	if a.recoveryJobs == nil {
		return false
	}
	a.recoveryJobs.mu.Lock()
	defer a.recoveryJobs.mu.Unlock()
	return a.recoveryJobs.job != nil && a.recoveryJobs.job.Maintenance
}
func (a *application) hasRecoveryJob() bool {
	if a.recoveryJobs == nil {
		return false
	}
	a.recoveryJobs.mu.Lock()
	defer a.recoveryJobs.mu.Unlock()
	return a.recoveryJobs.job != nil
}
func (s *recoveryJobs) handle() (*gquorum.Recovery, error) {
	s.mu.Lock()
	source := s.app.nodeOptions.Identity.ClusterID
	if s.job != nil {
		source = s.job.SourceCluster
	}
	s.mu.Unlock()
	opts := s.app.nodeOptions
	opts.Identity.ClusterID = source
	return gquorum.NewRecovery(opts, recovery.Options{Files: s.app.recoveryFiles})
}
func (s *recoveryJobs) view(ctx context.Context, id string) (*recoveryJob, error) {
	s.mu.Lock()
	job := cloneRecoveryJob(s.job)
	s.mu.Unlock()
	if job == nil || job.ID != id {
		return nil, fault.ErrHistoryUnavailable
	}
	if job.Acceptance != nil {
		handle, err := s.handle()
		if err != nil {
			return nil, err
		}
		status, err := handle.Status(ctx, job.Request.RecoveryID)
		if err == nil {
			job.Status = &status
		} else if !errors.Is(err, fault.ErrHistoryUnavailable) {
			return nil, err
		}
	}
	return job, nil
}
func (s *recoveryJobs) prepare(id string, req recovery.Request) error {
	if id == "" || req.Actor == "" || req.Comment == "" {
		return fault.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.job != nil {
		if s.job.ID == id && sameRecoveryRequest(s.job.Request, req) {
			return nil
		}
		return fault.ErrConflict
	}
	job := &recoveryJob{ID: id, SourceCluster: s.app.currentNode().Identity().ClusterID,
		Maintenance: true, Phase: "preparing", Request: req}
	if err := s.saveLocked(job); err != nil {
		return err
	}
	s.launchLocked()
	return nil
}
func sameRecoveryRequest(effective, requested recovery.Request) bool {
	// Generated fields belong to the saved plan. A repeated original prepare is
	// allowed; caller-provided nonempty identities must still match exactly.
	if requested.RecoveryID == "" {
		effective.RecoveryID = ""
	}
	if requested.ClusterID == "" {
		effective.ClusterID = ""
	}
	return reflect.DeepEqual(effective, requested)
}
func (s *recoveryJobs) command(id, action string, acceptance recovery.Acceptance,
	cancellation recoveryCancellation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.job == nil || s.job.ID != id {
		return fault.ErrHistoryUnavailable
	}
	job := cloneRecoveryJob(s.job)
	switch action {
	case "confirm":
		if job.Acceptance != nil {
			if *job.Acceptance != acceptance {
				return fault.ErrConflict
			}
		} else {
			if job.Phase != "prepared" || job.Plan == nil || job.Plan.Digest == "" || job.Error != "" ||
				acceptance.PlanDigest != job.Plan.Digest ||
				!acceptance.AcknowledgeDataLoss || !acceptance.OldDeploymentFenced {
				return fault.ErrInvalid
			}
			job.Acceptance, job.Phase = &acceptance, "confirmed"
		}
	case "retry":
		if !job.Maintenance {
			return nil
		}
	case "cancel":
		if cancellation.Actor == "" || cancellation.Reason == "" {
			return fault.ErrInvalid
		}
		if job.Cancellation != nil && *job.Cancellation != cancellation {
			return fault.ErrConflict
		}
		if job.Phase == "cancelled" {
			return nil
		}
		if !job.Maintenance {
			return fault.ErrConflict
		}
		job.Cancellation, job.Phase = &cancellation, "cancelling"
	default:
		return fault.ErrInvalid
	}
	job.Error = ""
	if err := s.saveLocked(job); err != nil {
		return err
	}
	if action == "cancel" && s.cancel != nil {
		s.cancel()
	}
	s.launchLocked()
	return nil
}
